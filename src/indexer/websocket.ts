import {
  Connection,
  PublicKey,
  Logs,
  Context,
  ConfirmedSignatureInfo,
  ParsedTransactionWithMeta,
} from "@solana/web3.js";
import { PrismaClient } from "@prisma/client";
import PQueue from "p-queue";
import WebSocket from "ws";
import { config } from "../config.js";
import { parseTransaction, parseTransactionLogs, toTypedEvent, type ParsedEvent } from "../parser/decoder.js";
import {
  handleEventAtomic,
  EventContext,
  suspendLocalDerivedDigests,
  resumeLocalDerivedDigests,
} from "../db/handlers.js";
import { trySaveLocalIndexerStateWithSql } from "../db/indexer-state-local.js";
import {
  clearIndexerStateSnapshot,
  getPool,
  loadIndexerStateSnapshot,
  restoreIndexerStateSnapshot,
  saveIndexerState,
} from "../db/supabase.js";
import { createChildLogger } from "../logger.js";
import { resolveEventBlockTime } from "./block-time.js";
import { compareCanonicalTransactionOrder } from "./canonical-order.js";
import {
  hasProgramDataLogsForProgram,
  maybeUpsertProofPassMatchesForLogs,
  recoverMissingProgramSignatures,
} from "./websocket-recovery.js";
import type { ReplayProcessedCursor } from "./poller.js";
import type { ProgramEvent } from "../parser/types.js";

const logger = createChildLogger("websocket");

// Health check interval (30 seconds)
const HEALTH_CHECK_INTERVAL = 30_000;
// Consider connection stale if no activity for 2 minutes
const STALE_THRESHOLD = 120_000;
// Keep websocket tx handling sequential so cursor advancement remains prefix-safe.
const MAX_CONCURRENT_HANDLERS = 1;
const MAX_QUEUE_SIZE = 10_000;
const RECONCILIATION_SIGNATURE_PAGE_LIMIT = 50;
const RECONCILIATION_SIGNATURE_SCAN_LIMIT = 200;
const RECONCILIATION_FAILURE_THRESHOLD = 3;
const RECONCILIATION_STATE_ROW_ID = "websocket-reconciliation-tip";
const RECONCILIATION_STATE_SOURCE = "websocket-reconciliation";
const SLOT_START_TX_INDEX = -1;

export interface WebSocketIndexerOptions {
  connection: Connection;
  prisma: PrismaClient | null;
  programId: PublicKey;
  wsUrl?: string;
  reconnectInterval?: number;
  maxRetries?: number;
  canonicalBufferMs?: number;
}

type PersistedCursorSnapshot = {
  signature: string;
  slot: bigint;
  txIndex: number | null;
  source: string | null;
  updatedAt: Date;
};

type CurrentCursorSnapshot = {
  lastSignature: string | null;
  lastSlot: bigint | null;
  lastTxIndex: number | null;
  source: string | null;
  updatedAt?: Date | null;
};

type BufferedLogNotification = {
  logs: Logs;
  ctx: Context;
  sequence: number;
};

type ResolvedLogOrder = {
  txIndex: number | undefined;
  blockTimeSeconds: number | null | undefined;
};

type ResolvedLogNotification = BufferedLogNotification & {
  order: ResolvedLogOrder;
};

type ReconciliationSignatureScan = {
  signatures: ConfirmedSignatureInfo[];
  scanComplete: boolean;
  scanLimitReached: boolean;
};

type IdentityLockEnrichment = {
  tx: ParsedTransactionWithMeta;
  events: ParsedEvent[];
};

type LogsSubscribeProbeResult = "supported" | "unsupported" | "unknown";

type ConnectionWithLogsProbeInternals = Pick<Connection, "onLogs" | "removeOnLogsListener"> & {
  _rpcWebSocketConnected?: boolean;
  _subscriptionHashByClientSubscriptionId?: Record<number, string>;
  _subscriptionsByHash?: Record<string, { state?: string; serverSubscriptionId?: number | string }>;
};

function isUnsupportedLogsSubscribeError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const maybeError = error as { code?: unknown; message?: unknown; error?: { code?: unknown; message?: unknown } };
  const nested = maybeError.error;
  const code = nested?.code ?? maybeError.code;
  const message = String(nested?.message ?? maybeError.message ?? "");
  return code === -32601 || /logsSubscribe/i.test(message) || /Method .*not found/i.test(message);
}

function logUnsupportedLogsSubscribe(wsUrl: string): void {
  logger.warn(
    { wsUrl },
    "RPC provider does not support Solana log subscriptions (logsSubscribe); websocket transport unavailable, use polling or a compatible streaming endpoint"
  );
}

function logUnavailableLogsSubscribe(wsUrl: string, result: LogsSubscribeProbeResult): void {
  logger.warn(
    { wsUrl, result },
    "RPC provider could not establish a confirmed Solana log subscription probe; websocket transport unavailable, use polling or a compatible streaming endpoint"
  );
}

async function probeRawLogsSubscribe(wsUrl: string, programId: string): Promise<LogsSubscribeProbeResult> {
  return await new Promise<LogsSubscribeProbeResult>((resolve) => {
    let settled = false;
    const ws = new WebSocket(wsUrl);
    const finish = (result: LogsSubscribeProbeResult): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      try {
        ws.close();
      } catch {
        // ignore close errors during probe cleanup
      }
      resolve(result);
    };
    const timeout = setTimeout(() => finish("unknown"), 3000);

    ws.on("open", () => {
      ws.send(JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "logsSubscribe",
        params: [{ mentions: [programId] }, { commitment: "confirmed" }],
      }));
    });

    ws.on("message", (raw) => {
      try {
        const parsed = JSON.parse(String(raw)) as { result?: unknown; error?: unknown };
        if (parsed.error) {
          finish(isUnsupportedLogsSubscribeError(parsed.error) ? "unsupported" : "unknown");
          return;
        }
        finish(parsed.result !== undefined ? "supported" : "unknown");
      } catch {
        finish("unknown");
      }
    });

    ws.on("error", () => finish("unknown"));
    ws.on("close", () => finish("unknown"));
  });
}

async function probeTemporaryLogsSubscription(
  connection: ConnectionWithLogsProbeInternals,
  programId: PublicKey
): Promise<boolean> {
  const hasTransportFlag = Object.prototype.hasOwnProperty.call(connection, "_rpcWebSocketConnected");
  const hasSubscriptionStateMaps =
    !!connection._subscriptionHashByClientSubscriptionId && !!connection._subscriptionsByHash;
  if (!hasTransportFlag || !hasSubscriptionStateMaps) {
    return false;
  }

  let logsSubscriptionId: number | null = null;
  try {
    logsSubscriptionId = connection.onLogs(programId, () => undefined, "confirmed");
    const deadline = Date.now() + 2_000;
    while (Date.now() < deadline) {
      const hash = logsSubscriptionId !== null
        ? connection._subscriptionHashByClientSubscriptionId?.[logsSubscriptionId]
        : undefined;
      const subscription = hash ? connection._subscriptionsByHash?.[hash] : undefined;
      const confirmed =
        subscription?.state === "subscribed"
        && subscription.serverSubscriptionId !== null
        && subscription.serverSubscriptionId !== undefined;
      if (confirmed) {
        return true;
      }
      if (hasTransportFlag && connection._rpcWebSocketConnected !== true) {
        await new Promise((resolve) => setTimeout(resolve, 50));
        continue;
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    return false;
  } catch (error) {
    logger.debug({ error }, "WebSocket log subscription probe failed");
    return false;
  } finally {
    if (logsSubscriptionId !== null) {
      try {
        await connection.removeOnLogsListener(logsSubscriptionId);
      } catch (error) {
        logger.debug({ error, subscriptionId: logsSubscriptionId }, "Failed to remove temporary log subscription");
      }
    }
  }
}

function shouldAdvanceCursor(
  currentSlot: bigint | null | undefined,
  currentTxIndex: number | null | undefined,
  currentSignature: string | null | undefined,
  nextSlot: bigint,
  nextTxIndex: number | null | undefined,
  nextSignature: string
): boolean {
  if (currentSlot === null || currentSlot === undefined) return true;
  if (!currentSignature) return true;
  return compareCanonicalTransactionOrder(
    { slot: currentSlot, txIndex: currentTxIndex, signature: currentSignature },
    { slot: nextSlot, txIndex: nextTxIndex, signature: nextSignature }
  ) <= 0;
}

export class WebSocketIndexer {
  private connection: Connection;
  private prisma: PrismaClient | null;
  private programId: PublicKey;
  private reconnectInterval: number;
  private maxRetries: number;
  private wsUrl: string | null;
  private subscriptionId: number | null = null;
  private isRunning = false;
  private retryCount = 0;
  private healthCheckTimer: NodeJS.Timeout | null = null;
  private lastActivityTime: number = Date.now();
  private processedCount = 0;
  private errorCount = 0;
  // Concurrency guards
  private isCheckingHealth = false;
  private isReconnecting = false;
  private overflowStopInProgress = false;
  private reconnectDelayTimer: NodeJS.Timeout | null = null;
  private reconnectDelayPromise: Promise<void> | null = null;
  private reconnectDelayResolve: (() => void) | null = null;
  private failSafeStopInProgress = false;
  private stopRequested = false;
  private freezeCursorAdvancement = false;
  private runToken = 0;
  private canonicalBufferMs: number;
  private canonicalFlushTimer: NodeJS.Timeout | null = null;
  private notificationSequence = 0;
  private pendingLogNotifications = new Map<string, BufferedLogNotification>();
  // Bounded concurrency queue to prevent OOM during high traffic
  private logQueue: PQueue;
  private droppedLogs = 0;
  private lastPersistedCursor: PersistedCursorSnapshot | null = null;
  private processingHoldCount = 0;
  private reconciliationTimer: NodeJS.Timeout | null = null;
  private isReconciling = false;
  private reconciliationTipSignature: string | null = null;
  private reconciliationFailureCount = 0;
  private stopPromise: Promise<void> | null = null;

  constructor(options: WebSocketIndexerOptions) {
    // Initialize bounded queue for log processing
    this.logQueue = new PQueue({ concurrency: MAX_CONCURRENT_HANDLERS });
    this.connection = options.connection;
    this.prisma = options.prisma;
    this.programId = options.programId;
    this.wsUrl = options.wsUrl ?? null;
    this.reconnectInterval = options.reconnectInterval || config.wsReconnectInterval;
    this.maxRetries = options.maxRetries || config.wsMaxRetries;
    this.canonicalBufferMs = Math.max(0, options.canonicalBufferMs ?? config.wsCanonicalBufferMs);
  }

  private async resolveSnapshotCursorTxIndex(
    signature: string,
    slot: bigint,
    txIndex: number | null | undefined,
    source: string | null
  ): Promise<number | null> {
    if (txIndex !== null && txIndex !== undefined) {
      return txIndex;
    }

    try {
      const block = await this.connection.getBlock(Number(slot), {
        maxSupportedTransactionVersion: config.maxSupportedTransactionVersion,
        transactionDetails: "full",
      });
      const resolvedTxIndex = block?.transactions?.findIndex(
        (tx) => tx.transaction.signatures[0] === signature
      ) ?? -1;
      if (resolvedTxIndex >= 0) {
        logger.info(
          {
            signature,
            slot: slot.toString(),
            txIndex: resolvedTxIndex,
            source,
          },
          "Resolved legacy websocket cursor tx_index from block"
        );
        return resolvedTxIndex;
      }
    } catch (error) {
      logger.warn(
        {
          signature,
          slot: slot.toString(),
          source,
          error: error instanceof Error ? error.message : String(error),
        },
        "Failed to resolve legacy websocket cursor tx_index from block"
      );
    }

    logger.warn(
      {
        signature,
        slot: slot.toString(),
        source,
        txIndex: SLOT_START_TX_INDEX,
      },
      "Treating legacy websocket cursor as slot-start"
    );
    return SLOT_START_TX_INDEX;
  }

  async start(): Promise<void> {
    if (this.isRunning) {
      logger.warn("WebSocket indexer already running");
      return;
    }

    this.isRunning = true;
    this.stopRequested = false;
    this.freezeCursorAdvancement = false;
    this.retryCount = 0;
    this.runToken += 1;
    this.lastPersistedCursor = null;
    this.clearCanonicalFlushTimer();
    this.pendingLogNotifications.clear();
    this.notificationSequence = 0;
    this.reconciliationTipSignature = null;
    this.reconciliationFailureCount = 0;
    this.stopReconciliationCheck();
    this.logQueue.start();
    this.lastActivityTime = Date.now();
    if (this.prisma) {
      try {
        const current = await this.prisma.indexerState.findUnique({
          where: { id: "main" },
          select: {
            lastSignature: true,
            lastSlot: true,
            lastTxIndex: true,
            source: true,
            updatedAt: true,
          },
        });
        if (current?.lastSignature && current.lastSlot !== null) {
          const slot = typeof current.lastSlot === "bigint" ? current.lastSlot : BigInt(current.lastSlot);
          const txIndex = await this.resolveSnapshotCursorTxIndex(
            current.lastSignature,
            slot,
            current.lastTxIndex,
            current.source ?? null
          );
          this.lastPersistedCursor = {
            signature: current.lastSignature,
            slot,
            txIndex,
            source: current.source ?? null,
            updatedAt: current.updatedAt ?? new Date(),
          };
        }
      } catch (error) {
        logger.warn({ error }, "Failed to snapshot current cursor before starting websocket indexer");
      }
    } else {
      try {
        const current = await loadIndexerStateSnapshot();
        if (current?.lastSignature && current.lastSlot !== null) {
          const txIndex = await this.resolveSnapshotCursorTxIndex(
            current.lastSignature,
            current.lastSlot,
            current.lastTxIndex,
            current.source
          );
          this.lastPersistedCursor = {
            signature: current.lastSignature,
            slot: current.lastSlot,
            txIndex,
            source: current.source ?? null,
            updatedAt: current.updatedAt,
          };
        }
      } catch (error) {
        logger.warn({ error }, "Failed to snapshot current Supabase cursor before starting websocket indexer");
      }
    }
    logger.info({ programId: this.programId.toBase58() }, "Starting WebSocket indexer");

    await this.initializeReconciliationTip();

    if (this.wsUrl) {
      const logsSupport = await probeRawLogsSubscribe(this.wsUrl, this.programId.toBase58());
      if (logsSupport !== "supported") {
        if (logsSupport === "unsupported") {
          logUnsupportedLogsSubscribe(this.wsUrl);
        } else {
          logUnavailableLogsSubscribe(this.wsUrl, logsSupport);
        }
        this.isRunning = false;
        return;
      }
    }

    await this.subscribe();
    if (!this.isRunning || this.subscriptionId === null) {
      return;
    }

    this.startHealthCheck();
    this.startReconciliationCheck();
  }

  holdProcessing(): void {
    this.processingHoldCount++;
    this.clearCanonicalFlushTimer();
  }

  releaseProcessing(): void {
    if (this.processingHoldCount > 0) {
      this.processingHoldCount--;
    }
    if (this.processingHoldCount === 0) {
      this.flushPendingLogNotifications();
    }
  }

  stop(): Promise<void> {
    if (this.stopPromise) {
      return this.stopPromise;
    }
    this.stopPromise = this.stopInternal().finally(() => {
      this.stopPromise = null;
    });
    return this.stopPromise;
  }

  private async stopInternal(): Promise<void> {
    logger.info({
      processedCount: this.processedCount,
      errorCount: this.errorCount,
      queueSize: this.logQueue.size + this.pendingLogNotifications.size,
      droppedLogs: this.droppedLogs
    }, "Stopping WebSocket indexer");

    this.isRunning = false;
    this.stopRequested = true;
    this.freezeCursorAdvancement = true;
    this.processingHoldCount = 0;
    this.retryCount = 0;
    this.stopHealthCheck();
    this.stopReconciliationCheck();
    this.clearReconnectDelay();

    this.clearCanonicalFlushTimer();
    const clearedBufferedLogs = this.pendingLogNotifications.size;
    if (clearedBufferedLogs > 0) {
      this.pendingLogNotifications.clear();
    }

    this.logQueue.pause();
    const clearedQueuedLogs = this.logQueue.size + clearedBufferedLogs;
    const hadInFlightWork = this.logQueue.pending > 0;
    const restoreCursorSnapshot = this.lastPersistedCursor
      ? { ...this.lastPersistedCursor }
      : null;
    if (clearedQueuedLogs > 0) {
      this.logQueue.clear();
      this.droppedLogs += clearedQueuedLogs;
      logger.info({ clearedQueuedLogs, droppedLogs: this.droppedLogs }, "Cleared queued WebSocket logs during shutdown");
    }

    if (this.subscriptionId !== null) {
      try {
        await this.connection.removeOnLogsListener(this.subscriptionId);
        logger.info({ subscriptionId: this.subscriptionId }, "Removed WebSocket subscription");
      } catch (error) {
        logger.warn({ error, subscriptionId: this.subscriptionId }, "Error removing subscription");
      }
      this.subscriptionId = null;
    }

    // Drain remaining queue items before shutdown.
    const queueDepth = this.logQueue.size + this.logQueue.pending;
    if (queueDepth > 0) {
      logger.info({ queueSize: this.logQueue.size, pending: this.logQueue.pending }, "Draining log queue before shutdown");
      await this.logQueue.onIdle();
    }

    if (restoreCursorSnapshot && (clearedQueuedLogs > 0 || hadInFlightWork)) {
      try {
        await this.restoreSafeCursorSnapshotAfterStop(restoreCursorSnapshot);
      } catch (error) {
        logger.warn({ error }, "Failed to restore safe cursor snapshot after websocket shutdown");
      }
    } else if (clearedQueuedLogs > 0 || hadInFlightWork) {
      const clearedAt = new Date();
      try {
        await this.clearSafeCursorSnapshotAfterStop(clearedAt);
      } catch (error) {
        logger.warn({ error }, "Failed to clear websocket cursor after shutdown without a safe snapshot");
      }
    }
  }

  private currentCursorIsOwnedByWebSocket(current: CurrentCursorSnapshot | null): boolean {
    return !current?.source || current.source === "websocket";
  }

  private currentCursorIsNewerThanSnapshot(
    current: CurrentCursorSnapshot | null,
    snapshot: PersistedCursorSnapshot
  ): boolean {
    if (!current?.lastSignature || current.lastSlot === null || current.lastSlot === undefined) {
      return false;
    }
    return compareCanonicalTransactionOrder(
      { slot: current.lastSlot, txIndex: current.lastTxIndex, signature: current.lastSignature },
      { slot: snapshot.slot, txIndex: snapshot.txIndex, signature: snapshot.signature }
    ) > 0;
  }

  private async restoreSafeCursorSnapshotAfterStop(
    snapshot: PersistedCursorSnapshot
  ): Promise<void> {
    const current = await this.loadCurrentCursorSnapshot();
    if (
      !this.currentCursorIsOwnedByWebSocket(current)
      || this.currentCursorIsNewerThanSnapshot(current, snapshot)
    ) {
      logger.info(
        {
          signature: snapshot.signature,
          slot: snapshot.slot.toString(),
          txIndex: snapshot.txIndex,
          currentSource: current?.source ?? null,
          currentSignature: current?.lastSignature ?? null,
          currentSlot: current?.lastSlot?.toString() ?? null,
          currentTxIndex: current?.lastTxIndex ?? null,
        },
        "Skipped websocket cursor snapshot restore after shutdown"
      );
      return;
    }

    if (this.prisma) {
      await this.prisma.indexerState.upsert({
        where: { id: "main" },
        create: {
          id: "main",
          lastSignature: snapshot.signature,
          lastSlot: snapshot.slot,
          lastTxIndex: snapshot.txIndex,
          source: snapshot.source ?? "websocket",
          updatedAt: snapshot.updatedAt,
        },
        update: {
          lastSignature: snapshot.signature,
          lastSlot: snapshot.slot,
          lastTxIndex: snapshot.txIndex,
          source: snapshot.source ?? "websocket",
          updatedAt: snapshot.updatedAt,
        },
      });
    } else {
      await restoreIndexerStateSnapshot(
        snapshot.signature,
        snapshot.slot,
        snapshot.txIndex,
        (snapshot.source as "poller" | "websocket" | "substreams" | null) ?? "websocket",
        snapshot.updatedAt,
      );
    }
    logger.info(
      {
        signature: snapshot.signature,
        slot: snapshot.slot.toString(),
        txIndex: snapshot.txIndex,
      },
      "Restored safe cursor snapshot after websocket shutdown"
    );
  }

  private async clearSafeCursorSnapshotAfterStop(clearedAt: Date): Promise<void> {
    const current = await this.loadCurrentCursorSnapshot();
    if (!this.currentCursorIsOwnedByWebSocket(current) || (current?.lastSignature && current.lastSlot !== null)) {
      logger.info(
        {
          currentSource: current?.source ?? null,
          currentSignature: current?.lastSignature ?? null,
          currentSlot: current?.lastSlot?.toString() ?? null,
          currentTxIndex: current?.lastTxIndex ?? null,
        },
        "Skipped websocket cursor clear after shutdown"
      );
      return;
    }

    if (this.prisma) {
      await this.prisma.indexerState.upsert({
        where: { id: "main" },
        create: {
          id: "main",
          lastSignature: null,
          lastSlot: null,
          lastTxIndex: null,
          source: "websocket",
          updatedAt: clearedAt,
        },
        update: {
          lastSignature: null,
          lastSlot: null,
          lastTxIndex: null,
          source: "websocket",
          updatedAt: clearedAt,
        },
      });
    } else {
      await clearIndexerStateSnapshot("websocket", clearedAt);
    }
    logger.info("Cleared websocket cursor after shutdown because no safe snapshot existed");
  }

  private rememberPersistedCursor(
    signature: string,
    slot: bigint,
    txIndex: number | null,
    source: string,
    updatedAt: Date,
  ): void {
    if (this.stopRequested || this.freezeCursorAdvancement || !this.isRunning) {
      return;
    }
    this.lastPersistedCursor = {
      signature,
      slot,
      txIndex,
      source,
      updatedAt,
    };
  }

  private canAdvanceMainCursor(
    currentSignature: string | null | undefined,
    currentSlot: bigint | number | null | undefined,
    currentTxIndex: number | null | undefined,
    next: ReplayProcessedCursor
  ): boolean {
    if (currentSlot === null || currentSlot === undefined) return true;
    if (!currentSignature) return true;
    const slot = typeof currentSlot === "bigint" ? currentSlot : BigInt(currentSlot);
    return compareCanonicalTransactionOrder(
      { slot, txIndex: currentTxIndex ?? null, signature: currentSignature },
      { slot: next.slot, txIndex: next.txIndex, signature: next.signature }
    ) <= 0;
  }

  private async saveReconciledMainCursor(cursor: ReplayProcessedCursor): Promise<boolean> {
    if (!this.isRunning || this.stopRequested || this.freezeCursorAdvancement) {
      return false;
    }

    if (this.prisma) {
      const current = await this.prisma.indexerState.findUnique({
        where: { id: "main" },
        select: { lastSignature: true, lastSlot: true, lastTxIndex: true },
      });
      if (!this.canAdvanceMainCursor(current?.lastSignature, current?.lastSlot, current?.lastTxIndex ?? null, cursor)) {
        return false;
      }
      if (!(await trySaveLocalIndexerStateWithSql(this.prisma, {
        signature: cursor.signature,
        slot: cursor.slot,
        txIndex: cursor.txIndex,
        source: RECONCILIATION_STATE_SOURCE,
        updatedAt: cursor.blockTime,
      }))) {
        await this.prisma.indexerState.upsert({
          where: { id: "main" },
          create: {
            id: "main",
            lastSignature: cursor.signature,
            lastSlot: cursor.slot,
            lastTxIndex: cursor.txIndex,
            source: RECONCILIATION_STATE_SOURCE,
            updatedAt: cursor.blockTime,
          },
          update: {
            lastSignature: cursor.signature,
            lastSlot: cursor.slot,
            lastTxIndex: cursor.txIndex,
            source: RECONCILIATION_STATE_SOURCE,
            updatedAt: cursor.blockTime,
          },
        });
      }
    } else {
      const current = await loadIndexerStateSnapshot();
      if (!this.canAdvanceMainCursor(current?.lastSignature, current?.lastSlot, current?.lastTxIndex ?? null, cursor)) {
        return false;
      }
      await saveIndexerState(
        cursor.signature,
        cursor.slot,
        cursor.txIndex,
        RECONCILIATION_STATE_SOURCE,
        cursor.blockTime
      );
    }

    if (this.canAdvanceMainCursor(
      this.lastPersistedCursor?.signature,
      this.lastPersistedCursor?.slot,
      this.lastPersistedCursor?.txIndex ?? null,
      cursor
    )) {
      this.rememberPersistedCursor(
        cursor.signature,
        cursor.slot,
        cursor.txIndex,
        RECONCILIATION_STATE_SOURCE,
        cursor.blockTime
      );
    }
    return true;
  }

  private startHealthCheck(): void {
    this.stopHealthCheck();

    this.healthCheckTimer = setInterval(() => {
      void this.checkHealth().catch((error) => {
        logger.error({ error }, "Unhandled WebSocket health check error");
      });
    }, HEALTH_CHECK_INTERVAL);

    logger.debug("Health check timer started");
  }

  private stopHealthCheck(): void {
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = null;
      logger.debug("Health check timer stopped");
    }
  }

  private startReconciliationCheck(): void {
    this.stopReconciliationCheck();
    this.reconciliationTimer = setInterval(() => {
      void this.runReconciliationCheck("periodic").catch((error) => {
        logger.error({ error }, "Unhandled WebSocket reconciliation check error");
      });
    }, config.wsReconciliationIntervalMs);
  }

  private stopReconciliationCheck(): void {
    if (this.reconciliationTimer) {
      clearInterval(this.reconciliationTimer);
      this.reconciliationTimer = null;
    }
  }

  private clearReconnectDelay(): void {
    if (this.reconnectDelayTimer) {
      clearTimeout(this.reconnectDelayTimer);
      this.reconnectDelayTimer = null;
    }

    const resolve = this.reconnectDelayResolve;
    this.reconnectDelayResolve = null;
    this.reconnectDelayPromise = null;
    resolve?.();
  }

  private clearCanonicalFlushTimer(): void {
    if (this.canonicalFlushTimer) {
      clearTimeout(this.canonicalFlushTimer);
      this.canonicalFlushTimer = null;
    }
  }

  private async loadReconciliationTipSignature(): Promise<string | null> {
    if (this.prisma) {
      const rows = await this.prisma.indexerState.findMany({
        where: { id: RECONCILIATION_STATE_ROW_ID },
        select: { lastSignature: true },
        take: 1,
      });
      return rows[0]?.lastSignature ?? null;
    }

    const db = getPool();
    const result = await db.query(
      `SELECT last_signature
       FROM indexer_state
       WHERE id = $1
       LIMIT 1`,
      [RECONCILIATION_STATE_ROW_ID]
    );
    return result.rows[0]?.last_signature ?? null;
  }

  private async saveReconciliationTipSignature(signature: string): Promise<void> {
    if (this.prisma) {
      await this.prisma.indexerState.upsert({
        where: { id: RECONCILIATION_STATE_ROW_ID },
        create: {
          id: RECONCILIATION_STATE_ROW_ID,
          lastSignature: signature,
          lastSlot: null,
          lastTxIndex: null,
          source: RECONCILIATION_STATE_SOURCE,
        },
        update: {
          lastSignature: signature,
          lastSlot: null,
          lastTxIndex: null,
          source: RECONCILIATION_STATE_SOURCE,
        },
      });
      return;
    }

    const db = getPool();
    await db.query(
      `INSERT INTO indexer_state (id, last_signature, last_slot, last_tx_index, source, updated_at)
       VALUES ($1, $2, NULL, NULL, $3, $4)
       ON CONFLICT (id) DO UPDATE SET
         last_signature = EXCLUDED.last_signature,
         last_slot = EXCLUDED.last_slot,
         last_tx_index = EXCLUDED.last_tx_index,
         source = EXCLUDED.source,
         updated_at = EXCLUDED.updated_at`,
      [
        RECONCILIATION_STATE_ROW_ID,
        signature,
        RECONCILIATION_STATE_SOURCE,
        new Date().toISOString(),
      ]
    );
  }

  private async initializeReconciliationTip(): Promise<void> {
    const cursor = this.lastPersistedCursor;
    if (!cursor?.signature) {
      return;
    }

    try {
      const storedTip = await this.loadReconciliationTipSignature();
      const canReuseStoredTip = Boolean(storedTip);
      const tip = canReuseStoredTip ? storedTip! : cursor.signature;
      this.reconciliationTipSignature = tip;
      if (!canReuseStoredTip) {
        await this.saveReconciliationTipSignature(tip);
      }
    } catch (error) {
      this.reconciliationTipSignature = cursor.signature;
      logger.warn(
        {
          error: error instanceof Error ? error.message : String(error),
          signature: cursor.signature,
        },
        "Failed to initialize durable WebSocket reconciliation tip"
      );
    }
  }

  private async persistReconciliationTip(signature: string): Promise<void> {
    try {
      await this.saveReconciliationTipSignature(signature);
      this.reconciliationTipSignature = signature;
    } catch (error) {
      logger.warn(
        {
          error: error instanceof Error ? error.message : String(error),
          signature,
        },
        "Failed to persist WebSocket reconciliation tip"
      );
    }
  }

  private async fetchReconciliationSignatures(until: string): Promise<ReconciliationSignatureScan> {
    const signatures: ConfirmedSignatureInfo[] = [];
    let before: string | undefined;
    let scanComplete = false;

    while (signatures.length < RECONCILIATION_SIGNATURE_SCAN_LIMIT) {
      const limit = Math.min(
        RECONCILIATION_SIGNATURE_PAGE_LIMIT,
        RECONCILIATION_SIGNATURE_SCAN_LIMIT - signatures.length
      );
      const options: { until: string; before?: string; limit: number } = { until, limit };
      if (before) {
        options.before = before;
      }

      const batch = await this.connection.getSignaturesForAddress(this.programId, options);
      if (batch.length === 0) {
        scanComplete = true;
        break;
      }

      signatures.push(...batch);
      if (batch.length < limit || batch.some((signature) => signature.signature === until)) {
        scanComplete = true;
        break;
      }

      before = batch[batch.length - 1]?.signature;
      if (!before) {
        break;
      }
    }

    return {
      signatures,
      scanComplete,
      scanLimitReached: signatures.length >= RECONCILIATION_SIGNATURE_SCAN_LIMIT && !scanComplete,
    };
  }

  private async runReconciliationCheck(reason: "periodic" | "stale-health"): Promise<boolean> {
    if (!this.isRunning || this.stopRequested || this.isReconciling) {
      return false;
    }
    const cursor = this.lastPersistedCursor;
    if (!cursor?.signature) {
      return false;
    }

    this.isReconciling = true;
    try {
      const previousTip = this.reconciliationTipSignature ?? cursor.signature;
      let scan: ReconciliationSignatureScan;
      try {
        scan = await this.fetchReconciliationSignatures(previousTip);
        this.reconciliationFailureCount = 0;
      } catch (error) {
        this.reconciliationFailureCount++;
        if (
          this.reconciliationFailureCount >= RECONCILIATION_FAILURE_THRESHOLD
          && this.isRunning
          && !this.stopRequested
        ) {
          this.errorCount++;
          this.scheduleFailSafeStop(
            "WebSocket reconciliation failed repeatedly; entering fail-safe stop so polling catch-up can verify continuity",
            {
              reason,
              error: error instanceof Error ? error.message : String(error),
              reconciliationFailureCount: this.reconciliationFailureCount,
              errorCount: this.errorCount,
            }
          );
          return true;
        }

        logger.warn(
          {
            reason,
            error: error instanceof Error ? error.message : String(error),
            reconciliationFailureCount: this.reconciliationFailureCount,
          },
          "WebSocket reconciliation check failed"
        );
        return false;
      }
      if (!this.isRunning || this.stopRequested) {
        return false;
      }

      if (scan.scanLimitReached) {
        this.errorCount++;
        this.scheduleFailSafeStop(
          "WebSocket reconciliation scan reached its signature limit before the previous tip; entering fail-safe stop so polling catch-up can verify continuity",
          {
            reason,
            previousTip,
            scannedSignatures: scan.signatures.length,
            lastScannedSignature: scan.signatures[scan.signatures.length - 1]?.signature ?? null,
            cursorSignature: cursor.signature,
            cursorSlot: cursor.slot.toString(),
            cursorTxIndex: cursor.txIndex,
            errorCount: this.errorCount,
          }
        );
        return true;
      }

      const replayWindow = scan.signatures.filter((signature) => signature.err === null);
      if (replayWindow.length > 0) {
        try {
          const replayResult = await recoverMissingProgramSignatures({
            connection: this.connection,
            prisma: this.prisma,
            programId: this.programId,
            signatures: replayWindow,
            isActive: () => this.isRunning && !this.stopRequested,
          });
          this.processedCount += replayResult.processed;
          const advancedCursor = scan.scanComplete && replayResult.newestCursor
            ? await this.saveReconciledMainCursor(replayResult.newestCursor)
            : false;
          logger.warn(
            {
              reason,
              replayed: replayResult.processed,
              advancedCursor,
              cursorSignature: replayResult.newestCursor?.signature ?? null,
              cursorSlot: replayResult.newestCursor?.slot.toString() ?? null,
              cursorTxIndex: replayResult.newestCursor?.txIndex ?? null,
              signatures: replayWindow.map((signature) => signature.signature),
              errorCount: this.errorCount,
            },
            "WebSocket reconciliation replayed signatures through poller pipeline"
          );
        } catch (error) {
          this.errorCount++;
          this.scheduleFailSafeStop(
            "WebSocket reconciliation replay failed; entering fail-safe stop so polling catch-up can retry",
            {
              reason,
              signatures: replayWindow.map((signature) => signature.signature),
              error: error instanceof Error ? error.message : String(error),
              errorCount: this.errorCount,
            }
          );
          return true;
        }
      }

      if (scan.scanComplete && scan.signatures[0]?.signature) {
        await this.persistReconciliationTip(scan.signatures[0].signature);
      }
      return false;
    } finally {
      this.isReconciling = false;
    }
  }

  private async waitForTransactionRetryDelay(ms: number): Promise<boolean> {
    if (!this.isRunning || this.stopRequested || ms <= 0) {
      return false;
    }
    return await new Promise<boolean>((resolve) => {
      setTimeout(() => resolve(this.isRunning && !this.stopRequested), ms);
    });
  }

  private async fetchParsedTransactionForIdentityLocks(
    signature: string,
    slot: number
  ): Promise<IdentityLockEnrichment | null> {
    const maxRetries = 5;
    let lastError: unknown = null;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const tx = await this.connection.getParsedTransaction(signature, {
          maxSupportedTransactionVersion: config.maxSupportedTransactionVersion,
        });
        if (tx) {
          const parsed = parseTransaction(tx);
          if (parsed?.events.length) {
            return { tx, events: parsed.events };
          }
          lastError = new Error("parsed transaction contained no events");
        }
      } catch (error) {
        lastError = error;
      }

      if (attempt >= maxRetries) {
        break;
      }
      logger.warn(
        {
          signature,
          slot,
          attempt,
          maxRetries,
          error: lastError instanceof Error ? lastError.message : lastError === null ? "transaction unavailable" : String(lastError),
        },
        "Parsed transaction unavailable for identity lock enrichment; retrying"
      );
      const retryDelayMs = Math.min(5000, 500 * (2 ** (attempt - 1)));
      if (!(await this.waitForTransactionRetryDelay(retryDelayMs))) {
        return null;
      }
    }

    const reason = lastError instanceof Error
      ? lastError.message
      : lastError === null
        ? "transaction unavailable"
        : String(lastError);
    throw new Error(`Parsed transaction unavailable for identity lock enrichment: ${reason}`);
  }

  private waitForReconnectDelay(): Promise<void> {
    if (this.reconnectDelayPromise) {
      return this.reconnectDelayPromise;
    }

    this.reconnectDelayPromise = new Promise<void>((resolve) => {
      let settled = false;
      const finish = () => {
        if (settled) return;
        settled = true;
        if (this.reconnectDelayTimer) {
          clearTimeout(this.reconnectDelayTimer);
          this.reconnectDelayTimer = null;
        }
        this.reconnectDelayResolve = null;
        this.reconnectDelayPromise = null;
        resolve();
      };

      this.reconnectDelayResolve = finish;
      this.reconnectDelayTimer = setTimeout(finish, this.reconnectInterval);
    });

    return this.reconnectDelayPromise;
  }

  private async checkHealth(): Promise<void> {
    if (!this.isRunning) return;
    const runToken = this.runToken;
    const observedSubscriptionId = this.subscriptionId;

    // Reentrancy guard - prevent overlapping health checks
    if (this.isCheckingHealth) {
      logger.debug("Health check already in progress, skipping");
      return;
    }

    this.isCheckingHealth = true;
    try {
      const timeSinceActivity = Date.now() - this.lastActivityTime;

      logger.debug({
        timeSinceActivity,
        subscriptionId: this.subscriptionId,
        processedCount: this.processedCount,
        errorCount: this.errorCount
      }, "Health check");

      // Check if connection is stale (no activity for too long)
      if (timeSinceActivity > STALE_THRESHOLD) {
        const wsHealthy = await probeTemporaryLogsSubscription(this.connection, this.programId);
        if (!this.isRunning || this.runToken !== runToken) {
          return;
        }
        if (wsHealthy) {
          const failSafeTriggered = await this.runReconciliationCheck("stale-health");
          if (!this.isRunning || this.runToken !== runToken || failSafeTriggered) {
            return;
          }
          logger.info({
            timeSinceActivity,
          }, "No WebSocket events but onLogs heartbeat probe is healthy after reconciliation - program may have low activity");
          this.lastActivityTime = Date.now();
          return;
        }

        logger.warn({
          timeSinceActivity,
          threshold: STALE_THRESHOLD,
        }, "WebSocket stale and onLogs heartbeat probe failed, reconnecting...");
        await this.forceReconnect(observedSubscriptionId);
        return;
      }

      // Regular connectivity check (not stale, just verify RPC is up)
      try {
        const slot = await this.connection.getSlot();
        if (!this.isRunning || this.runToken !== runToken) {
          return;
        }
        logger.debug({ slot }, "HTTP connectivity OK");
      } catch (error) {
        if (!this.isRunning || this.runToken !== runToken) {
          return;
        }
        logger.error({ error }, "Health check failed - connection error");
        await this.forceReconnect(observedSubscriptionId);
      }
    } finally {
      this.isCheckingHealth = false;
    }
  }

  private async forceReconnect(expectedSubscriptionId: number | null = this.subscriptionId): Promise<void> {
    const runToken = this.runToken;
    if (this.isReconnecting) {
      logger.debug("WebSocket recovery already in progress, skipping");
      return;
    }

    this.isReconnecting = true;
    try {
      if (!this.isRunning || this.runToken !== runToken) {
        return;
      }
      if (expectedSubscriptionId !== null && this.subscriptionId !== expectedSubscriptionId) {
        logger.debug(
          { expectedSubscriptionId, currentSubscriptionId: this.subscriptionId },
          "Skipping stale WebSocket recovery request"
        );
        return;
      }

      this.freezeCursorAdvancement = true;
      logger.warn(
        { subscriptionId: this.subscriptionId },
        "WebSocket transport needs recovery; stopping live subscription so processor catch-up can close any missed-log gap before resubscribe"
      );
      await this.stop();
    } finally {
      this.isReconnecting = false;
    }
  }

  private async subscribe(): Promise<void> {
    try {
      logger.info("Subscribing to program logs...");

      this.subscriptionId = this.connection.onLogs(
        this.programId,
        // Queue-based processing with backpressure to prevent OOM
        (logs: Logs, ctx: Context) => {
          if (!this.isRunning) {
            return;
          }

          this.lastActivityTime = Date.now();

          if (this.logQueue.size + this.pendingLogNotifications.size >= MAX_QUEUE_SIZE) {
            this.errorCount++;
            this.droppedLogs++;

            if (this.overflowStopInProgress) {
              return;
            }
            this.overflowStopInProgress = true;

            logger.error(
              {
                queueSize: this.logQueue.size,
                signature: logs.signature,
                errorCount: this.errorCount,
                droppedLogs: this.droppedLogs,
              },
              "WebSocket processing queue is full, entering fail-safe stop so poller catch-up can recover missed events"
            );
            void this.stop().finally(() => {
              this.overflowStopInProgress = false;
            });
            return;
          }

          this.enqueueLogNotification(logs, ctx);
        },
        "confirmed"
      );

      logger.info(
        { subscriptionId: this.subscriptionId },
        "WebSocket subscription active"
      );

      this.retryCount = 0;
      this.errorCount = 0;
    } catch (error) {
      if (this.wsUrl && isUnsupportedLogsSubscribeError(error)) {
        logUnsupportedLogsSubscribe(this.wsUrl);
        this.isRunning = false;
        this.stopHealthCheck();
        return;
      }
      logger.error({ error }, "Failed to subscribe to logs");
      await this.reconnect(this.runToken);
    }
  }

  private scheduleFailSafeStop(reason: string, details: Record<string, unknown>): void {
    if (this.failSafeStopInProgress) {
      return;
    }
    this.failSafeStopInProgress = true;
    this.freezeCursorAdvancement = true;
    logger.error(details, reason);
    void this.stop().finally(() => {
      this.failSafeStopInProgress = false;
    });
  }

  private enqueueLogNotification(logs: Logs, ctx: Context): void {
    if (!this.isRunning || this.stopRequested) {
      return;
    }

    const existing = this.pendingLogNotifications.get(logs.signature);
    this.pendingLogNotifications.set(logs.signature, {
      logs,
      ctx,
      sequence: existing?.sequence ?? ++this.notificationSequence,
    });

    if (this.canonicalBufferMs === 0) {
      this.flushPendingLogNotifications();
      return;
    }

    if (!this.canonicalFlushTimer) {
      this.canonicalFlushTimer = setTimeout(() => {
        this.canonicalFlushTimer = null;
        this.flushPendingLogNotifications();
      }, this.canonicalBufferMs);
    }
  }

  private flushPendingLogNotifications(): void {
    if (
      this.pendingLogNotifications.size === 0 ||
      !this.isRunning ||
      this.stopRequested ||
      this.processingHoldCount > 0
    ) {
      return;
    }

    this.clearCanonicalFlushTimer();
    const batch = [...this.pendingLogNotifications.values()];
    this.pendingLogNotifications.clear();

    this.logQueue.add(async () => {
      try {
        await this.processLogNotificationBatch(batch);
      } catch (error) {
        this.errorCount++;
        logger.error({
          error: error instanceof Error ? error.message : String(error),
          errorCount: this.errorCount
        }, "Error in WebSocket log batch - caught by queue");

        this.scheduleFailSafeStop(
          "WebSocket log processing failed; entering fail-safe stop so catch-up can replay from the last safe cursor",
          {
            error: error instanceof Error ? error.message : String(error),
            errorCount: this.errorCount,
          }
        );
      }
    });
  }

  private async resolveLogOrder(logs: Logs, ctx: Context): Promise<ResolvedLogOrder> {
    try {
      const block = await this.connection.getBlock(ctx.slot, {
        maxSupportedTransactionVersion: config.maxSupportedTransactionVersion,
        transactionDetails: "full",
      });
      if (block?.transactions) {
        const idx = block.transactions.findIndex(
          (tx) => tx.transaction.signatures[0] === logs.signature,
        );
        return {
          txIndex: idx >= 0 ? idx : undefined,
          blockTimeSeconds: block.blockTime,
        };
      }
      return {
        txIndex: undefined,
        blockTimeSeconds: block?.blockTime,
      };
    } catch (blockError) {
      logger.debug(
        { slot: ctx.slot, error: blockError instanceof Error ? blockError.message : String(blockError) },
        "Failed to resolve tx_index from block (non-fatal)",
      );
      return { txIndex: undefined, blockTimeSeconds: undefined };
    }
  }

  private async processLogNotificationBatch(batch: BufferedLogNotification[]): Promise<void> {
    const resolved: ResolvedLogNotification[] = [];
    for (const entry of batch) {
      if (!this.isRunning || this.stopRequested) {
        return;
      }
      resolved.push({
        ...entry,
        order: await this.resolveLogOrder(entry.logs, entry.ctx),
      });
    }

    const unresolved = resolved.find((entry) => entry.order.txIndex === undefined);
    if (unresolved) {
      this.errorCount++;
      this.scheduleFailSafeStop(
        "Deterministic tx_index unavailable in websocket batch; entering fail-safe stop before processing partial canonical order",
        {
          slot: unresolved.ctx.slot,
          signature: unresolved.logs.signature,
          errorCount: this.errorCount,
        }
      );
      return;
    }

    resolved.sort((a, b) => {
      const order = compareCanonicalTransactionOrder(
        { slot: a.ctx.slot, txIndex: a.order.txIndex, signature: a.logs.signature },
        { slot: b.ctx.slot, txIndex: b.order.txIndex, signature: b.logs.signature }
      );
      return order !== 0 ? order : a.sequence - b.sequence;
    });

    for (const entry of resolved) {
      if (!this.isRunning || this.stopRequested) {
        return;
      }
      try {
        await this.handleLogs(entry.logs, entry.ctx, entry.order);
      } catch (error) {
        this.errorCount++;
        logger.error({
          error: error instanceof Error ? error.message : String(error),
          signature: entry.logs.signature,
          errorCount: this.errorCount
        }, "Error in handleLogs - caught by queue");

        this.scheduleFailSafeStop(
          "WebSocket log processing failed; entering fail-safe stop so catch-up can replay from the last safe cursor",
          {
            signature: entry.logs.signature,
            error: error instanceof Error ? error.message : String(error),
            errorCount: this.errorCount,
          }
        );
        return;
      }
    }
  }

  private async loadCurrentCursorSnapshot(): Promise<CurrentCursorSnapshot | null> {
    if (this.prisma) {
      const current = await this.prisma.indexerState.findUnique({
        where: { id: "main" },
        select: {
          lastSignature: true,
          lastSlot: true,
          lastTxIndex: true,
          source: true,
          updatedAt: true,
        },
      });
      if (!current) {
        return null;
      }
      return {
        lastSignature: current.lastSignature ?? null,
        lastSlot: current.lastSlot === null || current.lastSlot === undefined
          ? null
          : BigInt(current.lastSlot),
        lastTxIndex: current.lastTxIndex ?? null,
        source: current.source ?? null,
        updatedAt: current.updatedAt ?? null,
      };
    }

    const current = await loadIndexerStateSnapshot();
    if (!current) {
      return null;
    }
    return {
      lastSignature: current.lastSignature,
      lastSlot: current.lastSlot,
      lastTxIndex: current.lastTxIndex,
      source: current.source,
      updatedAt: current.updatedAt,
    };
  }

  private async handleLogs(
    logs: Logs,
    ctx: Context,
    resolvedOrder?: ResolvedLogOrder
  ): Promise<void> {
    if (logs.err) {
      logger.debug({ signature: logs.signature }, "Transaction failed, skipping");
      return;
    }
    if (!this.isRunning || this.stopRequested) {
      return;
    }

    const startTime = Date.now();

    try {
      const logEvents = parseTransactionLogs(logs.logs);
      let events = logEvents;
      let parsedTx: ParsedTransactionWithMeta | null = null;
      const shouldEnrichIdentityLocks = logEvents.some(
        (event) =>
          (event.name === "CollectionPointerSet" || event.name === "ParentAssetSet") &&
          typeof event.data.lock !== "boolean"
      );

      if (shouldEnrichIdentityLocks) {
        try {
          const enrichment = await this.fetchParsedTransactionForIdentityLocks(logs.signature, ctx.slot);
          if (!this.isRunning || this.stopRequested) return;

          if (enrichment) {
            parsedTx = enrichment.tx;
            events = enrichment.events;
          }
        } catch (txFetchError) {
          this.errorCount++;
          this.scheduleFailSafeStop(
            "WebSocket could not enrich identity lock fields; entering fail-safe stop so polling catch-up can replay with full transaction data",
            {
              signature: logs.signature,
              slot: ctx.slot,
              error: txFetchError instanceof Error ? txFetchError.message : String(txFetchError),
              errorCount: this.errorCount,
            },
          );
          return;
        }
      }

      if (events.length === 0) {
        if (hasProgramDataLogsForProgram(logs.logs, this.programId.toBase58())) {
          this.errorCount++;
          this.scheduleFailSafeStop(
            "WebSocket received program data logs but decoded no events; entering fail-safe stop so polling catch-up can retry with full transaction data",
            {
              signature: logs.signature,
              slot: ctx.slot,
              errorCount: this.errorCount,
            }
          );
        }
        return;
      }

      logger.debug(
        { signature: logs.signature, eventCount: events.length, slot: ctx.slot },
        "Received logs"
      );

      // Prefer confirmed blockTime from parsed transaction when available.
      let blockTimeSeconds: number | null | undefined = parsedTx?.blockTime;

      // Resolve tx_index from block for metadata/tie-break parity with other indexers
      let txIndex = resolvedOrder?.txIndex;
      if (resolvedOrder?.blockTimeSeconds !== undefined) {
        blockTimeSeconds = resolvedOrder.blockTimeSeconds;
      }
      if (!resolvedOrder) {
        const order = await this.resolveLogOrder(logs, ctx);
        if (!this.isRunning || this.stopRequested) return;
        txIndex = order.txIndex;
        if (order.blockTimeSeconds !== undefined) {
          blockTimeSeconds = order.blockTimeSeconds;
        }
      }

      if (txIndex === undefined) {
        this.errorCount++;
        this.scheduleFailSafeStop(
          "Deterministic tx_index unavailable in websocket mode; entering fail-safe stop so catch-up can recover missed events",
          {
            slot: ctx.slot,
            signature: logs.signature,
            errorCount: this.errorCount,
          }
        );
        return;
      }

      const currentCursor = await this.loadCurrentCursorSnapshot();
      if (!this.isRunning || this.stopRequested) return;
      const nextSlot = BigInt(ctx.slot);
      let shouldAdvanceDurableCursor = true;
      if (
        currentCursor?.lastSlot !== null
        && currentCursor?.lastSlot !== undefined
        && !shouldAdvanceCursor(
          currentCursor.lastSlot,
          currentCursor.lastTxIndex,
          currentCursor.lastSignature,
          nextSlot,
          txIndex,
          logs.signature
        )
      ) {
        shouldAdvanceDurableCursor = false;
        logger.debug(
          {
            signature: logs.signature,
            slot: ctx.slot,
            txIndex,
            cursorSlot: currentCursor.lastSlot.toString(),
            cursorTxIndex: currentCursor.lastTxIndex,
            cursorSignature: currentCursor.lastSignature,
            cursorSource: currentCursor.source,
          },
          "Processing WebSocket log behind durable cursor without cursor advance"
        );
      }

      const blockTime = resolveEventBlockTime(blockTimeSeconds, ctx.slot);
      const typedEvents: Array<{
        typedEvent: ProgramEvent;
        eventOrdinal: number;
        rawEvent: typeof events[number];
      }> = [];
      for (const [eventOrdinal, event] of events.entries()) {
        const typedEvent = toTypedEvent(event);
        if (!typedEvent) {
          this.errorCount++;
          this.scheduleFailSafeStop(
            "WebSocket decoded an unsupported program event; entering fail-safe stop instead of advancing past an unknown event",
            {
              signature: logs.signature,
              slot: ctx.slot,
              eventOrdinal,
              eventName: event.name,
              errorCount: this.errorCount,
            }
          );
          return;
        }
        typedEvents.push({
          typedEvent,
          eventOrdinal,
          rawEvent: event,
        });
      }

      let allEventsProcessed = true;
      const suspendLocalDigests = Boolean(this.prisma && config.dbMode === "local");
      if (suspendLocalDigests) await suspendLocalDerivedDigests();
      try {
        for (const { typedEvent, eventOrdinal, rawEvent } of typedEvents) {
          if (!this.isRunning || this.stopRequested) {
            allEventsProcessed = false;
            logger.info({ signature: logs.signature, slot: ctx.slot }, "Stopping WebSocket transaction processing before cursor advancement");
            return;
          }

          const eventCtx: EventContext = {
            signature: logs.signature,
            slot: BigInt(ctx.slot),
            blockTime,
            txIndex,
            eventOrdinal,
            source: "websocket",
            skipCursorUpdate: true,
          };

          let eventProcessed = true;
          let eventErrorMessage: string | undefined;

          try {
            await handleEventAtomic(this.prisma, typedEvent, eventCtx);
            if (!this.isRunning || this.stopRequested) {
              allEventsProcessed = false;
              logger.info({ signature: logs.signature, slot: ctx.slot }, "WebSocket stop requested during event processing; cursor will not advance");
              return;
            }
          } catch (eventError) {
            eventProcessed = false;
            allEventsProcessed = false;
            eventErrorMessage = eventError instanceof Error ? eventError.message : String(eventError);
            logger.error({
              error: eventErrorMessage,
              eventType: typedEvent.type,
              signature: logs.signature
            }, "Error handling event — cursor will NOT advance past this tx");
          }

          // Only log to Prisma if in local mode
          if (this.prisma) {
            if (!this.isRunning || this.stopRequested) {
              allEventsProcessed = false;
              return;
            }
            try {
              await this.prisma.eventLog.create({
                data: {
                  eventType: eventProcessed ? typedEvent.type : "PROCESSING_FAILED",
                  signature: logs.signature,
                  slot: BigInt(ctx.slot),
                  blockTime,
                  data: rawEvent.data as object,
                  processed: eventProcessed,
                  error: eventErrorMessage,
                },
              });
            } catch (prismaError) {
              logger.warn({
                error: prismaError instanceof Error ? prismaError.message : String(prismaError),
                signature: logs.signature
              }, "Failed to log event to Prisma");
            }
          }
        }
        // Only advance cursor if ALL events in this tx were processed successfully
        if (!allEventsProcessed) {
          this.errorCount++;
          this.scheduleFailSafeStop(
            "WebSocket transaction processing failed; entering fail-safe stop so catch-up can replay from the last safe cursor",
            {
              signature: logs.signature,
              slot: ctx.slot,
              errorCount: this.errorCount,
            }
          );
          return;
        }

        await maybeUpsertProofPassMatchesForLogs({
          signature: logs.signature,
          slot: BigInt(ctx.slot),
          logs: logs.logs,
          typedEvents,
        });

        if (!shouldAdvanceDurableCursor) {
          logger.debug(
            { signature: logs.signature, slot: ctx.slot },
            "Skipping cursor update for WebSocket log behind durable cursor"
          );
          return;
        }

        if (this.freezeCursorAdvancement || this.stopRequested || !this.isRunning) {
          logger.info(
            { signature: logs.signature, slot: ctx.slot },
            "Skipping cursor update during websocket shutdown/fail-safe stop"
          );
          return;
        }

        // Update state - both local and Supabase modes (with monotonic guard)
        try {
          if (!this.isRunning || this.stopRequested || this.freezeCursorAdvancement) return;
          const newSlot = BigInt(ctx.slot);
          if (this.prisma) {
            const current = await this.prisma.indexerState.findUnique({
              where: { id: "main" },
              select: { lastSlot: true, lastTxIndex: true, lastSignature: true },
            });
            if (!this.isRunning || this.stopRequested || this.freezeCursorAdvancement) {
              logger.info(
                { signature: logs.signature, slot: ctx.slot },
                "Skipping cursor update after shutdown while websocket state read was in flight"
              );
              return;
            }
            if (!shouldAdvanceCursor(
              current?.lastSlot,
              current?.lastTxIndex,
              current?.lastSignature,
              newSlot,
              txIndex ?? null,
              logs.signature
            )) {
              logger.debug(
                {
                  slot: ctx.slot,
                  currentSlot: current?.lastSlot?.toString() ?? null,
                  txIndex: txIndex ?? null,
                  currentTxIndex: current?.lastTxIndex ?? null,
                  signature: logs.signature,
                  currentSignature: current?.lastSignature ?? null,
                },
                "WS cursor update skipped — monotonic guard"
              );
            } else {
              if (!this.isRunning || this.stopRequested || this.freezeCursorAdvancement) {
                logger.info(
                  { signature: logs.signature, slot: ctx.slot },
                  "Skipping cursor save before websocket persistence during shutdown/fail-safe stop"
                );
                return;
              }
              if (!(await trySaveLocalIndexerStateWithSql(this.prisma, {
                signature: logs.signature,
                slot: newSlot,
                txIndex: txIndex ?? null,
                source: "websocket",
                updatedAt: blockTime,
              }))) {
                if (!this.isRunning || this.stopRequested || this.freezeCursorAdvancement) {
                  logger.info(
                    { signature: logs.signature, slot: ctx.slot },
                    "Skipping cursor upsert after shutdown while websocket SQL guard path was in flight"
                  );
                  return;
                }
                await this.prisma.indexerState.upsert({
                  where: { id: "main" },
                  create: {
                    id: "main",
                    lastSignature: logs.signature,
                    lastSlot: newSlot,
                    lastTxIndex: txIndex ?? null,
                    source: "websocket",
                  },
                  update: {
                    lastSignature: logs.signature,
                    lastSlot: newSlot,
                    lastTxIndex: txIndex ?? null,
                    source: "websocket",
                  },
                });
              }
              this.rememberPersistedCursor(
                logs.signature,
                newSlot,
                txIndex ?? null,
                "websocket",
                blockTime
              );
            }
          } else {
            if (!this.isRunning || this.stopRequested || this.freezeCursorAdvancement) {
              logger.info(
                { signature: logs.signature, slot: ctx.slot },
                "Skipping Supabase cursor save before websocket persistence during shutdown/fail-safe stop"
              );
              return;
            }
            await saveIndexerState(logs.signature, newSlot, txIndex ?? null, "websocket", blockTime);
            this.rememberPersistedCursor(
              logs.signature,
              newSlot,
              txIndex ?? null,
              "websocket",
              blockTime
            );
          }
        } catch (stateError) {
          this.errorCount++;
          this.scheduleFailSafeStop(
            "Failed to save websocket cursor after processing transaction; entering fail-safe stop so catch-up can reconcile durable state",
            {
              error: stateError instanceof Error ? stateError.message : String(stateError),
              signature: logs.signature,
              slot: ctx.slot,
              errorCount: this.errorCount,
            }
          );
          return;
        }

        this.processedCount++;
        const duration = Date.now() - startTime;

        if (this.processedCount % 100 === 0) {
          logger.info({
            processedCount: this.processedCount,
            errorCount: this.errorCount,
            queueSize: this.logQueue.size,
            droppedLogs: this.droppedLogs,
            lastDuration: duration
          }, "WebSocket processing stats");
        }
      } finally {
        if (suspendLocalDigests) resumeLocalDerivedDigests();
      }

    } catch (error) {
      if (!this.isRunning || this.stopRequested) {
        return;
      }
      const errorMessage = error instanceof Error ? error.message : String(error);
      logger.error({ error: errorMessage, signature: logs.signature }, "Error handling logs");

      // Log errors only in local mode
      if (this.prisma) {
        try {
          await this.prisma.eventLog.create({
            data: {
              eventType: "PROCESSING_FAILED",
              signature: logs.signature,
              slot: BigInt(ctx.slot),
              blockTime: resolveEventBlockTime(undefined, ctx.slot),
              data: { logs: logs.logs },
              processed: false,
              error: errorMessage,
            },
          });
        } catch (logError) {
          logger.warn({ error: logError }, "Failed to log error to Prisma");
        }
      }

      // Re-throw to be caught by wrapper
      throw error;
    }
  }

  private async reconnect(expectedRunToken: number = this.runToken): Promise<void> {
    if (!this.isRunning || this.runToken !== expectedRunToken) return;

    if (this.retryCount >= this.maxRetries) {
      logger.error({
        retryCount: this.retryCount,
        maxRetries: this.maxRetries
      }, "Max retries exceeded, WebSocket indexer stopped");
      this.isRunning = false;
      this.stopHealthCheck();
      this.stopReconciliationCheck();
      return;
    }

    this.retryCount++;
    logger.info(
      { retryCount: this.retryCount, maxRetries: this.maxRetries, interval: this.reconnectInterval },
      "Reconnecting WebSocket"
    );

    await this.waitForReconnectDelay();

    // Re-check after timeout - stop() may have been called during wait
    if (!this.isRunning || this.runToken !== expectedRunToken) {
      logger.info("Reconnect aborted - stop() was called during wait");
      return;
    }

    await this.subscribe();
  }

  isActive(): boolean {
    return this.isRunning && this.subscriptionId !== null;
  }

  /**
   * Check if WebSocket is in recovery mode (running but reconnecting)
   * Used by monitor to avoid killing WS during self-healing
   */
  isRecovering(): boolean {
    return this.isRunning && (this.isReconnecting || this.isCheckingHealth);
  }

  getStats(): {
    processedCount: number;
    errorCount: number;
    lastActivity: number;
    queueSize: number;
    droppedLogs: number;
  } {
    return {
      processedCount: this.processedCount,
      errorCount: this.errorCount,
      lastActivity: this.lastActivityTime,
      queueSize: this.logQueue.size + this.pendingLogNotifications.size,
      droppedLogs: this.droppedLogs,
    };
  }
}

/**
 * Test if WebSocket endpoint is available
 * @param rpcUrl - HTTP RPC endpoint for connection test
 * @param wsUrl - WebSocket endpoint to configure
 */
export async function testWebSocketConnection(rpcUrl: string, wsUrl: string, programId = config.programId): Promise<boolean> {
  try {
    logger.debug({ rpcUrl, wsUrl }, "Testing WebSocket connection");

    const connection = new Connection(rpcUrl, {
      wsEndpoint: wsUrl,
      commitment: "confirmed",
      disableRetryOnRateLimit: true,
    });

    const slot = await connection.getSlot();

    const rawSupport = await probeRawLogsSubscribe(wsUrl, programId);
    if (rawSupport !== "supported") {
      if (rawSupport === "unsupported") {
        logUnsupportedLogsSubscribe(wsUrl);
      } else {
        logUnavailableLogsSubscribe(wsUrl, rawSupport);
      }
      logger.debug({ slot, rawSupport }, "WebSocket connection test complete");
      return false;
    }

    const wsReady = await probeTemporaryLogsSubscription(connection, new PublicKey(programId));

    logger.debug({ slot, wsReady }, "WebSocket connection test complete");
    return wsReady;
  } catch (error) {
    logger.debug({ error }, "WebSocket connection test failed");
    return false;
  }
}
