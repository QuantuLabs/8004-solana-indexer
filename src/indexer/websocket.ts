import {
  Connection,
  PublicKey,
  Logs,
  Context,
  ParsedTransactionWithMeta,
} from "@solana/web3.js";
import { PrismaClient } from "@prisma/client";
import PQueue from "p-queue";
import WebSocket from "ws";
import { config } from "../config.js";
import { parseTransaction, parseTransactionLogs, toTypedEvent } from "../parser/decoder.js";
import {
  handleEventAtomic,
  EventContext,
  suspendLocalDerivedDigests,
  resumeLocalDerivedDigests,
} from "../db/handlers.js";
import { trySaveLocalIndexerStateWithSql } from "../db/indexer-state-local.js";
import {
  clearIndexerStateSnapshot,
  loadIndexerStateSnapshot,
  restoreIndexerStateSnapshot,
  saveIndexerState,
} from "../db/supabase.js";
import { createChildLogger } from "../logger.js";
import { resolveEventBlockTime } from "./block-time.js";
import { matchProofPassFeedbacks } from "../extras/proofpass.js";
import { upsertProofPassMatches } from "../db/proofpass.js";
import type { NewFeedback, ProgramEvent } from "../parser/types.js";

const logger = createChildLogger("websocket");

// Health check interval (30 seconds)
const HEALTH_CHECK_INTERVAL = 30_000;
// Consider connection stale if no activity for 2 minutes
const STALE_THRESHOLD = 120_000;
// Keep websocket tx handling sequential so cursor advancement remains prefix-safe.
const MAX_CONCURRENT_HANDLERS = 1;
const MAX_QUEUE_SIZE = 10_000;

export interface WebSocketIndexerOptions {
  connection: Connection;
  prisma: PrismaClient | null;
  programId: PublicKey;
  wsUrl?: string;
  reconnectInterval?: number;
  maxRetries?: number;
}

type PersistedCursorSnapshot = {
  signature: string;
  slot: bigint;
  txIndex: number | null;
  source: string | null;
  updatedAt: Date;
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
  if (nextSlot > currentSlot) return true;
  if (nextSlot < currentSlot) return false;
  const currentOrderTxIndex = currentTxIndex ?? -1;
  const nextOrderTxIndex = nextTxIndex ?? -1;
  if (nextOrderTxIndex > currentOrderTxIndex) return true;
  if (nextOrderTxIndex < currentOrderTxIndex) return false;
  if (!currentSignature) return true;
  return nextSignature >= currentSignature;
}

async function maybeUpsertProofPassMatchesForLogs(params: {
  signature: string;
  slot: bigint;
  logs: string[];
  typedEvents: Array<{ typedEvent: ProgramEvent }>;
}): Promise<void> {
  if (!config.enableProofPass) {
    return;
  }

  const feedbackEvents = params.typedEvents
    .filter((entry): entry is { typedEvent: { type: "NewFeedback"; data: NewFeedback } } =>
      entry.typedEvent.type === "NewFeedback"
    )
    .map((entry) => entry.typedEvent.data);

  const matches = matchProofPassFeedbacks({
    logs: params.logs,
    signature: params.signature,
    slot: params.slot,
    feedbackEvents,
  });
  if (matches.length === 0) {
    return;
  }

  await upsertProofPassMatches(matches);
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
  // Bounded concurrency queue to prevent OOM during high traffic
  private logQueue: PQueue;
  private droppedLogs = 0;
  private lastPersistedCursor: PersistedCursorSnapshot | null = null;

  constructor(options: WebSocketIndexerOptions) {
    // Initialize bounded queue for log processing
    this.logQueue = new PQueue({ concurrency: MAX_CONCURRENT_HANDLERS });
    this.connection = options.connection;
    this.prisma = options.prisma;
    this.programId = options.programId;
    this.wsUrl = options.wsUrl ?? null;
    this.reconnectInterval = options.reconnectInterval || config.wsReconnectInterval;
    this.maxRetries = options.maxRetries || config.wsMaxRetries;
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
          this.lastPersistedCursor = {
            signature: current.lastSignature,
            slot: BigInt(current.lastSlot),
            txIndex: current.lastTxIndex ?? null,
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
          this.lastPersistedCursor = {
            signature: current.lastSignature,
            slot: current.lastSlot,
            txIndex: current.lastTxIndex ?? null,
            source: current.source ?? null,
            updatedAt: current.updatedAt,
          };
        }
      } catch (error) {
        logger.warn({ error }, "Failed to snapshot current Supabase cursor before starting websocket indexer");
      }
    }
    logger.info({ programId: this.programId.toBase58() }, "Starting WebSocket indexer");

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
  }

  async stop(): Promise<void> {
    logger.info({
      processedCount: this.processedCount,
      errorCount: this.errorCount,
      queueSize: this.logQueue.size,
      droppedLogs: this.droppedLogs
    }, "Stopping WebSocket indexer");

    this.isRunning = false;
    this.stopRequested = true;
    this.freezeCursorAdvancement = true;
    this.retryCount = 0;
    this.stopHealthCheck();
    this.clearReconnectDelay();

    this.logQueue.pause();
    const clearedQueuedLogs = this.logQueue.size;
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
        if (this.prisma) {
          await this.prisma.indexerState.upsert({
            where: { id: "main" },
            create: {
              id: "main",
              lastSignature: restoreCursorSnapshot.signature,
              lastSlot: restoreCursorSnapshot.slot,
              lastTxIndex: restoreCursorSnapshot.txIndex,
              source: restoreCursorSnapshot.source ?? "websocket",
              updatedAt: restoreCursorSnapshot.updatedAt,
            },
            update: {
              lastSignature: restoreCursorSnapshot.signature,
              lastSlot: restoreCursorSnapshot.slot,
              lastTxIndex: restoreCursorSnapshot.txIndex,
              source: restoreCursorSnapshot.source ?? "websocket",
              updatedAt: restoreCursorSnapshot.updatedAt,
            },
          });
        } else {
          await restoreIndexerStateSnapshot(
            restoreCursorSnapshot.signature,
            restoreCursorSnapshot.slot,
            restoreCursorSnapshot.txIndex,
            (restoreCursorSnapshot.source as "poller" | "websocket" | "substreams" | null) ?? "websocket",
            restoreCursorSnapshot.updatedAt,
          );
        }
        logger.info(
          {
            signature: restoreCursorSnapshot.signature,
            slot: restoreCursorSnapshot.slot.toString(),
            txIndex: restoreCursorSnapshot.txIndex,
          },
          "Restored safe cursor snapshot after websocket shutdown"
        );
      } catch (error) {
        logger.warn({ error }, "Failed to restore safe cursor snapshot after websocket shutdown");
      }
    } else if (clearedQueuedLogs > 0 || hadInFlightWork) {
      const clearedAt = new Date();
      try {
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
      } catch (error) {
        logger.warn({ error }, "Failed to clear websocket cursor after shutdown without a safe snapshot");
      }
    }
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
          logger.info({
            timeSinceActivity,
          }, "No WebSocket events but onLogs heartbeat probe is healthy - program may have low activity");
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
    // Concurrency guard - prevent overlapping reconnects
    if (this.isReconnecting) {
      logger.debug("Reconnection already in progress, skipping");
      return;
    }

    this.isReconnecting = true;
    try {
      logger.info("Forcing WebSocket reconnection...");

      // Clean up existing subscription
      if (expectedSubscriptionId !== null) {
        try {
          if (!this.isRunning || this.runToken !== runToken) {
            return;
          }
          await this.connection.removeOnLogsListener(expectedSubscriptionId);
        } catch (error) {
          logger.debug({ error }, "Error removing old subscription during reconnect");
        }
        if (this.subscriptionId === expectedSubscriptionId) {
          this.subscriptionId = null;
        }
      }

      // Reconnect
      if (!this.isRunning || this.runToken !== runToken) {
        return;
      }
      await this.reconnect(this.runToken);
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

          if (this.logQueue.size >= MAX_QUEUE_SIZE) {
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

          // Add to bounded queue instead of fire-and-forget
          this.logQueue.add(async () => {
            try {
              await this.handleLogs(logs, ctx);
            } catch (error) {
              this.errorCount++;
              logger.error({
                error: error instanceof Error ? error.message : String(error),
                signature: logs.signature,
                errorCount: this.errorCount
              }, "Error in handleLogs - caught by queue");

              // If too many errors, force reconnect
              if (this.errorCount > 10 && this.errorCount % 10 === 0) {
                logger.warn({ errorCount: this.errorCount }, "High error count, scheduling reconnect");
                this.forceReconnect().catch(e => {
                  logger.error({ error: e }, "Failed to reconnect after errors");
                });
              }
            }
          });
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

  private async handleLogs(logs: Logs, ctx: Context): Promise<void> {
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
          parsedTx = await this.connection.getParsedTransaction(logs.signature, {
            maxSupportedTransactionVersion: config.maxSupportedTransactionVersion,
          });
          if (!this.isRunning || this.stopRequested) return;

          if (parsedTx) {
            const parsed = parseTransaction(parsedTx);
            if (parsed?.events.length) {
              events = parsed.events;
            }
          }
        } catch (txFetchError) {
          logger.debug(
            {
              signature: logs.signature,
              error: txFetchError instanceof Error ? txFetchError.message : String(txFetchError),
            },
            "Failed to fetch parsed transaction for identity lock enrichment (non-fatal)"
          );
        }
      }

      if (events.length === 0) return;

      logger.debug(
        { signature: logs.signature, eventCount: events.length, slot: ctx.slot },
        "Received logs"
      );

      // Prefer confirmed blockTime from parsed transaction when available.
      let blockTimeSeconds: number | null | undefined = parsedTx?.blockTime;

      // Resolve tx_index from block for metadata/tie-break parity with other indexers
      let txIndex: number | undefined;
      try {
        const block = await this.connection.getBlock(ctx.slot, {
          maxSupportedTransactionVersion: config.maxSupportedTransactionVersion,
          transactionDetails: "full",
        });
        if (!this.isRunning || this.stopRequested) return;
        if (block?.blockTime !== null && block?.blockTime !== undefined) {
          blockTimeSeconds = block.blockTime;
        }
        if (block?.transactions) {
          const idx = block.transactions.findIndex(
            (tx) => tx.transaction.signatures[0] === logs.signature,
          );
          if (idx >= 0) txIndex = idx;
        }
      } catch (blockError) {
        logger.debug(
          { slot: ctx.slot, error: blockError instanceof Error ? blockError.message : String(blockError) },
          "Failed to resolve tx_index from block (non-fatal)",
        );
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

      const blockTime = resolveEventBlockTime(blockTimeSeconds, ctx.slot);
      const typedEvents: Array<{
        typedEvent: ProgramEvent;
        eventOrdinal: number;
        rawEvent: typeof events[number];
      }> = [];
      for (const [eventOrdinal, event] of events.entries()) {
        const typedEvent = toTypedEvent(event);
        if (!typedEvent) {
          continue;
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
          logger.warn({ signature: logs.signature, slot: ctx.slot },
            "Skipping cursor update — failed event(s) in this tx will be retried on restart");
          return;
        }

        await maybeUpsertProofPassMatchesForLogs({
          signature: logs.signature,
          slot: BigInt(ctx.slot),
          logs: logs.logs,
          typedEvents,
        });

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
            // Supabase mode — saveIndexerState already has SQL-level monotonic guard
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
          logger.error({
            error: stateError instanceof Error ? stateError.message : String(stateError),
            signature: logs.signature
          }, "Failed to save indexer state");
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
      queueSize: this.logQueue.size,
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
