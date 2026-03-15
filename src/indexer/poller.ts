import {
  Connection,
  PublicKey,
  ConfirmedSignatureInfo,
  ParsedTransactionWithMeta,
} from "@solana/web3.js";
import { PrismaClient } from "@prisma/client";
import { config } from "../config.js";
import { parseTransaction, toTypedEvent } from "../parser/decoder.js";
import {
  handleEventAtomic,
  EventContext,
  suspendLocalDerivedDigests,
  resumeLocalDerivedDigests,
} from "../db/handlers.js";
import { trySaveLocalIndexerStateWithSql } from "../db/indexer-state-local.js";
import { loadIndexerState, saveIndexerState, getPool } from "../db/supabase.js";
import { createChildLogger } from "../logger.js";
import {
  BatchRpcBackoffRequiredError,
  BatchRpcFetcher,
  EventBuffer,
} from "./batch-processor.js";
import { resolveEventBlockTime } from "./block-time.js";
import { metadataQueue } from "./metadata-queue.js";

const logger = createChildLogger("poller");

// Batch processing configuration
// Batch RPC fetching: enabled by default, can be disabled per instance via env
// Batch DB writes: only for Supabase mode (uses raw SQL)
const USE_BATCH_DB = config.dbMode === "supabase";
const MISSING_COLLECTION_ID_SCHEMA_FATAL_MESSAGE =
  "Missing collection_id schema in local database. Apply Prisma migrations (prisma migrate deploy) before starting the indexer.";

export interface PollerOptions {
  connection: Connection;
  prisma: PrismaClient | null;
  programId: PublicKey;
  pollingInterval?: number;
  batchSize?: number;
}

export interface PollerBootstrapOptions {
  retryDelayMs?: number;
  suppressEventLogWrites?: boolean;
}

interface ProcessNewTransactionsResult {
  fetchedCount: number;
  haltedOnError: boolean;
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

function compareSignatures(a: string, b: string): number {
  if (a === b) return 0;
  return a < b ? -1 : 1;
}

function compareTransactionCursor(
  aTxIndex: number | null | undefined,
  aSignature: string,
  bTxIndex: number | null | undefined,
  bSignature: string
): number {
  const aOrderTxIndex = aTxIndex ?? -1;
  const bOrderTxIndex = bTxIndex ?? -1;
  if (aOrderTxIndex !== bOrderTxIndex) {
    return aOrderTxIndex - bOrderTxIndex;
  }
  return compareSignatures(aSignature, bSignature);
}

function hasProgramDataLogsForProgram(
  tx: ParsedTransactionWithMeta,
  programId: string
): boolean {
  const logs = tx.meta?.logMessages ?? [];
  const invokeStack: string[] = [];

  for (const log of logs) {
    const invokeMatch = /^Program ([1-9A-HJ-NP-Za-km-z]+) invoke \[\d+\]$/.exec(log);
    if (invokeMatch) {
      invokeStack.push(invokeMatch[1]);
      continue;
    }

    const completionMatch = /^Program ([1-9A-HJ-NP-Za-km-z]+) (success|failed:.*)$/.exec(log);
    if (completionMatch) {
      if (invokeStack[invokeStack.length - 1] === completionMatch[1]) {
        invokeStack.pop();
      }
      continue;
    }

    if (log.startsWith("Program data:") && invokeStack[invokeStack.length - 1] === programId) {
      return true;
    }
  }

  return false;
}

function hasUnresolvedTxIndex(
  txIndexMap: Map<string, number | null>,
  signatures: ConfirmedSignatureInfo[]
): boolean {
  return signatures.some((sig) => {
    const value = txIndexMap.get(sig.signature);
    return value === null || value === undefined;
  });
}

function toErrorMessage(error: unknown): string {
  if (typeof error === "string") return error;
  if (error instanceof Error) return error.message || String(error);
  try {
    return JSON.stringify(error);
  } catch {
    return String(error);
  }
}

function toErrorDetails(error: unknown): Record<string, unknown> {
  if (error instanceof Error) {
    const rpcLike = error as Error & {
      code?: unknown;
      status?: unknown;
      data?: unknown;
      details?: unknown;
      cause?: unknown;
    };
    return {
      name: error.name,
      message: error.message || String(error),
      code: rpcLike.code ?? null,
      status: rpcLike.status ?? null,
      details: rpcLike.details ?? null,
      data: rpcLike.data ?? null,
      cause: rpcLike.cause instanceof Error ? rpcLike.cause.message : rpcLike.cause ?? null,
    };
  }
  if (typeof error === "object" && error !== null) {
    const objectError = error as Record<string, unknown>;
    return {
      message: toErrorMessage(error),
      code: objectError.code ?? null,
      status: objectError.status ?? null,
      details: objectError.details ?? null,
      data: objectError.data ?? null,
    };
  }
  return { message: toErrorMessage(error) };
}

function isMissingCollectionIdSchemaError(error: unknown): boolean {
  const maybe = error as { code?: unknown; message?: unknown } | null;
  const code = typeof maybe?.code === "string" ? maybe.code : "";
  const message = typeof maybe?.message === "string" ? maybe.message : String(error);
  const missingSchemaPattern = /missing collection_id schema|column .*collection_id|column .*lastSeenTxIndex|no such column: collection_id|no such column: lastSeenTxIndex|has no column named collection_id|has no column named lastSeenTxIndex|column "?collection_id"? does not exist|column "?lastSeenTxIndex"? does not exist|Unknown arg .*collectionId|Unknown arg .*lastSeenTxIndex/i;
  if (code === "P2022") {
    return /collection_id|collectionId|lastSeenTxIndex|CollectionPointer/i.test(message);
  }
  if (code === "P2010") {
    return missingSchemaPattern.test(message);
  }
  return missingSchemaPattern.test(message);
}

function isProviderOverloadedError(error: unknown): boolean {
  const maybe = error as {
    code?: unknown;
    status?: unknown;
    message?: unknown;
    response?: { status?: unknown };
  } | null;

  const statusish = [maybe?.code, maybe?.status, maybe?.response?.status];
  if (statusish.some((value) => value === 429 || value === 502 || value === 503 || value === 504)) {
    return true;
  }

  const message = typeof maybe?.message === "string" ? maybe.message.toLowerCase() : String(error).toLowerCase();
  return (
    message.includes("429") ||
    message.includes("too many requests") ||
    message.includes("rate limit") ||
    message.includes("502") ||
    message.includes("503") ||
    message.includes("504") ||
    message.includes("bad gateway") ||
    message.includes("service unavailable") ||
    message.includes("gateway timeout") ||
    message.includes("cloudflare") ||
    message.includes("timeout") ||
    message.includes("timed out") ||
    message.includes("socket hang up") ||
    message.includes("econnreset") ||
    message.includes("fetch failed")
  );
}

function isDeterministicPaginationBoundaryError(error: unknown): boolean {
  const message = toErrorMessage(error);
  return /Deterministic tx_index unavailable for pagination (slot boundary|stop slot)/i.test(message);
}

class RetryableTransactionFetchError extends Error {
  readonly signature: string;
  readonly slot: number;

  constructor(signature: string, slot: number) {
    super(`Transaction temporarily unavailable for signature ${signature}`);
    this.name = "RetryableTransactionFetchError";
    this.signature = signature;
    this.slot = slot;
  }
}

function isRetryableTransactionFetchError(error: unknown): error is RetryableTransactionFetchError {
  return error instanceof RetryableTransactionFetchError;
}

const HISTORICAL_SCAN_ROW_PREFIX = "historical-scan:";
const HISTORICAL_SCAN_PROGRESS_ROW_PREFIX = "historical-scan-progress:";
const HISTORICAL_SCAN_STATE_ROW_PREFIX = "historical-scan-state:";

interface HistoricalScanPageState {
  pageIndex: number;
  beforeSignature: string | null;
  nextBeforeSignature: string | null;
  signatures: ConfirmedSignatureInfo[] | null;
}

interface HistoricalScanState {
  nextBeforeSignature: string | null;
  scanComplete: boolean;
}

function historicalScanRowId(stopSignature: string, pageIndex: number): string {
  return `${HISTORICAL_SCAN_ROW_PREFIX}${stopSignature}:${pageIndex}`;
}

function historicalScanRowPrefix(stopSignature: string): string {
  return `${HISTORICAL_SCAN_ROW_PREFIX}${stopSignature}:`;
}

function historicalScanProgressRowId(stopSignature: string): string {
  return `${HISTORICAL_SCAN_PROGRESS_ROW_PREFIX}${stopSignature}`;
}

function historicalScanStateRowId(stopSignature: string): string {
  return `${HISTORICAL_SCAN_STATE_ROW_PREFIX}${stopSignature}`;
}

function parseHistoricalScanStateStopSignature(id: string): string | null {
  if (!id.startsWith(HISTORICAL_SCAN_STATE_ROW_PREFIX)) return null;
  const suffix = id.slice(HISTORICAL_SCAN_STATE_ROW_PREFIX.length);
  return suffix.length > 0 ? suffix : null;
}

function parseHistoricalScanStopSignature(id: string): string | null {
  if (!id.startsWith(HISTORICAL_SCAN_ROW_PREFIX)) return null;
  const suffix = id.slice(HISTORICAL_SCAN_ROW_PREFIX.length);
  const separator = suffix.lastIndexOf(":");
  if (separator <= 0) return null;
  return suffix.slice(0, separator);
}

function parseHistoricalScanPageIndex(id: string, stopSignature: string): number | null {
  const prefix = historicalScanRowPrefix(stopSignature);
  if (!id.startsWith(prefix)) return null;
  const value = Number.parseInt(id.slice(prefix.length), 10);
  return Number.isFinite(value) ? value : null;
}

function serializeHistoricalScanStatePayload(
  nextBeforeSignature: string | null,
  scanComplete: boolean
): string {
  return JSON.stringify({
    nextBeforeSignature,
    scanComplete,
  });
}

function parseHistoricalScanStatePayload(
  rawSource: string | null | undefined
): HistoricalScanState {
  if (!rawSource) {
    return {
      nextBeforeSignature: null,
      scanComplete: false,
    };
  }

  try {
    const parsed = JSON.parse(rawSource) as {
      nextBeforeSignature?: unknown;
      scanComplete?: unknown;
    };
    return {
      nextBeforeSignature:
        typeof parsed.nextBeforeSignature === "string" && parsed.nextBeforeSignature.length > 0
          ? parsed.nextBeforeSignature
          : null,
      scanComplete: parsed.scanComplete === true,
    };
  } catch {
    return {
      nextBeforeSignature: rawSource,
      scanComplete: false,
    };
  }
}

function serializeHistoricalScanPagePayload(
  nextBeforeSignature: string | null,
  signatures: ConfirmedSignatureInfo[]
): string {
  return JSON.stringify({
    nextBeforeSignature,
    signatures: signatures.map((signature) => ({
      signature: signature.signature,
      slot: signature.slot,
    })),
  });
}

function parseHistoricalScanPagePayload(
  rawSource: string | null | undefined
): { nextBeforeSignature: string | null; signatures: ConfirmedSignatureInfo[] | null } {
  if (!rawSource) {
    return { nextBeforeSignature: null, signatures: null };
  }

  try {
    const parsed = JSON.parse(rawSource) as {
      nextBeforeSignature?: unknown;
      signatures?: Array<{ signature?: unknown; slot?: unknown }>;
    };
    const nextBeforeSignature =
      typeof parsed.nextBeforeSignature === "string" && parsed.nextBeforeSignature.length > 0
        ? parsed.nextBeforeSignature
        : null;
    const signatures = Array.isArray(parsed.signatures)
      ? parsed.signatures.reduce<ConfirmedSignatureInfo[]>((acc, entry) => {
          const signature = typeof entry?.signature === "string" ? entry.signature : null;
          const slot = typeof entry?.slot === "number" ? entry.slot : null;
          if (!signature || slot === null || !Number.isFinite(slot)) {
            return acc;
          }
          acc.push({
            signature,
            slot,
            err: null,
            memo: null,
            blockTime: null,
            confirmationStatus: "confirmed",
          });
          return acc;
        }, [])
      : null;

    return {
      nextBeforeSignature,
      signatures: signatures && signatures.length > 0 ? signatures : null,
    };
  } catch {
    return {
      nextBeforeSignature: rawSource || null,
      signatures: null,
    };
  }
}


export class Poller {
  private connection: Connection;
  private prisma: PrismaClient | null;
  private programId: PublicKey;
  private pollingInterval: number;
  private batchSize: number;
  private isRunning = false;
  private lastSignature: string | null = null;
  private lastSlot: bigint | null = null;
  private lastTxIndex: number | null = null;
  private processedCount = 0;
  private errorCount = 0;
  private lastStatsLog = Date.now();
  // Track pagination continuation when hitting memory limits
  // pendingContinuation: where to resume pagination FROM
  // pendingStopSignature: where to STOP (original lastSignature when we hit the limit)
  private pendingContinuation: string | null = null;
  private pendingStopSignature: string | null = null;
  // True when fetchSignatures returned a retry-exhausted partial page set.
  // In that case we skip processing this cycle to avoid cursor/state advance.
  private hadPaginationPartial = false;
  private configuredStartCursorValidated = false;
  private suppressEventLogWrites = false;
  private runPromise: Promise<void> | null = null;
  private pollDelayTimeout: ReturnType<typeof setTimeout> | null = null;
  private pollDelayResolve: ((cancelled?: boolean) => void) | null = null;
  private stopSlotPaginationBefore: string | null = null;
  private stopSlotPaginationBeforeResolved = false;
  private stopSlotPaginationContinuation: string | null = null;
  private pendingHistoricalResume = false;
  private hasReachedLiveFrontier = false;

  // Batch processing components (Supabase mode only)
  private useBatchRpc: boolean;
  private batchFetcher: BatchRpcFetcher | null = null;
  private eventBuffer: EventBuffer | null = null;
  private stopSlot: bigint | null;

  constructor(options: PollerOptions) {
    this.connection = options.connection;
    this.prisma = options.prisma;
    this.programId = options.programId;
    this.pollingInterval = options.pollingInterval || config.pollingInterval;
    this.batchSize = options.batchSize || config.batchSize;
    this.useBatchRpc = config.pollerBatchRpcEnabled;
    this.stopSlot = config.indexerStopSlot;

    // Initialize batch RPC fetcher when enabled (has built-in fallback)
    if (this.useBatchRpc) {
      this.batchFetcher = new BatchRpcFetcher(this.connection);
      logger.info("Batch RPC fetching enabled (with fallback)");
    } else {
      logger.info("Batch RPC fetching disabled; using individual transaction RPC");
    }

    // Initialize batch DB writer (Supabase mode only)
    if (USE_BATCH_DB) {
      const pool = getPool();
      this.eventBuffer = new EventBuffer(pool, this.prisma);
      // Initialize metadata queue with same pool
      metadataQueue.setPool(pool);
      logger.info("Batch DB writes enabled (PostgreSQL)");
      logger.info({ metadataMode: config.metadataIndexMode }, "Metadata extraction queue initialized");
    }
  }

  private logStatsIfNeeded(): void {
    const now = Date.now();
    if (now - this.lastStatsLog > 60000) {
      logger.info({
        processedCount: this.processedCount,
        errorCount: this.errorCount,
        lastSignature: this.lastSignature?.slice(0, 16) + '...',
      }, "Poller stats (60s)");
      this.lastStatsLog = now;
    }
  }

  private async waitForDelay(ms: number): Promise<boolean> {
    if (!this.isRunning || ms <= 0) return false;

    return await new Promise<boolean>((resolve) => {
      const complete = (cancelled = false) => {
        if (this.pollDelayTimeout) {
          clearTimeout(this.pollDelayTimeout);
          this.pollDelayTimeout = null;
        }
        if (this.pollDelayResolve === complete) {
          this.pollDelayResolve = null;
        }
        resolve(!cancelled);
      };

      this.pollDelayResolve = complete;
      this.pollDelayTimeout = setTimeout(() => complete(false), ms);
    });
  }

  private async fetchParsedTransactionWithRetry(
    signature: string,
    slot: number
  ): Promise<ParsedTransactionWithMeta> {
    const maxRetries = 5;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const tx = await this.connection.getParsedTransaction(signature, {
          maxSupportedTransactionVersion: config.maxSupportedTransactionVersion,
        });
        if (tx) {
          return tx;
        }

        if (attempt < maxRetries) {
          logger.warn(
            { signature, slot, attempt, maxRetries },
            "Transaction lookup returned null; retrying before pausing historical replay"
          );
        }
      } catch (error) {
        if (!isProviderOverloadedError(error)) {
          throw error;
        }

        if (attempt < maxRetries) {
          logger.warn(
            {
              signature,
              slot,
              attempt,
              maxRetries,
              err: error instanceof Error ? error : undefined,
              error: toErrorDetails(error),
            },
            "Provider overloaded while fetching transaction; retrying"
          );
        }
      }

      if (attempt >= maxRetries) {
        break;
      }

      if (!this.isRunning) {
        break;
      }

      const retryDelayMs = Math.min(5000, 500 * (2 ** (attempt - 1)));
      const shouldRetry = await this.waitForDelay(retryDelayMs);
      if (!shouldRetry) {
        break;
      }
    }

    throw new RetryableTransactionFetchError(signature, slot);
  }

  private stopBeforeNewerSlot(slot: number | bigint, phase: "backfill" | "polling"): boolean {
    if (this.stopSlot === null) return false;
    const nextSlot = typeof slot === "bigint" ? slot : BigInt(slot);
    if (nextSlot <= this.stopSlot) return false;

    logger.info({
      phase,
      stopSlot: this.stopSlot.toString(),
      nextSlot: nextSlot.toString(),
      lastSignature: this.lastSignature,
      lastTxIndex: this.lastTxIndex,
    }, "INDEXER_STOP_SLOT reached, stopping poller before processing newer slots");

    this.isRunning = false;
    return true;
  }

  private async resolveStopSlotPaginationBefore(): Promise<string | null> {
    if (this.stopSlot === null) return null;
    if (this.stopSlotPaginationBeforeResolved) {
      return this.stopSlotPaginationBefore;
    }

    let beforeSignature: string | undefined = this.stopSlotPaginationContinuation || undefined;
    let retryCount = 0;
    let pageLimit = config.historicalScanSignaturePageLimit;
    const defaultMinPageLimit = Math.max(1, Math.min(25, this.batchSize));
    let minPageLimit = defaultMinPageLimit;

    while (this.isRunning) {
      try {
        const options: { limit: number; before?: string } = { limit: pageLimit };
        if (beforeSignature) options.before = beforeSignature;

        const batch = await this.connection.getSignaturesForAddress(this.programId, options);

        if (batch.length === 0) {
          this.stopSlotPaginationContinuation = null;
          this.stopSlotPaginationBefore = beforeSignature ?? null;
          this.stopSlotPaginationBeforeResolved = true;
          logger.info(
            {
              stopSlot: this.stopSlot.toString(),
              beforeSignature: this.stopSlotPaginationBefore,
            },
            "Resolved empty pagination anchor for INDEXER_STOP_SLOT window"
          );
          return this.stopSlotPaginationBefore;
        }

        const firstWithinWindow = batch.findIndex((sig) => BigInt(sig.slot) <= this.stopSlot!);
        if (firstWithinWindow >= 0) {
          const anchor = firstWithinWindow === 0 ? null : batch[firstWithinWindow - 1].signature;
          this.stopSlotPaginationContinuation = null;
          this.stopSlotPaginationBefore = anchor;
          this.stopSlotPaginationBeforeResolved = true;
          logger.info(
            {
              stopSlot: this.stopSlot.toString(),
              beforeSignature: anchor,
            },
            anchor
              ? "Resolved pagination anchor just above INDEXER_STOP_SLOT"
              : "Current pagination tip is already within INDEXER_STOP_SLOT window"
          );
          return anchor;
        }

        beforeSignature = batch[batch.length - 1]?.signature;
        this.stopSlotPaginationContinuation = beforeSignature ?? null;
        if (!beforeSignature) {
          this.stopSlotPaginationContinuation = null;
          this.stopSlotPaginationBefore = null;
          this.stopSlotPaginationBeforeResolved = true;
          return null;
        }

        retryCount = 0;
        minPageLimit = defaultMinPageLimit;
        pageLimit = config.historicalScanSignaturePageLimit;

        if (batch.length >= pageLimit) {
          if (!(await this.waitForDelay(100))) break;
        } else {
          this.stopSlotPaginationContinuation = null;
          this.stopSlotPaginationBefore = beforeSignature;
          this.stopSlotPaginationBeforeResolved = true;
          logger.info(
            {
              stopSlot: this.stopSlot.toString(),
              beforeSignature,
            },
            "Resolved pagination anchor after exhausting signatures above INDEXER_STOP_SLOT"
          );
          return beforeSignature;
        }
      } catch (error) {
        retryCount++;
        if (isProviderOverloadedError(error)) {
          minPageLimit = 1;
        }
        pageLimit = Math.max(minPageLimit, Math.floor(pageLimit / 2));
        logger.warn(
          {
            retryCount,
            pageLimit,
            beforeSignature,
            stopSlot: this.stopSlot.toString(),
            err: error instanceof Error ? error : undefined,
            error: toErrorDetails(error),
          },
          "Error while resolving pagination anchor for INDEXER_STOP_SLOT"
        );

        if (retryCount >= 3) {
          logger.warn(
            {
              beforeSignature,
              stopSlot: this.stopSlot.toString(),
              err: error instanceof Error ? error : undefined,
              error: toErrorDetails(error),
            },
            "Deferring stop-slot anchor resolution to the next cycle after repeated failures"
          );
          this.stopSlotPaginationContinuation = beforeSignature ?? this.stopSlotPaginationContinuation;
          return null;
        }

        const retryDelayMs = isProviderOverloadedError(error)
          ? Math.min(30000, 1000 * (2 ** retryCount))
          : 500 * retryCount;
        if (!(await this.waitForDelay(retryDelayMs))) break;
      }
    }

    return null;
  }

  async start(): Promise<void> {
    if (this.isRunning) {
      logger.warn("Poller already running");
      return;
    }

    this.isRunning = true;
    this.hasReachedLiveFrontier = false;
    logger.info({ programId: this.programId.toBase58() }, "Starting poller");

    try {
      await this.initializeCursor();
      const runPromise = this.poll().finally(() => {
        if (this.runPromise === runPromise) {
          this.runPromise = null;
        }
      });
      this.runPromise = runPromise;
    } catch (error) {
      this.isRunning = false;
      throw error;
    }
  }

  async bootstrap(options?: PollerBootstrapOptions): Promise<void> {
    if (this.isRunning) {
      logger.warn("Poller already running");
      return;
    }

    this.isRunning = true;
    this.hasReachedLiveFrontier = false;
    this.suppressEventLogWrites = Boolean(options?.suppressEventLogWrites);
    logger.info({ programId: this.programId.toBase58() }, "Bootstrapping poller catch-up");

    const runPromise = (async () => {
      await this.initializeCursor();

      while (this.isRunning) {
        const result = await this.processNewTransactions();
        if (result.haltedOnError) {
          const shouldRetry = await this.waitForDelay(options?.retryDelayMs ?? this.pollingInterval);
          if (!shouldRetry) break;
          continue;
        }

        if (result.fetchedCount === 0 && !this.pendingContinuation) {
          break;
        }
      }
    })().finally(() => {
      this.isRunning = false;
      this.pendingContinuation = null;
      this.pendingStopSignature = null;
      this.hadPaginationPartial = false;
      this.suppressEventLogWrites = false;
      if (this.runPromise === runPromise) {
        this.runPromise = null;
      }
    });

    this.runPromise = runPromise;
    await runPromise;
  }

  private async initializeCursor(): Promise<void> {
    while (this.isRunning) {
      await this.loadState();
      const activeHistoricalScan = await this.loadActiveHistoricalScanState(this.lastSignature);

      if (activeHistoricalScan) {
        const historicalCatchUpComplete = await this.catchUpHistoricalGap(
          activeHistoricalScan.stopSignature,
          activeHistoricalScan.pages
        );
        if (historicalCatchUpComplete) {
          this.pendingHistoricalResume = false;
          return;
        }
        this.pendingHistoricalResume = true;
      } else if (!this.lastSignature) {
        logger.info("No saved state - starting backfill from beginning");
        const backfillComplete = await this.backfill();
        if (backfillComplete) {
          this.pendingHistoricalResume = false;
          return;
        }
        this.pendingHistoricalResume = true;
      } else if (config.indexerStartSignature || this.pendingHistoricalResume) {
        const historicalCatchUpComplete = await this.catchUpHistoricalGap();
        if (historicalCatchUpComplete) {
          this.pendingHistoricalResume = false;
          return;
        }
        this.pendingHistoricalResume = true;
      } else {
        return;
      }

      const shouldRetry = await this.waitForDelay(this.pollingInterval);
      if (!shouldRetry) {
        return;
      }
    }
  }

  /**
   * Backfill all historical transactions from the program
   * Uses streaming approach to avoid OOM - fetches and processes in batches
   * Processes oldest-to-newest within each batch for correct ordering
   */
  private async backfill(): Promise<boolean> {
    logger.info("Starting historical backfill with streaming batches...");

    // First, find the oldest signature by paginating to the end
    // We need to process oldest-first, so we collect checkpoints
    const checkpoints: string[] = [];
    let beforeSignature: string | undefined = undefined;
    let totalEstimate = 0;
    let scanErrors = 0;

    // Phase 1: Collect checkpoint signatures (one per ~1000 txs) to avoid loading all in memory
    logger.info("Phase 1: Scanning for oldest transactions...");
    while (this.isRunning) {
      try {
        const signatures = await this.connection.getSignaturesForAddress(
          this.programId,
          { limit: config.historicalScanSignaturePageLimit, before: beforeSignature }
        );

        if (signatures.length === 0) break;

        const validSigs = signatures.filter((sig) => sig.err === null);
        totalEstimate += validSigs.length;

        const slotAwareCheckpoint = await this.resolveBackfillCheckpoint(signatures);
        totalEstimate += slotAwareCheckpoint.extraValidCount;

        if (slotAwareCheckpoint.checkpointSignature) {
          checkpoints.push(slotAwareCheckpoint.checkpointSignature);
        }

        beforeSignature = slotAwareCheckpoint.nextBeforeSignature;

        if (signatures.length < this.batchSize) break;

        if (!(await this.waitForDelay(100))) break;

        if (totalEstimate % 5000 === 0) {
          logger.info({ scanned: totalEstimate, checkpoints: checkpoints.length }, "Scanning progress...");
        }
      } catch (error) {
        scanErrors++;
        logger.error({ error, scanErrors, beforeSignature }, "Error during backfill scan");

        // Retry with exponential backoff
        if (scanErrors >= 5) {
          logger.error(
            { beforeSignature, scanErrors },
            "Too many scan errors during backfill scan; pausing startup catch-up and retrying later"
          );
          return false;
        }
        if (!(await this.waitForDelay(1000 * scanErrors))) break;
      }
    }

    logger.info({ totalEstimate, checkpoints: checkpoints.length }, "Phase 1 complete, starting Phase 2: processing oldest-first");

    // Phase 2: Process from oldest to newest using checkpoints in reverse
    // Process checkpoint windows from oldest (last checkpoint) to newest (first checkpoint)
    let processed = 0;

    for (let i = checkpoints.length - 1; i >= 0 && this.isRunning; i--) {
      const afterSig = checkpoints[i];
      const untilSig = i > 0 ? checkpoints[i - 1] : undefined;

      // Fetch signatures in this window (afterSig to untilSig)
      const windowSigs = await this.fetchSignatureWindow(afterSig, untilSig);

      if (windowSigs.length === 0) continue;

      // Process this batch (already in chronological order)
      try {
        processed += await this.processSignatureBatch(windowSigs, processed, totalEstimate);
      } catch (error) {
        if (isRetryableTransactionFetchError(error)) {
          logger.warn(
            {
              signature: error.signature,
              slot: error.slot,
              processed,
              total: totalEstimate,
            },
            "Backfill paused on retryable transaction fetch gap; will resume from persisted cursor"
          );
          return false;
        }
        throw error;
      }

      logger.info({ processed, total: totalEstimate, checkpoint: i }, "Backfill checkpoint processed");
    }

    // Phase 3: Process any remaining newest transactions (before first checkpoint)
    if (checkpoints.length > 0 && this.isRunning) {
      const newestSigs = await this.fetchSignatureWindow(undefined, checkpoints[0]);
      if (newestSigs.length > 0) {
        try {
          processed += await this.processSignatureBatch(newestSigs, processed, totalEstimate);
        } catch (error) {
          if (isRetryableTransactionFetchError(error)) {
            logger.warn(
              {
                signature: error.signature,
                slot: error.slot,
                processed,
                total: totalEstimate,
              },
              "Backfill paused on retryable transaction fetch gap; will resume from persisted cursor"
            );
            return false;
          }
          throw error;
        }
      }
    }

    logger.info({ processed }, "Backfill finished, switching to live polling");
    return true;
  }

  /**
   * Catch up from an existing cursor using a bounded historical scan.
   * This avoids reusing the live newest-first paginator for large historical gaps.
   */
  private async catchUpHistoricalGap(
    stopSignatureOverride?: string,
    persistedPagesOverride?: HistoricalScanPageState[]
  ): Promise<boolean> {
    const stopSignature = stopSignatureOverride ?? this.lastSignature;
    if (!stopSignature) return true;

    const persistedPages = persistedPagesOverride ?? (await this.loadHistoricalScanPages(stopSignature));
    const pageStates: HistoricalScanPageState[] = [...persistedPages];
    const persistedScanState = await this.loadHistoricalScanState(stopSignature);
    let scanComplete = persistedScanState?.scanComplete ?? false;
    let beforeSignature: string | undefined =
      persistedScanState?.nextBeforeSignature ?? undefined;

    if (!persistedScanState && pageStates.length > 0) {
      const lastPersistedPage = pageStates[pageStates.length - 1];
      beforeSignature = lastPersistedPage.nextBeforeSignature ?? undefined;
      scanComplete = beforeSignature === undefined;
    }

    if (!persistedScanState && pageStates.length === 0 && this.stopSlot !== null) {
      beforeSignature = (await this.resolveStopSlotPaginationBefore()) || undefined;
      if (!beforeSignature && !this.stopSlotPaginationBeforeResolved) {
        logger.info(
          {
            stopSlot: this.stopSlot.toString(),
            continuationPoint: this.stopSlotPaginationContinuation,
          },
          "Historical catch-up is waiting for stop-slot pagination anchor"
        );
        return false;
      }
    }

    if (pageStates.length > 0 || persistedScanState) {
      logger.info(
        {
          stopSignature,
          persistedPages: pageStates.length,
          scanComplete,
          resumeBeforeSignature: beforeSignature ?? null,
        },
        scanComplete
          ? "Resuming fully scanned historical catch-up from persisted state"
          : "Resuming bounded historical catch-up from persisted scan state"
      );
    }

    logger.info(
      {
        stopSignature,
        stopSlot: this.stopSlot?.toString() ?? null,
      },
      "Starting bounded historical catch-up from existing cursor"
    );

    const maxPagesPerPass = Math.max(1, config.historicalScanMaxPagesPerPass);
    let totalEstimate = 0;

    if (!scanComplete) {
      let pagesScannedThisPass = 0;
      while (this.isRunning && pagesScannedThisPass < maxPagesPerPass) {
        const pageInputBefore = beforeSignature;
        const page = await this.fetchHistoricalGapPage(stopSignature, pageInputBefore);
        if (page.paused) {
          await this.saveHistoricalScanState(stopSignature, pageInputBefore ?? null, false);
          return false;
        }
        if (page.signatures.length === 0) {
          if (page.pageFilteredOut && page.nextBeforeSignature) {
            pagesScannedThisPass += 1;
            beforeSignature = page.nextBeforeSignature;
            if (!(await this.waitForDelay(100))) break;
            continue;
          }
          scanComplete = true;
          beforeSignature = undefined;
          break;
        }

        await this.saveHistoricalScanPage(
          stopSignature,
          pageStates.length,
          pageInputBefore ?? null,
          page.nextBeforeSignature ?? null,
          page.signatures
        );

        pageStates.push({
          pageIndex: pageStates.length,
          beforeSignature: pageInputBefore ?? null,
          nextBeforeSignature: page.nextBeforeSignature ?? null,
          signatures: page.signatures,
        });
        totalEstimate += page.signatures.length;
        pagesScannedThisPass += 1;

        if (!page.nextBeforeSignature) {
          scanComplete = true;
          beforeSignature = undefined;
          break;
        }
        beforeSignature = page.nextBeforeSignature;

        if (!(await this.waitForDelay(100))) break;
      }

      await this.saveHistoricalScanState(stopSignature, beforeSignature ?? null, scanComplete);
      if (!scanComplete) {
        logger.info(
          {
            stopSignature,
            scannedPagesThisPass: Math.min(maxPagesPerPass, pageStates.length),
            persistedPages: pageStates.length,
            nextBeforeSignature: beforeSignature ?? null,
          },
          "Historical catch-up scan pass complete; replay will continue after scan reaches a safe boundary"
        );
        return false;
      }
    }

    if (pageStates.length === 0) {
      await this.clearHistoricalScanState(stopSignature);
      logger.info({ stopSignature }, "No historical gap detected from existing cursor");
      return true;
    }

    let processed = 0;
    const progressState = await this.loadHistoricalScanProgress(stopSignature);
    if (progressState?.nextPageIndex !== undefined && progressState.nextPageIndex < 0) {
      await this.clearHistoricalScanPages(stopSignature);
      await this.clearHistoricalScanProgress(stopSignature);
      await this.clearHistoricalScanState(stopSignature);
      logger.info({ stopSignature }, "Historical catch-up namespace already fully replayed; cleared stale persisted pages");
      return true;
    }
    const startPageIndex = progressState?.nextPageIndex ?? (pageStates.length - 1);
    const lastPageIndexToReplay = Math.max(-1, startPageIndex - maxPagesPerPass);
    for (let i = startPageIndex; i > lastPageIndexToReplay && this.isRunning; i--) {
      const pageState = pageStates[i];
      const persistedChronological =
        pageState.signatures && pageState.signatures.length > 0
          ? [...pageState.signatures].reverse()
          : null;
      let chronological = persistedChronological;
      if (!chronological) {
        const page = await this.fetchHistoricalGapPage(
          stopSignature,
          pageState.beforeSignature ?? undefined
        );
        if (page.signatures.length === 0) {
          throw new Error(
            `Historical catch-up page replay returned no signatures for persisted page ${pageState.pageIndex}`
          );
        }
        chronological = [...page.signatures].reverse();
      }
      const resumeAfterSignature =
        progressState?.nextPageIndex === i ? progressState.lastProcessedSignature : null;
      const resumeOffset = resumeAfterSignature
        ? chronological.findIndex((sig) => sig.signature === resumeAfterSignature)
        : -1;
      const replayBatch =
        resumeOffset >= 0 ? chronological.slice(resumeOffset + 1) : chronological;
      let lastReplayedSignature: string | null = null;
      try {
        const replayedCount = await this.processSignatureBatch(
          replayBatch,
          processed,
          totalEstimate,
          async (sig) => {
            lastReplayedSignature = sig.signature;
            if (!USE_BATCH_DB) {
              await this.saveHistoricalScanProgress(stopSignature, i, sig.signature);
            }
          }
        );
        processed += replayedCount;
        if (replayedCount < replayBatch.length) {
          if (USE_BATCH_DB && lastReplayedSignature) {
            await this.saveHistoricalScanProgress(stopSignature, i, lastReplayedSignature);
          }
          logger.info(
            {
              processed,
              replayedCount,
              replayBatchSize: replayBatch.length,
              stopSignature,
              pageIndex: i,
            },
            "Historical catch-up page replay paused before page completion; preserving page for resume"
          );
          return false;
        }
      } catch (error) {
        if (isRetryableTransactionFetchError(error)) {
          if (USE_BATCH_DB && lastReplayedSignature) {
            await this.saveHistoricalScanProgress(stopSignature, i, lastReplayedSignature);
          }
          logger.warn(
            {
              signature: error.signature,
              slot: error.slot,
              processed,
              total: totalEstimate,
            },
            "Historical catch-up paused on retryable transaction fetch gap; will resume from persisted cursor"
          );
          return false;
        }
        throw error;
      }
      await this.saveHistoricalScanProgress(stopSignature, i - 1, null);
      await this.clearHistoricalScanPage(stopSignature, pageState.pageIndex);
      logger.info(
        {
          processed,
          total: totalEstimate,
          page: pageStates.length - i,
          pageCount: pageStates.length,
        },
        "Historical catch-up page processed"
      );
    }

    if (lastPageIndexToReplay >= 0) {
      logger.info(
        {
          processed,
          total: totalEstimate,
          nextPageIndex: lastPageIndexToReplay,
          stopSignature,
        },
        "Historical catch-up replay pass complete; more persisted pages remain"
      );
      return false;
    }

    await this.clearHistoricalScanProgress(stopSignature);
    await this.clearHistoricalScanState(stopSignature);
    logger.info({ processed, total: totalEstimate }, "Historical catch-up finished, switching to live polling");
    return true;
  }

  private async loadHistoricalScanPages(stopSignature: string): Promise<HistoricalScanPageState[]> {
    const prefix = historicalScanRowPrefix(stopSignature);
    if (!this.prisma) {
      const db = getPool();
      const result = await db.query(
        `SELECT id, last_signature, source, last_tx_index
           FROM indexer_state
          WHERE id LIKE $1
          ORDER BY last_tx_index ASC NULLS LAST, id ASC`,
        [`${prefix}%`]
      );
      return result.rows
        .map((row) => {
          const pageIndex = parseHistoricalScanPageIndex(String(row.id), stopSignature);
          if (pageIndex === null) return null;
          return {
            pageIndex,
            beforeSignature: row.last_signature ?? null,
            ...parseHistoricalScanPagePayload(row.source ? String(row.source) : null),
          };
        })
        .filter((row): row is HistoricalScanPageState => row !== null);
    }

    const rows = await this.prisma.indexerState.findMany({
      where: {
        id: {
          startsWith: prefix,
        },
      },
      orderBy: {
        lastTxIndex: "asc",
      },
    });

    return rows
      .map((row) => {
        const pageIndex = parseHistoricalScanPageIndex(row.id, stopSignature);
        if (pageIndex === null) return null;
        return {
          pageIndex,
          beforeSignature: row.lastSignature ?? null,
          ...parseHistoricalScanPagePayload(row.source ?? null),
        };
      })
      .filter((row): row is HistoricalScanPageState => row !== null);
  }

  private async loadHistoricalScanState(stopSignature: string): Promise<HistoricalScanState | null> {
    const id = historicalScanStateRowId(stopSignature);
    if (!this.prisma) {
      const db = getPool();
      const result = await db.query(
        `SELECT source
           FROM indexer_state
          WHERE id = $1
          LIMIT 1`,
        [id]
      );
      const source = result.rows[0]?.source;
      if (source === null || source === undefined) {
        return null;
      }
      return parseHistoricalScanStatePayload(String(source));
    }

    const row = await this.prisma.indexerState.findUnique({
      where: { id },
      select: { source: true },
    });
    if (!row?.source) {
      return null;
    }
    return parseHistoricalScanStatePayload(row.source);
  }

  private async loadActiveHistoricalScanState(
    currentStopSignature?: string | null
  ): Promise<{ stopSignature: string; pages: HistoricalScanPageState[] } | null> {
    if (currentStopSignature) {
      const currentState = await this.loadHistoricalScanState(currentStopSignature);
      const currentPages = await this.filterHistoricalScanPagesByProgress(
        currentStopSignature,
        await this.loadHistoricalScanPages(currentStopSignature)
      );
      if (currentPages.length > 0 || currentState) {
        return { stopSignature: currentStopSignature, pages: currentPages };
      }
      return null;
    }

    if (!this.prisma) {
      const db = getPool();
      const result = await db.query(
        `SELECT id
           FROM indexer_state
          WHERE id LIKE $1
          ORDER BY updated_at DESC, last_tx_index DESC NULLS LAST, id DESC
          LIMIT 1`,
        [`${HISTORICAL_SCAN_ROW_PREFIX}%`]
      );
      const stopSignature = result.rows[0]?.id
        ? parseHistoricalScanStopSignature(String(result.rows[0].id))
        : null;
      if (stopSignature) {
        return {
          stopSignature,
          pages: await this.filterHistoricalScanPagesByProgress(
            stopSignature,
            await this.loadHistoricalScanPages(stopSignature)
          ),
        };
      }
      const stateResult = await db.query(
        `SELECT id
           FROM indexer_state
          WHERE id LIKE $1
          ORDER BY updated_at DESC, id DESC
          LIMIT 1`,
        [`${HISTORICAL_SCAN_STATE_ROW_PREFIX}%`]
      );
      const stateStopSignature = stateResult.rows[0]?.id
        ? parseHistoricalScanStateStopSignature(String(stateResult.rows[0].id))
        : null;
      if (!stateStopSignature) {
        return null;
      }
      return {
        stopSignature: stateStopSignature,
        pages: await this.filterHistoricalScanPagesByProgress(
          stateStopSignature,
          await this.loadHistoricalScanPages(stateStopSignature)
        ),
      };
    }

    const rows = await this.prisma.indexerState.findMany({
      where: {
        id: {
          startsWith: HISTORICAL_SCAN_ROW_PREFIX,
        },
      },
      orderBy: [
        { updatedAt: "desc" },
        { lastTxIndex: "desc" },
        { id: "desc" },
      ],
      take: 1,
    });
    const stopSignature = rows[0]?.id ? parseHistoricalScanStopSignature(rows[0].id) : null;
    if (stopSignature) {
      return {
        stopSignature,
        pages: await this.filterHistoricalScanPagesByProgress(
          stopSignature,
          await this.loadHistoricalScanPages(stopSignature)
        ),
      };
    }
    const stateRows = await this.prisma.indexerState.findMany({
      where: {
        id: {
          startsWith: HISTORICAL_SCAN_STATE_ROW_PREFIX,
        },
      },
      orderBy: [
        { updatedAt: "desc" },
        { id: "desc" },
      ],
      take: 1,
    });
    const stateStopSignature = stateRows[0]?.id
      ? parseHistoricalScanStateStopSignature(stateRows[0].id)
      : null;
    if (!stateStopSignature) {
      return null;
    }
    return {
      stopSignature: stateStopSignature,
      pages: await this.filterHistoricalScanPagesByProgress(
        stateStopSignature,
        await this.loadHistoricalScanPages(stateStopSignature)
      ),
    };
  }

  private async filterHistoricalScanPagesByProgress(
    stopSignature: string,
    pages: HistoricalScanPageState[]
  ): Promise<HistoricalScanPageState[]> {
    const progress = await this.loadHistoricalScanProgress(stopSignature);
    if (!progress) return pages;
    return pages.filter((page) => page.pageIndex <= progress.nextPageIndex);
  }

  private async saveHistoricalScanPage(
    stopSignature: string,
    pageIndex: number,
    beforeSignature: string | null,
    nextBeforeSignature: string | null,
    signatures: ConfirmedSignatureInfo[]
  ): Promise<void> {
    const id = historicalScanRowId(stopSignature, pageIndex);
    const serializedSource = serializeHistoricalScanPagePayload(nextBeforeSignature, signatures);

    if (!this.prisma) {
      const db = getPool();
      await db.query(
        `INSERT INTO indexer_state (id, last_signature, last_slot, last_tx_index, source, updated_at)
         VALUES ($1, $2, NULL, $3, $4, $5)
         ON CONFLICT (id) DO UPDATE SET
           last_signature = EXCLUDED.last_signature,
           last_slot = NULL,
           last_tx_index = EXCLUDED.last_tx_index,
           source = EXCLUDED.source,
           updated_at = EXCLUDED.updated_at`,
        [
          id,
          beforeSignature,
          pageIndex,
          serializedSource,
          new Date().toISOString(),
        ]
      );
      return;
    }

    await this.prisma.indexerState.upsert({
      where: { id },
      create: {
        id,
        lastSignature: beforeSignature,
        lastSlot: null,
        lastTxIndex: pageIndex,
        source: serializedSource,
      },
      update: {
        lastSignature: beforeSignature,
        lastSlot: null,
        lastTxIndex: pageIndex,
        source: serializedSource,
      },
    });
  }

  private async saveHistoricalScanState(
    stopSignature: string,
    nextBeforeSignature: string | null,
    scanComplete: boolean
  ): Promise<void> {
    const id = historicalScanStateRowId(stopSignature);
    const serializedSource = serializeHistoricalScanStatePayload(
      nextBeforeSignature,
      scanComplete
    );

    if (!this.prisma) {
      const db = getPool();
      await db.query(
        `INSERT INTO indexer_state (id, last_signature, last_slot, last_tx_index, source, updated_at)
         VALUES ($1, NULL, NULL, NULL, $2, $3)
         ON CONFLICT (id) DO UPDATE SET
           last_signature = EXCLUDED.last_signature,
           last_slot = EXCLUDED.last_slot,
           last_tx_index = EXCLUDED.last_tx_index,
           source = EXCLUDED.source,
           updated_at = EXCLUDED.updated_at`,
        [id, serializedSource, new Date().toISOString()]
      );
      return;
    }

    await this.prisma.indexerState.upsert({
      where: { id },
      create: {
        id,
        lastSignature: null,
        lastSlot: null,
        lastTxIndex: null,
        source: serializedSource,
      },
      update: {
        lastSignature: null,
        lastSlot: null,
        lastTxIndex: null,
        source: serializedSource,
      },
    });
  }

  private async clearHistoricalScanPage(stopSignature: string, pageIndex: number): Promise<void> {
    const id = historicalScanRowId(stopSignature, pageIndex);
    if (!this.prisma) {
      const db = getPool();
      await db.query(`DELETE FROM indexer_state WHERE id = $1`, [id]);
      return;
    }

    await this.prisma.indexerState.deleteMany({
      where: { id },
    });
  }

  private async clearHistoricalScanPages(stopSignature: string): Promise<void> {
    const prefix = `${HISTORICAL_SCAN_ROW_PREFIX}${stopSignature}:`;
    if (!this.prisma) {
      const db = getPool();
      await db.query(`DELETE FROM indexer_state WHERE id LIKE $1`, [`${prefix}%`]);
      return;
    }

    await this.prisma.indexerState.deleteMany({
      where: {
        id: {
          startsWith: prefix,
        },
      },
    });
  }

  private async clearHistoricalScanState(stopSignature: string): Promise<void> {
    const id = historicalScanStateRowId(stopSignature);
    if (!this.prisma) {
      const db = getPool();
      await db.query(`DELETE FROM indexer_state WHERE id = $1`, [id]);
      return;
    }

    await this.prisma.indexerState.deleteMany({
      where: { id },
    });
  }

  private async loadHistoricalScanProgress(
    stopSignature: string
  ): Promise<{ nextPageIndex: number; lastProcessedSignature: string | null } | null> {
    const id = historicalScanProgressRowId(stopSignature);
    if (!this.prisma) {
      const db = getPool();
      const result = await db.query(
        `SELECT last_tx_index, last_signature
           FROM indexer_state
          WHERE id = $1
          LIMIT 1`,
        [id]
      );
      const nextPageIndex = result.rows[0]?.last_tx_index;
      if (nextPageIndex === null || nextPageIndex === undefined) {
        return null;
      }
      return {
        nextPageIndex: Number(nextPageIndex),
        lastProcessedSignature: result.rows[0]?.last_signature ?? null,
      };
    }

    const row = await this.prisma.indexerState.findUnique({
      where: { id },
      select: { lastTxIndex: true, lastSignature: true },
    });
    if (row?.lastTxIndex === null || row?.lastTxIndex === undefined) {
      return null;
    }
    return {
      nextPageIndex: row.lastTxIndex,
      lastProcessedSignature: row.lastSignature ?? null,
    };
  }

  private async saveHistoricalScanProgress(
    stopSignature: string,
    nextPageIndex: number,
    lastProcessedSignature: string | null = null
  ): Promise<void> {
    const id = historicalScanProgressRowId(stopSignature);
    if (!this.prisma) {
      const db = getPool();
      await db.query(
        `INSERT INTO indexer_state (id, last_signature, last_slot, last_tx_index, source, updated_at)
         VALUES ($1, $2, NULL, $3, NULL, $4)
         ON CONFLICT (id) DO UPDATE SET
           last_signature = EXCLUDED.last_signature,
           last_slot = EXCLUDED.last_slot,
           last_tx_index = EXCLUDED.last_tx_index,
           source = EXCLUDED.source,
           updated_at = EXCLUDED.updated_at`,
        [id, lastProcessedSignature, nextPageIndex, new Date().toISOString()]
      );
      return;
    }

    await this.prisma.indexerState.upsert({
      where: { id },
      create: {
        id,
        lastSignature: lastProcessedSignature,
        lastSlot: null,
        lastTxIndex: nextPageIndex,
        source: "historical-scan-progress",
      },
      update: {
        lastSignature: lastProcessedSignature,
        lastSlot: null,
        lastTxIndex: nextPageIndex,
        source: "historical-scan-progress",
      },
    });
  }

  private async clearHistoricalScanProgress(stopSignature: string): Promise<void> {
    const id = historicalScanProgressRowId(stopSignature);
    if (!this.prisma) {
      const db = getPool();
      await db.query(`DELETE FROM indexer_state WHERE id = $1`, [id]);
      return;
    }

    await this.prisma.indexerState.deleteMany({
      where: { id },
    });
  }

  private async fetchHistoricalGapPage(
    stopSignature: string,
    beforeSignature?: string
  ): Promise<{
    signatures: ConfirmedSignatureInfo[];
    nextBeforeSignature: string | undefined;
    paused?: boolean;
    pageFilteredOut?: boolean;
  }> {
    let retryCount = 0;
    let pageLimit = config.historicalScanSignaturePageLimit;
    const defaultMinPageLimit = Math.max(1, Math.min(25, config.historicalScanSignaturePageLimit));
    let minPageLimit = defaultMinPageLimit;

    while (this.isRunning) {
      try {
        const options: { limit: number; before?: string; until: string } = {
          limit: pageLimit,
          until: stopSignature,
        };
        if (beforeSignature) {
          options.before = beforeSignature;
        }

        let batch = await this.connection.getSignaturesForAddress(this.programId, options);
        if (batch.length === 0) {
          return { signatures: [], nextBeforeSignature: undefined };
        }

        const stopIndex = batch.findIndex((sig) => sig.signature === stopSignature);
        if (stopIndex >= 0) {
          batch = batch.slice(0, stopIndex);
        }

        const page: ConfirmedSignatureInfo[] = [];
        const seen = new Set<string>();
        const pushUniqueSignature = (entry: ConfirmedSignatureInfo): void => {
          if (entry.err !== null) return;
          if (entry.signature === stopSignature) return;
          if (seen.has(entry.signature)) return;
          seen.add(entry.signature);
          page.push(entry);
        };

        for (const entry of batch) {
          pushUniqueSignature(entry);
        }

        if (batch.length === 0) {
          return { signatures: page, nextBeforeSignature: undefined };
        }

        let nextBeforeSignature = batch[batch.length - 1].signature;
        const boundarySlot = batch[batch.length - 1].slot;

        if (batch.length >= pageLimit) {
          while (this.isRunning && nextBeforeSignature) {
            const continuation = await this.connection.getSignaturesForAddress(
              this.programId,
              { limit: pageLimit, before: nextBeforeSignature, until: stopSignature }
            );

            if (continuation.length === 0) break;

            const continuationStopIndex = continuation.findIndex(
              (sig) => sig.signature === stopSignature
            );
            const effectiveContinuation = continuationStopIndex >= 0
              ? continuation.slice(0, continuationStopIndex)
              : continuation;

            let sawBoundarySlot = false;
            for (const candidate of effectiveContinuation) {
              if (candidate.slot !== boundarySlot) break;
              sawBoundarySlot = true;
              nextBeforeSignature = candidate.signature;
              pushUniqueSignature(candidate);
            }

            if (!sawBoundarySlot) break;
            if (continuationStopIndex >= 0) break;
            if (continuation.length < pageLimit) break;
            if (!(await this.waitForDelay(100))) break;
          }
        }

        return {
          signatures: page,
          nextBeforeSignature:
            batch.length < pageLimit || stopIndex >= 0 ? undefined : nextBeforeSignature,
          pageFilteredOut: page.length === 0 && batch.length > 0,
        };
      } catch (error) {
        retryCount++;
        if (isProviderOverloadedError(error)) {
          minPageLimit = 1;
        }
        pageLimit = Math.max(minPageLimit, Math.floor(pageLimit / 2));
        logger.warn(
          {
            retryCount,
            pageLimit,
            beforeSignature,
            stopSignature,
            err: error instanceof Error ? error : undefined,
            error: toErrorDetails(error),
          },
          "Error while scanning bounded historical catch-up page"
        );

        if (retryCount >= 5) {
          logger.error(
            {
              beforeSignature,
              stopSignature,
              err: error instanceof Error ? error : undefined,
              error: toErrorDetails(error),
            },
            "Historical catch-up page scan hit repeated RPC errors; pausing startup catch-up and retrying later"
          );
          return {
            signatures: [],
            nextBeforeSignature: beforeSignature,
            paused: true,
          };
        }

        const retryDelayMs = isProviderOverloadedError(error)
          ? Math.min(30000, 1000 * (2 ** retryCount))
          : 500 * retryCount;
        if (!(await this.waitForDelay(retryDelayMs))) break;
      }
    }

    return { signatures: [], nextBeforeSignature: undefined };
  }

  private async resolveBackfillCheckpoint(
    signatures: ConfirmedSignatureInfo[]
  ): Promise<{
    checkpointSignature: string | null;
    nextBeforeSignature: string | undefined;
    extraValidCount: number;
  }> {
    if (signatures.length === 0) {
      return {
        checkpointSignature: null,
        nextBeforeSignature: undefined,
        extraValidCount: 0,
      };
    }

    const boundarySlot = signatures[signatures.length - 1].slot;
    let checkpointSignature: string | null = null;
    for (let i = signatures.length - 1; i >= 0; i--) {
      const candidate = signatures[i];
      if (candidate.slot !== boundarySlot) break;
      if (candidate.err === null) {
        checkpointSignature = candidate.signature;
      }
    }

    let nextBeforeSignature = signatures[signatures.length - 1].signature;
    let extraValidCount = 0;

      if (signatures.length < config.historicalScanSignaturePageLimit) {
        return { checkpointSignature, nextBeforeSignature, extraValidCount };
      }

    while (this.isRunning && nextBeforeSignature) {
      const continuation = await this.connection.getSignaturesForAddress(
        this.programId,
        { limit: config.historicalScanSignaturePageLimit, before: nextBeforeSignature }
      );

      if (continuation.length === 0) break;

      let consumedBoundarySlot = false;
      for (const candidate of continuation) {
        if (candidate.slot !== boundarySlot) {
          return { checkpointSignature, nextBeforeSignature, extraValidCount };
        }
        consumedBoundarySlot = true;
        nextBeforeSignature = candidate.signature;
        if (candidate.err === null) {
          checkpointSignature = candidate.signature;
          extraValidCount++;
        }
      }

      if (!consumedBoundarySlot || continuation.length < config.historicalScanSignaturePageLimit) break;
      if (!(await this.waitForDelay(100))) break;
    }

    return { checkpointSignature, nextBeforeSignature, extraValidCount };
  }

  /**
   * Fetch signatures in a window (from afterSig to untilSig)
   * Returns signatures in chronological order (oldest first)
   */
  private async fetchSignatureWindow(
    afterSig: string | undefined,
    untilSig: string | undefined
  ): Promise<ConfirmedSignatureInfo[]> {
    const windowSigs: ConfirmedSignatureInfo[] = [];
    let beforeSig: string | undefined = untilSig;
    let retryCount = 0;

    while (this.isRunning) {
      try {
        const options: { limit: number; before?: string; until?: string } = {
          limit: this.batchSize,
        };
        if (beforeSig) options.before = beforeSig;
        if (afterSig) options.until = afterSig;

        const batch = await this.connection.getSignaturesForAddress(
          this.programId,
          options
        );

        if (batch.length === 0) break;

        const validBatch = batch.filter((sig) => sig.err === null);
        windowSigs.push(...validBatch);

        beforeSig = batch[batch.length - 1].signature;

        if (batch.length < this.batchSize) break;

        if (!(await this.waitForDelay(50))) break;
        retryCount = 0; // Reset on success
      } catch (error) {
        retryCount++;
        logger.warn({ error, retryCount, windowSize: windowSigs.length }, "Error fetching signature window");

        if (retryCount >= 3) {
          this.isRunning = false;
          logger.fatal({ windowSize: windowSigs.length }, "Too many errors fetching signature window; aborting backfill to avoid partial history gaps");
          throw new Error("Backfill window fetch failed after repeated RPC errors");
        }
        if (!(await this.waitForDelay(500 * retryCount))) break;
      }
    }

    // Reverse to get chronological order (oldest first)
    return windowSigs.reverse();
  }

  /**
   * Process a batch of signatures with slot grouping and tx_index resolution
   * Returns number of successfully processed transactions
   *
   * OPTIMIZATION: Uses batch RPC fetching (getParsedTransactions) in Supabase mode
   */
  private async processSignatureBatch(
    signatures: ConfirmedSignatureInfo[],
    previousCount: number,
    totalEstimate: number,
    onTransactionProcessed?: (sig: ConfirmedSignatureInfo) => Promise<void>
  ): Promise<number> {
    const startTime = Date.now();

    // BATCH RPC: Fetch all transactions in batch first (with fallback)
    let txCache: Map<string, ParsedTransactionWithMeta> | null = null;
    if (this.useBatchRpc && this.batchFetcher) {
      const sigList = signatures.map(s => s.signature);
      txCache = await this.batchFetcher.fetchTransactions(sigList);
      logger.debug({ requested: sigList.length, fetched: txCache.size }, "Batch RPC fetch complete");
    }

    // Group by slot for tx_index resolution
    const bySlot = new Map<number, ConfirmedSignatureInfo[]>();
    for (const sig of signatures) {
      if (!bySlot.has(sig.slot)) {
        bySlot.set(sig.slot, []);
      }
      bySlot.get(sig.slot)!.push(sig);
    }

    let processed = 0;
    const sortedSlots = Array.from(bySlot.keys()).sort((a, b) => a - b);

    for (const slot of sortedSlots) {
      if (!this.isRunning) break;
      if (this.stopBeforeNewerSlot(slot, "backfill")) break;

      const sigs = bySlot.get(slot)!;
      let txIndexMap: Map<string, number | null>;
      try {
        txIndexMap = await this.getTxIndexMap(slot, sigs);
        if (sigs.length > 1 && hasUnresolvedTxIndex(txIndexMap, sigs)) {
          throw new Error(`Deterministic tx_index unavailable for slot ${slot}`);
        }
      } catch (error) {
        logger.fatal(
          { slot, error: error instanceof Error ? error.message : String(error) },
          "Backfill aborted: deterministic tx_index unavailable for multi-transaction slot"
        );
        throw (error instanceof Error ? error : new Error(String(error)));
      }

      const sigsWithIndex = sigs.map((sig) => ({
        sig,
        txIndex: txIndexMap.get(sig.signature) ?? undefined
      })).sort((a, b) =>
        compareTransactionCursor(a.txIndex, a.sig.signature, b.txIndex, b.sig.signature)
      );

      for (const { sig, txIndex } of sigsWithIndex) {
        if (!this.isRunning) break;

        try {
          // Use cached transaction from batch RPC if available
          let blockTime: Date;
          if (this.useBatchRpc && txCache) {
            blockTime = await this.processTransactionBatch(sig, txIndex, txCache.get(sig.signature));
          } else {
            blockTime = await this.processTransaction(sig, txIndex);
          }
          if (USE_BATCH_DB && this.eventBuffer) {
            this.eventBuffer.noteCursor({
              signature: sig.signature,
              slot: BigInt(sig.slot),
              blockTime,
              txIndex,
            }, false);
          }
          this.lastSignature = sig.signature;
          this.lastSlot = BigInt(sig.slot);
          this.lastTxIndex = txIndex ?? null;
          // Skip individual cursor saves when using batch DB - handled by EventBuffer flush
          if (!USE_BATCH_DB) {
            await this.saveState(sig.signature, BigInt(sig.slot), txIndex ?? null, blockTime);
          }
          if (onTransactionProcessed) {
            await onTransactionProcessed(sig);
          }
          processed++;
          this.processedCount++;

          if ((previousCount + processed) % 100 === 0) {
            const elapsed = (Date.now() - startTime) / 1000;
            logger.info({
              processed: previousCount + processed,
              total: totalEstimate,
              rate: `${Math.round(processed / elapsed)} tx/s`,
              batchRpc: this.useBatchRpc,
              batchDb: USE_BATCH_DB
            }, "Backfill progress");
          }
        } catch (error) {
          this.errorCount++;
          if (isRetryableTransactionFetchError(error)) {
            if (USE_BATCH_DB && this.eventBuffer && (this.eventBuffer.size > 0 || this.eventBuffer.hasPendingCursor())) {
              await this.eventBuffer.flush();
            }
            logger.warn(
              {
                error: error.message,
                signature: error.signature,
                slot: error.slot,
              },
              "Backfill paused on retryable transaction fetch gap"
            );
            throw error;
          }
          this.isRunning = false;
          if (isMissingCollectionIdSchemaError(error)) {
            logger.fatal(
              {
                error: error instanceof Error ? error.message : String(error),
                signature: sig.signature,
                slot: sig.slot,
              },
              "Backfill aborted: missing collection_id schema in local database"
            );
            throw new Error(MISSING_COLLECTION_ID_SCHEMA_FATAL_MESSAGE);
          }
          logger.fatal({
            error: error instanceof Error ? error.message : String(error),
            signature: sig.signature,
            slot: sig.slot
          }, "Backfill aborted: transaction processing error");
          throw (error instanceof Error ? error : new Error(String(error)));
        }
      }
    }

    // BATCH DB: Flush remaining events at end of batch
    if (USE_BATCH_DB && this.eventBuffer) {
      if (!this.isRunning) {
        await this.eventBuffer.drain();
      } else if (this.eventBuffer.size > 0 || this.eventBuffer.hasPendingCursor()) {
        await this.eventBuffer.flush();
      }
    }

    return processed;
  }

  /**
   * Get transaction index within a block for the provided signatures.
   * Fetches the full block and maps signature -> absolute block index.
   * This keeps poller ordering aligned with websocket/substreams ordering.
   */
  private async getTxIndexMap(slot: number, sigs: ConfirmedSignatureInfo[]): Promise<Map<string, number | null>> {
    const txIndexMap = new Map<string, number | null>();

    const MAX_RETRIES = 3;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        const block = await this.connection.getBlock(slot, {
          maxSupportedTransactionVersion: config.maxSupportedTransactionVersion,
          transactionDetails: "full",
        });

        if (block?.transactions) {
          const sigSet = new Set(sigs.map(s => s.signature));
          block.transactions.forEach((tx, idx) => {
            const sig = tx.transaction.signatures[0];
            if (sigSet.has(sig)) {
              txIndexMap.set(sig, idx);
            }
          });
        }
        return txIndexMap;
      } catch (error) {
        if (attempt < MAX_RETRIES) {
          logger.warn({ slot, attempt, error: error instanceof Error ? error.message : String(error) }, "getBlock failed, retrying");
          const shouldRetry = await this.waitForDelay(500 * attempt);
          if (!shouldRetry) {
            sigs.forEach(sig => txIndexMap.set(sig.signature, null));
            break;
          }
        } else {
          logger.warn({ slot, sigCount: sigs.length }, "getBlock failed after retries, tx_index is unavailable");
          sigs.forEach(sig => txIndexMap.set(sig.signature, null));
        }
      }
    }

    return txIndexMap;
  }

  async stop(): Promise<void> {
    logger.info({
      processedCount: this.processedCount,
      errorCount: this.errorCount,
      lastSignature: this.lastSignature?.slice(0, 16) + '...'
    }, "Stopping poller");
    this.isRunning = false;
    this.hasReachedLiveFrontier = false;
    if (this.pollDelayTimeout) {
      clearTimeout(this.pollDelayTimeout);
      this.pollDelayTimeout = null;
    }
    if (this.pollDelayResolve) {
      const resolve = this.pollDelayResolve;
      this.pollDelayResolve = null;
      resolve(true);
    }

    const runPromise = this.runPromise;
    if (runPromise) {
      await Promise.allSettled([runPromise]);
    }

    // Flush any remaining events in batch mode
    if (this.eventBuffer) {
      if (this.eventBuffer.size > 0 || this.eventBuffer.hasPendingCursor()) {
        logger.info({ remaining: this.eventBuffer.size }, "Flushing remaining events before shutdown");
      }
      await this.eventBuffer.drain();
    }

    // Log batch stats
    if (this.batchFetcher) {
      const stats = this.batchFetcher.getStats();
      logger.info(stats, "Batch RPC fetcher stats");
    }
    if (this.eventBuffer) {
      const stats = this.eventBuffer.getStats();
      logger.info(stats, "Event buffer stats");
    }
  }

  getStats(): { processedCount: number; errorCount: number } {
    return {
      processedCount: this.processedCount,
      errorCount: this.errorCount,
    };
  }

  isCaughtUp(): boolean {
    return this.hasReachedLiveFrontier;
  }

  private logNoSavedCursorWarning(source: "local" | "supabase"): void {
    logger.warn(
      {
        source,
        rpcUrl: config.rpcUrl,
        indexerMode: config.indexerMode,
        programId: this.programId.toBase58(),
      },
      "No saved cursor found - starting RPC historical backfill from earliest available history on this endpoint. If RPC history is pruned, older transactions cannot be recovered. Use archival RPC or set INDEXER_START_SIGNATURE (+ INDEXER_START_SLOT)."
    );
  }

  private async tryApplyConfiguredStartCursor(source: "local" | "supabase"): Promise<boolean> {
    if (!config.indexerStartSignature) {
      return false;
    }

    await this.validateConfiguredStartCursor();
    this.lastSignature = config.indexerStartSignature;
    this.lastSlot = config.indexerStartSlot;
    this.lastTxIndex = null;
    logger.info(
      {
        source,
        lastSignature: this.lastSignature,
        lastSlot: config.indexerStartSlot?.toString() ?? null,
      },
      "No saved state found, bootstrapping cursor from env"
    );

    if (config.indexerStartSlot !== null) {
      await this.saveState(
        config.indexerStartSignature,
        config.indexerStartSlot,
        null,
        resolveEventBlockTime(undefined, config.indexerStartSlot)
      );
      logger.info(
        {
          source,
          lastSignature: config.indexerStartSignature,
          lastSlot: config.indexerStartSlot.toString(),
        },
        "Persisted bootstrap cursor from env"
      );
    }

    return true;
  }

  private async tryPreferConfiguredStartCursor(
    source: "local" | "supabase",
    savedSignature: string,
    savedSlot: bigint | null
  ): Promise<boolean> {
    if (!config.indexerStartSignature || config.indexerStartSlot === null || savedSlot === null) {
      return false;
    }

    if (config.indexerStartSignature === savedSignature) {
      return false;
    }

    if (savedSlot >= config.indexerStartSlot) {
      return false;
    }

    await this.validateConfiguredStartCursor();
    this.lastSignature = config.indexerStartSignature;
    this.lastSlot = config.indexerStartSlot;
    this.lastTxIndex = null;
    await this.saveState(
      config.indexerStartSignature,
      config.indexerStartSlot,
      null,
      resolveEventBlockTime(undefined, config.indexerStartSlot)
    );
    logger.warn(
      {
        source,
        savedSignature,
        savedSlot: savedSlot.toString(),
        configuredStartSignature: config.indexerStartSignature,
        configuredStartSlot: config.indexerStartSlot.toString(),
      },
      "Configured start cursor is ahead of saved state, fast-forwarding persisted cursor"
    );
    return true;
  }

  private async loadState(): Promise<void> {
    // Supabase mode - load from Supabase
    if (!this.prisma) {
      const state = await loadIndexerState();
      if (state.lastSignature) {
        this.lastSignature = state.lastSignature;
        this.lastSlot = state.lastSlot;
        this.lastTxIndex = state.lastTxIndex;
        logger.info(
          {
            lastSignature: this.lastSignature,
            lastSlot: state.lastSlot?.toString(),
            lastTxIndex: state.lastTxIndex,
          },
          "Supabase mode: resuming from signature"
        );
        const fastForwarded = await this.tryPreferConfiguredStartCursor(
          "supabase",
          state.lastSignature,
          state.lastSlot
        );
        if (fastForwarded) {
          return;
        }
        if (config.indexerStartSignature && config.indexerStartSignature !== state.lastSignature) {
          logger.info(
            {
              savedSignature: state.lastSignature,
              savedSlot: state.lastSlot?.toString() ?? null,
              configuredStartSignature: config.indexerStartSignature,
              configuredStartSlot: config.indexerStartSlot?.toString() ?? null,
            },
            "Supabase mode: ignoring INDEXER_START_SIGNATURE because saved state exists"
          );
        }
      } else {
        const bootstrapped = await this.tryApplyConfiguredStartCursor("supabase");
        if (!bootstrapped) {
          this.logNoSavedCursorWarning("supabase");
        }
      }
      return;
    }

    // Local mode - load from Prisma
    const state = await this.prisma.indexerState.findUnique({
      where: { id: "main" },
    });

    if (state?.lastSignature) {
      this.lastSignature = state.lastSignature;
      this.lastSlot = state.lastSlot ?? null;
      this.lastTxIndex = state.lastTxIndex ?? null;
      logger.info(
        {
          lastSignature: this.lastSignature,
          lastSlot: state.lastSlot?.toString() ?? null,
          lastTxIndex: state.lastTxIndex ?? null,
        },
        "Resuming from signature"
      );
      const fastForwarded = await this.tryPreferConfiguredStartCursor(
        "local",
        state.lastSignature,
        state.lastSlot
      );
      if (fastForwarded) {
        return;
      }
      if (config.indexerStartSignature && config.indexerStartSignature !== state.lastSignature) {
        logger.info(
          {
            savedSignature: state.lastSignature,
            savedSlot: state.lastSlot?.toString() ?? null,
            configuredStartSignature: config.indexerStartSignature,
            configuredStartSlot: config.indexerStartSlot?.toString() ?? null,
          },
          "Local mode: ignoring INDEXER_START_SIGNATURE because saved state exists"
        );
      }
    } else {
      const bootstrapped = await this.tryApplyConfiguredStartCursor("local");
      if (!bootstrapped) {
        this.logNoSavedCursorWarning("local");
      }
    }
  }

  private async validateConfiguredStartCursor(): Promise<void> {
    if (this.configuredStartCursorValidated) {
      return;
    }
    if (!config.indexerStartSignature || config.indexerStartSlot === null) {
      return;
    }

    const tx = await this.connection.getParsedTransaction(
      config.indexerStartSignature,
      { maxSupportedTransactionVersion: config.maxSupportedTransactionVersion }
    );
    const resolvedSlot = tx?.slot ?? null;
    if (resolvedSlot === null) {
      throw new Error(
        `Configured start cursor signature ${config.indexerStartSignature} was not found in RPC transaction history`
      );
    }
    if (BigInt(resolvedSlot) !== config.indexerStartSlot) {
      throw new Error(
        `Configured start cursor mismatch: signature ${config.indexerStartSignature} resolves to slot ${resolvedSlot}, expected ${config.indexerStartSlot.toString()}`
      );
    }

    this.configuredStartCursorValidated = true;
  }

  private async saveState(
    signature: string,
    slot: bigint,
    txIndex: number | null,
    updatedAt: Date = resolveEventBlockTime(undefined, slot)
  ): Promise<void> {
    // Supabase mode - save to Supabase
    if (!this.prisma) {
      await saveIndexerState(signature, slot, txIndex, "poller", updatedAt);
      return;
    }

    const current = await this.prisma.indexerState.findUnique({
      where: { id: "main" },
      select: { lastSlot: true, lastTxIndex: true, lastSignature: true },
    });
    if (!shouldAdvanceCursor(
      current?.lastSlot,
      current?.lastTxIndex ?? null,
      current?.lastSignature,
      slot,
      txIndex,
      signature
    )) {
      return;
    }

    this.lastSignature = signature;
    this.lastSlot = slot;
    this.lastTxIndex = txIndex ?? null;

    // Local mode - save to Prisma
    if (await trySaveLocalIndexerStateWithSql(this.prisma, {
      signature,
      slot,
      txIndex,
      updatedAt,
    })) {
      return;
    }

    await this.prisma.indexerState.upsert({
      where: { id: "main" },
      create: {
        id: "main",
        lastSignature: signature,
        lastSlot: slot,
        lastTxIndex: txIndex,
        source: "poller",
      },
      update: {
        lastSignature: signature,
        lastSlot: slot,
        lastTxIndex: txIndex,
        source: "poller",
      },
    });
  }

  private async poll(): Promise<void> {
    while (this.isRunning) {
      try {
        await this.processNewTransactions();
      } catch (error) {
        logger.error({ error }, "Error in polling loop");
      }

      if (!this.isRunning) {
        break;
      }

      const shouldContinue = await this.waitForDelay(this.pollingInterval);
      if (!shouldContinue) break;
    }
  }

  private async processNewTransactions(): Promise<ProcessNewTransactionsResult> {
    const suspendLocalDigests = Boolean(this.prisma && config.dbMode === "local");
    if (suspendLocalDigests) await suspendLocalDerivedDigests();
    try {
      const signatures = await this.fetchSignatures();

      if (signatures.length === 0) {
        if (this.hadPaginationPartial) {
          this.hasReachedLiveFrontier = false;
          logger.warn(
            {
              pendingContinuation: this.pendingContinuation,
              pendingStopSignature: this.pendingStopSignature,
            },
            "Pagination retries exhausted before any signatures could be processed"
          );
          return { fetchedCount: 0, haltedOnError: true };
        }
        this.hasReachedLiveFrontier = !this.pendingContinuation;
        logger.debug("No new transactions");
        return { fetchedCount: 0, haltedOnError: false };
      }

      this.hasReachedLiveFrontier = false;
      if (this.hadPaginationPartial) {
        logger.warn(
          {
            count: signatures.length,
            pendingContinuation: this.pendingContinuation,
            pendingStopSignature: this.pendingStopSignature,
          },
          "Skipping retry-exhausted partial pagination frontier until older gap is resolved"
        );
        return { fetchedCount: 0, haltedOnError: true };
      }

      logger.info({ count: signatures.length, batchRpc: this.useBatchRpc, batchDb: USE_BATCH_DB }, "Processing transactions");

      // Reverse to process oldest first
      const reversed = signatures.reverse();

      // BATCH RPC: Pre-fetch all transactions in batch (with fallback)
      let txCache: Map<string, ParsedTransactionWithMeta> | null = null;
      if (this.useBatchRpc && this.batchFetcher && reversed.length > 1) {
        const sigList = reversed.map(s => s.signature);
        try {
          txCache = await this.batchFetcher.fetchTransactions(sigList);
        } catch (error) {
          if (error instanceof BatchRpcBackoffRequiredError) {
            logger.warn(
              {
                requested: sigList.length,
                retryAfterMs: error.retryAfterMs,
              },
              "Batch RPC fetch entered cooldown; halting cycle for retry"
            );
            return { fetchedCount: 0, haltedOnError: true };
          }
          throw error;
        }
        logger.debug({ requested: sigList.length, fetched: txCache.size }, "Live poll batch RPC fetch");
      }

      // Group by slot for tx_index resolution
      const bySlot = new Map<number, ConfirmedSignatureInfo[]>();
      for (const sig of reversed) {
        if (!bySlot.has(sig.slot)) {
          bySlot.set(sig.slot, []);
        }
        bySlot.get(sig.slot)!.push(sig);
      }

      // Process slot by slot
      const sortedSlots = Array.from(bySlot.keys()).sort((a, b) => a - b);

      let haltedOnError = false;
      for (const slot of sortedSlots) {
        if (!this.isRunning) break;
        if (this.stopBeforeNewerSlot(slot, "polling")) break;
        const sigs = bySlot.get(slot)!;
        const txIndexMap = await this.getTxIndexMap(slot, sigs);
        if (sigs.length > 1 && hasUnresolvedTxIndex(txIndexMap, sigs)) {
          throw new Error(`Deterministic tx_index unavailable for live slot ${slot}`);
        }

        const sigsWithIndex = sigs.map((sig) => ({
          sig,
          txIndex: txIndexMap.get(sig.signature) ?? undefined
        })).sort((a, b) =>
          compareTransactionCursor(a.txIndex, a.sig.signature, b.txIndex, b.sig.signature)
        );

        let batchFailed = false;
        for (const { sig, txIndex } of sigsWithIndex) {
          if (!this.isRunning) break;
          try {
            // Use cached transaction from batch RPC if available
            let blockTime: Date;
            if (this.useBatchRpc && txCache) {
              blockTime = await this.processTransactionBatch(sig, txIndex, txCache.get(sig.signature));
            } else {
              blockTime = await this.processTransaction(sig, txIndex);
            }
            if (!this.isRunning) {
              break;
            }
            if (USE_BATCH_DB && this.eventBuffer) {
              this.eventBuffer.noteCursor({
                signature: sig.signature,
                slot: BigInt(sig.slot),
                blockTime,
                txIndex,
              }, false);
            }
            this.lastSignature = sig.signature;
            this.lastSlot = BigInt(sig.slot);
            this.lastTxIndex = txIndex ?? null;
            // Skip individual cursor saves when using batch DB - handled by EventBuffer flush
            if (!USE_BATCH_DB) {
              await this.saveState(sig.signature, BigInt(sig.slot), txIndex ?? null, blockTime);
            }
            this.processedCount++;
          } catch (error) {
            this.errorCount++;
            logger.error(
              { error: error instanceof Error ? error.message : String(error), signature: sig.signature },
              "Error processing transaction - stopping batch to prevent event loss"
            );
            try {
              await this.logFailedTransaction(sig, error);
            } catch (logError) {
              logger.warn(
                { error: logError instanceof Error ? logError.message : String(logError), signature: sig.signature },
                "Failed to log failed transaction"
              );
            }
            batchFailed = true;
            break;
          }
        }
        if (batchFailed) {
          haltedOnError = true;
          logger.warn(
            { slot, lastSignature: this.lastSignature },
            "Batch processing halted - will retry failed tx on next poll cycle"
          );
          break;
        }
      }

      // BATCH DB: Flush events after processing all transactions
      if (USE_BATCH_DB && this.eventBuffer) {
        if (!this.isRunning) {
          await this.eventBuffer.drain();
        } else if (this.eventBuffer.size > 0 || this.eventBuffer.hasPendingCursor()) {
          await this.eventBuffer.flush();
        }
      }

      this.logStatsIfNeeded();
      return { fetchedCount: signatures.length, haltedOnError };
    } finally {
      if (suspendLocalDigests) resumeLocalDerivedDigests();
    }
  }

  /**
   * Fetch new signatures since lastSignature
   * Uses pagination with `before` to handle cases where new tx count > batchSize
   * Returns signatures in newest-first order (caller should reverse for processing)
   */
  private async fetchSignatures(): Promise<ConfirmedSignatureInfo[]> {
    this.hadPaginationPartial = false;
    try {
      if (!this.lastSignature) {
        // No last signature - just get the latest batch
        const signatures = await this.connection.getSignaturesForAddress(
          this.programId,
          { limit: this.batchSize }
        );
        logger.debug({ count: signatures.length }, "Fetched initial signatures");
        return signatures.filter((sig) => sig.err === null);
      }

      // Paginate backwards from newest until we reach lastSignature (or pendingStopSignature if resuming)
      const allSignatures: ConfirmedSignatureInfo[] = [];
      const seenSignatures = new Set<string>();
      // Resume from continuation point if we hit memory limit in previous cycle
      let beforeSignature: string | undefined = this.pendingContinuation || undefined;
      if (!beforeSignature && this.stopSlot !== null) {
        beforeSignature = (await this.resolveStopSlotPaginationBefore()) || undefined;
        if (!beforeSignature && !this.stopSlotPaginationBeforeResolved) {
          logger.info(
            {
              stopSlot: this.stopSlot.toString(),
              continuationPoint: this.stopSlotPaginationContinuation,
            },
            "Stop-slot pagination anchor is not ready yet; deferring fetch to next cycle"
          );
          return [];
        }
      }
      // Use pendingStopSignature if resuming, otherwise use lastSignature
      const stopSignature = this.pendingStopSignature || this.lastSignature;
      let retryCount = 0;
      let pageLimit = this.batchSize;
      const defaultMinPageLimit = Math.max(1, Math.min(25, this.batchSize));
      let minPageLimit = defaultMinPageLimit;
      const maxSignaturesPerCycle = Math.max(this.batchSize, config.pollerMaxSignaturesPerCycle);

      const pushUniqueSignature = (entry: ConfirmedSignatureInfo): void => {
        if (entry.err !== null) return;
        if (seenSignatures.has(entry.signature)) return;
        seenSignatures.add(entry.signature);
        allSignatures.push(entry);
      };
      const retainOldestHistoricalSlice = (): void => {
        if (allSignatures.length <= maxSignaturesPerCycle) return;
        const overflow = allSignatures.length - maxSignaturesPerCycle;
        allSignatures.splice(0, overflow);
      };

      if (this.pendingContinuation) {
        logger.info({
          continuationPoint: beforeSignature,
          stopSignature: stopSignature
        }, "Resuming from previous continuation point");
        // Clear continuation (will be set again if we hit limit)
        // Keep pendingStopSignature until we finish the whole batch
        this.pendingContinuation = null;
      }

      while (true) {
        try {
          const options: { limit: number; before?: string } = {
            limit: pageLimit,
          };
          if (beforeSignature) {
            options.before = beforeSignature;
          }

          const batch = await this.connection.getSignaturesForAddress(
            this.programId,
            options
          );

          if (batch.length === 0) {
            // Reached the end - clear pendingStopSignature since we're done
            this.pendingStopSignature = null;
            break;
          }

          // Filter out failed transactions and check for stop signature
          for (let i = 0; i < batch.length; i++) {
            const sig = batch[i];
            if (sig.signature === stopSignature) {
              const stopSlot = sig.slot;
              const priorSameSlot = allSignatures.filter((existing) => existing.slot === stopSlot);
              const keepDifferentSlot = allSignatures.filter((existing) => existing.slot !== stopSlot);
              const sameSlotCandidates = [...priorSameSlot];
              const seenSameSlot = new Set(sameSlotCandidates.map((entry) => entry.signature));
              const pushSameSlotCandidate = (candidate: ConfirmedSignatureInfo): void => {
                if (candidate.slot !== stopSlot) return;
                if (candidate.err !== null) return;
                if (candidate.signature === stopSignature) return;
                if (seenSameSlot.has(candidate.signature)) return;
                seenSameSlot.add(candidate.signature);
                sameSlotCandidates.push(candidate);
              };
              for (let j = i + 1; j < batch.length; j++) {
                pushSameSlotCandidate(batch[j]);
              }
              // Continue one-or-more extra pages while the slot is still present.
              // This covers same-slot overflow when stopSignature appears before page end.
              let sameSlotBefore: string | undefined = batch[batch.length - 1]?.signature;
              while (sameSlotBefore && this.isRunning) {
                const sameSlotBatch = await this.connection.getSignaturesForAddress(
                  this.programId,
                  { limit: pageLimit, before: sameSlotBefore }
                );
                if (sameSlotBatch.length === 0) break;
                let sawStopSlot = false;
                for (const sameSlotSig of sameSlotBatch) {
                  if (sameSlotSig.slot !== stopSlot) continue;
                  sawStopSlot = true;
                  pushSameSlotCandidate(sameSlotSig);
                }
                const nextBefore = sameSlotBatch[sameSlotBatch.length - 1].signature;
                if (!sawStopSlot) break;
                if (sameSlotBatch.length < pageLimit) break;
                if (nextBefore === sameSlotBefore) break;
                sameSlotBefore = nextBefore;
                if (!(await this.waitForDelay(100))) break;
              }
              let stopTxIndex = this.lastTxIndex ?? -1;
              const sameSlotCursorEntries = [sig, ...sameSlotCandidates];
              try {
                const sameSlotTxIndexMap = await this.getTxIndexMap(stopSlot, sameSlotCursorEntries);
                if (hasUnresolvedTxIndex(sameSlotTxIndexMap, sameSlotCursorEntries)) {
                  throw new Error(`Deterministic tx_index unavailable for pagination stop slot ${stopSlot}`);
                }
                if (sameSlotTxIndexMap.has(stopSignature)) {
                  stopTxIndex = sameSlotTxIndexMap.get(stopSignature) ?? -1;
                }
                sameSlotCandidates.sort((a, b) =>
                  compareTransactionCursor(
                    sameSlotTxIndexMap.get(a.signature) ?? null,
                    a.signature,
                    sameSlotTxIndexMap.get(b.signature) ?? null,
                    b.signature
                  )
                );
                allSignatures.length = 0;
                seenSignatures.clear();
                for (const entry of keepDifferentSlot) {
                  pushUniqueSignature(entry);
                }
                for (const candidate of sameSlotCandidates) {
                  if (
                    compareTransactionCursor(
                      stopTxIndex,
                      stopSignature,
                      sameSlotTxIndexMap.get(candidate.signature) ?? null,
                      candidate.signature
                    ) < 0
                  ) {
                    pushUniqueSignature(candidate);
                  }
                }
              } catch (error) {
                logger.warn(
                  {
                    stopSlot,
                    stopSignature,
                    error: error instanceof Error ? error.message : String(error),
                  },
                  "Failed to resolve same-slot tx_index map during pagination stop handling"
                );
                throw error;
              }
              retainOldestHistoricalSlice();
              this.pendingStopSignature = null;
              return allSignatures;
            }
            pushUniqueSignature(sig);
          }

          const persistedStopSlot = this.lastSlot;
          if (persistedStopSlot !== null) {
            const crossedBelowPersistedSlot = batch.some(
              (sig) => BigInt(sig.slot) < persistedStopSlot
            );
            if (crossedBelowPersistedSlot) {
              const stopSlot = Number(persistedStopSlot);
              const keepNewerSlots = allSignatures.filter(
                (existing) => BigInt(existing.slot) > persistedStopSlot
              );
              const priorSameSlot = allSignatures.filter(
                (existing) => BigInt(existing.slot) === persistedStopSlot
              );
              const sameSlotCandidates = [...priorSameSlot];
              const seenSameSlot = new Set(sameSlotCandidates.map((entry) => entry.signature));
              const pushSameSlotCandidate = (candidate: ConfirmedSignatureInfo): void => {
                if (BigInt(candidate.slot) !== persistedStopSlot) return;
                if (candidate.err !== null) return;
                if (candidate.signature === stopSignature) return;
                if (seenSameSlot.has(candidate.signature)) return;
                seenSameSlot.add(candidate.signature);
                sameSlotCandidates.push(candidate);
              };
              for (const candidate of batch) {
                pushSameSlotCandidate(candidate);
              }
              let sameSlotBefore: string | undefined = batch[batch.length - 1]?.signature;
              while (sameSlotBefore && this.isRunning) {
                const sameSlotBatch = await this.connection.getSignaturesForAddress(
                  this.programId,
                  { limit: pageLimit, before: sameSlotBefore }
                );
                if (sameSlotBatch.length === 0) break;
                let sawStopSlot = false;
                for (const sameSlotSig of sameSlotBatch) {
                  if (BigInt(sameSlotSig.slot) !== persistedStopSlot) continue;
                  sawStopSlot = true;
                  pushSameSlotCandidate(sameSlotSig);
                }
                const nextBefore = sameSlotBatch[sameSlotBatch.length - 1].signature;
                if (!sawStopSlot) break;
                if (sameSlotBatch.length < pageLimit) break;
                if (nextBefore === sameSlotBefore) break;
                sameSlotBefore = nextBefore;
                if (!(await this.waitForDelay(100))) break;
              }
              let stopTxIndex = this.lastTxIndex ?? -1;
              const stopCursorSignature: ConfirmedSignatureInfo = {
                signature: stopSignature,
                slot: stopSlot,
                err: null,
                memo: null,
                blockTime: null,
                confirmationStatus: "finalized",
              };
              const sameSlotCursorEntries = [stopCursorSignature, ...sameSlotCandidates];
              try {
                const sameSlotTxIndexMap = await this.getTxIndexMap(stopSlot, sameSlotCursorEntries);
                if (hasUnresolvedTxIndex(sameSlotTxIndexMap, sameSlotCursorEntries)) {
                  throw new Error(`Deterministic tx_index unavailable for pagination slot boundary ${stopSlot}`);
                }
                if (sameSlotTxIndexMap.has(stopSignature)) {
                  stopTxIndex = sameSlotTxIndexMap.get(stopSignature) ?? -1;
                }
                sameSlotCandidates.sort((a, b) =>
                  compareTransactionCursor(
                    sameSlotTxIndexMap.get(a.signature) ?? null,
                    a.signature,
                    sameSlotTxIndexMap.get(b.signature) ?? null,
                    b.signature
                  )
                );
                allSignatures.length = 0;
                seenSignatures.clear();
                for (const entry of keepNewerSlots) {
                  pushUniqueSignature(entry);
                }
                for (const candidate of sameSlotCandidates) {
                  if (
                    compareTransactionCursor(
                      stopTxIndex,
                      stopSignature,
                      sameSlotTxIndexMap.get(candidate.signature) ?? null,
                      candidate.signature
                    ) < 0
                  ) {
                    pushUniqueSignature(candidate);
                  }
                }
              } catch (error) {
                logger.warn(
                  {
                    stopSlot,
                    stopSignature,
                    error: error instanceof Error ? error.message : String(error),
                  },
                  "Failed to resolve same-slot tx_index map during pagination slot-boundary handling"
                );
                throw error;
              }
              logger.warn(
                {
                  stopSlot: persistedStopSlot.toString(),
                  stopSignature,
                  beforeSignature,
                },
                "RPC pagination crossed below saved slot without returning stop signature; stopping at slot boundary"
              );
              retainOldestHistoricalSlice();
              this.pendingStopSignature = null;
              return allSignatures;
            }
          }

          // Move to older signatures for next iteration
          beforeSignature = batch[batch.length - 1].signature;

          // Log progress for large gaps (but continue - don't lose data!)
          if (allSignatures.length > 0 && allSignatures.length % 10000 === 0) {
            logger.info({ count: allSignatures.length }, "Large gap being processed, continuing pagination...");
          }

          // Integrity-first historical catch-up:
          // keep scanning to the stop boundary, but retain only the OLDEST slice
          // of the frontier for this cycle so replay order remains globally monotonic.
          if (allSignatures.length > maxSignaturesPerCycle) {
            retainOldestHistoricalSlice();
            logger.warn(
              {
                count: allSignatures.length,
                maxSignaturesPerCycle,
                beforeSignature,
                stopSignature,
              },
              "Per-cycle signature cap reached; retaining oldest historical slice for deterministic replay"
            );
          }

          // Small delay to avoid rate limiting during pagination
          if (batch.length >= pageLimit) {
            if (!(await this.waitForDelay(100))) break;
          } else {
            // Got fewer than requested, no more signatures
            this.pendingStopSignature = null;
            break;
          }

          retryCount = 0; // Reset on success
          minPageLimit = defaultMinPageLimit;
          pageLimit = this.batchSize;
        } catch (innerError) {
          retryCount++;
          if (isProviderOverloadedError(innerError)) {
            minPageLimit = 1;
          }
          pageLimit = Math.max(minPageLimit, Math.floor(pageLimit / 2));
          logger.warn(
            {
              retryCount,
              pageLimit,
              beforeSignature,
              stopSignature,
              collectedSignatures: allSignatures.length,
              err: innerError instanceof Error ? innerError : undefined,
              error: toErrorDetails(innerError),
            },
            "Error during signature pagination"
          );

          if (retryCount >= 3) {
            this.hadPaginationPartial = true;
            if (beforeSignature && !this.pendingContinuation) {
              this.pendingContinuation = beforeSignature;
              if (!this.pendingStopSignature) {
                this.pendingStopSignature = stopSignature;
              }
            }
            if (isDeterministicPaginationBoundaryError(innerError)) {
              this.hadPaginationPartial = false;
              logger.error(
                {
                  retryCount,
                  pageLimit,
                  beforeSignature,
                  stopSignature,
                  collectedSignatures: allSignatures.length,
                  pendingContinuation: this.pendingContinuation,
                  pendingStopSignature: this.pendingStopSignature,
                  err: innerError instanceof Error ? innerError : undefined,
                  error: toErrorDetails(innerError),
                },
                "Too many pagination boundary ordering errors, deferring frontier until deterministic ordering is available"
              );
              return [];
            }
            logger.error(
              {
                retryCount,
                pageLimit,
                beforeSignature,
                stopSignature,
                collectedSignatures: allSignatures.length,
                pendingContinuation: this.pendingContinuation,
                pendingStopSignature: this.pendingStopSignature,
                err: innerError instanceof Error ? innerError : undefined,
                error: toErrorDetails(innerError),
              },
              "Too many pagination errors, returning partial results"
            );
            break;
          }
          const retryDelayMs = isProviderOverloadedError(innerError)
            ? Math.min(30000, 1000 * (2 ** retryCount))
            : 500 * retryCount;
          if (!(await this.waitForDelay(retryDelayMs))) break;
        }
      }

      return allSignatures;
    } catch (error) {
      logger.error({ error }, "Error fetching signatures");
      return [];
    }
  }

  private async processTransaction(sig: ConfirmedSignatureInfo, txIndex?: number): Promise<Date> {
    const tx = await this.fetchParsedTransactionWithRetry(sig.signature, sig.slot);

    const blockTime = resolveEventBlockTime(sig.blockTime ?? tx.blockTime ?? undefined, sig.slot);
    const parsed = parseTransaction(tx);
    if (!parsed || parsed.events.length === 0) {
      if (hasProgramDataLogsForProgram(tx, this.programId.toBase58())) {
        throw new Error(`Failed to parse program events for signature ${sig.signature}`);
      }
      return blockTime;
    }

    logger.debug(
      { signature: sig.signature, eventCount: parsed.events.length, txIndex },
      "Parsed transaction"
    );

    const typedEvents = parsed.events.map((rawEvent, eventOrdinal) => {
      const typedEvent = toTypedEvent(rawEvent);
      if (!typedEvent) {
        throw new Error(`Failed to convert parsed program event ${rawEvent.name} for signature ${sig.signature}`);
      }
      return { typedEvent, eventOrdinal, rawEvent };
    });

    for (const { typedEvent, eventOrdinal, rawEvent } of typedEvents) {

      const ctx: EventContext = {
        signature: sig.signature,
        slot: BigInt(sig.slot),
        blockTime,
        txIndex,
        eventOrdinal,
        skipCursorUpdate: true,
      };

      await handleEventAtomic(this.prisma, typedEvent, ctx);

      // Only log to Prisma in local mode
      if (this.prisma && !this.suppressEventLogWrites) {
        await this.prisma.eventLog.create({
          data: {
            eventType: typedEvent.type,
            signature: sig.signature,
            slot: BigInt(sig.slot),
            blockTime: ctx.blockTime,
            data: rawEvent.data as object,
            processed: true,
          },
        });
      }
    }

    return blockTime;
  }

  /**
   * Process transaction in batch mode - adds events to buffer instead of direct DB write
   * Uses pre-fetched transaction from batch RPC call
   */
  private async processTransactionBatch(
    sig: ConfirmedSignatureInfo,
    txIndex: number | undefined,
    tx: ParsedTransactionWithMeta | undefined
  ): Promise<Date> {
    if (!tx) {
      // Fallback to individual fetch if not in cache
      logger.debug({ signature: sig.signature }, "Transaction not in batch cache, fetching individually");
      tx = await this.fetchParsedTransactionWithRetry(sig.signature, sig.slot);
    }

    const blockTime = resolveEventBlockTime(sig.blockTime ?? tx.blockTime ?? undefined, sig.slot);
    const parsed = parseTransaction(tx);
    if (!parsed || parsed.events.length === 0) {
      if (hasProgramDataLogsForProgram(tx, this.programId.toBase58())) {
        throw new Error(`Failed to parse program events for signature ${sig.signature}`);
      }
      return blockTime;
    }

    logger.debug(
      { signature: sig.signature, eventCount: parsed.events.length, txIndex },
      "Parsed transaction (batch mode)"
    );

    const typedEvents = parsed.events.map((event, eventOrdinal) => {
      const typedEvent = toTypedEvent(event);
      if (!typedEvent) {
        throw new Error(`Failed to convert parsed program event ${event.name} for signature ${sig.signature}`);
      }
      return { typedEvent, eventOrdinal };
    });

    for (const { typedEvent, eventOrdinal } of typedEvents) {

      const ctx = {
        signature: sig.signature,
        slot: BigInt(sig.slot),
        blockTime,
        txIndex,
        eventOrdinal,
        skipCursorUpdate: true,
      };

      // Add to event buffer instead of direct DB write
      if (this.eventBuffer) {
        await this.eventBuffer.addEvent({
          type: typedEvent.type,
          data: typedEvent.data as unknown as Record<string, unknown>,
          ctx,
        });
      } else {
        // Fallback to direct write if no buffer
        await handleEventAtomic(this.prisma, typedEvent, ctx as EventContext);
      }
    }

    return blockTime;
  }

  private async logFailedTransaction(
    sig: ConfirmedSignatureInfo,
    error: unknown
  ): Promise<void> {
    // Only log errors to Prisma in local mode
    if (!this.prisma || this.suppressEventLogWrites) return;

    await this.prisma.eventLog.create({
      data: {
        eventType: "PROCESSING_FAILED",
        signature: sig.signature,
        slot: BigInt(sig.slot),
        blockTime: resolveEventBlockTime(sig.blockTime, sig.slot),
        data: {},
        processed: false,
        error: error instanceof Error ? error.message : String(error),
      },
    });
  }
}
