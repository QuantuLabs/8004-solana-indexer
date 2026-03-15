/**
 * Batch Processor - High-performance event batching for ultra-fast indexing
 *
 * Optimizations:
 * 1. Batch RPC fetching: getParsedTransactions() instead of getParsedTransaction()
 * 2. Batch DB writes: accumulate events, flush in single transaction
 * 3. Pipeline pattern: fetch → parse → buffer → flush
 */

import { Connection, ParsedTransactionWithMeta } from "@solana/web3.js";
import { Pool, PoolClient } from "pg";
import { PrismaClient } from "@prisma/client";
import { createHash } from "crypto";
import { createChildLogger } from "../logger.js";
import { config } from "../config.js";
import { metadataQueue } from "./metadata-queue.js";
import { collectionMetadataQueue } from "./collection-metadata-queue.js";
import { classifyRevocationStatus } from "../db/revocation-classification.js";
import { classifyResponseStatus } from "../db/response-classification.js";
import { trySaveLocalIndexerStateWithSql } from "../db/indexer-state-local.js";
import { REVOCATION_CONFLICT_UPDATE_WHERE_SQL } from "../db/revocation-upsert-order.js";
import { compressForStorage } from "../utils/compression.js";
import { stripNullBytes } from "../utils/sanitize.js";
import { DEFAULT_PUBKEY } from "../constants.js";

const logger = createChildLogger("batch-processor");

// Batch configuration
const DEFAULT_BATCH_SIZE_RPC = 100;        // Max transactions per RPC call
const BATCH_SIZE_DB = 500;         // Max events per DB transaction
const FLUSH_INTERVAL_MS = 500;     // Flush every 500ms even if batch not full
const DEFAULT_MAX_PARALLEL_RPC = 3;        // Parallel RPC batch requests
const MAX_DEAD_LETTER = 10000;     // Max diagnostic events retained in dead letter queue
const DEAD_LETTER_BACKPRESSURE = 0.8; // Warn at 80% diagnostic queue capacity

const PROVIDER_BACKOFF_MIN_MS = 2000;
const PROVIDER_BACKOFF_MAX_MS = 30000;

const ZERO_HASH_HEX = "0".repeat(64);

function toRequiredHashHex(hash: Uint8Array | undefined | null): string | null {
  if (!hash) return null;
  return Buffer.from(hash).toString("hex");
}

function hashesMatchHex(stored: string | null, event: string | null): boolean {
  const sEmpty = !stored || stored === ZERO_HASH_HEX;
  const eEmpty = !event || event === ZERO_HASH_HEX;
  if (sEmpty && eEmpty) return true;
  if (sEmpty || eEmpty) return false;
  return stored === event;
}

// EventData type for batch event data - uses Record for type safety while allowing runtime values
type EventData = Record<string, any>;

export interface BatchEvent {
  type: string;
  data: EventData;
  ctx: {
    signature: string;
    slot: bigint;
    blockTime: Date;
    txIndex?: number;
    eventOrdinal?: number;
  };
}

interface DeadLetterEntry {
  event: BatchEvent;
  addedAt: number;
}

export interface BatchStats {
  eventsBuffered: number;
  eventsFlushed: number;
  flushCount: number;
  avgFlushTime: number;
  rpcBatchCount: number;
  avgRpcBatchTime: number;
}

function compareCursorCtx(
  a: BatchEvent["ctx"] | null | undefined,
  b: BatchEvent["ctx"] | null | undefined
): number {
  if (!a && !b) return 0;
  if (!a) return -1;
  if (!b) return 1;
  if (a.slot !== b.slot) {
    return a.slot < b.slot ? -1 : 1;
  }

  const aTxIndex = a.txIndex ?? -1;
  const bTxIndex = b.txIndex ?? -1;
  if (aTxIndex !== bTxIndex) {
    return aTxIndex - bTxIndex;
  }

  if (a.signature === b.signature) {
    return 0;
  }
  return a.signature < b.signature ? -1 : 1;
}

interface BatchRpcFetcherOptions {
  chunkSize?: number;
  maxParallelChunks?: number;
}

function isProviderOverloadedError(error: unknown): boolean {
  if (!error || typeof error !== "object") {
    return false;
  }

  const maybeError = error as {
    code?: unknown;
    status?: unknown;
    message?: unknown;
    response?: { status?: unknown };
    cause?: unknown;
    data?: unknown;
    error?: unknown;
  };

  if (
    maybeError.code === 429 ||
    maybeError.status === 429 ||
    maybeError.response?.status === 429 ||
    maybeError.code === 502 ||
    maybeError.status === 502 ||
    maybeError.response?.status === 502 ||
    maybeError.code === 503 ||
    maybeError.status === 503 ||
    maybeError.response?.status === 503 ||
    maybeError.code === 504 ||
    maybeError.status === 504 ||
    maybeError.response?.status === 504
  ) {
    return true;
  }

  if (typeof maybeError.message !== "string") {
    return false;
  }

  const message = maybeError.message.toLowerCase();
  if (
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
  ) {
    return true;
  }

  return (
    (maybeError.cause !== undefined && maybeError.cause !== error && isProviderOverloadedError(maybeError.cause)) ||
    (maybeError.data !== undefined && maybeError.data !== error && isProviderOverloadedError(maybeError.data)) ||
    (maybeError.error !== undefined && maybeError.error !== error && isProviderOverloadedError(maybeError.error))
  );
}

function isBatchUnsupportedError(error: unknown): boolean {
  if (!error || typeof error !== "object") {
    return false;
  }

  const maybeError = error as {
    code?: unknown;
    status?: unknown;
    message?: unknown;
    response?: { status?: unknown };
    cause?: unknown;
    data?: unknown;
    error?: unknown;
  };

  if (maybeError.code === -32403) {
    return true;
  }

  if (typeof maybeError.message !== "string") {
    return false;
  }

  const message = maybeError.message.toLowerCase();
  if (
    message.includes("batch requests are only available") ||
    message.includes("batch request is only available") ||
    message.includes("batch requests are not available") ||
    message.includes("batch request not supported") ||
    message.includes("batch unsupported") ||
    message.includes("-32403")
  ) {
    return true;
  }

  return (
    (maybeError.cause !== undefined && maybeError.cause !== error && isBatchUnsupportedError(maybeError.cause)) ||
    (maybeError.data !== undefined && maybeError.data !== error && isBatchUnsupportedError(maybeError.data)) ||
    (maybeError.error !== undefined && maybeError.error !== error && isBatchUnsupportedError(maybeError.error))
  );
}

export class BatchRpcBackoffRequiredError extends Error {
  readonly retryAfterMs: number;

  constructor(message: string, retryAfterMs: number) {
    super(message);
    this.name = "BatchRpcBackoffRequiredError";
    this.retryAfterMs = retryAfterMs;
  }
}

/**
 * Batch RPC Fetcher - Fetches multiple transactions in parallel
 */
export class BatchRpcFetcher {
  private connection: Connection;
  private chunkSize: number;
  private currentChunkSize: number;
  private maxParallelChunks: number;
  private currentMaxParallelChunks: number;
  private batchRpcUnsupported = false;
  private batchRpcUnsupportedLogged = false;
  private providerCooldownUntil = 0;
  private providerCooldownMs = 0;
  private stats = { batchCount: 0, totalTime: 0 };

  constructor(connection: Connection, options: BatchRpcFetcherOptions = {}) {
    this.connection = connection;
    this.chunkSize = options.chunkSize ?? config.pollerRpcChunkSize ?? DEFAULT_BATCH_SIZE_RPC;
    this.currentChunkSize = this.chunkSize;
    this.maxParallelChunks = options.maxParallelChunks ?? config.pollerRpcChunkConcurrency ?? DEFAULT_MAX_PARALLEL_RPC;
    this.currentMaxParallelChunks = this.maxParallelChunks;
  }

  private getProviderCooldownRemainingMs(): number {
    return Math.max(0, this.providerCooldownUntil - Date.now());
  }

  private enterProviderCooldown(error: unknown): number {
    const nextCooldownMs = this.providerCooldownMs > 0
      ? Math.min(PROVIDER_BACKOFF_MAX_MS, this.providerCooldownMs * 2)
      : PROVIDER_BACKOFF_MIN_MS;
    this.providerCooldownMs = nextCooldownMs;
    this.providerCooldownUntil = Date.now() + nextCooldownMs;
    this.currentMaxParallelChunks = Math.max(1, Math.floor(this.currentMaxParallelChunks / 2));
    logger.warn(
      {
        retryAfterMs: nextCooldownMs,
        currentChunkSize: this.currentChunkSize,
        currentMaxParallelChunks: this.currentMaxParallelChunks,
        error,
      },
      "RPC provider overloaded, entering batch fetch cooldown"
    );
    return nextCooldownMs;
  }

  private async fetchTransactionsIndividually(
    signatures: string[],
    reason: "single-signature" | "batch-unsupported" | "fallback"
  ): Promise<Map<string, ParsedTransactionWithMeta>> {
    const results = new Map<string, ParsedTransactionWithMeta>();

    if (reason === "batch-unsupported") {
      if (!this.batchRpcUnsupportedLogged) {
        this.batchRpcUnsupportedLogged = true;
        logger.warn(
          { count: signatures.length },
          "Batch parsed RPC unsupported by provider, using individual transaction fetches"
        );
      }
    }

    for (const sig of signatures) {
      try {
        const tx = await this.connection.getParsedTransaction(sig, {
          maxSupportedTransactionVersion: config.maxSupportedTransactionVersion
        });
        if (tx) {
          results.set(sig, tx);
        }
      } catch (error) {
        if (isProviderOverloadedError(error)) {
          const retryAfterMs = this.enterProviderCooldown(error);
          throw new BatchRpcBackoffRequiredError(
            "RPC provider overloaded during individual transaction fetch",
            retryAfterMs
          );
        }
        logger.warn({ signature: sig, error }, "Individual fetch failed");
      }
    }

    return results;
  }

  /**
   * Fetch multiple transactions in a single RPC call
   * Returns Map<signature, ParsedTransaction>
   */
  async fetchTransactions(
    signatures: string[]
  ): Promise<Map<string, ParsedTransactionWithMeta>> {
    const results = new Map<string, ParsedTransactionWithMeta>();
    const providerCooldownRemainingMs = this.getProviderCooldownRemainingMs();

    if (providerCooldownRemainingMs > 0) {
      throw new BatchRpcBackoffRequiredError(
        "RPC provider cooldown active for batch fetch",
        providerCooldownRemainingMs
      );
    }

    if (signatures.length === 1) {
      return this.fetchTransactionsIndividually(signatures, "single-signature");
    }

    if (this.batchRpcUnsupported) {
      return this.fetchTransactionsIndividually(signatures, "batch-unsupported");
    }

    // Split into chunks of configured getParsedTransactions size
    const chunks: string[][] = [];
    for (let i = 0; i < signatures.length; i += this.currentChunkSize) {
      chunks.push(signatures.slice(i, i + this.currentChunkSize));
    }

    const startTime = Date.now();

    // Process chunks with limited parallelism
    for (let i = 0; i < chunks.length; i += this.currentMaxParallelChunks) {
      const parallelChunks = chunks.slice(i, i + this.currentMaxParallelChunks);

      const batchResults = await Promise.all(
        parallelChunks.map(chunk => this.fetchChunk(chunk))
      );

      for (const chunkResult of batchResults) {
        for (const [sig, tx] of chunkResult) {
          results.set(sig, tx);
        }
      }
    }

    const elapsed = Date.now() - startTime;
    this.stats.batchCount++;
    this.stats.totalTime += elapsed;

    logger.debug({
      signatures: signatures.length,
      fetched: results.size,
      elapsed,
      avgTime: Math.round(this.stats.totalTime / this.stats.batchCount)
    }, "Batch RPC fetch complete");

    return results;
  }

  private async fetchChunk(
    signatures: string[]
  ): Promise<Map<string, ParsedTransactionWithMeta>> {
    const results = new Map<string, ParsedTransactionWithMeta>();
    const missing: string[] = [];

    try {
      // Use getParsedTransactions (plural) for batch fetching
      const transactions = await this.connection.getParsedTransactions(
        signatures,
        { maxSupportedTransactionVersion: config.maxSupportedTransactionVersion }
      );

      for (let i = 0; i < signatures.length; i++) {
        const tx = transactions[i];
        if (tx) {
          results.set(signatures[i], tx);
        } else {
          missing.push(signatures[i]);
        }
      }
    } catch (error) {
      if (isProviderOverloadedError(error)) {
        this.currentMaxParallelChunks = Math.max(1, Math.floor(this.currentMaxParallelChunks / 2));
      }

      if (isProviderOverloadedError(error) && signatures.length > 1) {
        const nextChunkSize = Math.max(1, Math.floor(signatures.length / 2));
        if (nextChunkSize < this.currentChunkSize) {
          this.currentChunkSize = nextChunkSize;
        }
        logger.warn(
          {
            count: signatures.length,
            nextChunkSize: this.currentChunkSize,
            nextParallelChunks: this.currentMaxParallelChunks,
            error,
          },
          "Batch RPC fetch overloaded, reducing chunk size and retrying subchunks"
        );

        const splitResults = new Map<string, ParsedTransactionWithMeta>();
        for (let i = 0; i < signatures.length; i += this.currentChunkSize) {
          const chunk = signatures.slice(i, i + this.currentChunkSize);
          const chunkResults = await this.fetchChunk(chunk);
          for (const [sig, tx] of chunkResults) {
            splitResults.set(sig, tx);
          }
        }
        return splitResults;
      }

      if (isProviderOverloadedError(error)) {
        const retryAfterMs = this.enterProviderCooldown(error);
        throw new BatchRpcBackoffRequiredError(
          "RPC provider overloaded during batch fetch",
          retryAfterMs
        );
      }

      if (isBatchUnsupportedError(error)) {
        this.batchRpcUnsupported = true;
        return this.fetchTransactionsIndividually(signatures, "batch-unsupported");
      }

      logger.warn({ error, count: signatures.length }, "Batch RPC fetch failed, falling back to individual");

      // Fallback to individual fetches
      return this.fetchTransactionsIndividually(signatures, "fallback");
    }

    if (missing.length > 0) {
      logger.warn({ missing: missing.length, count: signatures.length }, "Batch RPC returned partial nulls, refetching missing transactions individually");
      for (const sig of missing) {
        try {
          const tx = await this.connection.getParsedTransaction(sig, {
            maxSupportedTransactionVersion: config.maxSupportedTransactionVersion
          });
          if (tx) {
            results.set(sig, tx);
          }
        } catch (error) {
          if (isProviderOverloadedError(error)) {
            const retryAfterMs = this.enterProviderCooldown(error);
            throw new BatchRpcBackoffRequiredError(
              "RPC provider overloaded while refetching missing transactions",
              retryAfterMs
            );
          }
          logger.warn({ signature: sig, error }, "Individual refetch after partial batch null failed");
        }
      }
    }

    return results;
  }

  getStats() {
    return {
      batchCount: this.stats.batchCount,
      chunkSize: this.currentChunkSize,
      parallelChunks: this.currentMaxParallelChunks,
      avgTime: this.stats.batchCount > 0
        ? Math.round(this.stats.totalTime / this.stats.batchCount)
        : 0
    };
  }
}

// Retry configuration
const MAX_FLUSH_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

/**
 * Event Buffer - Accumulates events and flushes in batches
 */
export class EventBuffer {
  private buffer: BatchEvent[] = [];
  private flushTimer: NodeJS.Timeout | null = null;
  private flushInProgress = false;
  private activeFlushPromise: Promise<void> | null = null;
  private pool: Pool | null = null;
  private prisma: PrismaClient | null = null;
  private cursorCtx: BatchEvent["ctx"] | null = null;
  private retryCount = 0;
  private deadLetterQueue: DeadLetterEntry[] = [];

  private stats = {
    eventsBuffered: 0,
    eventsFlushed: 0,
    flushCount: 0,
    totalFlushTime: 0,
    deadLettered: 0
  };

  constructor(pool: Pool | null, prisma: PrismaClient | null) {
    this.pool = pool;
    this.prisma = prisma;
  }

  /**
   * Add event to buffer
   * Auto-flushes when buffer is full
   */
  async addEvent(event: BatchEvent): Promise<void> {
    // Backpressure signal for repeated flush failures.
    const dlqUtilization = this.deadLetterQueue.length / MAX_DEAD_LETTER;
    if (dlqUtilization > DEAD_LETTER_BACKPRESSURE) {
      logger.warn({
        deadLetterSize: this.deadLetterQueue.length,
        maxCapacity: MAX_DEAD_LETTER,
        utilization: `${Math.round(dlqUtilization * 100)}%`
      }, "Dead letter queue backpressure: queue is above 80% capacity, DB writes may be failing");
    }

    this.buffer.push(event);
    this.noteCursor(event.ctx);
    this.stats.eventsBuffered++;

    // Start flush timer if not already running
    if (!this.flushTimer) {
      this.flushTimer = setTimeout(() => this.flush(), FLUSH_INTERVAL_MS);
    }

    // Flush immediately if buffer is full
    if (this.buffer.length >= BATCH_SIZE_DB) {
      await this.flush();
    }
  }

  noteCursor(ctx: BatchEvent["ctx"], scheduleFlush: boolean = true): void {
    if (compareCursorCtx(ctx, this.cursorCtx) > 0) {
      this.cursorCtx = ctx;
    }

    if (scheduleFlush && !this.flushTimer) {
      this.flushTimer = setTimeout(() => this.flush(), FLUSH_INTERVAL_MS);
    }
  }

  /**
   * Force flush all buffered events
   */
  async flush(): Promise<void> {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }

    if (this.flushInProgress) {
      return this.activeFlushPromise ?? Promise.resolve();
    }

    if (this.buffer.length === 0 && !this.cursorCtx) {
      return;
    }

    this.flushInProgress = true;
    const eventsToFlush = [...this.buffer];
    const cursorCtx = this.cursorCtx;
    this.buffer = [];
    this.cursorCtx = null;

    const startTime = Date.now();
    this.activeFlushPromise = (async () => {
      try {
        if (config.dbMode === "supabase" && this.pool) {
          await this.flushToSupabase(eventsToFlush, cursorCtx);
        } else if (this.prisma) {
          await this.flushToPrisma(eventsToFlush, cursorCtx);
        }

        this.stats.eventsFlushed += eventsToFlush.length;
        this.stats.flushCount++;
        this.stats.totalFlushTime += Date.now() - startTime;

        logger.debug({
          events: eventsToFlush.length,
          elapsed: Date.now() - startTime,
          avgFlushTime: Math.round(this.stats.totalFlushTime / this.stats.flushCount)
        }, "Batch flush complete");

      } catch (error) {
        this.retryCount++;
        logger.error({ error, eventCount: eventsToFlush.length, retryCount: this.retryCount }, "Batch flush failed");

        if (this.retryCount >= MAX_FLUSH_RETRIES) {
          logger.error({
            eventCount: eventsToFlush.length,
            retryCount: this.retryCount
          }, "Max retries exceeded, failing stop-safe flush (events re-queued)");

          const spaceAvailable = Math.max(0, MAX_DEAD_LETTER - this.deadLetterQueue.length);
          const toCopy = Math.min(spaceAvailable, eventsToFlush.length);
          if (toCopy > 0) {
            const now = Date.now();
            this.deadLetterQueue.push(...eventsToFlush
              .slice(0, toCopy)
              .map(event => ({ event, addedAt: now })));
            this.stats.deadLettered += toCopy;
          }
          if (toCopy < eventsToFlush.length) {
            logger.warn({
              copiedToDeadLetter: toCopy,
              skippedDiagnosticCopies: eventsToFlush.length - toCopy,
              deadLetterSize: this.deadLetterQueue.length,
            }, "Dead letter queue at capacity; skipped excess diagnostic copies");
          }

          this.buffer = [...eventsToFlush, ...this.buffer];
          if (cursorCtx) {
            this.noteCursor(cursorCtx, false);
          }
          this.retryCount = 0;
          throw error;
        } else {
          this.buffer = [...eventsToFlush, ...this.buffer];
          if (cursorCtx) {
            this.noteCursor(cursorCtx, false);
          }
          await new Promise(r => setTimeout(r, RETRY_DELAY_MS * this.retryCount));
          throw error;
        }
      } finally {
        this.flushInProgress = false;
        this.activeFlushPromise = null;
      }
    })();

    return this.activeFlushPromise;
  }

  async drain(): Promise<void> {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }

    while (this.flushInProgress || this.buffer.length > 0 || this.cursorCtx) {
      if (this.flushInProgress) {
        await (this.activeFlushPromise ?? Promise.resolve());
        continue;
      }
      await this.flush();
    }
  }

  /**
   * Get dead letter queue events (for manual inspection/replay)
   */
  getDeadLetterQueue(): BatchEvent[] {
    return this.deadLetterQueue.map(entry => entry.event);
  }

  /**
   * Clear dead letter queue after manual handling
   */
  clearDeadLetterQueue(): void {
    this.deadLetterQueue = [];
  }

  hasPendingCursor(): boolean {
    return this.cursorCtx !== null;
  }

  /**
   * Flush events to Supabase in a single transaction
   * Collects URIs for async metadata extraction after commit
   */
  private async flushToSupabase(events: BatchEvent[], lastCtx: BatchEvent["ctx"] | null): Promise<void> {
    if (!this.pool) return;

    // Collect URIs for post-commit metadata extraction
    const uriTasks: Array<{ assetId: string; uri: string; verifiedAt: string }> = [];
    const collectionTasks: Array<{ assetId: string; col: string }> = [];

    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");

      for (const event of events) {
        await this.insertEventSupabase(client, event);

        // Collect URIs from agent registration and URI update events
        if (event.type === "AgentRegistered" && event.data.agentUri) {
          const asset = event.data.asset?.toBase58?.() || event.data.asset;
          uriTasks.push({
            assetId: asset,
            uri: event.data.agentUri,
            verifiedAt: event.ctx.blockTime.toISOString(),
          });
        } else if (event.type === "UriUpdated" && event.data.newUri) {
          const asset = event.data.asset?.toBase58?.() || event.data.asset;
          uriTasks.push({
            assetId: asset,
            uri: event.data.newUri,
            verifiedAt: event.ctx.blockTime.toISOString(),
          });
        } else if (event.type === "CollectionPointerSet" && event.data.col) {
          const asset = event.data.asset?.toBase58?.() || event.data.asset;
          collectionTasks.push({ assetId: asset, col: event.data.col });
        }
      }

      // Update cursor with last event context
      if (lastCtx) {
        await client.query(`
          INSERT INTO indexer_state (id, last_signature, last_slot, last_tx_index, source, updated_at)
          VALUES ('main', $1, $2, $3, 'poller', $4)
          ON CONFLICT (id) DO UPDATE SET
            last_signature = EXCLUDED.last_signature,
            last_slot = EXCLUDED.last_slot,
            last_tx_index = EXCLUDED.last_tx_index,
            source = EXCLUDED.source,
            updated_at = EXCLUDED.updated_at
          WHERE indexer_state.last_slot IS NULL
             OR indexer_state.last_slot < EXCLUDED.last_slot
             OR (
               indexer_state.last_slot = EXCLUDED.last_slot
               AND (
                 COALESCE(indexer_state.last_tx_index, -1)
                   < COALESCE(EXCLUDED.last_tx_index, -1)
                 OR (
                   COALESCE(indexer_state.last_tx_index, -1)
                     = COALESCE(EXCLUDED.last_tx_index, -1)
                   AND COALESCE(indexer_state.last_signature, '') COLLATE "C"
                     <= EXCLUDED.last_signature COLLATE "C"
                 )
               )
             )
        `, [lastCtx.signature, lastCtx.slot.toString(), lastCtx.txIndex ?? null, lastCtx.blockTime.toISOString()]);
      }

      await client.query("COMMIT");

      // After successful commit, queue URI metadata extraction (fire-and-forget)
      if (uriTasks.length > 0 && config.metadataIndexMode !== "off") {
        metadataQueue.addBatch(uriTasks);
      }
      if (collectionTasks.length > 0 && config.collectionMetadataIndexEnabled) {
        collectionMetadataQueue.addBatch(collectionTasks);
      }
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Insert single event to Supabase (called within transaction)
   */
  private async insertEventSupabase(client: PoolClient, event: BatchEvent): Promise<void> {
    const { type, data, ctx } = event;

    switch (type) {
      case "AgentRegistered":
        await this.insertAgentSupabase(client, data, ctx);
        break;
      case "NewFeedback":
        await this.insertFeedbackSupabase(client, data, ctx);
        break;
      case "FeedbackRevoked":
        await this.insertRevocationSupabase(client, data, ctx);
        break;
      case "ResponseAppended":
        await this.insertResponseSupabase(client, data, ctx);
        break;
      case "ValidationRequested":
        if (config.validationIndexEnabled) {
          await this.insertValidationRequestSupabase(client, data, ctx);
        }
        break;
      case "ValidationResponded":
        if (config.validationIndexEnabled) {
          await this.updateValidationResponseSupabase(client, data, ctx);
        }
        break;
      case "RegistryInitialized":
        await this.insertCollectionSupabase(client, data, ctx);
        break;
      case "UriUpdated":
        await this.updateAgentUriSupabase(client, data, ctx);
        break;
      case "WalletUpdated":
        await this.updateAgentWalletSupabase(client, data, ctx);
        break;
      case "WalletResetOnOwnerSync":
        await this.updateWalletResetOnOwnerSyncSupabase(client, data, ctx);
        break;
      case "CollectionPointerSet":
        await this.updateCollectionPointerSupabase(client, data, ctx);
        break;
      case "ParentAssetSet":
        await this.updateParentAssetSupabase(client, data, ctx);
        break;
      case "AtomEnabled":
        await this.updateAtomEnabledSupabase(client, data, ctx);
        break;
      case "MetadataSet":
        await this.insertMetadataSupabase(client, data, ctx);
        break;
      case "MetadataDeleted":
        await this.deleteMetadataSupabase(client, data, ctx);
        break;
      case "AgentOwnerSynced":
        await this.updateAgentOwnerSupabase(client, data, ctx);
        break;
      default:
        logger.debug({ type }, "Unhandled event type in batch processor");
    }
  }

  // Supabase insert helpers
  private async insertAgentSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const owner = data.owner?.toBase58?.() || data.owner;
    const collection = data.collection?.toBase58?.() || data.collection;

    await client.query(`
      INSERT INTO collections (collection, registry_type, created_at, status)
      VALUES ($1, $2, $3, 'PENDING')
      ON CONFLICT (collection) DO NOTHING
    `, [collection, "BASE", ctx.blockTime.toISOString()]);

    await client.query(`
      INSERT INTO agents (asset, owner, creator, agent_uri, collection, canonical_col, col_locked, parent_asset, parent_creator, parent_locked, atom_enabled, block_slot, tx_index, event_ordinal, tx_signature, created_at, updated_at, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, 'PENDING')
      ON CONFLICT (asset) DO UPDATE SET
        owner = EXCLUDED.owner,
        creator = COALESCE(agents.creator, EXCLUDED.creator),
        agent_uri = EXCLUDED.agent_uri,
        atom_enabled = EXCLUDED.atom_enabled,
        tx_index = EXCLUDED.tx_index,
        event_ordinal = EXCLUDED.event_ordinal,
        updated_at = EXCLUDED.updated_at
    `, [
        asset,
        owner,
        owner,
        data.agentUri || null,
        collection,
        "",
        false,
        null,
        null,
        false,
        data.atomEnabled || false,
        ctx.slot.toString(),
        ctx.txIndex ?? null,
        ctx.eventOrdinal ?? null,
        ctx.signature,
        ctx.blockTime.toISOString(),
        ctx.blockTime.toISOString()]);

    await this.replayOrphanFeedbackSupabase(client, asset);
  }

  private async hasAgentSupabase(client: PoolClient, asset: string): Promise<boolean> {
    const result = await client.query(`SELECT 1 FROM agents WHERE asset = $1 LIMIT 1`, [asset]);
    return (result.rowCount ?? result.rows.length) > 0;
  }

  private async upsertOrphanFeedbackSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const clientAddr = data.clientAddress?.toBase58?.() || data.clientAddress;
    const feedbackIndex = BigInt(data.feedbackIndex?.toString() || "0");
    const id = `${asset}:${clientAddr}:${feedbackIndex}`;
    const feedbackHash = toRequiredHashHex(data.sealHash);
    const runningDigest = data.newFeedbackDigest
      ? Buffer.from(data.newFeedbackDigest)
      : null;

    await client.query(
      `INSERT INTO orphan_feedbacks (
         id, asset, client_address, feedback_index, value, value_decimals, score, tag1, tag2, endpoint,
         feedback_uri, feedback_hash, running_digest, atom_enabled, new_trust_tier, new_quality_score,
         new_confidence, new_risk_score, new_diversity_ratio, block_slot, tx_index, event_ordinal,
         tx_signature, created_at
       )
       VALUES (
         $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
         $11, $12, $13, $14, $15, $16,
         $17, $18, $19, $20, $21, $22,
         $23, $24
       )
       ON CONFLICT (asset, client_address, feedback_index) DO UPDATE SET
         value = EXCLUDED.value,
         value_decimals = EXCLUDED.value_decimals,
         score = EXCLUDED.score,
         tag1 = EXCLUDED.tag1,
         tag2 = EXCLUDED.tag2,
         endpoint = EXCLUDED.endpoint,
         feedback_uri = EXCLUDED.feedback_uri,
         feedback_hash = EXCLUDED.feedback_hash,
         running_digest = EXCLUDED.running_digest,
         atom_enabled = EXCLUDED.atom_enabled,
         new_trust_tier = EXCLUDED.new_trust_tier,
         new_quality_score = EXCLUDED.new_quality_score,
         new_confidence = EXCLUDED.new_confidence,
         new_risk_score = EXCLUDED.new_risk_score,
         new_diversity_ratio = EXCLUDED.new_diversity_ratio,
         block_slot = EXCLUDED.block_slot,
         tx_index = EXCLUDED.tx_index,
         event_ordinal = EXCLUDED.event_ordinal,
         tx_signature = EXCLUDED.tx_signature,
         created_at = EXCLUDED.created_at`,
      [
        id,
        asset,
        clientAddr,
        feedbackIndex.toString(),
        data.value?.toString() || "0",
        data.valueDecimals || 0,
        data.score,
        data.tag1 || null,
        data.tag2 || null,
        data.endpoint || null,
        data.feedbackUri ?? null,
        feedbackHash,
        runningDigest,
        data.atomEnabled || false,
        data.newTrustTier || 0,
        data.newQualityScore || 0,
        data.newConfidence || 0,
        data.newRiskScore || 0,
        data.newDiversityRatio || 0,
        ctx.slot.toString(),
        ctx.txIndex ?? null,
        ctx.eventOrdinal ?? null,
        ctx.signature,
        ctx.blockTime.toISOString(),
      ]
    );
  }

  private async replayOrphanFeedbackSupabase(client: PoolClient, asset: string): Promise<void> {
    const pending = await client.query(
      `SELECT id, asset, client_address, feedback_index, value, value_decimals, score, tag1, tag2,
              endpoint, feedback_uri, feedback_hash, running_digest, atom_enabled, new_trust_tier,
              new_quality_score, new_confidence, new_risk_score, new_diversity_ratio, block_slot,
              tx_index, event_ordinal, tx_signature, created_at
       FROM orphan_feedbacks
       WHERE asset = $1
       ORDER BY COALESCE(block_slot, 0) ASC,
                COALESCE(tx_index, 2147483647) ASC,
                COALESCE(event_ordinal, 2147483647) ASC,
                created_at ASC,
                id ASC`,
      [asset]
    );

    for (const row of pending.rows) {
      const feedbackIndex = BigInt(row.feedback_index?.toString?.() ?? row.feedback_index ?? "0");
      const feedbackHash = row.feedback_hash ?? null;
      const insertResult = await client.query(`
        INSERT INTO feedbacks (id, feedback_id, asset, client_address, feedback_index, value, value_decimals, score, tag1, tag2, endpoint, feedback_uri, feedback_hash, running_digest, is_revoked, block_slot, tx_index, event_ordinal, tx_signature, created_at, status)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, 'PENDING')
        ON CONFLICT (id) DO UPDATE SET
          feedback_hash = EXCLUDED.feedback_hash,
          running_digest = EXCLUDED.running_digest
      `, [
        row.id, null, row.asset, row.client_address, row.feedback_index,
        row.value?.toString() ?? "0", row.value_decimals ?? 0, row.score,
        row.tag1, row.tag2, row.endpoint, row.feedback_uri, row.feedback_hash, row.running_digest,
        false, row.block_slot, row.tx_index, row.event_ordinal, row.tx_signature, row.created_at
      ]);

      if ((insertResult.rowCount ?? 0) > 0) {
        const baseUpdate = `
          feedback_count = COALESCE((
            SELECT COUNT(*)::int FROM feedbacks WHERE asset = $2 AND NOT is_revoked
          ), 0),
          raw_avg_score = COALESCE((
            SELECT ROUND(AVG(score))::smallint FROM feedbacks WHERE asset = $2 AND NOT is_revoked
          ), 0),
          updated_at = $1
        `;
        const updatedAt = row.created_at instanceof Date
          ? row.created_at.toISOString()
          : new Date(row.created_at).toISOString();
        if (row.atom_enabled) {
          await client.query(
            `UPDATE agents SET
               trust_tier = $3, quality_score = $4, confidence = $5,
               risk_score = $6, diversity_ratio = $7, ${baseUpdate}
             WHERE asset = $2`,
            [updatedAt, row.asset,
             row.new_trust_tier ?? 0, row.new_quality_score ?? 0, row.new_confidence ?? 0,
             row.new_risk_score ?? 0, row.new_diversity_ratio ?? 0]
          );
        } else {
          await client.query(
            `UPDATE agents SET ${baseUpdate} WHERE asset = $2`,
            [updatedAt, row.asset]
          );
        }
      }

      await this.replayOrphanResponsesSupabase(
        client,
        row.asset,
        row.client_address,
        feedbackIndex,
        feedbackHash
      );
      await this.reconcileOrphanRevocationSupabase(
        client,
        row.asset,
        row.client_address,
        feedbackIndex,
        feedbackHash
      );
      await client.query(`DELETE FROM orphan_feedbacks WHERE id = $1`, [row.id]);
    }
  }

  private async upsertOrphanResponseSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const clientAddr = data.client?.toBase58?.() || data.client;
    const responder = data.responder?.toBase58?.() || data.responder;
    const feedbackIndex = BigInt(data.feedbackIndex?.toString() || "0");
    const id = `${asset}:${clientAddr}:${feedbackIndex}:${responder}:${ctx.signature}`;
    const responseHash = toRequiredHashHex(data.responseHash);
    const sealHash = toRequiredHashHex(data.sealHash);
    const runningDigest = data.newResponseDigest
      ? Buffer.from(data.newResponseDigest)
      : null;

    await client.query(
      `INSERT INTO orphan_responses (
         id, asset, client_address, feedback_index, responder, response_uri, response_hash, seal_hash,
         running_digest, response_count, block_slot, tx_index, event_ordinal, tx_signature, created_at
       )
       VALUES (
         $1, $2, $3, $4, $5, $6, $7, $8,
         $9, $10, $11, $12, $13, $14, $15
       )
       ON CONFLICT (asset, client_address, feedback_index, responder, tx_signature) DO UPDATE SET
         response_uri = EXCLUDED.response_uri,
         response_hash = EXCLUDED.response_hash,
         seal_hash = EXCLUDED.seal_hash,
         running_digest = EXCLUDED.running_digest,
         response_count = EXCLUDED.response_count,
         block_slot = EXCLUDED.block_slot,
         tx_index = EXCLUDED.tx_index,
         event_ordinal = EXCLUDED.event_ordinal,
         created_at = EXCLUDED.created_at`,
      [
        id,
        asset,
        clientAddr,
        feedbackIndex.toString(),
        responder,
        data.responseUri || null,
        responseHash,
        sealHash,
        runningDigest,
        BigInt(data.newResponseCount?.toString() || "0").toString(),
        ctx.slot.toString(),
        ctx.txIndex ?? null,
        ctx.eventOrdinal ?? null,
        ctx.signature,
        ctx.blockTime.toISOString(),
      ]
    );
  }

  private async replayOrphanResponsesSupabase(
    client: PoolClient,
    asset: string,
    clientAddr: string,
    feedbackIndex: bigint,
    feedbackHash: string | null
  ): Promise<void> {
    const pending = await client.query(
      `SELECT id, asset, client_address, feedback_index, responder, response_uri, response_hash, seal_hash,
              running_digest, response_count, block_slot, tx_index, event_ordinal, tx_signature, created_at
       FROM orphan_responses
       WHERE asset = $1 AND client_address = $2 AND feedback_index = $3
       ORDER BY COALESCE(block_slot, 0) ASC,
                COALESCE(tx_index, 2147483647) ASC,
                COALESCE(event_ordinal, 2147483647) ASC,
                COALESCE(tx_signature, '') ASC,
                id ASC`,
      [asset, clientAddr, feedbackIndex.toString()]
    );

    for (const row of pending.rows) {
      const sealMismatch = !hashesMatchHex(row.seal_hash ?? null, feedbackHash);
      if (sealMismatch) {
        logger.warn(
          { asset, clientAddr, feedbackIndex: feedbackIndex.toString(), responder: row.responder },
          "Orphan response seal_hash mismatch with parent feedback during replay"
        );
      }

      const responseStatus = classifyResponseStatus(true, sealMismatch);
      await client.query(
        `INSERT INTO feedback_responses (id, response_id, asset, client_address, feedback_index, responder, response_uri, response_hash, seal_hash, running_digest, response_count, block_slot, tx_index, event_ordinal, tx_signature, created_at, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
         ON CONFLICT (id) DO UPDATE SET
           response_uri = EXCLUDED.response_uri,
           response_hash = EXCLUDED.response_hash,
           seal_hash = EXCLUDED.seal_hash,
           running_digest = EXCLUDED.running_digest,
           response_count = EXCLUDED.response_count,
           block_slot = EXCLUDED.block_slot,
           tx_index = EXCLUDED.tx_index,
           event_ordinal = EXCLUDED.event_ordinal,
           tx_signature = EXCLUDED.tx_signature,
           created_at = EXCLUDED.created_at,
           status = EXCLUDED.status`,
        [
          row.id,
          null,
          row.asset,
          row.client_address,
          row.feedback_index?.toString?.() ?? row.feedback_index,
          row.responder,
          row.response_uri,
          row.response_hash,
          row.seal_hash,
          row.running_digest,
          row.response_count?.toString?.() ?? row.response_count ?? "0",
          row.block_slot?.toString?.() ?? row.block_slot,
          row.tx_index ?? null,
          row.event_ordinal ?? null,
          row.tx_signature,
          row.created_at instanceof Date ? row.created_at.toISOString() : row.created_at,
          responseStatus,
        ]
      );
      await client.query(`DELETE FROM orphan_responses WHERE id = $1`, [row.id]);
    }
  }

  private async reconcileOrphanRevocationSupabase(
    client: PoolClient,
    asset: string,
    clientAddr: string,
    feedbackIndex: bigint,
    _feedbackHash: string | null
  ): Promise<void> {
    const existing = await client.query(
      `SELECT feedback_hash, status
       FROM revocations
       WHERE asset = $1 AND client_address = $2 AND feedback_index = $3
       LIMIT 1`,
      [asset, clientAddr, feedbackIndex.toString()]
    );
    if ((existing.rowCount ?? existing.rows.length) === 0) return;
    const row = existing.rows[0];
    if (!row) return;
    if (row.status !== "ORPHANED") return;

    await client.query(
      `UPDATE revocations
       SET status = 'PENDING'
       WHERE asset = $1 AND client_address = $2 AND feedback_index = $3 AND status = 'ORPHANED'`,
      [asset, clientAddr, feedbackIndex.toString()]
    );
  }

  private async insertFeedbackSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const client_addr = data.clientAddress?.toBase58?.() || data.clientAddress;
    const feedbackIndex = BigInt(data.feedbackIndex?.toString() || "0");
    if (!(await this.hasAgentSupabase(client, asset))) {
      await this.upsertOrphanFeedbackSupabase(client, data, ctx);
      logger.warn(
        { asset, client: client_addr, feedbackIndex: feedbackIndex.toString() },
        "Agent missing for feedback; stored orphan feedback (batch)"
      );
      return;
    }
    const id = `${asset}:${client_addr}:${feedbackIndex}`;

    const feedbackHash = toRequiredHashHex(data.sealHash);
    const runningDigest = data.newFeedbackDigest
      ? Buffer.from(data.newFeedbackDigest)
      : null;

    const insertResult = await client.query(`
      INSERT INTO feedbacks (id, feedback_id, asset, client_address, feedback_index, value, value_decimals, score, tag1, tag2, endpoint, feedback_uri, feedback_hash, running_digest, is_revoked, block_slot, tx_index, event_ordinal, tx_signature, created_at, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, 'PENDING')
      ON CONFLICT (id) DO UPDATE SET
        feedback_hash = EXCLUDED.feedback_hash,
        running_digest = EXCLUDED.running_digest
    `, [id, null, asset, client_addr, feedbackIndex.toString(),
        data.value?.toString() || "0", data.valueDecimals || 0, data.score,
        data.tag1 || null, data.tag2 || null, data.endpoint || null,
        data.feedbackUri ?? null, feedbackHash, runningDigest,
        false, ctx.slot.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString()]);

    if ((insertResult.rowCount ?? 0) !== 1) {
      logger.warn({ asset, feedbackIndex: feedbackIndex.toString(), rowCount: insertResult.rowCount }, "Unexpected batch feedback upsert result");
    }
    // Update agent stats (consistent with supabase.ts)
    const baseUpdate = `
      feedback_count = COALESCE((
        SELECT COUNT(*)::int FROM feedbacks WHERE asset = $2 AND NOT is_revoked
      ), 0),
      raw_avg_score = COALESCE((
        SELECT ROUND(AVG(score))::smallint FROM feedbacks WHERE asset = $2 AND NOT is_revoked
      ), 0),
      updated_at = $1
    `;
    if (data.atomEnabled) {
      await client.query(
        `UPDATE agents SET
           trust_tier = $3, quality_score = $4, confidence = $5,
           risk_score = $6, diversity_ratio = $7, ${baseUpdate}
         WHERE asset = $2`,
        [ctx.blockTime.toISOString(), asset,
         data.newTrustTier, data.newQualityScore, data.newConfidence,
         data.newRiskScore, data.newDiversityRatio]
      );
    } else {
      await client.query(
        `UPDATE agents SET ${baseUpdate} WHERE asset = $2`,
        [ctx.blockTime.toISOString(), asset]
      );
    }
    await this.replayOrphanResponsesSupabase(client, asset, client_addr, feedbackIndex, feedbackHash);
    await this.reconcileOrphanRevocationSupabase(client, asset, client_addr, feedbackIndex, feedbackHash);
  }

  private async insertRevocationSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const client_addr = data.clientAddress?.toBase58?.() || data.clientAddress;
    const feedbackIndex = BigInt(data.feedbackIndex?.toString() || "0");
    const id = `${asset}:${client_addr}:${feedbackIndex}`;

    const feedbackCheck = await client.query(
      `SELECT id, feedback_hash FROM feedbacks WHERE id = $1 LIMIT 1`, [id]
    );
    const hasFeedback = (feedbackCheck.rowCount ?? feedbackCheck.rows.length) > 0;
    const feedbackHash = feedbackCheck.rows[0]?.feedback_hash ?? null;
    const revokeSealHash = toRequiredHashHex(data.sealHash);
    const sealMismatch = hasFeedback && !hashesMatchHex(revokeSealHash, feedbackHash);
    const revokeStatus = classifyRevocationStatus(hasFeedback, sealMismatch);
    if (sealMismatch) {
      logger.warn(
        { asset, client: client_addr, feedbackIndex: feedbackIndex.toString() },
        "Revocation seal_hash mismatch with parent feedback"
      );
    }

    // Insert into revocations table
    const revokeDigest = data.newRevokeDigest
      ? Buffer.from(data.newRevokeDigest)
      : null;
    const revokeResult = await client.query(`
      INSERT INTO revocations (id, revocation_id, asset, client_address, feedback_index, feedback_hash, slot, original_score, atom_enabled, had_impact, running_digest, revoke_count, tx_index, event_ordinal, tx_signature, created_at, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
      ON CONFLICT (asset, client_address, feedback_index) DO UPDATE SET
        feedback_hash = EXCLUDED.feedback_hash,
        slot = EXCLUDED.slot,
        original_score = EXCLUDED.original_score,
        atom_enabled = EXCLUDED.atom_enabled,
        had_impact = EXCLUDED.had_impact,
        running_digest = EXCLUDED.running_digest,
        revoke_count = EXCLUDED.revoke_count,
        tx_index = EXCLUDED.tx_index,
        event_ordinal = EXCLUDED.event_ordinal,
        tx_signature = EXCLUDED.tx_signature,
        created_at = EXCLUDED.created_at,
        status = EXCLUDED.status
      ${REVOCATION_CONFLICT_UPDATE_WHERE_SQL}
    `, [id, null, asset, client_addr, feedbackIndex.toString(), revokeSealHash,
        (data.slot || ctx.slot).toString(), data.originalScore ?? null,
        data.atomEnabled || false, data.hadImpact || false,
        revokeDigest, (data.newRevokeCount || 0).toString(),
        ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString(),
        revokeStatus]);

    // Re-aggregate agent stats only for valid non-orphan revocations
    if (revokeStatus !== "ORPHANED" && (revokeResult.rowCount ?? 0) > 0) {
      await client.query(`
        UPDATE feedbacks SET is_revoked = true, revoked_at = $1
        WHERE asset = $2 AND client_address = $3 AND feedback_index = $4
      `, [ctx.blockTime.toISOString(), asset, client_addr, feedbackIndex.toString()]);
      const baseUpdate = `
        feedback_count = COALESCE((
          SELECT COUNT(*)::int FROM feedbacks WHERE asset = $2 AND NOT is_revoked
        ), 0),
        raw_avg_score = COALESCE((
          SELECT ROUND(AVG(score))::smallint FROM feedbacks WHERE asset = $2 AND NOT is_revoked
        ), 0),
        updated_at = $1
      `;
      await client.query(
        `UPDATE agents SET ${baseUpdate} WHERE asset = $2`,
        [ctx.blockTime.toISOString(), asset]
      );

      // Update ATOM metrics if applicable
      if (data.atomEnabled && data.hadImpact) {
        await client.query(`
          UPDATE agents SET
            trust_tier = $3, quality_score = $4, confidence = $5, updated_at = $1
          WHERE asset = $2
        `, [ctx.blockTime.toISOString(), asset,
            data.newTrustTier, data.newQualityScore, data.newConfidence]);
      }
    }
  }

  private async insertResponseSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const client_addr = data.client?.toBase58?.() || data.client;
    const responder = data.responder?.toBase58?.() || data.responder;
    const feedbackIndex = BigInt(data.feedbackIndex?.toString() || "0");
    const responseCount = BigInt(data.newResponseCount?.toString() || "0");
    const id = `${asset}:${client_addr}:${feedbackIndex}:${responder}:${ctx.signature}`;

    const responseHash = toRequiredHashHex(data.responseHash);
    const responseRunningDigest = data.newResponseDigest
      ? Buffer.from(data.newResponseDigest)
      : null;

    const feedbackCheck = await client.query(
      `SELECT id, feedback_hash FROM feedbacks WHERE asset = $1 AND client_address = $2 AND feedback_index = $3 LIMIT 1`,
      [asset, client_addr, feedbackIndex.toString()]
    );
    const hasFeedback = (feedbackCheck.rowCount ?? feedbackCheck.rows.length) > 0;

    if (!hasFeedback) {
      await this.upsertOrphanResponseSupabase(client, data, ctx);
      return;
    }

    const eventHash = toRequiredHashHex(data.sealHash);
    const feedbackHash = feedbackCheck.rows[0]?.feedback_hash ?? null;
    const sealMismatch = !hashesMatchHex(eventHash, feedbackHash);
    const responseStatus = classifyResponseStatus(true, sealMismatch);
    if (sealMismatch) {
      logger.warn(
        { asset, client: client_addr, feedbackIndex: feedbackIndex.toString(), responder },
        "Response seal_hash mismatch with parent feedback"
      );
    }
    await client.query(
      `INSERT INTO feedback_responses (id, response_id, asset, client_address, feedback_index, responder, response_uri, response_hash, seal_hash, running_digest, response_count, block_slot, tx_index, event_ordinal, tx_signature, created_at, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
       ON CONFLICT (id) DO UPDATE SET
         response_uri = EXCLUDED.response_uri,
         response_hash = EXCLUDED.response_hash,
         seal_hash = EXCLUDED.seal_hash,
         running_digest = EXCLUDED.running_digest,
         response_count = EXCLUDED.response_count,
         block_slot = EXCLUDED.block_slot,
         tx_index = EXCLUDED.tx_index,
         event_ordinal = EXCLUDED.event_ordinal,
         tx_signature = EXCLUDED.tx_signature,
         status = EXCLUDED.status`,
      [id, null, asset, client_addr, feedbackIndex.toString(), responder,
        data.responseUri || "", responseHash, eventHash, responseRunningDigest, responseCount.toString(),
        ctx.slot.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString(), responseStatus]
    );
  }

  private async insertValidationRequestSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const validator = data.validatorAddress?.toBase58?.() || data.validatorAddress;
    const requester = data.requester?.toBase58?.() || data.requester;
    const nonce = BigInt(data.nonce?.toString() || "0");
    const id = `${asset}:${validator}:${nonce}`;

    await client.query(`
      INSERT INTO validations (id, asset, validator_address, nonce, requester, request_uri, request_hash, block_slot, tx_index, event_ordinal, tx_signature, created_at, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'PENDING')
      ON CONFLICT (id) DO UPDATE SET
        requester = EXCLUDED.requester,
        request_uri = EXCLUDED.request_uri,
        request_hash = EXCLUDED.request_hash,
        block_slot = EXCLUDED.block_slot,
        tx_index = EXCLUDED.tx_index,
        event_ordinal = EXCLUDED.event_ordinal,
        tx_signature = EXCLUDED.tx_signature
    `, [id, asset, validator, nonce.toString(), requester, data.requestUri || "",
        data.requestHash ? Buffer.from(data.requestHash).toString("hex") : null,
        ctx.slot.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString()]);
  }

  private async updateValidationResponseSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const validator = data.validatorAddress?.toBase58?.() || data.validatorAddress;
    const nonce = BigInt(data.nonce?.toString() || "0");
    const id = `${asset}:${validator}:${nonce}`;

    // Use UPSERT to handle case where request wasn't indexed (DB reset, late start, etc.)
    await client.query(`
      INSERT INTO validations (id, asset, validator_address, nonce, response, response_uri, response_hash, tag, status, block_slot, tx_index, event_ordinal, tx_signature, created_at, updated_at, chain_status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14, 'PENDING')
      ON CONFLICT (id) DO UPDATE SET
        response = EXCLUDED.response,
        response_uri = EXCLUDED.response_uri,
        response_hash = EXCLUDED.response_hash,
        tag = EXCLUDED.tag,
        status = EXCLUDED.status,
        block_slot = EXCLUDED.block_slot,
        tx_index = EXCLUDED.tx_index,
        event_ordinal = EXCLUDED.event_ordinal,
        tx_signature = EXCLUDED.tx_signature,
        updated_at = EXCLUDED.updated_at
    `, [id, asset, validator, nonce.toString(), data.response || 0, data.responseUri || "",
        data.responseHash ? Buffer.from(data.responseHash).toString("hex") : null,
        data.tag || "", "RESPONDED",
        ctx.slot.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString()]);
  }

  // v0.6.0: RegistryInitialized replaces BaseRegistryCreated/UserRegistryCreated
  private async insertCollectionSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const collection = data.collection?.toBase58?.() || data.collection;
    const authority = data.authority?.toBase58?.() || data.authority;

    await client.query(`
      INSERT INTO collections (collection, registry_type, authority, created_at, status)
      VALUES ($1, $2, $3, $4, 'PENDING')
      ON CONFLICT (collection) DO UPDATE SET
        registry_type = EXCLUDED.registry_type,
        authority = EXCLUDED.authority
    `, [collection, "BASE", authority, ctx.blockTime.toISOString()]);
  }

  private async updateAgentUriSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    await client.query(`
      UPDATE agents SET agent_uri = $1, updated_at = $2 WHERE asset = $3
    `, [data.newUri || "", ctx.blockTime.toISOString(), asset]);
  }

  private async updateAgentWalletSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const walletRaw = data.newWallet?.toBase58?.() || data.newWallet;
    const wallet = walletRaw === DEFAULT_PUBKEY ? null : walletRaw;
    await client.query(`
      UPDATE agents SET agent_wallet = $1, updated_at = $2 WHERE asset = $3
    `, [wallet, ctx.blockTime.toISOString(), asset]);
  }

  private async updateWalletResetOnOwnerSyncSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const walletRaw = data.newWallet?.toBase58?.() || data.newWallet;
    const wallet = walletRaw === DEFAULT_PUBKEY ? null : walletRaw;
    const ownerAfterSync = data.ownerAfterSync?.toBase58?.() || data.ownerAfterSync;
    await client.query(
      `UPDATE agents SET owner = $1, agent_wallet = $2, updated_at = $3 WHERE asset = $4`,
      [ownerAfterSync, wallet, ctx.blockTime.toISOString(), asset]
    );
  }

  private async updateCollectionPointerSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const setBy = data.setBy?.toBase58?.() || data.setBy;
    const pointer = data.col || "";
    const lock = typeof data.lock === "boolean" ? data.lock : null;
    const previousResult = await client.query(
      `SELECT canonical_col AS prev_col, creator AS prev_creator
       FROM agents
       WHERE asset = $1
       LIMIT 1`,
      [asset]
    );
    const previous = previousResult.rows[0] as
      | { prev_col?: string | null; prev_creator?: string | null }
      | undefined;
    const prevCol = previous?.prev_col ?? "";
    const prevCreator = previous?.prev_creator ?? setBy;

    await client.query(
      `WITH previous AS (
         SELECT canonical_col AS prev_col, creator AS prev_creator
         FROM agents
         WHERE asset = $1
       ),
       resolved AS (
         SELECT COALESCE((SELECT prev_creator FROM previous), $3::text) AS creator_key
       ),
       updated AS (
         UPDATE agents
         SET canonical_col = $2,
             creator = COALESCE(creator, (SELECT creator_key FROM resolved)),
             updated_at = $5,
             col_locked = COALESCE($8, col_locked)
         WHERE asset = $1
         RETURNING 1
       ),
       inserted AS (
         INSERT INTO collection_pointers (
           col, creator, first_seen_asset, first_seen_at, first_seen_slot, first_seen_tx_signature,
           last_seen_at, last_seen_slot, last_seen_tx_index, last_seen_tx_signature, asset_count
         )
         SELECT $2, (SELECT creator_key FROM resolved), $1, $5, $4, $6, $5, $4, $7, $6, 1
         WHERE EXISTS (SELECT 1 FROM updated)
         ON CONFLICT (col, creator) DO NOTHING
       ),
       dec_old AS (
         UPDATE collection_pointers cp
         SET asset_count = GREATEST(0, cp.asset_count - 1)
         WHERE cp.col = COALESCE((SELECT prev_col FROM previous), '')
           AND cp.creator = COALESCE((SELECT prev_creator FROM previous), '')
           AND cp.col <> ''
           AND (cp.col <> $2 OR cp.creator <> (SELECT creator_key FROM resolved))
           AND EXISTS (SELECT 1 FROM updated)
         RETURNING 1
       )
       UPDATE collection_pointers cp
       SET last_seen_at = CASE
             WHEN cp.last_seen_slot IS NULL
               OR $4 > cp.last_seen_slot
               OR (
                 $4 = cp.last_seen_slot
                 AND (
                   COALESCE($7, -1) > COALESCE(cp.last_seen_tx_index, -1)
                   OR (
                     COALESCE($7, -1) = COALESCE(cp.last_seen_tx_index, -1)
                     AND COALESCE(cp.last_seen_tx_signature, '') <= $6
                   )
                 )
               )
             THEN $5
             ELSE cp.last_seen_at
           END,
           last_seen_slot = CASE
             WHEN cp.last_seen_slot IS NULL
               OR $4 > cp.last_seen_slot
               OR (
                 $4 = cp.last_seen_slot
                 AND (
                   COALESCE($7, -1) > COALESCE(cp.last_seen_tx_index, -1)
                   OR (
                     COALESCE($7, -1) = COALESCE(cp.last_seen_tx_index, -1)
                     AND COALESCE(cp.last_seen_tx_signature, '') <= $6
                   )
                 )
               )
             THEN $4
             ELSE cp.last_seen_slot
           END,
           last_seen_tx_index = CASE
             WHEN cp.last_seen_slot IS NULL
               OR $4 > cp.last_seen_slot
               OR (
                 $4 = cp.last_seen_slot
                 AND (
                   COALESCE($7, -1) > COALESCE(cp.last_seen_tx_index, -1)
                   OR (
                     COALESCE($7, -1) = COALESCE(cp.last_seen_tx_index, -1)
                     AND COALESCE(cp.last_seen_tx_signature, '') <= $6
                   )
                 )
               )
             THEN $7
             ELSE cp.last_seen_tx_index
           END,
           last_seen_tx_signature = CASE
             WHEN cp.last_seen_slot IS NULL
               OR $4 > cp.last_seen_slot
               OR (
                 $4 = cp.last_seen_slot
                 AND (
                   COALESCE($7, -1) > COALESCE(cp.last_seen_tx_index, -1)
                   OR (
                     COALESCE($7, -1) = COALESCE(cp.last_seen_tx_index, -1)
                     AND COALESCE(cp.last_seen_tx_signature, '') <= $6
                   )
                 )
               )
             THEN $6
             ELSE cp.last_seen_tx_signature
           END,
           asset_count = cp.asset_count + CASE
             WHEN COALESCE((SELECT prev_col FROM previous), '') <> $2
               OR COALESCE((SELECT prev_creator FROM previous), '') <> (SELECT creator_key FROM resolved)
             THEN 1 ELSE 0 END
       WHERE cp.col = $2
         AND cp.creator = (SELECT creator_key FROM resolved)
         AND EXISTS (SELECT 1 FROM updated)
      `,
      [asset, pointer, setBy, ctx.slot.toString(), ctx.blockTime.toISOString(), ctx.signature, ctx.txIndex ?? null, lock]
    );

    const currentResult = await client.query(
      `SELECT canonical_col AS col, COALESCE(creator, owner) AS creator
       FROM agents
       WHERE asset = $1
       LIMIT 1`,
      [asset]
    );
    const current = currentResult.rows[0] as { col?: string | null; creator?: string | null } | undefined;
    const currentCol = current?.col ?? "";
    const currentCreator = current?.creator ?? setBy;

    if (currentCol) {
      await this.recomputeCollectionPointerAssetCount(client, currentCol, currentCreator);
    }
    if (prevCol && (prevCol !== currentCol || prevCreator !== currentCreator)) {
      await this.recomputeCollectionPointerAssetCount(client, prevCol, prevCreator);
    }
  }

  private async recomputeCollectionPointerAssetCount(client: PoolClient, col: string, creator: string): Promise<void> {
    if (!col || !creator) return;
    await client.query(
      `UPDATE collection_pointers cp
       SET asset_count = sub.cnt
       FROM (
         SELECT $1::text AS col,
                $2::text AS creator,
                COUNT(*)::bigint AS cnt
         FROM agents
         WHERE canonical_col = $1
           AND COALESCE(creator, owner) = $2
       ) sub
       WHERE cp.col = sub.col
         AND cp.creator = sub.creator`,
      [col, creator]
    );
  }

  private async updateParentAssetSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const parentAsset = data.parentAsset?.toBase58?.() || data.parentAsset;
    const parentCreator = data.parentCreator?.toBase58?.() || data.parentCreator;
    const lock = typeof data.lock === "boolean" ? data.lock : null;
    await client.query(
      `UPDATE agents
       SET parent_asset = $1,
           parent_creator = $2,
           updated_at = $3,
           parent_locked = COALESCE($5, parent_locked)
       WHERE asset = $4`,
      [parentAsset, parentCreator, ctx.blockTime.toISOString(), asset, lock]
    );
  }

  private async updateAtomEnabledSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    await client.query(`
      UPDATE agents SET atom_enabled = true, updated_at = $1 WHERE asset = $2
    `, [ctx.blockTime.toISOString(), asset]);
  }

  private async insertMetadataSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const key = data.key || "";

    // Prevent collision with system-derived URI metadata (reserved namespace)
    if (key.startsWith("_uri:")) {
      logger.debug({ asset, key }, "Skipping reserved _uri: metadata key");
      return;
    }

    // Strip null bytes and compress (consistent with supabase.ts)
    const rawValue = data.value ? stripNullBytes(data.value) : Buffer.alloc(0);
    const compressedValue = await compressForStorage(rawValue);

    // Calculate key_hash from key (sha256(key)[0..16])
    const keyHash = createHash("sha256").update(key).digest().slice(0, 16).toString("hex");
    const id = `${asset}:${keyHash}`;

    await client.query(`
      INSERT INTO metadata (id, asset, key, key_hash, value, immutable, block_slot, tx_index, event_ordinal, tx_signature, created_at, updated_at, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'PENDING')
      ON CONFLICT (id) DO UPDATE SET
        value = EXCLUDED.value,
        immutable = metadata.immutable OR EXCLUDED.immutable,
        block_slot = EXCLUDED.block_slot,
        tx_index = EXCLUDED.tx_index,
        event_ordinal = EXCLUDED.event_ordinal,
        tx_signature = EXCLUDED.tx_signature,
        updated_at = EXCLUDED.updated_at
      WHERE NOT metadata.immutable
    `, [id, asset, key, keyHash, compressedValue, data.immutable || false,
        ctx.slot.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString(),
        ctx.blockTime.toISOString()]);
  }

  private async deleteMetadataSupabase(client: PoolClient, data: EventData, _ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const key = data.key || "";
    await client.query(`DELETE FROM metadata WHERE asset = $1 AND key = $2`, [asset, key]);
  }

  private async updateAgentOwnerSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const newOwner = data.newOwner?.toBase58?.() || data.newOwner;
    await client.query(
      `UPDATE agents SET owner = $1, updated_at = $2 WHERE asset = $3`,
      [newOwner, ctx.blockTime.toISOString(), asset]
    );
  }

  /**
   * Flush events to Prisma in a single transaction
   * Note: Prisma batch mode not implemented - local mode uses individual handleEventAtomic calls
   */
  private async flushToPrisma(_events: BatchEvent[], lastCtx: BatchEvent["ctx"] | null): Promise<void> {
    if (!this.prisma) return;

    await this.prisma.$transaction(async (tx) => {
      // Note: Batch mode is optimized for Supabase. Local mode still uses individual inserts
      // via the standard poller path. This method only updates the cursor.

      if (lastCtx) {
        if (await trySaveLocalIndexerStateWithSql(tx, {
          signature: lastCtx.signature,
          slot: lastCtx.slot,
          txIndex: lastCtx.txIndex ?? null,
          source: "poller",
          updatedAt: lastCtx.blockTime,
        })) {
          return;
        }

        await tx.indexerState.upsert({
          where: { id: "main" },
          create: {
            id: "main",
            lastSignature: lastCtx.signature,
            lastSlot: lastCtx.slot,
            lastTxIndex: lastCtx.txIndex ?? null,
            source: "poller",
          },
          update: {
            lastSignature: lastCtx.signature,
            lastSlot: lastCtx.slot,
            lastTxIndex: lastCtx.txIndex ?? null,
            source: "poller",
          },
        });
      }
    });
  }

  getStats(): BatchStats {
    return {
      eventsBuffered: this.stats.eventsBuffered,
      eventsFlushed: this.stats.eventsFlushed,
      flushCount: this.stats.flushCount,
      avgFlushTime: this.stats.flushCount > 0
        ? Math.round(this.stats.totalFlushTime / this.stats.flushCount)
        : 0,
      rpcBatchCount: 0,
      avgRpcBatchTime: 0
    };
  }

  /**
   * Get buffer size
   */
  get size(): number {
    return this.buffer.length;
  }
}
