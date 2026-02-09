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
import { compressForStorage } from "../utils/compression.js";
import { stripNullBytes } from "../utils/sanitize.js";

const logger = createChildLogger("batch-processor");

// Batch configuration
const BATCH_SIZE_RPC = 100;        // Max transactions per RPC call
const BATCH_SIZE_DB = 500;         // Max events per DB transaction
const FLUSH_INTERVAL_MS = 500;     // Flush every 500ms even if batch not full
const MAX_PARALLEL_RPC = 3;        // Parallel RPC batch requests
const MAX_DEAD_LETTER = 10000;     // Max events in dead letter queue (memory protection)
const DEAD_LETTER_BACKPRESSURE = 0.8; // Warn at 80% capacity
const DEAD_LETTER_MAX_AGE_MS = 5 * 60 * 1000; // Evict entries older than 5 minutes

// Helper to check for all-zero hash (empty hash should be NULL, not "00...00")
function isAllZeroHash(hash: Uint8Array | undefined | null): boolean {
  if (!hash) return true;
  return hash.every(b => b === 0);
}

// EventData type for batch event data - uses Record for type safety while allowing runtime values
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type EventData = Record<string, any>;

export interface BatchEvent {
  type: string;
  data: EventData;
  ctx: {
    signature: string;
    slot: bigint;
    blockTime: Date;
    txIndex?: number;
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

/**
 * Batch RPC Fetcher - Fetches multiple transactions in parallel
 */
export class BatchRpcFetcher {
  private connection: Connection;
  private stats = { batchCount: 0, totalTime: 0 };

  constructor(connection: Connection) {
    this.connection = connection;
  }

  /**
   * Fetch multiple transactions in a single RPC call
   * Returns Map<signature, ParsedTransaction>
   */
  async fetchTransactions(
    signatures: string[]
  ): Promise<Map<string, ParsedTransactionWithMeta>> {
    const results = new Map<string, ParsedTransactionWithMeta>();

    // Split into chunks of BATCH_SIZE_RPC
    const chunks: string[][] = [];
    for (let i = 0; i < signatures.length; i += BATCH_SIZE_RPC) {
      chunks.push(signatures.slice(i, i + BATCH_SIZE_RPC));
    }

    const startTime = Date.now();

    // Process chunks with limited parallelism
    for (let i = 0; i < chunks.length; i += MAX_PARALLEL_RPC) {
      const parallelChunks = chunks.slice(i, i + MAX_PARALLEL_RPC);

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

    try {
      // Use getParsedTransactions (plural) for batch fetching
      const transactions = await this.connection.getParsedTransactions(
        signatures,
        { maxSupportedTransactionVersion: 0 }
      );

      for (let i = 0; i < signatures.length; i++) {
        const tx = transactions[i];
        if (tx) {
          results.set(signatures[i], tx);
        }
      }
    } catch (error) {
      logger.warn({ error, count: signatures.length }, "Batch RPC fetch failed, falling back to individual");

      // Fallback to individual fetches
      for (const sig of signatures) {
        try {
          const tx = await this.connection.getParsedTransaction(sig, {
            maxSupportedTransactionVersion: 0
          });
          if (tx) {
            results.set(sig, tx);
          }
        } catch (e) {
          logger.warn({ signature: sig, error: e }, "Individual fetch failed");
        }
      }
    }

    return results;
  }

  getStats() {
    return {
      batchCount: this.stats.batchCount,
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
  private pool: Pool | null = null;
  private prisma: PrismaClient | null = null;
  private lastCtx: BatchEvent["ctx"] | null = null;
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
   * Evict dead letter entries older than DEAD_LETTER_MAX_AGE_MS
   */
  private evictStaleDeadLetters(): void {
    const now = Date.now();
    const before = this.deadLetterQueue.length;
    this.deadLetterQueue = this.deadLetterQueue.filter(
      entry => (now - entry.addedAt) < DEAD_LETTER_MAX_AGE_MS
    );
    const evicted = before - this.deadLetterQueue.length;
    if (evicted > 0) {
      logger.warn({ evicted, remaining: this.deadLetterQueue.length },
        "Evicted stale dead letter entries (older than 5 minutes)");
    }
  }

  /**
   * Add event to buffer
   * Auto-flushes when buffer is full
   */
  async addEvent(event: BatchEvent): Promise<void> {
    // Backpressure: evict stale dead letter entries and warn if near capacity
    if (this.deadLetterQueue.length > 0) {
      this.evictStaleDeadLetters();
    }
    const dlqUtilization = this.deadLetterQueue.length / MAX_DEAD_LETTER;
    if (dlqUtilization > DEAD_LETTER_BACKPRESSURE) {
      logger.warn({
        deadLetterSize: this.deadLetterQueue.length,
        maxCapacity: MAX_DEAD_LETTER,
        utilization: `${Math.round(dlqUtilization * 100)}%`
      }, "Dead letter queue backpressure: queue is above 80% capacity, DB writes may be failing");
    }

    this.buffer.push(event);
    this.lastCtx = event.ctx;
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

  /**
   * Force flush all buffered events
   */
  async flush(): Promise<void> {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }

    if (this.buffer.length === 0 || this.flushInProgress) {
      return;
    }

    this.flushInProgress = true;
    const eventsToFlush = [...this.buffer];
    const lastCtx = this.lastCtx;
    this.buffer = [];
    this.lastCtx = null;

    const startTime = Date.now();

    try {
      if (config.dbMode === "supabase" && this.pool) {
        await this.flushToSupabase(eventsToFlush, lastCtx);
      } else if (this.prisma) {
        await this.flushToPrisma(eventsToFlush, lastCtx);
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
        // Move to dead letter queue after max retries (poison pill protection)
        logger.warn({
          eventCount: eventsToFlush.length,
          retryCount: this.retryCount
        }, "Max retries exceeded, moving events to dead letter queue");

        // Evict stale entries before adding new ones
        this.evictStaleDeadLetters();

        // Memory protection: cap dead letter queue size
        const now = Date.now();
        const spaceAvailable = MAX_DEAD_LETTER - this.deadLetterQueue.length;
        if (spaceAvailable <= 0) {
          logger.error({ dropped: eventsToFlush.length }, "Dead letter queue full, dropping events");
        } else if (eventsToFlush.length > spaceAvailable) {
          const toKeep = eventsToFlush.slice(0, spaceAvailable);
          const dropped = eventsToFlush.length - toKeep.length;
          this.deadLetterQueue.push(...toKeep.map(e => ({ event: e, addedAt: now })));
          logger.error({ dropped, kept: toKeep.length }, "Dead letter queue nearly full, some events dropped");
        } else {
          this.deadLetterQueue.push(...eventsToFlush.map(e => ({ event: e, addedAt: now })));
        }
        this.stats.deadLettered += eventsToFlush.length;
        this.retryCount = 0;
        // Don't re-throw - continue processing new events
      } else {
        // Re-add events to buffer for retry (limited retries)
        this.buffer = [...eventsToFlush, ...this.buffer];
        // Wait before next retry
        await new Promise(r => setTimeout(r, RETRY_DELAY_MS * this.retryCount));
        throw error;
      }
    } finally {
      this.flushInProgress = false;
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

  /**
   * Flush events to Supabase in a single transaction
   * Collects URIs for async metadata extraction after commit
   */
  private async flushToSupabase(events: BatchEvent[], lastCtx: BatchEvent["ctx"] | null): Promise<void> {
    if (!this.pool) return;

    // Collect URIs for post-commit metadata extraction
    const uriTasks: Array<{ assetId: string; uri: string }> = [];

    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");

      for (const event of events) {
        await this.insertEventSupabase(client, event);

        // Collect URIs from agent registration and URI update events
        if (event.type === "AgentRegistered" && event.data.agentUri) {
          const asset = event.data.asset?.toBase58?.() || event.data.asset;
          uriTasks.push({ assetId: asset, uri: event.data.agentUri });
        } else if (event.type === "UriUpdated" && event.data.newUri) {
          const asset = event.data.asset?.toBase58?.() || event.data.asset;
          uriTasks.push({ assetId: asset, uri: event.data.newUri });
        }
      }

      // Update cursor with last event context
      if (lastCtx) {
        await client.query(`
          INSERT INTO indexer_state (id, last_signature, last_slot, source, updated_at)
          VALUES ('main', $1, $2, 'poller', NOW())
          ON CONFLICT (id) DO UPDATE SET
            last_signature = EXCLUDED.last_signature,
            last_slot = EXCLUDED.last_slot,
            source = EXCLUDED.source,
            updated_at = NOW()
          WHERE indexer_state.last_slot < EXCLUDED.last_slot OR indexer_state.last_slot IS NULL
        `, [lastCtx.signature, lastCtx.slot.toString()]);
      }

      await client.query("COMMIT");

      // After successful commit, queue URI metadata extraction (fire-and-forget)
      if (uriTasks.length > 0 && config.metadataIndexMode !== "off") {
        metadataQueue.addBatch(uriTasks);
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
        await this.insertValidationRequestSupabase(client, data, ctx);
        break;
      case "ValidationResponded":
        await this.updateValidationResponseSupabase(client, data, ctx);
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
      case "AtomEnabled":
        await this.updateAtomEnabledSupabase(client, data, ctx);
        break;
      case "MetadataSet":
        await this.insertMetadataSupabase(client, data, ctx);
        break;
      // Add other event types as needed
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
      INSERT INTO agents (asset, owner, agent_uri, collection, atom_enabled, block_slot, tx_index, tx_signature, created_at, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'PENDING')
      ON CONFLICT (asset) DO UPDATE SET
        owner = EXCLUDED.owner,
        agent_uri = EXCLUDED.agent_uri,
        atom_enabled = EXCLUDED.atom_enabled,
        block_slot = EXCLUDED.block_slot,
        tx_index = EXCLUDED.tx_index,
        updated_at = $10
    `, [asset, owner, data.agentUri || null, collection, data.atomEnabled || false,
        ctx.slot.toString(), ctx.txIndex || null, ctx.signature, ctx.blockTime.toISOString(),
        ctx.blockTime.toISOString()]);
  }

  private async insertFeedbackSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const client_addr = data.clientAddress?.toBase58?.() || data.clientAddress;
    const feedbackIndex = BigInt(data.feedbackIndex?.toString() || "0");
    const id = `${asset}:${client_addr}:${feedbackIndex}`;

    // Convert all-zero hash to NULL (consistent with supabase.ts)
    const feedbackHash = !isAllZeroHash(data.sealHash)
      ? Buffer.from(data.sealHash).toString("hex")
      : null;
    const runningDigest = data.newFeedbackDigest
      ? Buffer.from(data.newFeedbackDigest)
      : null;

    const insertResult = await client.query(`
      INSERT INTO feedbacks (id, asset, client_address, feedback_index, value, value_decimals, score, tag1, tag2, endpoint, feedback_uri, feedback_hash, running_digest, is_revoked, block_slot, tx_index, tx_signature, created_at, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, 'PENDING')
      ON CONFLICT (id) DO NOTHING
    `, [id, asset, client_addr, feedbackIndex.toString(),
        data.value?.toString() || "0", data.valueDecimals || 0, data.score,
        data.tag1 || null, data.tag2 || null, data.endpoint || null,
        data.feedbackUri || null, feedbackHash, runningDigest,
        false, ctx.slot.toString(), ctx.txIndex || null, ctx.signature, ctx.blockTime.toISOString()]);

    if (insertResult.rowCount === 0) return;

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
  }

  private async insertRevocationSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const client_addr = data.clientAddress?.toBase58?.() || data.clientAddress;
    const feedbackIndex = BigInt(data.feedbackIndex?.toString() || "0");
    const id = `${asset}:${client_addr}:${feedbackIndex}`;

    // Validate seal_hash against stored feedback
    const feedbackCheck = await client.query(
      `SELECT id, feedback_hash FROM feedbacks WHERE id = $1 LIMIT 1`, [id]
    );
    const isOrphan = feedbackCheck.rowCount === 0;

    // Mark feedback as revoked
    await client.query(`
      UPDATE feedbacks SET is_revoked = true, revoked_at = $1
      WHERE asset = $2 AND client_address = $3 AND feedback_index = $4
    `, [ctx.blockTime.toISOString(), asset, client_addr, feedbackIndex.toString()]);

    // Insert into revocations table
    const revokeDigest = data.newRevokeDigest
      ? Buffer.from(data.newRevokeDigest)
      : null;
    const revokeSealHash = !isAllZeroHash(data.sealHash)
      ? Buffer.from(data.sealHash).toString("hex")
      : null;

    await client.query(`
      INSERT INTO revocations (id, asset, client_address, feedback_index, feedback_hash, slot, original_score, atom_enabled, had_impact, running_digest, revoke_count, tx_signature, created_at, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
      ON CONFLICT (asset, client_address, feedback_index) DO NOTHING
    `, [id, asset, client_addr, feedbackIndex.toString(), revokeSealHash,
        (data.slot || ctx.slot).toString(), data.originalScore ?? null,
        data.atomEnabled || false, data.hadImpact || false,
        revokeDigest, (data.newRevokeCount || 0).toString(),
        ctx.signature, ctx.blockTime.toISOString(),
        isOrphan ? "ORPHANED" : "PENDING"]);

    // Re-aggregate agent stats
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

  private async insertResponseSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const client_addr = data.client?.toBase58?.() || data.client;
    const responder = data.responder?.toBase58?.() || data.responder;
    const feedbackIndex = BigInt(data.feedbackIndex?.toString() || "0");
    const id = `${asset}:${client_addr}:${feedbackIndex}:${responder}:${ctx.signature}`;

    // Convert all-zero hash to NULL (consistent with supabase.ts)
    const responseHash = !isAllZeroHash(data.responseHash)
      ? Buffer.from(data.responseHash).toString("hex")
      : null;
    const responseRunningDigest = data.newResponseDigest
      ? Buffer.from(data.newResponseDigest)
      : null;

    await client.query(`
      INSERT INTO feedback_responses (id, asset, client_address, feedback_index, responder, response_uri, response_hash, running_digest, block_slot, tx_index, tx_signature, created_at, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'PENDING')
      ON CONFLICT (id) DO NOTHING
    `, [id, asset, client_addr, feedbackIndex.toString(), responder,
        data.responseUri || "", responseHash, responseRunningDigest,
        ctx.slot.toString(), ctx.txIndex || null, ctx.signature, ctx.blockTime.toISOString()]);
  }

  private async insertValidationRequestSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const validator = data.validatorAddress?.toBase58?.() || data.validatorAddress;
    const requester = data.requester?.toBase58?.() || data.requester;
    const nonce = BigInt(data.nonce?.toString() || "0");
    const id = `${asset}:${validator}:${nonce}`;

    await client.query(`
      INSERT INTO validations (id, asset, validator_address, nonce, requester, request_uri, request_hash, block_slot, tx_index, tx_signature, created_at, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'PENDING')
      ON CONFLICT (id) DO NOTHING
    `, [id, asset, validator, nonce.toString(), requester, data.requestUri || "",
        data.requestHash ? Buffer.from(data.requestHash).toString("hex") : null,
        ctx.slot.toString(), ctx.txIndex || null, ctx.signature, ctx.blockTime.toISOString()]);
  }

  private async updateValidationResponseSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const validator = data.validatorAddress?.toBase58?.() || data.validatorAddress;
    const nonce = BigInt(data.nonce?.toString() || "0");
    const id = `${asset}:${validator}:${nonce}`;

    // Use UPSERT to handle case where request wasn't indexed (DB reset, late start, etc.)
    await client.query(`
      INSERT INTO validations (id, asset, validator_address, nonce, response, response_uri, response_hash, tag, status, block_slot, tx_index, tx_signature, created_at, updated_at, chain_status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $13, 'PENDING')
      ON CONFLICT (id) DO UPDATE SET
        response = EXCLUDED.response,
        response_uri = EXCLUDED.response_uri,
        response_hash = EXCLUDED.response_hash,
        tag = EXCLUDED.tag,
        status = EXCLUDED.status,
        updated_at = EXCLUDED.updated_at
    `, [id, asset, validator, nonce.toString(), data.response || 0, data.responseUri || "",
        data.responseHash ? Buffer.from(data.responseHash).toString("hex") : null,
        data.tag || "", "RESPONDED",
        ctx.slot.toString(), ctx.txIndex || null, ctx.signature, ctx.blockTime.toISOString()]);
  }

  // v0.6.0: RegistryInitialized replaces BaseRegistryCreated/UserRegistryCreated
  private async insertCollectionSupabase(client: PoolClient, data: EventData, ctx: BatchEvent["ctx"]): Promise<void> {
    const collection = data.collection?.toBase58?.() || data.collection;
    const authority = data.authority?.toBase58?.() || data.authority;

    await client.query(`
      INSERT INTO collections (collection, registry_type, authority, created_at, status)
      VALUES ($1, $2, $3, $4, 'PENDING')
      ON CONFLICT (collection) DO NOTHING
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
    const wallet = data.newWallet?.toBase58?.() || data.newWallet;
    await client.query(`
      UPDATE agents SET agent_wallet = $1, updated_at = $2 WHERE asset = $3
    `, [wallet, ctx.blockTime.toISOString(), asset]);
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
      INSERT INTO metadata (id, asset, key, key_hash, value, immutable, block_slot, tx_index, tx_signature, created_at, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'PENDING')
      ON CONFLICT (id) DO UPDATE SET
        value = EXCLUDED.value,
        immutable = metadata.immutable OR EXCLUDED.immutable,
        block_slot = EXCLUDED.block_slot,
        updated_at = $11
      WHERE NOT metadata.immutable
    `, [id, asset, key, keyHash, compressedValue, data.immutable || false,
        ctx.slot.toString(), ctx.txIndex || null, ctx.signature, ctx.blockTime.toISOString(),
        ctx.blockTime.toISOString()]);
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
        await tx.indexerState.upsert({
          where: { id: "main" },
          create: {
            id: "main",
            lastSignature: lastCtx.signature,
            lastSlot: lastCtx.slot,
            source: "poller",
          },
          update: {
            lastSignature: lastCtx.signature,
            lastSlot: lastCtx.slot,
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
