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
import { createChildLogger } from "../logger.js";
import { config } from "../config.js";

const logger = createChildLogger("batch-processor");

// Batch configuration
const BATCH_SIZE_RPC = 100;        // Max transactions per RPC call
const BATCH_SIZE_DB = 500;         // Max events per DB transaction
const FLUSH_INTERVAL_MS = 500;     // Flush every 500ms even if batch not full
const MAX_PARALLEL_RPC = 3;        // Parallel RPC batch requests

export interface BatchEvent {
  type: string;
  data: Record<string, unknown>;
  ctx: {
    signature: string;
    slot: bigint;
    blockTime: Date;
    txIndex?: number;
  };
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

  private stats = {
    eventsBuffered: 0,
    eventsFlushed: 0,
    flushCount: 0,
    totalFlushTime: 0
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
      logger.error({ error, eventCount: eventsToFlush.length }, "Batch flush failed");
      // Re-add events to buffer for retry
      this.buffer = [...eventsToFlush, ...this.buffer];
      throw error;
    } finally {
      this.flushInProgress = false;
    }
  }

  /**
   * Flush events to Supabase in a single transaction
   */
  private async flushToSupabase(events: BatchEvent[], lastCtx: BatchEvent["ctx"] | null): Promise<void> {
    if (!this.pool) return;

    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");

      for (const event of events) {
        await this.insertEventSupabase(client, event);
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
      case "AgentRegisteredInRegistry":
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
      case "BaseRegistryCreated":
      case "UserRegistryCreated":
        await this.insertCollectionSupabase(client, data, ctx, type);
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
  private async insertAgentSupabase(client: PoolClient, data: any, ctx: BatchEvent["ctx"]): Promise<void> {
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
        updated_at = NOW()
    `, [asset, owner, data.agentUri || null, collection, data.atomEnabled || false,
        ctx.slot.toString(), ctx.txIndex || null, ctx.signature, ctx.blockTime.toISOString()]);
  }

  private async insertFeedbackSupabase(client: PoolClient, data: any, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const client_addr = data.clientAddress?.toBase58?.() || data.clientAddress;
    const feedbackIndex = BigInt(data.feedbackIndex?.toString() || "0");
    const id = `${asset}:${client_addr}:${feedbackIndex}`;

    await client.query(`
      INSERT INTO feedbacks (id, asset, client_address, feedback_index, value, value_decimals, score, tag1, tag2, endpoint, feedback_uri, feedback_hash, block_slot, tx_index, tx_signature, created_at, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, 'PENDING')
      ON CONFLICT (id) DO UPDATE SET
        score = COALESCE(EXCLUDED.score, feedbacks.score),
        updated_at = NOW()
    `, [id, asset, client_addr, feedbackIndex.toString(),
        data.value?.toString() || "0", data.valueDecimals || 0, data.score,
        data.tag1 || "", data.tag2 || "", data.endpoint || "",
        data.feedbackUri || "", data.sealHash ? Buffer.from(data.sealHash).toString("hex") : null,
        ctx.slot.toString(), ctx.txIndex || null, ctx.signature, ctx.blockTime.toISOString()]);
  }

  private async insertRevocationSupabase(client: PoolClient, data: any, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const client_addr = data.clientAddress?.toBase58?.() || data.clientAddress;
    const feedbackIndex = BigInt(data.feedbackIndex?.toString() || "0");

    await client.query(`
      UPDATE feedbacks SET is_revoked = true, revoked_at = $1
      WHERE asset = $2 AND client_address = $3 AND feedback_index = $4
    `, [ctx.blockTime.toISOString(), asset, client_addr, feedbackIndex.toString()]);
  }

  private async insertResponseSupabase(client: PoolClient, data: any, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const client_addr = data.client?.toBase58?.() || data.client;
    const responder = data.responder?.toBase58?.() || data.responder;
    const feedbackIndex = BigInt(data.feedbackIndex?.toString() || "0");
    const id = `${asset}:${client_addr}:${feedbackIndex}:${responder}:${ctx.signature}`;

    await client.query(`
      INSERT INTO feedback_responses (id, asset, client_address, feedback_index, responder, response_uri, response_hash, block_slot, tx_index, tx_signature, created_at, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'PENDING')
      ON CONFLICT (id) DO NOTHING
    `, [id, asset, client_addr, feedbackIndex.toString(), responder,
        data.responseUri || "", data.responseHash ? Buffer.from(data.responseHash).toString("hex") : null,
        ctx.slot.toString(), ctx.txIndex || null, ctx.signature, ctx.blockTime.toISOString()]);
  }

  private async insertValidationRequestSupabase(client: PoolClient, data: any, ctx: BatchEvent["ctx"]): Promise<void> {
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

  private async updateValidationResponseSupabase(client: PoolClient, data: any, _ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const validator = data.validatorAddress?.toBase58?.() || data.validatorAddress;
    const nonce = BigInt(data.nonce?.toString() || "0");

    await client.query(`
      UPDATE validations SET
        response = $1, response_uri = $2, response_hash = $3, tag = $4, updated_at = NOW()
      WHERE asset = $5 AND validator_address = $6 AND nonce = $7
    `, [data.response || 0, data.responseUri || "",
        data.responseHash ? Buffer.from(data.responseHash).toString("hex") : null,
        data.tag || "", asset, validator, nonce.toString()]);
  }

  private async insertCollectionSupabase(client: PoolClient, data: any, ctx: BatchEvent["ctx"], type: string): Promise<void> {
    const collection = data.collection?.toBase58?.() || data.collection;
    const authority = type === "BaseRegistryCreated"
      ? (data.createdBy?.toBase58?.() || data.createdBy)
      : (data.owner?.toBase58?.() || data.owner);
    const registryType = type === "BaseRegistryCreated" ? "BASE" : "USER";

    await client.query(`
      INSERT INTO collections (collection, registry_type, authority, created_at, status)
      VALUES ($1, $2, $3, $4, 'PENDING')
      ON CONFLICT (collection) DO NOTHING
    `, [collection, registryType, authority, ctx.blockTime.toISOString()]);
  }

  private async updateAgentUriSupabase(client: PoolClient, data: any, _ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    await client.query(`
      UPDATE agents SET agent_uri = $1, updated_at = NOW() WHERE asset = $2
    `, [data.newUri || "", asset]);
  }

  private async updateAgentWalletSupabase(client: PoolClient, data: any, _ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const wallet = data.newWallet?.toBase58?.() || data.newWallet;
    await client.query(`
      UPDATE agents SET agent_wallet = $1, updated_at = NOW() WHERE asset = $2
    `, [wallet, asset]);
  }

  private async updateAtomEnabledSupabase(client: PoolClient, data: any, _ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    await client.query(`
      UPDATE agents SET atom_enabled = true, updated_at = NOW() WHERE asset = $1
    `, [asset]);
  }

  private async insertMetadataSupabase(client: PoolClient, data: any, ctx: BatchEvent["ctx"]): Promise<void> {
    const asset = data.asset?.toBase58?.() || data.asset;
    const key = data.key || "";
    const value = data.value ? Buffer.from(data.value).toString("utf8") : "";
    const id = `${asset}:${key}`;

    await client.query(`
      INSERT INTO metadata (id, asset, key, value, immutable, block_slot, tx_index, tx_signature, created_at, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'PENDING')
      ON CONFLICT (id) DO UPDATE SET
        value = EXCLUDED.value,
        block_slot = EXCLUDED.block_slot,
        updated_at = NOW()
    `, [id, asset, key, value, data.immutable || false, ctx.slot.toString(), ctx.txIndex || null, ctx.signature, ctx.blockTime.toISOString()]);
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
