/**
 * Metadata Queue - Background processing for URI metadata extraction
 *
 * Uses p-queue for fire-and-forget async processing with:
 * - Concurrency limit to avoid overwhelming IPFS gateways
 * - Deduplication to skip redundant fetches
 * - Freshness check before writes to prevent stale overwrites
 */

import PQueue from "p-queue";
import { createHash } from "crypto";
import { Pool } from "pg";
import { digestUri, serializeValue } from "./uriDigest.js";
import { compressForStorage } from "../utils/compression.js";
import { config } from "../config.js";
import { createChildLogger } from "../logger.js";

const logger = createChildLogger("metadata-queue");

// Queue configuration
const CONCURRENCY = 10;        // Max parallel URI fetches
const INTERVAL = 100;          // Min 100ms between operations (rate limiting)
const TIMEOUT_MS = 30000;      // 30s timeout per operation
const MAX_QUEUE_SIZE = 5000;   // Max pending tasks in queue (memory protection)

import { STANDARD_URI_FIELDS } from "../constants.js";

export interface MetadataTask {
  assetId: string;
  uri: string;
  addedAt: number;
}

/**
 * Singleton metadata extraction queue
 * Processes URI fetches in background without blocking batch sync
 */
class MetadataQueue {
  private queue: PQueue;
  private pool: Pool | null = null;
  private pending = new Map<string, MetadataTask>(); // assetId -> latest task
  private statsInterval: NodeJS.Timeout | null = null;
  private stats = {
    queued: 0,
    processed: 0,
    skippedStale: 0,
    skippedDuplicate: 0,
    errors: 0,
  };

  constructor() {
    this.queue = new PQueue({
      concurrency: CONCURRENCY,
      interval: INTERVAL,
      intervalCap: CONCURRENCY,
      timeout: TIMEOUT_MS,
    });

    this.statsInterval = setInterval(() => this.logStats(), 60000);
  }

  /**
   * Set the database pool (called at startup)
   */
  setPool(pool: Pool): void {
    this.pool = pool;
  }

  /**
   * Add a URI extraction task to the queue
   * Deduplicates by keeping only the latest URI per asset
   */
  add(assetId: string, uri: string): void {
    if (!uri || config.metadataIndexMode === "off") {
      return;
    }

    // Check for duplicate (same asset, same URI already pending)
    const existing = this.pending.get(assetId);
    if (existing && existing.uri === uri) {
      this.stats.skippedDuplicate++;
      logger.debug({ assetId }, "Skipped duplicate metadata task");
      return;
    }

    // Reject if queue is at capacity
    if (this.queue.size + this.queue.pending >= MAX_QUEUE_SIZE) {
      logger.warn({ assetId, queueSize: this.queue.size }, "Metadata queue full, rejecting task");
      return;
    }

    // Update to latest URI for this asset
    const task: MetadataTask = {
      assetId,
      uri,
      addedAt: Date.now(),
    };
    this.pending.set(assetId, task);
    this.stats.queued++;

    // Fire and forget - don't await
    this.queue.add(() => this.processTask(task)).catch((err) => {
      logger.error({ assetId, uri, error: err.message }, "Queue task failed");
      this.stats.errors++;
    });
  }

  /**
   * Add multiple tasks at once (used after batch commit)
   */
  addBatch(tasks: Array<{ assetId: string; uri: string }>): void {
    for (const task of tasks) {
      this.add(task.assetId, task.uri);
    }
    logger.info({ count: tasks.length, queueSize: this.queue.size }, "Added batch to metadata queue");
  }

  /**
   * Process a single metadata extraction task
   */
  private async processTask(task: MetadataTask): Promise<void> {
    const { assetId, uri } = task;

    try {
      // Remove from pending map
      const current = this.pending.get(assetId);
      if (current === task) {
        this.pending.delete(assetId);
      }

      // Freshness check: verify URI hasn't changed in DB
      if (this.pool) {
        const freshCheck = await this.pool.query(
          `SELECT agent_uri FROM agents WHERE asset = $1`,
          [assetId]
        );

        if (freshCheck.rows.length === 0) {
          logger.debug({ assetId }, "Agent no longer exists, skipping");
          this.stats.skippedStale++;
          return;
        }

        if (freshCheck.rows[0].agent_uri !== uri) {
          logger.debug({ assetId, expected: uri, current: freshCheck.rows[0].agent_uri },
            "URI changed, skipping stale fetch");
          this.stats.skippedStale++;
          return;
        }
      }

      // Purge old URI metadata before writing new
      if (this.pool) {
        await this.pool.query(
          `DELETE FROM metadata WHERE asset = $1 AND key LIKE '\\_uri:%' ESCAPE '\\'`,
          [assetId]
        );
      }

      // Fetch and digest URI
      const result = await digestUri(uri);

      if (result.status !== "ok" || !result.fields) {
        // Store error status
        await this.storeMetadata(assetId, "_uri:_status", JSON.stringify({
          status: result.status,
          error: result.error,
          bytes: result.bytes,
          hash: result.hash,
        }));
        logger.debug({ assetId, uri, status: result.status }, "URI digest failed");
        this.stats.processed++;
        return;
      }

      // Store each extracted field
      const maxValueBytes = config.metadataMaxValueBytes;
      for (const [key, value] of Object.entries(result.fields)) {
        const serialized = serializeValue(value, maxValueBytes);

        if (serialized.oversize) {
          await this.storeMetadata(assetId, `${key}_meta`, JSON.stringify({
            status: "oversize",
            bytes: serialized.bytes,
            sha256: result.hash,
          }));
        } else {
          await this.storeMetadata(assetId, key, serialized.value);
        }
      }

      // Store success status
      await this.storeMetadata(assetId, "_uri:_status", JSON.stringify({
        status: "ok",
        bytes: result.bytes,
        hash: result.hash,
        fieldCount: Object.keys(result.fields).length,
        truncatedKeys: result.truncatedKeys || false,
      }));

      // Sync nft_name from _uri:name if present
      const uriName = result.fields["_uri:name"];
      if (uriName && typeof uriName === "string" && this.pool) {
        await this.pool.query(
          `UPDATE agents SET nft_name = $1 WHERE asset = $2 AND (nft_name IS NULL OR nft_name = '')`,
          [uriName, assetId]
        );
      }

      this.stats.processed++;
      logger.info({ assetId, uri, fieldCount: Object.keys(result.fields).length }, "Metadata extracted");

    } catch (error: any) {
      this.stats.errors++;
      logger.error({ assetId, uri, error: error.message }, "Metadata extraction failed");
    }
  }

  /**
   * Store a single URI metadata entry
   */
  private async storeMetadata(assetId: string, key: string, value: string): Promise<void> {
    if (!this.pool) return;

    const keyHash = createHash("sha256").update(key).digest().slice(0, 16).toString("hex");
    const id = `${assetId}:${keyHash}`;

    // Only compress non-standard fields
    const shouldCompress = !STANDARD_URI_FIELDS.has(key);
    const storedValue = shouldCompress
      ? await compressForStorage(Buffer.from(value))
      : Buffer.concat([Buffer.from([0x00]), Buffer.from(value)]); // PREFIX_RAW

    await this.pool.query(
      `INSERT INTO metadata (id, asset, key, key_hash, value, immutable, block_slot, tx_signature, updated_at)
       VALUES ($1, $2, $3, $4, $5, false, 0, 'uri_derived', NOW())
       ON CONFLICT (id) DO UPDATE SET
         value = EXCLUDED.value,
         updated_at = NOW()`,
      [id, assetId, key, keyHash, storedValue]
    );
  }

  /**
   * Get queue statistics
   */
  getStats() {
    return {
      ...this.stats,
      queueSize: this.queue.size,
      pendingCount: this.pending.size,
    };
  }

  /**
   * Wait for queue to drain (useful for graceful shutdown)
   */
  async drain(): Promise<void> {
    await this.queue.onIdle();
  }

  /**
   * Clean up resources for graceful shutdown
   */
  shutdown(): void {
    if (this.statsInterval) {
      clearInterval(this.statsInterval);
      this.statsInterval = null;
    }
  }

  private logStats(): void {
    if (this.stats.queued === 0) return;
    logger.info(this.getStats(), "Metadata queue stats (60s)");
  }
}

// Export singleton instance
export const metadataQueue = new MetadataQueue();
