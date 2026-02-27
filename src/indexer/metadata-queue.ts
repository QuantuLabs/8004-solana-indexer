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
const RECOVERY_BATCH_SIZE = 1000;
const RECOVERY_INTERVAL_MS = 60_000;

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
  private deferred = new Map<string, MetadataTask>(); // assetId -> latest deferred task
  private statsInterval: NodeJS.Timeout | null = null;
  private recoveryInterval: NodeJS.Timeout | null = null;
  private recoveryInFlight = false;
  private stats = {
    queued: 0,
    processed: 0,
    skippedStale: 0,
    skippedDuplicate: 0,
    deferredQueued: 0,
    deferredPromoted: 0,
    deferredReplaced: 0,
    errors: 0,
  };
  private queueFullSignals = 0;

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
    this.startRecoveryLoop();
  }

  /**
   * Add a URI extraction task to the queue
   * Deduplicates by keeping only the latest URI per asset
   */
  add(assetId: string, uri: string): void {
    if (!uri || config.metadataIndexMode === "off") {
      return;
    }

    this.promoteDeferredIfCapacity();

    // Check for duplicate (same asset, same URI already pending)
    const existing = this.pending.get(assetId);
    if (existing && existing.uri === uri) {
      this.stats.skippedDuplicate++;
      logger.debug({ assetId }, "Skipped duplicate metadata task");
      return;
    }

    // Update to latest URI for this asset
    const task: MetadataTask = {
      assetId,
      uri,
      addedAt: Date.now(),
    };
    this.pending.set(assetId, task);
    this.enqueueOrDefer(task);
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
          `DELETE FROM metadata
           WHERE asset = $1
             AND key LIKE '\\_uri:%' ESCAPE '\\'
             AND NOT immutable`,
          [assetId]
        );
      }

      // Track the exact URI used for this extraction so recovery can detect mismatches after restarts.
      await this.storeMetadata(assetId, "_uri:_source", uri);

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
    } finally {
      this.promoteDeferredIfCapacity();
    }
  }

  private enqueueOrDefer(task: MetadataTask): void {
    if (this.queue.size + this.queue.pending >= MAX_QUEUE_SIZE) {
      this.deferTask(task);
      return;
    }

    this.stats.queued++;
    this.queue.add(() => this.processTask(task)).catch((err) => {
      logger.error({ assetId: task.assetId, uri: task.uri, error: err.message }, "Queue task failed");
      this.stats.errors++;
    });
  }

  private deferTask(task: MetadataTask): void {
    const existing = this.deferred.get(task.assetId);
    if (existing) {
      this.stats.deferredReplaced++;
      this.deferred.delete(task.assetId);
    }

    this.deferred.set(task.assetId, task);
    this.stats.deferredQueued++;
    this.queueFullSignals++;

    if (this.queueFullSignals % 10 === 1) {
      logger.warn(
        {
          assetId: task.assetId,
          queueSize: this.queue.size,
          inFlight: this.queue.pending,
          deferredCount: this.deferred.size,
          deferredQueued: this.stats.deferredQueued,
        },
        "Metadata queue at capacity, deferred latest task"
      );
    }
  }

  private promoteDeferredIfCapacity(): void {
    while (this.deferred.size > 0 && this.queue.size + this.queue.pending < MAX_QUEUE_SIZE) {
      const next = this.deferred.entries().next().value as [string, MetadataTask] | undefined;
      if (!next) return;

      const [assetId, task] = next;
      this.deferred.delete(assetId);
      this.stats.deferredPromoted++;
      this.enqueueOrDefer(task);
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
    const shouldCompress = !STANDARD_URI_FIELDS.has(key) && key !== "_uri:_source";
    const storedValue = shouldCompress
      ? await compressForStorage(Buffer.from(value))
      : Buffer.concat([Buffer.from([0x00]), Buffer.from(value)]); // PREFIX_RAW

    await this.pool.query(
      `INSERT INTO metadata (id, asset, key, key_hash, value, immutable, block_slot, tx_signature, updated_at)
       VALUES ($1, $2, $3, $4, $5, false, 0, 'uri_derived', NOW())
       ON CONFLICT (id) DO UPDATE SET
         value = EXCLUDED.value,
         updated_at = NOW()
       WHERE NOT metadata.immutable`,
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
      deferredCount: this.deferred.size,
    };
  }

  /**
   * Wait for queue to drain (useful for graceful shutdown)
   */
  async drain(): Promise<void> {
    do {
      this.promoteDeferredIfCapacity();
      await this.queue.onIdle();
    } while (this.deferred.size > 0);
  }

  /**
   * Clean up resources for graceful shutdown
   */
  shutdown(): void {
    if (this.statsInterval) {
      clearInterval(this.statsInterval);
      this.statsInterval = null;
    }
    if (this.recoveryInterval) {
      clearInterval(this.recoveryInterval);
      this.recoveryInterval = null;
    }
  }

  private logStats(): void {
    if (this.stats.queued === 0 && this.deferred.size === 0) return;
    logger.info(this.getStats(), "Metadata queue stats (60s)");
  }

  private startRecoveryLoop(): void {
    if (!this.pool || process.env.NODE_ENV === "test" || this.recoveryInterval) {
      return;
    }

    const tick = async () => {
      if (this.recoveryInFlight || !this.pool || config.metadataIndexMode === "off") {
        return;
      }
      this.recoveryInFlight = true;
      try {
        const { rows } = await this.pool.query<{ asset: string; agent_uri: string }>(
          `SELECT a.asset, a.agent_uri
           FROM agents a
           LEFT JOIN metadata m_source
             ON m_source.asset = a.asset
            AND m_source.key = '_uri:_source'
           LEFT JOIN metadata m_status
             ON m_status.asset = a.asset
            AND m_status.key = '_uri:_status'
           WHERE a.agent_uri IS NOT NULL
             AND a.agent_uri != ''
             AND (
               m_source.id IS NULL
               OR get_byte(m_source.value, 0) != 0
               OR convert_from(substring(m_source.value from 2), 'UTF8') != a.agent_uri
               OR m_status.id IS NULL
               OR get_byte(m_status.value, 0) != 0
               OR convert_from(substring(m_status.value from 2), 'UTF8') NOT LIKE '%"status":"ok"%'
             )
           LIMIT $1`,
          [RECOVERY_BATCH_SIZE]
        );

        if (rows.length > 0) {
          for (const row of rows) {
            this.add(row.asset, row.agent_uri);
          }
          logger.info({ recoveredCount: rows.length }, "Recovered missing URI metadata tasks");
        }
      } catch (error: any) {
        logger.warn({ error: error.message }, "Metadata recovery sweep failed");
      } finally {
        this.recoveryInFlight = false;
      }
    };

    void tick();
    this.recoveryInterval = setInterval(() => {
      void tick();
    }, RECOVERY_INTERVAL_MS);
  }
}

// Export singleton instance
export const metadataQueue = new MetadataQueue();
