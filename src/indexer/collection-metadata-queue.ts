import PQueue from "p-queue";
import { Pool } from "pg";
import { config } from "../config.js";
import { createChildLogger } from "../logger.js";
import { digestCollectionPointerDoc } from "./collectionDigest.js";

const logger = createChildLogger("collection-metadata-queue");

const CONCURRENCY = 5;
const INTERVAL = 100;
const TIMEOUT_MS = 30000;
const MAX_QUEUE_SIZE = 5000;
const RECOVERY_BATCH_SIZE = 1000;
const RECOVERY_INTERVAL_MS = 60_000;

export interface CollectionMetadataTask {
  assetId: string;
  col: string;
  addedAt: number;
}

class CollectionMetadataQueue {
  private queue: PQueue;
  private pool: Pool | null = null;
  private pending = new Map<string, CollectionMetadataTask>();
  private deferred = new Map<string, CollectionMetadataTask>();
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

  setPool(pool: Pool): void {
    this.pool = pool;
    this.startRecoveryLoop();
  }

  add(assetId: string, col: string): void {
    if (!config.collectionMetadataIndexEnabled || !assetId || !col) {
      return;
    }

    this.promoteDeferredIfCapacity();

    const existing = this.pending.get(assetId);
    if (existing && existing.col === col) {
      this.stats.skippedDuplicate++;
      return;
    }

    const task: CollectionMetadataTask = {
      assetId,
      col,
      addedAt: Date.now(),
    };

    this.pending.set(assetId, task);
    this.enqueueOrDefer(task);
  }

  addBatch(tasks: Array<{ assetId: string; col: string }>): void {
    for (const task of tasks) {
      this.add(task.assetId, task.col);
    }
  }

  private async processTask(task: CollectionMetadataTask): Promise<void> {
    const { assetId, col } = task;
    if (!this.pool) return;

    try {
      const currentPending = this.pending.get(assetId);
      if (currentPending === task) {
        this.pending.delete(assetId);
      }

      const currentAgent = await this.pool.query<{
        canonical_col: string;
        creator: string | null;
        owner: string;
      }>(
        `SELECT canonical_col, creator, owner FROM agents WHERE asset = $1`,
        [assetId]
      );

      if (currentAgent.rows.length === 0) {
        this.stats.skippedStale++;
        return;
      }

      const row = currentAgent.rows[0];
      if (row.canonical_col !== col) {
        this.stats.skippedStale++;
        return;
      }

      const creator = row.creator || row.owner;
      const result = await digestCollectionPointerDoc(col);

      if (result.status !== "ok" || !result.fields) {
        await this.pool.query(
          `UPDATE collection_pointers
           SET metadata_status = $1,
               metadata_hash = $2,
               metadata_bytes = $3,
               metadata_updated_at = NOW()
           WHERE col = $4
             AND creator = $5`,
          [result.status, result.hash || null, result.bytes ?? null, col, creator]
        );
        this.stats.processed++;
        return;
      }

      await this.pool.query(
        `UPDATE collection_pointers
         SET version = $1,
             name = $2,
             symbol = $3,
             description = $4,
             image = $5,
             banner_image = $6,
             social_website = $7,
             social_x = $8,
             social_discord = $9,
             metadata_status = $10,
             metadata_hash = $11,
             metadata_bytes = $12,
             metadata_updated_at = NOW()
         WHERE col = $13
           AND creator = $14`,
        [
          result.fields.version,
          result.fields.name,
          result.fields.symbol,
          result.fields.description,
          result.fields.image,
          result.fields.bannerImage,
          result.fields.socialWebsite,
          result.fields.socialX,
          result.fields.socialDiscord,
          "ok",
          result.hash || null,
          result.bytes ?? null,
          col,
          creator,
        ]
      );

      this.stats.processed++;
      logger.info({ assetId, col, creator }, "Collection metadata extracted");
    } catch (error: any) {
      this.stats.errors++;
      logger.error({ assetId, col, error: error.message }, "Collection metadata extraction failed");
    } finally {
      this.promoteDeferredIfCapacity();
    }
  }

  private enqueueOrDefer(task: CollectionMetadataTask): void {
    if (this.queue.size + this.queue.pending >= MAX_QUEUE_SIZE) {
      this.deferTask(task);
      return;
    }

    this.stats.queued++;
    this.queue.add(() => this.processTask(task)).catch((err) => {
      this.stats.errors++;
      logger.error({ assetId: task.assetId, col: task.col, error: err.message }, "Collection metadata queue task failed");
    });
  }

  private deferTask(task: CollectionMetadataTask): void {
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
        "Collection metadata queue at capacity, deferred latest task"
      );
    }
  }

  private promoteDeferredIfCapacity(): void {
    while (this.deferred.size > 0 && this.queue.size + this.queue.pending < MAX_QUEUE_SIZE) {
      const next = this.deferred.entries().next().value as [string, CollectionMetadataTask] | undefined;
      if (!next) return;

      const [assetId, task] = next;
      this.deferred.delete(assetId);
      this.stats.deferredPromoted++;
      this.enqueueOrDefer(task);
    }
  }

  getStats() {
    return {
      ...this.stats,
      queueSize: this.queue.size,
      pendingCount: this.pending.size,
      deferredCount: this.deferred.size,
    };
  }

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
    logger.info(this.getStats(), "Collection metadata queue stats (60s)");
  }

  private startRecoveryLoop(): void {
    if (!this.pool || process.env.NODE_ENV === "test" || this.recoveryInterval) {
      return;
    }

    const tick = async () => {
      if (this.recoveryInFlight || !this.pool || !config.collectionMetadataIndexEnabled) {
        return;
      }
      this.recoveryInFlight = true;
      try {
        const { rows } = await this.pool.query<{ asset: string; canonical_col: string }>(
          `SELECT a.asset, a.canonical_col
           FROM agents a
           LEFT JOIN collection_pointers cp
             ON cp.col = a.canonical_col
            AND cp.creator = COALESCE(a.creator, a.owner)
           WHERE a.canonical_col IS NOT NULL
             AND a.canonical_col != ''
             AND (
               cp.col IS NULL
               OR cp.metadata_status IS NULL
               OR cp.metadata_status != 'ok'
               OR cp.metadata_updated_at IS NULL
               OR cp.metadata_updated_at < cp.last_seen_at
             )
           LIMIT $1`,
          [RECOVERY_BATCH_SIZE]
        );

        if (rows.length > 0) {
          for (const row of rows) {
            this.add(row.asset, row.canonical_col);
          }
          logger.info({ recoveredCount: rows.length }, "Recovered missing collection metadata tasks");
        }
      } catch (error: any) {
        logger.warn({ error: error.message }, "Collection metadata recovery sweep failed");
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

export const collectionMetadataQueue = new CollectionMetadataQueue();
