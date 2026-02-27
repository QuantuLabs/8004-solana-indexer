import {
  Connection,
  PublicKey,
  ConfirmedSignatureInfo,
  ParsedTransactionWithMeta,
} from "@solana/web3.js";
import { PrismaClient } from "@prisma/client";
import { config } from "../config.js";
import { parseTransaction, toTypedEvent } from "../parser/decoder.js";
import { handleEventAtomic, EventContext } from "../db/handlers.js";
import { loadIndexerState, saveIndexerState, getPool } from "../db/supabase.js";
import { createChildLogger } from "../logger.js";
import { BatchRpcFetcher, EventBuffer } from "./batch-processor.js";
import { metadataQueue } from "./metadata-queue.js";

const logger = createChildLogger("poller");

// Batch processing configuration
// Batch RPC fetching: enabled for ALL modes (has built-in fallback)
// Batch DB writes: only for Supabase mode (uses raw SQL)
const USE_BATCH_RPC = true;
const USE_BATCH_DB = config.dbMode === "supabase";

export interface PollerOptions {
  connection: Connection;
  prisma: PrismaClient | null;
  programId: PublicKey;
  pollingInterval?: number;
  batchSize?: number;
}

export class Poller {
  private connection: Connection;
  private prisma: PrismaClient | null;
  private programId: PublicKey;
  private pollingInterval: number;
  private batchSize: number;
  private isRunning = false;
  private lastSignature: string | null = null;
  private processedCount = 0;
  private errorCount = 0;
  private lastStatsLog = Date.now();
  // Track pagination continuation when hitting memory limits
  // pendingContinuation: where to resume pagination FROM
  // pendingStopSignature: where to STOP (original lastSignature when we hit the limit)
  private pendingContinuation: string | null = null;
  private pendingStopSignature: string | null = null;

  // Batch processing components (Supabase mode only)
  private batchFetcher: BatchRpcFetcher | null = null;
  private eventBuffer: EventBuffer | null = null;

  constructor(options: PollerOptions) {
    this.connection = options.connection;
    this.prisma = options.prisma;
    this.programId = options.programId;
    this.pollingInterval = options.pollingInterval || config.pollingInterval;
    this.batchSize = options.batchSize || config.batchSize;

    // Initialize batch RPC fetcher (always enabled, has built-in fallback)
    if (USE_BATCH_RPC) {
      this.batchFetcher = new BatchRpcFetcher(this.connection);
      logger.info("Batch RPC fetching enabled (with fallback)");
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

  async start(): Promise<void> {
    if (this.isRunning) {
      logger.warn("Poller already running");
      return;
    }

    this.isRunning = true;
    logger.info({ programId: this.programId.toBase58() }, "Starting poller");

    await this.loadState();

    // If no saved state, do backfill first
    if (!this.lastSignature) {
      logger.info("No saved state - starting backfill from beginning");
      await this.backfill();
    }

    this.poll();
  }

  /**
   * Backfill all historical transactions from the program
   * Uses streaming approach to avoid OOM - fetches and processes in batches
   * Processes oldest-to-newest within each batch for correct ordering
   */
  private async backfill(): Promise<void> {
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
          { limit: this.batchSize, before: beforeSignature }
        );

        if (signatures.length === 0) break;

        const validSigs = signatures.filter((sig) => sig.err === null);
        totalEstimate += validSigs.length;

        // Save checkpoint every batch
        if (validSigs.length > 0) {
          checkpoints.push(validSigs[validSigs.length - 1].signature);
        }

        beforeSignature = signatures[signatures.length - 1].signature;

        if (signatures.length < this.batchSize) break;

        await new Promise((resolve) => setTimeout(resolve, 100));

        if (totalEstimate % 5000 === 0) {
          logger.info({ scanned: totalEstimate, checkpoints: checkpoints.length }, "Scanning progress...");
        }
      } catch (error) {
        scanErrors++;
        logger.error({ error, scanErrors, beforeSignature }, "Error during backfill scan");

        // Retry with exponential backoff
        if (scanErrors >= 5) {
          logger.error("Too many scan errors, aborting backfill");
          return;
        }
        await new Promise((resolve) => setTimeout(resolve, 1000 * scanErrors));
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
      processed += await this.processSignatureBatch(windowSigs, processed, totalEstimate);

      logger.info({ processed, total: totalEstimate, checkpoint: i }, "Backfill checkpoint processed");
    }

    // Phase 3: Process any remaining newest transactions (before first checkpoint)
    if (checkpoints.length > 0 && this.isRunning) {
      const newestSigs = await this.fetchSignatureWindow(undefined, checkpoints[0]);
      if (newestSigs.length > 0) {
        processed += await this.processSignatureBatch(newestSigs, processed, totalEstimate);
      }
    }

    logger.info({ processed }, "Backfill finished, switching to live polling");
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

        await new Promise((resolve) => setTimeout(resolve, 50));
        retryCount = 0; // Reset on success
      } catch (error) {
        retryCount++;
        logger.warn({ error, retryCount, windowSize: windowSigs.length }, "Error fetching signature window");

        if (retryCount >= 3) {
          logger.error("Too many errors fetching signature window, returning partial results");
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 500 * retryCount));
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
    totalEstimate: number
  ): Promise<number> {
    const startTime = Date.now();

    // BATCH RPC: Fetch all transactions in batch first (with fallback)
    let txCache: Map<string, ParsedTransactionWithMeta> | null = null;
    if (USE_BATCH_RPC && this.batchFetcher) {
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

      const sigs = bySlot.get(slot)!;
      let txIndexMap: Map<string, number | null>;
      try {
        txIndexMap = await this.getTxIndexMap(slot, sigs);
      } catch (error) {
        logger.warn({ slot, error: error instanceof Error ? error.message : String(error) }, "Failed to get tx index map, tx_index will be NULL");
        txIndexMap = new Map(sigs.map((s) => [s.signature, null]));
      }

      const sigsWithIndex = sigs.map(sig => ({
        sig,
        txIndex: txIndexMap.get(sig.signature) ?? undefined
      })).sort((a, b) => {
        const txA = a.txIndex ?? Number.MAX_SAFE_INTEGER;
        const txB = b.txIndex ?? Number.MAX_SAFE_INTEGER;
        if (txA !== txB) return txA - txB;
        return a.sig.signature.localeCompare(b.sig.signature);
      });

      for (const { sig, txIndex } of sigsWithIndex) {
        if (!this.isRunning) break;

        try {
          // Use cached transaction from batch RPC if available
          if (USE_BATCH_RPC && txCache) {
            await this.processTransactionBatch(sig, txIndex, txCache.get(sig.signature));
          } else {
            await this.processTransaction(sig, txIndex);
          }
          this.lastSignature = sig.signature;
          // Skip individual cursor saves when using batch DB - handled by EventBuffer flush
          if (!USE_BATCH_DB) {
            await this.saveState(sig.signature, BigInt(sig.slot));
          }
          processed++;
          this.processedCount++;

          if ((previousCount + processed) % 100 === 0) {
            const elapsed = (Date.now() - startTime) / 1000;
            logger.info({
              processed: previousCount + processed,
              total: totalEstimate,
              rate: `${Math.round(processed / elapsed)} tx/s`,
              batchRpc: USE_BATCH_RPC,
              batchDb: USE_BATCH_DB
            }, "Backfill progress");
          }
        } catch (error) {
          this.errorCount++;
          logger.error({
            error: error instanceof Error ? error.message : String(error),
            signature: sig.signature,
            slot: sig.slot
          }, "Error processing backfill transaction");
        }
      }
    }

    // BATCH DB: Flush remaining events at end of batch
    if (USE_BATCH_DB && this.eventBuffer && this.eventBuffer.size > 0) {
      await this.eventBuffer.flush();
    }

    return processed;
  }

  /**
   * Get transaction index within a block for multiple signatures
   * Fetches block once and maps signature -> index
   * Only called when multiple txs exist in the same slot (rare case)
   */
  private async getTxIndexMap(slot: number, sigs: ConfirmedSignatureInfo[]): Promise<Map<string, number | null>> {
    const txIndexMap = new Map<string, number | null>();

    // If only one transaction in slot, index is 0 - no need to fetch block
    if (sigs.length === 1) {
      txIndexMap.set(sigs[0].signature, 0);
      return txIndexMap;
    }

    const MAX_RETRIES = 3;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        const block = await this.connection.getBlock(slot, {
          maxSupportedTransactionVersion: 0,
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
          await new Promise(r => setTimeout(r, 500 * attempt));
        } else {
          logger.warn({ slot, sigCount: sigs.length }, "getBlock failed after retries, tx_index will be NULL (signature-order fallback)");
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

    // Flush any remaining events in batch mode
    if (this.eventBuffer && this.eventBuffer.size > 0) {
      logger.info({ remaining: this.eventBuffer.size }, "Flushing remaining events before shutdown");
      await this.eventBuffer.flush();
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

  private async loadState(): Promise<void> {
    // Supabase mode - load from Supabase
    if (!this.prisma) {
      const state = await loadIndexerState();
      if (state.lastSignature) {
        this.lastSignature = state.lastSignature;
        logger.info({ lastSignature: this.lastSignature, lastSlot: state.lastSlot?.toString() }, "Supabase mode: resuming from signature");
      } else {
        logger.info("Supabase mode: starting from latest transactions (no saved state)");
      }
      return;
    }

    // Local mode - load from Prisma
    const state = await this.prisma.indexerState.findUnique({
      where: { id: "main" },
    });

    if (state?.lastSignature) {
      this.lastSignature = state.lastSignature;
      logger.info({ lastSignature: this.lastSignature }, "Resuming from signature");
    } else {
      logger.info("Starting from latest transactions");
    }
  }

  private async saveState(signature: string, slot: bigint): Promise<void> {
    // Supabase mode - save to Supabase
    if (!this.prisma) {
      await saveIndexerState(signature, slot);
      return;
    }

    // Local mode - save to Prisma
    await this.prisma.indexerState.upsert({
      where: { id: "main" },
      create: {
        id: "main",
        lastSignature: signature,
        lastSlot: slot,
      },
      update: {
        lastSignature: signature,
        lastSlot: slot,
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

      await new Promise((resolve) =>
        setTimeout(resolve, this.pollingInterval)
      );
    }
  }

  private async processNewTransactions(): Promise<void> {
    const signatures = await this.fetchSignatures();

    if (signatures.length === 0) {
      logger.debug("No new transactions");
      return;
    }

    logger.info({ count: signatures.length, batchRpc: USE_BATCH_RPC, batchDb: USE_BATCH_DB }, "Processing transactions");

    // Reverse to process oldest first
    const reversed = signatures.reverse();

    // BATCH RPC: Pre-fetch all transactions in batch (with fallback)
    let txCache: Map<string, ParsedTransactionWithMeta> | null = null;
    if (USE_BATCH_RPC && this.batchFetcher && reversed.length > 1) {
      const sigList = reversed.map(s => s.signature);
      txCache = await this.batchFetcher.fetchTransactions(sigList);
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

    for (const slot of sortedSlots) {
      const sigs = bySlot.get(slot)!;
      const txIndexMap = await this.getTxIndexMap(slot, sigs);

      // Canonical within-slot ordering is by signature; tx_index is only a tie-break metadata field.
      const sigsWithIndex = sigs.map(sig => ({
        sig,
        txIndex: txIndexMap.get(sig.signature) ?? undefined
      })).sort((a, b) => {
        const txA = a.txIndex ?? Number.MAX_SAFE_INTEGER;
        const txB = b.txIndex ?? Number.MAX_SAFE_INTEGER;
        if (txA !== txB) return txA - txB;
        return a.sig.signature.localeCompare(b.sig.signature);
      });

      let batchFailed = false;
      for (const { sig, txIndex } of sigsWithIndex) {
        try {
          // Use cached transaction from batch RPC if available
          if (USE_BATCH_RPC && txCache) {
            await this.processTransactionBatch(sig, txIndex, txCache.get(sig.signature));
          } else {
            await this.processTransaction(sig, txIndex);
          }
          this.lastSignature = sig.signature;
          // Skip individual cursor saves when using batch DB - handled by EventBuffer flush
          if (!USE_BATCH_DB) {
            await this.saveState(sig.signature, BigInt(sig.slot));
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
        logger.warn(
          { slot, lastSignature: this.lastSignature },
          "Batch processing halted - will retry failed tx on next poll cycle"
        );
        break;
      }
    }

    // BATCH DB: Flush events after processing all transactions
    if (USE_BATCH_DB && this.eventBuffer && this.eventBuffer.size > 0) {
      await this.eventBuffer.flush();
    }

    this.logStatsIfNeeded();
  }

  /**
   * Fetch new signatures since lastSignature
   * Uses pagination with `before` to handle cases where new tx count > batchSize
   * Returns signatures in newest-first order (caller should reverse for processing)
   */
  private async fetchSignatures(): Promise<ConfirmedSignatureInfo[]> {
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
      // Resume from continuation point if we hit memory limit in previous cycle
      let beforeSignature: string | undefined = this.pendingContinuation || undefined;
      // Use pendingStopSignature if resuming, otherwise use lastSignature
      const stopSignature = this.pendingStopSignature || this.lastSignature;
      let retryCount = 0;

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
            limit: this.batchSize,
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
          for (const sig of batch) {
            if (sig.signature === stopSignature) {
              // Reached our checkpoint, we're done with the large batch
              this.pendingStopSignature = null;
              return allSignatures;
            }
            if (sig.err === null) {
              allSignatures.push(sig);
            }
          }

          // Move to older signatures for next iteration
          beforeSignature = batch[batch.length - 1].signature;

          // Log progress for large gaps (but continue - don't lose data!)
          if (allSignatures.length > 0 && allSignatures.length % 10000 === 0) {
            logger.info({ count: allSignatures.length }, "Large gap being processed, continuing pagination...");
          }

          // Memory safety: limit max signatures to process in one poll cycle
          if (allSignatures.length > 100000) {
            logger.warn({
              count: allSignatures.length,
              continuationPoint: beforeSignature,
              stopSignature: stopSignature
            }, "Large gap detected, will continue from checkpoint in next cycle");
            // Store continuation point and original stop signature
            this.pendingContinuation = beforeSignature!;
            // Only set pendingStopSignature if not already set (first time hitting limit)
            if (!this.pendingStopSignature) {
              this.pendingStopSignature = this.lastSignature;
            }
            break;
          }

          // Small delay to avoid rate limiting during pagination
          if (batch.length >= this.batchSize) {
            await new Promise((resolve) => setTimeout(resolve, 100));
          } else {
            // Got fewer than requested, no more signatures
            this.pendingStopSignature = null;
            break;
          }

          retryCount = 0; // Reset on success
        } catch (innerError) {
          retryCount++;
          logger.warn({ error: innerError, retryCount }, "Error during signature pagination");

          if (retryCount >= 3) {
            logger.error("Too many pagination errors, returning partial results");
            break;
          }
          await new Promise((resolve) => setTimeout(resolve, 500 * retryCount));
        }
      }

      return allSignatures;
    } catch (error) {
      logger.error({ error }, "Error fetching signatures");
      return [];
    }
  }

  private async processTransaction(sig: ConfirmedSignatureInfo, txIndex?: number): Promise<void> {
    const tx = await this.connection.getParsedTransaction(sig.signature, {
      maxSupportedTransactionVersion: 0,
    });

    if (!tx) {
      logger.warn({ signature: sig.signature }, "Transaction not found");
      return;
    }

    const parsed = parseTransaction(tx);
    if (!parsed || parsed.events.length === 0) {
      return;
    }

    logger.debug(
      { signature: sig.signature, eventCount: parsed.events.length, txIndex },
      "Parsed transaction"
    );

    for (const [eventOrdinal, event] of parsed.events.entries()) {
      const typedEvent = toTypedEvent(event);
      if (!typedEvent) continue;

      const ctx: EventContext = {
        signature: sig.signature,
        slot: BigInt(sig.slot),
        blockTime: sig.blockTime
          ? new Date(sig.blockTime * 1000)
          : new Date(),
        txIndex,
        eventOrdinal,
      };

      await handleEventAtomic(this.prisma, typedEvent, ctx);

      // Only log to Prisma in local mode
      if (this.prisma) {
        await this.prisma.eventLog.create({
          data: {
            eventType: typedEvent.type,
            signature: sig.signature,
            slot: BigInt(sig.slot),
            blockTime: ctx.blockTime,
            data: event.data as object,
            processed: true,
          },
        });
      }
    }
  }

  /**
   * Process transaction in batch mode - adds events to buffer instead of direct DB write
   * Uses pre-fetched transaction from batch RPC call
   */
  private async processTransactionBatch(
    sig: ConfirmedSignatureInfo,
    txIndex: number | undefined,
    tx: ParsedTransactionWithMeta | undefined
  ): Promise<void> {
    if (!tx) {
      // Fallback to individual fetch if not in cache
      logger.debug({ signature: sig.signature }, "Transaction not in batch cache, fetching individually");
      tx = await this.connection.getParsedTransaction(sig.signature, {
        maxSupportedTransactionVersion: 0,
      }) ?? undefined;
    }

    if (!tx) {
      logger.warn({ signature: sig.signature }, "Transaction not found");
      return;
    }

    const parsed = parseTransaction(tx);
    if (!parsed || parsed.events.length === 0) {
      return;
    }

    logger.debug(
      { signature: sig.signature, eventCount: parsed.events.length, txIndex },
      "Parsed transaction (batch mode)"
    );

    for (const [eventOrdinal, event] of parsed.events.entries()) {
      const typedEvent = toTypedEvent(event);
      if (!typedEvent) continue;

      const ctx = {
        signature: sig.signature,
        slot: BigInt(sig.slot),
        blockTime: sig.blockTime
          ? new Date(sig.blockTime * 1000)
          : new Date(),
        txIndex,
        eventOrdinal,
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
  }

  private async logFailedTransaction(
    sig: ConfirmedSignatureInfo,
    error: unknown
  ): Promise<void> {
    // Only log errors to Prisma in local mode
    if (!this.prisma) return;

    await this.prisma.eventLog.create({
      data: {
        eventType: "PROCESSING_FAILED",
        signature: sig.signature,
        slot: BigInt(sig.slot),
        blockTime: sig.blockTime
          ? new Date(sig.blockTime * 1000)
          : new Date(),
        data: {},
        processed: false,
        error: error instanceof Error ? error.message : String(error),
      },
    });
  }
}
