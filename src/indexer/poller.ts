import {
  Connection,
  PublicKey,
  ConfirmedSignatureInfo,
} from "@solana/web3.js";
import { PrismaClient } from "@prisma/client";
import { config } from "../config.js";
import { parseTransaction, toTypedEvent } from "../parser/decoder.js";
import { handleEvent, EventContext } from "../db/handlers.js";
import { loadIndexerState, saveIndexerState } from "../db/supabase.js";
import { createChildLogger } from "../logger.js";

const logger = createChildLogger("poller");

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

  constructor(options: PollerOptions) {
    this.connection = options.connection;
    this.prisma = options.prisma;
    this.programId = options.programId;
    this.pollingInterval = options.pollingInterval || config.pollingInterval;
    this.batchSize = options.batchSize || config.batchSize;
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

    // Phase 1: Collect checkpoint signatures (one per ~1000 txs) to avoid loading all in memory
    logger.info("Phase 1: Scanning for oldest transactions...");
    while (this.isRunning) {
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
        logger.info({ scanned: totalEstimate }, "Scanning progress...");
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

    while (this.isRunning) {
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
    }

    // Reverse to get chronological order (oldest first)
    return windowSigs.reverse();
  }

  /**
   * Process a batch of signatures with slot grouping and tx_index resolution
   * Returns number of successfully processed transactions
   */
  private async processSignatureBatch(
    signatures: ConfirmedSignatureInfo[],
    previousCount: number,
    totalEstimate: number
  ): Promise<number> {
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
      const txIndexMap = await this.getTxIndexMap(slot, sigs);

      const sigsWithIndex = sigs.map(sig => ({
        sig,
        txIndex: txIndexMap.get(sig.signature) ?? 0
      })).sort((a, b) => a.txIndex - b.txIndex);

      for (const { sig, txIndex } of sigsWithIndex) {
        if (!this.isRunning) break;

        try {
          await this.processTransaction(sig, txIndex);
          this.lastSignature = sig.signature;
          await this.saveState(sig.signature, BigInt(sig.slot));
          processed++;

          if ((previousCount + processed) % 100 === 0) {
            logger.info({ processed: previousCount + processed, total: totalEstimate }, "Backfill progress");
          }
        } catch (error) {
          logger.error({ error, signature: sig.signature }, "Error processing backfill transaction");
        }
      }
    }

    return processed;
  }

  /**
   * Get transaction index within a block for multiple signatures
   * Fetches block once and maps signature -> index
   * Only called when multiple txs exist in the same slot (rare case)
   */
  private async getTxIndexMap(slot: number, sigs: ConfirmedSignatureInfo[]): Promise<Map<string, number>> {
    const txIndexMap = new Map<string, number>();

    // If only one transaction in slot, index is 0 - no need to fetch block
    if (sigs.length === 1) {
      txIndexMap.set(sigs[0].signature, 0);
      return txIndexMap;
    }

    try {
      // Fetch block with full transaction details to get signatures in order
      const block = await this.connection.getBlock(slot, {
        maxSupportedTransactionVersion: 0,
        transactionDetails: "full",
      });

      if (block?.transactions) {
        // Build signature -> index map from block transactions (in execution order)
        const sigSet = new Set(sigs.map(s => s.signature));
        block.transactions.forEach((tx, idx) => {
          const sig = tx.transaction.signatures[0]; // First signature is the tx signature
          if (sigSet.has(sig)) {
            txIndexMap.set(sig, idx);
          }
        });
      }
    } catch (error) {
      logger.warn({ slot, error }, "Failed to fetch block for tx_index, using fallback order");
      // Fallback: assign sequential indices based on signature order
      sigs.forEach((sig, i) => txIndexMap.set(sig.signature, i));
    }

    return txIndexMap;
  }

  async stop(): Promise<void> {
    logger.info("Stopping poller");
    this.isRunning = false;
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

    logger.info({ count: signatures.length }, "Processing transactions");

    // Reverse to process oldest first
    const reversed = signatures.reverse();

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

      // Sort by tx_index within slot
      const sigsWithIndex = sigs.map(sig => ({
        sig,
        txIndex: txIndexMap.get(sig.signature) ?? 0
      })).sort((a, b) => a.txIndex - b.txIndex);

      for (const { sig, txIndex } of sigsWithIndex) {
        try {
          await this.processTransaction(sig, txIndex);
          this.lastSignature = sig.signature;
          await this.saveState(sig.signature, BigInt(sig.slot));
        } catch (error) {
          logger.error(
            { error, signature: sig.signature },
            "Error processing transaction"
          );
          await this.logFailedTransaction(sig, error);
        }
      }
    }
  }

  /**
   * Fetch new signatures since lastSignature
   * Uses pagination with `before` to handle cases where new tx count > batchSize
   * Returns signatures in newest-first order (caller should reverse for processing)
   */
  private async fetchSignatures(): Promise<ConfirmedSignatureInfo[]> {
    if (!this.lastSignature) {
      // No last signature - just get the latest batch
      const signatures = await this.connection.getSignaturesForAddress(
        this.programId,
        { limit: this.batchSize }
      );
      return signatures.filter((sig) => sig.err === null);
    }

    // Paginate backwards from newest until we reach lastSignature
    const allSignatures: ConfirmedSignatureInfo[] = [];
    let beforeSignature: string | undefined = undefined;

    while (true) {
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
        break;
      }

      // Filter out failed transactions and check for lastSignature
      for (const sig of batch) {
        if (sig.signature === this.lastSignature) {
          // Reached our checkpoint, we're done
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

      // Small delay to avoid rate limiting during pagination
      if (batch.length >= this.batchSize) {
        await new Promise((resolve) => setTimeout(resolve, 100));
      } else {
        // Got fewer than requested, no more signatures
        break;
      }
    }

    return allSignatures;
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

    for (const event of parsed.events) {
      const typedEvent = toTypedEvent(event);
      if (!typedEvent) continue;

      const ctx: EventContext = {
        signature: sig.signature,
        slot: BigInt(sig.slot),
        blockTime: sig.blockTime
          ? new Date(sig.blockTime * 1000)
          : new Date(),
        txIndex,
      };

      await handleEvent(this.prisma, typedEvent, ctx);

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
