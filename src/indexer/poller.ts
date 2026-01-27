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
   * Step 1: Fetch ALL signatures backwards (newest to oldest)
   * Step 2: Reverse to get chronological order (oldest to newest)
   * Step 3: Process in order
   */
  private async backfill(): Promise<void> {
    logger.info("Starting historical backfill - collecting all signatures...");

    // Step 1: Collect ALL signatures (newest to oldest)
    const allSignatures: ConfirmedSignatureInfo[] = [];
    let beforeSignature: string | undefined = undefined;

    while (this.isRunning) {
      const options: { limit: number; before?: string } = {
        limit: this.batchSize,
      };
      if (beforeSignature) {
        options.before = beforeSignature;
      }

      const signatures = await this.connection.getSignaturesForAddress(
        this.programId,
        options
      );

      const validSigs = signatures.filter((sig) => sig.err === null);

      if (validSigs.length === 0) {
        break;
      }

      allSignatures.push(...validSigs);
      logger.info({ fetched: validSigs.length, total: allSignatures.length }, "Fetched backfill batch");

      // Move to older signatures
      beforeSignature = validSigs[validSigs.length - 1].signature;

      // If we got fewer than requested, we've reached the end
      if (signatures.length < this.batchSize) {
        break;
      }

      // Small delay to avoid rate limiting
      await new Promise((resolve) => setTimeout(resolve, 200));
    }

    logger.info({ total: allSignatures.length }, "All signatures collected, processing oldest to newest...");

    // Step 2: Reverse to get chronological order (oldest first)
    allSignatures.reverse();

    // Step 3: Group by slot for efficient block fetching
    const bySlot = new Map<number, ConfirmedSignatureInfo[]>();
    for (const sig of allSignatures) {
      const slot = sig.slot;
      if (!bySlot.has(slot)) {
        bySlot.set(slot, []);
      }
      bySlot.get(slot)!.push(sig);
    }

    logger.info({ slots: bySlot.size, transactions: allSignatures.length }, "Grouped by slot for tx_index resolution");

    // Step 4: Process slot by slot with tx_index
    let processed = 0;
    const sortedSlots = Array.from(bySlot.keys()).sort((a, b) => a - b);

    for (const slot of sortedSlots) {
      if (!this.isRunning) break;

      const sigs = bySlot.get(slot)!;

      // Build signature -> txIndex map for this slot
      const txIndexMap = await this.getTxIndexMap(slot, sigs);

      // Process transactions in tx_index order
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

          if (processed % 10 === 0) {
            logger.info({ processed, total: allSignatures.length }, "Backfill progress");
          }
        } catch (error) {
          logger.error({ error, signature: sig.signature }, "Error processing backfill transaction");
        }
      }
    }

    logger.info({ processed, total: allSignatures.length }, "Backfill finished, switching to live polling");
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

      // Safety: if we've collected too many, log warning and break
      if (allSignatures.length > 10000) {
        logger.warn({ count: allSignatures.length }, "Large gap detected, processing available signatures");
        break;
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
