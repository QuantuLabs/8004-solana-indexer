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
   * Paginates backwards from most recent to oldest
   */
  private async backfill(): Promise<void> {
    logger.info("Starting historical backfill...");
    let beforeSignature: string | undefined = undefined;
    let totalProcessed = 0;

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
        logger.info({ totalProcessed }, "Backfill complete - no more transactions");
        break;
      }

      logger.info({ count: validSigs.length, totalProcessed }, "Backfill batch");

      // Process oldest first (reverse order)
      for (const sig of validSigs.reverse()) {
        try {
          await this.processTransaction(sig);
          this.lastSignature = sig.signature;
          await this.saveState(sig.signature, BigInt(sig.slot));
          totalProcessed++;
        } catch (error) {
          logger.error({ error, signature: sig.signature }, "Error processing backfill transaction");
        }
      }

      // Use the oldest signature from this batch to get the next older batch
      beforeSignature = validSigs[validSigs.length - 1].signature;

      // Small delay to avoid rate limiting
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    logger.info({ totalProcessed }, "Backfill finished, switching to live polling");
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

    // Process oldest first
    for (const sig of signatures.reverse()) {
      try {
        await this.processTransaction(sig);
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

  private async fetchSignatures(): Promise<ConfirmedSignatureInfo[]> {
    const options: {
      limit: number;
      before?: string;
      until?: string;
    } = {
      limit: this.batchSize,
    };

    if (this.lastSignature) {
      options.until = this.lastSignature;
    }

    const signatures = await this.connection.getSignaturesForAddress(
      this.programId,
      options
    );

    return signatures.filter((sig) => sig.err === null);
  }

  private async processTransaction(sig: ConfirmedSignatureInfo): Promise<void> {
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
      { signature: sig.signature, eventCount: parsed.events.length },
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
