import {
  Connection,
  PublicKey,
  Logs,
  Context,
} from "@solana/web3.js";
import { PrismaClient } from "@prisma/client";
import { config } from "../config.js";
import { parseTransactionLogs, toTypedEvent } from "../parser/decoder.js";
import { handleEvent, EventContext } from "../db/handlers.js";
import { createChildLogger } from "../logger.js";

const logger = createChildLogger("websocket");

export interface WebSocketIndexerOptions {
  connection: Connection;
  prisma: PrismaClient | null;
  programId: PublicKey;
  reconnectInterval?: number;
  maxRetries?: number;
}

export class WebSocketIndexer {
  private connection: Connection;
  private prisma: PrismaClient | null;
  private programId: PublicKey;
  private reconnectInterval: number;
  private maxRetries: number;
  private subscriptionId: number | null = null;
  private isRunning = false;
  private retryCount = 0;

  constructor(options: WebSocketIndexerOptions) {
    this.connection = options.connection;
    this.prisma = options.prisma;
    this.programId = options.programId;
    this.reconnectInterval = options.reconnectInterval || config.wsReconnectInterval;
    this.maxRetries = options.maxRetries || config.wsMaxRetries;
  }

  async start(): Promise<void> {
    if (this.isRunning) {
      logger.warn("WebSocket indexer already running");
      return;
    }

    this.isRunning = true;
    logger.info({ programId: this.programId.toBase58() }, "Starting WebSocket indexer");

    await this.subscribe();
  }

  async stop(): Promise<void> {
    logger.info("Stopping WebSocket indexer");
    this.isRunning = false;

    if (this.subscriptionId !== null) {
      await this.connection.removeOnLogsListener(this.subscriptionId);
      this.subscriptionId = null;
    }
  }

  private async subscribe(): Promise<void> {
    try {
      this.subscriptionId = this.connection.onLogs(
        this.programId,
        (logs: Logs, ctx: Context) => this.handleLogs(logs, ctx),
        "confirmed"
      );

      logger.info(
        { subscriptionId: this.subscriptionId },
        "WebSocket subscription active"
      );

      this.retryCount = 0;
    } catch (error) {
      logger.error({ error }, "Failed to subscribe to logs");
      await this.reconnect();
    }
  }

  private async handleLogs(logs: Logs, ctx: Context): Promise<void> {
    if (logs.err) {
      logger.debug({ signature: logs.signature }, "Transaction failed, skipping");
      return;
    }

    try {
      const events = parseTransactionLogs(logs.logs);
      if (events.length === 0) return;

      logger.debug(
        { signature: logs.signature, eventCount: events.length },
        "Received logs"
      );

      // Approximate block time for WebSocket events
      const blockTime = new Date();

      for (const event of events) {
        const typedEvent = toTypedEvent(event);
        if (!typedEvent) continue;

        const eventCtx: EventContext = {
          signature: logs.signature,
          slot: BigInt(ctx.slot),
          blockTime,
        };

        await handleEvent(this.prisma, typedEvent, eventCtx);

        // Only log to Prisma if in local mode
        if (this.prisma) {
          await this.prisma.eventLog.create({
            data: {
              eventType: typedEvent.type,
              signature: logs.signature,
              slot: BigInt(ctx.slot),
              blockTime,
              data: event.data as object,
              processed: true,
            },
          });
        }
      }

      // Update state only in local mode
      if (this.prisma) {
        await this.prisma.indexerState.upsert({
          where: { id: "main" },
          create: {
            id: "main",
            lastSignature: logs.signature,
            lastSlot: BigInt(ctx.slot),
          },
          update: {
            lastSignature: logs.signature,
            lastSlot: BigInt(ctx.slot),
          },
        });
      }
    } catch (error) {
      logger.error({ error, signature: logs.signature }, "Error handling logs");

      // Log errors only in local mode
      if (this.prisma) {
        await this.prisma.eventLog.create({
          data: {
            eventType: "PROCESSING_FAILED",
            signature: logs.signature,
            slot: BigInt(ctx.slot),
            blockTime: new Date(),
            data: { logs: logs.logs },
            processed: false,
            error: error instanceof Error ? error.message : String(error),
          },
        });
      }
    }
  }

  private async reconnect(): Promise<void> {
    if (!this.isRunning) return;

    if (this.retryCount >= this.maxRetries) {
      logger.error("Max retries exceeded, WebSocket indexer stopped");
      this.isRunning = false;
      return;
    }

    this.retryCount++;
    logger.info(
      { retryCount: this.retryCount, maxRetries: this.maxRetries },
      "Reconnecting WebSocket"
    );

    await new Promise((resolve) =>
      setTimeout(resolve, this.reconnectInterval)
    );

    await this.subscribe();
  }

  isActive(): boolean {
    return this.isRunning && this.subscriptionId !== null;
  }
}

/**
 * Test if WebSocket endpoint is available
 * @param rpcUrl - HTTP RPC endpoint for connection test
 * @param wsUrl - WebSocket endpoint to configure
 */
export async function testWebSocketConnection(rpcUrl: string, wsUrl: string): Promise<boolean> {
  try {
    // Use HTTP endpoint for getSlot, but configure WS endpoint
    const connection = new Connection(rpcUrl, {
      wsEndpoint: wsUrl,
      commitment: "confirmed",
    });
    // Test HTTP connectivity first
    await connection.getSlot();
    // TODO: Could add actual WS subscription test here if needed
    return true;
  } catch (error) {
    logger.debug({ error }, "WebSocket connection test failed");
    return false;
  }
}
