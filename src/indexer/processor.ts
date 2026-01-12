import { Connection, PublicKey } from "@solana/web3.js";
import { PrismaClient } from "@prisma/client";
import { config, IndexerMode } from "../config.js";
import { Poller } from "./poller.js";
import { WebSocketIndexer, testWebSocketConnection } from "./websocket.js";
import { createChildLogger } from "../logger.js";

const logger = createChildLogger("processor");

export interface ProcessorOptions {
  mode?: IndexerMode;
}

export class Processor {
  private connection: Connection;
  private prisma: PrismaClient | null;
  private programId: PublicKey;
  private mode: IndexerMode;
  private poller: Poller | null = null;
  private wsIndexer: WebSocketIndexer | null = null;
  private isRunning = false;

  constructor(prisma: PrismaClient | null, options?: ProcessorOptions) {
    this.prisma = prisma;
    this.mode = options?.mode || config.indexerMode;
    this.programId = new PublicKey(config.programId);
    this.connection = new Connection(config.rpcUrl, {
      wsEndpoint: config.wsUrl,
      commitment: "confirmed",
    });
  }

  async start(): Promise<void> {
    if (this.isRunning) {
      logger.warn("Processor already running");
      return;
    }

    this.isRunning = true;
    logger.info({ mode: this.mode }, "Starting processor");

    switch (this.mode) {
      case "websocket":
        await this.startWebSocket();
        break;

      case "polling":
        await this.startPolling();
        break;

      case "auto":
      default:
        await this.startAuto();
        break;
    }
  }

  async stop(): Promise<void> {
    logger.info("Stopping processor");
    this.isRunning = false;

    if (this.poller) {
      await this.poller.stop();
      this.poller = null;
    }

    if (this.wsIndexer) {
      await this.wsIndexer.stop();
      this.wsIndexer = null;
    }
  }

  private async startWebSocket(): Promise<void> {
    this.wsIndexer = new WebSocketIndexer({
      connection: this.connection,
      prisma: this.prisma,
      programId: this.programId,
    });

    await this.wsIndexer.start();
  }

  private async startPolling(): Promise<void> {
    this.poller = new Poller({
      connection: this.connection,
      prisma: this.prisma,
      programId: this.programId,
    });

    await this.poller.start();
  }

  private async startAuto(): Promise<void> {
    logger.info("Testing WebSocket connection...");
    const wsAvailable = await testWebSocketConnection(config.wsUrl);

    if (wsAvailable) {
      logger.info("WebSocket available, using WebSocket mode");
      await this.startWebSocket();

      // Backup poller for catching missed events (slower interval when WS is primary)
      this.poller = new Poller({
        connection: this.connection,
        prisma: this.prisma,
        programId: this.programId,
        pollingInterval: 30000,
      });
      await this.poller.start();

      this.monitorWebSocket();
    } else {
      logger.info("WebSocket not available, falling back to polling mode");
      await this.startPolling();
    }
  }

  private monitorWebSocket(): void {
    setInterval(async () => {
      if (!this.isRunning) return;

      if (this.wsIndexer && !this.wsIndexer.isActive()) {
        logger.warn("WebSocket connection lost, relying on polling");

        // Switch to faster polling when WS is down
        if (this.poller) {
          await this.poller.stop();
          this.poller = new Poller({
            connection: this.connection,
            prisma: this.prisma,
            programId: this.programId,
            pollingInterval: config.pollingInterval,
          });
          await this.poller.start();
        }
      }
    }, 10000);
  }

  getStatus(): {
    running: boolean;
    mode: IndexerMode;
    pollerActive: boolean;
    wsActive: boolean;
  } {
    return {
      running: this.isRunning,
      mode: this.mode,
      pollerActive: this.poller !== null,
      wsActive: this.wsIndexer?.isActive() ?? false,
    };
  }
}
