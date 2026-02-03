import { Connection, PublicKey } from "@solana/web3.js";
import { PrismaClient } from "@prisma/client";
import { Pool } from "pg";
import { config, IndexerMode } from "../config.js";
import { Poller } from "./poller.js";
import { WebSocketIndexer, testWebSocketConnection } from "./websocket.js";
import { DataVerifier } from "./verifier.js";
import { createChildLogger } from "../logger.js";

const logger = createChildLogger("processor");

export interface ProcessorOptions {
  mode?: IndexerMode;
}

export class Processor {
  private connection: Connection;
  private prisma: PrismaClient | null;
  private pool: Pool | null;
  private programId: PublicKey;
  private mode: IndexerMode;
  private poller: Poller | null = null;
  private wsIndexer: WebSocketIndexer | null = null;
  private verifier: DataVerifier | null = null;
  private isRunning = false;
  private wsMonitorInterval: ReturnType<typeof setInterval> | null = null;
  private wsMonitorInProgress = false; // Reentrancy guard for async interval

  constructor(prisma: PrismaClient | null, pool: Pool | null = null, options?: ProcessorOptions) {
    this.prisma = prisma;
    this.pool = pool;
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

    // Start background verifier for reorg resilience
    await this.startVerifier();
  }

  private async startVerifier(): Promise<void> {
    if (!config.verificationEnabled) {
      logger.info("Verification disabled via config");
      return;
    }

    this.verifier = new DataVerifier(
      this.connection,
      this.prisma,
      this.pool,
      config.verifyIntervalMs
    );

    await this.verifier.start();
    logger.info({ intervalMs: config.verifyIntervalMs }, "Background verifier started");
  }

  async stop(): Promise<void> {
    logger.info("Stopping processor");
    this.isRunning = false;

    // Clean up WebSocket monitor timeout
    if (this.wsMonitorInterval) {
      clearTimeout(this.wsMonitorInterval);
      this.wsMonitorInterval = null;
    }

    if (this.verifier) {
      await this.verifier.stop();
      this.verifier = null;
    }

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
    const wsAvailable = await testWebSocketConnection(config.rpcUrl, config.wsUrl);

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
    // Clean up any existing timeout before creating new one
    if (this.wsMonitorInterval) {
      clearTimeout(this.wsMonitorInterval);
      this.wsMonitorInterval = null;
    }

    const scheduleNextCheck = () => {
      if (!this.isRunning) return;
      this.wsMonitorInterval = setTimeout(() => this.runWebSocketCheck(), 10000);
    };

    scheduleNextCheck();
  }

  private async runWebSocketCheck(): Promise<void> {
    // Skip if not running or previous check still in progress
    if (!this.isRunning || this.wsMonitorInProgress) {
      this.scheduleNextWsCheck();
      return;
    }

    if (this.wsIndexer && !this.wsIndexer.isActive()) {
      // Check if WS is in self-healing mode (reconnecting/health-checking)
      if (this.wsIndexer.isRecovering()) {
        logger.debug("WebSocket in recovery mode, waiting for self-heal");
        this.scheduleNextWsCheck();
        return;
      }

      this.wsMonitorInProgress = true;
      try {
        logger.warn("WebSocket connection lost and not recovering, relying on polling");

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
      } catch (error) {
        logger.error({ error }, "Error in WebSocket monitor fallback");
      } finally {
        this.wsMonitorInProgress = false;
      }
    }

    this.scheduleNextWsCheck();
  }

  private scheduleNextWsCheck(): void {
    if (!this.isRunning) return;
    this.wsMonitorInterval = setTimeout(() => this.runWebSocketCheck(), 10000);
  }

  getStatus(): {
    running: boolean;
    mode: IndexerMode;
    pollerActive: boolean;
    wsActive: boolean;
    verifierActive: boolean;
    verifierStats?: ReturnType<DataVerifier["getStats"]>;
  } {
    return {
      running: this.isRunning,
      mode: this.mode,
      pollerActive: this.poller !== null,
      wsActive: this.wsIndexer?.isActive() ?? false,
      verifierActive: this.verifier !== null,
      verifierStats: this.verifier?.getStats(),
    };
  }
}
