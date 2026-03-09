import { Connection, PublicKey } from "@solana/web3.js";
import { PrismaClient } from "@prisma/client";
import { Pool } from "pg";
import { config, IndexerMode } from "../config.js";
import { Poller } from "./poller.js";
import { WebSocketIndexer, testWebSocketConnection } from "./websocket.js";
import { DataVerifier } from "./verifier.js";
import { createChildLogger } from "../logger.js";
import { enableLocalDerivedDigests } from "../db/handlers.js";
import { setVerifierActive } from "../observability/integrity-metrics.js";

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

    try {
      switch (this.mode) {
        case "websocket":
          await this.startWebSocket();
          this.monitorWebSocket();
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

      if (this.prisma && config.dbMode === "local") {
        enableLocalDerivedDigests(this.prisma);
        logger.info("Local derived digest workers enabled after bootstrap");
      }
    } catch (error) {
      logger.error({ error }, "Processor startup failed, cleaning up partial state");
      try {
        await this.stop();
      } catch (cleanupError) {
        logger.error({ error: cleanupError }, "Processor startup cleanup failed");
      }
      throw error;
    }
  }

  private async startVerifier(): Promise<void> {
    setVerifierActive(false);
    this.verifier = new DataVerifier(
      this.connection,
      this.prisma,
      this.pool,
      config.verifyIntervalMs
    );

    await this.verifier.start();
    setVerifierActive(true);
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
    setVerifierActive(false);

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
    const nextWsIndexer = new WebSocketIndexer({
      connection: this.connection,
      prisma: this.prisma,
      programId: this.programId,
    });
    await nextWsIndexer.start();
    this.wsIndexer = nextWsIndexer;
  }

  private createPoller(pollingInterval?: number): Poller {
    return new Poller({
      connection: this.connection,
      prisma: this.prisma,
      programId: this.programId,
      pollingInterval,
    });
  }

  private async startPolling(): Promise<void> {
    const nextPoller = this.createPoller();
    await nextPoller.start();
    this.poller = nextPoller;
  }

  private async bootstrapAutoCatchUp(): Promise<void> {
    const bootstrapPoller = this.createPoller();
    await bootstrapPoller.bootstrap();
  }

  private async startAutoWebSocketPipeline(): Promise<void> {
    await this.bootstrapAutoCatchUp();
    await this.startWebSocket();
    this.monitorWebSocket();
    await this.bootstrapAutoCatchUp();
  }

  private async startAuto(): Promise<void> {
    logger.info("Testing WebSocket connection...");
    const wsAvailable = await testWebSocketConnection(config.rpcUrl, config.wsUrl, config.programId);

    if (wsAvailable) {
      try {
        logger.info("WebSocket available, using WebSocket mode");
        await this.startAutoWebSocketPipeline();
        return;
      } catch (error) {
        logger.warn({ error }, "WebSocket startup failed, falling back to polling mode");
        if (this.wsMonitorInterval) {
          clearTimeout(this.wsMonitorInterval);
          this.wsMonitorInterval = null;
        }
        if (this.wsIndexer) {
          try {
            await this.wsIndexer.stop();
          } catch (stopError) {
            logger.warn({ error: stopError }, "Failed to stop WebSocket indexer after startup error");
          }
          this.wsIndexer = null;
        }
        await this.startPolling();
        return;
      }
    }

    logger.info("WebSocket not available, falling back to polling mode");
    await this.startPolling();
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
        if (this.mode === "websocket") {
          logger.warn("WebSocket connection lost and not recovering, restarting websocket with catch-up");
          await this.recoverWebSocketMode();
        } else {
          logger.warn("WebSocket connection lost and not recovering, switching to polling");
          await this.failOverToPolling();
        }
      } catch (error) {
        logger.error({ error }, "Error in WebSocket monitor fallback");
      } finally {
        this.wsMonitorInProgress = false;
      }
    }

    if (this.mode === "websocket" || this.wsIndexer) {
      this.scheduleNextWsCheck();
    }
  }

  private scheduleNextWsCheck(): void {
    if (!this.isRunning) return;
    this.wsMonitorInterval = setTimeout(() => this.runWebSocketCheck(), 10000);
  }

  private async recoverWebSocketMode(): Promise<void> {
    if (this.wsIndexer) {
      try {
        await this.wsIndexer.stop();
      } catch (error) {
        logger.warn({ error }, "Failed to stop inactive WebSocket indexer before recovery");
      }
      this.wsIndexer = null;
    }

    await this.bootstrapAutoCatchUp();
    await this.startWebSocket();
    await this.bootstrapAutoCatchUp();
  }

  private async failOverToPolling(): Promise<void> {
    if (!this.poller) {
      const nextPoller = this.createPoller(config.pollingInterval);
      await nextPoller.start();
      this.poller = nextPoller;
    }

    if (this.wsIndexer) {
      try {
        await this.wsIndexer.stop();
      } catch (error) {
        logger.warn({ error }, "Failed to stop WebSocket indexer during polling failover");
      }
      this.wsIndexer = null;
    }

    if (this.wsMonitorInterval) {
      clearTimeout(this.wsMonitorInterval);
      this.wsMonitorInterval = null;
    }
  }

  getStatus(): {
    running: boolean;
    mode: IndexerMode;
    configuredMode: IndexerMode;
    pollerActive: boolean;
    wsActive: boolean;
    verifierActive: boolean;
    verifierStats?: ReturnType<DataVerifier["getStats"]>;
  } {
    const wsActive = this.wsIndexer?.isActive() ?? false;
    const pollerActive = this.poller !== null;
    const effectiveMode: IndexerMode = pollerActive && !wsActive
      ? "polling"
      : wsActive
        ? "websocket"
        : this.mode;

    return {
      running: this.isRunning,
      mode: effectiveMode,
      configuredMode: this.mode,
      pollerActive,
      wsActive,
      verifierActive: this.verifier !== null,
      verifierStats: this.verifier?.getStats(),
    };
  }
}
