import { Connection, PublicKey } from "@solana/web3.js";
import { PrismaClient } from "@prisma/client";
import { Pool } from "pg";
import { config, IndexerMode } from "../config.js";
import { Poller, type PollerBootstrapOptions } from "./poller.js";
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
  private activeBootstrapPollers = new Set<Poller>();
  private pendingPoller: Poller | null = null;
  private pendingPollerStart: Promise<void> | null = null;
  private pendingWsIndexer: WebSocketIndexer | null = null;
  private pendingWsStart: Promise<void> | null = null;
  private pendingVerifier: DataVerifier | null = null;
  private pendingVerifierStart: Promise<void> | null = null;

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
      // Run verifier concurrently with ingestion/bootstrap so websocket/auto
      // do not accumulate unbounded PENDING rows during long catch-up.
      await this.startVerifier();
      if (!this.isRunning) {
        return;
      }

      switch (this.mode) {
        case "websocket":
          await this.startWebSocketPipeline();
          break;

        case "polling":
          await this.startPolling();
          break;

        case "auto":
        default:
          await this.startAuto();
          break;
      }

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
    const nextVerifier = new DataVerifier(
      this.connection,
      this.prisma,
      this.pool,
      config.verifyIntervalMs
    );
    this.verifier = nextVerifier;
    this.pendingVerifier = nextVerifier;
    const startPromise = nextVerifier.start();
    this.pendingVerifierStart = startPromise;
    try {
      await startPromise;
    } finally {
      if (this.pendingVerifierStart === startPromise) {
        this.pendingVerifierStart = null;
      }
      if (this.pendingVerifier === nextVerifier) {
        this.pendingVerifier = null;
      }
    }
    if (!this.isRunning || this.verifier !== nextVerifier) {
      await nextVerifier.stop();
      return;
    }
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

    const verifier = this.verifier;
    const pendingVerifier = this.pendingVerifier;
    const pendingVerifierStart = this.pendingVerifierStart;

    if (verifier) {
      await verifier.stop();
    } else if (pendingVerifier) {
      await pendingVerifier.stop();
    }
    if (pendingVerifierStart) {
      await Promise.allSettled([pendingVerifierStart]);
    }
    this.verifier = null;
    this.pendingVerifier = null;
    this.pendingVerifierStart = null;
    setVerifierActive(false);

    if (this.activeBootstrapPollers.size > 0) {
      const bootstrapPollers = [...this.activeBootstrapPollers];
      this.activeBootstrapPollers.clear();
      await Promise.allSettled(bootstrapPollers.map((poller) => poller.stop()));
    }

    const pendingPoller = this.pendingPoller;
    const pendingWsIndexer = this.pendingWsIndexer;
    const pendingStarts = [
      this.pendingPollerStart,
      this.pendingWsStart,
    ].filter((promise): promise is Promise<void> => promise !== null);

    if (pendingPoller && pendingPoller !== this.poller) {
      await pendingPoller.stop();
    }

    if (pendingWsIndexer && pendingWsIndexer !== this.wsIndexer) {
      await pendingWsIndexer.stop();
    }

    if (pendingStarts.length > 0) {
      await Promise.allSettled(pendingStarts);
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
    const nextWsIndexer = new WebSocketIndexer({
      connection: this.connection,
      prisma: this.prisma,
      programId: this.programId,
      wsUrl: config.wsUrl,
    });
    this.pendingWsIndexer = nextWsIndexer;
    const startPromise = nextWsIndexer.start();
    this.pendingWsStart = startPromise;
    try {
      await startPromise;
    } finally {
      if (this.pendingWsStart === startPromise) {
        this.pendingWsStart = null;
      }
      if (this.pendingWsIndexer === nextWsIndexer) {
        this.pendingWsIndexer = null;
      }
    }
    if (!this.isRunning) {
      await nextWsIndexer.stop();
      return;
    }
    if (!nextWsIndexer.isActive()) {
      await nextWsIndexer.stop();
      throw new Error("WebSocket indexer failed to establish an active subscription");
    }
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
    this.pendingPoller = nextPoller;
    const startPromise = nextPoller.start();
    this.pendingPollerStart = startPromise;
    try {
      await startPromise;
    } finally {
      if (this.pendingPollerStart === startPromise) {
        this.pendingPollerStart = null;
      }
      if (this.pendingPoller === nextPoller) {
        this.pendingPoller = null;
      }
    }
    if (!this.isRunning) {
      await nextPoller.stop();
      return;
    }
    this.poller = nextPoller;
  }

  private async bootstrapAutoCatchUp(options?: PollerBootstrapOptions): Promise<void> {
    if (!this.isRunning) return;
    const bootstrapPoller = this.createPoller();
    this.activeBootstrapPollers.add(bootstrapPoller);
    try {
      await bootstrapPoller.bootstrap(options);
    } finally {
      this.activeBootstrapPollers.delete(bootstrapPoller);
    }
  }

  private async startWebSocketPipeline(): Promise<void> {
    await this.bootstrapAutoCatchUp();
    if (!this.isRunning) return;
    await this.startWebSocket();
    if (!this.isRunning) return;
    // Close the bootstrap -> live subscription gap with a short catch-up pass.
    await this.bootstrapAutoCatchUp({ suppressEventLogWrites: true });
    if (!this.isRunning) return;
    this.monitorWebSocket();
  }

  private async startAutoWebSocketPipeline(): Promise<void> {
    await this.startWebSocketPipeline();
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

    const shouldRetryAutoFailover = this.mode === "auto" && !this.wsIndexer && !this.poller;
    if (shouldRetryAutoFailover || (this.wsIndexer && !this.wsIndexer.isActive())) {
      // Check if WS is in self-healing mode (reconnecting/health-checking)
      if (this.wsIndexer?.isRecovering()) {
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

    if (this.mode === "websocket" || this.wsIndexer || (this.mode === "auto" && !this.poller)) {
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
    if (!this.isRunning) return;
    await this.startWebSocket();
    if (!this.isRunning) return;
    await this.bootstrapAutoCatchUp({ suppressEventLogWrites: true });
  }

  private async failOverToPolling(): Promise<void> {
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

    if (!this.isRunning || this.poller) {
      return;
    }

    await this.startPolling();
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
