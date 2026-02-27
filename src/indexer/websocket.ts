import {
  Connection,
  PublicKey,
  Logs,
  Context,
  ParsedTransactionWithMeta,
} from "@solana/web3.js";
import { PrismaClient } from "@prisma/client";
import PQueue from "p-queue";
import { config } from "../config.js";
import { parseTransaction, parseTransactionLogs, toTypedEvent } from "../parser/decoder.js";
import { handleEventAtomic, EventContext } from "../db/handlers.js";
import { saveIndexerState } from "../db/supabase.js";
import { createChildLogger } from "../logger.js";

const logger = createChildLogger("websocket");

// Health check interval (30 seconds)
const HEALTH_CHECK_INTERVAL = 30_000;
// Consider connection stale if no activity for 2 minutes
const STALE_THRESHOLD = 120_000;
// Concurrency limits to prevent OOM during high traffic
const MAX_CONCURRENT_HANDLERS = 10;
const MAX_QUEUE_SIZE = 10_000;

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
  private healthCheckTimer: NodeJS.Timeout | null = null;
  private lastActivityTime: number = Date.now();
  private processedCount = 0;
  private errorCount = 0;
  // Concurrency guards
  private isCheckingHealth = false;
  private isReconnecting = false;
  private overflowStopInProgress = false;
  // Bounded concurrency queue to prevent OOM during high traffic
  private logQueue: PQueue;
  private droppedLogs = 0;

  constructor(options: WebSocketIndexerOptions) {
    // Initialize bounded queue for log processing
    this.logQueue = new PQueue({ concurrency: MAX_CONCURRENT_HANDLERS });
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
    this.lastActivityTime = Date.now();
    logger.info({ programId: this.programId.toBase58() }, "Starting WebSocket indexer");

    await this.subscribe();
    this.startHealthCheck();
  }

  async stop(): Promise<void> {
    logger.info({
      processedCount: this.processedCount,
      errorCount: this.errorCount,
      queueSize: this.logQueue.size,
      droppedLogs: this.droppedLogs
    }, "Stopping WebSocket indexer");

    this.isRunning = false;
    this.stopHealthCheck();

    if (this.subscriptionId !== null) {
      try {
        await this.connection.removeOnLogsListener(this.subscriptionId);
        logger.info({ subscriptionId: this.subscriptionId }, "Removed WebSocket subscription");
      } catch (error) {
        logger.warn({ error, subscriptionId: this.subscriptionId }, "Error removing subscription");
      }
      this.subscriptionId = null;
    }

    // Drain remaining queue items before shutdown.
    if (this.logQueue.size > 0) {
      logger.info({ queueSize: this.logQueue.size }, "Draining log queue before shutdown");
      await this.logQueue.onIdle();
    }
  }

  private startHealthCheck(): void {
    this.stopHealthCheck();

    this.healthCheckTimer = setInterval(() => {
      this.checkHealth();
    }, HEALTH_CHECK_INTERVAL);

    logger.debug("Health check timer started");
  }

  private stopHealthCheck(): void {
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = null;
      logger.debug("Health check timer stopped");
    }
  }

  private async checkHealth(): Promise<void> {
    if (!this.isRunning) return;

    // Reentrancy guard - prevent overlapping health checks
    if (this.isCheckingHealth) {
      logger.debug("Health check already in progress, skipping");
      return;
    }

    this.isCheckingHealth = true;
    try {
      const timeSinceActivity = Date.now() - this.lastActivityTime;

      logger.debug({
        timeSinceActivity,
        subscriptionId: this.subscriptionId,
        processedCount: this.processedCount,
        errorCount: this.errorCount
      }, "Health check");

      // Check if connection is stale (no activity for too long)
      if (timeSinceActivity > STALE_THRESHOLD) {
        // Ping RPC before reconnecting - connection may be healthy with no program activity
        try {
          const slot = await this.connection.getSlot();
          logger.info({
            timeSinceActivity,
            slot
          }, "No WebSocket events but RPC is healthy - program may have low activity");
          // RPC is alive, just reset the activity timer instead of reconnecting
          this.lastActivityTime = Date.now();
          return;
        } catch (error) {
          // RPC is down, reconnect
          logger.warn({
            timeSinceActivity,
            threshold: STALE_THRESHOLD,
            error: error instanceof Error ? error.message : String(error)
          }, "WebSocket stale AND RPC ping failed, reconnecting...");
          await this.forceReconnect();
          return;
        }
      }

      // Regular connectivity check (not stale, just verify RPC is up)
      try {
        const slot = await this.connection.getSlot();
        logger.debug({ slot }, "HTTP connectivity OK");
      } catch (error) {
        logger.error({ error }, "Health check failed - connection error");
        await this.forceReconnect();
      }
    } finally {
      this.isCheckingHealth = false;
    }
  }

  private async forceReconnect(): Promise<void> {
    // Concurrency guard - prevent overlapping reconnects
    if (this.isReconnecting) {
      logger.debug("Reconnection already in progress, skipping");
      return;
    }

    this.isReconnecting = true;
    try {
      logger.info("Forcing WebSocket reconnection...");

      // Clean up existing subscription
      if (this.subscriptionId !== null) {
        try {
          await this.connection.removeOnLogsListener(this.subscriptionId);
        } catch (error) {
          logger.debug({ error }, "Error removing old subscription during reconnect");
        }
        this.subscriptionId = null;
      }

      // Reconnect
      await this.reconnect();
    } finally {
      this.isReconnecting = false;
    }
  }

  private async subscribe(): Promise<void> {
    try {
      logger.info("Subscribing to program logs...");

      this.subscriptionId = this.connection.onLogs(
        this.programId,
        // Queue-based processing with backpressure to prevent OOM
        (logs: Logs, ctx: Context) => {
          if (!this.isRunning) {
            return;
          }

          this.lastActivityTime = Date.now();

          if (this.logQueue.size >= MAX_QUEUE_SIZE) {
            this.errorCount++;
            this.droppedLogs++;

            if (this.overflowStopInProgress) {
              return;
            }
            this.overflowStopInProgress = true;

            logger.error(
              {
                queueSize: this.logQueue.size,
                signature: logs.signature,
                errorCount: this.errorCount,
                droppedLogs: this.droppedLogs,
              },
              "WebSocket processing queue is full, entering fail-safe stop so poller catch-up can recover missed events"
            );
            void this.stop().finally(() => {
              this.overflowStopInProgress = false;
            });
            return;
          }

          // Add to bounded queue instead of fire-and-forget
          this.logQueue.add(async () => {
            try {
              await this.handleLogs(logs, ctx);
            } catch (error) {
              this.errorCount++;
              logger.error({
                error: error instanceof Error ? error.message : String(error),
                signature: logs.signature,
                errorCount: this.errorCount
              }, "Error in handleLogs - caught by queue");

              // If too many errors, force reconnect
              if (this.errorCount > 10 && this.errorCount % 10 === 0) {
                logger.warn({ errorCount: this.errorCount }, "High error count, scheduling reconnect");
                this.forceReconnect().catch(e => {
                  logger.error({ error: e }, "Failed to reconnect after errors");
                });
              }
            }
          });
        },
        "confirmed"
      );

      logger.info(
        { subscriptionId: this.subscriptionId },
        "WebSocket subscription active"
      );

      this.retryCount = 0;
      this.errorCount = 0;
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

    const startTime = Date.now();

    try {
      const logEvents = parseTransactionLogs(logs.logs);
      let events = logEvents;
      let parsedTx: ParsedTransactionWithMeta | null = null;
      const shouldEnrichIdentityLocks = logEvents.some(
        (event) =>
          (event.name === "CollectionPointerSet" || event.name === "ParentAssetSet") &&
          typeof event.data.lock !== "boolean"
      );

      if (shouldEnrichIdentityLocks) {
        try {
          parsedTx = await this.connection.getParsedTransaction(logs.signature, {
            maxSupportedTransactionVersion: config.maxSupportedTransactionVersion,
          });

          if (parsedTx) {
            const parsed = parseTransaction(parsedTx);
            if (parsed?.events.length) {
              events = parsed.events;
            }
          }
        } catch (txFetchError) {
          logger.debug(
            {
              signature: logs.signature,
              error: txFetchError instanceof Error ? txFetchError.message : String(txFetchError),
            },
            "Failed to fetch parsed transaction for identity lock enrichment (non-fatal)"
          );
        }
      }

      if (events.length === 0) return;

      logger.debug(
        { signature: logs.signature, eventCount: events.length, slot: ctx.slot },
        "Received logs"
      );

      // Prefer confirmed blockTime from parsed transaction when available.
      const blockTime =
        parsedTx?.blockTime !== null && parsedTx?.blockTime !== undefined
          ? new Date(parsedTx.blockTime * 1000)
          : new Date();

      // Resolve tx_index from block for metadata/tie-break parity with other indexers
      let txIndex: number | undefined;
      try {
        const block = await this.connection.getBlock(ctx.slot, {
          maxSupportedTransactionVersion: config.maxSupportedTransactionVersion,
          transactionDetails: "full",
        });
        if (block?.transactions) {
          const idx = block.transactions.findIndex(
            (tx) => tx.transaction.signatures[0] === logs.signature,
          );
          if (idx >= 0) txIndex = idx;
        }
      } catch (blockError) {
        logger.debug(
          { slot: ctx.slot, error: blockError instanceof Error ? blockError.message : String(blockError) },
          "Failed to resolve tx_index from block (non-fatal)",
        );
      }

      let allEventsProcessed = true;

      for (const [eventOrdinal, event] of events.entries()) {
        const typedEvent = toTypedEvent(event);
        if (!typedEvent) continue;

        const eventCtx: EventContext = {
          signature: logs.signature,
          slot: BigInt(ctx.slot),
          blockTime,
          txIndex,
          eventOrdinal,
          source: "websocket",
        };

        let eventProcessed = true;
        let eventErrorMessage: string | undefined;

        try {
          await handleEventAtomic(this.prisma, typedEvent, eventCtx);
        } catch (eventError) {
          eventProcessed = false;
          allEventsProcessed = false;
          eventErrorMessage = eventError instanceof Error ? eventError.message : String(eventError);
          logger.error({
            error: eventErrorMessage,
            eventType: typedEvent.type,
            signature: logs.signature
          }, "Error handling event — cursor will NOT advance past this tx");
        }

        // Only log to Prisma if in local mode
        if (this.prisma) {
          try {
            await this.prisma.eventLog.create({
              data: {
                eventType: eventProcessed ? typedEvent.type : "PROCESSING_FAILED",
                signature: logs.signature,
                slot: BigInt(ctx.slot),
                blockTime,
                data: event.data as object,
                processed: eventProcessed,
                error: eventErrorMessage,
              },
            });
          } catch (prismaError) {
            logger.warn({
              error: prismaError instanceof Error ? prismaError.message : String(prismaError),
              signature: logs.signature
            }, "Failed to log event to Prisma");
          }
        }
      }

      // Only advance cursor if ALL events in this tx were processed successfully
      if (!allEventsProcessed) {
        this.errorCount++;
        logger.warn({ signature: logs.signature, slot: ctx.slot },
          "Skipping cursor update — failed event(s) in this tx will be retried on restart");
        return;
      }

      // Update state - both local and Supabase modes (with monotonic guard)
      try {
        const newSlot = BigInt(ctx.slot);
        if (this.prisma) {
          const current = await this.prisma.indexerState.findUnique({
            where: { id: "main" },
            select: { lastSlot: true },
          });
          if (current && current.lastSlot !== null && newSlot < current.lastSlot) {
            logger.debug({ slot: ctx.slot, currentSlot: Number(current.lastSlot) },
              "WS cursor update skipped — slot behind current");
          } else {
            await this.prisma.indexerState.upsert({
              where: { id: "main" },
              create: {
                id: "main",
                lastSignature: logs.signature,
                lastSlot: newSlot,
                source: "websocket",
              },
              update: {
                lastSignature: logs.signature,
                lastSlot: newSlot,
                source: "websocket",
              },
            });
          }
        } else {
          // Supabase mode — saveIndexerState already has SQL-level monotonic guard
          await saveIndexerState(logs.signature, newSlot, "websocket");
        }
      } catch (stateError) {
        logger.error({
          error: stateError instanceof Error ? stateError.message : String(stateError),
          signature: logs.signature
        }, "Failed to save indexer state");
      }

      this.processedCount++;
      const duration = Date.now() - startTime;

      if (this.processedCount % 100 === 0) {
        logger.info({
          processedCount: this.processedCount,
          errorCount: this.errorCount,
          queueSize: this.logQueue.size,
          droppedLogs: this.droppedLogs,
          lastDuration: duration
        }, "WebSocket processing stats");
      }

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logger.error({ error: errorMessage, signature: logs.signature }, "Error handling logs");

      // Log errors only in local mode
      if (this.prisma) {
        try {
          await this.prisma.eventLog.create({
            data: {
              eventType: "PROCESSING_FAILED",
              signature: logs.signature,
              slot: BigInt(ctx.slot),
              blockTime: new Date(),
              data: { logs: logs.logs },
              processed: false,
              error: errorMessage,
            },
          });
        } catch (logError) {
          logger.warn({ error: logError }, "Failed to log error to Prisma");
        }
      }

      // Re-throw to be caught by wrapper
      throw error;
    }
  }

  private async reconnect(): Promise<void> {
    if (!this.isRunning) return;

    if (this.retryCount >= this.maxRetries) {
      logger.error({
        retryCount: this.retryCount,
        maxRetries: this.maxRetries
      }, "Max retries exceeded, WebSocket indexer stopped");
      this.isRunning = false;
      this.stopHealthCheck();
      return;
    }

    this.retryCount++;
    logger.info(
      { retryCount: this.retryCount, maxRetries: this.maxRetries, interval: this.reconnectInterval },
      "Reconnecting WebSocket"
    );

    await new Promise((resolve) =>
      setTimeout(resolve, this.reconnectInterval)
    );

    // Re-check after timeout - stop() may have been called during wait
    if (!this.isRunning) {
      logger.info("Reconnect aborted - stop() was called during wait");
      return;
    }

    await this.subscribe();
  }

  isActive(): boolean {
    return this.isRunning && this.subscriptionId !== null;
  }

  /**
   * Check if WebSocket is in recovery mode (running but reconnecting)
   * Used by monitor to avoid killing WS during self-healing
   */
  isRecovering(): boolean {
    return this.isRunning && (this.isReconnecting || this.isCheckingHealth);
  }

  getStats(): {
    processedCount: number;
    errorCount: number;
    lastActivity: number;
    queueSize: number;
    droppedLogs: number;
  } {
    return {
      processedCount: this.processedCount,
      errorCount: this.errorCount,
      lastActivity: this.lastActivityTime,
      queueSize: this.logQueue.size,
      droppedLogs: this.droppedLogs,
    };
  }
}

/**
 * Test if WebSocket endpoint is available
 * @param rpcUrl - HTTP RPC endpoint for connection test
 * @param wsUrl - WebSocket endpoint to configure
 */
export async function testWebSocketConnection(rpcUrl: string, wsUrl: string): Promise<boolean> {
  try {
    logger.debug({ rpcUrl, wsUrl }, "Testing WebSocket connection");

    // Use HTTP endpoint for getSlot, but configure WS endpoint
    const connection = new Connection(rpcUrl, {
      wsEndpoint: wsUrl,
      commitment: "confirmed",
    });
    // Test HTTP connectivity first
    const slot = await connection.getSlot();
    logger.debug({ slot }, "WebSocket connection test successful");
    return true;
  } catch (error) {
    logger.debug({ error }, "WebSocket connection test failed");
    return false;
  }
}
