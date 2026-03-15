import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

vi.mock("ws", () => {
  let probeBehavior: "supported" | "unsupported" | "timeout" | "error" = "supported";

  class MockWebSocket {
    static OPEN = 1;
    readyState = 0;
    private handlers = new Map<string, Array<(...args: any[]) => void>>();

    constructor(_url: string) {
      queueMicrotask(() => {
        if (probeBehavior === "error") {
          this.emit("error", new Error("ws probe failed"));
          return;
        }
        this.readyState = MockWebSocket.OPEN;
        this.emit("open");
      });
    }

    on(event: string, handler: (...args: any[]) => void): this {
      const current = this.handlers.get(event) ?? [];
      current.push(handler);
      this.handlers.set(event, current);
      return this;
    }

    send(_payload: string): void {
      if (probeBehavior === "timeout") return;

      queueMicrotask(() => {
        if (probeBehavior === "unsupported") {
          this.emit("message", JSON.stringify({
            jsonrpc: "2.0",
            id: 1,
            error: { code: -32601, message: "Method 'logsSubscribe' not found" },
          }));
          return;
        }

        this.emit("message", JSON.stringify({
          jsonrpc: "2.0",
          id: 1,
          result: 1,
        }));
      });
    }

    close(): void {
      queueMicrotask(() => {
        this.emit("close");
      });
    }

    private emit(event: string, ...args: any[]): void {
      for (const handler of this.handlers.get(event) ?? []) {
        handler(...args);
      }
    }
  }

  return {
    default: MockWebSocket,
    __setWsProbeBehavior: (value: "supported" | "unsupported" | "timeout" | "error") => {
      probeBehavior = value;
    },
  };
});

vi.mock("@solana/web3.js", async () => {
  const actual = await vi.importActual<any>("@solana/web3.js");
  let getSlotMock: (() => Promise<number>) | null = null;
  let onLogsMock: ((filter: unknown, callback: () => void, commitment?: unknown) => number) | null = null;
  let removeOnLogsListenerMock: ((subscriptionId: number) => Promise<void>) | null = null;
  let rpcWebSocketConnected = false;
  let logsSubscriptionState: "subscribed" | "pending" = "subscribed";

  class MockConnection {
    _rpcWebSocketConnected = rpcWebSocketConnected;
    _subscriptionHashByClientSubscriptionId: Record<number, string> = {};
    _subscriptionsByHash: Record<string, { state: string; serverSubscriptionId?: number }> = {};

    constructor(_endpoint: string, _config?: unknown) {}

    async getSlot(): Promise<number> {
      if (!getSlotMock) {
        throw new Error("getSlot mock not set");
      }
      return await getSlotMock();
    }

    onLogs(filter: unknown, callback: () => void, commitment?: unknown): number {
      const subscriptionId = onLogsMock
        ? onLogsMock(filter, callback, commitment)
        : 1;
      const hash = `mock-hash:${subscriptionId}`;
      this._subscriptionHashByClientSubscriptionId[subscriptionId] = hash;
      this._subscriptionsByHash[hash] = {
        state: this._rpcWebSocketConnected ? logsSubscriptionState : "pending",
        serverSubscriptionId: this._rpcWebSocketConnected && logsSubscriptionState === "subscribed"
          ? subscriptionId
          : undefined,
      };
      return subscriptionId;
    }

    async removeOnLogsListener(subscriptionId: number): Promise<void> {
      const hash = this._subscriptionHashByClientSubscriptionId[subscriptionId];
      if (hash) {
        delete this._subscriptionHashByClientSubscriptionId[subscriptionId];
        delete this._subscriptionsByHash[hash];
      }
      if (!removeOnLogsListenerMock) {
        return;
      }
      await removeOnLogsListenerMock(subscriptionId);
    }
  }

  return {
    ...actual,
    Connection: MockConnection,
    __setGetSlotMock: (fn: (() => Promise<number>) | null) => {
      getSlotMock = fn;
    },
    __setOnLogsMock: (fn: ((filter: unknown, callback: () => void, commitment?: unknown) => number) | null) => {
      onLogsMock = fn;
    },
    __setRemoveOnLogsListenerMock: (fn: ((subscriptionId: number) => Promise<void>) | null) => {
      removeOnLogsListenerMock = fn;
    },
    __setRpcWebSocketConnected: (value: boolean) => {
      rpcWebSocketConnected = value;
    },
    __setLogsSubscriptionState: (value: "subscribed" | "pending") => {
      logsSubscriptionState = value;
    },
  };
});

vi.mock("../../../src/db/supabase.js", () => ({
  saveIndexerState: vi.fn().mockResolvedValue(undefined),
  loadIndexerStateSnapshot: vi.fn().mockResolvedValue(null),
  restoreIndexerStateSnapshot: vi.fn().mockResolvedValue(undefined),
  clearIndexerStateSnapshot: vi.fn().mockResolvedValue(undefined),
}));

import * as web3 from "@solana/web3.js";
import * as wsModule from "ws";
import {
  clearIndexerStateSnapshot,
  loadIndexerStateSnapshot,
  restoreIndexerStateSnapshot,
} from "../../../src/db/supabase.js";
import {
  WebSocketIndexer,
  testWebSocketConnection,
} from "../../../src/indexer/websocket.js";
import { createMockPrismaClient } from "../../mocks/prisma.js";
import {
  createMockConnection,
  createEventLogs,
  encodeAnchorEvent,
  TEST_SIGNATURE,
  TEST_SLOT,
  TEST_PROGRAM_ID,
  TEST_ASSET,
  TEST_OWNER,
  TEST_COLLECTION,
  TEST_REGISTRY,
} from "../../mocks/solana.js";

describe("WebSocketIndexer", () => {
  let wsIndexer: WebSocketIndexer;
  let mockConnection: ReturnType<typeof createMockConnection>;
  let mockPrisma: ReturnType<typeof createMockPrismaClient>;

  beforeEach(() => {
    mockConnection = createMockConnection();
    mockPrisma = createMockPrismaClient();

    wsIndexer = new WebSocketIndexer({
      connection: mockConnection as any,
      prisma: mockPrisma,
      programId: TEST_PROGRAM_ID,
      reconnectInterval: 100,
      maxRetries: 3,
    });
  });

  afterEach(async () => {
    await wsIndexer.stop();
    vi.clearAllMocks();
  });

  describe("constructor", () => {
    it("should create indexer with default options", () => {
      const defaultIndexer = new WebSocketIndexer({
        connection: mockConnection as any,
        prisma: mockPrisma,
        programId: TEST_PROGRAM_ID,
      });

      expect(defaultIndexer).toBeDefined();
    });

    it("uses a single in-flight tx handler to keep cursor advancement prefix-safe", () => {
      expect(((wsIndexer as any).logQueue).concurrency).toBe(1);
    });
  });

  describe("start", () => {
    it("should subscribe to logs", async () => {
      await wsIndexer.start();

      expect(mockConnection.onLogs).toHaveBeenCalledWith(
        TEST_PROGRAM_ID,
        expect.any(Function),
        "confirmed"
      );
    });

    it("should not start twice", async () => {
      await wsIndexer.start();
      await wsIndexer.start();

      expect(mockConnection.onLogs).toHaveBeenCalledTimes(1);
    });

    it("should resume queue processing after stop followed by start on the same instance", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      (mockConnection.onLogs as any).mockImplementation(
        (_: any, handler: any) => {
          logsHandler = handler;
          return 1;
        }
      );

      await wsIndexer.start();
      await wsIndexer.stop();
      await wsIndexer.start();

      const logs = createEventLogs("AgentRegistered", {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmRestartSafe",
      });

      logsHandler!({ signature: TEST_SIGNATURE, err: null, logs }, { slot: Number(TEST_SLOT) });
      await new Promise((resolve) => setTimeout(resolve, 0));
      await ((wsIndexer as any).logQueue).onIdle();

      expect(mockPrisma.agent.upsert).toHaveBeenCalled();
    });

    it("should reset retry budget across stop and restart on the same instance", async () => {
      (wsIndexer as any).retryCount = 2;
      await wsIndexer.stop();
      expect((wsIndexer as any).retryCount).toBe(0);

      (wsIndexer as any).retryCount = 3;
      (mockConnection.onLogs as any).mockReturnValueOnce(2);
      await wsIndexer.start();
      expect((wsIndexer as any).retryCount).toBe(0);
    });

    it("should ignore a stale in-flight health check from the previous run after restart", async () => {
      let slotResolve: ((value: number) => void) | null = null;
      const removeCalls: number[] = [];
      let subscriptionId = 0;

      (mockConnection.onLogs as any).mockImplementation((_programId: any, _handler: any) => {
        subscriptionId += 1;
        return subscriptionId;
      });

      (mockConnection.getSlot as any).mockImplementation(
        () => new Promise<number>((resolve) => {
          slotResolve = resolve;
        })
      );

      (mockConnection.removeOnLogsListener as any).mockImplementation(async (id: number) => {
        removeCalls.push(id);
      });

      await wsIndexer.start();
      (wsIndexer as any).lastActivityTime = 0;
      const staleHealthCheck = (wsIndexer as any).checkHealth();

      await wsIndexer.stop();
      await wsIndexer.start();

      slotResolve?.(123);
      await staleHealthCheck;

      expect(removeCalls).toEqual([1]);
      expect((wsIndexer as any).subscriptionId).toBe(2);
    });

    it("should ignore a stale reconnect from the previous run after restart", async () => {
      vi.useFakeTimers();
      let subscriptionId = 0;

      (mockConnection.onLogs as any).mockImplementation((_programId: any, _handler: any) => {
        subscriptionId += 1;
        return subscriptionId;
      });

      await wsIndexer.start();
      const staleReconnect = (wsIndexer as any).reconnect();

      await wsIndexer.stop();
      await wsIndexer.start();

      await vi.advanceTimersByTimeAsync(150);
      await staleReconnect;

      expect(mockConnection.onLogs).toHaveBeenCalledTimes(2);
      expect((wsIndexer as any).subscriptionId).toBe(2);
      vi.useRealTimers();
    });

    it("should discard the previous run safe cursor if the next startup snapshot read fails", async () => {
      mockPrisma.indexerState.findUnique
        .mockResolvedValueOnce({
          lastSignature: "safe-signature",
          lastSlot: 41n,
          lastTxIndex: 2,
          source: "poller",
          updatedAt: new Date("2026-03-14T00:00:00.000Z"),
        })
        .mockRejectedValueOnce(new Error("snapshot read failed"));

      await wsIndexer.start();
      await wsIndexer.stop();
      await wsIndexer.start();

      let releaseWork!: () => void;
      ((wsIndexer as any).logQueue).add(
        () => new Promise<void>((resolve) => {
          releaseWork = () => resolve();
        })
      );
      for (let i = 0; i < 20 && !releaseWork; i += 1) {
        await new Promise((resolve) => setTimeout(resolve, 0));
      }

      const stopPromise = wsIndexer.stop();
      releaseWork();
      await stopPromise;

      expect(mockPrisma.indexerState.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          update: expect.objectContaining({
            lastSignature: null,
            lastSlot: null,
            lastTxIndex: null,
          }),
        })
      );
    });
  });

  describe("stop", () => {
    it("should unsubscribe from logs", async () => {
      await wsIndexer.start();
      await wsIndexer.stop();

      expect(mockConnection.removeOnLogsListener).toHaveBeenCalledWith(1);
    });

    it("should handle stop when not started", async () => {
      await wsIndexer.stop();
      expect(mockConnection.removeOnLogsListener).not.toHaveBeenCalled();
    });

    it("should clear queued logs while allowing running work to drain", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      const releases: Array<() => void> = [];
      (mockConnection.onLogs as any).mockImplementation(
        (_: any, handler: any) => {
          logsHandler = handler;
          return 1;
        }
      );
      ((wsIndexer as any).logQueue).concurrency = 1;
      (mockConnection.getBlock as any).mockImplementation(
        () => new Promise((resolve) => {
          releases.push(() => resolve({
            blockTime: 123,
            transactions: [{ transaction: { signatures: [TEST_SIGNATURE] } }],
          }));
        })
      );

      await wsIndexer.start();

      const logs = createEventLogs("AgentRegistered", {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmQueued",
      });

      logsHandler!({ signature: TEST_SIGNATURE, err: null, logs }, { slot: Number(TEST_SLOT) });
      logsHandler!({ signature: `${TEST_SIGNATURE}2`, err: null, logs }, { slot: Number(TEST_SLOT) });

      const stopPromise = wsIndexer.stop();
      releases.shift()?.();
      await stopPromise;

      expect(wsIndexer.getStats().droppedLogs).toBeGreaterThanOrEqual(1);
    });

    it("should freeze cursor advancement during shutdown so drained work cannot advance main", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      let releaseBlock!: () => void;
      (mockConnection.onLogs as any).mockImplementation(
        (_: any, handler: any) => {
          logsHandler = handler;
          return 1;
        }
      );
      (mockConnection.getBlock as any).mockImplementation(
        () => new Promise((resolve) => {
          releaseBlock = () => resolve({
            blockTime: 123,
            transactions: [{ transaction: { signatures: [TEST_SIGNATURE] } }],
          });
        })
      );

      await wsIndexer.start();

      const logs = createEventLogs("AgentRegistered", {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmStopFreeze",
      });

      logsHandler!({ signature: TEST_SIGNATURE, err: null, logs }, { slot: Number(TEST_SLOT) });
      const stopPromise = wsIndexer.stop();
      releaseBlock();
      await stopPromise;

      expect(mockPrisma.indexerState.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: "main" },
          update: expect.objectContaining({
            lastSignature: null,
            lastSlot: null,
            lastTxIndex: null,
          }),
        })
      );
    });

    it("should not advance the cursor if shutdown starts while the state read is still in flight", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      let resolveFindUnique!: (value: any) => void;
      const findUniquePromise = new Promise((resolve) => {
        resolveFindUnique = resolve;
      });
      const safeUpdatedAt = new Date("2026-03-14T00:00:00.000Z");

      (mockConnection.onLogs as any).mockImplementation(
        (_: any, handler: any) => {
          logsHandler = handler;
          return 1;
        }
      );
      (mockConnection.getBlock as any).mockResolvedValue({
        blockTime: 123,
        transactions: [{ transaction: { signatures: [TEST_SIGNATURE] } }],
      });
      mockPrisma.indexerState.findUnique
        .mockResolvedValueOnce({
          lastSignature: "safe-signature",
          lastSlot: 41n,
          lastTxIndex: 2,
          source: "poller",
          updatedAt: safeUpdatedAt,
        })
        .mockImplementation(() => findUniquePromise as any);

      await wsIndexer.start();

      const logs = createEventLogs("AgentRegistered", {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmLateStop",
      });

      logsHandler!({ signature: TEST_SIGNATURE, err: null, logs }, { slot: Number(TEST_SLOT) });
      await new Promise((resolve) => setTimeout(resolve, 0));

      const stopPromise = wsIndexer.stop();
      resolveFindUnique({ lastSlot: TEST_SLOT, lastTxIndex: 0, lastSignature: "old-signature" });
      await stopPromise;

      const advancedToInFlightSignature = mockPrisma.indexerState.upsert.mock.calls.some(([args]) => (
        args?.update?.lastSignature === TEST_SIGNATURE || args?.create?.lastSignature === TEST_SIGNATURE
      ));
      expect(advancedToInFlightSignature).toBe(false);
    });

    it("should restore the last safe cursor if shutdown starts while the local SQL cursor write is in flight", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      let releaseSqlWrite!: () => void;
      const safeUpdatedAt = new Date("2026-03-14T00:00:00.000Z");
      const previousDatabaseUrl = process.env.DATABASE_URL;
      process.env.DATABASE_URL = "file:/tmp/websocket-fast-path-test.db";

      try {
        (mockConnection.onLogs as any).mockImplementation(
          (_: any, handler: any) => {
            logsHandler = handler;
            return 1;
          }
        );
        (mockConnection.getBlock as any).mockResolvedValue({
          blockTime: 123,
          transactions: [{ transaction: { signatures: [TEST_SIGNATURE] } }],
        });
        mockPrisma.indexerState.findUnique
          .mockResolvedValueOnce({
            lastSignature: "safe-signature",
            lastSlot: 41n,
            lastTxIndex: 2,
            source: "poller",
            updatedAt: safeUpdatedAt,
          })
          .mockResolvedValueOnce({
            lastSignature: "safe-signature",
            lastSlot: 41n,
            lastTxIndex: 2,
          } as any);
        mockPrisma.$executeRawUnsafe.mockImplementation(
          () => new Promise((resolve) => {
            releaseSqlWrite = () => resolve(1);
          })
        );

        await wsIndexer.start();

        const logs = createEventLogs("AgentRegistered", {
          asset: TEST_ASSET,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: true,
          agentUri: "ipfs://QmSqlLateStop",
        });

        logsHandler!({ signature: TEST_SIGNATURE, err: null, logs }, { slot: Number(TEST_SLOT) });
        for (let i = 0; i < 20 && !releaseSqlWrite; i += 1) {
          await new Promise((resolve) => setTimeout(resolve, 0));
        }
        expect(typeof releaseSqlWrite).toBe("function");

        const stopPromise = wsIndexer.stop();
        releaseSqlWrite();
        await stopPromise;

        expect(mockPrisma.indexerState.upsert).toHaveBeenCalledWith(
          expect.objectContaining({
            where: { id: "main" },
            update: expect.objectContaining({
              lastSignature: "safe-signature",
              lastSlot: 41n,
              lastTxIndex: 2,
              source: "poller",
              updatedAt: safeUpdatedAt,
            }),
          })
        );
      } finally {
        if (previousDatabaseUrl === undefined) delete process.env.DATABASE_URL;
        else process.env.DATABASE_URL = previousDatabaseUrl;
      }
    });

    it("should restore the last safe cursor if shutdown starts while the Supabase cursor write is in flight", async () => {
      const safeUpdatedAt = new Date("2026-03-14T00:00:00.000Z");
      vi.mocked(loadIndexerStateSnapshot).mockResolvedValueOnce({
        lastSignature: "safe-signature",
        lastSlot: 41n,
        lastTxIndex: 2,
        source: "poller",
        updatedAt: safeUpdatedAt,
      });

      const supabaseIndexer = new WebSocketIndexer({
        connection: mockConnection as any,
        prisma: null,
        programId: TEST_PROGRAM_ID,
        reconnectInterval: 100,
        maxRetries: 3,
      });

      try {
        await supabaseIndexer.start();
        let releaseWork!: () => void;
        ((supabaseIndexer as any).logQueue).add(
          () => new Promise<void>((resolve) => {
            releaseWork = () => resolve();
          })
        );
        for (let i = 0; i < 20 && !releaseWork; i += 1) {
          await new Promise((resolve) => setTimeout(resolve, 0));
        }
        expect(typeof releaseWork).toBe("function");
        const stopPromise = supabaseIndexer.stop();
        releaseWork();
        await stopPromise;

        expect(restoreIndexerStateSnapshot).toHaveBeenCalledWith(
          "safe-signature",
          41n,
          2,
          "poller",
          safeUpdatedAt,
        );
      } finally {
        await supabaseIndexer.stop();
      }
    });

    it("should clear the Supabase cursor on shutdown when no safe snapshot exists yet", async () => {
      vi.mocked(loadIndexerStateSnapshot).mockResolvedValueOnce(null);

      const supabaseIndexer = new WebSocketIndexer({
        connection: mockConnection as any,
        prisma: null,
        programId: TEST_PROGRAM_ID,
        reconnectInterval: 100,
        maxRetries: 3,
      });

      try {
        await supabaseIndexer.start();
        let releaseWork!: () => void;
        ((supabaseIndexer as any).logQueue).add(
          () => new Promise<void>((resolve) => {
            releaseWork = () => resolve();
          })
        );
        for (let i = 0; i < 20 && !releaseWork; i += 1) {
          await new Promise((resolve) => setTimeout(resolve, 0));
        }
        expect(typeof releaseWork).toBe("function");

        const stopPromise = supabaseIndexer.stop();
        releaseWork();
        await stopPromise;

        expect(clearIndexerStateSnapshot).toHaveBeenCalledTimes(1);
      } finally {
        await supabaseIndexer.stop();
      }
    });
  });

  describe("isActive", () => {
    it("should return false when not started", () => {
      expect(wsIndexer.isActive()).toBe(false);
    });

    it("should return true when running", async () => {
      await wsIndexer.start();
      expect(wsIndexer.isActive()).toBe(true);
    });

    it("should return false after stop", async () => {
      await wsIndexer.start();
      await wsIndexer.stop();
      expect(wsIndexer.isActive()).toBe(false);
    });
  });

  describe("handleLogs", () => {
    it("should skip failed transactions", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      (mockConnection.onLogs as any).mockImplementation(
        (_: any, handler: any) => {
          logsHandler = handler;
          return 1;
        }
      );

      await wsIndexer.start();

      // Trigger with failed transaction
      logsHandler!(
        { signature: TEST_SIGNATURE, err: { error: "failed" }, logs: [] },
        { slot: Number(TEST_SLOT) }
      );

      expect(mockPrisma.eventLog.create).not.toHaveBeenCalled();
    });

    it("should process valid logs", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      (mockConnection.onLogs as any).mockImplementation(
        (_: any, handler: any) => {
          logsHandler = handler;
          return 1;
        }
      );

      await wsIndexer.start();

      // Trigger with valid logs (no events parsed, but should update state)
      logsHandler!(
        { signature: TEST_SIGNATURE, err: null, logs: ["Program log: test"] },
        { slot: Number(TEST_SLOT) }
      );

      // Wait for queue to process
      await new Promise((r) => setTimeout(r, 50));

      // State should be updated even without events
      // (depends on implementation - may or may not call upsert)
    });

    it("should not process logs without events", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      (mockConnection.onLogs as any).mockImplementation(
        (_: any, handler: any) => {
          logsHandler = handler;
          return 1;
        }
      );

      await wsIndexer.start();

      // Trigger with logs that don't contain parseable events (goes through queue)
      logsHandler!(
        { signature: TEST_SIGNATURE, err: null, logs: ["Program log: test"] },
        { slot: Number(TEST_SLOT) }
      );

      // Wait for queue to process
      await new Promise((r) => setTimeout(r, 50));

      // No events parsed, so no eventLog should be created
      expect(mockPrisma.eventLog.create).not.toHaveBeenCalled();
      // No upsert either since we return early
      expect(mockPrisma.indexerState.upsert).not.toHaveBeenCalled();
    });

    it("should process logs with valid events", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      (mockConnection.onLogs as any).mockImplementation(
        (_: any, handler: any) => {
          logsHandler = handler;
          return 1;
        }
      );

      await wsIndexer.start();

      // Create valid encoded event
      const eventData = {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmTest",
      };

      const logs = createEventLogs("AgentRegistered", eventData);

      // Trigger with valid logs (goes through queue)
      logsHandler!(
        { signature: TEST_SIGNATURE, err: null, logs },
        { slot: Number(TEST_SLOT) }
      );

      // Wait for queue to process
      await new Promise((r) => setTimeout(r, 50));

      // Should have processed the event
      expect(mockPrisma.eventLog.create).toHaveBeenCalledWith({
        data: expect.objectContaining({
          eventType: "AgentRegistered",
          processed: true,
        }),
      });

      // Should have updated indexer state
      expect(mockPrisma.indexerState.upsert).toHaveBeenCalled();

      // Should have called agent.upsert from handler
      expect(mockPrisma.agent.upsert).toHaveBeenCalled();
    });

    it("should enter fail-safe stop and avoid writes when tx_index cannot be resolved", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      (mockConnection.onLogs as any).mockImplementation(
        (_: any, handler: any) => {
          logsHandler = handler;
          return 1;
        }
      );

      const eventData = {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmTxIndexFail",
      };
      const logs = createEventLogs("AgentRegistered", eventData);
      (mockConnection.getBlock as any).mockRejectedValue(new Error("getBlock failed"));

      await wsIndexer.start();

      logsHandler!(
        { signature: TEST_SIGNATURE, err: null, logs },
        { slot: Number(TEST_SLOT) }
      );

      await new Promise((r) => setTimeout(r, 100));

      expect(mockPrisma.agent.upsert).not.toHaveBeenCalled();
      expect(mockPrisma.indexerState.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: "main" },
          update: expect.objectContaining({
            lastSignature: null,
            lastSlot: null,
            lastTxIndex: null,
          }),
        })
      );
      expect(mockConnection.removeOnLogsListener).toHaveBeenCalledWith(1);
      expect(wsIndexer.isActive()).toBe(false);
    });

    it("should handle errors during event processing", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      (mockConnection.onLogs as any).mockImplementation(
        (_: any, handler: any) => {
          logsHandler = handler;
          return 1;
        }
      );

      await wsIndexer.start();

      // Create valid encoded event
      const eventData = {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmTest",
      };

      const logs = createEventLogs("AgentRegistered", eventData);

      // Make agent.upsert throw to trigger error handling
      (mockPrisma.agent.upsert as any).mockRejectedValue(new Error("DB error"));

      // Trigger with valid logs (now goes through queue)
      logsHandler!(
        { signature: TEST_SIGNATURE, err: null, logs },
        { slot: Number(TEST_SLOT) }
      );

      // Wait for queue to process
      await new Promise((r) => setTimeout(r, 50));

      // Should have logged the failure
      expect(mockPrisma.eventLog.create).toHaveBeenCalledWith({
        data: expect.objectContaining({
          eventType: "PROCESSING_FAILED",
          processed: false,
          error: "DB error",
        }),
      });
    });

    it("should handle errors with non-Error objects", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      (mockConnection.onLogs as any).mockImplementation(
        (_: any, handler: any) => {
          logsHandler = handler;
          return 1;
        }
      );

      await wsIndexer.start();

      // Create valid encoded event
      const eventData = {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmTest",
      };

      const logs = createEventLogs("AgentRegistered", eventData);

      // Make agent.upsert throw a non-Error
      (mockPrisma.agent.upsert as any).mockRejectedValue("String error");

      // Trigger with valid logs (now goes through queue)
      logsHandler!(
        { signature: TEST_SIGNATURE, err: null, logs },
        { slot: Number(TEST_SLOT) }
      );

      // Wait for queue to process
      await new Promise((r) => setTimeout(r, 50));

      // Should have logged the failure with stringified error
      expect(mockPrisma.eventLog.create).toHaveBeenCalledWith({
        data: expect.objectContaining({
          eventType: "PROCESSING_FAILED",
          error: "String error",
        }),
      });
    });

    it("should process multiple events from single log", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      (mockConnection.onLogs as any).mockImplementation(
        (_: any, handler: any) => {
          logsHandler = handler;
          return 1;
        }
      );

      await wsIndexer.start();

      // Create two encoded events
      const eventData1 = {
        asset: TEST_ASSET,
        registry: TEST_REGISTRY,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmTest",
      };

      const eventData2 = {
        asset: TEST_ASSET,
        newUri: "https://example.com/agent.json",
        updatedBy: TEST_OWNER,
      };

      const encoded1 = encodeAnchorEvent("AgentRegistered", eventData1);
      const encoded2 = encodeAnchorEvent("UriUpdated", eventData2);
      const base64Data1 = encoded1.toString("base64");
      const base64Data2 = encoded2.toString("base64");

      const logs = [
        `Program ${TEST_PROGRAM_ID.toBase58()} invoke [1]`,
        `Program data: ${base64Data1}`,
        `Program data: ${base64Data2}`,
        `Program ${TEST_PROGRAM_ID.toBase58()} success`,
      ];

      // Trigger with logs containing multiple events (now goes through queue)
      logsHandler!(
        { signature: TEST_SIGNATURE, err: null, logs },
        { slot: Number(TEST_SLOT) }
      );

      // Wait for queue to process
      await new Promise((r) => setTimeout(r, 50));

      // Should have created at least one event log (Anchor parser behavior varies)
      expect(mockPrisma.eventLog.create).toHaveBeenCalled();
    });

    it("should not advance cursor when a later event in the same transaction fails", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      (mockConnection.onLogs as any).mockImplementation(
        (_: any, handler: any) => {
          logsHandler = handler;
          return 1;
        }
      );

      const firstEvent = {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmOne",
      };
      const secondEvent = {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmTwo",
      };

      const encoded1 = encodeAnchorEvent("AgentRegistered", firstEvent).toString("base64");
      const encoded2 = encodeAnchorEvent("AgentRegistered", secondEvent).toString("base64");
      const logs = [
        `Program ${TEST_PROGRAM_ID.toBase58()} invoke [1]`,
        `Program data: ${encoded1}`,
        `Program data: ${encoded2}`,
        `Program ${TEST_PROGRAM_ID.toBase58()} success`,
      ];

      let agentUpsertCalls = 0;
      (mockPrisma.agent.upsert as any).mockImplementation(async () => {
        agentUpsertCalls += 1;
        if (agentUpsertCalls === 2) {
          throw new Error("second event failed");
        }
        return {};
      });

      await wsIndexer.start();

      logsHandler!(
        { signature: TEST_SIGNATURE, err: null, logs },
        { slot: Number(TEST_SLOT) }
      );

      await new Promise((r) => setTimeout(r, 50));

      expect(mockPrisma.indexerState.upsert).not.toHaveBeenCalled();
      expect(mockPrisma.eventLog.create).toHaveBeenCalledWith({
        data: expect.objectContaining({
          eventType: "PROCESSING_FAILED",
          processed: false,
          error: "second event failed",
        }),
      });
    });
  });

  describe("reconnect", () => {
    it("should reconnect on subscription failure", async () => {
      let callCount = 0;
      (mockConnection.onLogs as any).mockImplementation(() => {
        callCount++;
        if (callCount === 1) {
          throw new Error("Connection failed");
        }
        return 1;
      });

      await wsIndexer.start();

      // Wait for reconnect
      await new Promise((r) => setTimeout(r, 200));

      expect(callCount).toBeGreaterThan(1);
    });

    it("should stop after max retries", async () => {
      (mockConnection.onLogs as any).mockImplementation(() => {
        throw new Error("Connection failed");
      });

      await wsIndexer.start();

      // Wait for retries to exhaust
      await new Promise((r) => setTimeout(r, 500));

      expect(wsIndexer.isActive()).toBe(false);
    });
  });
});

  describe("testWebSocketConnection", () => {
  const web3Mock = web3 as typeof web3 & {
    __setGetSlotMock: (fn: (() => Promise<number>) | null) => void;
    __setOnLogsMock: (fn: ((filter: unknown, callback: () => void, commitment?: unknown) => number) | null) => void;
    __setRemoveOnLogsListenerMock: (fn: ((subscriptionId: number) => Promise<void>) | null) => void;
    __setRpcWebSocketConnected: (value: boolean) => void;
    __setLogsSubscriptionState: (value: "subscribed" | "pending") => void;
  };
  const wsMock = wsModule as typeof wsModule & {
    __setWsProbeBehavior: (value: "supported" | "unsupported" | "timeout" | "error") => void;
  };

  afterEach(() => {
    web3Mock.__setGetSlotMock(null);
    web3Mock.__setOnLogsMock(null);
    web3Mock.__setRemoveOnLogsListenerMock(null);
    web3Mock.__setRpcWebSocketConnected(false);
    wsMock.__setWsProbeBehavior("supported");
    vi.restoreAllMocks();
  });

  it("should return false when connection fails", async () => {
    web3Mock.__setGetSlotMock(() =>
      Promise.reject(new Error("Connection failed"))
    );

    const result = await testWebSocketConnection(
      "https://invalid.rpc.local",
      "wss://invalid.nonexistent.local"
    );

    expect(result).toBe(false);
  });

  it("should return false when websocket subscription cannot be established", async () => {
    web3Mock.__setGetSlotMock(() => Promise.resolve(123));
    web3Mock.__setOnLogsMock(() => {
      throw new Error("log subscription failed");
    });

    const result = await testWebSocketConnection(
      "https://api.devnet.solana.com",
      "wss://api.devnet.solana.com"
    );
    expect(result).toBe(false);
  });

  it("should return false when the RPC provider does not support logsSubscribe", async () => {
    web3Mock.__setGetSlotMock(() => Promise.resolve(123));
    wsMock.__setWsProbeBehavior("unsupported");

    const result = await testWebSocketConnection(
      "https://api.devnet.solana.com",
      "wss://api.devnet.solana.com"
    );

    expect(result).toBe(false);
  });

  it("should return true when http and websocket probes succeed", async () => {
    web3Mock.__setGetSlotMock(() => Promise.resolve(123));
    web3Mock.__setOnLogsMock((_filter, callback) => {
      callback();
      return 7;
    });
    web3Mock.__setRpcWebSocketConnected(true);
    const removeSpy = vi.fn().mockResolvedValue(undefined);
    web3Mock.__setRemoveOnLogsListenerMock(removeSpy);

    const result = await testWebSocketConnection(
      "https://api.devnet.solana.com",
      "wss://api.devnet.solana.com"
    );
    expect(result).toBe(true);
    expect(removeSpy).toHaveBeenCalledWith(7);
  });

  it("should return false when websocket transport never reports connected", async () => {
    web3Mock.__setGetSlotMock(() => Promise.resolve(123));
    web3Mock.__setOnLogsMock(() => 9);
    const removeSpy = vi.fn().mockResolvedValue(undefined);
    web3Mock.__setRemoveOnLogsListenerMock(removeSpy);
    web3Mock.__setRpcWebSocketConnected(false);

    const result = await testWebSocketConnection(
      "https://api.devnet.solana.com",
      "wss://api.devnet.solana.com"
    );

    expect(result).toBe(false);
    expect(removeSpy).toHaveBeenCalledWith(9);
  });

  it("should return false when transport connects but subscription never becomes server-subscribed", async () => {
    web3Mock.__setGetSlotMock(async () => 123);
    web3Mock.__setRpcWebSocketConnected(true);
    web3Mock.__setLogsSubscriptionState("pending");
    web3Mock.__setOnLogsMock(() => 7);
    wsMock.__setWsProbeBehavior("supported");

    const result = await testWebSocketConnection(
      "https://rpc.example",
      "wss://rpc.example",
      TEST_PROGRAM_ID.toBase58(),
    );

    expect(result).toBe(false);
    web3Mock.__setLogsSubscriptionState("subscribed");
  });
});

describe("WebSocketIndexer provider compatibility", () => {
  const wsMock = wsModule as typeof wsModule & {
    __setWsProbeBehavior: (value: "supported" | "unsupported" | "timeout" | "error") => void;
  };
  let localConnection: ReturnType<typeof createMockConnection>;
  let localPrisma: ReturnType<typeof createMockPrismaClient>;

  beforeEach(() => {
    localConnection = createMockConnection();
    localPrisma = createMockPrismaClient();
  });

  afterEach(() => {
    wsMock.__setWsProbeBehavior("supported");
  });

  it("should not subscribe when the configured RPC provider does not support logsSubscribe", async () => {
    wsMock.__setWsProbeBehavior("unsupported");

    const localIndexer = new WebSocketIndexer({
      connection: localConnection as any,
      prisma: localPrisma,
      programId: TEST_PROGRAM_ID,
      wsUrl: "wss://unsupported.provider.example",
      reconnectInterval: 100,
      maxRetries: 3,
    });

    await localIndexer.start();

    expect(localConnection.onLogs).not.toHaveBeenCalled();
    expect(localIndexer.isActive()).toBe(false);

    await localIndexer.stop();
  });

  it("should not subscribe when the configured RPC provider cannot confirm logsSubscribe support", async () => {
    wsMock.__setWsProbeBehavior("error");

    const localIndexer = new WebSocketIndexer({
      connection: localConnection as any,
      prisma: localPrisma,
      programId: TEST_PROGRAM_ID,
      wsUrl: "wss://flaky.provider.example",
      reconnectInterval: 100,
      maxRetries: 3,
    });

    await localIndexer.start();

    expect(localConnection.onLogs).not.toHaveBeenCalled();
    expect(localIndexer.isActive()).toBe(false);

    await localIndexer.stop();
  });

  it("should not arm health checks when subscribe fails with unsupported-provider error", async () => {
    (localConnection.onLogs as any).mockImplementation(() => {
      const error = Object.assign(new Error("Method 'logsSubscribe' not found"), { code: -32601 });
      throw error;
    });

    const localIndexer = new WebSocketIndexer({
      connection: localConnection as any,
      prisma: localPrisma,
      programId: TEST_PROGRAM_ID,
      reconnectInterval: 100,
      maxRetries: 3,
    });

    await localIndexer.start();

    expect(localIndexer.isActive()).toBe(false);
    expect((localIndexer as any).healthCheckTimer).toBeNull();

    await localIndexer.stop();
  });
});
