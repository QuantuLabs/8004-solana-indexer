import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { config } from "../../../src/config.js";
import { matchProofPassFeedbacks } from "../../../src/extras/proofpass.js";
import { upsertProofPassMatches } from "../../../src/db/proofpass.js";

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
  getPool: vi.fn().mockReturnValue({
    query: vi.fn().mockResolvedValue({ rows: [] }),
  }),
  saveIndexerState: vi.fn().mockResolvedValue(undefined),
  loadIndexerStateSnapshot: vi.fn().mockResolvedValue(null),
  restoreIndexerStateSnapshot: vi.fn().mockResolvedValue(undefined),
  clearIndexerStateSnapshot: vi.fn().mockResolvedValue(undefined),
}));

vi.mock("../../../src/extras/proofpass.js", () => ({
  matchProofPassFeedbacks: vi.fn().mockReturnValue([]),
}));

vi.mock("../../../src/db/proofpass.js", () => ({
  upsertProofPassMatches: vi.fn().mockResolvedValue(undefined),
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
import { Poller } from "../../../src/indexer/poller.js";
import { createMockPrismaClient } from "../../mocks/prisma.js";
import {
  createMockConnection,
  createMockSignatureInfo,
  createMockParsedTransaction,
  createEventLogs,
  encodeAnchorEvent,
  TEST_SIGNATURE,
  TEST_SLOT,
  TEST_PROGRAM_ID,
  TEST_ASSET,
  TEST_OWNER,
  TEST_COLLECTION,
  TEST_CLIENT,
  TEST_REGISTRY,
} from "../../mocks/solana.js";

const createDeferred = <T = void>() => {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};

describe("WebSocketIndexer", () => {
  let wsIndexer: WebSocketIndexer;
  let mockConnection: ReturnType<typeof createMockConnection>;
  let mockPrisma: ReturnType<typeof createMockPrismaClient>;

  beforeEach(() => {
    mockConnection = createMockConnection();
    mockPrisma = createMockPrismaClient();
    (config as any).enableProofPass = false;
    vi.mocked(matchProofPassFeedbacks).mockReset();
    vi.mocked(matchProofPassFeedbacks).mockReturnValue([]);
    vi.mocked(upsertProofPassMatches).mockReset();
    vi.mocked(upsertProofPassMatches).mockResolvedValue(undefined);

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

    it("should resolve a legacy null snapshot tx index at startup", async () => {
      mockPrisma.indexerState.findUnique.mockResolvedValueOnce({
        lastSignature: "legacy-ws-cursor",
        lastSlot: 500n,
        lastTxIndex: null,
        source: "websocket",
        updatedAt: new Date("2026-03-14T00:00:00.000Z"),
      } as any);
      (mockConnection.getBlock as any).mockResolvedValueOnce({
        blockTime: 123,
        transactions: [
          { transaction: { signatures: ["older"] } },
          { transaction: { signatures: ["legacy-ws-cursor"] } },
        ],
      });

      await wsIndexer.start();

      expect(mockConnection.getBlock).toHaveBeenCalledWith(
        500,
        expect.objectContaining({ transactionDetails: "full" })
      );
      expect((wsIndexer as any).lastPersistedCursor.txIndex).toBe(1);
    });

    it("should treat a legacy null snapshot tx index as slot-start when resolution fails", async () => {
      mockPrisma.indexerState.findUnique.mockResolvedValueOnce({
        lastSignature: "legacy-ws-cursor",
        lastSlot: 500n,
        lastTxIndex: null,
        source: "websocket",
        updatedAt: new Date("2026-03-14T00:00:00.000Z"),
      } as any);
      (mockConnection.getBlock as any).mockRejectedValueOnce(new Error("block unavailable"));

      await wsIndexer.start();

      expect((wsIndexer as any).lastPersistedCursor.txIndex).toBe(-1);
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

    it("should return the same in-flight stop promise to concurrent callers", async () => {
      let releaseRemove!: () => void;
      (mockConnection.removeOnLogsListener as any).mockImplementationOnce(
        () => new Promise<void>((resolve) => {
          releaseRemove = resolve;
        })
      );

      await wsIndexer.start();

      const firstStop = wsIndexer.stop();
      const secondStop = wsIndexer.stop();

      expect(secondStop).toBe(firstStop);
      releaseRemove();
      await firstStop;
      expect(mockConnection.removeOnLogsListener).toHaveBeenCalledTimes(1);
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

    it("should not advance the cursor if advancement freezes immediately before save", async () => {
      let logsHandler: (logs: any, ctx: any) => void;

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
        .mockResolvedValueOnce(null)
        .mockResolvedValueOnce({
          lastSlot: 1n,
          lastTxIndex: 0,
          get lastSignature() {
            (wsIndexer as any).freezeCursorAdvancement = true;
            return "old-signature";
          },
        } as any);

      await wsIndexer.start();

      const logs = createEventLogs("AgentRegistered", {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmFreezeBeforeSave",
      });

      logsHandler!({ signature: TEST_SIGNATURE, err: null, logs }, { slot: Number(TEST_SLOT) });
      await ((wsIndexer as any).logQueue).onIdle();

      const advancedToFrozenSignature = mockPrisma.indexerState.upsert.mock.calls.some(([args]) => (
        args?.update?.lastSignature === TEST_SIGNATURE || args?.create?.lastSignature === TEST_SIGNATURE
      ));
      expect(advancedToFrozenSignature).toBe(false);
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

    it("should skip Supabase cursor restore after another source takes ownership", async () => {
      const safeUpdatedAt = new Date("2026-03-14T00:00:00.000Z");
      vi.mocked(loadIndexerStateSnapshot)
        .mockResolvedValueOnce({
          lastSignature: "safe-signature",
          lastSlot: 41n,
          lastTxIndex: 2,
          source: "websocket",
          updatedAt: safeUpdatedAt,
        })
        .mockResolvedValueOnce({
          lastSignature: "poller-signature",
          lastSlot: 42n,
          lastTxIndex: 0,
          source: "poller",
          updatedAt: new Date("2026-03-14T00:01:00.000Z"),
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
        const stopPromise = supabaseIndexer.stop();
        releaseWork();
        await stopPromise;

        expect(restoreIndexerStateSnapshot).not.toHaveBeenCalled();
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

    it("should skip Supabase cursor clear after another source takes ownership", async () => {
      vi.mocked(loadIndexerStateSnapshot)
        .mockResolvedValueOnce(null)
        .mockResolvedValueOnce({
          lastSignature: "poller-signature",
          lastSlot: 42n,
          lastTxIndex: 0,
          source: "poller",
          updatedAt: new Date("2026-03-14T00:01:00.000Z"),
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

        const stopPromise = supabaseIndexer.stop();
        releaseWork();
        await stopPromise;

        expect(clearIndexerStateSnapshot).not.toHaveBeenCalled();
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

  describe("holdProcessing/releaseProcessing", () => {
    it("should buffer notifications while held and flush through the canonical batch path on release", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      (mockConnection.onLogs as any).mockImplementation(
        (_: any, handler: any) => {
          logsHandler = handler;
          return 1;
        }
      );
      const batchSpy = vi
        .spyOn(wsIndexer as any, "processLogNotificationBatch")
        .mockResolvedValue(undefined);

      wsIndexer.holdProcessing();
      await wsIndexer.start();

      logsHandler!(
        { signature: TEST_SIGNATURE, err: null, logs: ["Program log: held"] },
        { slot: Number(TEST_SLOT) }
      );
      await new Promise((resolve) => setTimeout(resolve, 20));

      expect(batchSpy).not.toHaveBeenCalled();
      expect(wsIndexer.getStats().queueSize).toBe(1);

      wsIndexer.releaseProcessing();
      await new Promise((resolve) => setTimeout(resolve, 20));

      expect(batchSpy).toHaveBeenCalledTimes(1);
      expect(batchSpy.mock.calls[0][0][0]).toEqual(
        expect.objectContaining({
          logs: expect.objectContaining({ signature: TEST_SIGNATURE }),
          ctx: { slot: Number(TEST_SLOT) },
        })
      );
    });
  });

  describe("reconciliation", () => {
    const makeRecoveryTransaction = (signature: string) => createMockParsedTransaction(
      signature,
      createEventLogs("AgentRegistered", {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: `ipfs://${signature}`,
      })
    );

    const mockPollerReplay = (signatures: Array<ReturnType<typeof createMockSignatureInfo>>) => {
      const bySlot = new Map<number, string[]>();
      for (const signature of signatures.filter((sig) => sig.err === null)) {
        const slotSignatures = bySlot.get(signature.slot) ?? [];
        slotSignatures.push(signature.signature);
        bySlot.set(signature.slot, slotSignatures);
      }

      (mockConnection.getBlock as any).mockImplementation(async (slot: number) => ({
        blockTime: 123,
        transactions: (bySlot.get(slot) ?? []).map((signature) => ({
          transaction: { signatures: [signature] },
        })),
      }));
      (mockConnection.getParsedTransaction as any).mockImplementation(async (signature: string) => (
        makeRecoveryTransaction(signature)
      ));
    };

    it("should replay a persisted-tip miss through the poller after a restart", async () => {
      const sessionStartCursor = {
        lastSignature: "session-start",
        lastSlot: 100n,
        lastTxIndex: 0,
        source: "websocket",
        updatedAt: new Date("2026-03-14T00:00:00.000Z"),
      };
      const advancedCursor = {
        lastSignature: "advanced-cursor",
        lastSlot: 200n,
        lastTxIndex: 0,
        source: "poller",
        updatedAt: new Date("2026-03-14T00:01:00.000Z"),
      };
      let mainCursor = sessionStartCursor;
      let durableTip: string | null = null;

      mockPrisma.indexerState.findUnique.mockImplementation(async (args: any) => (
        args?.where?.id === "main" ? mainCursor as any : null
      ));
      mockPrisma.indexerState.findMany.mockImplementation(async (args: any) => (
        args?.where?.id === "websocket-reconciliation-tip" && durableTip
          ? [{ lastSignature: durableTip }]
          : []
      ));
      mockPrisma.indexerState.upsert.mockImplementation(async (args: any) => {
        if (args?.where?.id === "websocket-reconciliation-tip") {
          durableTip = args.update?.lastSignature ?? args.create?.lastSignature ?? null;
        }
        return args.update ?? args.create;
      });

      await wsIndexer.start();
      expect(durableTip).toBe("session-start");
      await wsIndexer.stop();

      mainCursor = advancedCursor;
      wsIndexer = new WebSocketIndexer({
        connection: mockConnection as any,
        prisma: mockPrisma,
        programId: TEST_PROGRAM_ID,
        reconnectInterval: 100,
        maxRetries: 3,
      });
      mockConnection.removeOnLogsListener.mockClear();
      const replaySignatures = [
        createMockSignatureInfo("missed-behind-advanced-cursor", 150, null),
      ];
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce(replaySignatures);
      mockPollerReplay(replaySignatures);

      await wsIndexer.start();
      const triggered = await (wsIndexer as any).runReconciliationCheck("periodic");

      expect(triggered).toBe(false);
      expect(mockConnection.getSignaturesForAddress).toHaveBeenCalledWith(
        TEST_PROGRAM_ID,
        { until: "session-start", limit: 50 }
      );
      expect(mockConnection.getParsedTransaction).toHaveBeenCalledWith(
        "missed-behind-advanced-cursor",
        expect.objectContaining({ maxSupportedTransactionVersion: config.maxSupportedTransactionVersion })
      );
      expect(mockConnection.getParsedTransaction).toHaveBeenCalledTimes(1);
      expect(mockConnection.getBlock).toHaveBeenCalledWith(
        150,
        expect.objectContaining({ transactionDetails: "full" })
      );
      expect(mockPrisma.agent.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({
            createdTxSignature: "missed-behind-advanced-cursor",
            txIndex: 0,
          }),
        })
      );
      expect(mainCursor).toEqual(advancedCursor);
      expect(durableTip).toBe("missed-behind-advanced-cursor");
      expect(mockConnection.removeOnLogsListener).not.toHaveBeenCalled();
      expect(wsIndexer.isActive()).toBe(true);
    });

    it("should replay a scanned window in canonical order without rewinding or stopping", async () => {
      const sessionStartCursor = {
        lastSignature: "session-start",
        lastSlot: 100n,
        lastTxIndex: 0,
        source: "websocket",
        updatedAt: new Date("2026-03-14T00:00:00.000Z"),
      };
      const advancedCursor = {
        lastSignature: "advanced-cursor",
        lastSlot: 200n,
        lastTxIndex: 0,
        source: "websocket",
        updatedAt: new Date("2026-03-14T00:01:00.000Z"),
      };
      let currentCursor = sessionStartCursor;
      let durableTip: string | null = null;

      mockPrisma.indexerState.findUnique.mockImplementation(async (args: any) => (
        args?.where?.id === "main" ? currentCursor as any : null
      ));
      mockPrisma.indexerState.findMany.mockImplementation(async (args: any) => (
        args?.where?.id === "websocket-reconciliation-tip" && durableTip
          ? [{ lastSignature: durableTip }]
          : []
      ));
      mockPrisma.indexerState.upsert.mockImplementation(async (args: any) => {
        if (args?.where?.id === "websocket-reconciliation-tip") {
          durableTip = args.update?.lastSignature ?? args.create?.lastSignature ?? null;
        }
        if (args?.where?.id === "main") {
          const update = args.update ?? args.create;
          currentCursor = {
            lastSignature: update.lastSignature,
            lastSlot: update.lastSlot,
            lastTxIndex: update.lastTxIndex,
            source: update.source,
            updatedAt: update.updatedAt,
          };
        }
        return args.update ?? args.create;
      });
      const replaySignatures = [
        createMockSignatureInfo("already-indexed", 150, null),
        createMockSignatureInfo("missed-behind-advanced-cursor", 150, null),
      ];
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce(replaySignatures);
      mockPollerReplay([
        createMockSignatureInfo("missed-behind-advanced-cursor", 150, null),
        createMockSignatureInfo("already-indexed", 150, null),
      ]);

      await wsIndexer.start();
      currentCursor = advancedCursor;
      (wsIndexer as any).lastPersistedCursor = {
        signature: advancedCursor.lastSignature,
        slot: advancedCursor.lastSlot,
        txIndex: advancedCursor.lastTxIndex,
        source: advancedCursor.source,
        updatedAt: advancedCursor.updatedAt,
      };

      expect(((wsIndexer as any).logQueue).size).toBe(0);
      expect(((wsIndexer as any).logQueue).pending).toBe(0);

      const triggered = await (wsIndexer as any).runReconciliationCheck("periodic");

      expect(triggered).toBe(false);
      expect(currentCursor).toEqual(advancedCursor);
      expect(mockConnection.getParsedTransaction).toHaveBeenCalledWith(
        "missed-behind-advanced-cursor",
        expect.any(Object)
      );
      expect(mockConnection.getParsedTransaction).toHaveBeenCalledWith(
        "already-indexed",
        expect.any(Object)
      );
      expect(mockConnection.getParsedTransaction).toHaveBeenCalledTimes(2);
      expect(mockConnection.getBlock).toHaveBeenCalledWith(
        150,
        expect.objectContaining({ transactionDetails: "full" })
      );
      const upsertedSignatures = mockPrisma.agent.upsert.mock.calls.map(([args]) => (
        args?.create?.createdTxSignature
      ));
      expect(upsertedSignatures).toEqual([
        "missed-behind-advanced-cursor",
        "already-indexed",
      ]);
      expect(mockPrisma.agent.upsert.mock.calls.map(([args]) => args?.create?.txIndex)).toEqual([0, 1]);
      expect(mockPrisma.indexerState.upsert).not.toHaveBeenCalledWith(
        expect.objectContaining({ where: { id: "main" } })
      );
      expect(durableTip).toBe("already-indexed");
      expect(mockConnection.removeOnLogsListener).not.toHaveBeenCalled();
      expect(wsIndexer.isActive()).toBe(true);
    });

    it("serializes accepted replay A before a later websocket notification B", async () => {
      mockPrisma.indexerState.findUnique.mockResolvedValue({
        lastSignature: "cursor-sig",
        lastSlot: 100n,
        lastTxIndex: 0,
        source: "websocket",
        updatedAt: new Date("2026-09-08T00:00:00.000Z"),
      } as any);
      await wsIndexer.start();
      (wsIndexer as any).canonicalBufferMs = 0;

      const replayStarted = createDeferred();
      const releaseReplay = createDeferred();
      const order: string[] = [];
      let activeWriters = 0;
      let maxActiveWriters = 0;
      vi.spyOn(wsIndexer as any, "performReconciliationCheck").mockImplementation(async () => {
        activeWriters += 1;
        maxActiveWriters = Math.max(maxActiveWriters, activeWriters);
        order.push("start:A");
        replayStarted.resolve();
        await releaseReplay.promise;
        order.push("commit:A");
        activeWriters -= 1;
        return false;
      });
      vi.spyOn(wsIndexer as any, "processLogNotificationBatch").mockImplementation(async () => {
        activeWriters += 1;
        maxActiveWriters = Math.max(maxActiveWriters, activeWriters);
        order.push("commit:B");
        activeWriters -= 1;
      });

      const reconciliation = (wsIndexer as any).runReconciliationCheck("periodic");
      await replayStarted.promise;
      (wsIndexer as any).enqueueLogNotification(
        { signature: "newer-B", err: null, logs: ["Program log: B"] },
        { slot: 102 }
      );

      expect(order).toEqual(["start:A"]);
      releaseReplay.resolve();
      await reconciliation;
      await (wsIndexer as any).logQueue.onIdle();

      expect(order).toEqual(["start:A", "commit:A", "commit:B"]);
      expect(maxActiveWriters).toBe(1);
    });

    it("admits no reconciliation write while bootstrap processing is held", async () => {
      mockPrisma.indexerState.findUnique.mockResolvedValue({
        lastSignature: "cursor-sig",
        lastSlot: 100n,
        lastTxIndex: 0,
        source: "websocket",
        updatedAt: new Date("2026-09-08T00:00:00.000Z"),
      } as any);
      wsIndexer.holdProcessing();
      await wsIndexer.start();
      const replay = vi
        .spyOn(wsIndexer as any, "performReconciliationCheck")
        .mockResolvedValue(false);

      expect(await (wsIndexer as any).runReconciliationCheck("periodic")).toBe(false);
      expect(replay).not.toHaveBeenCalled();

      wsIndexer.releaseProcessing();
      expect(await (wsIndexer as any).runReconciliationCheck("periodic")).toBe(false);
      expect(replay).toHaveBeenCalledTimes(1);
    });

    it("should drain an accepted reconciliation write before stop returns and start no later write", async () => {
      const cursorRow = {
        lastSignature: "cursor-sig",
        lastSlot: 100n,
        lastTxIndex: 0,
        source: "websocket",
        updatedAt: new Date("2026-09-08T00:00:00.000Z"),
      };
      let durableTip: string | null = "cursor-sig";
      mockPrisma.indexerState.findUnique.mockResolvedValue(cursorRow as any);
      mockPrisma.indexerState.findMany.mockImplementation(async (args: any) => (
        args?.where?.id === "websocket-reconciliation-tip" && durableTip
          ? [{ lastSignature: durableTip }]
          : []
      ));
      mockPrisma.indexerState.upsert.mockImplementation(async (args: any) => {
        if (args?.where?.id === "websocket-reconciliation-tip") {
          durableTip = args.update?.lastSignature ?? args.create?.lastSignature ?? null;
        }
        return args.update ?? args.create;
      });
      const replaySignatures = [
        createMockSignatureInfo("later-replay", 102, null),
        createMockSignatureInfo("accepted-replay", 101, null),
      ];
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce(replaySignatures);
      mockPollerReplay(replaySignatures);

      let releaseAccepted!: () => void;
      let acceptedStarted!: () => void;
      const acceptedStartedPromise = new Promise<void>((resolve) => {
        acceptedStarted = resolve;
      });
      const acceptedGate = new Promise<void>((resolve) => {
        releaseAccepted = resolve;
      });
      const writeOrder: string[] = [];
      mockPrisma.agent.upsert.mockImplementation(async (args: any) => {
        const signature = args.create.createdTxSignature;
        writeOrder.push(`start:${signature}`);
        if (signature === "accepted-replay") {
          acceptedStarted();
          await acceptedGate;
        }
        writeOrder.push(`finish:${signature}`);
        return args.create;
      });

      await wsIndexer.start();
      const reconciliationPromise = (wsIndexer as any).runReconciliationCheck("periodic");
      await acceptedStartedPromise;

      let stopSettled = false;
      const stopPromise = wsIndexer.stop().then(() => {
        stopSettled = true;
        writeOrder.push("start:main-poller");
      });
      await Promise.resolve();

      expect(stopSettled).toBe(false);
      expect(writeOrder).toEqual(["start:accepted-replay"]);

      releaseAccepted();
      await stopPromise;
      await expect(reconciliationPromise).resolves.toBe(false);

      expect(writeOrder).toEqual([
        "start:accepted-replay",
        "finish:accepted-replay",
        "start:main-poller",
      ]);
      expect((wsIndexer as any).processedCount).toBe(1);
      expect(durableTip).toBe("cursor-sig");
    });

    it("should persist the newest scanned signature after a complete clean reconciliation scan", async () => {
      const cursorRow = {
        lastSignature: "cursor-sig",
        lastSlot: 100n,
        lastTxIndex: 0,
        source: "websocket",
        updatedAt: new Date("2026-03-14T00:00:00.000Z"),
      };
      let durableTip: string | null = "cursor-sig";

      mockPrisma.indexerState.findUnique.mockResolvedValue(cursorRow as any);
      mockPrisma.indexerState.findMany.mockImplementation(async (args: any) => (
        args?.where?.id === "websocket-reconciliation-tip" && durableTip
          ? [{ lastSignature: durableTip }]
          : []
      ));
      mockPrisma.indexerState.upsert.mockImplementation(async (args: any) => {
        if (args?.where?.id === "websocket-reconciliation-tip") {
          durableTip = args.update?.lastSignature ?? args.create?.lastSignature ?? null;
        }
        return args.update ?? args.create;
      });
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([
        createMockSignatureInfo("observed-newest", 101, null),
      ]);
      mockPollerReplay([
        createMockSignatureInfo("observed-newest", 101, null),
      ]);

      await wsIndexer.start();

      const triggered = await (wsIndexer as any).runReconciliationCheck("periodic");

      expect(triggered).toBe(false);
      expect(mockConnection.getParsedTransaction).toHaveBeenCalledWith("observed-newest", expect.any(Object));
      expect(durableTip).toBe("observed-newest");
      expect(mockPrisma.indexerState.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: "websocket-reconciliation-tip" },
          update: expect.objectContaining({
            lastSignature: "observed-newest",
          }),
        })
      );
      expect(mockPrisma.indexerState.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: "main" },
          update: expect.objectContaining({
            lastSignature: "observed-newest",
            lastSlot: 101n,
            lastTxIndex: 0,
            source: "websocket-reconciliation",
          }),
        })
      );
      expect(wsIndexer.isActive()).toBe(true);
    });

    it("should fail-safe without advancing the reconciliation tip when the scan limit fills before the anchor", async () => {
      const cursorRow = {
        lastSignature: "cursor-sig",
        lastSlot: 100n,
        lastTxIndex: 0,
        source: "websocket",
        updatedAt: new Date("2026-03-14T00:00:00.000Z"),
      };
      let durableTip: string | null = "anchor-sig";
      const pages = Array.from({ length: 4 }, (_, pageIndex) =>
        Array.from({ length: 50 }, (_, index) => {
          const ordinal = pageIndex * 50 + index;
          return createMockSignatureInfo(`observed-${ordinal}`, 300 - ordinal, null);
        })
      );

      mockPrisma.indexerState.findUnique.mockResolvedValue(cursorRow as any);
      mockPrisma.indexerState.findMany.mockImplementation(async (args: any) => (
        args?.where?.id === "websocket-reconciliation-tip" && durableTip
          ? [{ lastSignature: durableTip }]
          : []
      ));
      mockPrisma.indexerState.upsert.mockImplementation(async (args: any) => {
        if (args?.where?.id === "websocket-reconciliation-tip") {
          durableTip = args.update?.lastSignature ?? args.create?.lastSignature ?? null;
        }
        return args.update ?? args.create;
      });
      for (const page of pages) {
        (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce(page);
      }

      await wsIndexer.start();

      const triggered = await (wsIndexer as any).runReconciliationCheck("periodic");
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(triggered).toBe(true);
      expect(durableTip).toBe("anchor-sig");
      expect(mockConnection.getSignaturesForAddress).toHaveBeenCalledTimes(4);
      expect(mockConnection.getSignaturesForAddress).toHaveBeenNthCalledWith(
        4,
        TEST_PROGRAM_ID,
        { until: "anchor-sig", limit: 50, before: "observed-149" }
      );
      expect(wsIndexer.isActive()).toBe(false);
    });

    it("recovers a simulated DB omission beyond 200 through fail-safe polling and restarts healthy", async () => {
      const previousPageLimit = config.historicalScanSignaturePageLimit;
      const previousMaxPages = config.historicalScanMaxPagesPerPass;
      (config as any).historicalScanSignaturePageLimit = 50;
      (config as any).historicalScanMaxPagesPerPass = 10;

      const history = [
        ...Array.from({ length: 201 }, (_, index) =>
          createMockSignatureInfo(`sig-${201 - index}`, 301 - index, null)
        ),
        createMockSignatureInfo("durable-anchor", 100, null),
      ];
      const state = new Map<string, any>([
        ["main", {
          id: "main",
          lastSignature: "sig-201",
          lastSlot: 301n,
          lastTxIndex: 0,
          source: "websocket",
          updatedAt: new Date("2026-09-08T00:00:00.000Z"),
        }],
        ["websocket-reconciliation-tip", {
          id: "websocket-reconciliation-tip",
          lastSignature: "durable-anchor",
          lastSlot: null,
          lastTxIndex: null,
          source: "websocket-reconciliation",
          updatedAt: new Date("2026-09-08T00:00:00.000Z"),
        }],
      ]);
      mockPrisma.indexerState.findUnique.mockImplementation(async (args: any) => (
        state.get(args.where.id) ?? null
      ));
      mockPrisma.indexerState.findMany.mockImplementation(async (args: any) => {
        const exactId = typeof args?.where?.id === "string" ? args.where.id : null;
        if (exactId) return state.has(exactId) ? [state.get(exactId)] : [];
        const prefix = args?.where?.id?.startsWith;
        if (!prefix) return [];
        return [...state.values()]
          .filter((row) => row.id.startsWith(prefix))
          .sort((a, b) => (a.lastTxIndex ?? 0) - (b.lastTxIndex ?? 0));
      });
      mockPrisma.indexerState.upsert.mockImplementation(async (args: any) => {
        const existing = state.get(args.where.id);
        const next = { ...(existing ?? args.create), ...(existing ? args.update : {}) };
        state.set(args.where.id, next);
        return next;
      });
      mockPrisma.indexerState.deleteMany.mockImplementation(async (args: any) => {
        const id = args?.where?.id;
        const prefix = id?.startsWith;
        for (const key of [...state.keys()]) {
          if (key === id || (prefix && key.startsWith(prefix))) state.delete(key);
        }
        return { count: 1 };
      });
      (mockConnection.getSignaturesForAddress as any).mockImplementation(
        async (_programId: unknown, options: { before?: string; until?: string; limit: number }) => {
          const start = options.before
            ? history.findIndex((entry) => entry.signature === options.before) + 1
            : 0;
          const untilIndex = options.until
            ? history.findIndex((entry) => entry.signature === options.until)
            : history.length;
          return history.slice(start, Math.min(start + options.limit, untilIndex));
        }
      );
      (mockConnection.getBlock as any).mockImplementation(async (slot: number) => {
        const signature = history.find((entry) => entry.slot === slot)?.signature;
        return {
          transactions: signature ? [{ transaction: { signatures: [signature] } }] : [],
        };
      });

      try {
        await wsIndexer.start();
        expect(await (wsIndexer as any).runReconciliationCheck("periodic")).toBe(true);
        await wsIndexer.stop();

        expect(state.get("websocket-reconciliation-tip")?.lastSignature).toBe("durable-anchor");
        expect(state.get("main")?.lastSignature).toBe("sig-201");

        const simulatedRows = new Set<string>();
        const recoveryPoller = new Poller({
          connection: mockConnection as any,
          prisma: mockPrisma,
          programId: TEST_PROGRAM_ID,
          pollingInterval: 100,
          batchSize: 50,
        });
        (recoveryPoller as any).useBatchRpc = false;
        (recoveryPoller as any).batchFetcher = null;
        vi.spyOn(recoveryPoller as any, "processTransaction").mockImplementation(async (sig: any) => {
          simulatedRows.add(sig.signature);
          return new Date("2026-09-08T00:00:00.000Z");
        });

        await recoveryPoller.bootstrap();
        await recoveryPoller.stop();

        expect(simulatedRows.has("sig-1")).toBe(true);
        expect(state.get("main")?.lastSignature).toBe("sig-201");
        expect(state.get("websocket-reconciliation-tip")?.lastSignature).toBe("sig-201");

        wsIndexer = new WebSocketIndexer({
          connection: mockConnection as any,
          prisma: mockPrisma,
          programId: TEST_PROGRAM_ID,
          reconnectInterval: 100,
          maxRetries: 3,
        });
        await wsIndexer.start();
        expect(await (wsIndexer as any).runReconciliationCheck("periodic")).toBe(false);
        expect(wsIndexer.isActive()).toBe(true);
        expect(state.get("websocket-reconciliation-tip")?.lastSignature).toBe("sig-201");
      } finally {
        (config as any).historicalScanSignaturePageLimit = previousPageLimit;
        (config as any).historicalScanMaxPagesPerPass = previousMaxPages;
      }
    });

    it("should replay a successful signature not present in indexed rows", async () => {
      const cursorRow = {
        lastSignature: "cursor-sig",
        lastSlot: 100n,
        lastTxIndex: 0,
        source: "websocket",
        updatedAt: new Date("2026-03-14T00:00:00.000Z"),
      };
      mockPrisma.indexerState.findUnique
        .mockResolvedValueOnce(cursorRow as any)
        .mockResolvedValueOnce(cursorRow as any);
      const replaySignatures = [
        createMockSignatureInfo("missed-sig", 101, null),
        createMockSignatureInfo("failed-sig", 102, { InstructionError: [0, "Custom"] }),
      ];
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce(replaySignatures);
      mockPollerReplay(replaySignatures);

      await wsIndexer.start();

      const triggered = await (wsIndexer as any).runReconciliationCheck("periodic");

      expect(triggered).toBe(false);
      expect(mockConnection.getSignaturesForAddress).toHaveBeenCalledWith(
        TEST_PROGRAM_ID,
        { until: "cursor-sig", limit: 50 }
      );
      expect(mockConnection.getParsedTransaction).toHaveBeenCalledWith("missed-sig", expect.any(Object));
      expect(mockConnection.getParsedTransaction).toHaveBeenCalledTimes(1);
      expect(mockConnection.getBlock).toHaveBeenCalledWith(
        101,
        expect.objectContaining({ transactionDetails: "full" })
      );
      expect(mockPrisma.agent.upsert).toHaveBeenCalled();
      expect(wsIndexer.isActive()).toBe(true);
    });

    it("should paginate reconciliation checks up to a missed successful signature", async () => {
      const cursorRow = {
        lastSignature: "cursor-sig",
        lastSlot: 100n,
        lastTxIndex: 0,
        source: "websocket",
        updatedAt: new Date("2026-03-14T00:00:00.000Z"),
      };
      const firstPage = Array.from({ length: 50 }, (_, index) =>
        createMockSignatureInfo(`observed-${index}`, 150 - index, null)
      );
      mockPrisma.indexerState.findUnique
        .mockResolvedValueOnce(cursorRow as any)
        .mockResolvedValueOnce(cursorRow as any);
      const secondPage = [
        createMockSignatureInfo("missed-second-page", 101, null),
      ];
      (mockConnection.getSignaturesForAddress as any)
        .mockResolvedValueOnce(firstPage)
        .mockResolvedValueOnce(secondPage);
      mockPollerReplay([...firstPage, ...secondPage]);

      await wsIndexer.start();

      const triggered = await (wsIndexer as any).runReconciliationCheck("periodic");

      expect(triggered).toBe(false);
      expect(mockConnection.getSignaturesForAddress).toHaveBeenNthCalledWith(
        1,
        TEST_PROGRAM_ID,
        { until: "cursor-sig", limit: 50 }
      );
      expect(mockConnection.getSignaturesForAddress).toHaveBeenNthCalledWith(
        2,
        TEST_PROGRAM_ID,
        { until: "cursor-sig", limit: 50, before: "observed-49" }
      );
      expect(mockConnection.getParsedTransaction).toHaveBeenCalledWith("missed-second-page", expect.any(Object));
      expect(mockConnection.getParsedTransaction).toHaveBeenCalledTimes(51);
      expect(wsIndexer.isActive()).toBe(true);
    });

    it("should keep scanning from the previous reconciliation tip when the websocket cursor advances", async () => {
      const initialCursor = {
        lastSignature: "initial-cursor",
        lastSlot: 100n,
        lastTxIndex: 0,
        source: "websocket",
        updatedAt: new Date("2026-03-14T00:00:00.000Z"),
      };
      const advancedCursor = {
        lastSignature: "advanced-cursor",
        lastSlot: 200n,
        lastTxIndex: 0,
        source: "websocket",
        updatedAt: new Date("2026-03-14T00:01:00.000Z"),
      };
      mockPrisma.indexerState.findUnique
        .mockResolvedValueOnce(initialCursor as any)
        .mockResolvedValueOnce(initialCursor as any)
        .mockResolvedValueOnce(advancedCursor as any);
      (mockConnection.getSignaturesForAddress as any)
        .mockResolvedValueOnce([])
        .mockResolvedValueOnce([
          createMockSignatureInfo("missed-behind-ws-cursor", 150, null),
        ]);
      mockPollerReplay([
        createMockSignatureInfo("missed-behind-ws-cursor", 150, null),
      ]);

      await wsIndexer.start();

      await (wsIndexer as any).runReconciliationCheck("periodic");
      (wsIndexer as any).lastPersistedCursor = {
        signature: "advanced-cursor",
        slot: 200n,
        txIndex: 0,
        source: "websocket",
        updatedAt: advancedCursor.updatedAt,
      };
      const triggered = await (wsIndexer as any).runReconciliationCheck("periodic");

      expect(mockConnection.getSignaturesForAddress).toHaveBeenNthCalledWith(
        2,
        TEST_PROGRAM_ID,
        { until: "initial-cursor", limit: 50 }
      );
      expect(triggered).toBe(false);
      expect(mockConnection.getParsedTransaction).toHaveBeenCalledWith("missed-behind-ws-cursor", expect.any(Object));
      expect(wsIndexer.isActive()).toBe(true);
    });

    it("should replay scanned signatures without an indexed-row diff", async () => {
      const startCursor = {
        lastSignature: "cursor-sig",
        lastSlot: 100n,
        lastTxIndex: 0,
        source: "websocket",
        updatedAt: new Date("2026-03-14T00:00:00.000Z"),
      };
      const durableCursor = {
        lastSignature: "poller-cursor",
        lastSlot: 120n,
        lastTxIndex: 2,
        source: "poller",
        updatedAt: new Date("2026-03-14T00:01:00.000Z"),
      };
      mockPrisma.indexerState.findUnique
        .mockResolvedValueOnce(startCursor as any)
        .mockResolvedValueOnce(durableCursor as any);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([
        createMockSignatureInfo("poller-covered-sig", 120, null),
      ]);
      mockPollerReplay([
        createMockSignatureInfo("poller-covered-sig", 120, null),
      ]);

      await wsIndexer.start();

      const triggered = await (wsIndexer as any).runReconciliationCheck("periodic");

      expect(triggered).toBe(false);
      expect(mockConnection.getParsedTransaction).toHaveBeenCalledWith("poller-covered-sig", expect.any(Object));
      expect(mockConnection.getBlock).toHaveBeenCalledWith(
        120,
        expect.objectContaining({ transactionDetails: "full" })
      );
      expect(wsIndexer.isActive()).toBe(true);
    });

    it("should spend one scan and no transaction fetches on an idle reconciliation interval", async () => {
      const cursorRow = {
        lastSignature: "cursor-sig",
        lastSlot: 100n,
        lastTxIndex: 0,
        source: "websocket",
        updatedAt: new Date("2026-03-14T00:00:00.000Z"),
      };
      mockPrisma.indexerState.findUnique
        .mockResolvedValueOnce(cursorRow as any)
        .mockResolvedValueOnce(cursorRow as any);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([]);

      await wsIndexer.start();

      const triggered = await (wsIndexer as any).runReconciliationCheck("periodic");

      expect(triggered).toBe(false);
      expect(mockConnection.getSignaturesForAddress).toHaveBeenCalledTimes(1);
      expect(mockConnection.getSignaturesForAddress).toHaveBeenCalledWith(
        TEST_PROGRAM_ID,
        { until: "cursor-sig", limit: 50 }
      );
      expect(mockConnection.getBlock).not.toHaveBeenCalled();
      expect(mockConnection.getParsedTransaction).not.toHaveBeenCalled();
      expect(wsIndexer.isActive()).toBe(true);
    });

    it("should enter fail-safe stop after three reconciliation RPC failures", async () => {
      const cursorRow = {
        lastSignature: "cursor-sig",
        lastSlot: 100n,
        lastTxIndex: 0,
        source: "websocket",
        updatedAt: new Date("2026-03-14T00:00:00.000Z"),
      };
      mockPrisma.indexerState.findUnique.mockResolvedValueOnce(cursorRow as any);
      (mockConnection.getSignaturesForAddress as any).mockRejectedValue(new Error("rpc down"));

      await wsIndexer.start();

      expect(await (wsIndexer as any).runReconciliationCheck("periodic")).toBe(false);
      expect(await (wsIndexer as any).runReconciliationCheck("periodic")).toBe(false);
      expect(await (wsIndexer as any).runReconciliationCheck("periodic")).toBe(true);
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(mockConnection.getSignaturesForAddress).toHaveBeenCalledTimes(3);
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

    it("should process buffered same-slot notifications in tx_index order", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      (mockConnection.onLogs as any).mockImplementation(
        (_: any, handler: any) => {
          logsHandler = handler;
          return 1;
        }
      );
      wsIndexer = new WebSocketIndexer({
        connection: mockConnection as any,
        prisma: mockPrisma,
        programId: TEST_PROGRAM_ID,
        reconnectInterval: 100,
        maxRetries: 3,
        canonicalBufferMs: 10,
      });

      const firstSignature = TEST_SIGNATURE;
      const laterSignature = "7wHu1qwD7q2ggbJqCPtxnHZ2TrLQfEV9B7NqcBYBqzXh9J6vQQYc4Kdb8ZnZJwZqNjKt1QZcJZGJ";
      const firstLogs = createEventLogs("AgentRegistered", {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmFirst",
      });
      const laterLogs = createEventLogs("AgentRegistered", {
        asset: TEST_CLIENT,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmLater",
      });

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue(null);
      (mockConnection.getBlock as any).mockResolvedValue({
        blockTime: 123,
        transactions: [
          { transaction: { signatures: [firstSignature] } },
          { transaction: { signatures: [laterSignature] } },
        ],
      });

      await wsIndexer.start();

      logsHandler!({ signature: laterSignature, err: null, logs: laterLogs }, { slot: Number(TEST_SLOT) });
      logsHandler!({ signature: firstSignature, err: null, logs: firstLogs }, { slot: Number(TEST_SLOT) });

      await new Promise((r) => setTimeout(r, 30));
      await ((wsIndexer as any).logQueue).onIdle();

      const upsertedAssets = mockPrisma.agent.upsert.mock.calls.map(([args]) => args?.create?.id);
      expect(upsertedAssets.slice(0, 2)).toEqual([
        TEST_ASSET.toBase58(),
        TEST_CLIENT.toBase58(),
      ]);
    });

    it("should process an older notification idempotently without rewinding the cursor", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      (mockConnection.onLogs as any).mockImplementation(
        (_: any, handler: any) => {
          logsHandler = handler;
          return 1;
        }
      );

      const laterSignature = "7wHu1qwD7q2ggbJqCPtxnHZ2TrLQfEV9B7NqcBYBqzXh9J6vQQYc4Kdb8ZnZJwZqNjKt1QZcJZGJ";
      const logs = createEventLogs("AgentRegistered", {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmStale",
      });

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue({
        lastSignature: laterSignature,
        lastSlot: TEST_SLOT,
        lastTxIndex: 1,
        source: "websocket",
        updatedAt: new Date("2026-03-14T00:01:00.000Z"),
      });
      (mockConnection.getBlock as any).mockResolvedValue({
        blockTime: 123,
        transactions: [
          { transaction: { signatures: [TEST_SIGNATURE] } },
          { transaction: { signatures: [laterSignature] } },
        ],
      });

      await wsIndexer.start();

      logsHandler!({ signature: TEST_SIGNATURE, err: null, logs }, { slot: Number(TEST_SLOT) });

      await new Promise((r) => setTimeout(r, 100));
      await ((wsIndexer as any).logQueue).onIdle().catch(() => undefined);

      expect(mockPrisma.agent.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({
            createdTxSignature: TEST_SIGNATURE,
            txIndex: 0,
          }),
        })
      );
      expect(mockPrisma.indexerState.upsert).not.toHaveBeenCalledWith(
        expect.objectContaining({ where: { id: "main" } })
      );
      expect(mockConnection.removeOnLogsListener).not.toHaveBeenCalled();
      expect(wsIndexer.isActive()).toBe(true);
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

    it("should fail-stop and prevent later queued logs from advancing after a transaction failure", async () => {
      let logsHandler: (logs: any, ctx: any) => void;
      (mockConnection.onLogs as any).mockImplementation(
        (_: any, handler: any) => {
          logsHandler = handler;
          return 1;
        }
      );

      const firstSignature = TEST_SIGNATURE;
      const laterSignature = "7wHu1qwD7q2ggbJqCPtxnHZ2TrLQfEV9B7NqcBYBqzXh9J6vQQYc4Kdb8ZnZJwZqNjKt1QZcJZGJ";
      const logs = createEventLogs("AgentRegistered", {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmFailStop",
      });

      (mockConnection.getBlock as any)
        .mockResolvedValueOnce({
          blockTime: 123,
          transactions: [{ transaction: { signatures: [firstSignature] } }],
        })
        .mockResolvedValueOnce({
          blockTime: 124,
          transactions: [{ transaction: { signatures: [laterSignature] } }],
        });
      (mockPrisma.agent.upsert as any).mockRejectedValueOnce(new Error("DB error"));

      await wsIndexer.start();

      logsHandler!({ signature: firstSignature, err: null, logs }, { slot: Number(TEST_SLOT) });
      logsHandler!({ signature: laterSignature, err: null, logs }, { slot: Number(TEST_SLOT) + 1 });

      await new Promise((r) => setTimeout(r, 150));
      await ((wsIndexer as any).logQueue).onIdle().catch(() => undefined);

      const advancedToLaterSignature = mockPrisma.indexerState.upsert.mock.calls.some(([args]) => (
        args?.update?.lastSignature === laterSignature || args?.create?.lastSignature === laterSignature
      ));
      expect(advancedToLaterSignature).toBe(false);
      expect(mockConnection.removeOnLogsListener).toHaveBeenCalledWith(1);
      expect(wsIndexer.isActive()).toBe(false);
    });

    it("should not advance cursor when ProofPass upsert fails after event processing", async () => {
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
        agentUri: "ipfs://QmProofPass",
      };
      const logs = createEventLogs("AgentRegistered", eventData);

      (config as any).enableProofPass = true;
      vi.mocked(matchProofPassFeedbacks).mockReturnValue([
        {
          id: "proofpass-match",
          asset: TEST_ASSET.toBase58(),
          clientAddress: TEST_OWNER.toBase58(),
          feedbackIndex: "1",
          txSignature: TEST_SIGNATURE,
          blockSlot: TEST_SLOT.toString(),
          feedbackHash: "aa".repeat(32),
          proofpassSession: "bb".repeat(32),
          contextType: 1,
          contextRefHash: "cc".repeat(32),
        },
      ] as any);
      vi.mocked(upsertProofPassMatches).mockRejectedValueOnce(new Error("proofpass upsert failed"));

      await wsIndexer.start();

      logsHandler!(
        { signature: TEST_SIGNATURE, err: null, logs },
        { slot: Number(TEST_SLOT) }
      );

      await new Promise((r) => setTimeout(r, 50));

      expect(upsertProofPassMatches).toHaveBeenCalledTimes(1);
      const advancedToProofPassSignature = mockPrisma.indexerState.upsert.mock.calls.some(([args]) => (
        args?.update?.lastSignature === TEST_SIGNATURE || args?.create?.lastSignature === TEST_SIGNATURE
      ));
      expect(advancedToProofPassSignature).toBe(false);
      expect(wsIndexer.isActive()).toBe(false);
      expect(mockPrisma.eventLog.create).toHaveBeenCalledWith({
        data: expect.objectContaining({
          eventType: "PROCESSING_FAILED",
          processed: false,
          error: "proofpass upsert failed",
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

      const advancedToFailedSignature = mockPrisma.indexerState.upsert.mock.calls.some(([args]) => (
        args?.update?.lastSignature === TEST_SIGNATURE || args?.create?.lastSignature === TEST_SIGNATURE
      ));
      expect(advancedToFailedSignature).toBe(false);
      expect(wsIndexer.isActive()).toBe(false);
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
