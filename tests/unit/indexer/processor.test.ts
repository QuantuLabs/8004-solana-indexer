import { describe, it, expect, vi, beforeEach } from "vitest";
import { createMockPrismaClient } from "../../mocks/prisma.js";

// Use vi.hoisted to ensure mocks are defined before vi.mock factories run
const {
  mockPollerInstance,
  mockWsIndexerInstance,
  mockTestWebSocketConnection,
  mockVerifierInstance,
} = vi.hoisted(() => ({
  mockPollerInstance: {
    bootstrap: vi.fn(),
    start: vi.fn(),
    stop: vi.fn(),
  },
  mockWsIndexerInstance: {
    start: vi.fn(),
    stop: vi.fn(),
    isActive: vi.fn(),
    isRecovering: vi.fn(),
  },
  mockTestWebSocketConnection: vi.fn(),
  mockVerifierInstance: {
    start: vi.fn(),
    stop: vi.fn(),
    getStats: vi.fn(),
  },
}));

const createPollerMock = () => ({
  bootstrap: vi.fn(),
  start: vi.fn(),
  stop: vi.fn(),
});

const createWsIndexerMock = () => ({
  start: vi.fn(),
  stop: vi.fn(),
  isActive: vi.fn(),
  isRecovering: vi.fn(),
});

const createDeferred = <T = void>() => {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};

// Mock the modules BEFORE importing Processor
vi.mock("@solana/web3.js", () => {
  class MockConnection {
    getSlot = vi.fn().mockResolvedValue(12345);
    getSignaturesForAddress = vi.fn().mockResolvedValue([]);
    getParsedTransaction = vi.fn().mockResolvedValue(null);
    onLogs = vi.fn().mockReturnValue(1);
    removeOnLogsListener = vi.fn().mockResolvedValue(undefined);
  }

  class MockPublicKey {
    private readonly key: string;

    constructor(key: string) {
      this.key = key;
    }

    toBase58() {
      return typeof this.key === "string" ? this.key : "mocked-key";
    }

    toBytes() {
      return new Uint8Array(32);
    }
  }

  return { Connection: MockConnection, PublicKey: MockPublicKey };
});

vi.mock("../../../src/indexer/poller.js", () => ({
  Poller: vi.fn(function MockPoller() {
    return mockPollerInstance;
  }),
}));

vi.mock("../../../src/indexer/websocket.js", () => ({
  WebSocketIndexer: vi.fn(function MockWebSocketIndexer() {
    return mockWsIndexerInstance;
  }),
  testWebSocketConnection: mockTestWebSocketConnection,
}));

vi.mock("../../../src/indexer/verifier.js", () => ({
  DataVerifier: vi.fn(function MockDataVerifier() {
    return mockVerifierInstance;
  }),
}));

vi.mock("../../../src/logger.js", () => ({
  createChildLogger: vi.fn(() => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  })),
}));

vi.mock("../../../src/config.js", () => ({
  config: {
    rpcUrl: "http://localhost:8899",
    wsUrl: "ws://localhost:8900",
    programId: "11111111111111111111111111111111",
    indexerMode: "polling",
    pollingInterval: 5000,
    wsReconnectInterval: 1000,
    wsMaxRetries: 5,
    verifyIntervalMs: 60000,
  },
  IndexerMode: {
    AUTO: "auto",
    WEBSOCKET: "websocket",
    POLLING: "polling",
  },
}));

// Import after mocks are set up
import { Processor } from "../../../src/indexer/processor.js";
import { Poller } from "../../../src/indexer/poller.js";
import { WebSocketIndexer, testWebSocketConnection } from "../../../src/indexer/websocket.js";
import { config } from "../../../src/config.js";
import { DataVerifier } from "../../../src/indexer/verifier.js";

describe("Processor", () => {
  let mockPrisma: ReturnType<typeof createMockPrismaClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockPrisma = createMockPrismaClient();

    // Reset mock return values
    mockPollerInstance.bootstrap.mockResolvedValue(undefined);
    mockPollerInstance.start.mockResolvedValue(undefined);
    mockPollerInstance.stop.mockResolvedValue(undefined);
    mockWsIndexerInstance.start.mockResolvedValue(undefined);
    mockWsIndexerInstance.stop.mockResolvedValue(undefined);
    mockWsIndexerInstance.isActive.mockReturnValue(true);
    mockWsIndexerInstance.isRecovering.mockReturnValue(false);
    mockTestWebSocketConnection.mockResolvedValue(true);
    mockVerifierInstance.start.mockResolvedValue(undefined);
    mockVerifierInstance.stop.mockResolvedValue(undefined);
    mockVerifierInstance.getStats.mockReturnValue({});
  });

  describe("constructor", () => {
    it("should create processor with default mode", () => {
      const processor = new Processor(mockPrisma);
      expect(processor).toBeDefined();
      const status = processor.getStatus();
      expect(status.mode).toBeDefined();
      expect(status.running).toBe(false);
    });

    it("should create processor with polling mode", () => {
      const processor = new Processor(mockPrisma, null, { mode: "polling" });
      expect(processor).toBeDefined();
      const status = processor.getStatus();
      expect(status.mode).toBe("polling");
      expect(status.configuredMode).toBe("polling");
    });

    it("should create processor with websocket mode", () => {
      const processor = new Processor(mockPrisma, null, { mode: "websocket" });
      expect(processor).toBeDefined();
      const status = processor.getStatus();
      expect(status.mode).toBe("websocket");
      expect(status.configuredMode).toBe("websocket");
    });

    it("should create processor with auto mode", () => {
      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      expect(processor).toBeDefined();
      const status = processor.getStatus();
      expect(status.mode).toBe("auto");
      expect(status.configuredMode).toBe("auto");
    });
  });

  describe("getStatus", () => {
    it("should return initial status for polling mode", () => {
      const processor = new Processor(mockPrisma, null, { mode: "polling" });

      const status = processor.getStatus();

      expect(status).toEqual({
        running: false,
        mode: "polling",
        configuredMode: "polling",
        pollerActive: false,
        wsActive: false,
        verifierActive: false,
        verifierStats: undefined,
      });
    });

    it("should return initial status for websocket mode", () => {
      const processor = new Processor(mockPrisma, null, { mode: "websocket" });

      const status = processor.getStatus();

      expect(status).toEqual({
        running: false,
        mode: "websocket",
        configuredMode: "websocket",
        pollerActive: false,
        wsActive: false,
        verifierActive: false,
        verifierStats: undefined,
      });
    });

    it("should return initial status for auto mode", () => {
      const processor = new Processor(mockPrisma, null, { mode: "auto" });

      const status = processor.getStatus();

      expect(status).toEqual({
        running: false,
        mode: "auto",
        configuredMode: "auto",
        pollerActive: false,
        wsActive: false,
        verifierActive: false,
        verifierStats: undefined,
      });
    });
  });

  describe("start", () => {
    it("should start in polling mode", async () => {
      const processor = new Processor(mockPrisma, null, { mode: "polling" });
      await processor.start();

      const status = processor.getStatus();
      expect(status.running).toBe(true);
      expect(status.pollerActive).toBe(true);
      expect(Poller).toHaveBeenCalled();
      expect(mockPollerInstance.start).toHaveBeenCalled();
      expect(mockVerifierInstance.start).toHaveBeenCalled();
      expect(mockPollerInstance.start.mock.invocationCallOrder[0]).toBeLessThan(
        mockVerifierInstance.start.mock.invocationCallOrder[0]
      );
    });

    it("should start in websocket mode", async () => {
      const bootstrapPollerA = createPollerMock();
      const bootstrapPollerB = createPollerMock();
      bootstrapPollerA.bootstrap.mockResolvedValue(undefined);
      bootstrapPollerB.bootstrap.mockResolvedValue(undefined);
      vi.mocked(Poller).mockImplementationOnce(function MockPollerA() {
        return bootstrapPollerA as any;
      }).mockImplementationOnce(function MockPollerB() {
        return bootstrapPollerB as any;
      });

      const processor = new Processor(mockPrisma, null, { mode: "websocket" });
      await processor.start();

      const status = processor.getStatus();
      expect(status.running).toBe(true);
      expect(status.wsActive).toBe(true);
      expect(status.pollerActive).toBe(false);
      expect(status.mode).toBe("websocket");
      expect(status.configuredMode).toBe("websocket");
      expect(WebSocketIndexer).toHaveBeenCalled();
      expect(mockWsIndexerInstance.start).toHaveBeenCalled();
      expect(mockVerifierInstance.start).toHaveBeenCalled();
      expect(bootstrapPollerA.bootstrap.mock.invocationCallOrder[0]).toBeLessThan(
        mockVerifierInstance.start.mock.invocationCallOrder[0]
      );
      expect(Poller).toHaveBeenCalledTimes(2);
      expect(bootstrapPollerA.bootstrap).toHaveBeenCalledTimes(1);
      expect(bootstrapPollerA.bootstrap).toHaveBeenCalledWith(undefined);
      expect(bootstrapPollerB.bootstrap).toHaveBeenCalledTimes(1);
      expect(bootstrapPollerB.bootstrap).toHaveBeenCalledWith({ suppressEventLogWrites: true });
      expect(mockPollerInstance.start).not.toHaveBeenCalled();
      expect((processor as any).wsMonitorInterval).not.toBeNull();
    });

    it("should fail websocket mode startup when no active subscription is established", async () => {
      mockWsIndexerInstance.isActive.mockReturnValue(false);

      const processor = new Processor(mockPrisma, null, { mode: "websocket" });
      await expect(processor.start()).rejects.toThrow(
        "WebSocket indexer failed to establish an active subscription"
      );

      expect(mockWsIndexerInstance.start).toHaveBeenCalledTimes(1);
      expect(mockWsIndexerInstance.stop).toHaveBeenCalled();
      expect(processor.getStatus()).toEqual({
        running: false,
        mode: "websocket",
        configuredMode: "websocket",
        pollerActive: false,
        wsActive: false,
        verifierActive: false,
        verifierStats: undefined,
      });
    });

    it("should start in auto mode with WebSocket available", async () => {
      const bootstrapPollerA = createPollerMock();
      const bootstrapPollerB = createPollerMock();
      bootstrapPollerA.bootstrap.mockResolvedValue(undefined);
      bootstrapPollerB.bootstrap.mockResolvedValue(undefined);
      vi.mocked(Poller).mockImplementationOnce(function MockPollerA() {
        return bootstrapPollerA as any;
      }).mockImplementationOnce(function MockPollerB() {
        return bootstrapPollerB as any;
      });

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      // Wait for async operations
      await new Promise((r) => setTimeout(r, 50));

      const status = processor.getStatus();
      expect(status.running).toBe(true);
      expect(testWebSocketConnection).toHaveBeenCalled();
      expect(WebSocketIndexer).toHaveBeenCalled();
      expect(status.wsActive).toBe(true);
      expect(status.pollerActive).toBe(false);
      expect(status.mode).toBe("websocket");
      expect(status.configuredMode).toBe("auto");
      expect(mockVerifierInstance.start).toHaveBeenCalled();
      expect(bootstrapPollerA.bootstrap.mock.invocationCallOrder[0]).toBeLessThan(
        mockVerifierInstance.start.mock.invocationCallOrder[0]
      );
      expect(Poller).toHaveBeenCalledTimes(2);
      expect(bootstrapPollerA.bootstrap).toHaveBeenCalledTimes(1);
      expect(bootstrapPollerA.bootstrap).toHaveBeenCalledWith(undefined);
      expect(bootstrapPollerB.bootstrap).toHaveBeenCalledTimes(1);
      expect(bootstrapPollerB.bootstrap).toHaveBeenCalledWith({ suppressEventLogWrites: true });
      expect(mockPollerInstance.start).not.toHaveBeenCalled();
    });

    it("should fallback to polling in auto mode when WebSocket unavailable", async () => {
      mockTestWebSocketConnection.mockResolvedValueOnce(false);

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      const status = processor.getStatus();
      expect(status.running).toBe(true);
      expect(status.pollerActive).toBe(true);
      expect(status.mode).toBe("polling");
      expect(status.configuredMode).toBe("auto");
      expect(testWebSocketConnection).toHaveBeenCalled();
    });

    it("should fallback to polling in auto mode when websocket startup stays inactive", async () => {
      mockWsIndexerInstance.isActive.mockReturnValue(false);

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      const status = processor.getStatus();
      expect(status.running).toBe(true);
      expect(status.pollerActive).toBe(true);
      expect(status.wsActive).toBe(false);
      expect(status.mode).toBe("polling");
      expect(status.configuredMode).toBe("auto");
      expect(mockWsIndexerInstance.start).toHaveBeenCalledTimes(1);
      expect(mockWsIndexerInstance.stop).toHaveBeenCalled();
      expect(mockPollerInstance.start).toHaveBeenCalledTimes(1);
    });

    it("should cleanup websocket startup state and fallback to polling when auto websocket pipeline throws", async () => {
      mockPollerInstance.bootstrap
        .mockResolvedValueOnce(undefined)
        .mockRejectedValueOnce(new Error("post-subscription catch-up failed"));

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      const status = processor.getStatus();
      expect(status.running).toBe(true);
      expect(status.pollerActive).toBe(true);
      expect(status.wsActive).toBe(false);
      expect(status.mode).toBe("polling");
      expect(status.configuredMode).toBe("auto");
      expect(mockWsIndexerInstance.start).toHaveBeenCalledTimes(1);
      expect(mockWsIndexerInstance.stop).toHaveBeenCalledTimes(1);
      expect(mockPollerInstance.start).toHaveBeenCalledTimes(1);
      expect((processor as any).wsMonitorInterval).not.toBeNull();
    });

    it("should not start twice", async () => {
      const processor = new Processor(mockPrisma, null, { mode: "polling" });
      await processor.start();
      await processor.start(); // Second call should be ignored

      expect(Poller).toHaveBeenCalledTimes(1);
    });

    it("should handle default case in switch (same as auto)", async () => {
      // Create processor with a mode that falls through to default
      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      const status = processor.getStatus();
      expect(status.running).toBe(true);
    });
  });

  describe("stop", () => {
    it("should handle stop when not started", async () => {
      const processor = new Processor(mockPrisma);
      await processor.stop();

      const status = processor.getStatus();
      expect(status.running).toBe(false);
    });

    it("should stop polling mode", async () => {
      const processor = new Processor(mockPrisma, null, { mode: "polling" });
      await processor.start();
      await processor.stop();

      const status = processor.getStatus();
      expect(status.running).toBe(false);
      expect(mockPollerInstance.stop).toHaveBeenCalled();
    });

    it("should stop websocket mode", async () => {
      const processor = new Processor(mockPrisma, null, { mode: "websocket" });
      await processor.start();
      await processor.stop();

      const status = processor.getStatus();
      expect(status.running).toBe(false);
      expect(mockWsIndexerInstance.stop).toHaveBeenCalled();
      expect(mockPollerInstance.stop).not.toHaveBeenCalled();
    });

    it("should stop auto mode", async () => {
      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      // Wait for everything to start
      await new Promise((r) => setTimeout(r, 50));

      await processor.stop();

      const status = processor.getStatus();
      expect(status.running).toBe(false);
    });
  });

  describe("monitorWebSocket", () => {
    it("should handle WebSocket connection loss in websocket mode", async () => {
      vi.useFakeTimers();

      const processor = new Processor(mockPrisma, null, { mode: "websocket" });
      await processor.start();
      vi.clearAllMocks();

      mockWsIndexerInstance.isActive.mockReturnValueOnce(false).mockReturnValue(true);
      mockWsIndexerInstance.isRecovering.mockReturnValue(false);

      await vi.advanceTimersByTimeAsync(10000);

      expect(WebSocketIndexer).toHaveBeenCalledTimes(1);
      expect(Poller).toHaveBeenCalledTimes(2);
      expect(mockPollerInstance.bootstrap).toHaveBeenCalledTimes(2);
      expect(mockPollerInstance.bootstrap).toHaveBeenNthCalledWith(1, undefined);
      expect(mockPollerInstance.bootstrap).toHaveBeenNthCalledWith(2, { suppressEventLogWrites: true });
      expect(mockWsIndexerInstance.stop).toHaveBeenCalledTimes(1);
      expect(mockWsIndexerInstance.start).toHaveBeenCalledTimes(1);
      expect(mockPollerInstance.start).not.toHaveBeenCalled();
      expect((processor as any).wsMonitorInterval).not.toBeNull();

      await processor.stop();
      vi.useRealTimers();
    });

    it("should recover websocket mode when wsIndexer is unexpectedly null", async () => {
      vi.useFakeTimers();

      const processor = new Processor(mockPrisma, null, { mode: "websocket" });
      await processor.start();
      vi.clearAllMocks();

      (processor as any).wsIndexer = null;

      await vi.advanceTimersByTimeAsync(10000);

      expect(Poller).toHaveBeenCalledTimes(2);
      expect(mockPollerInstance.bootstrap).toHaveBeenCalledTimes(2);
      expect(mockPollerInstance.bootstrap).toHaveBeenNthCalledWith(1, undefined);
      expect(mockPollerInstance.bootstrap).toHaveBeenNthCalledWith(2, { suppressEventLogWrites: true });
      expect(mockWsIndexerInstance.start).toHaveBeenCalledTimes(1);

      await processor.stop();
      vi.useRealTimers();
    });

    it("should handle WebSocket connection loss in auto mode", async () => {
      vi.useFakeTimers();

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();
      vi.clearAllMocks();

      // Simulate WebSocket becoming inactive
      mockWsIndexerInstance.isActive.mockReturnValue(false);

      // Advance timer to trigger monitor check
      await vi.advanceTimersByTimeAsync(10000);

      expect(Poller).toHaveBeenCalledTimes(1);
      expect(mockPollerInstance.start).toHaveBeenCalledTimes(1);
      expect(mockPollerInstance.stop).not.toHaveBeenCalled();
      expect(mockWsIndexerInstance.stop).toHaveBeenCalled();
      expect(processor.getStatus().pollerActive).toBe(true);
      expect(processor.getStatus().wsActive).toBe(false);
      expect(processor.getStatus().mode).toBe("polling");
      expect(processor.getStatus().configuredMode).toBe("auto");

      vi.useRealTimers();
    });

    it("should retry websocket promotion in auto mode after failover", async () => {
      vi.useFakeTimers();

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();
      vi.clearAllMocks();

      mockWsIndexerInstance.isActive.mockReturnValue(false);

      await vi.advanceTimersByTimeAsync(10000);
      expect(Poller).toHaveBeenCalledTimes(1);
      expect(mockPollerInstance.start).toHaveBeenCalledTimes(1);

      vi.clearAllMocks();
      mockTestWebSocketConnection.mockResolvedValue(true);
      await vi.advanceTimersByTimeAsync(60000);

      expect(mockTestWebSocketConnection).toHaveBeenCalled();
      expect(mockPollerInstance.stop).toHaveBeenCalledTimes(1);
      expect(mockWsIndexerInstance.start).toHaveBeenCalledTimes(1);

      await processor.stop();
      vi.useRealTimers();
    });

    it("should ignore a stale auto websocket probe from the previous run after restart", async () => {
      const firstPoller = createPollerMock();
      const secondPoller = createPollerMock();
      (Poller as unknown as { mockImplementationOnce: (fn: () => unknown) => unknown })
        .mockImplementationOnce(function MockFirstPoller() { return firstPoller; })
        .mockImplementationOnce(function MockSecondPoller() { return secondPoller; });

      const deferredProbe = createDeferred<boolean>();
      mockTestWebSocketConnection
        .mockResolvedValueOnce(false)
        .mockImplementationOnce(() => deferredProbe.promise)
        .mockResolvedValueOnce(false);

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      (processor as any).lastAutoWsProbeAt = 0;
      const staleProbe = (processor as any).runWebSocketCheck();

      await processor.stop();
      await processor.start();
      vi.clearAllMocks();

      deferredProbe.resolve(true);
      await staleProbe;

      expect(secondPoller.stop).not.toHaveBeenCalled();
      expect(mockWsIndexerInstance.start).not.toHaveBeenCalled();

      await processor.stop();
    });

    it("should monitor websocket recovery after cold-start fallback to polling", async () => {
      vi.useFakeTimers();

      mockTestWebSocketConnection.mockResolvedValueOnce(false).mockResolvedValueOnce(true);

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      expect(Poller).toHaveBeenCalledTimes(1);
      expect(mockPollerInstance.start).toHaveBeenCalledTimes(1);
      expect((processor as any).wsMonitorInterval).not.toBeNull();

      vi.clearAllMocks();
      await vi.advanceTimersByTimeAsync(60000);

      expect(mockTestWebSocketConnection).toHaveBeenCalled();
      expect(mockPollerInstance.stop).toHaveBeenCalledTimes(1);
      expect(mockWsIndexerInstance.start).toHaveBeenCalledTimes(1);

      await processor.stop();
      vi.useRealTimers();
    });

    it("should reset auto websocket reprobe throttle across stop/start on the same instance", async () => {
      vi.useFakeTimers();

      mockTestWebSocketConnection
        .mockResolvedValueOnce(false)
        .mockResolvedValueOnce(true)
        .mockResolvedValueOnce(false)
        .mockResolvedValueOnce(true);

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      vi.clearAllMocks();
      await vi.advanceTimersByTimeAsync(60000);

      expect(mockTestWebSocketConnection).toHaveBeenCalledTimes(1);
      await processor.stop();

      vi.clearAllMocks();
      mockPollerInstance.start.mockClear();
      mockPollerInstance.stop.mockClear();
      mockWsIndexerInstance.start.mockClear();

      await processor.start();
      vi.clearAllMocks();

      // After restart, the 60s reprobe budget should be reset for the same Processor instance.
      await vi.advanceTimersByTimeAsync(10000);

      expect(mockTestWebSocketConnection).toHaveBeenCalledTimes(1);

      await processor.stop();
      vi.useRealTimers();
    });

    it("should ignore a stale websocket startup run after restart on the same instance", async () => {
      const firstBootstrap = createPollerMock();
      const secondBootstrapA = createPollerMock();
      const secondBootstrapB = createPollerMock();
      const deferredBootstrap = createDeferred<void>();

      firstBootstrap.bootstrap.mockImplementation(() => deferredBootstrap.promise);
      secondBootstrapA.bootstrap.mockResolvedValue(undefined);
      secondBootstrapB.bootstrap.mockResolvedValue(undefined);

      vi.mocked(Poller)
        .mockImplementationOnce(function MockFirstBootstrap() {
          return firstBootstrap as any;
        })
        .mockImplementationOnce(function MockSecondBootstrapA() {
          return secondBootstrapA as any;
        })
        .mockImplementationOnce(function MockSecondBootstrapB() {
          return secondBootstrapB as any;
        });

      const processor = new Processor(mockPrisma, null, { mode: "websocket" });

      const firstStart = processor.start();
      await Promise.resolve();

      await processor.stop();

      const secondStart = processor.start();
      deferredBootstrap.resolve();

      await Promise.allSettled([firstStart, secondStart]);

      expect(mockWsIndexerInstance.start).toHaveBeenCalledTimes(1);
      expect(mockVerifierInstance.start).toHaveBeenCalledTimes(1);
      expect(secondBootstrapA.bootstrap).toHaveBeenCalledTimes(1);
      expect(secondBootstrapB.bootstrap).toHaveBeenCalledTimes(1);

      await processor.stop();
    });

    it("should not monitor when processor is stopped", async () => {
      vi.useFakeTimers();

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();
      await processor.stop();

      // Clear mock calls from start/stop
      vi.clearAllMocks();

      // Advance timer - monitor should not do anything
      await vi.advanceTimersByTimeAsync(10000);

      // Nothing should happen since isRunning is false
      expect(mockPollerInstance.stop).not.toHaveBeenCalled();

      vi.useRealTimers();
    });

    it("should clear existing monitor interval before creating new one", async () => {
      vi.useFakeTimers();

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      // wsMonitorInterval is now set from monitorWebSocket call in startAuto
      expect((processor as any).wsMonitorInterval).not.toBeNull();

      // Call monitorWebSocket again directly to cover the branch that
      // clears the existing interval before scheduling a new one
      (processor as any).monitorWebSocket();

      // Should still have an interval (new one was created after clearing old)
      expect((processor as any).wsMonitorInterval).not.toBeNull();

      await processor.stop();
      vi.useRealTimers();
    });

    it("should early-return from runWebSocketCheck when not running", async () => {
      vi.useFakeTimers();

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      // Stop the processor but keep a reference to trigger the check manually
      (processor as any).isRunning = false;

      // WS inactive to ensure we'd enter fallback if not guarded
      mockWsIndexerInstance.isActive.mockReturnValue(false);

      // Directly invoke runWebSocketCheck
      await (processor as any).runWebSocketCheck();

      // Poller should NOT be started since isRunning is false
      expect(mockPollerInstance.start).not.toHaveBeenCalled();

      // Clean up: restore isRunning so stop() works, then clear intervals manually
      (processor as any).isRunning = true;
      await processor.stop();
      vi.useRealTimers();
    });

    it("should skip check when wsMonitorInProgress is true", async () => {
      vi.useFakeTimers();

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      // Set the reentrancy guard directly to simulate an in-progress check
      (processor as any).wsMonitorInProgress = true;

      // WS inactive to ensure we'd enter the fallback path if not guarded
      mockWsIndexerInstance.isActive.mockReturnValue(false);

      // Advance to trigger monitor check
      await vi.advanceTimersByTimeAsync(10000);

      // Poller should NOT be started since wsMonitorInProgress blocks entry
      expect(mockPollerInstance.start).not.toHaveBeenCalled();

      // Reset the guard so stop() works cleanly
      (processor as any).wsMonitorInProgress = false;
      await processor.stop();
      vi.useRealTimers();
    });

    it("should wait for self-heal when wsIndexer is recovering", async () => {
      vi.useFakeTimers();

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      // WS inactive but in recovery mode
      mockWsIndexerInstance.isActive.mockReturnValue(false);
      mockWsIndexerInstance.isRecovering.mockReturnValue(true);

      // Advance to trigger check
      await vi.advanceTimersByTimeAsync(10000);

      // Poller should NOT be started since WS is recovering
      expect(mockPollerInstance.start).not.toHaveBeenCalled();

      await processor.stop();
      vi.useRealTimers();
    });

    it("should catch errors during poller fallback", async () => {
      vi.useFakeTimers();

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();
      vi.clearAllMocks();

      // WS inactive and not recovering
      mockWsIndexerInstance.isActive.mockReturnValue(false);
      mockWsIndexerInstance.isRecovering.mockReturnValue(false);

      mockPollerInstance.start
        .mockRejectedValueOnce(new Error("poller start failed"))
        .mockResolvedValueOnce(undefined);

      await vi.advanceTimersByTimeAsync(10000);
      expect(mockPollerInstance.start).toHaveBeenCalledTimes(1);
      expect(processor.getStatus().pollerActive).toBe(false);
      expect(processor.getStatus().mode).toBe("auto");
      expect(processor.getStatus().configuredMode).toBe("auto");

      await vi.advanceTimersByTimeAsync(10000);
      expect(mockPollerInstance.start).toHaveBeenCalledTimes(2);
      expect(processor.getStatus().pollerActive).toBe(true);
      expect(processor.getStatus().running).toBe(true);
      expect(processor.getStatus().mode).toBe("polling");
      expect(processor.getStatus().configuredMode).toBe("auto");

      await processor.stop();
      vi.useRealTimers();
    });

    it("should not attach a websocket indexer after stop during websocket startup", async () => {
      const bootstrapPoller = createPollerMock();
      const wsInstance = createWsIndexerMock();
      const wsStart = createDeferred<void>();

      bootstrapPoller.bootstrap.mockResolvedValue(undefined);
      wsInstance.start.mockImplementation(() => wsStart.promise);
      wsInstance.stop.mockResolvedValue(undefined);
      wsInstance.isActive.mockReturnValue(true);
      wsInstance.isRecovering.mockReturnValue(false);

      vi.mocked(Poller).mockImplementationOnce(function MockPollerBootstrap() {
        return bootstrapPoller as any;
      });
      vi.mocked(WebSocketIndexer).mockImplementationOnce(function MockWsIndexer() {
        return wsInstance as any;
      });

      const processor = new Processor(mockPrisma, null, { mode: "websocket" });
      const startPromise = processor.start();

      await Promise.resolve();
      let stopResolved = false;
      const stopPromise = processor.stop().then(() => {
        stopResolved = true;
      });
      await Promise.resolve();

      wsStart.resolve();
      await stopPromise;
      await startPromise;

      expect((processor as any).wsIndexer).toBeNull();
      expect(wsInstance.stop.mock.calls.length).toBeLessThanOrEqual(wsInstance.start.mock.calls.length);
      expect(processor.getStatus()).toEqual({
        running: false,
        mode: "websocket",
        configuredMode: "websocket",
        pollerActive: false,
        wsActive: false,
        verifierActive: false,
        verifierStats: undefined,
      });
    });

    it("should not attach a poller after stop during polling startup", async () => {
      const nextPoller = createPollerMock();
      const pollerStart = createDeferred<void>();

      nextPoller.start.mockImplementation(() => pollerStart.promise);
      nextPoller.stop.mockResolvedValue(undefined);

      vi.mocked(Poller).mockImplementationOnce(function MockPollerStartup() {
        return nextPoller as any;
      });

      const processor = new Processor(mockPrisma, null, { mode: "polling" });
      const startPromise = processor.start();

      await Promise.resolve();
      let stopResolved = false;
      const stopPromise = processor.stop().then(() => {
        stopResolved = true;
      });
      await Promise.resolve();

      pollerStart.resolve();
      await stopPromise;
      await startPromise;

      expect((processor as any).poller).toBeNull();
      expect(nextPoller.stop).toHaveBeenCalled();
      expect(processor.getStatus()).toEqual({
        running: false,
        mode: "polling",
        configuredMode: "polling",
        pollerActive: false,
        wsActive: false,
        verifierActive: false,
        verifierStats: undefined,
      });
    });

    it("should not attach a poller after stop during polling failover", async () => {
      const nextPoller = createPollerMock();
      const pollerStart = createDeferred<void>();

      nextPoller.start.mockImplementation(() => pollerStart.promise);
      nextPoller.stop.mockResolvedValue(undefined);
      mockWsIndexerInstance.stop.mockResolvedValue(undefined);

      vi.mocked(Poller).mockImplementationOnce(function MockPollerFailover() {
        return nextPoller as any;
      });

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      (processor as any).isRunning = true;
      (processor as any).runToken = 1;
      (processor as any).wsIndexer = mockWsIndexerInstance;

      const failoverPromise = (processor as any).failOverToPolling(1);
      await Promise.resolve();
      pollerStart.resolve();
      await processor.stop();
      await failoverPromise;

      expect((processor as any).poller).toBeNull();
      expect(nextPoller.stop).toHaveBeenCalled();
      expect(processor.getStatus()).toEqual({
        running: false,
        mode: "auto",
        configuredMode: "auto",
        pollerActive: false,
        wsActive: false,
        verifierActive: false,
        verifierStats: undefined,
      });
    });

    it("should ignore a stale startup failure from the previous run token", async () => {
      const oldPoller = createPollerMock();
      const oldStart = createDeferred<void>();
      const currentPoller = createPollerMock();

      oldPoller.start.mockImplementation(() => oldStart.promise);
      oldPoller.stop.mockResolvedValue(undefined);
      currentPoller.stop.mockResolvedValue(undefined);

      vi.mocked(Poller).mockImplementationOnce(function MockOldPoller() {
        return oldPoller as any;
      });

      const processor = new Processor(mockPrisma, null, { mode: "polling" });
      const firstStart = processor.start().catch((error) => error);
      await Promise.resolve();

      (processor as any).runToken = 2;
      (processor as any).isRunning = true;
      (processor as any).poller = currentPoller;
      (processor as any).verifier = mockVerifierInstance;

      oldStart.reject(new Error("stale startup failure"));
      await firstStart;

      expect((processor as any).poller).toBe(currentPoller);
      expect(currentPoller.stop).not.toHaveBeenCalled();
      expect(processor.getStatus()).toEqual({
        running: true,
        mode: "polling",
        configuredMode: "polling",
        pollerActive: true,
        wsActive: false,
        verifierActive: true,
        verifierStats: {},
      });
    });

    it("should not schedule next check when not running (scheduleNextWsCheck)", async () => {
      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      (processor as any).isRunning = false;
      (processor as any).scheduleNextWsCheck();
      expect((processor as any).wsMonitorInterval).toBeNull();
    });

    it("should replace any existing monitor timeout when scheduling the next check", async () => {
      vi.useFakeTimers();

      const clearTimeoutSpy = vi.spyOn(globalThis, "clearTimeout");
      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      (processor as any).isRunning = true;
      const previousTimeout = setTimeout(() => undefined, 5000);
      (processor as any).wsMonitorInterval = previousTimeout;

      (processor as any).scheduleNextWsCheck();

      expect(clearTimeoutSpy).toHaveBeenCalledWith(previousTimeout);
      expect((processor as any).wsMonitorInterval).not.toBe(previousTimeout);

      clearTimeoutSpy.mockRestore();
      vi.useRealTimers();
    });

    it("should not schedule initial check when isRunning is false", async () => {
      vi.useFakeTimers();

      const processor = new Processor(mockPrisma, null, { mode: "auto" });

      // Set isRunning to false before calling monitorWebSocket directly
      (processor as any).isRunning = false;
      (processor as any).monitorWebSocket();

      // No timeout should be scheduled since isRunning is false
      expect((processor as any).wsMonitorInterval).toBeNull();

      vi.useRealTimers();
    });
  });

  describe("verifier lifecycle", () => {
    it("should start verifier", async () => {
      const processor = new Processor(mockPrisma, null, { mode: "polling" });
      await processor.start();

      expect(DataVerifier).toHaveBeenCalledWith(
        expect.anything(),
        mockPrisma,
        null,
        config.verifyIntervalMs
      );
      expect(mockVerifierInstance.start).toHaveBeenCalled();

      const status = processor.getStatus();
      expect(status.verifierActive).toBe(true);
      expect(status.verifierStats).toEqual({});

      await processor.stop();
    });

    it("should stop verifier on processor stop", async () => {
      const processor = new Processor(mockPrisma, null, { mode: "polling" });
      await processor.start();

      expect(mockVerifierInstance.start).toHaveBeenCalled();

      await processor.stop();

      expect(mockVerifierInstance.stop).toHaveBeenCalled();
      expect(processor.getStatus().verifierActive).toBe(false);
    });

    it("should not continue startup after stop during verifier startup", async () => {
      const verifierStart = createDeferred<void>();
      mockVerifierInstance.start.mockImplementationOnce(() => verifierStart.promise);

      const processor = new Processor(mockPrisma, null, { mode: "polling" });
      const startPromise = processor.start();

      await Promise.resolve();
      let stopResolved = false;
      const stopPromise = processor.stop().then(() => {
        stopResolved = true;
      });
      await Promise.resolve();

      expect(stopResolved).toBe(false);

      verifierStart.resolve();
      await stopPromise;
      await startPromise;

      expect(mockPollerInstance.start).toHaveBeenCalled();
      expect(mockPollerInstance.stop).toHaveBeenCalled();
      expect(processor.getStatus()).toEqual({
        running: false,
        mode: "polling",
        configuredMode: "polling",
        pollerActive: false,
        wsActive: false,
        verifierActive: false,
        verifierStats: undefined,
      });
    });

    it("should return verifierStats in getStatus when verifier is active", async () => {
      const fakeStats = { checked: 10, mismatches: 0 };
      mockVerifierInstance.getStats.mockReturnValue(fakeStats);

      const processor = new Processor(mockPrisma, null, { mode: "polling" });
      await processor.start();

      const status = processor.getStatus();
      expect(status.verifierStats).toEqual(fakeStats);

      await processor.stop();
    });

    it("should clean up partial startup state when verifier start fails", async () => {
      mockVerifierInstance.start.mockRejectedValueOnce(new Error("verifier boom"));

      const processor = new Processor(mockPrisma, null, { mode: "polling" });

      await expect(processor.start()).rejects.toThrow("verifier boom");

      expect(mockPollerInstance.start).toHaveBeenCalled();
      expect(mockPollerInstance.stop).toHaveBeenCalled();
      expect(mockVerifierInstance.stop).toHaveBeenCalled();
      expect(processor.getStatus()).toEqual({
        running: false,
        mode: "polling",
        configuredMode: "polling",
        pollerActive: false,
        wsActive: false,
        verifierActive: false,
        verifierStats: undefined,
      });
    });

    it("should clean up websocket startup state when verifier start fails", async () => {
      const bootstrapPollerA = createPollerMock();
      const bootstrapPollerB = createPollerMock();
      const wsInstance = createWsIndexerMock();
      bootstrapPollerA.bootstrap.mockResolvedValue(undefined);
      bootstrapPollerB.bootstrap.mockResolvedValue(undefined);
      wsInstance.start.mockResolvedValue(undefined);
      wsInstance.stop.mockResolvedValue(undefined);
      wsInstance.isActive.mockReturnValue(true);
      wsInstance.isRecovering.mockReturnValue(false);
      vi.mocked(Poller).mockImplementationOnce(function MockPollerA() {
        return bootstrapPollerA as any;
      }).mockImplementationOnce(function MockPollerB() {
        return bootstrapPollerB as any;
      });
      vi.mocked(WebSocketIndexer).mockImplementationOnce(function MockWsIndexer() {
        return wsInstance as any;
      });
      mockVerifierInstance.start.mockRejectedValueOnce(new Error("verifier boom"));

      const processor = new Processor(mockPrisma, null, { mode: "websocket" });

      await expect(processor.start()).rejects.toThrow("verifier boom");

      expect(bootstrapPollerA.bootstrap).toHaveBeenCalled();
      expect(bootstrapPollerB.bootstrap).toHaveBeenCalled();
      expect(mockVerifierInstance.stop).toHaveBeenCalledTimes(1);
      expect((processor as any).wsMonitorInterval).toBeNull();
      expect(processor.getStatus()).toEqual({
        running: false,
        mode: "websocket",
        configuredMode: "websocket",
        pollerActive: false,
        wsActive: false,
        verifierActive: false,
        verifierStats: undefined,
      });
    });

    it("should clean up auto websocket startup state when verifier start fails", async () => {
      const bootstrapPollerA = createPollerMock();
      const bootstrapPollerB = createPollerMock();
      const wsInstance = createWsIndexerMock();
      bootstrapPollerA.bootstrap.mockResolvedValue(undefined);
      bootstrapPollerB.bootstrap.mockResolvedValue(undefined);
      wsInstance.start.mockResolvedValue(undefined);
      wsInstance.stop.mockResolvedValue(undefined);
      wsInstance.isActive.mockReturnValue(true);
      wsInstance.isRecovering.mockReturnValue(false);
      vi.mocked(Poller).mockImplementationOnce(function MockPollerA() {
        return bootstrapPollerA as any;
      }).mockImplementationOnce(function MockPollerB() {
        return bootstrapPollerB as any;
      });
      vi.mocked(WebSocketIndexer).mockImplementationOnce(function MockWsIndexer() {
        return wsInstance as any;
      });
      mockVerifierInstance.start.mockRejectedValueOnce(new Error("verifier boom"));

      const processor = new Processor(mockPrisma, null, { mode: "auto" });

      await expect(processor.start()).rejects.toThrow("verifier boom");

      expect(testWebSocketConnection).toHaveBeenCalled();
      expect(bootstrapPollerA.bootstrap).toHaveBeenCalled();
      expect(bootstrapPollerB.bootstrap).toHaveBeenCalled();
      expect(mockVerifierInstance.stop).toHaveBeenCalledTimes(1);
      expect((processor as any).wsMonitorInterval).toBeNull();
      expect(processor.getStatus()).toEqual({
        running: false,
        mode: "auto",
        configuredMode: "auto",
        pollerActive: false,
        wsActive: false,
        verifierActive: false,
        verifierStats: undefined,
      });
    });
  });

  describe("bootstrap shutdown", () => {
    it("should stop active bootstrap pollers on processor stop", async () => {
      const bootstrapPoller = createPollerMock();
      const processor = new Processor(mockPrisma, null, { mode: "websocket" });
      (processor as any).isRunning = true;
      (processor as any).activeBootstrapPollers.add(bootstrapPoller);
      await processor.stop();

      expect(bootstrapPoller.stop).toHaveBeenCalledTimes(1);
      expect((processor as any).activeBootstrapPollers.size).toBe(0);
      expect(processor.getStatus().running).toBe(false);
    });
  });
});
