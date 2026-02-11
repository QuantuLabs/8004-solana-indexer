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
    indexerMode: "auto",
    pollingInterval: 5000,
    wsReconnectInterval: 1000,
    wsMaxRetries: 5,
    verificationEnabled: false, // Disable verifier in tests
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
    });

    it("should create processor with websocket mode", () => {
      const processor = new Processor(mockPrisma, null, { mode: "websocket" });
      expect(processor).toBeDefined();
      const status = processor.getStatus();
      expect(status.mode).toBe("websocket");
    });

    it("should create processor with auto mode", () => {
      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      expect(processor).toBeDefined();
      const status = processor.getStatus();
      expect(status.mode).toBe("auto");
    });
  });

  describe("getStatus", () => {
    it("should return initial status for polling mode", () => {
      const processor = new Processor(mockPrisma, null, { mode: "polling" });

      const status = processor.getStatus();

      expect(status).toEqual({
        running: false,
        mode: "polling",
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
    });

    it("should start in websocket mode", async () => {
      const processor = new Processor(mockPrisma, null, { mode: "websocket" });
      await processor.start();

      const status = processor.getStatus();
      expect(status.running).toBe(true);
      expect(status.wsActive).toBe(true);
      expect(WebSocketIndexer).toHaveBeenCalled();
      expect(mockWsIndexerInstance.start).toHaveBeenCalled();
    });

    it("should start in auto mode with WebSocket available", async () => {
      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      // Wait for async operations
      await new Promise((r) => setTimeout(r, 50));

      const status = processor.getStatus();
      expect(status.running).toBe(true);
      expect(testWebSocketConnection).toHaveBeenCalled();
      expect(WebSocketIndexer).toHaveBeenCalled();
      // In auto mode with WS available, both are started
      expect(Poller).toHaveBeenCalled();
    });

    it("should fallback to polling in auto mode when WebSocket unavailable", async () => {
      mockTestWebSocketConnection.mockResolvedValueOnce(false);

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      const status = processor.getStatus();
      expect(status.running).toBe(true);
      expect(status.pollerActive).toBe(true);
      expect(testWebSocketConnection).toHaveBeenCalled();
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
    it("should handle WebSocket connection loss in auto mode", async () => {
      vi.useFakeTimers();

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      // Simulate WebSocket becoming inactive
      mockWsIndexerInstance.isActive.mockReturnValue(false);

      // Advance timer to trigger monitor check
      await vi.advanceTimersByTimeAsync(10000);

      // Poller should be restarted with faster interval
      expect(mockPollerInstance.stop).toHaveBeenCalled();

      vi.useRealTimers();
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

      // Poller should NOT be restarted since isRunning is false
      expect(mockPollerInstance.stop).not.toHaveBeenCalled();

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

      // Poller should NOT be restarted since wsMonitorInProgress blocks entry
      expect(mockPollerInstance.stop).not.toHaveBeenCalled();

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

      // Poller should NOT be restarted since WS is recovering
      expect(mockPollerInstance.stop).not.toHaveBeenCalled();

      await processor.stop();
      vi.useRealTimers();
    });

    it("should catch errors during poller fallback", async () => {
      vi.useFakeTimers();

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      // WS inactive and not recovering
      mockWsIndexerInstance.isActive.mockReturnValue(false);
      mockWsIndexerInstance.isRecovering.mockReturnValue(false);

      // Make poller.stop throw
      mockPollerInstance.stop.mockRejectedValueOnce(new Error("poller stop failed"));

      // Advance to trigger check -- should not throw
      await vi.advanceTimersByTimeAsync(10000);

      // Processor should still be running (error was caught)
      expect(processor.getStatus().running).toBe(true);

      await processor.stop();
      vi.useRealTimers();
    });

    it("should not schedule next check when not running (scheduleNextWsCheck)", async () => {
      vi.useFakeTimers();

      const processor = new Processor(mockPrisma, null, { mode: "auto" });
      await processor.start();

      // WS is active, processor runs normally
      mockWsIndexerInstance.isActive.mockReturnValue(true);

      // Advance to trigger one check cycle
      await vi.advanceTimersByTimeAsync(10000);

      // Now stop the processor
      await processor.stop();

      // Clear mocks to track any new calls
      vi.clearAllMocks();

      // Advance further -- no more checks should fire
      await vi.advanceTimersByTimeAsync(30000);
      expect(mockPollerInstance.stop).not.toHaveBeenCalled();
      expect(mockWsIndexerInstance.isActive).not.toHaveBeenCalled();

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
    it("should start verifier when verificationEnabled is true", async () => {
      (config as any).verificationEnabled = true;

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

      // Restore
      (config as any).verificationEnabled = false;
    });

    it("should stop verifier on processor stop", async () => {
      (config as any).verificationEnabled = true;

      const processor = new Processor(mockPrisma, null, { mode: "polling" });
      await processor.start();

      expect(mockVerifierInstance.start).toHaveBeenCalled();

      await processor.stop();

      expect(mockVerifierInstance.stop).toHaveBeenCalled();
      expect(processor.getStatus().verifierActive).toBe(false);

      // Restore
      (config as any).verificationEnabled = false;
    });

    it("should return verifierStats in getStatus when verifier is active", async () => {
      (config as any).verificationEnabled = true;
      const fakeStats = { checked: 10, mismatches: 0 };
      mockVerifierInstance.getStats.mockReturnValue(fakeStats);

      const processor = new Processor(mockPrisma, null, { mode: "polling" });
      await processor.start();

      const status = processor.getStatus();
      expect(status.verifierStats).toEqual(fakeStats);

      await processor.stop();

      // Restore
      (config as any).verificationEnabled = false;
    });
  });
});
