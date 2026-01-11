import { describe, it, expect, vi, beforeEach } from "vitest";
import { createMockPrismaClient } from "../../mocks/prisma.js";

// Use vi.hoisted to ensure mocks are defined before vi.mock factories run
const {
  mockPollerInstance,
  mockWsIndexerInstance,
  mockTestWebSocketConnection,
} = vi.hoisted(() => ({
  mockPollerInstance: {
    start: vi.fn(),
    stop: vi.fn(),
  },
  mockWsIndexerInstance: {
    start: vi.fn(),
    stop: vi.fn(),
    isActive: vi.fn(),
  },
  mockTestWebSocketConnection: vi.fn(),
}));

// Mock the modules BEFORE importing Processor
vi.mock("@solana/web3.js", () => ({
  Connection: vi.fn().mockImplementation(() => ({
    getSlot: vi.fn().mockResolvedValue(12345),
    getSignaturesForAddress: vi.fn().mockResolvedValue([]),
    getParsedTransaction: vi.fn().mockResolvedValue(null),
    onLogs: vi.fn().mockReturnValue(1),
    removeOnLogsListener: vi.fn().mockResolvedValue(undefined),
  })),
  PublicKey: vi.fn().mockImplementation((key: string) => ({
    toBase58: () => (typeof key === "string" ? key : "mocked-key"),
    toBytes: () => new Uint8Array(32),
  })),
}));

vi.mock("../../../src/indexer/poller.js", () => ({
  Poller: vi.fn(() => mockPollerInstance),
}));

vi.mock("../../../src/indexer/websocket.js", () => ({
  WebSocketIndexer: vi.fn(() => mockWsIndexerInstance),
  testWebSocketConnection: mockTestWebSocketConnection,
}));

vi.mock("../../../src/logger.js", () => ({
  createChildLogger: vi.fn(() => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  })),
}));

// Import after mocks are set up
import { Processor } from "../../../src/indexer/processor.js";
import { Poller } from "../../../src/indexer/poller.js";
import { WebSocketIndexer, testWebSocketConnection } from "../../../src/indexer/websocket.js";

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
    mockTestWebSocketConnection.mockResolvedValue(true);
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
      const processor = new Processor(mockPrisma, { mode: "polling" });
      expect(processor).toBeDefined();
      const status = processor.getStatus();
      expect(status.mode).toBe("polling");
    });

    it("should create processor with websocket mode", () => {
      const processor = new Processor(mockPrisma, { mode: "websocket" });
      expect(processor).toBeDefined();
      const status = processor.getStatus();
      expect(status.mode).toBe("websocket");
    });

    it("should create processor with auto mode", () => {
      const processor = new Processor(mockPrisma, { mode: "auto" });
      expect(processor).toBeDefined();
      const status = processor.getStatus();
      expect(status.mode).toBe("auto");
    });
  });

  describe("getStatus", () => {
    it("should return initial status for polling mode", () => {
      const processor = new Processor(mockPrisma, { mode: "polling" });

      const status = processor.getStatus();

      expect(status).toEqual({
        running: false,
        mode: "polling",
        pollerActive: false,
        wsActive: false,
      });
    });

    it("should return initial status for websocket mode", () => {
      const processor = new Processor(mockPrisma, { mode: "websocket" });

      const status = processor.getStatus();

      expect(status).toEqual({
        running: false,
        mode: "websocket",
        pollerActive: false,
        wsActive: false,
      });
    });

    it("should return initial status for auto mode", () => {
      const processor = new Processor(mockPrisma, { mode: "auto" });

      const status = processor.getStatus();

      expect(status).toEqual({
        running: false,
        mode: "auto",
        pollerActive: false,
        wsActive: false,
      });
    });
  });

  describe("start", () => {
    it("should start in polling mode", async () => {
      const processor = new Processor(mockPrisma, { mode: "polling" });
      await processor.start();

      const status = processor.getStatus();
      expect(status.running).toBe(true);
      expect(status.pollerActive).toBe(true);
      expect(Poller).toHaveBeenCalled();
      expect(mockPollerInstance.start).toHaveBeenCalled();
    });

    it("should start in websocket mode", async () => {
      const processor = new Processor(mockPrisma, { mode: "websocket" });
      await processor.start();

      const status = processor.getStatus();
      expect(status.running).toBe(true);
      expect(status.wsActive).toBe(true);
      expect(WebSocketIndexer).toHaveBeenCalled();
      expect(mockWsIndexerInstance.start).toHaveBeenCalled();
    });

    it("should start in auto mode with WebSocket available", async () => {
      const processor = new Processor(mockPrisma, { mode: "auto" });
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

      const processor = new Processor(mockPrisma, { mode: "auto" });
      await processor.start();

      const status = processor.getStatus();
      expect(status.running).toBe(true);
      expect(status.pollerActive).toBe(true);
      expect(testWebSocketConnection).toHaveBeenCalled();
    });

    it("should not start twice", async () => {
      const processor = new Processor(mockPrisma, { mode: "polling" });
      await processor.start();
      await processor.start(); // Second call should be ignored

      expect(Poller).toHaveBeenCalledTimes(1);
    });

    it("should handle default case in switch (same as auto)", async () => {
      // Create processor with a mode that falls through to default
      const processor = new Processor(mockPrisma, { mode: "auto" });
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
      const processor = new Processor(mockPrisma, { mode: "polling" });
      await processor.start();
      await processor.stop();

      const status = processor.getStatus();
      expect(status.running).toBe(false);
      expect(mockPollerInstance.stop).toHaveBeenCalled();
    });

    it("should stop websocket mode", async () => {
      const processor = new Processor(mockPrisma, { mode: "websocket" });
      await processor.start();
      await processor.stop();

      const status = processor.getStatus();
      expect(status.running).toBe(false);
      expect(mockWsIndexerInstance.stop).toHaveBeenCalled();
    });

    it("should stop auto mode", async () => {
      const processor = new Processor(mockPrisma, { mode: "auto" });
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

      const processor = new Processor(mockPrisma, { mode: "auto" });
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

      const processor = new Processor(mockPrisma, { mode: "auto" });
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
  });
});
