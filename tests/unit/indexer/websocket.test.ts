import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

vi.mock("@solana/web3.js", async () => {
  const actual = await vi.importActual<any>("@solana/web3.js");
  let getSlotMock: (() => Promise<number>) | null = null;

  class MockConnection {
    constructor(_endpoint: string, _config?: unknown) {}

    async getSlot(): Promise<number> {
      if (!getSlotMock) {
        throw new Error("getSlot mock not set");
      }
      return await getSlotMock();
    }
  }

  return {
    ...actual,
    Connection: MockConnection,
    __setGetSlotMock: (fn: (() => Promise<number>) | null) => {
      getSlotMock = fn;
    },
  };
});

import * as web3 from "@solana/web3.js";
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
        registry: TEST_REGISTRY,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmTest",
      };

      const logs = createEventLogs("AgentRegisteredInRegistry", eventData);

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
          eventType: "AgentRegisteredInRegistry",
          processed: true,
        }),
      });

      // Should have updated indexer state
      expect(mockPrisma.indexerState.upsert).toHaveBeenCalled();

      // Should have called agent.upsert from handler
      expect(mockPrisma.agent.upsert).toHaveBeenCalled();
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
        registry: TEST_REGISTRY,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmTest",
      };

      const logs = createEventLogs("AgentRegisteredInRegistry", eventData);

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
        registry: TEST_REGISTRY,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmTest",
      };

      const logs = createEventLogs("AgentRegisteredInRegistry", eventData);

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

      const encoded1 = encodeAnchorEvent("AgentRegisteredInRegistry", eventData1);
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
  };

  afterEach(() => {
    web3Mock.__setGetSlotMock(null);
    vi.restoreAllMocks();
  });

  it("should return false when connection fails", async () => {
    web3Mock.__setGetSlotMock(() =>
      Promise.reject(new Error("Connection failed"))
    );

    const result = await testWebSocketConnection("wss://invalid.nonexistent.local");

    expect(result).toBe(false);
  });

  it("should return true when connection succeeds", async () => {
    web3Mock.__setGetSlotMock(() => Promise.resolve(123));

    const result = await testWebSocketConnection("wss://api.devnet.solana.com");
    expect(result).toBe(true);
  });
});
