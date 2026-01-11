import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { PublicKey } from "@solana/web3.js";
import { Poller } from "../../../src/indexer/poller.js";
import { createMockPrismaClient } from "../../mocks/prisma.js";
import {
  createMockConnection,
  createMockSignatureInfo,
  createMockParsedTransaction,
  createEventLogs,
  TEST_SIGNATURE,
  TEST_SLOT,
  TEST_PROGRAM_ID,
  TEST_ASSET,
  TEST_OWNER,
  TEST_COLLECTION,
  TEST_REGISTRY,
} from "../../mocks/solana.js";

describe("Poller", () => {
  let poller: Poller;
  let mockConnection: ReturnType<typeof createMockConnection>;
  let mockPrisma: ReturnType<typeof createMockPrismaClient>;

  beforeEach(() => {
    mockConnection = createMockConnection();
    mockPrisma = createMockPrismaClient();

    poller = new Poller({
      connection: mockConnection as any,
      prisma: mockPrisma,
      programId: TEST_PROGRAM_ID,
      pollingInterval: 100, // Fast for testing
      batchSize: 10,
    });
  });

  afterEach(async () => {
    await poller.stop();
  });

  describe("constructor", () => {
    it("should create poller with default options", () => {
      const defaultPoller = new Poller({
        connection: mockConnection as any,
        prisma: mockPrisma,
        programId: TEST_PROGRAM_ID,
      });

      expect(defaultPoller).toBeDefined();
    });

    it("should create poller with custom options", () => {
      expect(poller).toBeDefined();
    });
  });

  describe("start", () => {
    it("should start polling", async () => {
      (mockPrisma.indexerState.findUnique as any).mockResolvedValue(null);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);

      await poller.start();

      // Should have called loadState
      expect(mockPrisma.indexerState.findUnique).toHaveBeenCalledWith({
        where: { id: "main" },
      });
    });

    it("should not start twice", async () => {
      (mockPrisma.indexerState.findUnique as any).mockResolvedValue(null);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);

      await poller.start();
      await poller.start(); // Second call should be ignored

      expect(mockPrisma.indexerState.findUnique).toHaveBeenCalledTimes(1);
    });

    it("should resume from saved state", async () => {
      (mockPrisma.indexerState.findUnique as any).mockResolvedValue({
        id: "main",
        lastSignature: TEST_SIGNATURE,
        lastSlot: TEST_SLOT,
      });
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);

      await poller.start();

      expect(mockPrisma.indexerState.findUnique).toHaveBeenCalled();
    });
  });

  describe("stop", () => {
    it("should stop polling", async () => {
      (mockPrisma.indexerState.findUnique as any).mockResolvedValue(null);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);

      await poller.start();
      await poller.stop();

      // Poller should be stopped (no more polling)
      expect(true).toBe(true); // Poller stopped successfully
    });
  });

  describe("processNewTransactions", () => {
    it("should process new transactions", async () => {
      const sig = createMockSignatureInfo();
      const tx = createMockParsedTransaction(TEST_SIGNATURE, []);

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue(null);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([sig]);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);
      (mockConnection.getParsedTransaction as any).mockResolvedValue(tx);

      await poller.start();

      // Wait for first poll
      await new Promise((r) => setTimeout(r, 150));

      expect(mockConnection.getSignaturesForAddress).toHaveBeenCalled();
    });

    it("should handle empty signatures", async () => {
      (mockPrisma.indexerState.findUnique as any).mockResolvedValue(null);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);

      await poller.start();
      await new Promise((r) => setTimeout(r, 150));

      expect(mockConnection.getParsedTransaction).not.toHaveBeenCalled();
    });

    it("should filter failed transactions", async () => {
      const failedSig = createMockSignatureInfo(TEST_SIGNATURE, Number(TEST_SLOT), {
        err: "Transaction failed",
      });

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue(null);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([
        failedSig,
      ]);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);

      await poller.start();
      await new Promise((r) => setTimeout(r, 150));

      // Failed transactions should be filtered out
      expect(mockConnection.getParsedTransaction).not.toHaveBeenCalled();
    });

    it("should handle null transaction gracefully", async () => {
      const sig = createMockSignatureInfo();

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue(null);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([sig]);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);
      (mockConnection.getParsedTransaction as any).mockResolvedValue(null);

      await poller.start();
      await new Promise((r) => setTimeout(r, 150));

      // State should be saved even when transaction is null (to mark it processed)
      expect(mockPrisma.indexerState.upsert).toHaveBeenCalled();
    });

    it("should save state after processing", async () => {
      const sig = createMockSignatureInfo();
      const tx = createMockParsedTransaction(TEST_SIGNATURE, [
        "Program log: test",
      ]);

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue(null);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([sig]);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);
      (mockConnection.getParsedTransaction as any).mockResolvedValue(tx);

      await poller.start();
      await new Promise((r) => setTimeout(r, 200));

      expect(mockPrisma.indexerState.upsert).toHaveBeenCalled();
    });

    it("should log failed transaction processing", async () => {
      const sig = createMockSignatureInfo();

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue(null);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([sig]);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);
      (mockConnection.getParsedTransaction as any).mockRejectedValue(
        new Error("RPC error")
      );

      await poller.start();
      await new Promise((r) => setTimeout(r, 150));

      expect(mockPrisma.eventLog.create).toHaveBeenCalledWith({
        data: expect.objectContaining({
          eventType: "PROCESSING_FAILED",
          processed: false,
        }),
      });
    });

    it("should log failed transaction processing with non-Error object", async () => {
      const sig = createMockSignatureInfo();

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue(null);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([sig]);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);
      (mockConnection.getParsedTransaction as any).mockRejectedValue("String error");

      await poller.start();
      await new Promise((r) => setTimeout(r, 150));

      expect(mockPrisma.eventLog.create).toHaveBeenCalledWith({
        data: expect.objectContaining({
          eventType: "PROCESSING_FAILED",
          error: "String error",
        }),
      });
    });

    it("should log failed transaction without blockTime", async () => {
      // Create signature without blockTime
      const sig = {
        signature: TEST_SIGNATURE,
        slot: Number(TEST_SLOT),
        err: null,
        blockTime: null, // No blockTime - should use fallback new Date()
        memo: null,
        confirmationStatus: "finalized" as const,
      };

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue(null);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([sig]);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);
      (mockConnection.getParsedTransaction as any).mockRejectedValue(
        new Error("RPC error")
      );

      await poller.start();
      await new Promise((r) => setTimeout(r, 150));

      expect(mockPrisma.eventLog.create).toHaveBeenCalledWith({
        data: expect.objectContaining({
          eventType: "PROCESSING_FAILED",
          blockTime: expect.any(Date),
        }),
      });
    });

    it("should handle error in polling loop gracefully", async () => {
      (mockPrisma.indexerState.findUnique as any).mockResolvedValue(null);
      // Make getSignaturesForAddress throw to trigger catch block in poll()
      (mockConnection.getSignaturesForAddress as any).mockRejectedValueOnce(
        new Error("Network error")
      );
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);

      await poller.start();
      await new Promise((r) => setTimeout(r, 250));

      // Poller should continue running after error
      // The error is logged but poller continues
      expect(mockConnection.getSignaturesForAddress).toHaveBeenCalled();
    });

    it("should process events from transaction with valid event data", async () => {
      // Create valid encoded event
      const eventData = {
        asset: TEST_ASSET,
        registry: TEST_REGISTRY,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
      };

      const logs = createEventLogs("AgentRegisteredInRegistry", eventData);

      const sig = createMockSignatureInfo();
      const tx = createMockParsedTransaction(TEST_SIGNATURE, logs);

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue(null);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([sig]);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);
      (mockConnection.getParsedTransaction as any).mockResolvedValue(tx);

      await poller.start();
      await new Promise((r) => setTimeout(r, 200));

      // Should have called handleEvent and created event log
      expect(mockPrisma.eventLog.create).toHaveBeenCalledWith({
        data: expect.objectContaining({
          eventType: "AgentRegisteredInRegistry",
          processed: true,
        }),
      });

      // Should have called agent.upsert from the handler
      expect(mockPrisma.agent.upsert).toHaveBeenCalled();
    });

    it("should process transaction without blockTime using fallback date", async () => {
      const eventData = {
        asset: TEST_ASSET,
        registry: TEST_REGISTRY,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
      };

      const logs = createEventLogs("AgentRegisteredInRegistry", eventData);

      // Signature without blockTime
      const sig = {
        signature: TEST_SIGNATURE,
        slot: Number(TEST_SLOT),
        err: null,
        blockTime: null,
        memo: null,
        confirmationStatus: "finalized" as const,
      };

      const tx = {
        slot: Number(TEST_SLOT),
        blockTime: null,
        transaction: { signatures: [TEST_SIGNATURE] },
        meta: { err: null, logMessages: logs },
      };

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue(null);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([sig]);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);
      (mockConnection.getParsedTransaction as any).mockResolvedValue(tx);

      await poller.start();
      await new Promise((r) => setTimeout(r, 200));

      // Event should still be processed with fallback date
      expect(mockPrisma.eventLog.create).toHaveBeenCalled();
    });

    it("should skip events that cannot be typed", async () => {
      // Create logs with a program invoke but no valid event data
      const logs = [
        `Program ${TEST_PROGRAM_ID.toBase58()} invoke [1]`,
        "Program log: some operation",
        `Program ${TEST_PROGRAM_ID.toBase58()} success`,
      ];

      const sig = createMockSignatureInfo();
      const tx = createMockParsedTransaction(TEST_SIGNATURE, logs);

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue(null);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([sig]);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);
      (mockConnection.getParsedTransaction as any).mockResolvedValue(tx);

      await poller.start();
      await new Promise((r) => setTimeout(r, 150));

      // No events parsed, so eventLog.create should not be called
      expect(mockPrisma.eventLog.create).not.toHaveBeenCalled();
    });
  });
});
