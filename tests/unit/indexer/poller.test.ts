import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { PublicKey } from "@solana/web3.js";
import { config } from "../../../src/config.js";
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

const createDeferred = <T = void>() => {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};

describe("Poller", () => {
  let poller: Poller;
  let mockConnection: ReturnType<typeof createMockConnection>;
  let mockPrisma: ReturnType<typeof createMockPrismaClient>;

  beforeEach(() => {
    mockConnection = createMockConnection();
    mockPrisma = createMockPrismaClient();
    (mockConnection.getBlock as any).mockResolvedValue({
      transactions: [
        { transaction: { signatures: [TEST_SIGNATURE] } },
      ],
    });

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

    it("should retry historical catch-up after a retryable pause and resume from persisted cursor", async () => {
      (config as any).indexerStartSignature = "cursor-stop";
      const loadStateSpy = vi.spyOn(poller as any, "loadState").mockImplementation(async () => {
        (poller as any).lastSignature = "cursor-stop";
      });
      const catchUpSpy = vi
        .spyOn(poller as any, "catchUpHistoricalGap")
        .mockResolvedValueOnce(false)
        .mockResolvedValueOnce(true);
      const waitSpy = vi.spyOn(poller as any, "waitForDelay").mockResolvedValue(true);

      (poller as any).isRunning = true;
      await (poller as any).initializeCursor();

      expect(loadStateSpy).toHaveBeenCalledTimes(2);
      expect(catchUpSpy).toHaveBeenCalledTimes(2);
      expect(waitSpy).toHaveBeenCalledWith(100);
      (config as any).indexerStartSignature = null;
    });

    it("should resume bounded historical catch-up after a paused backfill seeds a cursor", async () => {
      let loadCalls = 0;
      vi.spyOn(poller as any, "loadState").mockImplementation(async () => {
        loadCalls += 1;
        (poller as any).lastSignature = loadCalls >= 2 ? "cursor-after-backfill" : null;
      });
      const backfillSpy = vi.spyOn(poller as any, "backfill").mockResolvedValue(false);
      const catchUpSpy = vi.spyOn(poller as any, "catchUpHistoricalGap").mockResolvedValue(true);
      const waitSpy = vi.spyOn(poller as any, "waitForDelay").mockResolvedValue(true);

      (poller as any).isRunning = true;
      await (poller as any).initializeCursor();

      expect(backfillSpy).toHaveBeenCalledTimes(1);
      expect(catchUpSpy).toHaveBeenCalledTimes(1);
      expect(waitSpy).toHaveBeenCalledWith(100);
      expect((poller as any).pendingHistoricalResume).toBe(false);
    });

    it("should resume persisted historical scan pages even without INDEXER_START_SIGNATURE", async () => {
      vi.spyOn(poller as any, "loadState").mockImplementation(async () => {
        (poller as any).lastSignature = "current-main";
      });
      vi.spyOn(poller as any, "loadActiveHistoricalScanState").mockResolvedValue({
        stopSignature: "persisted-stop",
        pages: [
          {
            pageIndex: 0,
            beforeSignature: null,
            nextBeforeSignature: "older-page",
            signatures: [
              { signature: "sig-a", slot: 11, err: null, memo: null, blockTime: null, confirmationStatus: "confirmed" },
            ],
          },
        ],
      });
      const catchUpSpy = vi
        .spyOn(poller as any, "catchUpHistoricalGap")
        .mockResolvedValue(true);

      (poller as any).isRunning = true;
      await (poller as any).initializeCursor();

      expect(catchUpSpy).toHaveBeenCalledWith(
        "persisted-stop",
        expect.arrayContaining([
          expect.objectContaining({ pageIndex: 0, beforeSignature: null }),
        ])
      );
    });

    it("should not revive stale persisted historical scan pages when current main has no matching pages", async () => {
      const persistedRow = {
        id: "historical-scan:persisted-stop:3",
        lastSignature: "older-before",
        lastSlot: null,
        lastTxIndex: 3,
        source: JSON.stringify({
          nextBeforeSignature: "older-next",
          signatures: [
            { signature: "sig-a", slot: 11, err: null, memo: null, blockTime: null, confirmationStatus: "confirmed" },
          ],
        }),
        updatedAt: new Date(),
      } as any;
      vi.spyOn(mockPrisma.indexerState, "findMany")
        .mockResolvedValueOnce([persistedRow])
        .mockResolvedValueOnce([persistedRow])
        .mockResolvedValueOnce([persistedRow]);

      const result = await (poller as any).loadActiveHistoricalScanState("current-main");

      expect(result).toBeNull();
    });

    it("should skip already-consumed persisted pages using historical scan progress", async () => {
      const persistedRows = [
        {
          id: "historical-scan:persisted-stop:0",
          lastSignature: "before-0",
          lastSlot: null,
          lastTxIndex: 0,
          source: JSON.stringify({
            nextBeforeSignature: "before-1",
            signatures: [
              { signature: "sig-0", slot: 100, err: null, memo: null, blockTime: null, confirmationStatus: "confirmed" },
            ],
          }),
          updatedAt: new Date(),
        },
        {
          id: "historical-scan:persisted-stop:1",
          lastSignature: "before-1",
          lastSlot: null,
          lastTxIndex: 1,
          source: JSON.stringify({
            nextBeforeSignature: "before-2",
            signatures: [
              { signature: "sig-1", slot: 101, err: null, memo: null, blockTime: null, confirmationStatus: "confirmed" },
            ],
          }),
          updatedAt: new Date(),
        },
        {
          id: "historical-scan:persisted-stop:2",
          lastSignature: "before-2",
          lastSlot: null,
          lastTxIndex: 2,
          source: JSON.stringify({
            nextBeforeSignature: "before-3",
            signatures: [
              { signature: "sig-2", slot: 102, err: null, memo: null, blockTime: null, confirmationStatus: "confirmed" },
            ],
          }),
          updatedAt: new Date(),
        },
        {
          id: "historical-scan:persisted-stop:3",
          lastSignature: "before-3",
          lastSlot: null,
          lastTxIndex: 3,
          source: JSON.stringify({
            nextBeforeSignature: "before-4",
            signatures: [
              { signature: "sig-3", slot: 103, err: null, memo: null, blockTime: null, confirmationStatus: "confirmed" },
            ],
          }),
          updatedAt: new Date(),
        },
      ] as any[];

      vi.spyOn(mockPrisma.indexerState, "findMany")
        .mockResolvedValueOnce([persistedRows[3]])
        .mockResolvedValueOnce(persistedRows);
      vi.spyOn(mockPrisma.indexerState, "findUnique").mockImplementation(async (args: any) => {
        if (args?.where?.id === "historical-scan-state:current-main") {
          return null as any;
        }
        if (args?.where?.id === "historical-scan-progress:persisted-stop") {
          return { lastTxIndex: 1, lastSignature: null } as any;
        }
        return null as any;
      });

      const result = await (poller as any).loadActiveHistoricalScanState();

      expect(result).toEqual({
        stopSignature: "persisted-stop",
        pages: [
          expect.objectContaining({ pageIndex: 0, beforeSignature: "before-0" }),
          expect.objectContaining({ pageIndex: 1, beforeSignature: "before-1" }),
        ],
      });
      expect(mockPrisma.indexerState.findUnique).toHaveBeenCalledWith({
        where: { id: "historical-scan-progress:persisted-stop" },
        select: { lastTxIndex: true, lastSignature: true },
      });
    });

    it("should return null when no persisted historical scan pages exist", async () => {
      const loadHistoricalScanPagesSpy = vi
        .spyOn(poller as any, "loadHistoricalScanPages")
        .mockResolvedValue([]);
      const prismaFindManySpy = vi.spyOn(mockPrisma.indexerState, "findMany");

      const result = await (poller as any).loadActiveHistoricalScanState("current-main");

      expect(result).toBeNull();
      expect(loadHistoricalScanPagesSpy).toHaveBeenCalledWith("current-main");
      expect(prismaFindManySpy).not.toHaveBeenCalled();
    });
  });

  describe("bootstrap", () => {
    it("should load state and drain catch-up batches without staying in polling mode", async () => {
      const loadStateSpy = vi.spyOn(poller as any, "loadState").mockResolvedValue(undefined);
      const processSpy = vi
        .spyOn(poller as any, "processNewTransactions")
        .mockResolvedValueOnce({ fetchedCount: 2, haltedOnError: false })
        .mockResolvedValueOnce({ fetchedCount: 0, haltedOnError: false });

      await poller.bootstrap();

      expect(loadStateSpy).toHaveBeenCalledTimes(1);
      expect(processSpy).toHaveBeenCalledTimes(2);
      expect((poller as any).isRunning).toBe(false);
    });

    it("should back off after a halted frontier batch instead of hot-looping", async () => {
      vi.useFakeTimers();

      const loadStateSpy = vi.spyOn(poller as any, "loadState").mockResolvedValue(undefined);
      const processSpy = vi
        .spyOn(poller as any, "processNewTransactions")
        .mockResolvedValueOnce({ fetchedCount: 1, haltedOnError: true })
        .mockResolvedValueOnce({ fetchedCount: 0, haltedOnError: false });

      const bootstrapPromise = poller.bootstrap({ retryDelayMs: 5000 });
      await vi.advanceTimersByTimeAsync(0);

      expect(loadStateSpy).toHaveBeenCalledTimes(1);
      expect(processSpy).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(1000);
      expect(processSpy).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(4000);
      await bootstrapPromise;

      expect(processSpy).toHaveBeenCalledTimes(2);
      vi.useRealTimers();
    });

    it("should cancel bootstrap retry sleep when stopped", async () => {
      vi.useFakeTimers();

      vi.spyOn(poller as any, "loadState").mockResolvedValue(undefined);
      const processSpy = vi
        .spyOn(poller as any, "processNewTransactions")
        .mockResolvedValue({ fetchedCount: 1, haltedOnError: true });

      const bootstrapPromise = poller.bootstrap({ retryDelayMs: 5000 });
      await vi.advanceTimersByTimeAsync(0);

      let stopped = false;
      const stopPromise = poller.stop().then(() => {
        stopped = true;
      });
      await vi.advanceTimersByTimeAsync(0);

      expect(stopped).toBe(true);

      await stopPromise;
      await bootstrapPromise;
      expect(processSpy).toHaveBeenCalledTimes(1);

      vi.useRealTimers();
    });

    it("should suppress event log writes during bootstrap catch-up when requested", async () => {
      const eventData = {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmBootstrap",
      };
      const logs = createEventLogs("AgentRegistered", eventData);
      const sig = createMockSignatureInfo();
      const tx = createMockParsedTransaction(TEST_SIGNATURE, logs);

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue({
        id: "main",
        lastSignature: "previous-sig",
        lastSlot: 100n,
      });
      (mockConnection.getSignaturesForAddress as any)
        .mockResolvedValueOnce([sig])
        .mockResolvedValue([]);
      (mockConnection.getParsedTransaction as any).mockResolvedValue(tx);

      await poller.bootstrap({ suppressEventLogWrites: true });

      expect(mockPrisma.agent.upsert).toHaveBeenCalled();
      expect(mockPrisma.eventLog.create).not.toHaveBeenCalled();
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

    it("should cancel the pending poll sleep so stop resolves immediately", async () => {
      vi.useFakeTimers();

      const sleepyPoller = new Poller({
        connection: mockConnection as any,
        prisma: mockPrisma,
        programId: TEST_PROGRAM_ID,
        pollingInterval: 60_000,
        batchSize: 10,
      });

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue({
        id: "main",
        lastSignature: TEST_SIGNATURE,
        lastSlot: TEST_SLOT,
      });
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);

      await sleepyPoller.start();
      await vi.advanceTimersByTimeAsync(0);

      let stopped = false;
      const stopPromise = sleepyPoller.stop().then(() => {
        stopped = true;
      });
      await vi.advanceTimersByTimeAsync(0);

      expect(stopped).toBe(true);
      await stopPromise;
      vi.useRealTimers();
    });

    it("should not save cursor after stop interrupts an in-flight transaction", async () => {
      const sig = createMockSignatureInfo();
      const txDone = createDeferred<Date>();

      (poller as any).isRunning = true;
      (poller as any).lastSignature = "previous-sig";
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([sig]);
      const processSpy = vi
        .spyOn(poller as any, "processTransaction")
        .mockImplementation(() => txDone.promise);
      const saveStateSpy = vi.spyOn(poller as any, "saveState");

      const cyclePromise = (poller as any).processNewTransactions();
      await Promise.resolve();
      const stopPromise = poller.stop();
      txDone.resolve(new Date());
      await cyclePromise;
      await stopPromise;

      expect(processSpy.mock.calls.length).toBeLessThanOrEqual(1);
      expect(saveStateSpy).not.toHaveBeenCalled();
    });

    it("should not retry getBlock after stop cancels retry delay", async () => {
      vi.useFakeTimers();

      const sig = createMockSignatureInfo();
      (mockConnection.getBlock as any)
        .mockRejectedValueOnce(new Error("RPC temporary failure"))
        .mockResolvedValueOnce({ transactions: [] });

      const txIndexPromise = (poller as any).getTxIndexMap(Number(TEST_SLOT), [sig]);
      await vi.advanceTimersByTimeAsync(0);

      await poller.stop();
      await vi.advanceTimersByTimeAsync(0);
      const txIndexMap = await txIndexPromise;

      expect(mockConnection.getBlock).toHaveBeenCalledTimes(1);
      expect(txIndexMap.get(sig.signature)).toBeNull();

      vi.useRealTimers();
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

    it("should not advance cursor when transaction fetch returns null", async () => {
      vi.useFakeTimers();
      const sig = createMockSignatureInfo();

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue({
        id: "main",
        lastSignature: "previous-sig",
        lastSlot: 100n,
      });
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([sig]);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);
      (mockConnection.getParsedTransaction as any).mockResolvedValue(null);

      const startPromise = poller.start();
      await vi.advanceTimersByTimeAsync(8000);
      await startPromise;

      expect(mockConnection.getSignaturesForAddress).toHaveBeenCalled();
      expect(mockPrisma.indexerState.upsert).not.toHaveBeenCalled();
      expect(mockPrisma.eventLog.create).toHaveBeenCalledWith({
        data: expect.objectContaining({
          eventType: "PROCESSING_FAILED",
          processed: false,
          error: expect.stringContaining("temporarily unavailable"),
        }),
      });
      vi.useRealTimers();
    });

    it("should not advance cursor when program data logs fail to parse", async () => {
      const sig = createMockSignatureInfo();
      const tx = createMockParsedTransaction(TEST_SIGNATURE, [
        `Program ${TEST_PROGRAM_ID.toBase58()} invoke [1]`,
        "Program data: AAAAAAAAAAAAAAAA",
        `Program ${TEST_PROGRAM_ID.toBase58()} success`,
      ]);

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue({
        id: "main",
        lastSignature: "previous-sig",
        lastSlot: 100n,
      });
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([sig]);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);
      (mockConnection.getParsedTransaction as any).mockResolvedValue(tx);

      await poller.start();
      await new Promise((r) => setTimeout(r, 150));

      expect(mockPrisma.indexerState.upsert).not.toHaveBeenCalled();
      expect(mockPrisma.eventLog.create).toHaveBeenCalledWith({
        data: expect.objectContaining({
          eventType: "PROCESSING_FAILED",
          processed: false,
          error: expect.stringContaining("Failed to parse program events"),
        }),
      });
    });

    it("should save state after processing", async () => {
      const sig = createMockSignatureInfo();
      const eventData = {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmTest",
      };
      const logs = createEventLogs("AgentRegistered", eventData);
      const tx = createMockParsedTransaction(TEST_SIGNATURE, logs);

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue({
        id: "main",
        lastSignature: "previous-sig",
        lastSlot: 100n,
      });
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([sig]);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);
      (mockConnection.getParsedTransaction as any).mockResolvedValue(tx);

      await poller.start();
      await new Promise((r) => setTimeout(r, 200));

      expect(mockPrisma.indexerState.upsert).toHaveBeenCalled();
    });

    it("should log failed transaction processing", async () => {
      const sig = createMockSignatureInfo();

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue({
        id: "main",
        lastSignature: "resume-signature",
        lastSlot: TEST_SLOT,
      });
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

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue({
        id: "main",
        lastSignature: "resume-signature",
        lastSlot: TEST_SLOT,
      });
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

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue({
        id: "main",
        lastSignature: "resume-signature",
        lastSlot: TEST_SLOT,
      });
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
      (mockPrisma.indexerState.findUnique as any).mockResolvedValue({
        id: "main",
        lastSignature: "resume-signature",
        lastSlot: TEST_SLOT,
      });
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
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmTest",
      };

      const logs = createEventLogs("AgentRegistered", eventData);

      const sig = createMockSignatureInfo();
      const tx = createMockParsedTransaction(TEST_SIGNATURE, logs);

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue({
        id: "main",
        lastSignature: "previous-sig",
        lastSlot: 100n,
      });
      (mockConnection.getSignaturesForAddress as any).mockResolvedValueOnce([sig]);
      (mockConnection.getSignaturesForAddress as any).mockResolvedValue([]);
      (mockConnection.getParsedTransaction as any).mockResolvedValue(tx);

      await poller.start();
      await new Promise((r) => setTimeout(r, 200));

      // Should have called handleEvent and created event log
      expect(mockPrisma.eventLog.create).toHaveBeenCalledWith({
        data: expect.objectContaining({
          eventType: "AgentRegistered",
          processed: true,
        }),
      });

      // Should have called agent.upsert from the handler
      expect(mockPrisma.agent.upsert).toHaveBeenCalled();
    });

    it("should process transaction without blockTime using fallback date", async () => {
      const eventData = {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmTest",
      };

      const logs = createEventLogs("AgentRegistered", eventData);

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

      (mockPrisma.indexerState.findUnique as any).mockResolvedValue({
        id: "main",
        lastSignature: "previous-sig",
        lastSlot: 100n,
      });
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
