import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Mock dependencies before imports
vi.mock("../../../src/config.js", () => ({
  config: {
    dbMode: "supabase",
    metadataIndexMode: "normal",
    validationIndexEnabled: true,
  },
}));

vi.mock("../../../src/logger.js", () => {
  const mockLogger = {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    fatal: vi.fn(),
    child: vi.fn(() => mockLogger),
  };
  return {
    createChildLogger: vi.fn(() => mockLogger),
    logger: mockLogger,
  };
});

vi.mock("../../../src/indexer/metadata-queue.js", () => ({
  metadataQueue: {
    addBatch: vi.fn(),
  },
}));

vi.mock("../../../src/utils/compression.js", () => ({
  compressForStorage: vi.fn().mockResolvedValue(Buffer.from([0x00, 0x01, 0x02])),
}));

vi.mock("../../../src/utils/sanitize.js", () => ({
  stripNullBytes: vi.fn((data: Uint8Array) => Buffer.from(data)),
}));

vi.mock("../../../src/constants.js", () => ({
  DEFAULT_PUBKEY: "11111111111111111111111111111111",
}));

import {
  BatchRpcFetcher,
  EventBuffer,
  BatchEvent,
} from "../../../src/indexer/batch-processor.js";
import { config } from "../../../src/config.js";
import { metadataQueue } from "../../../src/indexer/metadata-queue.js";

function createMockConnection() {
  return {
    getParsedTransactions: vi.fn().mockResolvedValue([]),
    getParsedTransaction: vi.fn().mockResolvedValue(null),
  } as any;
}

function createMockPool() {
  const mockClient = {
    query: vi.fn().mockResolvedValue({ rows: [], rowCount: 0 }),
    release: vi.fn(),
  };
  return {
    connect: vi.fn().mockResolvedValue(mockClient),
    query: vi.fn().mockResolvedValue({ rows: [], rowCount: 0 }),
    _client: mockClient,
  } as any;
}

function createMockPrisma() {
  return {
    $transaction: vi.fn().mockImplementation(async (fn: Function) => fn(mockPrismaInner())),
    indexerState: {
      upsert: vi.fn().mockResolvedValue({}),
    },
    feedback: {
      updateMany: vi.fn().mockResolvedValue({ count: 0 }),
    },
    feedbackResponse: {
      updateMany: vi.fn().mockResolvedValue({ count: 0 }),
    },
    revocation: {
      updateMany: vi.fn().mockResolvedValue({ count: 0 }),
    },
  } as any;
}

function mockPrismaInner() {
  return {
    indexerState: {
      upsert: vi.fn().mockResolvedValue({}),
    },
  };
}

function makeEvent(
  type: string,
  data: Record<string, any> = {},
  overrideCtx?: Partial<BatchEvent["ctx"]>
): BatchEvent {
  return {
    type,
    data,
    ctx: {
      signature: "test-sig-" + Math.random().toString(36).slice(2),
      slot: 12345n,
      blockTime: new Date("2024-01-15T10:00:00Z"),
      txIndex: 0,
      ...overrideCtx,
    },
  };
}

describe("BatchRpcFetcher", () => {
  let mockConnection: ReturnType<typeof createMockConnection>;

  beforeEach(() => {
    mockConnection = createMockConnection();
  });

  describe("fetchTransactions", () => {
    it("should return empty map for empty signatures", async () => {
      const fetcher = new BatchRpcFetcher(mockConnection);
      const result = await fetcher.fetchTransactions([]);
      expect(result.size).toBe(0);
    });

    it("should fetch batch of transactions", async () => {
      const tx = {
        slot: 12345,
        blockTime: 1234567890,
        transaction: { signatures: ["sig1"] },
      };
      mockConnection.getParsedTransactions.mockResolvedValue([tx]);

      const fetcher = new BatchRpcFetcher(mockConnection);
      const result = await fetcher.fetchTransactions(["sig1"]);

      expect(result.size).toBe(1);
      expect(result.get("sig1")).toBe(tx);
    });

    it("should handle null transactions in batch", async () => {
      mockConnection.getParsedTransactions.mockResolvedValue([null, { slot: 1 }]);

      const fetcher = new BatchRpcFetcher(mockConnection);
      const result = await fetcher.fetchTransactions(["sig1", "sig2"]);

      expect(result.size).toBe(1);
      expect(result.has("sig1")).toBe(false);
      expect(result.has("sig2")).toBe(true);
    });

    it("should split into chunks of 100", async () => {
      const sigs = Array.from({ length: 150 }, (_, i) => `sig${i}`);
      mockConnection.getParsedTransactions.mockResolvedValue(
        sigs.slice(0, 100).map((s) => ({ slot: 1, transaction: { signatures: [s] } }))
      );

      const fetcher = new BatchRpcFetcher(mockConnection);
      await fetcher.fetchTransactions(sigs);

      expect(mockConnection.getParsedTransactions).toHaveBeenCalledTimes(2);
    });

    it("should fall back to individual fetches on batch failure", async () => {
      mockConnection.getParsedTransactions.mockRejectedValue(new Error("batch fail"));
      const tx = { slot: 1, transaction: { signatures: ["sig1"] } };
      mockConnection.getParsedTransaction.mockResolvedValue(tx);

      const fetcher = new BatchRpcFetcher(mockConnection);
      const result = await fetcher.fetchTransactions(["sig1"]);

      expect(result.size).toBe(1);
      expect(mockConnection.getParsedTransaction).toHaveBeenCalledWith(
        "sig1",
        expect.any(Object)
      );
    });

    it("should handle individual fetch failures gracefully", async () => {
      mockConnection.getParsedTransactions.mockRejectedValue(new Error("batch fail"));
      mockConnection.getParsedTransaction.mockRejectedValue(new Error("individual fail"));

      const fetcher = new BatchRpcFetcher(mockConnection);
      const result = await fetcher.fetchTransactions(["sig1"]);

      expect(result.size).toBe(0);
    });

    it("should limit parallel chunks", async () => {
      const sigs = Array.from({ length: 500 }, (_, i) => `sig${i}`);
      mockConnection.getParsedTransactions.mockResolvedValue([]);

      const fetcher = new BatchRpcFetcher(mockConnection);
      await fetcher.fetchTransactions(sigs);

      // 500 sigs / 100 per chunk = 5 chunks, max 3 parallel
      expect(mockConnection.getParsedTransactions).toHaveBeenCalledTimes(5);
    });
  });

  describe("getStats", () => {
    it("should return accurate stats", async () => {
      const fetcher = new BatchRpcFetcher(mockConnection);
      mockConnection.getParsedTransactions.mockResolvedValue([]);

      await fetcher.fetchTransactions(["sig1"]);
      await fetcher.fetchTransactions(["sig2"]);

      const stats = fetcher.getStats();
      expect(stats.batchCount).toBe(2);
      expect(stats.avgTime).toBeGreaterThanOrEqual(0);
    });

    it("should return 0 avgTime with no batches", () => {
      const fetcher = new BatchRpcFetcher(mockConnection);
      const stats = fetcher.getStats();
      expect(stats.avgTime).toBe(0);
    });
  });
});

describe("EventBuffer", () => {
  let mockPool: ReturnType<typeof createMockPool>;
  let mockPrisma: ReturnType<typeof createMockPrisma>;

  beforeEach(() => {
    vi.useFakeTimers();
    mockPool = createMockPool();
    mockPrisma = createMockPrisma();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe("addEvent and flush", () => {
    it("should buffer events", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(makeEvent("AgentRegistered"));
      expect(buffer.size).toBe(1);
    });

    it("should auto-flush when buffer reaches BATCH_SIZE_DB", async () => {
      const buffer = new EventBuffer(mockPool, null);

      // Add 500 events (BATCH_SIZE_DB)
      for (let i = 0; i < 500; i++) {
        await buffer.addEvent(makeEvent("AgentRegistered", {
          asset: `asset${i}`,
          owner: `owner${i}`,
          collection: `coll${i}`,
        }));
      }

      // Should have flushed
      expect(buffer.size).toBe(0);
      expect(mockPool.connect).toHaveBeenCalled();
    });

    it("should auto-flush on timer", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(makeEvent("AgentRegistered"));

      // Advance timer past FLUSH_INTERVAL_MS (500ms)
      await vi.advanceTimersByTimeAsync(600);

      expect(buffer.size).toBe(0);
    });

    it("should not flush when buffer is empty", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.flush();
      expect(mockPool.connect).not.toHaveBeenCalled();
    });

    it("should not double-flush (flushInProgress guard)", async () => {
      const client = mockPool._client;
      let firstQueryCalled = false;
      const resolvers: Function[] = [];
      client.query.mockImplementation(() => {
        if (!firstQueryCalled) {
          firstQueryCalled = true;
          return new Promise((resolve) => {
            resolvers.push(() => resolve({ rows: [], rowCount: 0 }));
          });
        }
        return Promise.resolve({ rows: [], rowCount: 0 });
      });

      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(makeEvent("AgentRegistered"));

      // Start first flush (will block on BEGIN)
      const flush1 = buffer.flush();
      // Try second flush immediately - should return immediately (flushInProgress)
      await buffer.flush();

      // Resolve all blocked queries to let first flush complete
      resolvers.forEach(r => r());
      await flush1;
    });
  });

  describe("flushToSupabase", () => {
    it("should insert AgentRegistered event", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("AgentRegistered", {
          asset: "assetKey123456789012345678901234",
          owner: "ownerKey123456789012345678901234",
          collection: "collKey1234567890123456789012345",
          agentUri: "https://example.com/agent.json",
          atomEnabled: true,
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      // BEGIN, INSERT agents, cursor update, COMMIT
      expect(client.query).toHaveBeenCalledWith("BEGIN");
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should queue URI metadata on AgentRegistered with URI", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("AgentRegistered", {
          asset: "assetKey123456789012345678901234",
          owner: "ownerKey123456789012345678901234",
          collection: "collKey1234567890123456789012345",
          agentUri: "https://example.com/agent.json",
        })
      );
      await buffer.flush();

      expect(metadataQueue.addBatch).toHaveBeenCalledWith(
        expect.arrayContaining([
          expect.objectContaining({ uri: "https://example.com/agent.json" }),
        ])
      );
    });

    it("should queue URI metadata on UriUpdated", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("UriUpdated", {
          asset: "assetKey123456789012345678901234",
          updatedBy: "updaterKey1234567890123456789012",
          newUri: "https://example.com/updated.json",
        })
      );
      await buffer.flush();

      expect(metadataQueue.addBatch).toHaveBeenCalled();
    });

    it("should handle toBase58 on pubkey objects", async () => {
      const buffer = new EventBuffer(mockPool, null);
      const mockPubkey = { toBase58: () => "mockBase58Key" };
      await buffer.addEvent(
        makeEvent("AgentRegistered", {
          asset: mockPubkey,
          owner: mockPubkey,
          collection: mockPubkey,
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should insert NewFeedback event", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("NewFeedback", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 0n,
          value: 100n,
          valueDecimals: 0,
          score: 85,
          tag1: "quality",
          tag2: "speed",
          endpoint: "/api/v1",
          feedbackUri: "https://example.com/feedback",
          sealHash: new Uint8Array(32).fill(0xab),
          newFeedbackDigest: new Uint8Array(32).fill(0xcd),
          atomEnabled: true,
          newTrustTier: 2,
          newQualityScore: 8500,
          newConfidence: 7000,
          newRiskScore: 10,
          newDiversityRatio: 200,
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should insert NewFeedback without ATOM update", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("NewFeedback", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 0n,
          score: 85,
          sealHash: null,
          atomEnabled: false,
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should handle all-zero sealHash as null", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("NewFeedback", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 0n,
          sealHash: new Uint8Array(32).fill(0),
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should skip duplicate feedback (rowCount 0)", async () => {
      const client = mockPool._client;
      // First INSERT returns rowCount 0 (duplicate)
      client.query.mockResolvedValue({ rows: [], rowCount: 0 });

      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("NewFeedback", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 0n,
        })
      );
      await buffer.flush();

      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should insert FeedbackRevoked event", async () => {
      const client = mockPool._client;
      // Feedback exists
      client.query.mockImplementation((sql: string, params?: any[]) => {
        if (typeof sql === 'string' && sql.includes("SELECT id, feedback_hash FROM feedbacks")) {
          return { rows: [{ id: "f1", feedback_hash: "ab".repeat(32) }], rowCount: 1 };
        }
        return { rows: [], rowCount: 0 };
      });

      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("FeedbackRevoked", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 0n,
          sealHash: new Uint8Array(32).fill(0xab),
          newRevokeDigest: new Uint8Array(32).fill(0xcd),
          originalScore: 85,
          atomEnabled: true,
          hadImpact: true,
          newTrustTier: 1,
          newQualityScore: 7000,
          newConfidence: 5000,
          newRevokeCount: 1,
        })
      );
      await buffer.flush();

      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should handle revocation of non-existing feedback (orphan)", async () => {
      const client = mockPool._client;
      client.query.mockResolvedValue({ rows: [], rowCount: 0 });

      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("FeedbackRevoked", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 0n,
          sealHash: new Uint8Array(32).fill(0xab),
        })
      );
      await buffer.flush();

      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should warn on seal_hash mismatch during revocation", async () => {
      const client = mockPool._client;
      client.query.mockImplementation((sql: string) => {
        if (typeof sql === 'string' && sql.includes("SELECT id, feedback_hash FROM feedbacks")) {
          return { rows: [{ id: "f1", feedback_hash: "ff".repeat(32) }], rowCount: 1 };
        }
        return { rows: [], rowCount: 0 };
      });

      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("FeedbackRevoked", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 0n,
          sealHash: new Uint8Array(32).fill(0xab), // Different from stored
        })
      );
      await buffer.flush();

      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should insert ResponseAppended event", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("ResponseAppended", {
          asset: "asset1",
          client: "client1",
          responder: "responder1",
          feedbackIndex: 0n,
          responseUri: "https://example.com/response",
          responseHash: new Uint8Array(32).fill(0xef),
          newResponseDigest: new Uint8Array(32).fill(0xab),
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should handle response with all-zero hash", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("ResponseAppended", {
          asset: "asset1",
          client: "client1",
          responder: "responder1",
          feedbackIndex: 0n,
          responseHash: new Uint8Array(32).fill(0),
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should insert ValidationRequested event", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("ValidationRequested", {
          asset: "asset1",
          validatorAddress: "validator1",
          requester: "requester1",
          nonce: 1n,
          requestUri: "https://example.com/req",
          requestHash: new Uint8Array(32).fill(0xab),
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
      const validationInsert = client.query.mock.calls.find(
        (call: any[]) =>
          typeof call[0] === "string" && call[0].includes("INSERT INTO validations")
      );
      expect(validationInsert).toBeDefined();
    });

    it("should no-op ValidationRequested when validation indexing is disabled", async () => {
      const previous = (config as any).validationIndexEnabled;
      (config as any).validationIndexEnabled = false;

      try {
        const buffer = new EventBuffer(mockPool, null);
        await buffer.addEvent(
          makeEvent("ValidationRequested", {
            asset: "asset1",
            validatorAddress: "validator1",
            requester: "requester1",
            nonce: 2n,
            requestUri: "https://example.com/noop",
            requestHash: new Uint8Array(32).fill(0xab),
          })
        );
        await buffer.flush();

        const client = mockPool._client;
        const validationInsert = client.query.mock.calls.find(
          (call: any[]) =>
            typeof call[0] === "string" && call[0].includes("INSERT INTO validations")
        );
        expect(validationInsert).toBeUndefined();
      } finally {
        (config as any).validationIndexEnabled = previous;
      }
    });

    it("should handle ValidationResponded event", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("ValidationResponded", {
          asset: "asset1",
          validatorAddress: "validator1",
          nonce: 1n,
          response: 1,
          responseUri: "https://example.com/resp",
          responseHash: new Uint8Array(32).fill(0xab),
          tag: "valid",
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
      const validationUpsert = client.query.mock.calls.find(
        (call: any[]) =>
          typeof call[0] === "string" && call[0].includes("INSERT INTO validations")
      );
      expect(validationUpsert).toBeDefined();
    });

    it("should no-op ValidationResponded when validation indexing is disabled", async () => {
      const previous = (config as any).validationIndexEnabled;
      (config as any).validationIndexEnabled = false;

      try {
        const buffer = new EventBuffer(mockPool, null);
        await buffer.addEvent(
          makeEvent("ValidationResponded", {
            asset: "asset1",
            validatorAddress: "validator1",
            nonce: 3n,
            response: 1,
            responseUri: "https://example.com/noop-resp",
            responseHash: new Uint8Array(32).fill(0xab),
            tag: "noop",
          })
        );
        await buffer.flush();

        const client = mockPool._client;
        const validationUpsert = client.query.mock.calls.find(
          (call: any[]) =>
            typeof call[0] === "string" && call[0].includes("INSERT INTO validations")
        );
        expect(validationUpsert).toBeUndefined();
      } finally {
        (config as any).validationIndexEnabled = previous;
      }
    });

    it("should insert RegistryInitialized event", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("RegistryInitialized", {
          collection: "collection1",
          authority: "authority1",
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should handle UriUpdated event", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("UriUpdated", {
          asset: "asset1",
          newUri: "https://example.com/new",
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should handle WalletUpdated event", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("WalletUpdated", {
          asset: "asset1",
          newWallet: "newWallet1",
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should set wallet to null for default pubkey", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("WalletUpdated", {
          asset: "asset1",
          newWallet: "11111111111111111111111111111111",
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should handle AtomEnabled event", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("AtomEnabled", {
          asset: "asset1",
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should handle MetadataSet event", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("MetadataSet", {
          asset: "asset1",
          key: "capabilities",
          value: new Uint8Array([1, 2, 3]),
          immutable: false,
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should keep immutable guard in metadata upsert", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("MetadataSet", {
          asset: "asset1",
          key: "guarded",
          value: new Uint8Array([1, 2, 3]),
          immutable: false,
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      const metadataInsertCall = client.query.mock.calls.find(
        (call: any[]) =>
          typeof call[0] === "string" &&
          call[0].includes("INSERT INTO metadata") &&
          call[0].includes("WHERE NOT metadata.immutable")
      );
      expect(metadataInsertCall).toBeDefined();
    });

    it("should skip _uri: prefixed metadata keys", async () => {
      const client = mockPool._client;
      const queryCallsBefore = client.query.mock.calls.length;

      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("MetadataSet", {
          asset: "asset1",
          key: "_uri:name",
          value: new Uint8Array([1]),
        })
      );
      await buffer.flush();

      // Should still COMMIT but skip the INSERT for _uri: key
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should handle MetadataDeleted event", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("MetadataDeleted", {
          asset: "asset1",
          key: "capabilities",
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should handle AgentOwnerSynced event", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("AgentOwnerSynced", {
          asset: "asset1",
          newOwner: "newOwner1",
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should handle unrecognized event type", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(makeEvent("UnknownEventType", { foo: "bar" }));
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should update cursor with last event context", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("AgentRegistered", { asset: "a1", owner: "o1", collection: "c1" }, {
          signature: "last-sig",
          slot: 99999n,
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      // Check that cursor update query was called
      const cursorCall = client.query.mock.calls.find(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("indexer_state")
      );
      expect(cursorCall).toBeDefined();
    });

    it("should not queue metadata when metadataIndexMode is off", async () => {
      const { config } = await import("../../../src/config.js");
      (config as any).metadataIndexMode = "off";

      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("AgentRegistered", {
          asset: "a1",
          owner: "o1",
          collection: "c1",
          agentUri: "https://example.com",
        })
      );
      await buffer.flush();

      expect(metadataQueue.addBatch).not.toHaveBeenCalled();
      (config as any).metadataIndexMode = "normal";
    });

    it("should ROLLBACK on error", async () => {
      vi.useRealTimers();
      const client = mockPool._client;
      client.query.mockImplementation((sql: string) => {
        if (sql === "BEGIN") return { rows: [], rowCount: 0 };
        if (sql === "ROLLBACK") return { rows: [], rowCount: 0 };
        throw new Error("DB error");
      });

      const buffer = new EventBuffer(mockPool, null);
      buffer.addEvent(makeEvent("AgentRegistered"));

      await expect(buffer.flush()).rejects.toThrow("DB error");
      expect(client.query).toHaveBeenCalledWith("ROLLBACK");
      expect(client.release).toHaveBeenCalled();
      vi.useFakeTimers();
    });
  });

  describe("deterministic sequential IDs (batch path)", () => {
    it("assigns feedback_id sequentially per asset and backfills on conflict", async () => {
      const client = mockPool._client;
      const maxByAsset = new Map<string, bigint>();

      client.query.mockImplementation((sql: string, params?: any[]) => {
        if (sql === "BEGIN" || sql === "COMMIT" || sql === "ROLLBACK") {
          return { rows: [], rowCount: 0 };
        }
        if (typeof sql === "string" && sql.includes("SELECT feedback_id::text AS feedback_id FROM feedbacks WHERE id = $1 LIMIT 1")) {
          const id = params?.[0] as string;
          if (id === "asset1:client1:2") {
            return { rows: [{ feedback_id: null }], rowCount: 1 };
          }
          return { rows: [], rowCount: 0 };
        }
        if (typeof sql === "string" && sql.includes("SELECT MAX(feedback_id)::text AS max_id")) {
          const asset = params?.[0] as string;
          const currentMax = maxByAsset.get(asset) ?? 0n;
          return { rows: [{ max_id: currentMax === 0n ? null : currentMax.toString() }], rowCount: 1 };
        }
        if (typeof sql === "string" && sql.includes("INSERT INTO feedbacks (id, feedback_id")) {
          const asset = params?.[2] as string;
          const assignedId = BigInt(params?.[1] as string);
          const prev = maxByAsset.get(asset) ?? 0n;
          maxByAsset.set(asset, assignedId > prev ? assignedId : prev);
          if ((params?.[0] as string) === "asset1:client1:2") {
            return { rows: [], rowCount: 0 };
          }
          return { rows: [], rowCount: 1 };
        }
        return { rows: [], rowCount: 1 };
      });

      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("NewFeedback", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 0n,
        })
      );
      await buffer.addEvent(
        makeEvent("NewFeedback", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 1n,
        })
      );
      await buffer.addEvent(
        makeEvent("NewFeedback", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 2n,
        })
      );
      await buffer.flush();

      const feedbackInsertCalls = client.query.mock.calls.filter(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("INSERT INTO feedbacks (id, feedback_id")
      );
      expect(feedbackInsertCalls).toHaveLength(3);
      expect(feedbackInsertCalls[0][1][1]).toBe("1");
      expect(feedbackInsertCalls[1][1][1]).toBe("2");
      expect(feedbackInsertCalls[2][1][1]).toBe("3");

      const feedbackBackfillCall = client.query.mock.calls.find(
        (call: any[]) =>
          typeof call[0] === "string" &&
          call[0].includes("SET feedback_id = COALESCE(feedback_id, $2::bigint)")
      );
      expect(feedbackBackfillCall).toBeDefined();
      expect(feedbackBackfillCall?.[1]).toEqual(["asset1:client1:2", "3"]);
    });

    it("assigns revocation_id for non-orphans and keeps orphan revocations null", async () => {
      const client = mockPool._client;
      const maxByAsset = new Map<string, bigint>();

      client.query.mockImplementation((sql: string, params?: any[]) => {
        if (sql === "BEGIN" || sql === "COMMIT" || sql === "ROLLBACK") {
          return { rows: [], rowCount: 0 };
        }
        if (typeof sql === "string" && sql.includes("SELECT id, feedback_hash FROM feedbacks WHERE id = $1 LIMIT 1")) {
          const id = params?.[0] as string;
          if (id === "asset1:client1:0") {
            return { rows: [{ id, feedback_hash: "ab".repeat(32) }], rowCount: 1 };
          }
          return { rows: [], rowCount: 0 };
        }
        if (typeof sql === "string" && sql.includes("SELECT revocation_id::text AS revocation_id")) {
          return { rows: [], rowCount: 0 };
        }
        if (typeof sql === "string" && sql.includes("SELECT MAX(revocation_id)::text AS max_id")) {
          const asset = params?.[0] as string;
          const currentMax = maxByAsset.get(asset) ?? 0n;
          return { rows: [{ max_id: currentMax === 0n ? null : currentMax.toString() }], rowCount: 1 };
        }
        if (typeof sql === "string" && sql.includes("INSERT INTO revocations (id, revocation_id")) {
          const asset = params?.[2] as string;
          const assignedId = params?.[1];
          if (assignedId !== null) {
            const next = BigInt(assignedId as string);
            const prev = maxByAsset.get(asset) ?? 0n;
            maxByAsset.set(asset, next > prev ? next : prev);
          }
          return { rows: [], rowCount: 1 };
        }
        return { rows: [], rowCount: 1 };
      });

      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("FeedbackRevoked", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 0n,
          sealHash: new Uint8Array(32).fill(0xab),
          newRevokeCount: 1,
        })
      );
      await buffer.addEvent(
        makeEvent("FeedbackRevoked", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 1n,
          sealHash: new Uint8Array(32).fill(0xab),
          newRevokeCount: 2,
        })
      );
      await buffer.flush();

      const revocationInsertCalls = client.query.mock.calls.filter(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("INSERT INTO revocations (id, revocation_id")
      );
      expect(revocationInsertCalls).toHaveLength(2);
      expect(revocationInsertCalls[0][1][1]).toBe("1");
      expect(revocationInsertCalls[0][1][16]).toBe("PENDING");
      expect(revocationInsertCalls[1][1][1]).toBeNull();
      expect(revocationInsertCalls[1][1][16]).toBe("ORPHANED");
      expect(revocationInsertCalls[0][0]).toContain("revocation_id = CASE");
    });

    it("assigns response_id per feedback scope and keeps orphan/seal-mismatch IDs null", async () => {
      const client = mockPool._client;
      const maxByScope = new Map<string, bigint>();

      client.query.mockImplementation((sql: string, params?: any[]) => {
        if (sql === "BEGIN" || sql === "COMMIT" || sql === "ROLLBACK") {
          return { rows: [], rowCount: 0 };
        }
        if (typeof sql === "string" && sql.includes("SELECT id, feedback_hash FROM feedbacks WHERE asset = $1 AND client_address = $2 AND feedback_index = $3 LIMIT 1")) {
          const feedbackIndex = params?.[2] as string;
          if (feedbackIndex === "9") {
            return { rows: [], rowCount: 0 };
          }
          if (feedbackIndex === "10") {
            return { rows: [{ id: "fb:10", feedback_hash: "ff".repeat(32) }], rowCount: 1 };
          }
          return { rows: [{ id: "fb:ok", feedback_hash: "ab".repeat(32) }], rowCount: 1 };
        }
        if (typeof sql === "string" && sql.includes("SELECT response_id::text AS response_id FROM feedback_responses WHERE id = $1 LIMIT 1")) {
          return { rows: [], rowCount: 0 };
        }
        if (typeof sql === "string" && sql.includes("SELECT MAX(response_id)::text AS max_id")) {
          const scope = `${params?.[0]}:${params?.[1]}:${params?.[2]}`;
          const currentMax = maxByScope.get(scope) ?? 0n;
          return { rows: [{ max_id: currentMax === 0n ? null : currentMax.toString() }], rowCount: 1 };
        }
        if (typeof sql === "string" && sql.includes("INSERT INTO feedback_responses (id, response_id")) {
          const status = params?.[15];
          const scope = `${params?.[2]}:${params?.[3]}:${params?.[4]}`;
          const assignedId = params?.[1];
          if (assignedId !== null && status !== "ORPHANED") {
            const next = BigInt(assignedId as string);
            const prev = maxByScope.get(scope) ?? 0n;
            maxByScope.set(scope, next > prev ? next : prev);
          }
          return { rows: [], rowCount: 1 };
        }
        return { rows: [], rowCount: 1 };
      });

      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("ResponseAppended", {
          asset: "asset1",
          client: "client1",
          responder: "responder1",
          feedbackIndex: 0n,
          sealHash: new Uint8Array(32).fill(0xab),
          responseHash: new Uint8Array(32).fill(0xab),
        }, { signature: "sig-1" })
      );
      await buffer.addEvent(
        makeEvent("ResponseAppended", {
          asset: "asset1",
          client: "client1",
          responder: "responder2",
          feedbackIndex: 0n,
          sealHash: new Uint8Array(32).fill(0xab),
          responseHash: new Uint8Array(32).fill(0xab),
        }, { signature: "sig-2" })
      );
      await buffer.addEvent(
        makeEvent("ResponseAppended", {
          asset: "asset1",
          client: "client1",
          responder: "responder3",
          feedbackIndex: 1n,
          sealHash: new Uint8Array(32).fill(0xab),
          responseHash: new Uint8Array(32).fill(0xab),
        }, { signature: "sig-3" })
      );
      await buffer.addEvent(
        makeEvent("ResponseAppended", {
          asset: "asset1",
          client: "client1",
          responder: "responder4",
          feedbackIndex: 9n,
          sealHash: new Uint8Array(32).fill(0xab),
        }, { signature: "sig-4" })
      );
      await buffer.addEvent(
        makeEvent("ResponseAppended", {
          asset: "asset1",
          client: "client1",
          responder: "responder5",
          feedbackIndex: 10n,
          sealHash: new Uint8Array(32).fill(0xab),
        }, { signature: "sig-5" })
      );
      await buffer.flush();

      const responseInsertCalls = client.query.mock.calls.filter(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("INSERT INTO feedback_responses (id, response_id")
      );
      expect(responseInsertCalls).toHaveLength(5);

      expect(responseInsertCalls[0][1][1]).toBe("1");
      expect(responseInsertCalls[0][1][15]).toBe("PENDING");
      expect(responseInsertCalls[1][1][1]).toBe("2");
      expect(responseInsertCalls[1][1][15]).toBe("PENDING");
      expect(responseInsertCalls[2][1][1]).toBe("1");
      expect(responseInsertCalls[2][1][15]).toBe("PENDING");

      expect(responseInsertCalls[3][1][1]).toBeNull();
      expect(responseInsertCalls[3][1][15]).toBe("ORPHANED");
      expect(responseInsertCalls[3][0]).toContain("ON CONFLICT (id) DO NOTHING");

      expect(responseInsertCalls[4][1][1]).toBeNull();
      expect(responseInsertCalls[4][1][15]).toBe("ORPHANED");
      expect(responseInsertCalls[4][0]).toContain("response_id = CASE");
    });
  });

  describe("flushToPrisma", () => {
    it("should update cursor via prisma transaction", async () => {
      const { config } = await import("../../../src/config.js");
      (config as any).dbMode = "local";

      const buffer = new EventBuffer(null, mockPrisma);
      await buffer.addEvent(makeEvent("AgentRegistered"));
      await buffer.flush();

      expect(mockPrisma.$transaction).toHaveBeenCalled();
      (config as any).dbMode = "supabase";
    });

    it("should skip prisma flush when prisma is null", async () => {
      const { config } = await import("../../../src/config.js");
      (config as any).dbMode = "local";

      const buffer = new EventBuffer(null, null);
      await buffer.addEvent(makeEvent("AgentRegistered"));
      await buffer.flush(); // No error

      (config as any).dbMode = "supabase";
    });

    it("should skip supabase flush when pool is null", async () => {
      const buffer = new EventBuffer(null, null);
      await buffer.addEvent(makeEvent("AgentRegistered"));
      await buffer.flush(); // No error
    });
  });

  describe("retry and dead letter queue", () => {
    it("should retry on flush failure", async () => {
      vi.useRealTimers();
      const client = mockPool._client;
      let callCount = 0;
      client.query.mockImplementation((sql: string) => {
        if (sql === "BEGIN" || sql === "ROLLBACK" || sql === "COMMIT") {
          return { rows: [], rowCount: 0 };
        }
        callCount++;
        if (callCount <= 1) throw new Error("Transient error");
        return { rows: [], rowCount: 0 };
      });

      const buffer = new EventBuffer(mockPool, null);
      buffer.addEvent(makeEvent("AgentRegistered"));

      await expect(buffer.flush()).rejects.toThrow("Transient error");
      expect(buffer.size).toBeGreaterThan(0);
      vi.useFakeTimers();
    });

    it("should move to dead letter after max retries", async () => {
      vi.useRealTimers();
      const client = mockPool._client;
      client.query.mockImplementation((sql: string) => {
        if (sql === "BEGIN" || sql === "ROLLBACK") return { rows: [], rowCount: 0 };
        throw new Error("Persistent error");
      });

      const buffer = new EventBuffer(mockPool, null);
      buffer.addEvent(makeEvent("AgentRegistered"));

      for (let i = 0; i < 3; i++) {
        try { await buffer.flush(); } catch {}
      }

      const dlq = buffer.getDeadLetterQueue();
      expect(dlq.length).toBeGreaterThan(0);
      vi.useFakeTimers();
    });

    it("should clear dead letter queue", async () => {
      vi.useRealTimers();
      const client = mockPool._client;
      client.query.mockImplementation((sql: string) => {
        if (sql === "BEGIN" || sql === "ROLLBACK") return { rows: [], rowCount: 0 };
        throw new Error("error");
      });

      const buffer = new EventBuffer(mockPool, null);
      buffer.addEvent(makeEvent("AgentRegistered"));

      for (let i = 0; i < 3; i++) {
        try { await buffer.flush(); } catch {}
      }

      expect(buffer.getDeadLetterQueue().length).toBeGreaterThan(0);
      buffer.clearDeadLetterQueue();
      expect(buffer.getDeadLetterQueue().length).toBe(0);
      vi.useFakeTimers();
    });

    it("should warn on dead letter backpressure", async () => {
      vi.useRealTimers();
      const client = mockPool._client;
      client.query.mockImplementation((sql: string) => {
        if (sql === "BEGIN" || sql === "ROLLBACK") return { rows: [], rowCount: 0 };
        throw new Error("error");
      });

      const buffer = new EventBuffer(mockPool, null);

      for (let i = 0; i < 3; i++) {
        buffer.addEvent(makeEvent("AgentRegistered"));
        try { await buffer.flush(); } catch {}
      }

      client.query.mockResolvedValue({ rows: [], rowCount: 0 });
      buffer.addEvent(makeEvent("AgentRegistered"));
      vi.useFakeTimers();
    });
  });

  describe("getStats", () => {
    it("should return accurate buffer stats", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(makeEvent("AgentRegistered"));

      const stats = buffer.getStats();
      expect(stats.eventsBuffered).toBe(1);
      expect(stats.eventsFlushed).toBe(0);
    });

    it("should track flushed events", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(makeEvent("AgentRegistered"));
      await buffer.flush();

      const stats = buffer.getStats();
      expect(stats.eventsFlushed).toBe(1);
      expect(stats.flushCount).toBe(1);
    });

    it("should return 0 avgFlushTime with no flushes", () => {
      const buffer = new EventBuffer(mockPool, null);
      expect(buffer.getStats().avgFlushTime).toBe(0);
    });
  });

  describe("size", () => {
    it("should reflect buffer length", async () => {
      const buffer = new EventBuffer(mockPool, null);
      expect(buffer.size).toBe(0);
      await buffer.addEvent(makeEvent("AgentRegistered"));
      expect(buffer.size).toBe(1);
    });
  });

  describe("revocation with ATOM impact", () => {
    it("should update ATOM metrics on impactful revocation", async () => {
      const client = mockPool._client;
      client.query.mockImplementation((sql: string) => {
        if (typeof sql === 'string' && sql.includes("SELECT id, feedback_hash FROM feedbacks")) {
          return { rows: [{ id: "f1", feedback_hash: null }], rowCount: 1 };
        }
        return { rows: [], rowCount: 0 };
      });

      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("FeedbackRevoked", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 0n,
          atomEnabled: true,
          hadImpact: true,
          newTrustTier: 1,
          newQualityScore: 7000,
          newConfidence: 5000,
        })
      );
      await buffer.flush();

      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });
  });

  describe("dead letter retention", () => {
    it("should keep stale dead letter entries (diagnostic history is append-only)", async () => {
      vi.useRealTimers();

      const buffer = new EventBuffer(mockPool, null);

      // Directly inject stale dead letter entries.
      const dlq = (buffer as any).deadLetterQueue;
      const staleTime = Date.now() - 6 * 60 * 1000; // 6 minutes ago
      dlq.push(
        { event: makeEvent("AgentRegistered"), addedAt: staleTime },
        { event: makeEvent("AgentRegistered"), addedAt: staleTime },
      );

      expect(buffer.getDeadLetterQueue().length).toBe(2);

      // Adding a new event should not evict stale diagnostic entries.
      const client = mockPool._client;
      client.query.mockResolvedValue({ rows: [], rowCount: 0 });
      await buffer.addEvent(makeEvent("AgentRegistered"));

      expect(buffer.getDeadLetterQueue().length).toBe(2);

      // Flush remaining to clean up
      await buffer.flush();
      vi.useFakeTimers();
    });
  });

  describe("backpressure warning", () => {
    it("should warn when DLQ exceeds 80% capacity", async () => {
      vi.useRealTimers();
      const { createChildLogger } = await import("../../../src/logger.js");
      const mockLogger = (createChildLogger as any)();
      mockLogger.warn.mockClear();

      const buffer = new EventBuffer(mockPool, null);

      // Fill the DLQ to >80% capacity (MAX_DEAD_LETTER = 10000, threshold = 8000)
      const dlq = (buffer as any).deadLetterQueue;
      for (let i = 0; i < 8500; i++) {
        dlq.push({ event: makeEvent("AgentRegistered"), addedAt: Date.now() });
      }

      // Adding new event triggers the backpressure check
      const client = mockPool._client;
      client.query.mockResolvedValue({ rows: [], rowCount: 0 });
      await buffer.addEvent(makeEvent("AgentRegistered"));

      expect(mockLogger.warn).toHaveBeenCalledWith(
        expect.objectContaining({
          deadLetterSize: expect.any(Number),
          maxCapacity: expect.any(Number),
          utilization: expect.any(String),
        }),
        expect.stringContaining("backpressure")
      );

      // Flush remaining to clean up
      await buffer.flush();
      vi.useFakeTimers();
    });
  });

  describe("DLQ overflow scenarios", () => {
    it("should throw and re-buffer events when DLQ is completely full", async () => {
      vi.useRealTimers();
      const client = mockPool._client;
      client.query.mockImplementation((sql: string) => {
        if (sql === "BEGIN" || sql === "ROLLBACK") return { rows: [], rowCount: 0 };
        throw new Error("Persistent error");
      });

      const buffer = new EventBuffer(mockPool, null);

      // Pre-fill DLQ to exactly MAX_DEAD_LETTER (10000)
      const dlq = (buffer as any).deadLetterQueue;
      for (let i = 0; i < 10000; i++) {
        dlq.push({ event: makeEvent("AgentRegistered"), addedAt: Date.now() });
      }

      // Force retryCount to MAX_FLUSH_RETRIES - 1 so next failure hits DLQ path
      (buffer as any).retryCount = 2;

      // Add event and flush - should throw because DLQ is full
      buffer.addEvent(makeEvent("AgentRegistered"));
      await expect(buffer.flush()).rejects.toThrow("Persistent error");

      // Events should be re-buffered
      expect(buffer.size).toBeGreaterThan(0);
      vi.useFakeTimers();
    });

    it("should partially dead-letter when DLQ has limited space", async () => {
      vi.useRealTimers();
      const client = mockPool._client;
      client.query.mockImplementation((sql: string) => {
        if (sql === "BEGIN" || sql === "ROLLBACK") return { rows: [], rowCount: 0 };
        throw new Error("Persistent error");
      });

      const buffer = new EventBuffer(mockPool, null);

      // Pre-fill DLQ to leave only 2 slots
      const prefillCount = 9998;
      for (let i = 0; i < prefillCount; i++) {
        (buffer as any).deadLetterQueue.push({ event: makeEvent("AgentRegistered"), addedAt: Date.now() });
      }
      expect((buffer as any).deadLetterQueue.length).toBe(prefillCount);

      // Force retryCount to MAX_FLUSH_RETRIES - 1 so next failure hits DLQ path
      (buffer as any).retryCount = 2;

      // Add 5 events directly to buffer.
      const eventsToAdd = 5;
      for (let i = 0; i < eventsToAdd; i++) {
        (buffer as any).buffer.push(makeEvent("AgentRegistered", { asset: `asset${i}` }));
      }

      // Flush once - retryCount=3 >= MAX, enters fail-stop path with bounded DLQ copy.
      await expect(buffer.flush()).rejects.toThrow("Persistent error");

      // DLQ should be at 10000 (9998 + 2 that fit)
      expect((buffer as any).deadLetterQueue.length).toBe(10000);
      // All source events remain buffered for retry.
      expect(buffer.size).toBe(eventsToAdd);
      vi.useFakeTimers();
    });
  });

  describe("hashesMatchHex edge cases", () => {
    it("should handle one empty and one non-empty hash (mismatch)", async () => {
      const client = mockPool._client;
      client.query.mockImplementation((sql: string) => {
        if (typeof sql === 'string' && sql.includes("SELECT id, feedback_hash FROM feedbacks")) {
          return { rows: [{ id: "f1", feedback_hash: "ab".repeat(32) }], rowCount: 1 };
        }
        return { rows: [], rowCount: 0 };
      });

      const buffer = new EventBuffer(mockPool, null);
      // Revocation with all-zero sealHash (treated as null/empty) but stored hash is non-empty
      await buffer.addEvent(
        makeEvent("FeedbackRevoked", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 0n,
          sealHash: new Uint8Array(32).fill(0), // all zeros = empty
        })
      );
      await buffer.flush();

      const { createChildLogger } = await import("../../../src/logger.js");
      const mockLogger = (createChildLogger as any)();
      expect(mockLogger.warn).toHaveBeenCalledWith(
        expect.objectContaining({ asset: "asset1" }),
        expect.stringContaining("seal_hash mismatch")
      );
    });

    it("should match when both stored and event hashes are empty/zero", async () => {
      const client = mockPool._client;
      client.query.mockImplementation((sql: string) => {
        if (typeof sql === 'string' && sql.includes("SELECT id, feedback_hash FROM feedbacks")) {
          return { rows: [{ id: "f1", feedback_hash: null }], rowCount: 1 };
        }
        return { rows: [], rowCount: 0 };
      });

      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("FeedbackRevoked", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 0n,
          sealHash: new Uint8Array(32).fill(0),
        })
      );
      await buffer.flush();

      // No mismatch warning
      const { createChildLogger } = await import("../../../src/logger.js");
      const mockLogger = (createChildLogger as any)();
      const mismatchCalls = mockLogger.warn.mock.calls.filter(
        (call: any[]) => typeof call[1] === 'string' && call[1].includes("seal_hash mismatch")
      );
      expect(mismatchCalls.length).toBe(0);
    });
  });

  describe("feedback with rowCount tracking", () => {
    it("should update agent stats with ATOM when rowCount > 0 and atomEnabled", async () => {
      const client = mockPool._client;
      let insertCall = 0;
      client.query.mockImplementation((sql: string) => {
        if (typeof sql === 'string' && sql.includes("INSERT INTO feedbacks")) {
          return { rows: [], rowCount: 1 }; // successful insert
        }
        return { rows: [], rowCount: 0 };
      });

      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("NewFeedback", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 0n,
          score: 90,
          sealHash: new Uint8Array(32).fill(0xab),
          atomEnabled: true,
          newTrustTier: 3,
          newQualityScore: 9000,
          newConfidence: 8000,
          newRiskScore: 5,
          newDiversityRatio: 180,
        })
      );
      await buffer.flush();

      // Should have called UPDATE agents with ATOM metrics
      const updateCall = client.query.mock.calls.find(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("trust_tier")
      );
      expect(updateCall).toBeDefined();
    });

    it("should update agent stats without ATOM when atomEnabled is false", async () => {
      const client = mockPool._client;
      client.query.mockImplementation((sql: string) => {
        if (typeof sql === 'string' && sql.includes("INSERT INTO feedbacks")) {
          return { rows: [], rowCount: 1 }; // successful insert
        }
        return { rows: [], rowCount: 0 };
      });

      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("NewFeedback", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 0n,
          score: 90,
          sealHash: new Uint8Array(32).fill(0xab),
          atomEnabled: false,
        })
      );
      await buffer.flush();

      // Should have UPDATE agents without ATOM fields
      const updateCall = client.query.mock.calls.find(
        (call: any[]) => typeof call[0] === "string" &&
          call[0].includes("UPDATE agents SET") &&
          !call[0].includes("trust_tier")
      );
      expect(updateCall).toBeDefined();
    });
  });

  describe("response with toBase58 on pubkey objects", () => {
    it("should handle toBase58 on ResponseAppended fields", async () => {
      const buffer = new EventBuffer(mockPool, null);
      const mockPubkey = (val: string) => ({ toBase58: () => val });
      await buffer.addEvent(
        makeEvent("ResponseAppended", {
          asset: mockPubkey("assetBase58"),
          client: mockPubkey("clientBase58"),
          responder: mockPubkey("responderBase58"),
          feedbackIndex: 5n,
          responseUri: "https://example.com/response",
          responseHash: new Uint8Array(32).fill(0xef),
          newResponseDigest: new Uint8Array(32).fill(0xab),
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });
  });

  describe("validation with toBase58 on pubkey objects", () => {
    it("should handle toBase58 on ValidationRequested fields", async () => {
      const buffer = new EventBuffer(mockPool, null);
      const mockPubkey = (val: string) => ({ toBase58: () => val });
      await buffer.addEvent(
        makeEvent("ValidationRequested", {
          asset: mockPubkey("assetBase58"),
          validatorAddress: mockPubkey("validatorBase58"),
          requester: mockPubkey("requesterBase58"),
          nonce: 1n,
          requestUri: "https://example.com/req",
          requestHash: new Uint8Array(32).fill(0xab),
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should handle toBase58 on ValidationResponded fields", async () => {
      const buffer = new EventBuffer(mockPool, null);
      const mockPubkey = (val: string) => ({ toBase58: () => val });
      await buffer.addEvent(
        makeEvent("ValidationResponded", {
          asset: mockPubkey("assetBase58"),
          validatorAddress: mockPubkey("validatorBase58"),
          nonce: 1n,
          response: 1,
          responseUri: "https://example.com/resp",
          responseHash: new Uint8Array(32).fill(0xab),
          tag: "valid",
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });
  });

  describe("revocation with toBase58 on pubkey objects", () => {
    it("should handle toBase58 on FeedbackRevoked fields", async () => {
      const client = mockPool._client;
      client.query.mockResolvedValue({ rows: [], rowCount: 0 });

      const buffer = new EventBuffer(mockPool, null);
      const mockPubkey = (val: string) => ({ toBase58: () => val });
      await buffer.addEvent(
        makeEvent("FeedbackRevoked", {
          asset: mockPubkey("assetBase58"),
          clientAddress: mockPubkey("clientBase58"),
          feedbackIndex: 0n,
          sealHash: new Uint8Array(32).fill(0xab),
        })
      );
      await buffer.flush();

      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });
  });

  describe("collection and owner events with toBase58", () => {
    it("should handle toBase58 on RegistryInitialized fields", async () => {
      const buffer = new EventBuffer(mockPool, null);
      const mockPubkey = (val: string) => ({ toBase58: () => val });
      await buffer.addEvent(
        makeEvent("RegistryInitialized", {
          collection: mockPubkey("collBase58"),
          authority: mockPubkey("authBase58"),
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should handle toBase58 on UriUpdated fields", async () => {
      const buffer = new EventBuffer(mockPool, null);
      const mockPubkey = (val: string) => ({ toBase58: () => val });
      await buffer.addEvent(
        makeEvent("UriUpdated", {
          asset: mockPubkey("assetBase58"),
          newUri: "https://example.com/updated.json",
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should handle toBase58 on WalletUpdated fields", async () => {
      const buffer = new EventBuffer(mockPool, null);
      const mockPubkey = (val: string) => ({ toBase58: () => val });
      await buffer.addEvent(
        makeEvent("WalletUpdated", {
          asset: mockPubkey("assetBase58"),
          newWallet: mockPubkey("walletBase58"),
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should handle toBase58 on AtomEnabled fields", async () => {
      const buffer = new EventBuffer(mockPool, null);
      const mockPubkey = (val: string) => ({ toBase58: () => val });
      await buffer.addEvent(
        makeEvent("AtomEnabled", {
          asset: mockPubkey("assetBase58"),
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should handle toBase58 on AgentOwnerSynced fields", async () => {
      const buffer = new EventBuffer(mockPool, null);
      const mockPubkey = (val: string) => ({ toBase58: () => val });
      await buffer.addEvent(
        makeEvent("AgentOwnerSynced", {
          asset: mockPubkey("assetBase58"),
          newOwner: mockPubkey("ownerBase58"),
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });
  });
});
