import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Mock dependencies before imports
vi.mock("../../../src/config.js", () => ({
  config: {
    dbMode: "supabase",
    metadataIndexMode: "normal",
    validationIndexEnabled: true,
    maxSupportedTransactionVersion: 0,
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
  const defaultQuery = vi.fn().mockImplementation((sql: string) => {
    if (typeof sql === "string" && sql.includes("SELECT 1 FROM agents WHERE asset")) {
      return Promise.resolve({ rows: [{ exists: 1 }], rowCount: 1 });
    }
    return Promise.resolve({ rows: [], rowCount: 0 });
  });
  const mockClient = {
    query: defaultQuery,
    release: vi.fn(),
  };
  return {
    connect: vi.fn().mockResolvedValue(mockClient),
    query: defaultQuery,
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

    it("should refetch null transactions in batch individually", async () => {
      const tx1 = { slot: 1, transaction: { signatures: ["sig1"] } };
      const tx2 = { slot: 1, transaction: { signatures: ["sig2"] } };
      mockConnection.getParsedTransactions.mockResolvedValue([null, tx2]);
      mockConnection.getParsedTransaction.mockResolvedValue(tx1);

      const fetcher = new BatchRpcFetcher(mockConnection);
      const result = await fetcher.fetchTransactions(["sig1", "sig2"]);

      expect(result.size).toBe(2);
      expect(result.get("sig1")).toBe(tx1);
      expect(result.has("sig2")).toBe(true);
      expect(mockConnection.getParsedTransaction).toHaveBeenCalledWith("sig1", {
        maxSupportedTransactionVersion: 0,
      });
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

    it("should allow smaller configured RPC chunks", async () => {
      const sigs = Array.from({ length: 120 }, (_, i) => `sig${i}`);
      mockConnection.getParsedTransactions.mockResolvedValue([]);

      const fetcher = new BatchRpcFetcher(mockConnection, { chunkSize: 40 });
      await fetcher.fetchTransactions(sigs);

      expect(mockConnection.getParsedTransactions).toHaveBeenCalledTimes(3);
      expect(mockConnection.getParsedTransactions.mock.calls[0][0]).toHaveLength(40);
      expect(mockConnection.getParsedTransactions.mock.calls[1][0]).toHaveLength(40);
      expect(mockConnection.getParsedTransactions.mock.calls[2][0]).toHaveLength(40);
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

    it("should allow lower parallel chunk concurrency", async () => {
      const sigs = Array.from({ length: 120 }, (_, i) => `sig${i}`);
      let inFlight = 0;
      let maxInFlight = 0;

      mockConnection.getParsedTransactions.mockImplementation(async () => {
        inFlight++;
        maxInFlight = Math.max(maxInFlight, inFlight);
        await Promise.resolve();
        await Promise.resolve();
        inFlight--;
        return [];
      });

      const fetcher = new BatchRpcFetcher(mockConnection, {
        chunkSize: 40,
        maxParallelChunks: 1,
      });
      await fetcher.fetchTransactions(sigs);

      expect(mockConnection.getParsedTransactions).toHaveBeenCalledTimes(3);
      expect(maxInFlight).toBe(1);
    });

    it("should reduce chunk size and retry subchunks on repeated 429s", async () => {
      const sigs = ["sig1", "sig2", "sig3", "sig4"];
      const txs = new Map(
        sigs.map((sig) => [sig, { slot: 1, transaction: { signatures: [sig] } }])
      );

      mockConnection.getParsedTransactions.mockImplementation(async (chunk: string[]) => {
        if (chunk.length === 4) {
          throw new Error("429 Too Many Requests");
        }
        return chunk.map((sig) => txs.get(sig));
      });

      const fetcher = new BatchRpcFetcher(mockConnection, {
        chunkSize: 4,
        maxParallelChunks: 1,
      });
      const result = await fetcher.fetchTransactions(sigs);

      expect(result.size).toBe(4);
      expect(Array.from(result.keys())).toEqual(sigs);
      expect(mockConnection.getParsedTransactions).toHaveBeenCalledTimes(3);
      expect(mockConnection.getParsedTransactions.mock.calls[0][0]).toHaveLength(4);
      expect(mockConnection.getParsedTransactions.mock.calls[1][0]).toHaveLength(2);
      expect(mockConnection.getParsedTransactions.mock.calls[2][0]).toHaveLength(2);

      await fetcher.fetchTransactions(sigs);
      expect(mockConnection.getParsedTransactions.mock.calls[3][0]).toHaveLength(2);
      expect(mockConnection.getParsedTransactions.mock.calls[4][0]).toHaveLength(2);
      expect(fetcher.getStats().chunkSize).toBe(2);
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

    it("should flush cursor-only progress when no events were buffered", async () => {
      const buffer = new EventBuffer(mockPool, null);
      buffer.noteCursor({
        signature: "cursor-only-sig",
        slot: 777n,
        blockTime: new Date("2026-03-06T12:00:00.000Z"),
        txIndex: 4,
      });

      await buffer.flush();

      const client = mockPool._client;
      const cursorCall = client.query.mock.calls.find(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("indexer_state")
      );
      expect(cursorCall).toBeDefined();
      expect(cursorCall[1]).toEqual([
        "cursor-only-sig",
        "777",
        4,
        "2026-03-06T12:00:00.000Z",
      ]);
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
      // Try second flush immediately - should join the in-flight flush
      const flush2 = buffer.flush();
      await Promise.resolve();

      // Resolve all blocked queries to let first flush complete
      resolvers.forEach(r => r());
      await Promise.all([flush1, flush2]);
      expect(client.query.mock.calls.filter((call: any[]) => call[0] === "BEGIN")).toHaveLength(1);
      expect(client.query.mock.calls.filter((call: any[]) => call[0] === "COMMIT")).toHaveLength(1);
    });

    it("should drain tail events added while a flush is already in progress", async () => {
      const client = mockPool._client;
      let beginCalls = 0;
      let releaseFirstBegin!: () => void;

      client.query.mockImplementation((sql: string) => {
        if (sql === "BEGIN") {
          beginCalls++;
          if (beginCalls === 1) {
            return new Promise((resolve) => {
              releaseFirstBegin = () => resolve({ rows: [], rowCount: 0 });
            });
          }
        }
        return Promise.resolve({ rows: [], rowCount: 0 });
      });

      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(makeEvent("AgentRegistered", { asset: "asset-a", owner: "owner-a", collection: "col-a" }));

      const firstFlush = buffer.flush();
      await Promise.resolve();

      await buffer.addEvent(makeEvent("AgentRegistered", { asset: "asset-b", owner: "owner-b", collection: "col-b" }));
      const drainPromise = buffer.drain();

      releaseFirstBegin();
      await firstFlush;
      await drainPromise;

      expect(buffer.size).toBe(0);
      expect(client.query.mock.calls.filter((call: any[]) => call[0] === "BEGIN")).toHaveLength(2);
      expect(client.query.mock.calls.filter((call: any[]) => call[0] === "COMMIT")).toHaveLength(2);
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
      const insertCall = client.query.mock.calls.find((c: any[]) =>
        typeof c[0] === "string" && c[0].includes("INSERT INTO agents")
      );
      expect(insertCall).toBeDefined();
      expect(insertCall![0]).toContain("created_at, updated_at, status");
      expect(insertCall![0]).toContain("updated_at = EXCLUDED.updated_at");
      expect(insertCall![0]).not.toContain("block_slot = EXCLUDED.block_slot");

      const collectionCallIndex = client.query.mock.calls.findIndex((c: any[]) =>
        typeof c[0] === "string" && c[0].includes("INSERT INTO collections")
      );
      const agentCallIndex = client.query.mock.calls.findIndex((c: any[]) =>
        typeof c[0] === "string" && c[0].includes("INSERT INTO agents")
      );
      expect(collectionCallIndex).toBeGreaterThanOrEqual(0);
      expect(agentCallIndex).toBeGreaterThan(collectionCallIndex);
    });

    it("should use monotonic CollectionPointerSet SQL with tx_index in batch mode", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("CollectionPointerSet", {
          asset: "assetKey123456789012345678901234",
          setBy: "ownerKey123456789012345678901234",
          col: "c1:test-pointer",
          lock: true,
        }, {
          txIndex: 7,
        })
      );

      await buffer.flush();

      const client = mockPool._client;
      const updateCall = client.query.mock.calls.find((c: any[]) =>
        typeof c[0] === "string" && c[0].includes("INSERT INTO collection_pointers")
      );
      expect(updateCall).toBeDefined();
      expect(updateCall![0]).toContain("last_seen_tx_index");
      expect(updateCall![0]).toContain("WHERE EXISTS (SELECT 1 FROM updated)");
      expect(updateCall![0]).toContain("COALESCE($7, -1) > COALESCE(cp.last_seen_tx_index, -1)");
      expect(updateCall![1][6]).toBe(7);
    });

    it("replays orphan response and revocation chains when agent registration replays orphan feedback", async () => {
      const client = mockPool._client;
      const feedbackHash = "ab".repeat(32);
      client.query.mockImplementation((sql: string) => {
        if (sql === "BEGIN" || sql === "COMMIT" || sql === "ROLLBACK") return { rows: [], rowCount: 0 };
        if (typeof sql === "string" && sql.includes("FROM orphan_feedbacks")) {
          return {
            rows: [{
              id: "ofb-1",
              asset: "assetKey123456789012345678901234",
              client_address: "client1",
              feedback_index: "0",
              value: "1000",
              value_decimals: 2,
              score: 80,
              tag1: "quality",
              tag2: "speed",
              endpoint: "/chat",
              feedback_uri: "ipfs://feedback",
              feedback_hash: feedbackHash,
              running_digest: Buffer.from(new Uint8Array(32).fill(0xab)),
              atom_enabled: false,
              new_trust_tier: 0,
              new_quality_score: 0,
              new_confidence: 0,
              new_risk_score: 0,
              new_diversity_ratio: 0,
              block_slot: "100",
              tx_index: 2,
              event_ordinal: 0,
              tx_signature: "feedback-sig",
              created_at: new Date("2026-03-06T00:00:00.000Z").toISOString(),
            }],
            rowCount: 1,
          };
        }
        if (typeof sql === "string" && sql.includes("INSERT INTO feedbacks")) {
          return { rows: [], rowCount: 1 };
        }
        if (typeof sql === "string" && sql.includes("UPDATE agents SET")) {
          return { rows: [], rowCount: 1 };
        }
        if (typeof sql === "string" && sql.includes("FROM orphan_responses")) {
          return {
            rows: [{
              id: "orphan-response-1",
              asset: "assetKey123456789012345678901234",
              client_address: "client1",
              feedback_index: "0",
              responder: "responder1",
              response_uri: "ipfs://resp",
              response_hash: feedbackHash,
              seal_hash: feedbackHash,
              running_digest: Buffer.from(new Uint8Array(32).fill(0xab)),
              response_count: "1",
              block_slot: "101",
              tx_index: 3,
              event_ordinal: 0,
              tx_signature: "response-sig",
              created_at: new Date("2026-03-06T00:00:01.000Z").toISOString(),
            }],
            rowCount: 1,
          };
        }
        if (typeof sql === "string" && sql.includes("INSERT INTO feedback_responses (id, response_id")) {
          return { rows: [], rowCount: 1 };
        }
        if (typeof sql === "string" && sql.includes("DELETE FROM orphan_responses")) {
          return { rows: [], rowCount: 1 };
        }
        if (typeof sql === "string" && sql.includes("FROM revocations")) {
          return { rows: [{ feedback_hash: feedbackHash, status: "ORPHANED" }], rowCount: 1 };
        }
        if (typeof sql === "string" && sql.includes("UPDATE revocations")) {
          return { rows: [], rowCount: 1 };
        }
        return { rows: [], rowCount: 1 };
      });

      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("AgentRegistered", {
          asset: "assetKey123456789012345678901234",
          owner: "ownerKey123456789012345678901234",
          collection: "collKey1234567890123456789012345",
          agentUri: "",
          atomEnabled: false,
        })
      );
      await buffer.flush();

      expect(client.query.mock.calls.some((call: any[]) =>
        typeof call[0] === "string" && call[0].includes("INSERT INTO feedback_responses (id, response_id")
      )).toBe(true);
      expect(client.query.mock.calls.some((call: any[]) =>
        typeof call[0] === "string" && call[0].includes("UPDATE revocations")
      )).toBe(true);
      expect(client.query.mock.calls.some((call: any[]) =>
        typeof call[0] === "string" && call[0].includes("DELETE FROM orphan_feedbacks")
      )).toBe(true);
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
          expect.objectContaining({
            uri: "https://example.com/agent.json",
            verifiedAt: expect.any(String),
          }),
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

    it("should preserve empty feedbackUri as empty string (not null)", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("NewFeedback", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 0n,
          value: 100n,
          valueDecimals: 0,
          score: 85,
          feedbackUri: "",
          sealHash: new Uint8Array(32).fill(0xab),
          atomEnabled: false,
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      const insertCall = client.query.mock.calls.find(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("INSERT INTO feedbacks (id, feedback_id")
      );
      expect(insertCall).toBeDefined();
      expect(insertCall[1][11]).toBe("");
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

    it("should preserve all-zero sealHash bytes for feedback inserts", async () => {
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
      const insertCall = client.query.mock.calls.find(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("INSERT INTO feedbacks (id, feedback_id")
      );
      expect(insertCall).toBeDefined();
      expect(insertCall[1][12]).toBe("0".repeat(64));
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

    it("should classify revocations correctly when rowCount is null", async () => {
      const client = mockPool._client;
      client.query.mockImplementation((sql: string, params?: any[]) => {
        if (typeof sql === "string" && sql.includes("SELECT id, feedback_hash FROM feedbacks")) {
          const id = params?.[0] as string;
          if (id === "asset1:client1:0") {
            return { rows: [{ id, feedback_hash: "ab".repeat(32) }], rowCount: null };
          }
          return { rows: [], rowCount: null };
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
      expect(revocationInsertCalls[0][1][16]).toBe("PENDING");
      expect(revocationInsertCalls[1][1][16]).toBe("ORPHANED");
    });

    it("should skip revoke side-effects when revocation upsert loses the ordering conflict", async () => {
      const client = mockPool._client;
      client.query.mockImplementation((sql: string) => {
        if (typeof sql === "string" && sql.includes("SELECT id, feedback_hash FROM feedbacks")) {
          return { rows: [{ id: "fb1", feedback_hash: "ab".repeat(32) }], rowCount: 1 };
        }
        if (typeof sql === "string" && sql.includes("INSERT INTO revocations")) {
          return { rows: [], rowCount: 0 };
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
      await buffer.flush();

      const feedbackSideEffects = client.query.mock.calls.filter(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("UPDATE feedbacks SET is_revoked")
      );
      const agentSideEffects = client.query.mock.calls.filter(
        (call: any[]) =>
          typeof call[0] === "string"
          && call[0].includes("UPDATE agents SET")
          && call[0].includes("feedback_count = COALESCE")
      );
      expect(feedbackSideEffects).toHaveLength(0);
      expect(agentSideEffects).toHaveLength(0);
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

    it("should preserve all-zero sealHash bytes for revocation inserts", async () => {
      const client = mockPool._client;
      client.query.mockImplementation((sql: string) => {
        if (typeof sql === "string" && sql.includes("SELECT id, feedback_hash FROM feedbacks")) {
          return { rows: [{ id: "f1", feedback_hash: "0".repeat(64) }], rowCount: 1 };
        }
        return { rows: [], rowCount: 1 };
      });

      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("FeedbackRevoked", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 0n,
          sealHash: new Uint8Array(32).fill(0),
          newRevokeCount: 1,
        })
      );
      await buffer.flush();

      const insertCall = client.query.mock.calls.find(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("INSERT INTO revocations (id, revocation_id")
      );
      expect(insertCall).toBeDefined();
      expect(insertCall[1][5]).toBe("0".repeat(64));
      expect(insertCall[1][16]).toBe("PENDING");
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

    it("should preserve all-zero responseHash bytes for orphan response inserts", async () => {
      const buffer = new EventBuffer(mockPool, null);
      await buffer.addEvent(
        makeEvent("ResponseAppended", {
          asset: "asset1",
          client: "client1",
          responder: "responder1",
          feedbackIndex: 0n,
          responseHash: new Uint8Array(32).fill(0),
          sealHash: new Uint8Array(32).fill(0),
        })
      );
      await buffer.flush();

      const client = mockPool._client;
      const insertCall = client.query.mock.calls.find(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("INSERT INTO orphan_responses")
      );
      expect(insertCall).toBeDefined();
      expect(insertCall[1][6]).toBe("0".repeat(64));
      expect(insertCall[1][7]).toBe("0".repeat(64));
      expect(client.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should preserve all-zero responseHash and sealHash bytes for response inserts", async () => {
      const client = mockPool._client;
      client.query.mockImplementation((sql: string) => {
        if (typeof sql === "string" && sql.includes("SELECT id, feedback_hash FROM feedbacks")) {
          return { rows: [{ id: "f1", feedback_hash: "0".repeat(64) }], rowCount: 1 };
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
          responseUri: "https://example.com/zero-response",
          responseHash: new Uint8Array(32).fill(0),
          sealHash: new Uint8Array(32).fill(0),
          newResponseCount: 1n,
        })
      );
      await buffer.flush();

      const insertCall = client.query.mock.calls.find(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("INSERT INTO feedback_responses (id, response_id")
      );
      expect(insertCall).toBeDefined();
      expect(insertCall[1][7]).toBe("0".repeat(64));
      expect(insertCall[1][15]).toBe("PENDING");
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
      const insertCall = client.query.mock.calls.find((call: any[]) =>
        typeof call[0] === "string" && call[0].includes("INSERT INTO collections")
      );
      expect(insertCall).toBeDefined();
      expect(insertCall![0]).toContain("authority = EXCLUDED.authority");
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
      expect(metadataInsertCall[0]).toContain("created_at, updated_at, status");
      expect(metadataInsertCall[0]).toContain("updated_at = EXCLUDED.updated_at");
      expect(metadataInsertCall[0]).toContain("tx_signature = EXCLUDED.tx_signature");
      expect(metadataInsertCall[1][10]).toBe(metadataInsertCall[1][11]);
    });

    it("should flush cursor state even when no events are buffered", async () => {
      const buffer = new EventBuffer(mockPool, null);
      buffer.noteCursor({
        signature: "cursor-only-sig",
        slot: 42n,
        blockTime: new Date("2026-03-06T12:00:00.000Z"),
        txIndex: 3,
      });

      await buffer.flush();

      const client = mockPool._client;
      const cursorCall = client.query.mock.calls.find(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("indexer_state")
      );
      expect(cursorCall).toBeDefined();
      expect(cursorCall[1][0]).toBe("cursor-only-sig");
      expect(cursorCall[1][1]).toBe("42");
      expect(cursorCall[1][2]).toBe(3);
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
      expect(cursorCall[0]).toContain("indexer_state.last_slot = EXCLUDED.last_slot");
      expect(cursorCall[0]).toContain("COALESCE(indexer_state.last_signature, '') COLLATE \"C\"");
      expect(cursorCall[0]).toContain("<= EXCLUDED.last_signature COLLATE \"C\"");
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
    it("delegates feedback_id assignment to DB triggers and keeps app-side IDs null", async () => {
      const client = mockPool._client;

      client.query.mockImplementation((sql: string, params?: any[]) => {
        if (sql === "BEGIN" || sql === "COMMIT" || sql === "ROLLBACK") {
          return { rows: [], rowCount: 0 };
        }
        if (typeof sql === "string" && sql.includes("INSERT INTO feedbacks (id, feedback_id")) {
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
      expect(feedbackInsertCalls[0][1][1]).toBeNull();
      expect(feedbackInsertCalls[1][1][1]).toBeNull();
      expect(feedbackInsertCalls[2][1][1]).toBeNull();

      const feedbackBackfillCall = client.query.mock.calls.find(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("SET feedback_id = COALESCE(feedback_id")
      );
      expect(feedbackBackfillCall).toBeUndefined();
    });

    it("delegates revocation_id assignment to DB triggers and keeps app-side IDs null", async () => {
      const client = mockPool._client;

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
        if (typeof sql === "string" && sql.includes("INSERT INTO revocations (id, revocation_id")) {
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
      expect(revocationInsertCalls[0][1][1]).toBeNull();
      expect(revocationInsertCalls[0][1][16]).toBe("PENDING");
      expect(revocationInsertCalls[1][1][1]).toBeNull();
      expect(revocationInsertCalls[1][1][16]).toBe("ORPHANED");
      expect(revocationInsertCalls[0][0]).not.toContain("revocation_id = CASE");
    });

    it("delegates response_id assignment to DB triggers and keeps app-side IDs null", async () => {
      const client = mockPool._client;

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
          if (feedbackIndex === "11") {
            return { rows: [{ id: "fb:11", feedback_hash: "ab".repeat(32) }], rowCount: null };
          }
          return { rows: [{ id: "fb:ok", feedback_hash: "ab".repeat(32) }], rowCount: 1 };
        }
        if (typeof sql === "string" && sql.includes("INSERT INTO feedback_responses (id, response_id")) {
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
      await buffer.addEvent(
        makeEvent("ResponseAppended", {
          asset: "asset1",
          client: "client1",
          responder: "responder6",
          feedbackIndex: 11n,
          sealHash: new Uint8Array(32).fill(0xab),
          responseHash: new Uint8Array(32).fill(0xab),
        }, { signature: "sig-6" })
      );
      await buffer.flush();

      const responseInsertCalls = client.query.mock.calls.filter(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("INSERT INTO feedback_responses (id, response_id")
      );
      const orphanInsertCalls = client.query.mock.calls.filter(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("INSERT INTO orphan_responses")
      );
      expect(responseInsertCalls).toHaveLength(5);
      expect(orphanInsertCalls).toHaveLength(1);

      expect(responseInsertCalls[0][1][1]).toBeNull();
      expect(responseInsertCalls[0][1][15]).toBe("PENDING");
      expect(responseInsertCalls[1][1][1]).toBeNull();
      expect(responseInsertCalls[1][1][15]).toBe("PENDING");
      expect(responseInsertCalls[2][1][1]).toBeNull();
      expect(responseInsertCalls[2][1][15]).toBe("PENDING");
      expect(responseInsertCalls[3][1][1]).toBeNull();
      expect(responseInsertCalls[3][1][15]).toBe("ORPHANED");
      expect(responseInsertCalls[4][1][1]).toBeNull();
      expect(responseInsertCalls[4][1][15]).toBe("PENDING");

      expect(orphanInsertCalls[0][1]).toContain("ab".repeat(32));
      expect(orphanInsertCalls[0][0]).toContain("ON CONFLICT (asset, client_address, feedback_index, responder, tx_signature) DO UPDATE");
    });

    it("replays staged orphan responses when feedback arrives later in the same flush", async () => {
      const client = mockPool._client;

      client.query.mockImplementation((sql: string, params?: any[]) => {
        if (sql === "BEGIN" || sql === "COMMIT" || sql === "ROLLBACK") {
          return { rows: [], rowCount: 0 };
        }
        if (typeof sql === "string" && sql.includes("SELECT id, feedback_hash FROM feedbacks WHERE asset = $1 AND client_address = $2 AND feedback_index = $3 LIMIT 1")) {
          const feedbackIndex = params?.[2] as string;
          if (feedbackIndex === "0") {
            return { rows: [], rowCount: 0 };
          }
          return { rows: [], rowCount: 0 };
        }
        if (typeof sql === "string" && sql.includes("INSERT INTO feedbacks")) {
          return { rows: [], rowCount: 1 };
        }
        if (typeof sql === "string" && sql.includes("UPDATE agents SET")) {
          return { rows: [], rowCount: 1 };
        }
        if (typeof sql === "string" && sql.includes("FROM orphan_responses")) {
          return {
            rows: [
              {
                id: "asset1:client1:0:responder1:sig-orphan",
                asset: "asset1",
                client_address: "client1",
                feedback_index: "0",
                responder: "responder1",
                response_uri: "ipfs://resp",
                response_hash: "ab".repeat(32),
                seal_hash: "ab".repeat(32),
                running_digest: Buffer.from(new Uint8Array(32).fill(0xab)),
                response_count: "1",
                block_slot: "100",
                tx_index: 2,
                event_ordinal: 0,
                tx_signature: "sig-orphan",
                created_at: new Date("2026-03-06T00:00:00.000Z").toISOString(),
              },
            ],
            rowCount: 1,
          };
        }
        if (typeof sql === "string" && sql.includes("INSERT INTO feedback_responses (id, response_id")) {
          return { rows: [], rowCount: 1 };
        }
        if (typeof sql === "string" && sql.includes("DELETE FROM orphan_responses")) {
          return { rows: [], rowCount: 1 };
        }
        if (typeof sql === "string" && sql.includes("FROM revocations")) {
          return { rows: [], rowCount: 0 };
        }
        if (typeof sql === "string" && sql.includes("INSERT INTO orphan_responses")) {
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
          responseUri: "ipfs://resp",
          newResponseDigest: new Uint8Array(32).fill(0xab),
          newResponseCount: 1n,
        }, { signature: "sig-orphan", txIndex: 2, eventOrdinal: 0, slot: 100n, blockTime: new Date("2026-03-06T00:00:00.000Z") })
      );
      await buffer.addEvent(
        makeEvent("NewFeedback", {
          asset: "asset1",
          clientAddress: "client1",
          feedbackIndex: 0n,
          value: 1000n,
          valueDecimals: 2,
          score: 80,
          sealHash: new Uint8Array(32).fill(0xab),
          atomEnabled: false,
          newFeedbackDigest: new Uint8Array(32).fill(0xab),
          newFeedbackCount: 1n,
          tag1: "quality",
          tag2: "speed",
          endpoint: "/chat",
          feedbackUri: "ipfs://feedback",
          isUniqueClient: true,
        }, { signature: "sig-feedback", txIndex: 3, eventOrdinal: 0, slot: 100n, blockTime: new Date("2026-03-06T00:00:01.000Z") })
      );
      await buffer.flush();

      const orphanInsertIndex = client.query.mock.calls.findIndex(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("INSERT INTO orphan_responses")
      );
      const responseInsertIndex = client.query.mock.calls.findIndex(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("INSERT INTO feedback_responses (id, response_id")
      );
      const orphanDeleteIndex = client.query.mock.calls.findIndex(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("DELETE FROM orphan_responses")
      );

      expect(orphanInsertIndex).toBeGreaterThan(-1);
      expect(responseInsertIndex).toBeGreaterThan(orphanInsertIndex);
      expect(orphanDeleteIndex).toBeGreaterThan(responseInsertIndex);
      expect(client.query.mock.calls[responseInsertIndex][1][15]).toBe("PENDING");
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
        if (typeof sql === "string" && sql.includes("SELECT 1 FROM agents WHERE asset")) {
          return { rows: [{ exists: 1 }], rowCount: 1 };
        }
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
        if (typeof sql === "string" && sql.includes("SELECT 1 FROM agents WHERE asset")) {
          return { rows: [{ exists: 1 }], rowCount: 1 };
        }
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

  describe("agent mutable updates preserve creation slot semantics in PG batch mode", () => {
    it("does not rewrite block_slot on owner, wallet reset, collection pointer, or parent updates", async () => {
      const client = mockPool._client;
      const buffer = new EventBuffer(mockPool, null) as any;
      const ctx = {
        signature: "sig-mut",
        slot: 999n,
        txIndex: 3,
        eventOrdinal: 1,
        blockTime: new Date("2026-03-06T00:00:00.000Z"),
      };

      client.query.mockClear();
      await buffer.updateAgentOwnerSupabase(client, { asset: "asset1", newOwner: "owner1" }, ctx);
      const ownerCall = client.query.mock.calls.find((c: any[]) =>
        typeof c[0] === "string" && c[0].includes("UPDATE agents SET owner = $1")
      );
      expect(ownerCall).toBeDefined();
      expect(ownerCall![0]).not.toContain("block_slot");

      client.query.mockClear();
      await buffer.updateWalletResetOnOwnerSyncSupabase(
        client,
        { asset: "asset1", ownerAfterSync: "owner2", newWallet: "wallet2" },
        ctx
      );
      const walletResetCall = client.query.mock.calls.find((c: any[]) =>
        typeof c[0] === "string" && c[0].includes("UPDATE agents SET owner = $1, agent_wallet = $2")
      );
      expect(walletResetCall).toBeDefined();
      expect(walletResetCall![0]).not.toContain("block_slot");

      client.query.mockClear();
      await buffer.updateCollectionPointerSupabase(
        client,
        { asset: "asset1", col: "c1:test-pointer", setBy: "owner1", lock: true },
        ctx
      );
      const collectionCall = client.query.mock.calls.find((c: any[]) =>
        typeof c[0] === "string" && c[0].includes("SET canonical_col = $2")
      );
      expect(collectionCall).toBeDefined();
      expect(collectionCall![0]).not.toContain("block_slot");

      client.query.mockClear();
      await buffer.updateParentAssetSupabase(
        client,
        { asset: "asset1", parentAsset: "parent1", parentCreator: "creator1", lock: true },
        ctx
      );
      const parentCall = client.query.mock.calls.find((c: any[]) =>
        typeof c[0] === "string" && c[0].includes("SET parent_asset = $1")
      );
      expect(parentCall).toBeDefined();
      expect(parentCall![0]).not.toContain("block_slot");
      expect(parentCall![1]).toHaveLength(5);
    });
  });
});
