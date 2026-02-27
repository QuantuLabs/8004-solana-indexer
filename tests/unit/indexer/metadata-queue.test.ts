import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const { capturedTasks, mockPQueueInstance } = vi.hoisted(() => {
  const capturedTasks: Array<() => Promise<void>> = [];
  const mockPQueueInstance = {
    size: 0,
    pending: 0,
    add: vi.fn((fn: () => Promise<void>) => {
      capturedTasks.push(fn);
      return Promise.resolve();
    }),
    onIdle: vi.fn().mockResolvedValue(undefined),
  };
  return { capturedTasks, mockPQueueInstance };
});

vi.mock("p-queue", () => {
  class MockPQueue {
    constructor() {
      return mockPQueueInstance as any;
    }
  }
  return { default: MockPQueue };
});

vi.mock("../../../src/config.js", () => ({
  config: {
    metadataIndexMode: "normal",
    metadataMaxValueBytes: 10000,
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

vi.mock("../../../src/indexer/uriDigest.js", () => ({
  digestUri: vi.fn().mockResolvedValue({
    status: "ok",
    fields: {
      "_uri:name": "Test Agent",
      "_uri:description": "A test agent",
    },
    bytes: 100,
    hash: "abc123",
  }),
  serializeValue: vi.fn().mockReturnValue({
    value: "serialized",
    oversize: false,
    bytes: 10,
  }),
}));

vi.mock("../../../src/utils/compression.js", () => ({
  compressForStorage: vi.fn().mockResolvedValue(Buffer.from([0x00, 0x01])),
}));

vi.mock("../../../src/constants.js", () => ({
  STANDARD_URI_FIELDS: new Set([
    "_uri:type", "_uri:name", "_uri:description", "_uri:image",
    "_uri:services", "_uri:registrations", "_uri:supported_trust",
    "_uri:active", "_uri:x402_support", "_uri:skills", "_uri:domains",
    "_uri:_status",
  ]),
}));

import { metadataQueue } from "../../../src/indexer/metadata-queue.js";
import { digestUri, serializeValue } from "../../../src/indexer/uriDigest.js";
import { compressForStorage } from "../../../src/utils/compression.js";
import { config } from "../../../src/config.js";

function createMockPool() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [], rowCount: 0 }),
  } as any;
}

function resetQueueState() {
  const q = metadataQueue as any;
  q.pending.clear();
  q.deferred.clear();
  q.queueFullSignals = 0;
  q.stats = {
    queued: 0,
    processed: 0,
    skippedStale: 0,
    skippedDuplicate: 0,
    deferredQueued: 0,
    deferredPromoted: 0,
    deferredReplaced: 0,
    errors: 0,
  };
  capturedTasks.length = 0;
  mockPQueueInstance.size = 0;
  mockPQueueInstance.pending = 0;
  mockPQueueInstance.add.mockClear();
  mockPQueueInstance.onIdle.mockClear();
}

describe("MetadataQueue", () => {
  let mockPool: ReturnType<typeof createMockPool>;

  beforeEach(() => {
    resetQueueState();
    mockPool = createMockPool();
    metadataQueue.setPool(mockPool);

    // Restore default mocks (setup.ts clearAllMocks wipes them)
    (digestUri as any).mockResolvedValue({
      status: "ok",
      fields: {
        "_uri:name": "Test Agent",
        "_uri:description": "A test agent",
      },
      bytes: 100,
      hash: "abc123",
    });
    (serializeValue as any).mockReturnValue({
      value: "serialized",
      oversize: false,
      bytes: 10,
    });
    (compressForStorage as any).mockResolvedValue(Buffer.from([0x00, 0x01]));
  });

  async function runCapturedTasks() {
    const tasks = [...capturedTasks];
    capturedTasks.length = 0;
    for (const fn of tasks) {
      try {
        await fn();
      } catch {}
    }
  }

  describe("add", () => {
    it("should add task to queue", () => {
      metadataQueue.add("asset1", "https://example.com/agent.json");
      const stats = metadataQueue.getStats();
      expect(stats.queued).toBe(1);
    });

    it("should skip empty URI", () => {
      metadataQueue.add("asset1", "");
      const stats = metadataQueue.getStats();
      expect(stats.queued).toBe(0);
    });

    it("should skip when metadataIndexMode is off", () => {
      (config as any).metadataIndexMode = "off";
      metadataQueue.add("asset1", "https://example.com");
      const stats = metadataQueue.getStats();
      expect(stats.queued).toBe(0);
      (config as any).metadataIndexMode = "normal";
    });

    it("should deduplicate same asset + same URI", () => {
      metadataQueue.add("asset1", "https://example.com/agent.json");
      metadataQueue.add("asset1", "https://example.com/agent.json");
      const stats = metadataQueue.getStats();
      expect(stats.skippedDuplicate).toBe(1);
    });

    it("should update URI for same asset with different URI", () => {
      metadataQueue.add("asset1", "https://example.com/v1.json");
      metadataQueue.add("asset1", "https://example.com/v2.json");
      const stats = metadataQueue.getStats();
      expect(stats.queued).toBe(2);
    });

    it("should defer tasks when queue is at capacity", () => {
      mockPQueueInstance.size = 5000;
      mockPQueueInstance.pending = 0;
      metadataQueue.add("assetFull", "https://example.com/full.json");
      const stats = metadataQueue.getStats();
      expect(stats.queued).toBe(0);
      expect(stats.deferredQueued).toBe(1);
      expect(stats.deferredCount).toBe(1);
    });

    it("should promote deferred tasks when capacity returns", () => {
      mockPQueueInstance.size = 5000;
      metadataQueue.add("assetDeferred", "https://example.com/deferred.json");
      expect(metadataQueue.getStats().deferredCount).toBe(1);

      mockPQueueInstance.size = 0;
      metadataQueue.add("assetNew", "https://example.com/new.json");

      const stats = metadataQueue.getStats();
      expect(stats.deferredCount).toBe(0);
      expect(stats.deferredPromoted).toBeGreaterThanOrEqual(1);
      expect(stats.queued).toBeGreaterThanOrEqual(2);
      expect(capturedTasks.length).toBeGreaterThanOrEqual(2);
    });

    it("should keep only latest deferred task per asset", () => {
      mockPQueueInstance.size = 5000;
      metadataQueue.add("assetLatest", "https://example.com/v1.json");
      metadataQueue.add("assetLatest", "https://example.com/v2.json");

      const q = metadataQueue as any;
      expect(q.deferred.get("assetLatest")?.uri).toBe("https://example.com/v2.json");

      const stats = metadataQueue.getStats();
      expect(stats.deferredReplaced).toBeGreaterThanOrEqual(1);
      expect(stats.deferredCount).toBe(1);
    });
  });

  describe("addBatch", () => {
    it("should add multiple tasks", () => {
      metadataQueue.addBatch([
        { assetId: "asset1", uri: "https://example.com/1" },
        { assetId: "asset2", uri: "https://example.com/2" },
      ]);
      const stats = metadataQueue.getStats();
      expect(stats.queued).toBe(2);
    });
  });

  describe("processTask", () => {
    it("should fetch URI and store metadata", async () => {
      mockPool.query.mockImplementation((sql: string) => {
        if (typeof sql === "string" && sql.includes("SELECT agent_uri")) {
          return { rows: [{ agent_uri: "https://example.com/agent.json" }] };
        }
        return { rows: [], rowCount: 0 };
      });

      metadataQueue.add("asset1", "https://example.com/agent.json");
      await runCapturedTasks();

      const stats = metadataQueue.getStats();
      expect(stats.processed).toBeGreaterThanOrEqual(1);
    });

    it("should skip if agent no longer exists", async () => {
      mockPool.query.mockResolvedValue({ rows: [], rowCount: 0 });
      metadataQueue.add("asset1", "https://example.com/agent.json");
      await runCapturedTasks();

      const stats = metadataQueue.getStats();
      expect(stats.skippedStale).toBeGreaterThanOrEqual(1);
    });

    it("should skip if URI changed (stale fetch)", async () => {
      mockPool.query.mockImplementation((sql: string) => {
        if (typeof sql === "string" && sql.includes("SELECT agent_uri")) {
          return { rows: [{ agent_uri: "https://example.com/updated.json" }] };
        }
        return { rows: [], rowCount: 0 };
      });

      metadataQueue.add("asset1", "https://example.com/old.json");
      await runCapturedTasks();

      const stats = metadataQueue.getStats();
      expect(stats.skippedStale).toBeGreaterThanOrEqual(1);
    });

    it("should handle digest failure", async () => {
      (digestUri as any).mockResolvedValueOnce({
        status: "error",
        error: "Fetch failed",
        bytes: 0,
        hash: null,
      });

      mockPool.query.mockImplementation((sql: string) => {
        if (typeof sql === "string" && sql.includes("SELECT agent_uri")) {
          return { rows: [{ agent_uri: "https://example.com/agent.json" }] };
        }
        return { rows: [], rowCount: 0 };
      });

      metadataQueue.add("asset1", "https://example.com/agent.json");
      await runCapturedTasks();

      const stats = metadataQueue.getStats();
      expect(stats.processed).toBeGreaterThanOrEqual(1);
    });

    it("should handle oversize values", async () => {
      (serializeValue as any).mockReturnValueOnce({
        value: "truncated",
        oversize: true,
        bytes: 500000,
      });

      (digestUri as any).mockResolvedValueOnce({
        status: "ok",
        fields: { "_uri:large_field": "data" },
        bytes: 500000,
        hash: "abc",
      });

      mockPool.query.mockImplementation((sql: string) => {
        if (typeof sql === "string" && sql.includes("SELECT agent_uri")) {
          return { rows: [{ agent_uri: "https://example.com/agent.json" }] };
        }
        return { rows: [], rowCount: 0 };
      });

      metadataQueue.add("asset1", "https://example.com/agent.json");
      await runCapturedTasks();

      const stats = metadataQueue.getStats();
      expect(stats.processed).toBeGreaterThanOrEqual(1);
    });

    it("should sync nft_name from _uri:name", async () => {
      (digestUri as any).mockResolvedValueOnce({
        status: "ok",
        fields: { "_uri:name": "My Agent" },
        bytes: 50,
        hash: "abc",
      });

      mockPool.query.mockImplementation((sql: string) => {
        if (typeof sql === "string" && sql.includes("SELECT agent_uri")) {
          return { rows: [{ agent_uri: "https://example.com/agent.json" }] };
        }
        return { rows: [], rowCount: 0 };
      });

      metadataQueue.add("asset1", "https://example.com/agent.json");
      await runCapturedTasks();

      const nftNameCalls = mockPool.query.mock.calls.filter(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("nft_name")
      );
      expect(nftNameCalls.length).toBeGreaterThanOrEqual(1);
    });

    it("should handle processTask error gracefully", async () => {
      mockPool.query.mockRejectedValue(new Error("DB error"));
      metadataQueue.add("asset1", "https://example.com/agent.json");
      await runCapturedTasks();

      const stats = metadataQueue.getStats();
      expect(stats.errors).toBeGreaterThanOrEqual(1);
    });

    it("should purge old URI metadata before writing new", async () => {
      mockPool.query.mockImplementation((sql: string) => {
        if (typeof sql === "string" && sql.includes("SELECT agent_uri")) {
          return { rows: [{ agent_uri: "https://example.com/agent.json" }] };
        }
        return { rows: [], rowCount: 0 };
      });

      metadataQueue.add("asset1", "https://example.com/agent.json");
      await runCapturedTasks();

      const deleteCalls = mockPool.query.mock.calls.filter(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("DELETE FROM metadata")
      );
      expect(deleteCalls.length).toBeGreaterThanOrEqual(1);
    });

    it("should store truncatedKeys in status", async () => {
      (digestUri as any).mockResolvedValueOnce({
        status: "ok",
        fields: { "_uri:name": "Agent" },
        bytes: 100,
        hash: "abc",
        truncatedKeys: true,
      });

      mockPool.query.mockImplementation((sql: string) => {
        if (typeof sql === "string" && sql.includes("SELECT agent_uri")) {
          return { rows: [{ agent_uri: "https://example.com/agent.json" }] };
        }
        return { rows: [], rowCount: 0 };
      });

      metadataQueue.add("asset1", "https://example.com/agent.json");
      await runCapturedTasks();

      // _uri:_status is stored as a Buffer containing PREFIX_RAW + JSON with truncatedKeys
      const insertCalls = mockPool.query.mock.calls.filter(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("INSERT INTO metadata")
      );
      const hasStatusWithTruncated = insertCalls.some((call: any[]) => {
        const params = call[1] as any[];
        if (!params) return false;
        const valueBuf = params[4]; // 5th param is the value buffer
        if (Buffer.isBuffer(valueBuf)) {
          const decoded = valueBuf.slice(1).toString(); // skip PREFIX_RAW byte
          return decoded.includes("truncatedKeys");
        }
        return false;
      });
      expect(hasStatusWithTruncated).toBe(true);
    });

    it("should process without pool (skip freshness and storage)", async () => {
      metadataQueue.setPool(null as any);
      metadataQueue.add("asset1", "https://example.com/agent.json");
      await runCapturedTasks();

      const stats = metadataQueue.getStats();
      expect(stats.processed).toBeGreaterThanOrEqual(1);
    });
  });

  describe("storeMetadata", () => {
    it("should store standard fields without compression", async () => {
      mockPool.query.mockImplementation((sql: string) => {
        if (typeof sql === "string" && sql.includes("SELECT agent_uri")) {
          return { rows: [{ agent_uri: "https://example.com/agent.json" }] };
        }
        return { rows: [], rowCount: 0 };
      });

      metadataQueue.add("asset1", "https://example.com/agent.json");
      await runCapturedTasks();

      const insertCalls = mockPool.query.mock.calls.filter(
        (call: any[]) => typeof call[0] === "string" && call[0].includes("INSERT INTO metadata")
      );
      expect(insertCalls.length).toBeGreaterThanOrEqual(1);
    });

    it("should compress non-standard fields", async () => {
      (digestUri as any).mockResolvedValueOnce({
        status: "ok",
        fields: { "custom_field": "custom data" },
        bytes: 50,
        hash: "abc",
      });

      mockPool.query.mockImplementation((sql: string) => {
        if (typeof sql === "string" && sql.includes("SELECT agent_uri")) {
          return { rows: [{ agent_uri: "https://example.com/agent.json" }] };
        }
        return { rows: [], rowCount: 0 };
      });

      metadataQueue.add("asset1", "https://example.com/agent.json");
      await runCapturedTasks();

      expect(compressForStorage).toHaveBeenCalled();
    });
  });

  describe("getStats", () => {
    it("should return accurate stats", () => {
      const stats = metadataQueue.getStats();
      expect(stats).toHaveProperty("queued");
      expect(stats).toHaveProperty("processed");
      expect(stats).toHaveProperty("errors");
      expect(stats).toHaveProperty("queueSize");
      expect(stats).toHaveProperty("pendingCount");
      expect(stats).toHaveProperty("deferredCount");
      expect(stats).toHaveProperty("deferredQueued");
      expect(stats).toHaveProperty("deferredPromoted");
    });
  });

  describe("drain", () => {
    it("should wait for queue to be idle", async () => {
      await metadataQueue.drain();
      expect(mockPQueueInstance.onIdle).toHaveBeenCalled();
    });
  });

  describe("shutdown", () => {
    it("should clear stats interval", () => {
      metadataQueue.shutdown();
      metadataQueue.shutdown();
    });
  });

  describe("logStats", () => {
    it("should log stats when there are queued items", () => {
      metadataQueue.add("asset1", "https://example.com/1");
      (metadataQueue as any).logStats();
    });

    it("should not log when no items queued", () => {
      (metadataQueue as any).logStats();
    });
  });

  describe("queue.add rejection", () => {
    it("should catch queue.add rejection and increment errors", async () => {
      mockPQueueInstance.add.mockRejectedValueOnce(new Error("Queue internal error"));
      metadataQueue.add("assetErr", "https://example.com/err.json");

      await new Promise((r) => setTimeout(r, 10));

      const stats = metadataQueue.getStats();
      expect(stats.errors).toBeGreaterThanOrEqual(1);
    });
  });
});
