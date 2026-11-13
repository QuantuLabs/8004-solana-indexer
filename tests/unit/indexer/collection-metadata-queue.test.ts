import { describe, it, expect, vi, beforeEach } from "vitest";

const { capturedTasks, mockPQueueInstance } = vi.hoisted(() => {
  const capturedTasks: Array<() => Promise<void>> = [];
  const mockPQueueInstance = {
    size: 0,
    pending: 0,
    add: vi.fn((fn: () => Promise<void>) => {
      capturedTasks.push(fn);
      return Promise.resolve();
    }),
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
    collectionMetadataIndexEnabled: true,
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

vi.mock("../../../src/indexer/collectionDigest.js", () => ({
  digestCollectionPointerDoc: vi.fn().mockResolvedValue({
    status: "ok",
    fields: {
      version: "1.0.0",
      name: "Test Collection",
      symbol: "TEST",
      description: "test",
      image: "https://example.com/image.png",
      bannerImage: null,
      socialWebsite: null,
      socialX: null,
      socialDiscord: null,
    },
    hash: "abc123",
    bytes: 123,
  }),
}));

import { collectionMetadataQueue } from "../../../src/indexer/collection-metadata-queue.js";
import { digestCollectionPointerDoc } from "../../../src/indexer/collectionDigest.js";
import { config } from "../../../src/config.js";

function createMockPool() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [], rowCount: 0 }),
  } as any;
}

function resetQueueState() {
  const q = collectionMetadataQueue as any;
  q.pending.clear();
  q.deferred.clear();
  q.recoveryRetryAt.clear();
  q.recoveryCursor = ["", ""];
  q.queueFullSignals = 0;
  q.stats = {
    queued: 0,
    processed: 0,
    skippedStale: 0,
    skippedDuplicate: 0,
    deferredQueued: 0,
    deferredPromoted: 0,
    deferredReplaced: 0,
    deferredDropped: 0,
    errors: 0,
  };
  capturedTasks.length = 0;
  mockPQueueInstance.size = 0;
  mockPQueueInstance.pending = 0;
  mockPQueueInstance.add.mockClear();
}

describe("CollectionMetadataQueue", () => {
  let mockPool: ReturnType<typeof createMockPool>;

  beforeEach(() => {
    resetQueueState();
    mockPool = createMockPool();
    collectionMetadataQueue.setPool(mockPool);

    (digestCollectionPointerDoc as any).mockResolvedValue({
      status: "ok",
      fields: {
        version: "1.0.0",
        name: "Test Collection",
        symbol: "TEST",
        description: "test",
        image: "https://example.com/image.png",
        bannerImage: null,
        socialWebsite: null,
        socialX: null,
        socialDiscord: null,
      },
      hash: "abc123",
      bytes: 123,
    });
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

  it("should skip when collection metadata indexing is disabled", () => {
    (config as any).collectionMetadataIndexEnabled = false;
    collectionMetadataQueue.add("asset-1", "c1:cid");
    expect(collectionMetadataQueue.getStats().queued).toBe(0);
    (config as any).collectionMetadataIndexEnabled = true;
  });

  it("should drop deferred tasks when total queue cap is full", () => {
    mockPQueueInstance.size = 5000;
    collectionMetadataQueue.add("asset-full", "c1:full");

    const stats = collectionMetadataQueue.getStats();
    expect(stats.queued).toBe(0);
    expect(stats.deferredQueued).toBe(1);
    expect(stats.deferredDropped).toBe(1);
    expect(stats.deferredCount).toBe(0);
    expect(stats.totalQueueLoad).toBe(5000);
  });

  it("should promote deferred tasks when capacity returns", () => {
    const q = collectionMetadataQueue as any;
    const deferred = {
      assetId: "asset-deferred",
      col: "c1:deferred",
      addedAt: Date.now(),
    };
    q.pending.set("asset-deferred", deferred);
    q.deferred.set("asset-deferred", deferred);
    expect(collectionMetadataQueue.getStats().deferredCount).toBe(1);

    mockPQueueInstance.size = 0;
    collectionMetadataQueue.add("asset-new", "c1:new");

    const stats = collectionMetadataQueue.getStats();
    expect(stats.deferredCount).toBe(0);
    expect(stats.deferredPromoted).toBeGreaterThanOrEqual(1);
    expect(stats.queued).toBeGreaterThanOrEqual(2);
    expect(capturedTasks.length).toBeGreaterThanOrEqual(2);
  });

  it("should keep only latest deferred task per asset", () => {
    const q = collectionMetadataQueue as any;
    const oldTask = {
      assetId: "asset-latest",
      col: "c1:old",
      addedAt: Date.now(),
    };
    mockPQueueInstance.size = 4998;
    q.pending.set("asset-latest", oldTask);
    q.deferred.set("asset-latest", oldTask);
    collectionMetadataQueue.add("asset-latest", "c1:new");

    expect(q.deferred.get("asset-latest")?.col).toBe("c1:new");

    const stats = collectionMetadataQueue.getStats();
    expect(stats.deferredReplaced).toBeGreaterThanOrEqual(1);
    expect(stats.deferredCount).toBe(1);
    expect(stats.deferredDropped).toBe(0);
  });

  it("should evict the oldest deferred task when a newer deferred task needs room", () => {
    const q = collectionMetadataQueue as any;
    const oldTask = {
      assetId: "asset-old",
      col: "c1:old",
      addedAt: Date.now(),
    };
    mockPQueueInstance.size = 4998;
    q.pending.set("asset-old", oldTask);
    q.deferred.set("asset-old", oldTask);

    collectionMetadataQueue.add("asset-new", "c1:new");

    const stats = collectionMetadataQueue.getStats();
    expect(q.deferred.has("asset-old")).toBe(false);
    expect(q.pending.has("asset-old")).toBe(false);
    expect(q.deferred.get("asset-new")?.col).toBe("c1:new");
    expect(stats.deferredDropped).toBe(1);
    expect(stats.deferredCount).toBe(1);
    expect(stats.totalQueueLoad).toBe(5000);
  });

  it("should process queued task with freshness check", async () => {
    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes("SELECT canonical_col")) {
        return {
          rows: [{ canonical_col: "c1:ok", creator: "creator-1", owner: "owner-1" }],
          rowCount: 1,
        };
      }
      return { rows: [], rowCount: 1 };
    });

    collectionMetadataQueue.add("asset-1", "c1:ok");
    await runCapturedTasks();

    expect(digestCollectionPointerDoc).toHaveBeenCalledWith("c1:ok");
    expect(collectionMetadataQueue.getStats().processed).toBeGreaterThanOrEqual(1);
  });

  describe("recovery sweep", () => {
    const sweep = (nowMs: number) => (collectionMetadataQueue as any).runRecoverySweep(nowMs);
    const RETRY_MS = 15 * 60_000;

    it("enqueues one task per pointer and selects distinct pairs", async () => {
      mockPool.query.mockResolvedValueOnce({
        rows: [
          { asset: "a1", canonical_col: "c1:dead", creator: "cr" },
          { asset: "a2", canonical_col: "c1:dead", creator: "cr" },
        ],
      });
      await sweep(1_000);
      const sql = mockPool.query.mock.calls[0][0] as string;
      expect(sql).toContain("DISTINCT ON (a.canonical_col, COALESCE(a.creator, a.owner))");
      expect(collectionMetadataQueue.getStats().queued).toBe(1);
    });

    it("keeps separate cooldowns for pairs whose joined text is identical", async () => {
      mockPool.query.mockResolvedValueOnce({
        rows: [
          { asset: "a1", canonical_col: "c1:x", creator: "y|r" },
          { asset: "a2", canonical_col: "c1:x|y", creator: "r" },
        ],
      });
      await sweep(1_000);
      expect(collectionMetadataQueue.getStats().queued).toBe(2);
    });

    it("respects the cooldown between sweeps", async () => {
      const rows = [{ asset: "a1", canonical_col: "c1:dead", creator: "cr" }];
      mockPool.query.mockResolvedValue({ rows });
      await sweep(1_000);
      (collectionMetadataQueue as any).pending.clear();
      await sweep(1_000 + RETRY_MS - 1);
      expect(collectionMetadataQueue.getStats().queued).toBe(1);
      await sweep(1_000 + RETRY_MS);
      expect(collectionMetadataQueue.getStats().queued).toBe(2);
    });

    it("retries every failed status and wraps around a round-robin cursor", async () => {
      mockPool.query.mockResolvedValue({
        rows: [
          { asset: "a1", canonical_col: "c1:a", creator: "cr" },
          { asset: "a2", canonical_col: "c1:b", creator: "cr2" },
        ],
      });
      await sweep(1_000);
      await sweep(2_000);
      const [sql, p0] = mockPool.query.mock.calls[0] as [string, unknown[]];
      const p1 = mockPool.query.mock.calls[1][1] as unknown[];
      expect(sql).toContain("cp.metadata_status != 'ok'");
      expect(sql).toContain("ORDER BY ((p.canonical_col, p.creator) <= ($2::text, $3::text))");
      expect(sql).not.toContain("ALL(");
      expect(sql).toContain("a.asset != ''");
      expect(p0).toEqual([1000, "", ""]);
      expect(p1).toEqual([1000, "c1:b", "cr2"]);
    });

    it("does not reserve the cooldown for a duplicate of a pending task", async () => {
      const rows = [{ asset: "a1", canonical_col: "c1:dead", creator: "cr" }];
      mockPool.query.mockResolvedValue({ rows });
      collectionMetadataQueue.add("a1", "c1:dead");
      await sweep(1_000);
      expect(collectionMetadataQueue.getStats().queued).toBe(1);
      expect((collectionMetadataQueue as any).recoveryRetryAt.has(JSON.stringify(["c1:dead", "cr"]))).toBe(false);
      (collectionMetadataQueue as any).pending.clear();
      await sweep(2_000);
      expect(collectionMetadataQueue.getStats().queued).toBe(2);
    });

    it("keeps the cooldown after an extraction", async () => {
      const q = collectionMetadataQueue as any;
      q.recoveryRetryAt.set(JSON.stringify(["c1:ok", "cr"]), Date.now() + RETRY_MS);
      mockPool.query.mockResolvedValueOnce({ rows: [{ canonical_col: "c1:ok", creator: "cr", owner: "cr" }] });
      collectionMetadataQueue.add("a1", "c1:ok");
      await runCapturedTasks();
      expect(q.recoveryRetryAt.has(JSON.stringify(["c1:ok", "cr"]))).toBe(true);
    });
  });
});
