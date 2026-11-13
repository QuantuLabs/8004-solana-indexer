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

    it("skips terminal statuses unless stale, retries timeout/error", async () => {
      await sweep(1_000);
      const sql = mockPool.query.mock.calls[0][0] as string;
      expect(sql).toContain("cp.metadata_status NOT IN ('ok', 'oversize', 'invalid_json', 'blocked')");
      expect(sql).toContain("cp.metadata_updated_at < cp.last_seen_at");
      expect(sql).not.toContain("cp.metadata_status != 'ok'");
    });

    it("clears the cooldown on successful extraction", async () => {
      const q = collectionMetadataQueue as any;
      q.recoveryRetryAt.set("c1:ok|cr", Date.now() + RETRY_MS);
      mockPool.query.mockResolvedValueOnce({ rows: [{ canonical_col: "c1:ok", creator: "cr", owner: "cr" }] });
      collectionMetadataQueue.add("a1", "c1:ok");
      await runCapturedTasks();
      expect(q.recoveryRetryAt.has("c1:ok|cr")).toBe(false);
    });
  });
});
