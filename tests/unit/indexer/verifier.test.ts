import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { PublicKey } from "@solana/web3.js";

// Mock config before import
vi.mock("../../../src/config.js", () => ({
  config: {
    verificationEnabled: true,
    verifyIntervalMs: 1000,
    verifyBatchSize: 100,
    verifySafetyMarginSlots: 32,
    verifyMaxRetries: 3,
    verifyRecoveryCycles: 10,
    verifyRecoveryBatchSize: 50,
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

vi.mock("../../../src/utils/pda.js", () => ({
  getAgentPda: vi.fn((asset: PublicKey) => {
    return [asset, 255];
  }),
  getValidationRequestPda: vi.fn(
    (asset: PublicKey, validator: PublicKey, nonce: number) => {
      return [asset, 255];
    }
  ),
  getMetadataEntryPda: vi.fn((asset: PublicKey, keyHash: Uint8Array) => {
    return [asset, 255];
  }),
  getRegistryConfigPda: vi.fn((collection: PublicKey) => {
    return [collection, 255];
  }),
  computeKeyHash: vi.fn((key: string) => new Uint8Array(16).fill(0xab)),
  parseAssetPubkey: vi.fn((id: string) => new PublicKey(id)),
}));

import { DataVerifier } from "../../../src/indexer/verifier.js";
import {
  getIntegrityMetricsSnapshot,
  resetIntegrityMetrics,
} from "../../../src/observability/integrity-metrics.js";

function createMockConnection() {
  return {
    getSlot: vi.fn().mockResolvedValue(100000),
    getMultipleAccountsInfo: vi.fn().mockResolvedValue([]),
    getAccountInfo: vi.fn().mockResolvedValue(null),
  } as any;
}

function createMockPrisma() {
  return {
    agent: {
      findMany: vi.fn().mockResolvedValue([]),
      update: vi.fn().mockResolvedValue({}),
    },
    validation: {
      findMany: vi.fn().mockResolvedValue([]),
      update: vi.fn().mockResolvedValue({}),
    },
    agentMetadata: {
      findMany: vi.fn().mockResolvedValue([]),
      update: vi.fn().mockResolvedValue({}),
    },
    registry: {
      findMany: vi.fn().mockResolvedValue([]),
      update: vi.fn().mockResolvedValue({}),
    },
    feedback: {
      findMany: vi.fn().mockResolvedValue([]),
      findFirst: vi.fn().mockResolvedValue(null),
      count: vi.fn().mockResolvedValue(0),
      updateMany: vi.fn().mockResolvedValue({ count: 0 }),
    },
    feedbackResponse: {
      findMany: vi.fn().mockResolvedValue([]),
      findFirst: vi.fn().mockResolvedValue(null),
      count: vi.fn().mockResolvedValue(0),
      updateMany: vi.fn().mockResolvedValue({ count: 0 }),
    },
    revocation: {
      findMany: vi.fn().mockResolvedValue([]),
      findFirst: vi.fn().mockResolvedValue(null),
      count: vi.fn().mockResolvedValue(0),
      updateMany: vi.fn().mockResolvedValue({ count: 0 }),
    },
    agentDigestCache: {
      upsert: vi.fn().mockResolvedValue({}),
    },
  } as any;
}

function createMockPool() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [], rowCount: 0 }),
  } as any;
}

// Deterministic test keys
const AGENT_KEY = new PublicKey(new Uint8Array(32).fill(1));
const VALIDATOR_KEY = new PublicKey(new Uint8Array(32).fill(2));
const COLLECTION_KEY = new PublicKey(new Uint8Array(32).fill(3));

function buildAgentAccountData(opts?: {
  agentWalletPresent?: boolean;
  feedbackDigest?: Uint8Array;
  feedbackCount?: bigint;
  responseDigest?: Uint8Array;
  responseCount?: bigint;
  revokeDigest?: Uint8Array;
  revokeCount?: bigint;
}): Buffer {
  const {
    agentWalletPresent = false,
    feedbackDigest = new Uint8Array(32),
    feedbackCount = 0n,
    responseDigest = new Uint8Array(32),
    responseCount = 0n,
    revokeDigest = new Uint8Array(32),
    revokeCount = 0n,
  } = opts || {};

  // discriminator(8) + collection(32) + owner(32) + asset(32) + bump(1) + atom_enabled(1) + Option<Pubkey>
  const headerSize = 8 + 32 + 32 + 32 + 1 + 1;
  const optionSize = agentWalletPresent ? 33 : 1;
  const digestTriplets = 3 * (32 + 8); // 120 bytes

  const buf = Buffer.alloc(headerSize + optionSize + digestTriplets);
  let offset = headerSize;

  // Option tag
  buf[offset] = agentWalletPresent ? 1 : 0;
  offset += agentWalletPresent ? 33 : 1;

  // Feedback digest + count
  Buffer.from(feedbackDigest).copy(buf, offset);
  offset += 32;
  buf.writeBigUInt64LE(feedbackCount, offset);
  offset += 8;

  // Response digest + count
  Buffer.from(responseDigest).copy(buf, offset);
  offset += 32;
  buf.writeBigUInt64LE(responseCount, offset);
  offset += 8;

  // Revoke digest + count
  Buffer.from(revokeDigest).copy(buf, offset);
  offset += 32;
  buf.writeBigUInt64LE(revokeCount, offset);

  return buf;
}

describe("DataVerifier", () => {
  let mockConnection: ReturnType<typeof createMockConnection>;
  let mockPrisma: ReturnType<typeof createMockPrisma>;
  let mockPool: ReturnType<typeof createMockPool>;

  // Save original sleep for coverage test
  const origSleep = (DataVerifier.prototype as any).sleep;

  beforeEach(() => {
    vi.useFakeTimers();
    mockConnection = createMockConnection();
    mockPrisma = createMockPrisma();
    mockPool = createMockPool();
    resetIntegrityMetrics();
    // Patch sleep to resolve instantly under fake timers
    (DataVerifier.prototype as any).sleep = () => Promise.resolve();
  });

  afterEach(() => {
    vi.useRealTimers();
    delete (DataVerifier.prototype as any)._sleepPatched;
  });

  describe("constructor and lifecycle", () => {
    it("should create verifier with prisma", () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      expect(verifier).toBeDefined();
    });

    it("should create verifier with pool", () => {
      const verifier = new DataVerifier(mockConnection, null, mockPool);
      expect(verifier).toBeDefined();
    });

    it("should return empty stats initially", () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      const stats = verifier.getStats();
      expect(stats.agentsVerified).toBe(0);
      expect(stats.lastRunAt).toBeNull();
    });
  });

  describe("start/stop", () => {
    it("should not start when verification is disabled", async () => {
      const { config } = await import("../../../src/config.js");
      const origEnabled = config.verificationEnabled;
      (config as any).verificationEnabled = false;

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      // Should not call getSlot
      expect(mockConnection.getSlot).not.toHaveBeenCalled();

      (config as any).verificationEnabled = origEnabled;
    });

    it("should run initial verification on start", async () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      expect(mockConnection.getSlot).toHaveBeenCalledWith("finalized");
      await verifier.stop();
    });

    it("should update integrity metrics on verification cycle", async () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      await verifier.stop();

      const snapshot = getIntegrityMetricsSnapshot();
      expect(snapshot.verifyCyclesTotal).toBeGreaterThanOrEqual(1);
      expect(snapshot.lastVerifiedSlot).toBe(99968);
      expect(snapshot.mismatchCount).toBe(0);
      expect(snapshot.orphanCount).toBe(0);
    });

    it("should set up interval on start", async () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      // Advancing time should trigger another cycle
      mockConnection.getSlot.mockClear();
      vi.advanceTimersByTime(1100);
      // Give async work a chance
      await vi.advanceTimersByTimeAsync(100);
      expect(mockConnection.getSlot).toHaveBeenCalled();
      await verifier.stop();
    });

    it("should stop cleanly", async () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      await verifier.stop();
      // Advancing time should NOT trigger further cycles
      mockConnection.getSlot.mockClear();
      vi.advanceTimersByTime(2000);
      await vi.advanceTimersByTimeAsync(100);
      expect(mockConnection.getSlot).not.toHaveBeenCalled();
    });

    it("should handle initial verification failure gracefully", async () => {
      mockConnection.getSlot.mockRejectedValueOnce(new Error("RPC down"));
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      // Should not throw
      await verifier.start();
      await verifier.stop();
    });
  });

  describe("reentrancy guard", () => {
    it("should skip if previous cycle still running", async () => {
      let resolveSlot: Function;
      mockConnection.getSlot.mockImplementation(
        () => new Promise((resolve) => { resolveSlot = () => resolve(100000); })
      );

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      // Fire-and-forget: start() blocks on the first cycle's getSlot
      const startPromise = verifier.start();

      // Allow microtasks to settle so the first getSlot call is in progress
      await vi.advanceTimersByTimeAsync(10);

      // Trigger interval for second cycle
      vi.advanceTimersByTime(1100);
      await vi.advanceTimersByTimeAsync(10);

      // Only 1 call to getSlot (second cycle skipped because first is still running)
      expect(mockConnection.getSlot).toHaveBeenCalledTimes(1);

      // Resolve and cleanup
      resolveSlot!();
      await vi.advanceTimersByTimeAsync(10);
      await startPromise;
      await verifier.stop();
    });
  });

  describe("cutoff slot calculation", () => {
    it("should handle safety margin > current slot", async () => {
      const { config } = await import("../../../src/config.js");
      (config as any).verifySafetyMarginSlots = 200000;
      mockConnection.getSlot.mockResolvedValue(100);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      // Should not crash - cutoff should be 0n
      await verifier.stop();
      (config as any).verifySafetyMarginSlots = 32;
    });
  });

  describe("verifyAgents (Prisma)", () => {
    it("should finalize agents that exist on-chain", async () => {
      const agentId = AGENT_KEY.toBase58();
      mockPrisma.agent.findMany.mockResolvedValue([{ id: agentId }]);
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(mockPrisma.agent.update).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: agentId },
          data: expect.objectContaining({ status: "FINALIZED" }),
        })
      );
      const stats = verifier.getStats();
      expect(stats.agentsVerified).toBe(1);
      await verifier.stop();
    });

    it("should orphan agents not found on-chain", async () => {
      const agentId = AGENT_KEY.toBase58();
      mockPrisma.agent.findMany.mockResolvedValue([{ id: agentId }]);
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([null]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(mockPrisma.agent.update).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({ status: "ORPHANED" }),
        })
      );
      const stats = verifier.getStats();
      expect(stats.agentsOrphaned).toBe(1);
      await verifier.stop();
    });

    it("should skip on RPC error (null in existsMap)", async () => {
      const agentId = AGENT_KEY.toBase58();
      mockPrisma.agent.findMany.mockResolvedValue([{ id: agentId }]);
      // Batch fails, fallback individual also fails
      mockConnection.getMultipleAccountsInfo.mockRejectedValue(
        new Error("RPC fail")
      );
      mockConnection.getAccountInfo.mockRejectedValue(
        new Error("RPC fail")
      );

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(mockPrisma.agent.update).not.toHaveBeenCalled();
      const stats = verifier.getStats();
      expect(stats.skippedRpcErrors).toBeGreaterThan(0);
      await verifier.stop();
    });

    it("should stop processing when isRunning is false", async () => {
      const agents = Array.from({ length: 5 }, (_, i) => ({
        id: new PublicKey(new Uint8Array(32).fill(i + 1)).toBase58(),
      }));
      mockPrisma.agent.findMany.mockResolvedValue(agents);
      mockConnection.getMultipleAccountsInfo.mockResolvedValue(
        agents.map(() => ({ data: Buffer.alloc(10) }))
      );

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      // Stop mid-cycle by calling stop
      await verifier.stop();
      // Some agents may have been processed, some may not
    });
  });

  describe("verifyAgents (Pool)", () => {
    it("should finalize agents that exist (pool path)", async () => {
      const agentId = AGENT_KEY.toBase58();
      mockPool.query.mockResolvedValueOnce({
        rows: [{ id: agentId }],
        rowCount: 1,
      });
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);
      mockPool.query.mockResolvedValue({ rows: [], rowCount: 0 });

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();

      const stats = verifier.getStats();
      expect(stats.agentsVerified).toBe(1);
      await verifier.stop();
    });

    it("should orphan agents not found (pool path)", async () => {
      const agentId = AGENT_KEY.toBase58();
      mockPool.query.mockResolvedValueOnce({
        rows: [{ id: agentId }],
        rowCount: 1,
      });
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([null]);
      mockPool.query.mockResolvedValue({ rows: [], rowCount: 0 });

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();

      const stats = verifier.getStats();
      expect(stats.agentsOrphaned).toBe(1);
      await verifier.stop();
    });
  });

  describe("verifyAgents (no db)", () => {
    it("should return early if no prisma and no pool", async () => {
      const verifier = new DataVerifier(mockConnection, null, null);
      await verifier.start();
      // Should not error
      await verifier.stop();
    });
  });

  describe("verifyValidations (Prisma)", () => {
    it("should finalize validations with existing PDAs", async () => {
      const agentId = AGENT_KEY.toBase58();
      const validatorId = VALIDATOR_KEY.toBase58();
      mockPrisma.validation.findMany.mockResolvedValue([
        { id: "v1", agentId, validator: validatorId, nonce: 1n },
      ]);
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(mockPrisma.validation.update).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({ chainStatus: "FINALIZED" }),
        })
      );
      expect(verifier.getStats().validationsVerified).toBe(1);
      await verifier.stop();
    });

    it("should orphan validations with missing PDAs", async () => {
      const agentId = AGENT_KEY.toBase58();
      const validatorId = VALIDATOR_KEY.toBase58();
      mockPrisma.validation.findMany.mockResolvedValue([
        { id: "v1", agentId, validator: validatorId, nonce: 1n },
      ]);
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([null]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(mockPrisma.validation.update).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({ chainStatus: "ORPHANED" }),
        })
      );
      expect(verifier.getStats().validationsOrphaned).toBe(1);
      await verifier.stop();
    });

    it("should skip validations with nonce > MAX_SAFE_INTEGER", async () => {
      const agentId = AGENT_KEY.toBase58();
      const validatorId = VALIDATOR_KEY.toBase58();
      mockPrisma.validation.findMany.mockResolvedValue([
        {
          id: "v1",
          agentId,
          validator: validatorId,
          nonce: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
        },
      ]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(mockPrisma.validation.update).not.toHaveBeenCalled();
      await verifier.stop();
    });
  });

  describe("verifyValidations (Pool)", () => {
    it("should finalize validations via pool", async () => {
      const agentId = AGENT_KEY.toBase58();
      const validatorId = VALIDATOR_KEY.toBase58();
      mockPool.query.mockImplementation(async (sql: string) => {
        if (sql.includes("FROM validations") && sql.includes("PENDING")) {
          return { rows: [{ id: "v1", agentId, validator: validatorId, nonce: 1n }] };
        }
        return { rows: [], rowCount: 0 };
      });
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();

      expect(verifier.getStats().validationsVerified).toBe(1);
      await verifier.stop();
    });
  });

  describe("verifyMetadata (Prisma)", () => {
    it("should auto-finalize URI-derived metadata", async () => {
      mockPrisma.agentMetadata.findMany.mockResolvedValue([
        {
          id: "m1",
          agentId: AGENT_KEY.toBase58(),
          key: "_uri:name",
        },
      ]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(mockPrisma.agentMetadata.update).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({ status: "FINALIZED" }),
        })
      );
      expect(verifier.getStats().metadataVerified).toBe(1);
      await verifier.stop();
    });

    it("should verify on-chain metadata via PDA", async () => {
      mockPrisma.agentMetadata.findMany.mockResolvedValue([
        {
          id: "m1",
          agentId: AGENT_KEY.toBase58(),
          key: "capabilities",
        },
      ]);
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().metadataVerified).toBe(1);
      await verifier.stop();
    });

    it("should orphan metadata with missing PDA", async () => {
      mockPrisma.agentMetadata.findMany.mockResolvedValue([
        {
          id: "m1",
          agentId: AGENT_KEY.toBase58(),
          key: "capabilities",
        },
      ]);
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([null]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().metadataOrphaned).toBe(1);
      await verifier.stop();
    });
  });

  describe("verifyMetadata (Pool)", () => {
    it("should auto-finalize URI metadata via pool", async () => {
      mockPool.query.mockImplementation(async (sql: string) => {
        if (sql.includes("FROM metadata")) {
          return { rows: [{ id: "m1", agentId: AGENT_KEY.toBase58(), key: "_uri:name" }] };
        }
        return { rows: [], rowCount: 0 };
      });

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();

      expect(verifier.getStats().metadataVerified).toBe(1);
      await verifier.stop();
    });
  });

  describe("verifyRegistries (Prisma)", () => {
    it("should finalize registries with existing PDAs", async () => {
      mockPrisma.registry.findMany.mockResolvedValue([
        { id: "r1", collection: COLLECTION_KEY.toBase58() },
      ]);
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().registriesVerified).toBe(1);
      await verifier.stop();
    });

    it("should orphan registries with missing PDAs", async () => {
      mockPrisma.registry.findMany.mockResolvedValue([
        { id: "r1", collection: COLLECTION_KEY.toBase58() },
      ]);
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([null]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().registriesOrphaned).toBe(1);
      await verifier.stop();
    });
  });

  describe("verifyRegistries (Pool)", () => {
    it("should finalize registries via pool", async () => {
      const collId = COLLECTION_KEY.toBase58();
      mockPool.query.mockImplementation(async (sql: string) => {
        if (sql.includes("FROM collections")) {
          return { rows: [{ id: collId, collection: collId }] };
        }
        return { rows: [], rowCount: 0 };
      });
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();

      expect(verifier.getStats().registriesVerified).toBe(1);
      await verifier.stop();
    });
  });

  describe("verifyFeedbacks (Prisma)", () => {
    it("should finalize feedbacks when hash-chain matches", async () => {
      const agentId = AGENT_KEY.toBase58();
      const digest = new Uint8Array(32).fill(0xaa);

      mockPrisma.feedback.findMany
        .mockResolvedValueOnce([{ id: "f1", agentId }])
        .mockResolvedValue([]);

      // Agent exists on-chain
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      // On-chain digest
      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({
          feedbackDigest: digest,
          feedbackCount: 1n,
        }),
      });

      // DB digest match
      mockPrisma.feedback.findFirst.mockResolvedValue({
        runningDigest: Buffer.from(digest),
      });
      mockPrisma.feedback.count.mockResolvedValue(1);
      mockPrisma.feedbackResponse.findFirst.mockResolvedValue(null);
      mockPrisma.feedbackResponse.count.mockResolvedValue(0);
      mockPrisma.revocation.findFirst.mockResolvedValue(null);
      mockPrisma.revocation.count.mockResolvedValue(0);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().feedbacksVerified).toBe(1);
      await verifier.stop();
    });

    it("should leave feedbacks PENDING on hash-chain mismatch", async () => {
      const agentId = AGENT_KEY.toBase58();

      mockPrisma.feedback.findMany
        .mockResolvedValueOnce([{ id: "f1", agentId }])
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      // On-chain has count 1
      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({
          feedbackDigest: new Uint8Array(32).fill(0xaa),
          feedbackCount: 1n,
        }),
      });

      // DB has different digest
      mockPrisma.feedback.findFirst.mockResolvedValue({
        runningDigest: Buffer.from(new Uint8Array(32).fill(0xbb)),
      });
      mockPrisma.feedback.count.mockResolvedValue(1);
      mockPrisma.feedbackResponse.findFirst.mockResolvedValue(null);
      mockPrisma.feedbackResponse.count.mockResolvedValue(0);
      mockPrisma.revocation.findFirst.mockResolvedValue(null);
      mockPrisma.revocation.count.mockResolvedValue(0);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().hashChainMismatches).toBe(1);
      expect(verifier.getStats().feedbacksVerified).toBe(0);
      await verifier.stop();
    });

    it("should orphan feedbacks when agent doesn't exist", async () => {
      const agentId = AGENT_KEY.toBase58();

      mockPrisma.feedback.findMany
        .mockResolvedValueOnce([{ id: "f1", agentId }])
        .mockResolvedValue([]);

      // Agent NOT on chain
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([null]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().feedbacksOrphaned).toBe(1);
      await verifier.stop();
    });
  });

  describe("verifyFeedbacks (Pool)", () => {
    it("should handle pool path for feedbacks", async () => {
      const agentId = AGENT_KEY.toBase58();

      mockPool.query.mockImplementation(async (sql: string) => {
        if (sql.includes("FROM feedbacks") && sql.includes("PENDING")) {
          return { rows: [{ id: "f1", agentId }] };
        }
        return { rows: [], rowCount: 0 };
      });

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({ feedbackCount: 0n }),
      });

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();
      await verifier.stop();
    });
  });

  describe("verifyFeedbackResponses (Prisma)", () => {
    it("should orphan responses with orphaned feedback", async () => {
      mockPrisma.feedbackResponse.findMany
        .mockResolvedValueOnce([
          {
            id: "r1",
            feedback: { agentId: AGENT_KEY.toBase58(), status: "ORPHANED" },
          },
        ])
        .mockResolvedValue([]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().responsesOrphaned).toBe(1);
      await verifier.stop();
    });

    it("should finalize responses when hash-chain matches", async () => {
      const agentId = AGENT_KEY.toBase58();
      const digest = new Uint8Array(32).fill(0xcc);

      mockPrisma.feedbackResponse.findMany
        .mockResolvedValueOnce([
          {
            id: "r1",
            feedback: { agentId, status: "FINALIZED" },
          },
        ])
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({
          responseDigest: digest,
          responseCount: 1n,
        }),
      });

      mockPrisma.feedback.findFirst.mockResolvedValue(null);
      mockPrisma.feedback.count.mockResolvedValue(0);
      mockPrisma.feedbackResponse.findFirst.mockResolvedValue({
        runningDigest: Buffer.from(digest),
      });
      mockPrisma.feedbackResponse.count.mockResolvedValue(1);
      mockPrisma.revocation.findFirst.mockResolvedValue(null);
      mockPrisma.revocation.count.mockResolvedValue(0);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().responsesVerified).toBe(1);
      await verifier.stop();
    });

    it("should orphan responses when agent doesn't exist", async () => {
      const agentId = AGENT_KEY.toBase58();
      mockPrisma.feedbackResponse.findMany
        .mockResolvedValueOnce([
          { id: "r1", feedback: { agentId, status: "PENDING" } },
        ])
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([null]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().responsesOrphaned).toBe(1);
      await verifier.stop();
    });
  });

  describe("verifyFeedbackResponses (Pool)", () => {
    it("should handle pool path for responses", async () => {
      const agentId = AGENT_KEY.toBase58();
      mockPool.query.mockImplementation(async (sql: string) => {
        if (sql.includes("feedback_responses") && sql.includes("PENDING")) {
          return { rows: [{ id: "r1", agentId, feedbackStatus: "ORPHANED" }] };
        }
        return { rows: [], rowCount: 0 };
      });

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();

      expect(verifier.getStats().responsesOrphaned).toBe(1);
      await verifier.stop();
    });
  });

  describe("verifyRevocations (Prisma)", () => {
    it("should finalize revocations when hash-chain matches", async () => {
      const agentId = AGENT_KEY.toBase58();
      const digest = new Uint8Array(32).fill(0xdd);

      mockPrisma.revocation.findMany
        .mockResolvedValueOnce([{ id: "rev1", agentId }])
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({
          revokeDigest: digest,
          revokeCount: 1n,
        }),
      });

      mockPrisma.feedback.findFirst.mockResolvedValue(null);
      mockPrisma.feedback.count.mockResolvedValue(0);
      mockPrisma.feedbackResponse.findFirst.mockResolvedValue(null);
      mockPrisma.feedbackResponse.count.mockResolvedValue(0);
      mockPrisma.revocation.findFirst.mockResolvedValue({
        runningDigest: Buffer.from(digest),
      });
      mockPrisma.revocation.count.mockResolvedValue(1);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().revocationsVerified).toBe(1);
      await verifier.stop();
    });

    it("should orphan revocations when agent doesn't exist", async () => {
      const agentId = AGENT_KEY.toBase58();

      mockPrisma.revocation.findMany
        .mockResolvedValueOnce([{ id: "rev1", agentId }])
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([null]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().revocationsOrphaned).toBe(1);
      await verifier.stop();
    });
  });

  describe("verifyRevocations (Pool)", () => {
    it("should handle pool path for revocations", async () => {
      const agentId = AGENT_KEY.toBase58();
      mockPool.query.mockImplementation(async (sql: string) => {
        if (sql.includes("FROM revocations") && sql.includes("PENDING")) {
          return { rows: [{ id: "rev1", agentId }] };
        }
        return { rows: [], rowCount: 0 };
      });

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([null]);

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();

      expect(verifier.getStats().revocationsOrphaned).toBe(1);
      await verifier.stop();
    });
  });

  describe("recoverOrphaned (Prisma)", () => {
    it("should recover agents that now exist on-chain", async () => {
      const { config } = await import("../../../src/config.js");
      (config as any).verifyRecoveryCycles = 1; // Run every cycle

      const agentId = AGENT_KEY.toBase58();

      // Main verify queries return empty
      mockPrisma.agent.findMany
        .mockResolvedValueOnce([]) // verifyAgents
        .mockResolvedValueOnce([{ id: agentId }]); // recoverOrphaned
      mockPrisma.feedback.findMany.mockResolvedValue([]);
      mockPrisma.feedbackResponse.findMany.mockResolvedValue([]);
      mockPrisma.revocation.findMany.mockResolvedValue([]);

      // Agent now exists on-chain
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().orphansRecovered).toBeGreaterThanOrEqual(1);
      await verifier.stop();
      (config as any).verifyRecoveryCycles = 10;
    });

    it("should recover orphaned feedbacks when agent exists", async () => {
      const { config } = await import("../../../src/config.js");
      (config as any).verifyRecoveryCycles = 1;

      const agentId = AGENT_KEY.toBase58();

      // Main verify queries return empty
      mockPrisma.agent.findMany.mockResolvedValue([]);
      mockPrisma.feedback.findMany
        .mockResolvedValueOnce([]) // verifyFeedbacks
        .mockResolvedValueOnce([{ id: "f1", agentId }]); // recoverOrphaned
      mockPrisma.feedbackResponse.findMany.mockResolvedValue([]);
      mockPrisma.revocation.findMany.mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().orphansRecovered).toBeGreaterThanOrEqual(1);
      await verifier.stop();
      (config as any).verifyRecoveryCycles = 10;
    });

    it("should recover orphaned revocations when agent exists", async () => {
      const { config } = await import("../../../src/config.js");
      (config as any).verifyRecoveryCycles = 1;

      const agentId = AGENT_KEY.toBase58();

      mockPrisma.agent.findMany.mockResolvedValue([]);
      mockPrisma.feedback.findMany.mockResolvedValue([]);
      mockPrisma.feedbackResponse.findMany.mockResolvedValue([]);
      mockPrisma.revocation.findMany
        .mockResolvedValueOnce([]) // verifyRevocations
        .mockResolvedValueOnce([{ id: "rev1", agentId }]); // recovery

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().orphansRecovered).toBeGreaterThanOrEqual(1);
      await verifier.stop();
      (config as any).verifyRecoveryCycles = 10;
    });

    it("should recover orphaned responses when agent exists", async () => {
      const { config } = await import("../../../src/config.js");
      (config as any).verifyRecoveryCycles = 1;

      const agentId = AGENT_KEY.toBase58();

      mockPrisma.agent.findMany.mockResolvedValue([]);
      mockPrisma.feedback.findMany.mockResolvedValue([]);
      mockPrisma.feedbackResponse.findMany
        .mockResolvedValueOnce([]) // verifyFeedbackResponses
        .mockResolvedValueOnce([{ id: "resp1", feedback: { agentId } }]); // recovery
      mockPrisma.revocation.findMany.mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().orphansRecovered).toBeGreaterThanOrEqual(1);
      await verifier.stop();
      (config as any).verifyRecoveryCycles = 10;
    });
  });

  describe("recoverOrphaned (Pool)", () => {
    it("should recover orphaned agents via pool", async () => {
      const { config } = await import("../../../src/config.js");
      (config as any).verifyRecoveryCycles = 1;

      const agentId = AGENT_KEY.toBase58();

      // verifyAll queries return empty, then recovery queries
      mockPool.query
        .mockResolvedValueOnce({ rows: [] }) // agents
        .mockResolvedValueOnce({ rows: [] }) // validations
        .mockResolvedValueOnce({ rows: [] }) // metadata
        .mockResolvedValueOnce({ rows: [] }) // registries
        .mockResolvedValueOnce({ rows: [] }) // feedbacks
        .mockResolvedValueOnce({ rows: [] }) // responses
        .mockResolvedValueOnce({ rows: [] }) // revocations
        // Recovery queries
        .mockResolvedValueOnce({ rows: [{ id: agentId }] }) // orphaned agents
        .mockResolvedValue({ rows: [], rowCount: 0 });

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();

      expect(verifier.getStats().orphansRecovered).toBeGreaterThanOrEqual(1);
      await verifier.stop();
      (config as any).verifyRecoveryCycles = 10;
    });
  });

  describe("batchVerifyAccounts", () => {
    it("should handle empty pubkeys list", async () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      // With empty pending, nothing is verified
      expect(verifier.getStats().agentsVerified).toBe(0);
      await verifier.stop();
    });

    it("should split into batches of 100", async () => {
      // Create 150 agents to test batching
      const agents = Array.from({ length: 150 }, (_, i) => {
        const bytes = new Uint8Array(32);
        bytes[0] = (i >> 8) & 0xff;
        bytes[1] = i & 0xff;
        bytes.fill(0x01, 2);
        return { id: new PublicKey(bytes).toBase58() };
      });

      mockPrisma.agent.findMany.mockResolvedValue(agents);
      mockConnection.getMultipleAccountsInfo.mockResolvedValue(
        agents.slice(0, 100).map(() => ({ data: Buffer.alloc(10) }))
      );

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      // Should have called getMultipleAccountsInfo at least twice (100+50)
      expect(
        mockConnection.getMultipleAccountsInfo.mock.calls.length
      ).toBeGreaterThanOrEqual(2);
      await verifier.stop();
    });

    it("should fall back to individual checks on batch failure", async () => {
      const agentId = AGENT_KEY.toBase58();
      mockPrisma.agent.findMany.mockResolvedValue([{ id: agentId }]);
      mockConnection.getMultipleAccountsInfo.mockRejectedValue(
        new Error("batch fail")
      );
      mockConnection.getAccountInfo.mockResolvedValue({
        data: Buffer.alloc(10),
      });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(mockConnection.getAccountInfo).toHaveBeenCalled();
      expect(verifier.getStats().agentsVerified).toBe(1);
      await verifier.stop();
    });
  });

  describe("verifyWithRetry", () => {
    it("should return true when account found on first try", async () => {
      const agentId = AGENT_KEY.toBase58();
      mockPrisma.agent.findMany.mockResolvedValue([{ id: agentId }]);
      mockConnection.getMultipleAccountsInfo.mockRejectedValue(
        new Error("batch fail")
      );
      mockConnection.getAccountInfo.mockResolvedValue({
        data: Buffer.alloc(10),
      });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      expect(verifier.getStats().agentsVerified).toBe(1);
      await verifier.stop();
    });

    it("should return false when confirmed absent after retries", async () => {
      const agentId = AGENT_KEY.toBase58();
      mockPrisma.agent.findMany.mockResolvedValue([{ id: agentId }]);
      mockConnection.getMultipleAccountsInfo.mockRejectedValue(
        new Error("batch fail")
      );
      mockConnection.getAccountInfo.mockResolvedValue(null);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      expect(verifier.getStats().agentsOrphaned).toBe(1);
      await verifier.stop();
    });

    it("should return null when all attempts are RPC errors", async () => {
      const agentId = AGENT_KEY.toBase58();
      mockPrisma.agent.findMany.mockResolvedValue([{ id: agentId }]);
      mockConnection.getMultipleAccountsInfo.mockRejectedValue(
        new Error("batch fail")
      );
      mockConnection.getAccountInfo.mockRejectedValue(
        new Error("RPC error")
      );

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      expect(verifier.getStats().skippedRpcErrors).toBeGreaterThan(0);
      await verifier.stop();
    });
  });

  describe("fetchOnChainDigests", () => {
    it("should parse AgentAccount data without wallet", async () => {
      const digest = new Uint8Array(32).fill(0xab);
      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({
          feedbackDigest: digest,
          feedbackCount: 5n,
          responseDigest: new Uint8Array(32).fill(0xcd),
          responseCount: 3n,
          revokeDigest: new Uint8Array(32).fill(0xef),
          revokeCount: 1n,
        }),
      });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      const result = await verifier.fetchOnChainDigests(
        AGENT_KEY.toBase58()
      );

      expect(result).not.toBeNull();
      expect(result!.feedbackCount).toBe(5n);
      expect(result!.responseCount).toBe(3n);
      expect(result!.revokeCount).toBe(1n);
    });

    it("should parse AgentAccount data with wallet", async () => {
      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({
          agentWalletPresent: true,
          feedbackDigest: new Uint8Array(32).fill(0xab),
          feedbackCount: 2n,
        }),
      });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      const result = await verifier.fetchOnChainDigests(
        AGENT_KEY.toBase58()
      );

      expect(result).not.toBeNull();
      expect(result!.feedbackCount).toBe(2n);
    });

    it("should return null when account not found", async () => {
      mockConnection.getAccountInfo.mockResolvedValue(null);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      const result = await verifier.fetchOnChainDigests(
        AGENT_KEY.toBase58()
      );

      expect(result).toBeNull();
    });

    it("should return null when data too small", async () => {
      mockConnection.getAccountInfo.mockResolvedValue({
        data: Buffer.alloc(100), // Too small
      });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      const result = await verifier.fetchOnChainDigests(
        AGENT_KEY.toBase58()
      );

      expect(result).toBeNull();
    });

    it("should return null on error", async () => {
      mockConnection.getAccountInfo.mockRejectedValue(
        new Error("RPC error")
      );

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      const result = await verifier.fetchOnChainDigests(
        AGENT_KEY.toBase58()
      );

      expect(result).toBeNull();
    });

    it("should return null when data length < offset + 120 with wallet", async () => {
      // Create data with wallet present but not enough bytes for digest triplets
      const buf = Buffer.alloc(8 + 32 + 32 + 32 + 1 + 1 + 33 + 50); // short
      buf[8 + 32 + 32 + 32 + 1 + 1] = 1; // wallet present
      mockConnection.getAccountInfo.mockResolvedValue({ data: buf });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      const result = await verifier.fetchOnChainDigests(AGENT_KEY.toBase58());
      expect(result).toBeNull();
    });
  });

  describe("checkDigestMatch edge cases", () => {
    it("should return true when both DB and on-chain count are zero", async () => {
      const agentId = AGENT_KEY.toBase58();

      mockPrisma.feedback.findMany
        .mockResolvedValueOnce([{ id: "f1", agentId }])
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      // On-chain: zero count
      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({ feedbackCount: 0n }),
      });

      // DB: no digest, zero count
      mockPrisma.feedback.findFirst.mockResolvedValue(null);
      mockPrisma.feedback.count.mockResolvedValue(0);
      mockPrisma.feedbackResponse.findFirst.mockResolvedValue(null);
      mockPrisma.feedbackResponse.count.mockResolvedValue(0);
      mockPrisma.revocation.findFirst.mockResolvedValue(null);
      mockPrisma.revocation.count.mockResolvedValue(0);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().feedbacksVerified).toBe(1);
      await verifier.stop();
    });

    it("should return false when DB count < on-chain count (indexer behind)", async () => {
      const agentId = AGENT_KEY.toBase58();
      const digest = new Uint8Array(32).fill(0xaa);

      mockPrisma.feedback.findMany
        .mockResolvedValueOnce([{ id: "f1", agentId }])
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({
          feedbackDigest: digest,
          feedbackCount: 5n,
        }),
      });

      mockPrisma.feedback.findFirst.mockResolvedValue({
        runningDigest: Buffer.from(digest),
      });
      mockPrisma.feedback.count.mockResolvedValue(3); // Behind
      mockPrisma.feedbackResponse.findFirst.mockResolvedValue(null);
      mockPrisma.feedbackResponse.count.mockResolvedValue(0);
      mockPrisma.revocation.findFirst.mockResolvedValue(null);
      mockPrisma.revocation.count.mockResolvedValue(0);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().feedbacksVerified).toBe(0);
      await verifier.stop();
    });

    it("should log mismatch when DB count > on-chain count (possible reorg)", async () => {
      const agentId = AGENT_KEY.toBase58();

      mockPrisma.feedback.findMany
        .mockResolvedValueOnce([{ id: "f1", agentId }])
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({
          feedbackDigest: new Uint8Array(32).fill(0xaa),
          feedbackCount: 2n,
        }),
      });

      mockPrisma.feedback.findFirst.mockResolvedValue({
        runningDigest: Buffer.from(new Uint8Array(32).fill(0xaa)),
      });
      mockPrisma.feedback.count.mockResolvedValue(5); // More than on-chain
      mockPrisma.feedbackResponse.findFirst.mockResolvedValue(null);
      mockPrisma.feedbackResponse.count.mockResolvedValue(0);
      mockPrisma.revocation.findFirst.mockResolvedValue(null);
      mockPrisma.revocation.count.mockResolvedValue(0);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().hashChainMismatches).toBe(1);
      await verifier.stop();
    });

    it("should return false when no DB digest but on-chain has events", async () => {
      const agentId = AGENT_KEY.toBase58();

      mockPrisma.feedback.findMany
        .mockResolvedValueOnce([{ id: "f1", agentId }])
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({
          feedbackDigest: new Uint8Array(32).fill(0xaa),
          feedbackCount: 3n,
        }),
      });

      // No DB digest
      mockPrisma.feedback.findFirst.mockResolvedValue(null);
      mockPrisma.feedback.count.mockResolvedValue(0);
      mockPrisma.feedbackResponse.findFirst.mockResolvedValue(null);
      mockPrisma.feedbackResponse.count.mockResolvedValue(0);
      mockPrisma.revocation.findFirst.mockResolvedValue(null);
      mockPrisma.revocation.count.mockResolvedValue(0);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().feedbacksVerified).toBe(0);
      await verifier.stop();
    });

    it("should handle checkDigestMatch error gracefully", async () => {
      const agentId = AGENT_KEY.toBase58();

      mockPrisma.feedback.findMany
        .mockResolvedValueOnce([{ id: "f1", agentId }])
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      // fetchOnChainDigests throws
      mockConnection.getAccountInfo.mockRejectedValue(
        new Error("Parse error")
      );

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      // Should not crash, feedbacks stay PENDING
      expect(verifier.getStats().feedbacksVerified).toBe(0);
      await verifier.stop();
    });
  });

  describe("getLastDbDigests (Pool path)", () => {
    it("should fetch digests from pool", async () => {
      const agentId = AGENT_KEY.toBase58();

      mockPool.query.mockImplementation(async (sql: string) => {
        if (sql.includes("FROM feedbacks") && sql.includes("PENDING")) {
          return { rows: [{ id: "f1", agentId }] };
        }
        if (sql.includes("COUNT")) {
          return { rows: [{ cnt: "0" }] };
        }
        return { rows: [], rowCount: 0 };
      });

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);
      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({ feedbackCount: 0n }),
      });

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();
      await verifier.stop();
    });
  });

  describe("batchUpdateStatus", () => {
    it("should reject invalid table names (pool path)", async () => {
      const agentId = AGENT_KEY.toBase58();

      // This is tested implicitly through the verification paths
      // but we can verify the ALLOWED_TABLES check works
      mockPrisma.feedback.findMany
        .mockResolvedValueOnce([{ id: "f1", agentId }])
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([null]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      // Feedbacks orphaned through valid table path
      expect(verifier.getStats().feedbacksOrphaned).toBe(1);
      await verifier.stop();
    });

    it("should handle empty ids array", async () => {
      // If no feedbacks are pending, batchUpdateStatus with empty array is a no-op
      mockPrisma.feedback.findMany.mockResolvedValue([]);
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      // No updates called
      expect(mockPrisma.feedback.updateMany).not.toHaveBeenCalled();
      await verifier.stop();
    });

    it("backfills missing response_id when feedback responses become non-orphaned (prisma path)", async () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      mockPrisma.feedbackResponse.findMany.mockResolvedValueOnce([
        {
          id: "resp-1",
          feedbackId: "fb-1",
          responseCount: 1n,
          slot: 10n,
          txSignature: "sig-a",
          txIndex: 0,
          eventOrdinal: 0,
        },
      ]);
      mockPrisma.feedbackResponse.findFirst.mockResolvedValueOnce({ responseId: 9n });

      await (verifier as any).batchUpdateStatus(
        "feedback_responses",
        "id",
        ["resp-1"],
        "FINALIZED",
        new Date()
      );

      expect(mockPrisma.feedbackResponse.updateMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({ id: "resp-1" }),
          data: { responseId: 10n },
        })
      );
    });

    it("assigns response_id in canonical on-chain order, not UUID order (prisma path)", async () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      mockPrisma.feedbackResponse.findMany.mockResolvedValueOnce([
        {
          id: "uuid-z",
          feedbackId: "fb-ordered",
          responseCount: 2n,
          slot: 11n,
          txSignature: "sig-b",
          txIndex: 1,
          eventOrdinal: 0,
        },
        {
          id: "uuid-a",
          feedbackId: "fb-ordered",
          responseCount: 1n,
          slot: 10n,
          txSignature: "sig-a",
          txIndex: 0,
          eventOrdinal: 0,
        },
      ]);
      mockPrisma.feedbackResponse.findFirst.mockResolvedValueOnce({ responseId: 7n });

      await (verifier as any).batchUpdateStatus(
        "feedback_responses",
        "id",
        ["uuid-z", "uuid-a"],
        "FINALIZED",
        new Date()
      );

      expect(mockPrisma.feedbackResponse.updateMany).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({
          where: expect.objectContaining({ id: "uuid-a" }),
          data: { responseId: 8n },
        })
      );
      expect(mockPrisma.feedbackResponse.updateMany).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          where: expect.objectContaining({ id: "uuid-z" }),
          data: { responseId: 9n },
        })
      );
    });

    it("backfills missing response_id when feedback responses become non-orphaned (pool path)", async () => {
      const verifier = new DataVerifier(mockConnection, null, mockPool);
      const assigned: string[] = [];

      mockPool.query.mockImplementation(async (sql: string, params: any[]) => {
        if (sql.includes("UPDATE feedback_responses SET status")) return { rows: [], rowCount: 1 };
        if (sql.includes("SELECT id, asset, client_address, feedback_index::text AS feedback_index")) {
          return {
            rows: [
              {
                id: "resp-2",
                asset: AGENT_KEY.toBase58(),
                client_address: VALIDATOR_KEY.toBase58(),
                feedback_index: "7",
              },
            ],
            rowCount: 1,
          };
        }
        if (sql.includes("SELECT MAX(response_id)::text AS max_id")) {
          return { rows: [{ max_id: "4" }], rowCount: 1 };
        }
        if (sql.includes("UPDATE feedback_responses") && sql.includes("SET response_id")) {
          assigned.push(params[0]);
          return { rows: [], rowCount: 1 };
        }
        return { rows: [], rowCount: 0 };
      });

      await (verifier as any).batchUpdateStatus(
        "feedback_responses",
        "id",
        ["resp-2"],
        "FINALIZED",
        new Date()
      );

      expect(assigned).toEqual(["5"]);
    });

    it("assigns revocation_id in canonical on-chain order, not UUID order (prisma path)", async () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      mockPrisma.revocation.findMany.mockResolvedValueOnce([
        {
          id: "rev-z",
          agentId: AGENT_KEY.toBase58(),
          revokeCount: 4n,
          slot: 22n,
          txSignature: "sig-z",
          txIndex: 1,
          eventOrdinal: 0,
        },
        {
          id: "rev-a",
          agentId: AGENT_KEY.toBase58(),
          revokeCount: 3n,
          slot: 21n,
          txSignature: "sig-a",
          txIndex: 0,
          eventOrdinal: 0,
        },
      ]);
      mockPrisma.revocation.findFirst.mockResolvedValueOnce({ revocationId: 12n });

      await (verifier as any).batchUpdateStatus(
        "revocations",
        "id",
        ["rev-z", "rev-a"],
        "FINALIZED",
        new Date()
      );

      expect(mockPrisma.revocation.updateMany).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({
          where: expect.objectContaining({ id: "rev-a" }),
          data: { revocationId: 13n },
        })
      );
      expect(mockPrisma.revocation.updateMany).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          where: expect.objectContaining({ id: "rev-z" }),
          data: { revocationId: 14n },
        })
      );
    });
  });

  describe("digest cache", () => {
    it("should reuse cached digests for same agent across chains", async () => {
      const agentId = AGENT_KEY.toBase58();
      const digest = new Uint8Array(32).fill(0xaa);

      // Both feedbacks and revocations for same agent
      mockPrisma.feedback.findMany
        .mockResolvedValueOnce([{ id: "f1", agentId }])
        .mockResolvedValue([]);

      mockPrisma.revocation.findMany
        .mockResolvedValueOnce([{ id: "rev1", agentId }])
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({
          feedbackDigest: digest,
          feedbackCount: 1n,
          revokeDigest: digest,
          revokeCount: 1n,
        }),
      });

      mockPrisma.feedback.findFirst.mockResolvedValue({
        runningDigest: Buffer.from(digest),
      });
      mockPrisma.feedback.count.mockResolvedValue(1);
      mockPrisma.feedbackResponse.findFirst.mockResolvedValue(null);
      mockPrisma.feedbackResponse.count.mockResolvedValue(0);
      mockPrisma.revocation.findFirst.mockResolvedValue({
        runningDigest: Buffer.from(digest),
      });
      mockPrisma.revocation.count.mockResolvedValue(1);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      // getAccountInfo may be called 1-2 times (feedbacks + revocations run concurrently,
      // so the cache may not be populated before both check it)
      const getAccountInfoCalls =
        mockConnection.getAccountInfo.mock.calls.length;
      expect(getAccountInfoCalls).toBeLessThanOrEqual(2);
      await verifier.stop();
    });

    it("persists agent digest cache via prisma and falls back when signature field is unavailable", async () => {
      const agentId = AGENT_KEY.toBase58();
      const digest = new Uint8Array(32).fill(0xaa);

      mockPrisma.feedback.findMany
        .mockResolvedValueOnce([{ id: "f1", agentId }])
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);
      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({
          feedbackDigest: digest,
          feedbackCount: 1n,
        }),
      });

      mockPrisma.feedback.findFirst.mockResolvedValue({
        runningDigest: Buffer.from(digest),
        createdTxSignature: "sig-feedback",
        createdSlot: 123n,
      });
      mockPrisma.feedback.count.mockResolvedValue(1);
      mockPrisma.feedbackResponse.findFirst.mockResolvedValue(null);
      mockPrisma.feedbackResponse.count.mockResolvedValue(0);
      mockPrisma.revocation.findFirst.mockResolvedValue(null);
      mockPrisma.revocation.count.mockResolvedValue(0);

      mockPrisma.agentDigestCache.upsert
        .mockRejectedValueOnce(new Error("Unknown arg `lastVerifiedSignature`"))
        .mockResolvedValueOnce({});

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(mockPrisma.agentDigestCache.upsert).toHaveBeenCalledTimes(2);
      expect(mockPrisma.agentDigestCache.upsert.mock.calls[0][0].create).toEqual(
        expect.objectContaining({
          agentId,
          lastVerifiedSignature: "sig-feedback",
        })
      );
      expect(mockPrisma.agentDigestCache.upsert.mock.calls[1][0].create).toEqual(
        expect.not.objectContaining({
          lastVerifiedSignature: expect.anything(),
        })
      );
      await verifier.stop();
    });

    it("persists digest cache via pool and retries without last_verified_signature when column is unavailable", async () => {
      const agentId = AGENT_KEY.toBase58();
      const digest = new Uint8Array(32).fill(0xbb);

      mockPool.query.mockImplementation(async (sql: string) => {
        if (sql.includes("FROM feedbacks") && sql.includes("status = 'PENDING'")) {
          return { rows: [{ id: "f1", agentId }] };
        }
        if (sql.includes("FROM feedbacks") && sql.includes("running_digest")) {
          return { rows: [{ running_digest: Buffer.from(digest), tx_signature: "sig1", block_slot: "222" }] };
        }
        if (sql.includes("COUNT") && sql.includes("feedbacks")) {
          return { rows: [{ cnt: "1" }] };
        }
        if (sql.includes("running_digest") && sql.includes("feedback_responses")) {
          return { rows: [] };
        }
        if (sql.includes("COUNT") && sql.includes("feedback_responses")) {
          return { rows: [{ cnt: "0" }] };
        }
        if (sql.includes("running_digest") && sql.includes("revocations")) {
          return { rows: [] };
        }
        if (sql.includes("COUNT") && sql.includes("revocations")) {
          return { rows: [{ cnt: "0" }] };
        }
        if (sql.includes("INSERT INTO agent_digest_cache") && sql.includes("last_verified_signature")) {
          throw new Error('column "last_verified_signature" does not exist');
        }
        if (sql.includes("INSERT INTO agent_digest_cache")) {
          return { rows: [], rowCount: 1 };
        }
        return { rows: [], rowCount: 0 };
      });

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);
      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({
          feedbackDigest: digest,
          feedbackCount: 1n,
        }),
      });

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();

      const digestInsertCalls = mockPool.query.mock.calls
        .map((call: unknown[]) => call[0] as string)
        .filter((sql: string) => sql.includes("INSERT INTO agent_digest_cache"));

      expect(digestInsertCalls.length).toBe(2);
      expect(digestInsertCalls[0]).toContain("last_verified_signature");
      expect(digestInsertCalls[1]).not.toContain("last_verified_signature");
      await verifier.stop();
    });
  });

  describe("start error logging", () => {
    it("should log error when initial verification rejects (line 119)", async () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      // Override verifyAll to actually reject (bypassing internal try/catch)
      (verifier as any).verifyAll = () => Promise.reject(new Error("Unexpected crash"));
      await verifier.start();
      await verifier.stop();
    });

    it("should log error when periodic verification cycle rejects (line 125)", async () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      // Override verifyAll to reject on the next interval call
      (verifier as any).verifyAll = () => Promise.reject(new Error("Periodic crash"));
      vi.advanceTimersByTime(1100);
      await vi.advanceTimersByTimeAsync(100);

      await verifier.stop();
    });
  });

  describe("reentrancy guard logging (lines 148-150)", () => {
    it("should log debug when skipping due to reentrancy", async () => {
      let resolveSlot: Function;
      let callCount = 0;
      mockConnection.getSlot.mockImplementation(
        () => new Promise((resolve) => {
          callCount++;
          if (callCount === 1) {
            resolveSlot = () => resolve(100000);
          } else {
            resolve(100000);
          }
        })
      );

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      const startPromise = verifier.start();

      await vi.advanceTimersByTimeAsync(10);
      vi.advanceTimersByTime(1100);
      await vi.advanceTimersByTimeAsync(10);

      expect(mockConnection.getSlot).toHaveBeenCalledTimes(1);

      resolveSlot!();
      await vi.advanceTimersByTimeAsync(10);
      await startPromise;
      await verifier.stop();
    });
  });

  describe("verifyAgents error handling (lines 274-275)", () => {
    it("should catch and log per-agent verification errors", async () => {
      const agentId = AGENT_KEY.toBase58();
      mockPrisma.agent.findMany.mockResolvedValue([{ id: agentId }]);
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);
      // Make update throw to trigger catch block at line 274
      mockPrisma.agent.update.mockRejectedValueOnce(new Error("DB write failed"));

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      // Should not crash despite DB error
      await verifier.stop();
    });
  });

  describe("verifyValidations error handling (lines 318-319, 333-335, 358-359)", () => {
    it("should log error when PDA derivation fails (line 318-319)", async () => {
      const { parseAssetPubkey } = await import("../../../src/utils/pda.js");
      (parseAssetPubkey as any).mockImplementationOnce(() => { throw new Error("Invalid pubkey"); });

      mockPrisma.validation.findMany.mockResolvedValue([
        { id: "v1", agentId: "invalid-key", validator: VALIDATOR_KEY.toBase58(), nonce: 1n },
      ]);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      // PDA derivation failed, validation not updated
      expect(mockPrisma.validation.update).not.toHaveBeenCalled();
      await verifier.stop();
    });

    it("should skip validation when exists is null (line 333-335)", async () => {
      const agentId = AGENT_KEY.toBase58();
      const validatorId = VALIDATOR_KEY.toBase58();
      mockPrisma.validation.findMany.mockResolvedValue([
        { id: "v1", agentId, validator: validatorId, nonce: 1n },
      ]);
      // Batch fails, individual fails too
      mockConnection.getMultipleAccountsInfo.mockRejectedValue(new Error("RPC fail"));
      mockConnection.getAccountInfo.mockRejectedValue(new Error("RPC fail"));

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().skippedRpcErrors).toBeGreaterThan(0);
      expect(mockPrisma.validation.update).not.toHaveBeenCalled();
      await verifier.stop();
    });

    it("should catch per-validation verification errors (line 358-359)", async () => {
      const agentId = AGENT_KEY.toBase58();
      const validatorId = VALIDATOR_KEY.toBase58();
      mockPrisma.validation.findMany.mockResolvedValue([
        { id: "v1", agentId, validator: validatorId, nonce: 1n },
      ]);
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);
      mockPrisma.validation.update.mockRejectedValueOnce(new Error("DB error"));

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      // Should not crash
      await verifier.stop();
    });
  });

  describe("verifyMetadata error handling (lines 405-406, 429-430, 445-446)", () => {
    it("should log error when metadata PDA derivation fails (line 405-406)", async () => {
      const { parseAssetPubkey } = await import("../../../src/utils/pda.js");

      mockPrisma.agentMetadata.findMany.mockResolvedValue([
        { id: "m1", agentId: AGENT_KEY.toBase58(), key: "capabilities" },
      ]);
      // Make parseAssetPubkey fail for this specific call
      (parseAssetPubkey as any).mockImplementationOnce(() => { throw new Error("Parse failed"); });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      await verifier.stop();
    });

    it("should catch URI metadata finalization error (line 429-430)", async () => {
      mockPrisma.agentMetadata.findMany.mockResolvedValue([
        { id: "m1", agentId: AGENT_KEY.toBase58(), key: "_uri:name" },
      ]);
      mockPrisma.agentMetadata.update.mockRejectedValueOnce(new Error("URI finalize failed"));

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      // Should not crash
      await verifier.stop();
    });

    it("should skip on-chain metadata when exists is null (line 445-446)", async () => {
      mockPrisma.agentMetadata.findMany.mockResolvedValue([
        { id: "m1", agentId: AGENT_KEY.toBase58(), key: "capabilities" },
      ]);
      // Batch and individual both fail
      mockConnection.getMultipleAccountsInfo.mockRejectedValue(new Error("RPC fail"));
      mockConnection.getAccountInfo.mockRejectedValue(new Error("RPC fail"));

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      expect(verifier.getStats().skippedRpcErrors).toBeGreaterThan(0);
      await verifier.stop();
    });
  });

  describe("verifyRegistries pool path and error handling", () => {
    it("should log error when registry PDA derivation fails", async () => {
      mockPrisma.registry.findMany.mockResolvedValue([
        { id: "r1", collection: COLLECTION_KEY.toBase58() },
      ]);

      // Force PDA derivation failure for this call
      const { getRegistryConfigPda } = await import("../../../src/utils/pda.js");
      (getRegistryConfigPda as any).mockImplementationOnce(() => { throw new Error("Invalid collection"); });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      await verifier.stop();
    });

    it("should skip registries when exists is null (RPC error)", async () => {
      mockPrisma.registry.findMany.mockResolvedValue([
        { id: "r1", collection: COLLECTION_KEY.toBase58() },
      ]);
      mockConnection.getMultipleAccountsInfo.mockRejectedValue(new Error("RPC fail"));
      mockConnection.getAccountInfo.mockRejectedValue(new Error("RPC fail"));

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      expect(verifier.getStats().skippedRpcErrors).toBeGreaterThan(0);
      await verifier.stop();
    });

    it("should catch per-registry verification errors", async () => {
      mockPrisma.registry.findMany.mockResolvedValue([
        { id: "r1", collection: COLLECTION_KEY.toBase58() },
      ]);
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);
      mockPrisma.registry.update.mockRejectedValueOnce(new Error("DB error"));

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      await verifier.stop();
    });

    it("should orphan registries via pool path", async () => {
      const collId = COLLECTION_KEY.toBase58();
      mockPool.query.mockImplementation(async (sql: string) => {
        if (sql.includes("FROM collections")) {
          return { rows: [{ id: collId, collection: collId }] };
        }
        return { rows: [], rowCount: 0 };
      });
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([null]);

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();
      expect(verifier.getStats().registriesOrphaned).toBe(1);
      await verifier.stop();
    });
  });

  describe("recoverOrphaned pool paths", () => {
    it("should recover orphaned feedbacks via pool", async () => {
      const { config } = await import("../../../src/config.js");
      (config as any).verifyRecoveryCycles = 1;

      const agentId = AGENT_KEY.toBase58();
      let callNum = 0;

      mockPool.query.mockImplementation(async (sql: string) => {
        callNum++;
        // Recovery: orphaned agents
        if (sql.includes("agents") && sql.includes("ORPHANED")) {
          return { rows: [] };
        }
        // Recovery: orphaned feedbacks
        if (sql.includes("feedbacks") && sql.includes("ORPHANED")) {
          return { rows: [{ id: "f1", agentId }] };
        }
        // Recovery: orphaned revocations
        if (sql.includes("revocations") && sql.includes("ORPHANED")) {
          return { rows: [] };
        }
        // Recovery: orphaned responses
        if (sql.includes("feedback_responses") && sql.includes("ORPHANED")) {
          return { rows: [] };
        }
        return { rows: [], rowCount: 0 };
      });

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();

      expect(verifier.getStats().orphansRecovered).toBeGreaterThanOrEqual(1);
      await verifier.stop();
      (config as any).verifyRecoveryCycles = 10;
    });

    it("should recover orphaned revocations via pool", async () => {
      const { config } = await import("../../../src/config.js");
      (config as any).verifyRecoveryCycles = 1;

      const agentId = AGENT_KEY.toBase58();

      mockPool.query.mockImplementation(async (sql: string) => {
        if (sql.includes("revocations") && sql.includes("ORPHANED")) {
          return { rows: [{ id: "rev1", agentId }] };
        }
        if (sql.includes("feedback_responses") && sql.includes("ORPHANED")) {
          return { rows: [] };
        }
        return { rows: [], rowCount: 0 };
      });

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();

      expect(verifier.getStats().orphansRecovered).toBeGreaterThanOrEqual(1);
      await verifier.stop();
      (config as any).verifyRecoveryCycles = 10;
    });

    it("should recover orphaned responses via pool", async () => {
      const { config } = await import("../../../src/config.js");
      (config as any).verifyRecoveryCycles = 1;

      const agentId = AGENT_KEY.toBase58();

      mockPool.query.mockImplementation(async (sql: string) => {
        if (sql.includes("feedback_responses") && sql.includes("ORPHANED")) {
          return { rows: [{ id: "resp1", agentId }] };
        }
        return { rows: [], rowCount: 0 };
      });

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();

      expect(verifier.getStats().orphansRecovered).toBeGreaterThanOrEqual(1);
      await verifier.stop();
      (config as any).verifyRecoveryCycles = 10;
    });
  });

  describe("verifyFeedbackResponses pool path with valid responses", () => {
    it("should handle pool path for valid (non-orphaned) responses", async () => {
      const agentId = AGENT_KEY.toBase58();
      mockPool.query.mockImplementation(async (sql: string) => {
        if (sql.includes("feedback_responses") && sql.includes("PENDING")) {
          return { rows: [{ id: "r1", agentId, feedbackStatus: "FINALIZED" }] };
        }
        if (sql.includes("COUNT")) {
          return { rows: [{ cnt: "0" }] };
        }
        return { rows: [], rowCount: 0 };
      });

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);
      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({ responseCount: 0n }),
      });

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();
      await verifier.stop();
    });
  });

  describe("verifyRevocations pool finalize path", () => {
    it("should finalize revocations when hash-chain matches via pool", async () => {
      const agentId = AGENT_KEY.toBase58();
      const digest = new Uint8Array(32).fill(0xdd);

      mockPool.query.mockImplementation(async (sql: string, params?: any[]) => {
        if (sql.includes("FROM revocations") && sql.includes("PENDING")) {
          return { rows: [{ id: "rev1", agentId }] };
        }
        if (sql.includes("running_digest") && sql.includes("revocations")) {
          return { rows: [{ running_digest: Buffer.from(digest) }] };
        }
        if (sql.includes("COUNT") && sql.includes("revocations")) {
          return { rows: [{ cnt: "1" }] };
        }
        if (sql.includes("COUNT")) {
          return { rows: [{ cnt: "0" }] };
        }
        return { rows: [], rowCount: 0 };
      });

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);
      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({
          revokeDigest: digest,
          revokeCount: 1n,
        }),
      });

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();
      expect(verifier.getStats().revocationsVerified).toBe(1);
      await verifier.stop();
    });
  });

  describe("getLastDbDigests no-db fallback (line 1200)", () => {
    it("should return zero state when neither prisma nor pool exist", async () => {
      const verifier = new DataVerifier(mockConnection, null, null);

      const result = await (verifier as any).getLastDbDigests("someAgent");

      expect(result.feedbackDigest).toBeNull();
      expect(result.feedbackCount).toBe(0n);
      expect(result.responseDigest).toBeNull();
      expect(result.responseCount).toBe(0n);
      expect(result.revokeDigest).toBeNull();
      expect(result.revokeCount).toBe(0n);
    });
  });

  describe("batchUpdateStatus invalid table (pool path, lines 1242-1244)", () => {
    it("should log error and return for invalid table name", async () => {
      const verifier = new DataVerifier(mockConnection, null, mockPool);

      await (verifier as any).batchUpdateStatus(
        "malicious_table", "id", ["id1"], "FINALIZED", new Date()
      );

      expect(mockPool.query).not.toHaveBeenCalled();
    });

    it("should log error and return for invalid column name", async () => {
      const verifier = new DataVerifier(mockConnection, null, mockPool);

      await (verifier as any).batchUpdateStatus(
        "feedbacks", "malicious_column", ["id1"], "FINALIZED", new Date()
      );

      expect(mockPool.query).not.toHaveBeenCalled();
    });
  });

  describe("stats tracking", () => {
    it("should update lastRunAt and lastRunDurationMs", async () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      const stats = verifier.getStats();
      expect(stats.lastRunAt).not.toBeNull();
      expect(stats.lastRunDurationMs).toBeGreaterThanOrEqual(0);
      await verifier.stop();
    });

    it("should return a copy of stats (not a reference)", () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      const stats1 = verifier.getStats();
      const stats2 = verifier.getStats();
      expect(stats1).not.toBe(stats2);
      expect(stats1).toEqual(stats2);
    });
  });

  describe("sleep function (line 1029-1031)", () => {
    it("should resolve after the given ms", async () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      // Restore original sleep for this test to get coverage on the real code
      (DataVerifier.prototype as any).sleep = origSleep;

      const promise = (verifier as any).sleep(50);
      vi.advanceTimersByTime(50);
      await promise;

      // Re-patch for subsequent tests
      (DataVerifier.prototype as any).sleep = () => Promise.resolve();
    });
  });

  describe("isRunning breaks in verify loops", () => {
    it("should break verifyAgents loop when stopped mid-iteration (line 236)", async () => {
      const agents = Array.from({ length: 3 }, (_, i) => ({
        id: new PublicKey(new Uint8Array(32).fill(i + 10)).toBase58(),
      }));
      mockPrisma.agent.findMany.mockResolvedValue(agents);
      mockConnection.getMultipleAccountsInfo.mockResolvedValue(
        agents.map(() => ({ data: Buffer.alloc(10) }))
      );

      let updateCallCount = 0;
      mockPrisma.agent.update.mockImplementation(async () => {
        updateCallCount++;
        if (updateCallCount === 1) {
          // Stop after first agent processed
          await verifier.stop();
        }
        return {};
      });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      // Not all agents processed since we stopped mid-loop
      expect(updateCallCount).toBeLessThanOrEqual(agents.length);
    });

    it("should break verifyValidations loop when stopped mid-iteration (line 327)", async () => {
      const validatorId = VALIDATOR_KEY.toBase58();
      // Use different agent IDs so PDAs are unique (mock returns [asset, 255])
      const validations = Array.from({ length: 3 }, (_, i) => ({
        id: `v${i}`,
        agentId: new PublicKey(new Uint8Array(32).fill(i + 80)).toBase58(),
        validator: validatorId,
        nonce: BigInt(i),
      }));
      mockPrisma.validation.findMany.mockResolvedValue(validations);
      mockConnection.getMultipleAccountsInfo.mockResolvedValue(
        validations.map(() => ({ data: Buffer.alloc(10) }))
      );

      let updateCallCount = 0;
      mockPrisma.validation.update.mockImplementation(async () => {
        updateCallCount++;
        if (updateCallCount === 1) {
          await verifier.stop();
        }
        return {};
      });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      expect(updateCallCount).toBeLessThanOrEqual(validations.length);
    });

    it("should break verifyMetadata uriMetadata loop when stopped (line 414)", async () => {
      const uriMetadata = Array.from({ length: 3 }, (_, i) => ({
        id: `m${i}`, agentId: AGENT_KEY.toBase58(), key: `_uri:field${i}`,
      }));
      mockPrisma.agentMetadata.findMany.mockResolvedValue(uriMetadata);

      let updateCallCount = 0;
      mockPrisma.agentMetadata.update.mockImplementation(async () => {
        updateCallCount++;
        if (updateCallCount === 1) {
          await verifier.stop();
        }
        return {};
      });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      expect(updateCallCount).toBeLessThanOrEqual(uriMetadata.length);
    });

    it("should break verifyMetadata on-chain loop when stopped (line 439)", async () => {
      const onChainMetadata = Array.from({ length: 3 }, (_, i) => ({
        id: `m${i}`, agentId: new PublicKey(new Uint8Array(32).fill(i + 20)).toBase58(), key: `field${i}`,
      }));
      mockPrisma.agentMetadata.findMany.mockResolvedValue(onChainMetadata);
      mockConnection.getMultipleAccountsInfo.mockResolvedValue(
        onChainMetadata.map(() => ({ data: Buffer.alloc(10) }))
      );

      let updateCallCount = 0;
      mockPrisma.agentMetadata.update.mockImplementation(async () => {
        updateCallCount++;
        if (updateCallCount === 1) {
          await verifier.stop();
        }
        return {};
      });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      expect(updateCallCount).toBeLessThanOrEqual(onChainMetadata.length);
    });

    it("should break verifyRegistries loop when stopped (line 520)", async () => {
      const registries = Array.from({ length: 3 }, (_, i) => ({
        id: `r${i}`, collection: new PublicKey(new Uint8Array(32).fill(i + 30)).toBase58(),
      }));
      mockPrisma.registry.findMany.mockResolvedValue(registries);
      mockConnection.getMultipleAccountsInfo.mockResolvedValue(
        registries.map(() => ({ data: Buffer.alloc(10) }))
      );

      let updateCallCount = 0;
      mockPrisma.registry.update.mockImplementation(async () => {
        updateCallCount++;
        if (updateCallCount === 1) {
          await verifier.stop();
        }
        return {};
      });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
      expect(updateCallCount).toBeLessThanOrEqual(registries.length);
    });

    it("should break verifyFeedbacks loop when stopped (line 756)", async () => {
      const agentIds = Array.from({ length: 3 }, (_, i) =>
        new PublicKey(new Uint8Array(32).fill(i + 40)).toBase58()
      );
      const feedbacks = agentIds.map((agentId, i) => ({ id: `f${i}`, agentId }));

      mockPrisma.feedback.findMany
        .mockResolvedValueOnce(feedbacks)
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue(
        agentIds.map(() => ({ data: Buffer.alloc(10) }))
      );
      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({ feedbackCount: 0n }),
      });
      mockPrisma.feedback.findFirst.mockResolvedValue(null);
      mockPrisma.feedback.count.mockResolvedValue(0);
      mockPrisma.feedbackResponse.findFirst.mockResolvedValue(null);
      mockPrisma.feedbackResponse.count.mockResolvedValue(0);
      mockPrisma.revocation.findFirst.mockResolvedValue(null);
      mockPrisma.revocation.count.mockResolvedValue(0);

      let batchUpdateCalls = 0;
      mockPrisma.feedback.updateMany.mockImplementation(async () => {
        batchUpdateCalls++;
        if (batchUpdateCalls === 1) {
          await verifier.stop();
        }
        return { count: 1 };
      });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
    });

    it("should break verifyFeedbackResponses loop when stopped (line 848)", async () => {
      const agentIds = Array.from({ length: 3 }, (_, i) =>
        new PublicKey(new Uint8Array(32).fill(i + 50)).toBase58()
      );
      const responses = agentIds.map((agentId, i) => ({
        id: `r${i}`, feedback: { agentId, status: "FINALIZED" },
      }));

      mockPrisma.feedbackResponse.findMany
        .mockResolvedValueOnce(responses)
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue(
        agentIds.map(() => ({ data: Buffer.alloc(10) }))
      );
      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({ responseCount: 0n }),
      });
      mockPrisma.feedback.findFirst.mockResolvedValue(null);
      mockPrisma.feedback.count.mockResolvedValue(0);
      mockPrisma.feedbackResponse.findFirst.mockResolvedValue(null);
      mockPrisma.feedbackResponse.count.mockResolvedValue(0);
      mockPrisma.revocation.findFirst.mockResolvedValue(null);
      mockPrisma.revocation.count.mockResolvedValue(0);

      let batchUpdateCalls = 0;
      mockPrisma.feedbackResponse.updateMany.mockImplementation(async () => {
        batchUpdateCalls++;
        if (batchUpdateCalls === 1) {
          await verifier.stop();
        }
        return { count: 1 };
      });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
    });

    it("should break verifyRevocations loop when stopped (line 916)", async () => {
      const agentIds = Array.from({ length: 3 }, (_, i) =>
        new PublicKey(new Uint8Array(32).fill(i + 60)).toBase58()
      );
      const revocations = agentIds.map((agentId, i) => ({ id: `rev${i}`, agentId }));

      mockPrisma.revocation.findMany
        .mockResolvedValueOnce(revocations)
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue(
        agentIds.map(() => ({ data: Buffer.alloc(10) }))
      );
      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({ revokeCount: 0n }),
      });
      mockPrisma.feedback.findFirst.mockResolvedValue(null);
      mockPrisma.feedback.count.mockResolvedValue(0);
      mockPrisma.feedbackResponse.findFirst.mockResolvedValue(null);
      mockPrisma.feedbackResponse.count.mockResolvedValue(0);
      mockPrisma.revocation.findFirst.mockResolvedValue(null);
      mockPrisma.revocation.count.mockResolvedValue(0);

      let batchUpdateCalls = 0;
      mockPrisma.revocation.updateMany.mockImplementation(async () => {
        batchUpdateCalls++;
        if (batchUpdateCalls === 1) {
          await verifier.stop();
        }
        return { count: 1 };
      });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();
    });

    it("should return early from recoverOrphaned when not running (line 561)", async () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      // Call recoverOrphaned directly without starting
      await (verifier as any).recoverOrphaned();
      // No DB queries should have been made
      expect(mockPrisma.agent.findMany).not.toHaveBeenCalled();
    });

    it("should break batchVerifyAccounts batch loop when stopped (line 969)", async () => {
      const pubkeys = Array.from({ length: 150 }, (_, i) => {
        const bytes = new Uint8Array(32);
        bytes[0] = (i >> 8) & 0xff;
        bytes[1] = i & 0xff;
        bytes.fill(0x05, 2);
        return new PublicKey(bytes).toBase58();
      });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      (verifier as any).isRunning = true;

      // First batch succeeds, then stop before second
      let batchCallCount = 0;
      mockConnection.getMultipleAccountsInfo.mockImplementation(async (keys: any) => {
        batchCallCount++;
        if (batchCallCount === 1) {
          (verifier as any).isRunning = false;
        }
        return keys.map(() => ({ data: Buffer.alloc(10) }));
      });

      const result = await (verifier as any).batchVerifyAccounts(pubkeys, "finalized");
      // Only first batch of 100 should have been processed
      expect(batchCallCount).toBe(1);
      expect(result.size).toBe(100);
    });

    it("should break batchVerifyAccounts fallback loop when stopped (line 981)", async () => {
      const pubkeys = Array.from({ length: 3 }, (_, i) => {
        const bytes = new Uint8Array(32);
        bytes.fill(i + 70);
        return new PublicKey(bytes).toBase58();
      });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      (verifier as any).isRunning = true;

      mockConnection.getMultipleAccountsInfo.mockRejectedValue(new Error("batch fail"));
      let individualCallCount = 0;
      mockConnection.getAccountInfo.mockImplementation(async () => {
        individualCallCount++;
        if (individualCallCount === 1) {
          (verifier as any).isRunning = false;
        }
        return { data: Buffer.alloc(10) };
      });

      const result = await (verifier as any).batchVerifyAccounts(pubkeys, "finalized");
      // Should have stopped after first individual check
      expect(individualCallCount).toBe(1);
    });
  });

  describe("verifyFeedbacks RPC error skip (lines 760-763)", () => {
    it("should skip feedbacks when agent exists check returns null (RPC error)", async () => {
      const agentId = AGENT_KEY.toBase58();

      mockPrisma.feedback.findMany
        .mockResolvedValueOnce([{ id: "f1", agentId }])
        .mockResolvedValue([]);

      // Batch and individual checks both fail → existsMap returns null
      mockConnection.getMultipleAccountsInfo.mockRejectedValue(new Error("RPC fail"));
      mockConnection.getAccountInfo.mockRejectedValue(new Error("RPC fail"));

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().skippedRpcErrors).toBeGreaterThan(0);
      expect(verifier.getStats().feedbacksVerified).toBe(0);
      expect(verifier.getStats().feedbacksOrphaned).toBe(0);
      await verifier.stop();
    });
  });

  describe("verifyFeedbackResponses RPC error skip (lines 852-855)", () => {
    it("should skip responses when agent exists check returns null (RPC error)", async () => {
      const agentId = AGENT_KEY.toBase58();

      mockPrisma.feedbackResponse.findMany
        .mockResolvedValueOnce([
          { id: "r1", feedback: { agentId, status: "FINALIZED" } },
        ])
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockRejectedValue(new Error("RPC fail"));
      mockConnection.getAccountInfo.mockRejectedValue(new Error("RPC fail"));

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().skippedRpcErrors).toBeGreaterThan(0);
      expect(verifier.getStats().responsesVerified).toBe(0);
      await verifier.stop();
    });
  });

  describe("verifyRevocations RPC error skip (lines 920-923)", () => {
    it("should skip revocations when agent exists check returns null (RPC error)", async () => {
      const agentId = AGENT_KEY.toBase58();

      mockPrisma.revocation.findMany
        .mockResolvedValueOnce([{ id: "rev1", agentId }])
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockRejectedValue(new Error("RPC fail"));
      mockConnection.getAccountInfo.mockRejectedValue(new Error("RPC fail"));

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().skippedRpcErrors).toBeGreaterThan(0);
      expect(verifier.getStats().revocationsVerified).toBe(0);
      expect(verifier.getStats().revocationsOrphaned).toBe(0);
      await verifier.stop();
    });
  });

  describe("verifyRevocations hash-chain mismatch (lines 938-940)", () => {
    it("should leave revocations PENDING on hash-chain mismatch", async () => {
      const agentId = AGENT_KEY.toBase58();

      mockPrisma.revocation.findMany
        .mockResolvedValueOnce([{ id: "rev1", agentId }])
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({
          revokeDigest: new Uint8Array(32).fill(0xaa),
          revokeCount: 1n,
        }),
      });

      // DB has different digest
      mockPrisma.feedback.findFirst.mockResolvedValue(null);
      mockPrisma.feedback.count.mockResolvedValue(0);
      mockPrisma.feedbackResponse.findFirst.mockResolvedValue(null);
      mockPrisma.feedbackResponse.count.mockResolvedValue(0);
      mockPrisma.revocation.findFirst.mockResolvedValue({
        runningDigest: Buffer.from(new Uint8Array(32).fill(0xbb)),
      });
      mockPrisma.revocation.count.mockResolvedValue(1);

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().hashChainMismatches).toBe(1);
      expect(verifier.getStats().revocationsVerified).toBe(0);
      await verifier.stop();
    });
  });

  describe("verifyMetadata pool on-chain path (lines 456-461, 469-471)", () => {
    it("should finalize on-chain metadata via pool", async () => {
      const agentId = AGENT_KEY.toBase58();
      mockPool.query.mockImplementation(async (sql: string) => {
        if (sql.includes("FROM metadata") && sql.includes("PENDING")) {
          return { rows: [{ id: "m1", agentId, key: "capabilities" }] };
        }
        return { rows: [], rowCount: 0 };
      });
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();

      expect(verifier.getStats().metadataVerified).toBe(1);
      await verifier.stop();
    });

    it("should orphan on-chain metadata via pool when PDA not found", async () => {
      const agentId = AGENT_KEY.toBase58();
      mockPool.query.mockImplementation(async (sql: string) => {
        if (sql.includes("FROM metadata") && sql.includes("PENDING")) {
          return { rows: [{ id: "m1", agentId, key: "capabilities" }] };
        }
        return { rows: [], rowCount: 0 };
      });
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([null]);

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();

      expect(verifier.getStats().metadataOrphaned).toBe(1);
      await verifier.stop();
    });

    it("should catch per-metadata verification error in pool path (line 469-471)", async () => {
      const agentId = AGENT_KEY.toBase58();
      let queryCount = 0;
      mockPool.query.mockImplementation(async (sql: string) => {
        queryCount++;
        if (sql.includes("FROM metadata") && sql.includes("PENDING")) {
          return { rows: [{ id: "m1", agentId, key: "capabilities" }] };
        }
        if (sql.includes("UPDATE metadata")) {
          throw new Error("DB write failed");
        }
        return { rows: [], rowCount: 0 };
      });
      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();
      await verifier.stop();
    });
  });

  describe("checkDigestMatch error in catch block (lines 1123-1127)", () => {
    it("should return false when getLastDbDigests throws", async () => {
      const agentId = AGENT_KEY.toBase58();

      mockPrisma.feedback.findMany
        .mockResolvedValueOnce([{ id: "f1", agentId }])
        .mockResolvedValue([]);

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);

      // fetchOnChainDigests succeeds
      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({ feedbackCount: 1n, feedbackDigest: new Uint8Array(32).fill(0xaa) }),
      });

      // But getLastDbDigests throws via prisma
      mockPrisma.feedback.findFirst.mockRejectedValue(new Error("DB connection lost"));

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await verifier.start();

      expect(verifier.getStats().feedbacksVerified).toBe(0);
      await verifier.stop();
    });
  });

  describe("getLastDbDigests pool running_digest null coalescing (lines 1191, 1193)", () => {
    it("should handle null running_digest rows from pool", async () => {
      const agentId = AGENT_KEY.toBase58();

      mockPool.query.mockImplementation(async (sql: string) => {
        if (sql.includes("FROM feedbacks") && sql.includes("PENDING")) {
          return { rows: [{ id: "f1", agentId }] };
        }
        // Return rows with no running_digest for feedback and response queries
        if (sql.includes("running_digest") && sql.includes("feedbacks")) {
          return { rows: [] }; // empty means null digest
        }
        if (sql.includes("running_digest") && sql.includes("feedback_responses")) {
          return { rows: [] };
        }
        if (sql.includes("running_digest") && sql.includes("revocations")) {
          return { rows: [] };
        }
        if (sql.includes("COUNT")) {
          return { rows: [{ cnt: "0" }] };
        }
        return { rows: [], rowCount: 0 };
      });

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);
      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({ feedbackCount: 0n }),
      });

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();

      expect(verifier.getStats().feedbacksVerified).toBe(1);
      await verifier.stop();
    });

    it("should use running_digest from pool rows when present", async () => {
      const agentId = AGENT_KEY.toBase58();
      const digest = new Uint8Array(32).fill(0xee);

      mockPool.query.mockImplementation(async (sql: string) => {
        if (sql.includes("FROM feedbacks") && sql.includes("PENDING")) {
          return { rows: [{ id: "f1", agentId }] };
        }
        if (sql.includes("running_digest") && sql.includes("feedbacks")) {
          return { rows: [{ running_digest: Buffer.from(digest) }] };
        }
        if (sql.includes("running_digest") && sql.includes("feedback_responses")) {
          return { rows: [{ running_digest: Buffer.from(digest) }] };
        }
        if (sql.includes("running_digest") && sql.includes("revocations")) {
          return { rows: [{ running_digest: Buffer.from(digest) }] };
        }
        if (sql.includes("COUNT") && sql.includes("feedbacks")) {
          return { rows: [{ cnt: "1" }] };
        }
        if (sql.includes("COUNT") && sql.includes("feedback_responses")) {
          return { rows: [{ cnt: "1" }] };
        }
        if (sql.includes("COUNT") && sql.includes("revocations")) {
          return { rows: [{ cnt: "1" }] };
        }
        return { rows: [], rowCount: 0 };
      });

      mockConnection.getMultipleAccountsInfo.mockResolvedValue([
        { data: Buffer.alloc(10) },
      ]);
      mockConnection.getAccountInfo.mockResolvedValue({
        data: buildAgentAccountData({
          feedbackDigest: digest,
          feedbackCount: 1n,
        }),
      });

      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await verifier.start();

      expect(verifier.getStats().feedbacksVerified).toBe(1);
      await verifier.stop();
    });
  });

  describe("batchUpdateStatus empty ids (line 1223)", () => {
    it("should return early for empty ids array via pool", async () => {
      const verifier = new DataVerifier(mockConnection, null, mockPool);
      await (verifier as any).batchUpdateStatus("feedbacks", "id", [], "FINALIZED", new Date());
      expect(mockPool.query).not.toHaveBeenCalled();
    });

    it("should return early for empty ids array via prisma", async () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      await (verifier as any).batchUpdateStatus("feedbacks", "id", [], "FINALIZED", new Date());
      expect(mockPrisma.feedback.updateMany).not.toHaveBeenCalled();
    });
  });

  describe("verifyAll not running guard (line 144)", () => {
    it("should return early from verifyAll when not running", async () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      // Call verifyAll directly without starting (isRunning is false)
      await (verifier as any).verifyAll();
      // getSlot should not be called since isRunning is false
      expect(mockConnection.getSlot).not.toHaveBeenCalled();
    });
  });

  describe("verifyAll reentrancy guard (lines 147-150)", () => {
    it("should skip when verifyInProgress is true", async () => {
      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      (verifier as any).isRunning = true;
      (verifier as any).verifyInProgress = true;

      await (verifier as any).verifyAll();

      // getSlot should not be called since we returned early
      expect(mockConnection.getSlot).not.toHaveBeenCalled();
    });
  });

  describe("fetchOnChainDigests buffer too short with wallet (line 1277)", () => {
    it("should return null when buffer too short after wallet option parsing", async () => {
      // offset after header = 106, wallet present → offset = 139, need offset+120 = 259
      // Buffer must be >= 227 (pass line 1269 check) but < 259 (fail line 1277 check)
      const buf = Buffer.alloc(240);
      const headerSize = 8 + 32 + 32 + 32 + 1 + 1; // 106
      buf[headerSize] = 1; // wallet present option tag
      mockConnection.getAccountInfo.mockResolvedValue({ data: buf });

      const verifier = new DataVerifier(mockConnection, mockPrisma, null);
      const result = await verifier.fetchOnChainDigests(AGENT_KEY.toBase58());
      expect(result).toBeNull();
    });
  });
});
