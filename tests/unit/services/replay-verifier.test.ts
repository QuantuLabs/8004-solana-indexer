import { describe, it, expect, beforeEach, vi } from "vitest";
import { PublicKey } from "@solana/web3.js";
import { createMockPrismaClient } from "../../mocks/prisma.js";
import { TEST_ASSET, TEST_CLIENT, TEST_OWNER } from "../../mocks/solana.js";
import { ReplayVerifier } from "../../../src/services/replay-verifier.js";

const ZERO_DIGEST_HEX = Buffer.alloc(32).toString("hex");
const AGENT_ID = TEST_ASSET.toBase58();
const CLIENT_ID = TEST_CLIENT.toBase58();
const OWNER_ID = TEST_OWNER.toBase58();

function makeSealHash(index: number): Buffer {
  const buf = Buffer.alloc(32);
  buf.writeUInt32LE(index);
  return buf;
}

function makeFeedbackRow(index: number, opts?: { runningDigest?: Buffer | null }) {
  const sealHash = makeSealHash(index);
  return {
    agentId: AGENT_ID,
    client: CLIENT_ID,
    feedbackIndex: BigInt(index),
    feedbackHash: new Uint8Array(sealHash),
    createdSlot: 100n + BigInt(index),
    runningDigest: opts?.runningDigest ? new Uint8Array(opts.runningDigest) : null,
  };
}

function makeRevocationRow(index: number, revokeCount: number, opts?: { runningDigest?: Buffer | null }) {
  return {
    agentId: AGENT_ID,
    client: CLIENT_ID,
    feedbackIndex: BigInt(index),
    feedbackHash: new Uint8Array(makeSealHash(index)),
    revokeCount: BigInt(revokeCount),
    slot: 200n + BigInt(revokeCount),
    runningDigest: opts?.runningDigest ? new Uint8Array(opts.runningDigest) : null,
  };
}

function makeResponseRow(fbIndex: number, responseCount: number, opts?: { runningDigest?: Buffer | null }) {
  return {
    responder: OWNER_ID,
    responseHash: new Uint8Array(makeSealHash(responseCount + 1000)),
    responseCount: BigInt(responseCount),
    slot: 300n + BigInt(responseCount),
    runningDigest: opts?.runningDigest ? new Uint8Array(opts.runningDigest) : null,
    feedback: {
      agentId: AGENT_ID,
      client: CLIENT_ID,
      feedbackIndex: BigInt(fbIndex),
      feedbackHash: new Uint8Array(makeSealHash(fbIndex)),
    },
  };
}

describe("ReplayVerifier", () => {
  let prisma: ReturnType<typeof createMockPrismaClient>;
  let verifier: ReplayVerifier;

  beforeEach(() => {
    prisma = createMockPrismaClient();
    verifier = new ReplayVerifier(prisma);
  });

  describe("fullReplay - empty agent", () => {
    it("should return valid with zero digest for agent with no events", async () => {
      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.valid).toBe(true);
      expect(result.feedback.count).toBe(0);
      expect(result.response.count).toBe(0);
      expect(result.revoke.count).toBe(0);
      expect(result.feedback.finalDigest).toBe(ZERO_DIGEST_HEX);
      expect(result.response.finalDigest).toBe(ZERO_DIGEST_HEX);
      expect(result.revoke.finalDigest).toBe(ZERO_DIGEST_HEX);
      expect(result.feedback.checkpointsStored).toBe(0);
      expect(result.duration).toBeGreaterThanOrEqual(0);
    });
  });

  describe("fullReplay - feedback chain", () => {
    it("should replay single feedback and produce non-zero digest", async () => {
      const row = makeFeedbackRow(0);
      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.valid).toBe(true);
      expect(result.feedback.count).toBe(1);
      expect(result.feedback.finalDigest).not.toBe(ZERO_DIGEST_HEX);
      expect(result.feedback.finalDigest).toHaveLength(64); // 32 bytes hex
    });

    it("should produce deterministic digest for same input", async () => {
      const row = makeFeedbackRow(0);

      // First run
      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const result1 = await verifier.fullReplay(AGENT_ID);

      // Second run with fresh verifier
      const prisma2 = createMockPrismaClient();
      const verifier2 = new ReplayVerifier(prisma2);
      (prisma2.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);
      (prisma2.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma2.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const result2 = await verifier2.fullReplay(AGENT_ID);

      expect(result1.feedback.finalDigest).toBe(result2.feedback.finalDigest);
    });

    it("should produce different digests for different inputs", async () => {
      // First agent
      const row0 = makeFeedbackRow(0);
      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row0])
        .mockResolvedValueOnce([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const result1 = await verifier.fullReplay(AGENT_ID);

      // Different feedback (index 1 has different sealHash)
      const prisma2 = createMockPrismaClient();
      const verifier2 = new ReplayVerifier(prisma2);
      const row1 = makeFeedbackRow(1);
      (prisma2.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row1])
        .mockResolvedValueOnce([]);
      (prisma2.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma2.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const result2 = await verifier2.fullReplay(AGENT_ID);

      expect(result1.feedback.finalDigest).not.toBe(result2.feedback.finalDigest);
    });

    it("should replay multiple feedbacks and count correctly", async () => {
      const rows = Array.from({ length: 5 }, (_, i) => makeFeedbackRow(i));
      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(rows)
        .mockResolvedValueOnce([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.valid).toBe(true);
      expect(result.feedback.count).toBe(5);
      expect(result.feedback.finalDigest).not.toBe(ZERO_DIGEST_HEX);
    });

    it("should validate stored runningDigest when present", async () => {
      // Run once to get the correct digest
      const row = makeFeedbackRow(0);
      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const refResult = await verifier.fullReplay(AGENT_ID);
      const correctDigest = Buffer.from(refResult.feedback.finalDigest, "hex");

      // Run again with correct stored digest
      const prisma2 = createMockPrismaClient();
      const verifier2 = new ReplayVerifier(prisma2);
      const rowWithDigest = makeFeedbackRow(0, { runningDigest: correctDigest });
      (prisma2.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([rowWithDigest])
        .mockResolvedValueOnce([]);
      (prisma2.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma2.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const validResult = await verifier2.fullReplay(AGENT_ID);

      expect(validResult.valid).toBe(true);
      expect(validResult.feedback.valid).toBe(true);
    });

    it("should detect digest mismatch when stored digest is wrong", async () => {
      const wrongDigest = Buffer.alloc(32, 0xff);
      const row = makeFeedbackRow(0, { runningDigest: wrongDigest });

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.valid).toBe(false);
      expect(result.feedback.valid).toBe(false);
      expect(result.feedback.mismatchAt).toBe(1);
    });

    it("should skip validation when runningDigest is null", async () => {
      const row = makeFeedbackRow(0); // null runningDigest by default

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.valid).toBe(true);
      expect(result.feedback.valid).toBe(true);
      expect(result.feedback.count).toBe(1);
    });

    it("should report first mismatch only, not subsequent ones", async () => {
      const wrongDigest = Buffer.alloc(32, 0xff);
      const rows = Array.from({ length: 3 }, (_, i) =>
        makeFeedbackRow(i, { runningDigest: wrongDigest }),
      );

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(rows)
        .mockResolvedValueOnce([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.feedback.valid).toBe(false);
      expect(result.feedback.mismatchAt).toBe(1); // First mismatch
      expect(result.feedback.count).toBe(3); // Still processes all
    });
  });

  describe("fullReplay - revoke chain", () => {
    it("should replay revocations and produce non-zero digest", async () => {
      const row = makeRevocationRow(0, 0);

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.revoke.valid).toBe(true);
      expect(result.revoke.count).toBe(1);
      expect(result.revoke.finalDigest).not.toBe(ZERO_DIGEST_HEX);
    });

    it("should detect mismatch in revoke chain", async () => {
      const wrongDigest = Buffer.alloc(32, 0xaa);
      const row = makeRevocationRow(0, 0, { runningDigest: wrongDigest });

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.valid).toBe(false);
      expect(result.revoke.valid).toBe(false);
      expect(result.revoke.mismatchAt).toBe(1);
    });
  });

  describe("fullReplay - response chain", () => {
    it("should replay responses and produce non-zero digest", async () => {
      const row = makeResponseRow(0, 0);

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.response.valid).toBe(true);
      expect(result.response.count).toBe(1);
      expect(result.response.finalDigest).not.toBe(ZERO_DIGEST_HEX);
    });

    it("should detect mismatch in response chain", async () => {
      const wrongDigest = Buffer.alloc(32, 0xbb);
      const row = makeResponseRow(0, 0, { runningDigest: wrongDigest });

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.valid).toBe(false);
      expect(result.response.valid).toBe(false);
      expect(result.response.mismatchAt).toBe(1);
    });
  });

  describe("checkpoints", () => {
    it("should store checkpoints at CHECKPOINT_INTERVAL", async () => {
      verifier.CHECKPOINT_INTERVAL = 2;

      const rows = Array.from({ length: 3 }, (_, i) => makeFeedbackRow(i));

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(rows)
        .mockResolvedValueOnce([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.feedback.checkpointsStored).toBe(1); // at count=2
      expect(prisma.hashChainCheckpoint.upsert).toHaveBeenCalledTimes(1);
      expect(prisma.hashChainCheckpoint.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { agentId_chainType_eventCount: { agentId: AGENT_ID, chainType: "feedback", eventCount: 2n } },
          create: expect.objectContaining({ agentId: AGENT_ID, chainType: "feedback", eventCount: 2n }),
          update: expect.any(Object),
        }),
      );
    });

    it("should store multiple checkpoints for large datasets", async () => {
      verifier.CHECKPOINT_INTERVAL = 3;

      const rows = Array.from({ length: 9 }, (_, i) => makeFeedbackRow(i));

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(rows)
        .mockResolvedValueOnce([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.feedback.checkpointsStored).toBe(3); // at count=3, 6, 9
      expect(prisma.hashChainCheckpoint.upsert).toHaveBeenCalledTimes(3);
    });

    it("should not store checkpoint when count doesn't hit interval", async () => {
      verifier.CHECKPOINT_INTERVAL = 100;

      const rows = Array.from({ length: 5 }, (_, i) => makeFeedbackRow(i));

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(rows)
        .mockResolvedValueOnce([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.feedback.checkpointsStored).toBe(0);
      expect(prisma.hashChainCheckpoint.upsert).not.toHaveBeenCalled();
    });
  });

  describe("incrementalVerify", () => {
    it("should start from checkpoint and produce same final digest", async () => {
      // First: full replay of 4 events
      const rows = Array.from({ length: 4 }, (_, i) => makeFeedbackRow(i));
      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(rows)
        .mockResolvedValueOnce([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const fullResult = await verifier.fullReplay(AGENT_ID);

      // Get the digest at count=2 by replaying just first 2
      const prisma2 = createMockPrismaClient();
      const verifier2 = new ReplayVerifier(prisma2);
      const firstTwo = rows.slice(0, 2);
      (prisma2.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(firstTwo)
        .mockResolvedValueOnce([]);
      (prisma2.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma2.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const midResult = await verifier2.fullReplay(AGENT_ID);

      // Now: incremental from checkpoint at count=2
      const prisma3 = createMockPrismaClient();
      const verifier3 = new ReplayVerifier(prisma3);
      (prisma3.hashChainCheckpoint.findFirst as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce({ eventCount: 2, digest: midResult.feedback.finalDigest }) // feedback
        .mockResolvedValueOnce(null) // response
        .mockResolvedValueOnce(null); // revoke
      const lastTwo = rows.slice(2);
      (prisma3.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(lastTwo)
        .mockResolvedValueOnce([]);
      (prisma3.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma3.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const incResult = await verifier3.incrementalVerify(AGENT_ID);

      expect(incResult.valid).toBe(true);
      expect(incResult.feedback.count).toBe(4);
      expect(incResult.feedback.finalDigest).toBe(fullResult.feedback.finalDigest);
    });

    it("should fall back to full replay when no checkpoints exist", async () => {
      (prisma.hashChainCheckpoint.findFirst as ReturnType<typeof vi.fn>).mockResolvedValue(null);
      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.incrementalVerify(AGENT_ID);

      expect(result.valid).toBe(true);
      expect(result.feedback.count).toBe(0);
      expect(result.feedback.finalDigest).toBe(ZERO_DIGEST_HEX);
    });
  });

  describe("getCheckpoint", () => {
    it("should return null when no checkpoints exist", async () => {
      (prisma.hashChainCheckpoint.findFirst as ReturnType<typeof vi.fn>).mockResolvedValue(null);

      const result = await verifier.getCheckpoint(AGENT_ID, "feedback");
      expect(result).toBeNull();
    });

    it("should return latest checkpoint for chain type", async () => {
      const cp = { eventCount: 5000, digest: "a".repeat(64) };
      (prisma.hashChainCheckpoint.findFirst as ReturnType<typeof vi.fn>).mockResolvedValue(cp);

      const result = await verifier.getCheckpoint(AGENT_ID, "feedback");
      expect(result).toEqual(cp);
    });

    it("should filter by targetCount when provided", async () => {
      const cp = { eventCount: 3000, digest: "b".repeat(64) };
      (prisma.hashChainCheckpoint.findFirst as ReturnType<typeof vi.fn>).mockResolvedValue(cp);

      const result = await verifier.getCheckpoint(AGENT_ID, "feedback", 4000);
      expect(result).toEqual(cp);

      expect(prisma.hashChainCheckpoint.findFirst).toHaveBeenCalledWith({
        where: { agentId: AGENT_ID, chainType: "feedback", eventCount: { lte: 4000n } },
        orderBy: { eventCount: "desc" },
      });
    });

    it("should query without targetCount filter when not provided", async () => {
      (prisma.hashChainCheckpoint.findFirst as ReturnType<typeof vi.fn>).mockResolvedValue(null);

      await verifier.getCheckpoint(AGENT_ID, "response");

      expect(prisma.hashChainCheckpoint.findFirst).toHaveBeenCalledWith({
        where: { agentId: AGENT_ID, chainType: "response" },
        orderBy: { eventCount: "desc" },
      });
    });
  });

  describe("batching", () => {
    it("should handle multiple batches of events", async () => {
      // BATCH_SIZE is 1000 in the verifier
      const batch1 = Array.from({ length: 1000 }, (_, i) => makeFeedbackRow(i));
      const batch2 = Array.from({ length: 5 }, (_, i) => makeFeedbackRow(1000 + i));

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(batch1)
        .mockResolvedValueOnce(batch2)
        .mockResolvedValueOnce([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.feedback.count).toBe(1005);
      expect(prisma.feedback.findMany).toHaveBeenCalledTimes(2);
    });

    it("should stop batching when batch is smaller than BATCH_SIZE", async () => {
      const smallBatch = Array.from({ length: 500 }, (_, i) => makeFeedbackRow(i));

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(smallBatch);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.feedback.count).toBe(500);
      // Only called once since batch < BATCH_SIZE
      expect(prisma.feedback.findMany).toHaveBeenCalledTimes(1);
    });
  });

  describe("cross-chain independence", () => {
    it("should replay all three chains independently", async () => {
      const fbRow = makeFeedbackRow(0);
      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([fbRow])
        .mockResolvedValueOnce([]);

      const rvRow = makeRevocationRow(0, 0);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([rvRow])
        .mockResolvedValueOnce([]);

      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.feedback.count).toBe(1);
      expect(result.revoke.count).toBe(1);
      expect(result.response.count).toBe(0);
      expect(result.feedback.finalDigest).not.toBe(ZERO_DIGEST_HEX);
      expect(result.revoke.finalDigest).not.toBe(ZERO_DIGEST_HEX);
      expect(result.response.finalDigest).toBe(ZERO_DIGEST_HEX);
      // All three digests are different
      expect(result.feedback.finalDigest).not.toBe(result.revoke.finalDigest);
    });
  });

  describe("result structure", () => {
    it("should include agentId and duration in result", async () => {
      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.agentId).toBe(AGENT_ID);
      expect(typeof result.duration).toBe("number");
      expect(result).toHaveProperty("feedback");
      expect(result).toHaveProperty("response");
      expect(result).toHaveProperty("revoke");
      expect(result.feedback.chainType).toBe("feedback");
      expect(result.response.chainType).toBe("response");
      expect(result.revoke.chainType).toBe("revoke");
    });

    it("should be invalid if any single chain is invalid", async () => {
      const wrongDigest = Buffer.alloc(32, 0xff);
      const rvRow = makeRevocationRow(0, 0, { runningDigest: wrongDigest });

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([rvRow])
        .mockResolvedValueOnce([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.valid).toBe(false);
      expect(result.feedback.valid).toBe(true);
      expect(result.response.valid).toBe(true);
      expect(result.revoke.valid).toBe(false);
    });
  });

  describe("response chain checkpoints", () => {
    it("should store checkpoints for response chain at interval", async () => {
      verifier.CHECKPOINT_INTERVAL = 2;

      const rows = Array.from({ length: 3 }, (_, i) => makeResponseRow(i, i));

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(rows)
        .mockResolvedValueOnce([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.response.count).toBe(3);
      expect(result.response.checkpointsStored).toBe(1); // at count=2
      expect(prisma.hashChainCheckpoint.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { agentId_chainType_eventCount: { agentId: AGENT_ID, chainType: "response", eventCount: 2n } },
        }),
      );
    });
  });

  describe("revoke chain checkpoints", () => {
    it("should store checkpoints for revoke chain at interval", async () => {
      verifier.CHECKPOINT_INTERVAL = 2;

      const rows = Array.from({ length: 3 }, (_, i) => makeRevocationRow(i, i));

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(rows)
        .mockResolvedValueOnce([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.revoke.count).toBe(3);
      expect(result.revoke.checkpointsStored).toBe(1); // at count=2
      expect(prisma.hashChainCheckpoint.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { agentId_chainType_eventCount: { agentId: AGENT_ID, chainType: "revoke", eventCount: 2n } },
        }),
      );
    });
  });

  describe("response chain batching", () => {
    it("should handle multiple batches of responses", async () => {
      const batch1 = Array.from({ length: 1000 }, (_, i) => makeResponseRow(i, i));
      const batch2 = Array.from({ length: 5 }, (_, i) => makeResponseRow(1000 + i, 1000 + i));

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(batch1)
        .mockResolvedValueOnce(batch2)
        .mockResolvedValueOnce([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.response.count).toBe(1005);
      expect(prisma.feedbackResponse.findMany).toHaveBeenCalledTimes(2);
    });

    it("should stop response batching when batch is smaller than BATCH_SIZE", async () => {
      const smallBatch = Array.from({ length: 500 }, (_, i) => makeResponseRow(i, i));

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(smallBatch);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.response.count).toBe(500);
      expect(prisma.feedbackResponse.findMany).toHaveBeenCalledTimes(1);
    });
  });

  describe("revoke chain batching", () => {
    it("should handle multiple batches of revocations", async () => {
      const batch1 = Array.from({ length: 1000 }, (_, i) => makeRevocationRow(i, i));
      const batch2 = Array.from({ length: 5 }, (_, i) => makeRevocationRow(1000 + i, 1000 + i));

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(batch1)
        .mockResolvedValueOnce(batch2)
        .mockResolvedValueOnce([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.revoke.count).toBe(1005);
      expect(prisma.revocation.findMany).toHaveBeenCalledTimes(2);
    });

    it("should stop revoke batching when batch is smaller than BATCH_SIZE", async () => {
      const smallBatch = Array.from({ length: 500 }, (_, i) => makeRevocationRow(i, i));

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(smallBatch);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.revoke.count).toBe(500);
      expect(prisma.revocation.findMany).toHaveBeenCalledTimes(1);
    });
  });

  describe("hashBytesToBuffer null handling", () => {
    it("should handle null feedbackHash in feedback (returns 32-zero buffer)", async () => {
      const row = {
        ...makeFeedbackRow(0),
        feedbackHash: null,
      };

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.valid).toBe(true);
      expect(result.feedback.count).toBe(1);
    });

    it("should handle null responseHash in response (returns 32-zero buffer)", async () => {
      const row = {
        ...makeResponseRow(0, 0),
        responseHash: null,
      };

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.valid).toBe(true);
      expect(result.response.count).toBe(1);
    });

    it("should handle null feedbackHash on response's feedback (returns 32-zero buffer)", async () => {
      const row = {
        ...makeResponseRow(0, 0),
        feedback: {
          ...makeResponseRow(0, 0).feedback,
          feedbackHash: null,
        },
      };

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.valid).toBe(true);
      expect(result.response.count).toBe(1);
    });

    it("should handle null feedbackHash in revocation (returns 32-zero buffer)", async () => {
      const row = {
        ...makeRevocationRow(0, 0),
        feedbackHash: null,
      };

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.valid).toBe(true);
      expect(result.revoke.count).toBe(1);
    });
  });

  describe("null slot handling", () => {
    it("should handle null createdSlot in feedback (defaults to 0n)", async () => {
      const row = {
        ...makeFeedbackRow(0),
        createdSlot: null,
      };

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.valid).toBe(true);
      expect(result.feedback.count).toBe(1);
    });

    it("should handle null slot in response (defaults to 0n)", async () => {
      const row = {
        ...makeResponseRow(0, 0),
        slot: null,
      };

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.valid).toBe(true);
      expect(result.response.count).toBe(1);
    });
  });

  describe("incrementalVerify with all checkpoints", () => {
    it("should use checkpoints for all three chains", async () => {
      // Compute reference digests first
      const fbRow = makeFeedbackRow(0);
      (prisma.feedback.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([fbRow])
        .mockResolvedValueOnce([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const refResult = await verifier.fullReplay(AGENT_ID);

      // Now incremental with checkpoints for all 3 chains
      const prisma2 = createMockPrismaClient();
      const verifier2 = new ReplayVerifier(prisma2);
      (prisma2.hashChainCheckpoint.findFirst as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce({ eventCount: 1, digest: refResult.feedback.finalDigest }) // feedback checkpoint
        .mockResolvedValueOnce({ eventCount: 0, digest: ZERO_DIGEST_HEX }) // response checkpoint
        .mockResolvedValueOnce({ eventCount: 0, digest: ZERO_DIGEST_HEX }); // revoke checkpoint
      (prisma2.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma2.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma2.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const incResult = await verifier2.incrementalVerify(AGENT_ID);

      expect(incResult.valid).toBe(true);
      expect(incResult.feedback.finalDigest).toBe(refResult.feedback.finalDigest);
    });
  });

  describe("response chain with null responseCount in pagination", () => {
    it("should handle null responseCount on last response in batch", async () => {
      const row = {
        ...makeResponseRow(0, 0),
        responseCount: null,
      };

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.response.count).toBe(1);
      expect(result.response.valid).toBe(true);
    });
  });

  describe("response chain with valid running digest", () => {
    it("should validate correct running digest in response chain", async () => {
      // First pass: get correct digest
      const row = makeResponseRow(0, 0);
      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const refResult = await verifier.fullReplay(AGENT_ID);
      const correctDigest = Buffer.from(refResult.response.finalDigest, "hex");

      // Second pass: with correct stored digest
      const prisma2 = createMockPrismaClient();
      const verifier2 = new ReplayVerifier(prisma2);
      const rowWithDigest = makeResponseRow(0, 0, { runningDigest: correctDigest });
      (prisma2.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma2.feedbackResponse.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([rowWithDigest])
        .mockResolvedValueOnce([]);
      (prisma2.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const result = await verifier2.fullReplay(AGENT_ID);

      expect(result.response.valid).toBe(true);
    });
  });

  describe("revoke chain with valid running digest", () => {
    it("should validate correct running digest in revoke chain", async () => {
      // First pass: get correct digest
      const row = makeRevocationRow(0, 0);
      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);
      const refResult = await verifier.fullReplay(AGENT_ID);
      const correctDigest = Buffer.from(refResult.revoke.finalDigest, "hex");

      // Second pass: with correct stored digest
      const prisma2 = createMockPrismaClient();
      const verifier2 = new ReplayVerifier(prisma2);
      const rowWithDigest = makeRevocationRow(0, 0, { runningDigest: correctDigest });
      (prisma2.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma2.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma2.revocation.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([rowWithDigest])
        .mockResolvedValueOnce([]);
      const result = await verifier2.fullReplay(AGENT_ID);

      expect(result.revoke.valid).toBe(true);
    });
  });

  describe("incrementalVerify with non-zero startCount for response chain", () => {
    it("should resume response chain from checkpoint with startCount > 0", async () => {
      // First: full replay of 3 responses to get the reference digest
      const allRows = Array.from({ length: 3 }, (_, i) => makeResponseRow(i, i));
      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(allRows)
        .mockResolvedValueOnce([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const fullResult = await verifier.fullReplay(AGENT_ID);

      // Get the digest at count=2 by replaying just first 2
      const prisma2 = createMockPrismaClient();
      const verifier2 = new ReplayVerifier(prisma2);
      const firstTwo = allRows.slice(0, 2);
      (prisma2.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma2.feedbackResponse.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(firstTwo)
        .mockResolvedValueOnce([]);
      (prisma2.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const midResult = await verifier2.fullReplay(AGENT_ID);

      // Incremental from checkpoint at count=2 for response chain
      const prisma3 = createMockPrismaClient();
      const verifier3 = new ReplayVerifier(prisma3);
      (prisma3.hashChainCheckpoint.findFirst as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(null) // feedback - no checkpoint
        .mockResolvedValueOnce({ eventCount: 2, digest: midResult.response.finalDigest }) // response checkpoint at count=2
        .mockResolvedValueOnce(null); // revoke - no checkpoint
      (prisma3.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const lastOne = allRows.slice(2);
      (prisma3.feedbackResponse.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(lastOne)
        .mockResolvedValueOnce([]);
      (prisma3.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const incResult = await verifier3.incrementalVerify(AGENT_ID);

      expect(incResult.valid).toBe(true);
      expect(incResult.response.count).toBe(3);
      expect(incResult.response.finalDigest).toBe(fullResult.response.finalDigest);
    });
  });

  describe("incrementalVerify with non-zero startCount for revoke chain", () => {
    it("should resume revoke chain from checkpoint with startCount > 0", async () => {
      // First: full replay of 3 revocations to get the reference digest
      const allRows = Array.from({ length: 3 }, (_, i) => makeRevocationRow(i, i));
      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(allRows)
        .mockResolvedValueOnce([]);
      const fullResult = await verifier.fullReplay(AGENT_ID);

      // Get the digest at count=2 by replaying just first 2
      const prisma2 = createMockPrismaClient();
      const verifier2 = new ReplayVerifier(prisma2);
      const firstTwo = allRows.slice(0, 2);
      (prisma2.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma2.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma2.revocation.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(firstTwo)
        .mockResolvedValueOnce([]);
      const midResult = await verifier2.fullReplay(AGENT_ID);

      // Incremental from checkpoint at count=2 for revoke chain
      const prisma3 = createMockPrismaClient();
      const verifier3 = new ReplayVerifier(prisma3);
      (prisma3.hashChainCheckpoint.findFirst as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(null) // feedback - no checkpoint
        .mockResolvedValueOnce(null) // response - no checkpoint
        .mockResolvedValueOnce({ eventCount: 2, digest: midResult.revoke.finalDigest }); // revoke checkpoint at count=2
      (prisma3.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma3.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const lastOne = allRows.slice(2);
      (prisma3.revocation.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(lastOne)
        .mockResolvedValueOnce([]);
      const incResult = await verifier3.incrementalVerify(AGENT_ID);

      expect(incResult.valid).toBe(true);
      expect(incResult.revoke.count).toBe(3);
      expect(incResult.revoke.finalDigest).toBe(fullResult.revoke.finalDigest);
    });
  });

  describe("response chain skips validation when runningDigest is null", () => {
    it("should skip digest validation when response runningDigest is null", async () => {
      const row = makeResponseRow(0, 0); // null runningDigest by default

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.response.valid).toBe(true);
      expect(result.response.count).toBe(1);
    });
  });

  describe("revoke chain skips validation when runningDigest is null", () => {
    it("should skip digest validation when revoke runningDigest is null", async () => {
      const row = makeRevocationRow(0, 0); // null runningDigest by default

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce([row])
        .mockResolvedValueOnce([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.revoke.valid).toBe(true);
      expect(result.revoke.count).toBe(1);
    });
  });

  describe("multiple responses and revokes with digest mismatch", () => {
    it("should report first mismatch only in response chain", async () => {
      const wrongDigest = Buffer.alloc(32, 0xff);
      const rows = Array.from({ length: 3 }, (_, i) =>
        makeResponseRow(i, i, { runningDigest: wrongDigest }),
      );

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(rows)
        .mockResolvedValueOnce([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.response.valid).toBe(false);
      expect(result.response.mismatchAt).toBe(1); // First mismatch
      expect(result.response.count).toBe(3); // Still processes all
    });

    it("should report first mismatch only in revoke chain", async () => {
      const wrongDigest = Buffer.alloc(32, 0xff);
      const rows = Array.from({ length: 3 }, (_, i) =>
        makeRevocationRow(i, i, { runningDigest: wrongDigest }),
      );

      (prisma.feedback.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.feedbackResponse.findMany as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      (prisma.revocation.findMany as ReturnType<typeof vi.fn>)
        .mockResolvedValueOnce(rows)
        .mockResolvedValueOnce([]);

      const result = await verifier.fullReplay(AGENT_ID);

      expect(result.revoke.valid).toBe(false);
      expect(result.revoke.mismatchAt).toBe(1);
      expect(result.revoke.count).toBe(3);
    });
  });
});
