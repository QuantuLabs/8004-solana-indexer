import { beforeEach, describe, expect, it, vi } from "vitest";
import { EventContext, handleEvent, handleEventAtomic } from "../../../src/db/handlers.js";
import { ProgramEvent } from "../../../src/parser/types.js";
import { createMockPrismaClient } from "../../mocks/prisma.js";
import {
  TEST_ASSET,
  TEST_BLOCK_TIME,
  TEST_CLIENT,
  TEST_COLLECTION,
  TEST_HASH,
  TEST_OWNER,
  TEST_SIGNATURE,
  TEST_SLOT,
} from "../../mocks/solana.js";

type OrphanFeedbackDelegate = {
  findMany: ReturnType<typeof vi.fn>;
  upsert: ReturnType<typeof vi.fn>;
  create: ReturnType<typeof vi.fn>;
  delete: ReturnType<typeof vi.fn>;
  deleteMany: ReturnType<typeof vi.fn>;
};

function attachOrphanFeedbackDelegate(prisma: unknown): OrphanFeedbackDelegate {
  const delegate: OrphanFeedbackDelegate = {
    findMany: vi.fn().mockResolvedValue([]),
    upsert: vi.fn().mockResolvedValue(undefined),
    create: vi.fn().mockResolvedValue(undefined),
    delete: vi.fn().mockResolvedValue(undefined),
    deleteMany: vi.fn().mockResolvedValue({ count: 0 }),
  };
  (prisma as any).orphanFeedback = delegate;
  return delegate;
}

function makeFeedbackEvent(feedbackIndex: bigint): ProgramEvent {
  return {
    type: "NewFeedback",
    data: {
      asset: TEST_ASSET,
      clientAddress: TEST_CLIENT,
      feedbackIndex,
      value: 1000n + feedbackIndex,
      valueDecimals: 2,
      score: 80,
      tag1: "quality",
      tag2: "speed",
      endpoint: "/api/chat",
      feedbackUri: `ipfs://feedback-${feedbackIndex.toString()}`,
      feedbackFileHash: null,
      sealHash: TEST_HASH,
      slot: TEST_SLOT,
      atomEnabled: true,
      newFeedbackDigest: TEST_HASH,
      newFeedbackCount: feedbackIndex + 1n,
      newTrustTier: 0,
      newQualityScore: 0,
      newConfidence: 0,
      newRiskScore: 0,
      newDiversityRatio: 0,
      isUniqueClient: true,
    },
  };
}

function makeAgentRegisteredEvent(): ProgramEvent {
  return {
    type: "AgentRegistered",
    data: {
      asset: TEST_ASSET,
      collection: TEST_COLLECTION,
      owner: TEST_OWNER,
      atomEnabled: true,
      agentUri: "ipfs://agent",
    },
  };
}

describe("OrphanFeedback flow (owner tests)", () => {
  let prisma: ReturnType<typeof createMockPrismaClient>;
  let ctx: EventContext;
  let orphanFeedback: OrphanFeedbackDelegate;

  beforeEach(() => {
    prisma = createMockPrismaClient();
    orphanFeedback = attachOrphanFeedbackDelegate(prisma);
    ctx = {
      signature: TEST_SIGNATURE,
      slot: TEST_SLOT,
      blockTime: TEST_BLOCK_TIME,
      txIndex: 7,
      eventOrdinal: 0,
    };

    // Missing parent agent is the condition that currently triggers P2003.
    (prisma.agent.findUnique as any).mockResolvedValue(null);
    (prisma.feedback.findMany as any).mockResolvedValue([]);
    (prisma.feedback.upsert as any).mockResolvedValue({ id: "feedback-created", feedbackId: 1n });
  });

  it("feedback before agent: should not crash and should persist orphan (atomic path)", async () => {
    await expect(handleEventAtomic(prisma, makeFeedbackEvent(0n), ctx)).resolves.toBeUndefined();

    const orphanWrites =
      orphanFeedback.upsert.mock.calls.length +
      orphanFeedback.create.mock.calls.length;

    expect(orphanWrites).toBeGreaterThan(0);
    expect(prisma.feedback.upsert).not.toHaveBeenCalled();
  });

  it("agent registration after orphan feedback: should reconcile into feedback and delete orphan", async () => {
    (prisma.agent.findUnique as any)
      .mockResolvedValueOnce(null)
      .mockResolvedValue({ id: TEST_ASSET.toBase58() });

    orphanFeedback.findMany.mockResolvedValue([
      {
        id: "ofb-1",
        agentId: TEST_ASSET.toBase58(),
        client: TEST_CLIENT.toBase58(),
        feedbackIndex: 0n,
        value: "1000",
        valueDecimals: 2,
        score: 80,
        tag1: "quality",
        tag2: "speed",
        endpoint: "/api/chat",
        feedbackUri: "ipfs://feedback-0",
        feedbackHash: Uint8Array.from(TEST_HASH),
        runningDigest: Uint8Array.from(TEST_HASH),
        atomEnabled: true,
        newTrustTier: 0,
        newQualityScore: 0,
        newConfidence: 0,
        newRiskScore: 0,
        newDiversityRatio: 0,
        createdAt: TEST_BLOCK_TIME,
        slot: TEST_SLOT,
        txSignature: TEST_SIGNATURE,
        txIndex: 7,
        eventOrdinal: 0,
      },
    ]);

    await expect(handleEventAtomic(prisma, makeAgentRegisteredEvent(), ctx)).resolves.toBeUndefined();

    expect(prisma.feedback.upsert).toHaveBeenCalledWith(
      expect.objectContaining({
        create: expect.objectContaining({
          agentId: TEST_ASSET.toBase58(),
          client: TEST_CLIENT.toBase58(),
          feedbackIndex: 0n,
        }),
      })
    );
    expect(orphanFeedback.delete).toHaveBeenCalledWith({ where: { id: "ofb-1" } });
  });

  it("agent registration after orphan feedback uses a transaction in non-atomic mode", async () => {
    (prisma.agent.findUnique as any)
      .mockResolvedValueOnce({ id: TEST_ASSET.toBase58(), creator: TEST_OWNER.toBase58() })
      .mockResolvedValue({ id: TEST_ASSET.toBase58() });

    orphanFeedback.findMany.mockResolvedValue([
      {
        id: "ofb-2",
        agentId: TEST_ASSET.toBase58(),
        client: TEST_CLIENT.toBase58(),
        feedbackIndex: 0n,
        value: "1000",
        valueDecimals: 2,
        score: 80,
        tag1: "quality",
        tag2: "speed",
        endpoint: "/api/chat",
        feedbackUri: "ipfs://feedback-0",
        feedbackHash: Uint8Array.from(TEST_HASH),
        runningDigest: Uint8Array.from(TEST_HASH),
        atomEnabled: true,
        newTrustTier: 0,
        newQualityScore: 0,
        newConfidence: 0,
        newRiskScore: 0,
        newDiversityRatio: 0,
        createdAt: TEST_BLOCK_TIME,
        slot: TEST_SLOT,
        txSignature: TEST_SIGNATURE,
        txIndex: 7,
        eventOrdinal: 0,
      },
    ]);

    await expect(handleEvent(prisma, makeAgentRegisteredEvent(), ctx)).resolves.toBeUndefined();

    expect(prisma.$transaction).toHaveBeenCalled();
    expect(orphanFeedback.delete).toHaveBeenCalledWith({ where: { id: "ofb-2" } });
  });

  it("reconciled feedback_id should be deterministic and gapless per agent", async () => {
    (prisma.agent.findUnique as any)
      .mockResolvedValueOnce(null)
      .mockResolvedValue({ id: TEST_ASSET.toBase58() });

    const maxByAgent = new Map<string, bigint>();
    (prisma.feedback.findMany as any).mockImplementation(async (args: any) => {
      const agentId = String(args?.where?.agentId ?? "");
      const max = maxByAgent.get(agentId) ?? 0n;
      return max > 0n ? [{ feedbackId: max }] : [];
    });
    (prisma.feedback.upsert as any).mockImplementation(async ({ create }: any) => {
      const agentId = String(create.agentId);
      const next = BigInt(create.feedbackId ?? 0n);
      const prev = maxByAgent.get(agentId) ?? 0n;
      if (next > prev) maxByAgent.set(agentId, next);
      return { id: `fb-${create.feedbackIndex.toString()}`, feedbackId: next };
    });
    orphanFeedback.findMany.mockResolvedValue([
      {
        id: "ofb-1",
        agentId: TEST_ASSET.toBase58(),
        client: TEST_CLIENT.toBase58(),
        feedbackIndex: 10n,
        value: "1010",
        valueDecimals: 2,
        score: 80,
        tag1: "quality",
        tag2: "speed",
        endpoint: "/api/chat",
        feedbackUri: "ipfs://feedback-10",
        feedbackHash: Uint8Array.from(TEST_HASH),
        runningDigest: Uint8Array.from(TEST_HASH),
        atomEnabled: true,
        newTrustTier: 0,
        newQualityScore: 0,
        newConfidence: 0,
        newRiskScore: 0,
        newDiversityRatio: 0,
        createdAt: TEST_BLOCK_TIME,
        slot: TEST_SLOT,
        txSignature: `${TEST_SIGNATURE}-a`,
        txIndex: 1,
        eventOrdinal: 0,
      },
      {
        id: "ofb-2",
        agentId: TEST_ASSET.toBase58(),
        client: TEST_CLIENT.toBase58(),
        feedbackIndex: 11n,
        value: "1011",
        valueDecimals: 2,
        score: 81,
        tag1: "quality",
        tag2: "speed",
        endpoint: "/api/chat",
        feedbackUri: "ipfs://feedback-11",
        feedbackHash: Uint8Array.from(TEST_HASH),
        runningDigest: Uint8Array.from(TEST_HASH),
        atomEnabled: true,
        newTrustTier: 0,
        newQualityScore: 0,
        newConfidence: 0,
        newRiskScore: 0,
        newDiversityRatio: 0,
        createdAt: TEST_BLOCK_TIME,
        slot: TEST_SLOT + 1n,
        txSignature: `${TEST_SIGNATURE}-b`,
        txIndex: 2,
        eventOrdinal: 0,
      },
    ]);

    await handleEvent(prisma, makeAgentRegisteredEvent(), ctx);

    const createdRows = (prisma.feedback.upsert as any).mock.calls.map(
      (call: any[]) => call[0]?.create
    );
    const scopedIds = createdRows
      .map((row: any) => row?.feedbackId)
      .filter((v: unknown) => typeof v === "bigint") as bigint[];

    expect(scopedIds).toEqual([1n, 2n]);
    expect(new Set(scopedIds).size).toBe(scopedIds.length);
  });
});
