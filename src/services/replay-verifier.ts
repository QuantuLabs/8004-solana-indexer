import { PrismaClient } from "@prisma/client";
import { keccak_256 } from "@noble/hashes/sha3.js";
import { PublicKey } from "@solana/web3.js";
import { createChildLogger } from "../logger.js";

const logger = createChildLogger("replay-verifier");

const DOMAIN_FEEDBACK = Buffer.from("8004_FEEDBACK_V1");
const DOMAIN_RESPONSE = Buffer.from("8004_RESPONSE_V1");
const DOMAIN_REVOKE = Buffer.from("8004_REVOKE_V1");
const DOMAIN_LEAF_V1 = Buffer.from("8004_LEAF_V1____");

const BATCH_SIZE = 1000;
const ZERO_DIGEST = Buffer.alloc(32);

export interface ChainReplayResult {
  chainType: string;
  finalDigest: string;
  count: number;
  valid: boolean;
  mismatchAt?: number;
  checkpointsStored: number;
}

export interface VerificationResult {
  agentId: string;
  feedback: ChainReplayResult;
  response: ChainReplayResult;
  revoke: ChainReplayResult;
  valid: boolean;
  duration: number;
}

interface Checkpoint {
  eventCount: number;
  digest: string;
}

function chainHash(prevDigest: Buffer, domain: Buffer, leaf: Buffer): Buffer<ArrayBuffer> {
  const data = Buffer.concat([prevDigest, domain, leaf]);
  return Buffer.from(keccak_256(data)) as Buffer<ArrayBuffer>;
}

function computeFeedbackLeafV1(
  asset: Buffer,
  client: Buffer,
  feedbackIndex: bigint,
  sealHash: Buffer,
  slot: bigint,
): Buffer {
  const data = Buffer.alloc(16 + 32 + 32 + 8 + 32 + 8);
  let offset = 0;
  DOMAIN_LEAF_V1.copy(data, offset); offset += 16;
  asset.copy(data, offset); offset += 32;
  client.copy(data, offset); offset += 32;
  data.writeBigUInt64LE(feedbackIndex, offset); offset += 8;
  sealHash.copy(data, offset); offset += 32;
  data.writeBigUInt64LE(slot, offset);
  return Buffer.from(keccak_256(data));
}

function computeResponseLeaf(
  asset: Buffer,
  client: Buffer,
  feedbackIndex: bigint,
  responder: Buffer,
  responseHash: Buffer,
  feedbackHash: Buffer,
  slot: bigint,
): Buffer {
  const data = Buffer.alloc(32 + 32 + 8 + 32 + 32 + 32 + 8);
  let offset = 0;
  asset.copy(data, offset); offset += 32;
  client.copy(data, offset); offset += 32;
  data.writeBigUInt64LE(feedbackIndex, offset); offset += 8;
  responder.copy(data, offset); offset += 32;
  responseHash.copy(data, offset); offset += 32;
  feedbackHash.copy(data, offset); offset += 32;
  data.writeBigUInt64LE(slot, offset);
  return Buffer.from(keccak_256(data));
}

function computeRevokeLeaf(
  asset: Buffer,
  client: Buffer,
  feedbackIndex: bigint,
  feedbackHash: Buffer,
  slot: bigint,
): Buffer {
  const data = Buffer.alloc(32 + 32 + 8 + 32 + 8);
  let offset = 0;
  asset.copy(data, offset); offset += 32;
  client.copy(data, offset); offset += 32;
  data.writeBigUInt64LE(feedbackIndex, offset); offset += 8;
  feedbackHash.copy(data, offset); offset += 32;
  data.writeBigUInt64LE(slot, offset);
  return Buffer.from(keccak_256(data));
}

function pubkeyToBuffer(base58: string): Buffer {
  return new PublicKey(base58).toBuffer();
}

function hashBytesToBuffer(hash: Uint8Array | null): Buffer {
  if (!hash) return Buffer.alloc(32);
  return Buffer.from(hash);
}

export class ReplayVerifier {
  CHECKPOINT_INTERVAL = 1000;

  constructor(private prisma: PrismaClient) {}

  async fullReplay(agentId: string): Promise<VerificationResult> {
    const start = Date.now();

    const [feedback, response, revoke] = await Promise.all([
      this.replayChainFromDB(agentId, "feedback", Buffer.from(ZERO_DIGEST), 0),
      this.replayChainFromDB(agentId, "response", Buffer.from(ZERO_DIGEST), 0),
      this.replayChainFromDB(agentId, "revoke", Buffer.from(ZERO_DIGEST), 0),
    ]);

    return {
      agentId,
      feedback,
      response,
      revoke,
      valid: feedback.valid && response.valid && revoke.valid,
      duration: Date.now() - start,
    };
  }

  async incrementalVerify(agentId: string): Promise<VerificationResult> {
    const start = Date.now();

    const [fbCp, rsCp, rvCp] = await Promise.all([
      this.getCheckpoint(agentId, "feedback"),
      this.getCheckpoint(agentId, "response"),
      this.getCheckpoint(agentId, "revoke"),
    ]);

    const [feedback, response, revoke] = await Promise.all([
      this.replayChainFromDB(
        agentId, "feedback",
        fbCp ? Buffer.from(fbCp.digest, "hex") : Buffer.from(ZERO_DIGEST),
        fbCp?.eventCount ?? 0,
      ),
      this.replayChainFromDB(
        agentId, "response",
        rsCp ? Buffer.from(rsCp.digest, "hex") : Buffer.from(ZERO_DIGEST),
        rsCp?.eventCount ?? 0,
      ),
      this.replayChainFromDB(
        agentId, "revoke",
        rvCp ? Buffer.from(rvCp.digest, "hex") : Buffer.from(ZERO_DIGEST),
        rvCp?.eventCount ?? 0,
      ),
    ]);

    return {
      agentId,
      feedback,
      response,
      revoke,
      valid: feedback.valid && response.valid && revoke.valid,
      duration: Date.now() - start,
    };
  }

  async getCheckpoint(agentId: string, chainType: string, targetCount?: number): Promise<Checkpoint | null> {
    const where: { agentId: string; chainType: string; eventCount?: { lte: bigint } } = { agentId, chainType };
    if (targetCount !== undefined) {
      where.eventCount = { lte: BigInt(targetCount) };
    }

    const cp = await this.prisma.hashChainCheckpoint.findFirst({
      where,
      orderBy: { eventCount: "desc" },
    });

    if (!cp) return null;
    return { eventCount: Number(cp.eventCount), digest: cp.digest };
  }

  private async replayChainFromDB(
    agentId: string,
    chainType: "feedback" | "response" | "revoke",
    startDigest: Buffer,
    startCount: number,
  ): Promise<ChainReplayResult> {
    let digest = Buffer.from(startDigest);
    let count = startCount;
    let valid = true;
    let mismatchAt: number | undefined;
    let checkpointsStored = 0;

    if (chainType === "feedback") {
      let lastIndex = startCount > 0 ? BigInt(startCount - 1) : -1n;
      while (true) {
        const feedbacks = await this.prisma.feedback.findMany({
          where: { agentId, feedbackIndex: { gt: lastIndex }, status: { not: "ORPHANED" } },
          orderBy: { feedbackIndex: "asc" },
          take: BATCH_SIZE,
        });
        if (feedbacks.length === 0) break;

        for (const f of feedbacks) {
          const assetBuf = pubkeyToBuffer(f.agentId);
          const clientBuf = pubkeyToBuffer(f.client);
          const sealHash = hashBytesToBuffer(f.feedbackHash);
          const slot = f.createdSlot ?? 0n;

          const leaf = computeFeedbackLeafV1(assetBuf, clientBuf, f.feedbackIndex, sealHash, slot);
          digest = chainHash(digest, DOMAIN_FEEDBACK, leaf);
          count++;

          if (f.runningDigest && valid) {
            const storedDigest = Buffer.from(f.runningDigest);
            if (!digest.equals(storedDigest)) {
              valid = false;
              mismatchAt = count;
              logger.warn({ agentId, chainType, count, expected: storedDigest.toString("hex"), computed: digest.toString("hex") }, "Digest mismatch");
            }
          }

          if (count % this.CHECKPOINT_INTERVAL === 0) {
            await this.storeCheckpoint(agentId, chainType, count, digest.toString("hex"));
            checkpointsStored++;
          }
        }
        lastIndex = feedbacks[feedbacks.length - 1].feedbackIndex;
        if (feedbacks.length < BATCH_SIZE) break;
      }
    } else if (chainType === "response") {
      let lastResponseCount = startCount > 0 ? BigInt(startCount - 1) : -1n;
      while (true) {
        const responses = await this.prisma.feedbackResponse.findMany({
          where: {
            feedback: { agentId },
            responseCount: { gt: lastResponseCount },
            status: { not: "ORPHANED" },
          },
          orderBy: { responseCount: "asc" },
          take: BATCH_SIZE,
          include: {
            feedback: {
              select: { agentId: true, client: true, feedbackIndex: true, feedbackHash: true },
            },
          },
        });
        if (responses.length === 0) break;

        for (const r of responses) {
          const assetBuf = pubkeyToBuffer(r.feedback.agentId);
          const clientBuf = pubkeyToBuffer(r.feedback.client);
          const responderBuf = pubkeyToBuffer(r.responder);
          const responseHash = hashBytesToBuffer(r.responseHash);
          const feedbackHash = hashBytesToBuffer(r.feedback.feedbackHash);
          const slot = r.slot ?? 0n;

          const leaf = computeResponseLeaf(assetBuf, clientBuf, r.feedback.feedbackIndex, responderBuf, responseHash, feedbackHash, slot);
          digest = chainHash(digest, DOMAIN_RESPONSE, leaf);
          count++;

          if (r.runningDigest && valid) {
            const storedDigest = Buffer.from(r.runningDigest);
            if (!digest.equals(storedDigest)) {
              valid = false;
              mismatchAt = count;
              logger.warn({ agentId, chainType, count, expected: storedDigest.toString("hex"), computed: digest.toString("hex") }, "Digest mismatch");
            }
          }

          if (count % this.CHECKPOINT_INTERVAL === 0) {
            await this.storeCheckpoint(agentId, chainType, count, digest.toString("hex"));
            checkpointsStored++;
          }
        }
        lastResponseCount = responses[responses.length - 1].responseCount ?? lastResponseCount;
        if (responses.length < BATCH_SIZE) break;
      }
    } else {
      let lastRevokeCount = startCount > 0 ? BigInt(startCount - 1) : -1n;
      while (true) {
        const revocations = await this.prisma.revocation.findMany({
          where: { agentId, revokeCount: { gt: lastRevokeCount }, status: { not: "ORPHANED" } },
          orderBy: { revokeCount: "asc" },
          take: BATCH_SIZE,
        });
        if (revocations.length === 0) break;

        for (const r of revocations) {
          const assetBuf = pubkeyToBuffer(r.agentId);
          const clientBuf = pubkeyToBuffer(r.client);
          const feedbackHash = hashBytesToBuffer(r.feedbackHash);
          const slot = r.slot;

          const leaf = computeRevokeLeaf(assetBuf, clientBuf, r.feedbackIndex, feedbackHash, slot);
          digest = chainHash(digest, DOMAIN_REVOKE, leaf);
          count++;

          if (r.runningDigest && valid) {
            const storedDigest = Buffer.from(r.runningDigest);
            if (!digest.equals(storedDigest)) {
              valid = false;
              mismatchAt = count;
              logger.warn({ agentId, chainType, count, expected: storedDigest.toString("hex"), computed: digest.toString("hex") }, "Digest mismatch");
            }
          }

          if (count % this.CHECKPOINT_INTERVAL === 0) {
            await this.storeCheckpoint(agentId, chainType, count, digest.toString("hex"));
            checkpointsStored++;
          }
        }
        lastRevokeCount = revocations[revocations.length - 1].revokeCount;
        if (revocations.length < BATCH_SIZE) break;
      }
    }

    return {
      chainType,
      finalDigest: digest.toString("hex"),
      count,
      valid,
      mismatchAt,
      checkpointsStored,
    };
  }

  private async storeCheckpoint(agentId: string, chainType: string, eventCount: number, digest: string): Promise<void> {
    const eventCountBigInt = BigInt(eventCount);
    await this.prisma.hashChainCheckpoint.upsert({
      where: { agentId_chainType_eventCount: { agentId, chainType, eventCount: eventCountBigInt } },
      create: { agentId, chainType, eventCount: eventCountBigInt, digest },
      update: { digest },
    });
  }
}
