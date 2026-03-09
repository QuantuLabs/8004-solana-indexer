import { PrismaClient } from "@prisma/client";
import { keccak_256 } from "@noble/hashes/sha3.js";
import { PublicKey } from "@solana/web3.js";
import type { Pool } from "pg";
import { createChildLogger } from "../logger.js";

const logger = createChildLogger("replay-verifier");

const DOMAIN_FEEDBACK = Buffer.from("8004_FEEDBACK_V1");
const DOMAIN_RESPONSE = Buffer.from("8004_RESPONSE_V1");
const DOMAIN_REVOKE = Buffer.from("8004_REVOKE_V1");
const DOMAIN_LEAF_V1 = Buffer.from("8004_LEAF_V1____");
const DOMAIN_RESPONSE_LEAF_V1 = Buffer.from("8004_RSP_LEAF_V1");
const DOMAIN_REVOKE_LEAF_V1 = Buffer.from("8004_RVK_LEAF_V1");

const BATCH_SIZE = 1000;
const ZERO_DIGEST = Buffer.alloc(32);

export type ReplayChainType = "feedback" | "response" | "revoke";

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

export interface DerivedCheckpoint {
  chainType: ReplayChainType;
  eventCount: number;
  digest: string;
  createdAt: string;
}

export interface ReplayDataPage {
  events: Array<Record<string, unknown>>;
  hasMore: boolean;
  nextFromCount: number;
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
  const data = Buffer.alloc(16 + 32 + 32 + 8 + 32 + 32 + 32 + 8);
  let offset = 0;
  DOMAIN_RESPONSE_LEAF_V1.copy(data, offset); offset += 16;
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
  const data = Buffer.alloc(16 + 32 + 32 + 8 + 32 + 8);
  let offset = 0;
  DOMAIN_REVOKE_LEAF_V1.copy(data, offset); offset += 16;
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

function hashHexToBuffer(hash: string | null | undefined): Buffer {
  if (!hash) return Buffer.alloc(32);
  return Buffer.from(hash, "hex");
}

function toBigIntSafe(value: unknown, fallback = 0n): bigint {
  if (typeof value === "bigint") return value;
  if (typeof value === "number" && Number.isFinite(value)) return BigInt(Math.trunc(value));
  if (typeof value === "string" && value.trim() !== "") {
    try {
      return BigInt(value);
    } catch {
      return fallback;
    }
  }
  return fallback;
}

function toNumberSafe(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number.parseInt(value, 10);
    if (Number.isFinite(parsed)) return parsed;
  }
  return fallback;
}

function toIsoString(value: unknown): string {
  if (value instanceof Date) return value.toISOString();
  const parsed = new Date(String(value));
  return Number.isNaN(parsed.getTime()) ? new Date(0).toISOString() : parsed.toISOString();
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

async function fetchLatestCheckpointFromPool(
  pool: Pool,
  agentId: string,
  chainType: ReplayChainType,
  targetCount?: number,
): Promise<Checkpoint | null> {
  const target = targetCount !== undefined ? BigInt(targetCount) : null;

  if (chainType === "feedback") {
    const params = [agentId];
    const targetSql = target !== null ? `AND (feedback_index + 1) <= $2::bigint` : "";
    if (target !== null) params.push(target.toString());
    const { rows } = await pool.query<{ event_count: string; digest: string }>(
      `SELECT
         (feedback_index + 1)::text AS event_count,
         encode(running_digest, 'hex') AS digest
       FROM feedbacks
       WHERE asset = $1
         AND status != 'ORPHANED'
         AND running_digest IS NOT NULL
         AND ((feedback_index + 1) % 1000) = 0
         ${targetSql}
       ORDER BY feedback_index DESC
       LIMIT 1`,
      params,
    );
    const row = rows[0];
    return row ? { eventCount: toNumberSafe(row.event_count), digest: row.digest } : null;
  }

  if (chainType === "response") {
    const params = [agentId];
    const targetSql = target !== null ? `AND (response_count + 1) <= $2::bigint` : "";
    if (target !== null) params.push(target.toString());
    const { rows } = await pool.query<{ event_count: string; digest: string }>(
      `WITH ordered AS (
         SELECT
           (ROW_NUMBER() OVER (
             PARTITION BY asset
             ORDER BY block_slot ASC, tx_index ASC NULLS LAST, event_ordinal ASC NULLS LAST, tx_signature ASC NULLS LAST, id ASC
           ) - 1)::bigint AS response_count,
           encode(running_digest, 'hex') AS digest
         FROM feedback_responses
         WHERE asset = $1
           AND status != 'ORPHANED'
           AND running_digest IS NOT NULL
       )
       SELECT
         (response_count + 1)::text AS event_count,
         digest
       FROM ordered
       WHERE ((response_count + 1) % 1000) = 0
         ${targetSql}
       ORDER BY response_count DESC
       LIMIT 1`,
      params,
    );
    const row = rows[0];
    return row ? { eventCount: toNumberSafe(row.event_count), digest: row.digest } : null;
  }

  const params = [agentId];
  const targetSql = target !== null ? `AND (revoke_count + 1) <= $2::bigint` : "";
  if (target !== null) params.push(target.toString());
  const { rows } = await pool.query<{ event_count: string; digest: string }>(
    `SELECT
       (revoke_count + 1)::text AS event_count,
       encode(running_digest, 'hex') AS digest
     FROM revocations
     WHERE asset = $1
       AND status != 'ORPHANED'
       AND running_digest IS NOT NULL
       AND ((revoke_count + 1) % 1000) = 0
       ${targetSql}
     ORDER BY revoke_count DESC
     LIMIT 1`,
    params,
  );
  const row = rows[0];
  return row ? { eventCount: toNumberSafe(row.event_count), digest: row.digest } : null;
}

async function queryCheckpointsForChain(
  pool: Pool,
  agentId: string,
  chainType: ReplayChainType,
): Promise<DerivedCheckpoint[]> {
  if (chainType === "feedback") {
    const { rows } = await pool.query<{ event_count: string; digest: string; created_at: Date | string }>(
      `SELECT
         (feedback_index + 1)::text AS event_count,
         encode(running_digest, 'hex') AS digest,
         created_at
       FROM feedbacks
       WHERE asset = $1
         AND status != 'ORPHANED'
         AND running_digest IS NOT NULL
         AND ((feedback_index + 1) % 1000) = 0
       ORDER BY feedback_index ASC`,
      [agentId],
    );
    return rows.map((row) => ({
      chainType,
      eventCount: toNumberSafe(row.event_count),
      digest: row.digest,
      createdAt: toIsoString(row.created_at),
    }));
  }

  if (chainType === "response") {
    const { rows } = await pool.query<{ event_count: string; digest: string; created_at: Date | string }>(
      `WITH ordered AS (
         SELECT
           (ROW_NUMBER() OVER (
             PARTITION BY asset
             ORDER BY block_slot ASC, tx_index ASC NULLS LAST, event_ordinal ASC NULLS LAST, tx_signature ASC NULLS LAST, id ASC
           ) - 1)::bigint AS response_count,
           encode(running_digest, 'hex') AS digest,
           created_at
         FROM feedback_responses
         WHERE asset = $1
           AND status != 'ORPHANED'
           AND running_digest IS NOT NULL
       )
       SELECT
         (response_count + 1)::text AS event_count,
         digest,
         created_at
       FROM ordered
       WHERE ((response_count + 1) % 1000) = 0
       ORDER BY response_count ASC`,
      [agentId],
    );
    return rows.map((row) => ({
      chainType,
      eventCount: toNumberSafe(row.event_count),
      digest: row.digest,
      createdAt: toIsoString(row.created_at),
    }));
  }

  const { rows } = await pool.query<{ event_count: string; digest: string; created_at: Date | string }>(
    `SELECT
       (revoke_count + 1)::text AS event_count,
       encode(running_digest, 'hex') AS digest,
       created_at
     FROM revocations
     WHERE asset = $1
       AND status != 'ORPHANED'
       AND running_digest IS NOT NULL
       AND ((revoke_count + 1) % 1000) = 0
     ORDER BY revoke_count ASC`,
    [agentId],
  );
  return rows.map((row) => ({
    chainType,
    eventCount: toNumberSafe(row.event_count),
    digest: row.digest,
    createdAt: toIsoString(row.created_at),
  }));
}

export async function listCheckpointsFromPool(
  pool: Pool,
  agentId: string,
  chainType?: ReplayChainType,
): Promise<DerivedCheckpoint[]> {
  if (chainType) {
    return queryCheckpointsForChain(pool, agentId, chainType);
  }

  const [feedback, response, revoke] = await Promise.all([
    queryCheckpointsForChain(pool, agentId, "feedback"),
    queryCheckpointsForChain(pool, agentId, "response"),
    queryCheckpointsForChain(pool, agentId, "revoke"),
  ]);

  return [...feedback, ...response, ...revoke].sort((a, b) => {
    if (a.eventCount !== b.eventCount) return a.eventCount - b.eventCount;
    return a.chainType.localeCompare(b.chainType);
  });
}

export async function getLatestCheckpointsFromPool(pool: Pool, agentId: string): Promise<{
  feedback: Omit<DerivedCheckpoint, "chainType"> | null;
  response: Omit<DerivedCheckpoint, "chainType"> | null;
  revoke: Omit<DerivedCheckpoint, "chainType"> | null;
}> {
  const [feedback, response, revoke] = await Promise.all([
    queryCheckpointsForChain(pool, agentId, "feedback"),
    queryCheckpointsForChain(pool, agentId, "response"),
    queryCheckpointsForChain(pool, agentId, "revoke"),
  ]);

  const stripChain = (entry: DerivedCheckpoint | undefined): Omit<DerivedCheckpoint, "chainType"> | null => (
    entry ? { eventCount: entry.eventCount, digest: entry.digest, createdAt: entry.createdAt } : null
  );

  return {
    feedback: stripChain(feedback[feedback.length - 1]),
    response: stripChain(response[response.length - 1]),
    revoke: stripChain(revoke[revoke.length - 1]),
  };
}

export async function fetchReplayDataFromPool(
  pool: Pool,
  asset: string,
  chainType: ReplayChainType,
  fromCount = 0,
  toCount?: number,
  limit = BATCH_SIZE,
): Promise<ReplayDataPage> {
  const lowerBound = BigInt(fromCount);
  const upperBound = toCount !== undefined ? BigInt(toCount) : lowerBound + BigInt(limit);

  if (upperBound <= lowerBound) {
    return { events: [], hasMore: false, nextFromCount: fromCount };
  }

  const take = Math.max(0, Math.min(limit, Number(upperBound - lowerBound)));
  if (take === 0) {
    return { events: [], hasMore: false, nextFromCount: fromCount };
  }

  if (chainType === "feedback") {
    const { rows } = await pool.query<{
      client: string;
      feedback_index: string;
      feedback_hash: string | null;
      slot: string;
      running_digest: string | null;
    }>(
      `SELECT
         client_address AS client,
         feedback_index::text AS feedback_index,
         feedback_hash,
         block_slot::text AS slot,
         encode(running_digest, 'hex') AS running_digest
       FROM feedbacks
       WHERE asset = $1
         AND status != 'ORPHANED'
         AND feedback_index >= $2::bigint
         AND feedback_index < $3::bigint
       ORDER BY feedback_index ASC
       LIMIT $4::int`,
      [asset, lowerBound.toString(), upperBound.toString(), take],
    );

    const events = rows.map((row) => ({
      asset,
      client: row.client,
      feedback_index: row.feedback_index,
      feedback_hash: row.feedback_hash,
      slot: toNumberSafe(row.slot),
      running_digest: row.running_digest,
    }));
    const last = rows.length > 0 ? toNumberSafe(rows[rows.length - 1]?.feedback_index) : fromCount;
    return {
      events,
      hasMore: rows.length === take,
      nextFromCount: rows.length > 0 ? last + 1 : fromCount,
    };
  }

  if (chainType === "response") {
    const { rows } = await pool.query<{
      client: string;
      feedback_index: string;
      responder: string;
      response_hash: string | null;
      feedback_hash: string | null;
      slot: string;
      running_digest: string | null;
      response_count: string;
    }>(
      `WITH ordered AS (
         SELECT
           fr.asset,
           fr.client_address AS client,
           fr.feedback_index::text AS feedback_index,
           fr.responder,
           fr.response_hash,
           encode(fr.running_digest, 'hex') AS running_digest,
           fr.block_slot::text AS slot,
           (ROW_NUMBER() OVER (
             PARTITION BY fr.asset
             ORDER BY fr.block_slot ASC, fr.tx_index ASC NULLS LAST, fr.event_ordinal ASC NULLS LAST, fr.tx_signature ASC NULLS LAST, fr.id ASC
           ) - 1)::bigint::text AS response_count
         FROM feedback_responses fr
         WHERE fr.asset = $1
           AND fr.status != 'ORPHANED'
       )
       SELECT
         o.client,
         o.feedback_index,
         o.responder,
         o.response_hash,
         f.feedback_hash,
         o.slot,
         o.running_digest,
         o.response_count
       FROM ordered o
       LEFT JOIN feedbacks f
         ON f.asset = $1
        AND f.client_address = o.client
        AND f.feedback_index::text = o.feedback_index
        AND f.status != 'ORPHANED'
       WHERE (o.response_count)::bigint >= $2::bigint
         AND (o.response_count)::bigint < $3::bigint
       ORDER BY (o.response_count)::bigint ASC
       LIMIT $4::int`,
      [asset, lowerBound.toString(), upperBound.toString(), take],
    );

    const events = rows.map((row) => ({
      asset,
      client: row.client,
      feedback_index: row.feedback_index,
      responder: row.responder,
      response_hash: row.response_hash,
      feedback_hash: row.feedback_hash,
      slot: toNumberSafe(row.slot),
      running_digest: row.running_digest,
      response_count: toNumberSafe(row.response_count),
    }));
    const last = rows.length > 0 ? toNumberSafe(rows[rows.length - 1]?.response_count) : fromCount;
    return {
      events,
      hasMore: rows.length === take,
      nextFromCount: rows.length > 0 ? last + 1 : fromCount,
    };
  }

  const { rows } = await pool.query<{
    client: string;
    feedback_index: string;
    feedback_hash: string | null;
    slot: string;
    running_digest: string | null;
    revoke_count: string;
  }>(
    `SELECT
       client_address AS client,
       feedback_index::text AS feedback_index,
       feedback_hash,
       slot::text AS slot,
       encode(running_digest, 'hex') AS running_digest,
       revoke_count::text AS revoke_count
     FROM revocations
     WHERE asset = $1
       AND status != 'ORPHANED'
       AND revoke_count >= $2::bigint
       AND revoke_count < $3::bigint
     ORDER BY revoke_count ASC
     LIMIT $4::int`,
    [asset, lowerBound.toString(), upperBound.toString(), take],
  );

  const events = rows.map((row) => ({
    asset,
    client: row.client,
    feedback_index: row.feedback_index,
    feedback_hash: row.feedback_hash,
    slot: toNumberSafe(row.slot),
    running_digest: row.running_digest,
    revoke_count: row.revoke_count,
  }));
  const last = rows.length > 0 ? toNumberSafe(rows[rows.length - 1]?.revoke_count) : fromCount;
  return {
    events,
    hasMore: rows.length === take,
    nextFromCount: rows.length > 0 ? last + 1 : fromCount,
  };
}

export class PoolReplayVerifier {
  CHECKPOINT_INTERVAL = 1000;

  constructor(private pool: Pool) {}

  async incrementalVerify(agentId: string): Promise<VerificationResult> {
    const start = Date.now();

    const [fbCp, rsCp, rvCp] = await Promise.all([
      fetchLatestCheckpointFromPool(this.pool, agentId, "feedback"),
      fetchLatestCheckpointFromPool(this.pool, agentId, "response"),
      fetchLatestCheckpointFromPool(this.pool, agentId, "revoke"),
    ]);

    const [feedback, response, revoke] = await Promise.all([
      this.replayFeedback(agentId, fbCp),
      this.replayResponse(agentId, rsCp),
      this.replayRevoke(agentId, rvCp),
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

  private async replayFeedback(agentId: string, checkpoint: Checkpoint | null): Promise<ChainReplayResult> {
    let digest = Buffer.from(checkpoint ? Buffer.from(checkpoint.digest, "hex") : ZERO_DIGEST);
    let count = checkpoint?.eventCount ?? 0;
    let valid = true;
    let mismatchAt: number | undefined;
    let checkpointsStored = 0;
    let nextIndex = BigInt(count);

    while (true) {
      const { rows } = await this.pool.query<{
        client: string;
        feedback_index: string;
        feedback_hash: string | null;
        slot: string;
        running_digest: string | null;
      }>(
        `SELECT
           client_address AS client,
           feedback_index::text AS feedback_index,
           feedback_hash,
           block_slot::text AS slot,
           encode(running_digest, 'hex') AS running_digest
         FROM feedbacks
         WHERE asset = $1
           AND status != 'ORPHANED'
           AND feedback_index >= $2::bigint
         ORDER BY feedback_index ASC
         LIMIT $3::int`,
        [agentId, nextIndex.toString(), BATCH_SIZE],
      );
      if (rows.length === 0) break;

      for (const row of rows) {
        const feedbackIndex = toBigIntSafe(row.feedback_index);
        const slot = toBigIntSafe(row.slot);
        const leaf = computeFeedbackLeafV1(
          pubkeyToBuffer(agentId),
          pubkeyToBuffer(row.client),
          feedbackIndex,
          hashHexToBuffer(row.feedback_hash),
          slot,
        );
        digest = chainHash(digest, DOMAIN_FEEDBACK, leaf);
        count++;

        if (row.running_digest && valid) {
          const storedDigest = Buffer.from(row.running_digest, "hex");
          if (!digest.equals(storedDigest)) {
            valid = false;
            mismatchAt = count;
            logger.warn({ agentId, chainType: "feedback", count, expected: storedDigest.toString("hex"), computed: digest.toString("hex") }, "Digest mismatch");
          }
        }

        if (count % this.CHECKPOINT_INTERVAL === 0) {
          checkpointsStored++;
        }
      }

      nextIndex = toBigIntSafe(rows[rows.length - 1]?.feedback_index, nextIndex) + 1n;
      if (rows.length < BATCH_SIZE) break;
    }

    return {
      chainType: "feedback",
      finalDigest: digest.toString("hex"),
      count,
      valid,
      mismatchAt,
      checkpointsStored,
    };
  }

  private async replayResponse(agentId: string, checkpoint: Checkpoint | null): Promise<ChainReplayResult> {
    let digest = Buffer.from(checkpoint ? Buffer.from(checkpoint.digest, "hex") : ZERO_DIGEST);
    let count = checkpoint?.eventCount ?? 0;
    let valid = true;
    let mismatchAt: number | undefined;
    let checkpointsStored = 0;
    let nextCount = BigInt(count);

    while (true) {
      const { rows } = await this.pool.query<{
        client: string;
        feedback_index: string;
        responder: string;
        response_hash: string | null;
        feedback_hash: string | null;
        slot: string;
        running_digest: string | null;
        response_count: string;
      }>(
        `WITH ordered AS (
           SELECT
             fr.asset,
             fr.client_address AS client,
             fr.feedback_index::text AS feedback_index,
             fr.responder,
             fr.response_hash,
             encode(fr.running_digest, 'hex') AS running_digest,
             fr.block_slot::text AS slot,
             (ROW_NUMBER() OVER (
               PARTITION BY fr.asset
               ORDER BY fr.block_slot ASC, fr.tx_index ASC NULLS LAST, fr.event_ordinal ASC NULLS LAST, fr.tx_signature ASC NULLS LAST, fr.id ASC
             ) - 1)::bigint::text AS response_count
           FROM feedback_responses fr
           WHERE fr.asset = $1
             AND fr.status != 'ORPHANED'
         )
         SELECT
           o.client,
           o.feedback_index,
           o.responder,
           o.response_hash,
           f.feedback_hash,
           o.slot,
           o.running_digest,
           o.response_count
         FROM ordered o
         LEFT JOIN feedbacks f
           ON f.asset = $1
          AND f.client_address = o.client
          AND f.feedback_index::text = o.feedback_index
          AND f.status != 'ORPHANED'
         WHERE (o.response_count)::bigint >= $2::bigint
         ORDER BY (o.response_count)::bigint ASC
         LIMIT $3::int`,
        [agentId, nextCount.toString(), BATCH_SIZE],
      );
      if (rows.length === 0) break;

      for (const row of rows) {
        const leaf = computeResponseLeaf(
          pubkeyToBuffer(agentId),
          pubkeyToBuffer(row.client),
          toBigIntSafe(row.feedback_index),
          pubkeyToBuffer(row.responder),
          hashHexToBuffer(row.response_hash),
          hashHexToBuffer(row.feedback_hash),
          toBigIntSafe(row.slot),
        );
        digest = chainHash(digest, DOMAIN_RESPONSE, leaf);
        count++;

        if (row.running_digest && valid) {
          const storedDigest = Buffer.from(row.running_digest, "hex");
          if (!digest.equals(storedDigest)) {
            valid = false;
            mismatchAt = count;
            logger.warn({ agentId, chainType: "response", count, expected: storedDigest.toString("hex"), computed: digest.toString("hex") }, "Digest mismatch");
          }
        }

        if (count % this.CHECKPOINT_INTERVAL === 0) {
          checkpointsStored++;
        }
      }

      nextCount = toBigIntSafe(rows[rows.length - 1]?.response_count, nextCount) + 1n;
      if (rows.length < BATCH_SIZE) break;
    }

    return {
      chainType: "response",
      finalDigest: digest.toString("hex"),
      count,
      valid,
      mismatchAt,
      checkpointsStored,
    };
  }

  private async replayRevoke(agentId: string, checkpoint: Checkpoint | null): Promise<ChainReplayResult> {
    let digest = Buffer.from(checkpoint ? Buffer.from(checkpoint.digest, "hex") : ZERO_DIGEST);
    let count = checkpoint?.eventCount ?? 0;
    let valid = true;
    let mismatchAt: number | undefined;
    let checkpointsStored = 0;
    let nextCount = BigInt(count);

    while (true) {
      const { rows } = await this.pool.query<{
        client: string;
        feedback_index: string;
        feedback_hash: string | null;
        slot: string;
        running_digest: string | null;
        revoke_count: string;
      }>(
        `SELECT
           client_address AS client,
           feedback_index::text AS feedback_index,
           feedback_hash,
           slot::text AS slot,
           encode(running_digest, 'hex') AS running_digest,
           revoke_count::text AS revoke_count
         FROM revocations
         WHERE asset = $1
           AND status != 'ORPHANED'
           AND revoke_count >= $2::bigint
         ORDER BY revoke_count ASC
         LIMIT $3::int`,
        [agentId, nextCount.toString(), BATCH_SIZE],
      );
      if (rows.length === 0) break;

      for (const row of rows) {
        const leaf = computeRevokeLeaf(
          pubkeyToBuffer(agentId),
          pubkeyToBuffer(row.client),
          toBigIntSafe(row.feedback_index),
          hashHexToBuffer(row.feedback_hash),
          toBigIntSafe(row.slot),
        );
        digest = chainHash(digest, DOMAIN_REVOKE, leaf);
        count++;

        if (row.running_digest && valid) {
          const storedDigest = Buffer.from(row.running_digest, "hex");
          if (!digest.equals(storedDigest)) {
            valid = false;
            mismatchAt = count;
            logger.warn({ agentId, chainType: "revoke", count, expected: storedDigest.toString("hex"), computed: digest.toString("hex") }, "Digest mismatch");
          }
        }

        if (count % this.CHECKPOINT_INTERVAL === 0) {
          checkpointsStored++;
        }
      }

      nextCount = toBigIntSafe(rows[rows.length - 1]?.revoke_count, nextCount) + 1n;
      if (rows.length < BATCH_SIZE) break;
    }

    return {
      chainType: "revoke",
      finalDigest: digest.toString("hex"),
      count,
      valid,
      mismatchAt,
      checkpointsStored,
    };
  }
}
