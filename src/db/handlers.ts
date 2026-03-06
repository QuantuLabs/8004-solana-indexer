import { PrismaClient } from "@prisma/client";
import PQueue from "p-queue";
import { PublicKey } from "@solana/web3.js";
import {
  ProgramEvent,
  AgentRegistered,
  AtomEnabled,
  AgentOwnerSynced,
  UriUpdated,
  WalletUpdated,
  WalletResetOnOwnerSync,
  MetadataSet,
  MetadataDeleted,
  RegistryInitialized,
  CollectionPointerSet,
  ParentAssetSet,
  NewFeedback,
  FeedbackRevoked,
  ResponseAppended,
  ValidationRequested,
  ValidationResponded,
} from "../parser/types.js";
import { createChildLogger } from "../logger.js";
import { config, ChainStatus } from "../config.js";
import * as supabaseHandlers from "./supabase.js";
import { classifyRevocationStatus } from "./revocation-classification.js";
import { digestUri, serializeValue, toDeterministicUriStatus } from "../indexer/uriDigest.js";
import { digestCollectionPointerDoc } from "../indexer/collectionDigest.js";
import { compressForStorage } from "../utils/compression.js";
import { stripNullBytes } from "../utils/sanitize.js";
import { DEFAULT_PUBKEY, STANDARD_URI_FIELDS } from "../constants.js";

const logger = createChildLogger("db-handlers");

// Global concurrency limiter for URI metadata fetching
// Prevents OOM from unbounded fire-and-forget digest operations
const MAX_URI_FETCH_CONCURRENT = 5;
const uriDigestQueue = new PQueue({ concurrency: MAX_URI_FETCH_CONCURRENT });
const MAX_COLLECTION_FETCH_CONCURRENT = 3;
const collectionDigestQueue = new PQueue({ concurrency: MAX_COLLECTION_FETCH_CONCURRENT });
const pendingUriDigests = new Map<string, { prisma: PrismaClient; uri: string; verifiedAt?: Date }>();
const inFlightUriDigests = new Set<string>();
const pendingCollectionDigests = new Map<string, { prisma: PrismaClient; pointer: string }>();
const inFlightCollectionDigests = new Set<string>();
const LOCAL_RECOVERY_INTERVAL_MS = 60_000;
const LOCAL_RECOVERY_BATCH_SIZE = 500;
let localRecoveryInterval: NodeJS.Timeout | null = null;
let localRecoveryInFlight = false;
let agentIdAssignmentTail: Promise<void> = Promise.resolve();
let feedbackIdAssignmentTail: Promise<void> = Promise.resolve();
let responseIdAssignmentTail: Promise<void> = Promise.resolve();
let revocationIdAssignmentTail: Promise<void> = Promise.resolve();
let collectionIdAssignmentTail: Promise<void> = Promise.resolve();

async function withAssignmentLock<T>(
  getTail: () => Promise<void>,
  setTail: (tail: Promise<void>) => void,
  task: () => Promise<T>
): Promise<T> {
  const previous = getTail();
  let release: () => void = () => {};
  setTail(new Promise<void>((resolve) => {
    release = () => resolve();
  }));
  await previous;
  try {
    return await task();
  } finally {
    release();
  }
}

async function withAgentIdAssignmentLock<T>(task: () => Promise<T>): Promise<T> {
  return withAssignmentLock(
    () => agentIdAssignmentTail,
    (tail) => { agentIdAssignmentTail = tail; },
    task
  );
}

async function withFeedbackIdAssignmentLock<T>(task: () => Promise<T>): Promise<T> {
  return withAssignmentLock(
    () => feedbackIdAssignmentTail,
    (tail) => { feedbackIdAssignmentTail = tail; },
    task
  );
}

async function withResponseIdAssignmentLock<T>(task: () => Promise<T>): Promise<T> {
  return withAssignmentLock(
    () => responseIdAssignmentTail,
    (tail) => { responseIdAssignmentTail = tail; },
    task
  );
}

async function withRevocationIdAssignmentLock<T>(task: () => Promise<T>): Promise<T> {
  return withAssignmentLock(
    () => revocationIdAssignmentTail,
    (tail) => { revocationIdAssignmentTail = tail; },
    task
  );
}

async function withCollectionIdAssignmentLock<T>(task: () => Promise<T>): Promise<T> {
  return withAssignmentLock(
    () => collectionIdAssignmentTail,
    (tail) => { collectionIdAssignmentTail = tail; },
    task
  );
}

function asBigInt(value: bigint | number | null | undefined): bigint {
  if (typeof value === "bigint") return value;
  if (typeof value === "number" && Number.isFinite(value)) return BigInt(Math.trunc(value));
  return 0n;
}

type CollectionDelegate = {
  findUnique(args: unknown): Promise<{ collectionId: bigint | null } | null>;
  findMany(args: unknown): Promise<Array<{ collectionId: bigint | null }>>;
  upsert(args: unknown): Promise<unknown>;
};

type CollectionRawSqlClient = {
  $executeRawUnsafe?: (query: string, ...values: unknown[]) => Promise<unknown>;
};

const COLLECTION_ID_ASSIGNMENT_MAX_RETRIES = 3;
const ATOMIC_COLLECTION_ID_TX_MAX_RETRIES = 2;
const COLLECTION_POINTER_DB_ALLOCATED_UPSERT_SQL = `
INSERT INTO "CollectionPointer" (
  "col",
  "creator",
  "firstSeenAsset",
  "firstSeenAt",
  "firstSeenSlot",
  "firstSeenTxSignature",
  "lastSeenAt",
  "lastSeenSlot",
  "lastSeenTxSignature",
  "assetCount",
  "collection_id"
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (
  SELECT COALESCE(MAX("collection_id"), 0) + 1
  FROM "CollectionPointer"
  WHERE "collection_id" IS NOT NULL
))
ON CONFLICT("col", "creator") DO UPDATE SET
  "lastSeenAt" = excluded."lastSeenAt",
  "lastSeenSlot" = excluded."lastSeenSlot",
  "lastSeenTxSignature" = excluded."lastSeenTxSignature",
  "collection_id" = COALESCE(
    "CollectionPointer"."collection_id",
    (
      SELECT COALESCE(MAX("collection_id"), 0) + 1
      FROM "CollectionPointer"
      WHERE "collection_id" IS NOT NULL
    )
  )
`;

function isMissingCollectionIdSchemaError(error: unknown): boolean {
  const maybe = error as { code?: unknown; message?: unknown } | null;
  const code = typeof maybe?.code === "string" ? maybe.code : "";
  const message = typeof maybe?.message === "string" ? maybe.message : String(error);
  const missingSchemaPattern = /column .*collection_id|no such column: collection_id|has no column named collection_id|column "?collection_id"? does not exist|Unknown arg .*collectionId/i;
  if (code === "P2022") {
    return /collection_id|collectionId|CollectionPointer/i.test(message);
  }
  if (code === "P2010") {
    return missingSchemaPattern.test(message);
  }
  return missingSchemaPattern.test(message);
}

function isCollectionIdUniqueConstraintError(error: unknown): boolean {
  const maybe = error as { code?: unknown; message?: unknown; meta?: unknown } | null;
  const code = typeof maybe?.code === "string" ? maybe.code : "";
  if (code !== "P2002") return false;

  const message = typeof maybe?.message === "string" ? maybe.message : "";
  if (/collection_id|collectionId/i.test(message)) return true;

  const meta = maybe?.meta as { target?: unknown } | undefined;
  const target = meta?.target;
  if (Array.isArray(target)) {
    return target.some((entry) => typeof entry === "string" && /collection_id|collectionId/i.test(entry));
  }
  if (typeof target === "string") {
    return /collection_id|collectionId/i.test(target);
  }
  return false;
}

async function tryDbSideCollectionIdUpsert(
  client: PrismaClientOrTx,
  pointer: string,
  creator: string,
  assetId: string,
  ctx: EventContext
): Promise<boolean> {
  const sqlClient = client as unknown as CollectionRawSqlClient;
  if (typeof sqlClient.$executeRawUnsafe !== "function") {
    return false;
  }

  await sqlClient.$executeRawUnsafe(
    COLLECTION_POINTER_DB_ALLOCATED_UPSERT_SQL,
    pointer,
    creator,
    assetId,
    ctx.blockTime,
    ctx.slot,
    ctx.signature,
    ctx.blockTime,
    ctx.slot,
    ctx.signature,
    0n
  );

  return true;
}

async function upsertCollectionPointerWithOptionalCollectionId(
  client: PrismaClientOrTx,
  collection: CollectionDelegate,
  pointer: string,
  creator: string,
  assetId: string,
  ctx: EventContext
): Promise<void> {
  const where = { col_creator: { col: pointer, creator } };
  const createBase = {
    col: pointer,
    creator,
    firstSeenAsset: assetId,
    firstSeenAt: ctx.blockTime,
    firstSeenSlot: ctx.slot,
    firstSeenTxSignature: ctx.signature,
    lastSeenAt: ctx.blockTime,
    lastSeenSlot: ctx.slot,
    lastSeenTxSignature: ctx.signature,
    assetCount: 0n,
  };
  const updateBase = {
    lastSeenAt: ctx.blockTime,
    lastSeenSlot: ctx.slot,
    lastSeenTxSignature: ctx.signature,
  };

  const canAttemptCollectionIdAssignment = true;

  if (canAttemptCollectionIdAssignment) {
    try {
      const usedDbAllocator = await tryDbSideCollectionIdUpsert(client, pointer, creator, assetId, ctx);
      if (usedDbAllocator) {
        return;
      }
    } catch (error) {
      if (isMissingCollectionIdSchemaError(error)) {
        throw new Error(
          "Missing collection_id schema in local database. Apply Prisma migrations (prisma migrate deploy) before starting the indexer."
        );
      } else {
        logger.warn(
          { pointer, creator, error: error instanceof Error ? error.message : String(error) },
          "db-side collection_id allocation failed; falling back to max-scan assignment"
        );
      }
    }
  }

  if (canAttemptCollectionIdAssignment) {
    for (let attempt = 0; attempt < COLLECTION_ID_ASSIGNMENT_MAX_RETRIES; attempt++) {
      try {
        const existingCollection = await collection.findUnique({
          where,
          select: { collectionId: true },
        });
        let assignedCollectionId: bigint | undefined;
        if (!existingCollection || existingCollection.collectionId === null) {
          const highestAssigned = await collection.findMany({
            where: { collectionId: { not: null } },
            select: { collectionId: true },
            orderBy: { collectionId: "desc" },
            take: 1,
          });
          assignedCollectionId = asBigInt(highestAssigned[0]?.collectionId) + 1n;
        }

        await collection.upsert({
          where,
          create: {
            ...createBase,
            collectionId: assignedCollectionId ?? null,
          },
          update: {
            ...updateBase,
            ...(assignedCollectionId !== undefined ? { collectionId: assignedCollectionId } : {}),
          },
        });
        return;
      } catch (error) {
        if (isMissingCollectionIdSchemaError(error)) {
          throw new Error(
            "Missing collection_id schema in local database. Apply Prisma migrations (prisma migrate deploy) before starting the indexer."
          );
        }

        const isLastAttempt = attempt >= COLLECTION_ID_ASSIGNMENT_MAX_RETRIES - 1;
        if (!isLastAttempt && isCollectionIdUniqueConstraintError(error)) {
          logger.warn(
            { pointer, creator, attempt: attempt + 1 },
            "collection_id assignment hit unique conflict; retrying assignment"
          );
          continue;
        }
        throw error;
      }
    }
  }

  await collection.upsert({
    where,
    create: createBase,
    update: updateBase,
  });
}

function enqueueUriDigestWorker(assetId: string): void {
  if (inFlightUriDigests.has(assetId)) return;
  const pending = pendingUriDigests.get(assetId);
  if (!pending) return;

  inFlightUriDigests.add(assetId);
  uriDigestQueue
    .add(async () => {
      try {
        while (true) {
          const next = pendingUriDigests.get(assetId);
          if (!next) break;
          pendingUriDigests.delete(assetId);

          try {
            await digestAndStoreUriMetadataLocal(next.prisma, assetId, next.uri, next.verifiedAt);
          } catch (err: any) {
            logger.warn({ assetId, uri: next.uri, error: err.message }, "Failed to digest URI metadata");
          }
        }
      } finally {
        inFlightUriDigests.delete(assetId);
        if (pendingUriDigests.has(assetId)) {
          enqueueUriDigestWorker(assetId);
        }
      }
    })
    .catch((err: any) => {
      inFlightUriDigests.delete(assetId);
      logger.error({ assetId, error: err.message }, "URI digest queue task failed");
      if (pendingUriDigests.has(assetId)) {
        enqueueUriDigestWorker(assetId);
      }
    });
}

function scheduleUriDigest(
  prisma: PrismaClient,
  assetId: string,
  uri: string,
  verifiedAt?: Date
): void {
  if (!uri || config.metadataIndexMode === "off") return;
  pendingUriDigests.set(assetId, { prisma, uri, verifiedAt });
  enqueueUriDigestWorker(assetId);
}

function enqueueCollectionDigestWorker(assetId: string): void {
  if (inFlightCollectionDigests.has(assetId)) return;
  const pending = pendingCollectionDigests.get(assetId);
  if (!pending) return;

  inFlightCollectionDigests.add(assetId);
  collectionDigestQueue
    .add(async () => {
      try {
        while (true) {
          const next = pendingCollectionDigests.get(assetId);
          if (!next) break;
          pendingCollectionDigests.delete(assetId);

          try {
            await digestAndStoreCollectionMetadataLocal(next.prisma, assetId, next.pointer);
          } catch (err: any) {
            logger.warn({ assetId, col: next.pointer, error: err.message }, "Failed to digest collection metadata");
          }
        }
      } finally {
        inFlightCollectionDigests.delete(assetId);
        if (pendingCollectionDigests.has(assetId)) {
          enqueueCollectionDigestWorker(assetId);
        }
      }
    })
    .catch((err: any) => {
      inFlightCollectionDigests.delete(assetId);
      logger.error({ assetId, error: err.message }, "Collection digest queue task failed");
      if (pendingCollectionDigests.has(assetId)) {
        enqueueCollectionDigestWorker(assetId);
      }
    });
}

function scheduleCollectionDigest(prisma: PrismaClient, assetId: string, pointer: string): void {
  if (!pointer || !config.collectionMetadataIndexEnabled) return;
  pendingCollectionDigests.set(assetId, { prisma, pointer });
  enqueueCollectionDigestWorker(assetId);
}

function decodeRawMetadataValue(value: Uint8Array | Buffer | null | undefined): string | null {
  if (!value || value.length === 0) return null;
  const buffer = Buffer.from(value);
  if (buffer[0] !== 0x00) return null;
  return buffer.slice(1).toString("utf8");
}

function shouldRetryUriRecovery(status: string | null | undefined): boolean {
  if (!status || status.length === 0) return true;
  if (status.includes('"status":"ok"')) return false;
  try {
    const parsed = JSON.parse(status) as { retryable?: unknown };
    if (parsed.retryable === false) return false;
  } catch {
    // Keep retrying legacy/non-JSON payloads for backward compatibility.
  }
  return true;
}

function startLocalDigestRecoveryLoop(prisma: PrismaClient): void {
  if (process.env.NODE_ENV === "test" || localRecoveryInterval) {
    return;
  }

  const tick = async () => {
    if (localRecoveryInFlight) return;
    localRecoveryInFlight = true;
    try {
      if (config.metadataIndexMode !== "off") {
        const agents = await prisma.agent.findMany({
          where: {
            uri: { not: "" },
          },
          select: {
            id: true,
            uri: true,
          },
          orderBy: { updatedAt: "desc" },
          take: LOCAL_RECOVERY_BATCH_SIZE,
        });

        if (agents.length > 0) {
          const metadataRows = await prisma.agentMetadata.findMany({
            where: {
              agentId: { in: agents.map((a) => a.id) },
              key: { in: ["_uri:_source", "_uri:_status"] },
            },
            select: {
              agentId: true,
              key: true,
              value: true,
            },
          });

          const sourceByAgent = new Map<string, string | null>();
          const statusByAgent = new Map<string, string | null>();
          for (const row of metadataRows) {
            const decoded = decodeRawMetadataValue(row.value);
            if (row.key === "_uri:_source") {
              sourceByAgent.set(row.agentId, decoded);
            } else if (row.key === "_uri:_status") {
              statusByAgent.set(row.agentId, decoded);
            }
          }

          for (const agent of agents) {
            const source = sourceByAgent.get(agent.id);
            const status = statusByAgent.get(agent.id);
            if (source !== agent.uri || shouldRetryUriRecovery(status)) {
              scheduleUriDigest(prisma, agent.id, agent.uri);
            }
          }
        }
      }

      if (config.collectionMetadataIndexEnabled) {
        const collections = await prisma.collection.findMany({
          select: {
            col: true,
            creator: true,
            metadataStatus: true,
            metadataUpdatedAt: true,
            lastSeenAt: true,
          },
          orderBy: { lastSeenAt: "desc" },
          take: LOCAL_RECOVERY_BATCH_SIZE,
        });

        for (const collection of collections) {
          const stale =
            collection.metadataStatus !== "ok" ||
            !collection.metadataUpdatedAt ||
            collection.metadataUpdatedAt.getTime() < collection.lastSeenAt.getTime();
          if (!stale) continue;

          const agent = await prisma.agent.findFirst({
            where: {
              collectionPointer: collection.col,
              creator: collection.creator,
            },
            select: { id: true },
          });
          if (agent) {
            scheduleCollectionDigest(prisma, agent.id, collection.col);
          }
        }
      }
    } catch (error: any) {
      logger.warn({ error: error.message }, "Local metadata recovery sweep failed");
    } finally {
      localRecoveryInFlight = false;
    }
  };

  void tick();
  localRecoveryInterval = setInterval(() => {
    void tick();
  }, LOCAL_RECOVERY_INTERVAL_MS);
}

// Default status for new records (will be verified later)
const DEFAULT_STATUS: ChainStatus = "PENDING";

// Type alias for Prisma transaction client
type PrismaTransactionClient = Omit<PrismaClient, "$connect" | "$disconnect" | "$on" | "$transaction" | "$use" | "$extends">;

// Union type for handlers that work with both PrismaClient and transaction client
type PrismaClientOrTx = PrismaClient | PrismaTransactionClient;

/**
 * Normalize hash: all-zero means "no hash" → NULL (parity with Supabase)
 */
function normalizeHash(hash: Uint8Array | number[]): Uint8Array<ArrayBuffer> | null {
  if (!hash) return null;
  const normalized = Uint8Array.from(hash) as Uint8Array<ArrayBuffer>;
  for (let i = 0; i < normalized.length; i++) {
    if (normalized[i] !== 0) return normalized;
  }
  return null;
}

/**
 * Compare two hash values for semantic matching (seal_hash vs feedback_hash)
 */
function isZeroHash(h: Uint8Array | Uint8Array<ArrayBuffer>): boolean {
  for (let i = 0; i < h.length; i++) {
    if (h[i] !== 0) return false;
  }
  return true;
}

function hashesMatch(a: Uint8Array | Uint8Array<ArrayBuffer> | null | undefined, b: Uint8Array | Uint8Array<ArrayBuffer> | null | undefined): boolean {
  // Treat null and all-zero as equivalent (handles old null data vs new preserved zeros)
  const aEmpty = !a || isZeroHash(a);
  const bEmpty = !b || isZeroHash(b);
  if (aEmpty && bEmpty) return true;
  if (aEmpty || bEmpty) return false;
  if (a!.length !== b!.length) return false;
  for (let i = 0; i < a!.length; i++) {
    if (a![i] !== b![i]) return false;
  }
  return true;
}

type OrphanFeedbackRecord = {
  id: string;
  agentId: string;
  client: string;
  feedbackIndex: bigint;
  value: string;
  valueDecimals: number;
  score: number | null;
  tag1: string;
  tag2: string;
  endpoint: string;
  feedbackUri: string;
  feedbackHash: Uint8Array<ArrayBuffer> | null;
  runningDigest: Uint8Array<ArrayBuffer> | null;
  atomEnabled: boolean;
  newTrustTier: number;
  newQualityScore: number;
  newConfidence: number;
  newRiskScore: number;
  newDiversityRatio: number;
  createdAt: Date;
  txSignature: string | null;
  slot: bigint | null;
  txIndex: number | null;
  eventOrdinal: number | null;
};

function denormalizeHash(hash: Uint8Array<ArrayBuffer> | null): Uint8Array<ArrayBuffer> {
  if (!hash || hash.length === 0) {
    return new Uint8Array(32) as Uint8Array<ArrayBuffer>;
  }
  return Uint8Array.from(hash) as Uint8Array<ArrayBuffer>;
}

async function upsertOrphanFeedback(
  client: PrismaClientOrTx,
  data: NewFeedback,
  ctx: EventContext
): Promise<void> {
  const orphanFeedback = (client as any).orphanFeedback as {
    upsert(args: unknown): Promise<unknown>;
  };
  const agentId = data.asset.toBase58();
  const clientAddress = data.clientAddress.toBase58();
  await orphanFeedback.upsert({
    where: {
      agentId_client_feedbackIndex: {
        agentId,
        client: clientAddress,
        feedbackIndex: data.feedbackIndex,
      },
    },
    create: {
      agentId,
      client: clientAddress,
      feedbackIndex: data.feedbackIndex,
      value: data.value.toString(),
      valueDecimals: data.valueDecimals,
      score: data.score,
      tag1: data.tag1,
      tag2: data.tag2,
      endpoint: data.endpoint,
      feedbackUri: data.feedbackUri,
      feedbackHash: normalizeHash(data.sealHash),
      runningDigest: Uint8Array.from(data.newFeedbackDigest) as Uint8Array<ArrayBuffer>,
      atomEnabled: data.atomEnabled,
      newTrustTier: data.newTrustTier,
      newQualityScore: data.newQualityScore,
      newConfidence: data.newConfidence,
      newRiskScore: data.newRiskScore,
      newDiversityRatio: data.newDiversityRatio,
      createdAt: ctx.blockTime,
      txSignature: ctx.signature,
      slot: ctx.slot,
      txIndex: ctx.txIndex ?? null,
      eventOrdinal: ctx.eventOrdinal ?? null,
    },
    update: {
      value: data.value.toString(),
      valueDecimals: data.valueDecimals,
      score: data.score,
      tag1: data.tag1,
      tag2: data.tag2,
      endpoint: data.endpoint,
      feedbackUri: data.feedbackUri,
      feedbackHash: normalizeHash(data.sealHash),
      runningDigest: Uint8Array.from(data.newFeedbackDigest) as Uint8Array<ArrayBuffer>,
      atomEnabled: data.atomEnabled,
      newTrustTier: data.newTrustTier,
      newQualityScore: data.newQualityScore,
      newConfidence: data.newConfidence,
      newRiskScore: data.newRiskScore,
      newDiversityRatio: data.newDiversityRatio,
      createdAt: ctx.blockTime,
      txSignature: ctx.signature,
      slot: ctx.slot,
      txIndex: ctx.txIndex ?? null,
      eventOrdinal: ctx.eventOrdinal ?? null,
    },
  });
}

function orphanFeedbackToEvent(record: OrphanFeedbackRecord): { data: NewFeedback; ctx: EventContext } {
  const slot = record.slot ?? 0n;
  const data: NewFeedback = {
    asset: new PublicKey(record.agentId),
    clientAddress: new PublicKey(record.client),
    feedbackIndex: record.feedbackIndex,
    value: BigInt(record.value),
    valueDecimals: record.valueDecimals,
    score: record.score,
    feedbackFileHash: null,
    sealHash: denormalizeHash(record.feedbackHash),
    atomEnabled: record.atomEnabled,
    newTrustTier: record.newTrustTier,
    newQualityScore: record.newQualityScore,
    newConfidence: record.newConfidence,
    newRiskScore: record.newRiskScore,
    newDiversityRatio: record.newDiversityRatio,
    isUniqueClient: false,
    newFeedbackDigest: record.runningDigest
      ? Uint8Array.from(record.runningDigest) as Uint8Array<ArrayBuffer>
      : (new Uint8Array(32) as Uint8Array<ArrayBuffer>),
    newFeedbackCount: 0n,
    tag1: record.tag1,
    tag2: record.tag2,
    endpoint: record.endpoint,
    feedbackUri: record.feedbackUri,
    slot,
  };

  const ctx: EventContext = {
    signature: record.txSignature || `orphan-feedback:${record.id}`,
    slot,
    blockTime: record.createdAt,
    txIndex: record.txIndex ?? undefined,
    eventOrdinal: record.eventOrdinal ?? undefined,
  };

  return { data, ctx };
}

function toRoundedScore(avg: number | null | undefined): number {
  if (avg === null || avg === undefined || Number.isNaN(avg)) return 0;
  return Math.round(avg);
}

function resolveDeterministicMetadataVerifiedAt(
  updatedAt: Date | null | undefined,
  createdAt: Date | null | undefined
): Date {
  return updatedAt ?? createdAt ?? new Date(0);
}

interface AgentAtomPatch {
  trustTier?: number;
  qualityScore?: number;
  confidence?: number;
  riskScore?: number;
  diversityRatio?: number;
}

async function syncAgentFeedbackStatsTx(
  tx: PrismaTransactionClient,
  assetId: string,
  blockTime: Date,
  atomPatch?: AgentAtomPatch
): Promise<void> {
  const aggregate = await tx.feedback.aggregate({
    where: {
      agentId: assetId,
      revoked: false,
    },
    _count: { _all: true },
    _avg: { score: true },
  });

  const feedbackCount = Number(aggregate?._count?._all ?? 0);
  const avgScore = aggregate?._avg?.score;
  const baseData = {
    feedbackCount,
    rawAvgScore: toRoundedScore(avgScore),
    updatedAt: blockTime,
  };

  await tx.agent.updateMany({
    where: { id: assetId },
    data: atomPatch ? { ...baseData, ...atomPatch } : baseData,
  });
}

async function syncAgentFeedbackStats(
  prisma: PrismaClient,
  assetId: string,
  blockTime: Date,
  atomPatch?: AgentAtomPatch
): Promise<void> {
  const aggregate = await prisma.feedback.aggregate({
    where: {
      agentId: assetId,
      revoked: false,
    },
    _count: { _all: true },
    _avg: { score: true },
  });

  const feedbackCount = Number(aggregate?._count?._all ?? 0);
  const avgScore = aggregate?._avg?.score;
  const baseData = {
    feedbackCount,
    rawAvgScore: toRoundedScore(avgScore),
    updatedAt: blockTime,
  };

  await prisma.agent.updateMany({
    where: { id: assetId },
    data: atomPatch ? { ...baseData, ...atomPatch } : baseData,
  });
}

export interface EventContext {
  signature: string;
  slot: bigint;
  blockTime: Date;
  txIndex?: number; // Transaction index within the block (metadata / tertiary tie-breaker)
  eventOrdinal?: number; // Event index within the transaction (deterministic intra-tx ordering)
  source?: "poller" | "websocket" | "substreams"; // Event source for cursor tracking
}

/**
 * Atomic event handler - wraps event processing and cursor update in a single transaction
 * This ensures crash/reorg resilience: either both succeed or both fail
 */
export async function handleEventAtomic(
  prisma: PrismaClient | null,
  event: ProgramEvent,
  ctx: EventContext
): Promise<void> {
  // Route to Supabase handlers if in supabase mode
  if (config.dbMode === "supabase") {
    return supabaseHandlers.handleEventAtomic(event, ctx);
  }

  // Local mode - use Prisma transaction (prisma must be non-null in local mode)
  if (!prisma) {
    throw new Error("Prisma client required in local mode");
  }

  startLocalDigestRecoveryLoop(prisma);

  for (let attempt = 0; attempt < ATOMIC_COLLECTION_ID_TX_MAX_RETRIES; attempt++) {
    try {
      await prisma.$transaction(async (tx) => {
        // 1. Handle event
        await handleEventInner(tx, event, ctx);

        // 2. Update cursor atomically with monotonic guard
        await updateCursorAtomic(tx, ctx);
      });
      break;
    } catch (error) {
      const canRetry =
        attempt < ATOMIC_COLLECTION_ID_TX_MAX_RETRIES - 1
        && isCollectionIdUniqueConstraintError(error);
      if (canRetry) {
        logger.warn(
          { attempt: attempt + 1, signature: ctx.signature, slot: ctx.slot.toString() },
          "Atomic transaction hit collection_id unique conflict; retrying"
        );
        continue;
      }
      throw error;
    }
  }

  // 3. Trigger derived metadata extraction AFTER transaction (fire-and-forget)
  // This is outside the transaction to avoid blocking event processing
  await triggerDerivedDigestsIfNeeded(prisma, event, ctx);
}

/**
 * Update indexer cursor with monotonic guard
 * Advances only when:
 * - new slot is greater than current slot, or
 * - same slot and new tx_index/signature are >= current cursor order
 */
async function updateCursorAtomic(
  tx: PrismaTransactionClient,
  ctx: EventContext
): Promise<void> {
  const current = await tx.indexerState.findUnique({
    where: { id: "main" },
    select: { lastSlot: true, lastSignature: true, lastTxIndex: true },
  });

  // Monotonic guard:
  // - reject backward slot movement
  // - for same slot, reject lexicographically older signatures
  if (
    current
    && current.lastSlot !== null
    && (
      ctx.slot < current.lastSlot
      || (
        ctx.slot === current.lastSlot
        && (
          (current.lastTxIndex ?? Number.MAX_SAFE_INTEGER) > (ctx.txIndex ?? Number.MAX_SAFE_INTEGER)
          || (
            (current.lastTxIndex ?? Number.MAX_SAFE_INTEGER) === (ctx.txIndex ?? Number.MAX_SAFE_INTEGER)
            && current.lastSignature !== null
            && ctx.signature < current.lastSignature
          )
        )
      )
    )
  ) {
    return;
  }

  await tx.indexerState.upsert({
    where: { id: "main" },
    create: {
      id: "main",
      lastSignature: ctx.signature,
      lastSlot: ctx.slot,
      lastTxIndex: ctx.txIndex ?? null,
      source: ctx.source || "poller",
    },
    update: {
      lastSignature: ctx.signature,
      lastSlot: ctx.slot,
      lastTxIndex: ctx.txIndex ?? null,
      source: ctx.source || "poller",
    },
  });
}

/**
 * Trigger derived metadata extraction for events that contain URIs/collections
 * Called AFTER atomic transaction completes (fire-and-forget via queue)
 */
async function triggerDerivedDigestsIfNeeded(
  prisma: PrismaClient,
  event: ProgramEvent,
  ctx: EventContext
): Promise<void> {
  let uriAssetId: string | null = null;
  let uri: string | null = null;

  if (event.type === "AgentRegistered") {
    uriAssetId = event.data.asset.toBase58();
    uri = event.data.agentUri || null;
  } else if (event.type === "UriUpdated") {
    uriAssetId = event.data.asset.toBase58();
    uri = event.data.newUri || null;
  }

  if (uriAssetId && uri && config.metadataIndexMode !== "off") {
    scheduleUriDigest(prisma, uriAssetId, uri, ctx.blockTime);
  }

  if (event.type === "CollectionPointerSet" && config.collectionMetadataIndexEnabled) {
    const assetId = event.data.asset.toBase58();
    const pointer = event.data.col;
    scheduleCollectionDigest(prisma, assetId, pointer);
  }
}

/**
 * Inner event handler - runs inside transaction
 */
async function handleEventInner(
  tx: PrismaTransactionClient,
  event: ProgramEvent,
  ctx: EventContext
): Promise<void> {
  switch (event.type) {
    case "AgentRegistered":
      await handleAgentRegisteredTx(tx, event.data, ctx);
      break;
    case "AgentOwnerSynced":
      await handleAgentOwnerSyncedTx(tx, event.data, ctx);
      break;
    case "AtomEnabled":
      await handleAtomEnabledTx(tx, event.data, ctx);
      break;
    case "UriUpdated":
      await handleUriUpdatedTx(tx, event.data, ctx);
      break;
    case "WalletUpdated":
      await handleWalletUpdatedTx(tx, event.data, ctx);
      break;
    case "WalletResetOnOwnerSync":
      await handleWalletResetOnOwnerSyncTx(tx, event.data, ctx);
      break;
    case "MetadataSet":
      await handleMetadataSetTx(tx, event.data, ctx);
      break;
    case "MetadataDeleted":
      await handleMetadataDeletedTx(tx, event.data, ctx);
      break;
    case "RegistryInitialized":
      await handleRegistryInitializedTx(tx, event.data, ctx);
      break;
    case "CollectionPointerSet":
      await handleCollectionPointerSetTx(tx, event.data, ctx);
      break;
    case "ParentAssetSet":
      await handleParentAssetSetTx(tx, event.data, ctx);
      break;
    case "NewFeedback":
      await handleNewFeedbackTx(tx, event.data, ctx);
      break;
    case "FeedbackRevoked":
      await handleFeedbackRevokedTx(tx, event.data, ctx);
      break;
    case "ResponseAppended":
      await handleResponseAppendedTx(tx, event.data, ctx);
      break;
    case "ValidationRequested":
      if (config.validationIndexEnabled) {
        await handleValidationRequestedTx(tx, event.data, ctx);
      }
      break;
    case "ValidationResponded":
      if (config.validationIndexEnabled) {
        await handleValidationRespondedTx(tx, event.data, ctx);
      }
      break;
    default:
      logger.warn({ event }, "Unhandled event type");
  }
}

/**
 * @deprecated Use handleEventAtomic instead. This non-atomic handler does not
 * wrap event processing + cursor update in a single transaction, which means
 * a crash between the two operations can leave the indexer in an inconsistent state.
 */
export async function handleEvent(
  prisma: PrismaClient | null,
  event: ProgramEvent,
  ctx: EventContext
): Promise<void> {
  // Route to Supabase handlers if in supabase mode
  if (config.dbMode === "supabase") {
    return supabaseHandlers.handleEvent(event, ctx);
  }

  // Local mode - use Prisma/SQLite
  if (!prisma) {
    throw new Error("PrismaClient required for local mode");
  }

  startLocalDigestRecoveryLoop(prisma);

  switch (event.type) {
    case "AgentRegistered":
      await handleAgentRegistered(prisma, event.data, ctx);
      break;

    case "AgentOwnerSynced":
      await handleAgentOwnerSynced(prisma, event.data, ctx);
      break;

    case "AtomEnabled":
      await handleAtomEnabled(prisma, event.data, ctx);
      break;

    case "UriUpdated":
      await handleUriUpdated(prisma, event.data, ctx);
      break;

    case "WalletUpdated":
      await handleWalletUpdated(prisma, event.data, ctx);
      break;
    case "WalletResetOnOwnerSync":
      await handleWalletResetOnOwnerSync(prisma, event.data, ctx);
      break;

    case "MetadataSet":
      await handleMetadataSet(prisma, event.data, ctx);
      break;

    case "MetadataDeleted":
      await handleMetadataDeleted(prisma, event.data, ctx);
      break;

    case "RegistryInitialized":
      await handleRegistryInitialized(prisma, event.data, ctx);
      break;
    case "CollectionPointerSet":
      await handleCollectionPointerSet(prisma, event.data, ctx);
      break;
    case "ParentAssetSet":
      await handleParentAssetSet(prisma, event.data, ctx);
      break;

    case "NewFeedback":
      await handleNewFeedback(prisma, event.data, ctx);
      break;

    case "FeedbackRevoked":
      await handleFeedbackRevoked(prisma, event.data, ctx);
      break;

    case "ResponseAppended":
      await handleResponseAppended(prisma, event.data, ctx);
      break;

    case "ValidationRequested":
      if (config.validationIndexEnabled) {
        await handleValidationRequested(prisma, event.data, ctx);
      }
      break;

    case "ValidationResponded":
      if (config.validationIndexEnabled) {
        await handleValidationResponded(prisma, event.data, ctx);
      }
      break;

    default:
      logger.warn({ event }, "Unhandled event type");
  }
}

// Core handler that works with both PrismaClient and transaction client
// v0.6.0: AgentRegistered event no longer has registry field (single-collection)
async function handleAgentRegisteredCore(
  client: PrismaClientOrTx,
  data: AgentRegistered,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const agentUri = data.agentUri || "";
  const collectionId = data.collection.toBase58();
  await withAgentIdAssignmentLock(async () => {
    const existingAgent = await client.agent.findUnique({
      where: { id: assetId },
      select: { agentId: true, creator: true },
    });
    const ownerBase58 = data.owner.toBase58();
    const canonicalCreator = existingAgent
      ? (() => {
          if (!existingAgent.creator) {
            throw new Error(`Invariant violation: missing creator for existing agent ${assetId}`);
          }
          return existingAgent.creator;
        })()
      : ownerBase58;

    let assignedAgentId: bigint | undefined;
    if (!existingAgent || existingAgent.agentId === null) {
      const highestAssigned = await client.agent.findMany({
        where: { agentId: { not: null } },
        select: { agentId: true },
        orderBy: { agentId: "desc" },
        take: 1,
      });
      const maxAssigned = highestAssigned[0]?.agentId;
      assignedAgentId = asBigInt(maxAssigned) + 1n;
    }

    await client.agent.upsert({
      where: { id: assetId },
      create: {
        id: assetId,
        owner: data.owner.toBase58(),
        creator: canonicalCreator,
        uri: agentUri,
        nftName: "",
        collection: collectionId,
        collectionPointer: "",
        colLocked: false,
        parentLocked: false,
        registry: collectionId, // v0.6.0: registry = collection (single-collection arch)
        atomEnabled: data.atomEnabled,
        createdTxSignature: ctx.signature,
        createdSlot: ctx.slot,
        txIndex: ctx.txIndex ?? null,
        eventOrdinal: ctx.eventOrdinal ?? null,
        agentId: assignedAgentId ?? null,
        status: DEFAULT_STATUS,
        createdAt: ctx.blockTime,
        updatedAt: ctx.blockTime,
      },
      update: {
        collection: collectionId,
        registry: collectionId, // v0.6.0: registry = collection
        atomEnabled: data.atomEnabled,
        uri: agentUri,
        creator: canonicalCreator,
        txIndex: ctx.txIndex ?? null,
        eventOrdinal: ctx.eventOrdinal ?? null,
        updatedAt: ctx.blockTime,
        ...(assignedAgentId !== undefined ? { agentId: assignedAgentId } : {}),
      },
    });
  });

  logger.info({ assetId, owner: data.owner.toBase58(), uri: agentUri }, "Agent registered");
}

async function reconcileOrphanFeedbacksTx(
  tx: PrismaTransactionClient,
  assetId: string
): Promise<void> {
  const orphans = await (tx as any).orphanFeedback.findMany({
    where: { agentId: assetId },
    orderBy: [
      { slot: "asc" },
      { txIndex: "asc" },
      { eventOrdinal: "asc" },
      { createdAt: "asc" },
      { id: "asc" },
    ],
  }) as OrphanFeedbackRecord[];
  const orphanFeedback = (tx as any).orphanFeedback as {
    delete(args: unknown): Promise<unknown>;
  };

  for (const orphan of orphans) {
    const replay = orphanFeedbackToEvent(orphan);
    await handleNewFeedbackTx(tx, replay.data, replay.ctx);
    await orphanFeedback.delete({ where: { id: orphan.id } });
  }

  if (orphans.length > 0) {
    logger.info({ assetId, count: orphans.length }, "Reconciled orphan feedbacks");
  }
}

async function reconcileOrphanFeedbacks(
  prisma: PrismaClient,
  assetId: string
): Promise<void> {
  const orphans = await (prisma as any).orphanFeedback.findMany({
    where: { agentId: assetId },
    orderBy: [
      { slot: "asc" },
      { txIndex: "asc" },
      { eventOrdinal: "asc" },
      { createdAt: "asc" },
      { id: "asc" },
    ],
  }) as OrphanFeedbackRecord[];
  const orphanFeedback = (prisma as any).orphanFeedback as {
    delete(args: unknown): Promise<unknown>;
  };

  for (const orphan of orphans) {
    const replay = orphanFeedbackToEvent(orphan);
    await handleNewFeedback(prisma, replay.data, replay.ctx);
    await orphanFeedback.delete({ where: { id: orphan.id } });
  }

  if (orphans.length > 0) {
    logger.info({ assetId, count: orphans.length }, "Reconciled orphan feedbacks");
  }
}

async function handleAgentRegisteredTx(
  tx: PrismaTransactionClient,
  data: AgentRegistered,
  ctx: EventContext
): Promise<void> {
  await handleAgentRegisteredCore(tx, data, ctx);
  const assetId = data.asset.toBase58();
  await reconcileOrphanFeedbacksTx(tx, assetId);
}

// Non-atomic wrapper with URI digest side effect
async function handleAgentRegistered(
  prisma: PrismaClient,
  data: AgentRegistered,
  ctx: EventContext
): Promise<void> {
  await handleAgentRegisteredCore(prisma, data, ctx);
  const assetId = data.asset.toBase58();
  await reconcileOrphanFeedbacks(prisma, assetId);
  const agentUri = data.agentUri || "";

  // Trigger URI metadata extraction if configured and URI is present
  if (agentUri && config.metadataIndexMode !== "off") {
    scheduleUriDigest(prisma, assetId, agentUri, ctx.blockTime);
  }
}

async function handleAgentOwnerSyncedTx(
  tx: PrismaTransactionClient,
  data: AgentOwnerSynced,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const result = await tx.agent.updateMany({
    where: { id: assetId },
    data: { owner: data.newOwner.toBase58(), updatedAt: ctx.blockTime },
  });
  if (result.count === 0) {
    logger.warn({ assetId }, "Agent not found for owner sync, event may be out of order");
    return;
  }
  logger.info({ assetId, oldOwner: data.oldOwner.toBase58(), newOwner: data.newOwner.toBase58() }, "Agent owner synced");
}

async function handleAgentOwnerSynced(
  prisma: PrismaClient,
  data: AgentOwnerSynced,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();

  // Use updateMany to avoid P2025 error if agent doesn't exist yet (out-of-order events)
  const result = await prisma.agent.updateMany({
    where: { id: assetId },
    data: {
      owner: data.newOwner.toBase58(),
      updatedAt: ctx.blockTime,
    },
  });

  if (result.count === 0) {
    logger.warn({ assetId }, "Agent not found for owner sync, event may be out of order");
    return;
  }

  logger.info(
    {
      assetId,
      oldOwner: data.oldOwner.toBase58(),
      newOwner: data.newOwner.toBase58(),
    },
    "Agent owner synced"
  );
}

async function handleAtomEnabledTx(
  tx: PrismaTransactionClient,
  data: AtomEnabled,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const result = await tx.agent.updateMany({
    where: { id: assetId },
    data: { atomEnabled: true, updatedAt: ctx.blockTime },
  });
  if (result.count === 0) {
    logger.warn({ assetId }, "Agent not found for ATOM enable, event may be out of order");
    return;
  }
  logger.info({ assetId, enabledBy: data.enabledBy.toBase58() }, "ATOM enabled");
}

async function handleAtomEnabled(
  prisma: PrismaClient,
  data: AtomEnabled,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();

  // Use updateMany to avoid P2025 error if agent doesn't exist yet (out-of-order events)
  const result = await prisma.agent.updateMany({
    where: { id: assetId },
    data: {
      atomEnabled: true,
      updatedAt: ctx.blockTime,
    },
  });

  if (result.count === 0) {
    logger.warn({ assetId }, "Agent not found for ATOM enable, event may be out of order");
    return;
  }

  logger.info({ assetId, enabledBy: data.enabledBy.toBase58() }, "ATOM enabled");
}

async function handleUriUpdatedTx(
  tx: PrismaTransactionClient,
  data: UriUpdated,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const newUri = data.newUri || "";
  const result = await tx.agent.updateMany({
    where: { id: assetId },
    data: { uri: newUri, updatedAt: ctx.blockTime },
  });
  if (result.count === 0) {
    logger.warn({ assetId }, "Agent not found for URI update, event may be out of order");
    return;
  }
  logger.info({ assetId, newUri }, "Agent URI updated");
}

async function handleUriUpdated(
  prisma: PrismaClient,
  data: UriUpdated,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const newUri = data.newUri || "";

  // Use updateMany to avoid P2025 error if agent doesn't exist yet (out-of-order events)
  const result = await prisma.agent.updateMany({
    where: { id: assetId },
    data: {
      uri: newUri,
      updatedAt: ctx.blockTime,
    },
  });

  if (result.count === 0) {
    logger.warn({ assetId }, "Agent not found for URI update, event may be out of order");
    return;
  }

  logger.info({ assetId, newUri }, "Agent URI updated");

  // Trigger URI metadata extraction if configured and URI is present
  if (newUri && config.metadataIndexMode !== "off") {
    scheduleUriDigest(prisma, assetId, newUri, ctx.blockTime);
  }
}

async function handleWalletUpdatedTx(
  tx: PrismaTransactionClient,
  data: WalletUpdated,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const newWalletRaw = data.newWallet.toBase58();
  const newWallet = newWalletRaw === DEFAULT_PUBKEY ? null : newWalletRaw;
  const result = await tx.agent.updateMany({
    where: { id: assetId },
    data: { wallet: newWallet, updatedAt: ctx.blockTime },
  });
  if (result.count === 0) {
    logger.warn({ assetId }, "Agent not found for wallet update, event may be out of order");
    return;
  }
  logger.info({ assetId, newWallet: newWallet ?? "(reset)" }, "Agent wallet updated");
}

async function handleWalletUpdated(
  prisma: PrismaClient,
  data: WalletUpdated,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  // Convert default pubkey to NULL (wallet reset semantics)
  const newWalletRaw = data.newWallet.toBase58();
  const newWallet = newWalletRaw === DEFAULT_PUBKEY ? null : newWalletRaw;

  // Use updateMany to avoid P2025 error if agent doesn't exist yet (out-of-order events)
  const result = await prisma.agent.updateMany({
    where: { id: assetId },
    data: {
      wallet: newWallet,
      updatedAt: ctx.blockTime,
    },
  });

  if (result.count === 0) {
    logger.warn({ assetId }, "Agent not found for wallet update, event may be out of order");
    return;
  }

  logger.info(
    { assetId, newWallet: newWallet ?? "(reset)" },
    "Agent wallet updated"
  );
}

async function handleWalletResetOnOwnerSyncTx(
  tx: PrismaTransactionClient,
  data: WalletResetOnOwnerSync,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const newWalletRaw = data.newWallet.toBase58();
  const newWallet = newWalletRaw === DEFAULT_PUBKEY ? null : newWalletRaw;
  const ownerAfterSync = data.ownerAfterSync.toBase58();

  const result = await tx.agent.updateMany({
    where: { id: assetId },
    data: {
      owner: ownerAfterSync,
      wallet: newWallet,
      updatedAt: ctx.blockTime,
    },
  });

  if (result.count === 0) {
    logger.warn({ assetId }, "Agent not found for wallet reset on owner sync, event may be out of order");
    return;
  }

  logger.info({ assetId, ownerAfterSync, newWallet: newWallet ?? "(reset)" }, "Wallet reset on owner sync");
}

async function handleWalletResetOnOwnerSync(
  prisma: PrismaClient,
  data: WalletResetOnOwnerSync,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const newWalletRaw = data.newWallet.toBase58();
  const newWallet = newWalletRaw === DEFAULT_PUBKEY ? null : newWalletRaw;
  const ownerAfterSync = data.ownerAfterSync.toBase58();

  const result = await prisma.agent.updateMany({
    where: { id: assetId },
    data: {
      owner: ownerAfterSync,
      wallet: newWallet,
      updatedAt: ctx.blockTime,
    },
  });

  if (result.count === 0) {
    logger.warn({ assetId }, "Agent not found for wallet reset on owner sync, event may be out of order");
    return;
  }

  logger.info({ assetId, ownerAfterSync, newWallet: newWallet ?? "(reset)" }, "Wallet reset on owner sync");
}

async function handleCollectionPointerSetTx(
  tx: PrismaTransactionClient,
  data: CollectionPointerSet,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const pointer = data.col;
  const setBy = data.setBy.toBase58();
  const existing = await tx.agent.findUnique({
    where: { id: assetId },
    select: { collectionPointer: true, creator: true },
  });
  const creator = existing?.creator ?? setBy;

  await withCollectionIdAssignmentLock(async () => {
    await upsertCollectionPointerWithOptionalCollectionId(
      tx,
      tx.collection as unknown as CollectionDelegate,
      pointer,
      creator,
      assetId,
      ctx
    );
  });

  const result = await tx.agent.updateMany({
    where: { id: assetId },
    data: {
      collectionPointer: pointer,
      creator,
      ...(typeof data.lock === "boolean" ? { colLocked: data.lock } : {}),
      updatedAt: ctx.blockTime,
    },
  });

  if (result.count > 0) {
    const previousPointer = existing?.collectionPointer ?? "";
    const previousCreator = existing?.creator ?? creator;
    if (previousPointer !== pointer && previousPointer !== "") {
      const previousCount = await tx.agent.count({
        where: {
          collectionPointer: previousPointer,
          creator: previousCreator,
        },
      });
      await tx.collection.updateMany({
        where: { col: previousPointer, creator: previousCreator },
        data: { assetCount: BigInt(previousCount) },
      });
    }

    const currentCount = await tx.agent.count({
      where: {
        collectionPointer: pointer,
        creator,
      },
    });
    await tx.collection.update({
      where: { col_creator: { col: pointer, creator } },
      data: { assetCount: BigInt(currentCount) },
    });
  } else {
    logger.warn({ assetId }, "Agent not found for collection pointer set, event may be out of order");
  }

  logger.info({ assetId, col: pointer, setBy }, "Collection pointer set");
}

async function handleCollectionPointerSet(
  prisma: PrismaClient,
  data: CollectionPointerSet,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const pointer = data.col;
  const setBy = data.setBy.toBase58();
  const existing = await prisma.agent.findUnique({
    where: { id: assetId },
    select: { collectionPointer: true, creator: true },
  });
  const creator = existing?.creator ?? setBy;

  await withCollectionIdAssignmentLock(async () => {
    await upsertCollectionPointerWithOptionalCollectionId(
      prisma,
      prisma.collection as unknown as CollectionDelegate,
      pointer,
      creator,
      assetId,
      ctx
    );
  });

  const result = await prisma.agent.updateMany({
    where: { id: assetId },
    data: {
      collectionPointer: pointer,
      creator,
      ...(typeof data.lock === "boolean" ? { colLocked: data.lock } : {}),
      updatedAt: ctx.blockTime,
    },
  });

  if (result.count > 0) {
    const previousPointer = existing?.collectionPointer ?? "";
    const previousCreator = existing?.creator ?? creator;
    if (previousPointer !== pointer && previousPointer !== "") {
      const previousCount = await prisma.agent.count({
        where: {
          collectionPointer: previousPointer,
          creator: previousCreator,
        },
      });
      await prisma.collection.updateMany({
        where: { col: previousPointer, creator: previousCreator },
        data: { assetCount: BigInt(previousCount) },
      });
    }

    const currentCount = await prisma.agent.count({
      where: {
        collectionPointer: pointer,
        creator,
      },
    });
    await prisma.collection.update({
      where: { col_creator: { col: pointer, creator } },
      data: { assetCount: BigInt(currentCount) },
    });
  } else {
    logger.warn({ assetId }, "Agent not found for collection pointer set, event may be out of order");
  }

  logger.info({ assetId, col: pointer, setBy }, "Collection pointer set");

  if (config.collectionMetadataIndexEnabled) {
    scheduleCollectionDigest(prisma, assetId, pointer);
  }
}

async function handleParentAssetSetTx(
  tx: PrismaTransactionClient,
  data: ParentAssetSet,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const result = await tx.agent.updateMany({
    where: { id: assetId },
    data: {
      parentAsset: data.parentAsset.toBase58(),
      parentCreator: data.parentCreator.toBase58(),
      ...(typeof data.lock === "boolean" ? { parentLocked: data.lock } : {}),
      updatedAt: ctx.blockTime,
    },
  });

  if (result.count === 0) {
    logger.warn({ assetId }, "Agent not found for parent asset set, event may be out of order");
    return;
  }

  logger.info({
    assetId,
    parentAsset: data.parentAsset.toBase58(),
    parentCreator: data.parentCreator.toBase58(),
    setBy: data.setBy.toBase58(),
  }, "Parent asset set");
}

async function handleParentAssetSet(
  prisma: PrismaClient,
  data: ParentAssetSet,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const result = await prisma.agent.updateMany({
    where: { id: assetId },
    data: {
      parentAsset: data.parentAsset.toBase58(),
      parentCreator: data.parentCreator.toBase58(),
      ...(typeof data.lock === "boolean" ? { parentLocked: data.lock } : {}),
      updatedAt: ctx.blockTime,
    },
  });

  if (result.count === 0) {
    logger.warn({ assetId }, "Agent not found for parent asset set, event may be out of order");
    return;
  }

  logger.info({
    assetId,
    parentAsset: data.parentAsset.toBase58(),
    parentCreator: data.parentCreator.toBase58(),
    setBy: data.setBy.toBase58(),
  }, "Parent asset set");
}

async function handleMetadataSetTx(
  tx: PrismaTransactionClient,
  data: MetadataSet,
  ctx: EventContext
): Promise<void> {
  if (data.key.startsWith("_uri:")) {
    logger.warn({ assetId: data.asset.toBase58(), key: data.key }, "Skipping reserved _uri: prefix");
    return;
  }
  const assetId = data.asset.toBase58();
  const cleanValue = stripNullBytes(data.value);
  const prefixedValue = Buffer.concat([Buffer.from([0x00]), cleanValue]);
  const existing = await tx.agentMetadata.findUnique({
    where: { agentId_key: { agentId: assetId, key: data.key } },
    select: { immutable: true },
  });
  if (existing?.immutable) {
    logger.debug({ assetId, key: data.key }, "Skipping update: metadata is immutable");
    return;
  }
  await tx.agentMetadata.upsert({
    where: { agentId_key: { agentId: assetId, key: data.key } },
    create: {
      agentId: assetId,
      key: data.key,
      value: prefixedValue,
      immutable: data.immutable,
      txSignature: ctx.signature,
      slot: ctx.slot,
      txIndex: ctx.txIndex ?? null,
      eventOrdinal: ctx.eventOrdinal ?? null,
      status: DEFAULT_STATUS,
    },
    update: {
      value: prefixedValue,
      immutable: data.immutable,
      txSignature: ctx.signature,
      slot: ctx.slot,
      txIndex: ctx.txIndex ?? null,
      eventOrdinal: ctx.eventOrdinal ?? null,
    },
  });
  logger.info({ assetId, key: data.key }, "Metadata set");
}

async function handleMetadataSet(
  prisma: PrismaClient,
  data: MetadataSet,
  ctx: EventContext
): Promise<void> {
  // Skip _uri: prefix (reserved for indexer-derived metadata)
  if (data.key.startsWith("_uri:")) {
    logger.warn({ assetId: data.asset.toBase58(), key: data.key }, "Skipping reserved _uri: prefix");
    return;
  }

  const assetId = data.asset.toBase58();

  // Strip NULL bytes that break PostgreSQL UTF-8 encoding, then add PREFIX_RAW (0x00)
  const cleanValue = stripNullBytes(data.value);
  const prefixedValue = Buffer.concat([Buffer.from([0x00]), cleanValue]);

  const existing = await prisma.agentMetadata.findUnique({
    where: { agentId_key: { agentId: assetId, key: data.key } },
    select: { immutable: true },
  });
  if (existing?.immutable) {
    logger.debug({ assetId, key: data.key }, "Skipping update: metadata is immutable");
    return;
  }

  await prisma.agentMetadata.upsert({
    where: {
      agentId_key: {
        agentId: assetId,
        key: data.key,
      },
    },
    create: {
      agentId: assetId,
      key: data.key,
      value: prefixedValue,
      immutable: data.immutable,
      txSignature: ctx.signature,
      slot: ctx.slot,
      txIndex: ctx.txIndex ?? null,
      eventOrdinal: ctx.eventOrdinal ?? null,
      status: DEFAULT_STATUS,
    },
    update: {
      value: prefixedValue,
      immutable: data.immutable,
      txSignature: ctx.signature,
      slot: ctx.slot,
      txIndex: ctx.txIndex ?? null,
      eventOrdinal: ctx.eventOrdinal ?? null,
    },
  });

  logger.info({ assetId, key: data.key }, "Metadata set");
}

async function handleMetadataDeletedTx(
  tx: PrismaTransactionClient,
  data: MetadataDeleted,
  _ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  await tx.agentMetadata.deleteMany({
    where: { agentId: assetId, key: data.key },
  });
  logger.info({ assetId, key: data.key }, "Metadata deleted");
}

async function handleMetadataDeleted(
  prisma: PrismaClient,
  data: MetadataDeleted,
  _ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();

  await prisma.agentMetadata.deleteMany({
    where: {
      agentId: assetId,
      key: data.key,
    },
  });

  logger.info({ assetId, key: data.key }, "Metadata deleted");
}

// v0.6.0: RegistryInitialized replaces BaseRegistryCreated/UserRegistryCreated
async function handleRegistryInitializedTx(
  tx: PrismaTransactionClient,
  data: RegistryInitialized,
  ctx: EventContext
): Promise<void> {
  const collectionId = data.collection.toBase58();
  await tx.registry.upsert({
    where: { id: collectionId },
    create: {
      id: collectionId,
      collection: collectionId,
      registryType: "Base", // v0.6.0: single-collection, always base
      authority: data.authority.toBase58(),
      txSignature: ctx.signature,
      slot: ctx.slot,
      status: DEFAULT_STATUS,
      createdAt: ctx.blockTime,
    },
    update: {},
  });
  logger.info({ collection: collectionId }, "Registry initialized");
}

async function handleRegistryInitialized(
  prisma: PrismaClient,
  data: RegistryInitialized,
  ctx: EventContext
): Promise<void> {
  const collectionId = data.collection.toBase58();
  await prisma.registry.upsert({
    where: { id: collectionId },
    create: {
      id: collectionId,
      collection: collectionId,
      registryType: "Base", // v0.6.0: single-collection, always base
      authority: data.authority.toBase58(),
      txSignature: ctx.signature,
      slot: ctx.slot,
      status: DEFAULT_STATUS,
      createdAt: ctx.blockTime,
    },
    update: {},
  });

  logger.info({ collection: collectionId }, "Registry initialized");
}

async function handleNewFeedbackTx(
  tx: PrismaTransactionClient,
  data: NewFeedback,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const clientAddress = data.clientAddress.toBase58();
  const agent = await tx.agent.findUnique({
    where: { id: assetId },
    select: { id: true },
  });
  if (agent === null) {
    await upsertOrphanFeedback(tx, data, ctx);
    logger.warn(
      { assetId, clientAddress, feedbackIndex: data.feedbackIndex.toString() },
      "Agent missing for feedback; stored orphan feedback"
    );
    return;
  }
  const feedback = await withFeedbackIdAssignmentLock(async () => {
    const existingFeedback = await tx.feedback.findUnique({
      where: {
        agentId_client_feedbackIndex: {
          agentId: assetId,
          client: clientAddress,
          feedbackIndex: data.feedbackIndex,
        },
      },
      select: { feedbackId: true },
    });

    let assignedFeedbackId: bigint | undefined;
    if (!existingFeedback || existingFeedback.feedbackId === null) {
      const highestAssigned = await tx.feedback.findMany({
        where: { agentId: assetId, feedbackId: { not: null } },
        select: { feedbackId: true },
        orderBy: { feedbackId: "desc" },
        take: 1,
      });
      assignedFeedbackId = asBigInt(highestAssigned[0]?.feedbackId) + 1n;
    }

    return tx.feedback.upsert({
      where: {
        agentId_client_feedbackIndex: {
          agentId: assetId,
          client: clientAddress,
          feedbackIndex: data.feedbackIndex,
        },
      },
      create: {
        agentId: assetId,
        client: clientAddress,
        feedbackIndex: data.feedbackIndex,
        value: data.value.toString(),
        valueDecimals: data.valueDecimals,
        score: data.score,
        tag1: data.tag1,
        tag2: data.tag2,
        endpoint: data.endpoint,
        feedbackUri: data.feedbackUri,
        feedbackHash: normalizeHash(data.sealHash),
        runningDigest: Uint8Array.from(data.newFeedbackDigest) as Uint8Array<ArrayBuffer>,
        createdTxSignature: ctx.signature,
        createdSlot: ctx.slot,
        txIndex: ctx.txIndex ?? null,
        eventOrdinal: ctx.eventOrdinal ?? null,
        feedbackId: assignedFeedbackId ?? null,
        status: DEFAULT_STATUS,
        createdAt: ctx.blockTime,
      },
      update: {
        ...(assignedFeedbackId !== undefined ? { feedbackId: assignedFeedbackId } : {}),
      },
    });
  });
  // Reconcile orphan responses
  const orphans = await tx.orphanResponse.findMany({
    where: { agentId: assetId, client: clientAddress, feedbackIndex: data.feedbackIndex },
    orderBy: [
      { slot: "asc" },
      { txIndex: "asc" },
      { eventOrdinal: "asc" },
      { txSignature: "asc" },
      { id: "asc" },
    ],
  });
  for (const orphan of orphans) {
    const orphanSealMismatch = !hashesMatch(orphan.sealHash, feedback.feedbackHash);
    const orphanResponseStatus = orphanSealMismatch ? "ORPHANED" as const : DEFAULT_STATUS;
    await withResponseIdAssignmentLock(async () => {
      const orphanTxSignature = orphan.txSignature ?? "";
      const existingResponse = await tx.feedbackResponse.findUnique({
        where: {
          feedbackId_responder_txSignature: {
            feedbackId: feedback.id,
            responder: orphan.responder,
            txSignature: orphanTxSignature,
          },
        },
        select: { responseId: true },
      });

      let assignedResponseId: bigint | undefined;
      if (
        orphanResponseStatus !== "ORPHANED" &&
        (!existingResponse || existingResponse.responseId === null)
      ) {
        const highestAssigned = await tx.feedbackResponse.findMany({
          where: { feedbackId: feedback.id, responseId: { not: null } },
          select: { responseId: true },
          orderBy: { responseId: "desc" },
          take: 1,
        });
        assignedResponseId = asBigInt(highestAssigned[0]?.responseId) + 1n;
      }

      await tx.feedbackResponse.upsert({
        where: {
          feedbackId_responder_txSignature: {
            feedbackId: feedback.id,
            responder: orphan.responder,
            txSignature: orphanTxSignature,
          },
        },
        create: {
          feedbackId: feedback.id,
          responder: orphan.responder,
          responseUri: orphan.responseUri,
          responseHash: orphan.responseHash,
          runningDigest: orphan.runningDigest,
          responseCount: orphan.responseCount,
          txSignature: orphan.txSignature,
          slot: orphan.slot,
          txIndex: orphan.txIndex,
          eventOrdinal: orphan.eventOrdinal,
          responseId: orphanResponseStatus === "ORPHANED" ? null : (assignedResponseId ?? null),
          status: orphanResponseStatus,
          createdAt: orphan.createdAt,
        },
        update: {
          responseCount: orphan.responseCount,
          ...(orphanResponseStatus === "ORPHANED"
            ? { status: orphanResponseStatus }
            : assignedResponseId !== undefined
              ? { responseId: assignedResponseId, status: orphanResponseStatus }
              : { status: orphanResponseStatus }),
        },
      });
    });
    await tx.orphanResponse.delete({ where: { id: orphan.id } });
  }
  if (orphans.length > 0) {
    logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString(), count: orphans.length }, "Reconciled orphan responses");
  }

  // Reconcile orphan revocation for the same feedback scope if it can now be proven.
  const orphanRevocation = await tx.revocation.findUnique({
    where: {
      agentId_client_feedbackIndex: {
        agentId: assetId,
        client: clientAddress,
        feedbackIndex: data.feedbackIndex,
      },
    },
    select: { revocationId: true, feedbackHash: true, status: true },
  });
  if (
    orphanRevocation &&
    orphanRevocation.status === "ORPHANED" &&
    hashesMatch(orphanRevocation.feedbackHash, feedback.feedbackHash)
  ) {
    await withRevocationIdAssignmentLock(async () => {
      let assignedRevocationId: bigint | undefined;
      if (orphanRevocation.revocationId === null) {
        const highestAssigned = await tx.revocation.findMany({
          where: { agentId: assetId, revocationId: { not: null } },
          select: { revocationId: true },
          orderBy: { revocationId: "desc" },
          take: 1,
        });
        assignedRevocationId = asBigInt(highestAssigned[0]?.revocationId) + 1n;
      }

      await tx.revocation.updateMany({
        where: {
          agentId: assetId,
          client: clientAddress,
          feedbackIndex: data.feedbackIndex,
          status: "ORPHANED",
        },
        data: {
          status: DEFAULT_STATUS,
          ...(assignedRevocationId !== undefined ? { revocationId: assignedRevocationId } : {}),
        },
      });
    });
    logger.info(
      { assetId, feedbackIndex: data.feedbackIndex.toString() },
      "Reconciled orphan revocation"
    );
  }

  await syncAgentFeedbackStatsTx(
    tx,
    assetId,
    ctx.blockTime,
    data.atomEnabled
      ? {
          trustTier: data.newTrustTier,
          qualityScore: data.newQualityScore,
          confidence: data.newConfidence,
          riskScore: data.newRiskScore,
          diversityRatio: data.newDiversityRatio,
        }
      : undefined
  );

  logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString(), score: data.score }, "New feedback");
}

async function handleNewFeedback(
  prisma: PrismaClient,
  data: NewFeedback,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const clientAddress = data.clientAddress.toBase58();
  const agent = await prisma.agent.findUnique({
    where: { id: assetId },
    select: { id: true },
  });
  if (agent === null) {
    await upsertOrphanFeedback(prisma, data, ctx);
    logger.warn(
      { assetId, clientAddress, feedbackIndex: data.feedbackIndex.toString() },
      "Agent missing for feedback; stored orphan feedback"
    );
    return;
  }

  const feedback = await withFeedbackIdAssignmentLock(async () => {
    const existingFeedback = await prisma.feedback.findUnique({
      where: {
        agentId_client_feedbackIndex: {
          agentId: assetId,
          client: clientAddress,
          feedbackIndex: data.feedbackIndex,
        },
      },
      select: { feedbackId: true },
    });

    let assignedFeedbackId: bigint | undefined;
    if (!existingFeedback || existingFeedback.feedbackId === null) {
      const highestAssigned = await prisma.feedback.findMany({
        where: { agentId: assetId, feedbackId: { not: null } },
        select: { feedbackId: true },
        orderBy: { feedbackId: "desc" },
        take: 1,
      });
      assignedFeedbackId = asBigInt(highestAssigned[0]?.feedbackId) + 1n;
    }

    return prisma.feedback.upsert({
      where: {
        agentId_client_feedbackIndex: {
          agentId: assetId,
          client: clientAddress,
          feedbackIndex: data.feedbackIndex,
        },
      },
      create: {
        agentId: assetId,
        client: clientAddress,
        feedbackIndex: data.feedbackIndex,
        value: data.value.toString(),
        valueDecimals: data.valueDecimals,
        score: data.score,
        tag1: data.tag1,
        tag2: data.tag2,
        endpoint: data.endpoint,
        feedbackUri: data.feedbackUri,
        feedbackHash: normalizeHash(data.sealHash),
        runningDigest: Uint8Array.from(data.newFeedbackDigest) as Uint8Array<ArrayBuffer>,
        createdTxSignature: ctx.signature,
        createdSlot: ctx.slot,
        txIndex: ctx.txIndex ?? null,
        eventOrdinal: ctx.eventOrdinal ?? null,
        feedbackId: assignedFeedbackId ?? null,
        status: DEFAULT_STATUS,
        createdAt: ctx.blockTime,
      },
      update: {
        ...(assignedFeedbackId !== undefined ? { feedbackId: assignedFeedbackId } : {}),
      },
    });
  });

  // Reconcile orphan responses
  const orphans = await prisma.orphanResponse.findMany({
    where: { agentId: assetId, client: clientAddress, feedbackIndex: data.feedbackIndex },
    orderBy: [
      { slot: "asc" },
      { txIndex: "asc" },
      { eventOrdinal: "asc" },
      { txSignature: "asc" },
      { id: "asc" },
    ],
  });

  for (const orphan of orphans) {
    const orphanSealMismatch = !hashesMatch(orphan.sealHash, feedback.feedbackHash);
    const orphanResponseStatus = orphanSealMismatch ? "ORPHANED" as const : DEFAULT_STATUS;
    await withResponseIdAssignmentLock(async () => {
      const orphanTxSignature = orphan.txSignature ?? "";
      const existingResponse = await prisma.feedbackResponse.findUnique({
        where: {
          feedbackId_responder_txSignature: {
            feedbackId: feedback.id,
            responder: orphan.responder,
            txSignature: orphanTxSignature,
          },
        },
        select: { responseId: true },
      });

      let assignedResponseId: bigint | undefined;
      if (
        orphanResponseStatus !== "ORPHANED" &&
        (!existingResponse || existingResponse.responseId === null)
      ) {
        const highestAssigned = await prisma.feedbackResponse.findMany({
          where: { feedbackId: feedback.id, responseId: { not: null } },
          select: { responseId: true },
          orderBy: { responseId: "desc" },
          take: 1,
        });
        assignedResponseId = asBigInt(highestAssigned[0]?.responseId) + 1n;
      }

      await prisma.feedbackResponse.upsert({
        where: {
          feedbackId_responder_txSignature: {
            feedbackId: feedback.id,
            responder: orphan.responder,
            txSignature: orphanTxSignature,
          },
        },
        create: {
          feedbackId: feedback.id,
          responder: orphan.responder,
          responseUri: orphan.responseUri,
          responseHash: orphan.responseHash,
          runningDigest: orphan.runningDigest,
          responseCount: orphan.responseCount,
          txSignature: orphan.txSignature,
          slot: orphan.slot,
          txIndex: orphan.txIndex,
          eventOrdinal: orphan.eventOrdinal,
          responseId: orphanResponseStatus === "ORPHANED" ? null : (assignedResponseId ?? null),
          status: orphanResponseStatus,
          createdAt: orphan.createdAt,
        },
        update: {
          responseCount: orphan.responseCount,
          ...(orphanResponseStatus === "ORPHANED"
            ? { status: orphanResponseStatus }
            : assignedResponseId !== undefined
              ? { responseId: assignedResponseId, status: orphanResponseStatus }
              : { status: orphanResponseStatus }),
        },
      });
    });
    await prisma.orphanResponse.delete({ where: { id: orphan.id } });
  }

  if (orphans.length > 0) {
    logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString(), count: orphans.length }, "Reconciled orphan responses");
  }

  // Reconcile orphan revocation for the same feedback scope if it can now be proven.
  const orphanRevocation = await prisma.revocation.findUnique({
    where: {
      agentId_client_feedbackIndex: {
        agentId: assetId,
        client: clientAddress,
        feedbackIndex: data.feedbackIndex,
      },
    },
    select: { revocationId: true, feedbackHash: true, status: true },
  });
  if (
    orphanRevocation &&
    orphanRevocation.status === "ORPHANED" &&
    hashesMatch(orphanRevocation.feedbackHash, feedback.feedbackHash)
  ) {
    await withRevocationIdAssignmentLock(async () => {
      let assignedRevocationId: bigint | undefined;
      if (orphanRevocation.revocationId === null) {
        const highestAssigned = await prisma.revocation.findMany({
          where: { agentId: assetId, revocationId: { not: null } },
          select: { revocationId: true },
          orderBy: { revocationId: "desc" },
          take: 1,
        });
        assignedRevocationId = asBigInt(highestAssigned[0]?.revocationId) + 1n;
      }

      await prisma.revocation.updateMany({
        where: {
          agentId: assetId,
          client: clientAddress,
          feedbackIndex: data.feedbackIndex,
          status: "ORPHANED",
        },
        data: {
          status: DEFAULT_STATUS,
          ...(assignedRevocationId !== undefined ? { revocationId: assignedRevocationId } : {}),
        },
      });
    });
    logger.info(
      { assetId, feedbackIndex: data.feedbackIndex.toString() },
      "Reconciled orphan revocation"
    );
  }

  await syncAgentFeedbackStats(
    prisma,
    assetId,
    ctx.blockTime,
    data.atomEnabled
      ? {
          trustTier: data.newTrustTier,
          qualityScore: data.newQualityScore,
          confidence: data.newConfidence,
          riskScore: data.newRiskScore,
          diversityRatio: data.newDiversityRatio,
        }
      : undefined
  );

  logger.info(
    {
      assetId,
      feedbackIndex: data.feedbackIndex.toString(),
      score: data.score,
    },
    "New feedback"
  );
}

async function handleFeedbackRevokedTx(
  tx: PrismaTransactionClient,
  data: FeedbackRevoked,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const clientAddress = data.clientAddress.toBase58();
  const eventSealHash = normalizeHash(data.sealHash);

  const feedback = await tx.feedback.findUnique({
    where: {
      agentId_client_feedbackIndex: {
        agentId: assetId,
        client: clientAddress,
        feedbackIndex: data.feedbackIndex,
      },
    },
    select: { feedbackHash: true },
  });

  let sealMismatch = false;
  if (!feedback) {
    logger.warn(
      { assetId, client: clientAddress, feedbackIndex: data.feedbackIndex.toString() },
      "Feedback not found for revocation (orphan revoke)"
    );
  } else if (!hashesMatch(eventSealHash, feedback.feedbackHash)) {
    sealMismatch = true;
    logger.warn(
      { assetId, client: clientAddress, feedbackIndex: data.feedbackIndex.toString() },
      "seal_hash mismatch: revocation sealHash does not match stored feedbackHash"
    );
  }

  // Parity with Supabase batch path:
  // - missing feedback => ORPHANED
  // - seal mismatch with existing feedback => warn, keep PENDING
  const revokeStatus = classifyRevocationStatus(Boolean(feedback));
  if (revokeStatus !== "ORPHANED") {
    await tx.feedback.updateMany({
      where: { agentId: assetId, client: clientAddress, feedbackIndex: data.feedbackIndex },
      data: { revoked: true, revokedTxSignature: ctx.signature, revokedSlot: ctx.slot },
    });
  }
  await withRevocationIdAssignmentLock(async () => {
    const existingRevocation = await tx.revocation.findUnique({
      where: {
        agentId_client_feedbackIndex: {
          agentId: assetId,
          client: clientAddress,
          feedbackIndex: data.feedbackIndex,
        },
      },
      select: { revocationId: true },
    });

    let assignedRevocationId: bigint | undefined;
    if (revokeStatus !== "ORPHANED" && (!existingRevocation || existingRevocation.revocationId === null)) {
      const highestAssigned = await tx.revocation.findMany({
        where: { agentId: assetId, revocationId: { not: null } },
        select: { revocationId: true },
        orderBy: { revocationId: "desc" },
        take: 1,
      });
      assignedRevocationId = asBigInt(highestAssigned[0]?.revocationId) + 1n;
    }

    await tx.revocation.upsert({
      where: { agentId_client_feedbackIndex: { agentId: assetId, client: clientAddress, feedbackIndex: data.feedbackIndex } },
      create: {
        agentId: assetId,
        client: clientAddress,
        feedbackIndex: data.feedbackIndex,
        feedbackHash: eventSealHash,
        slot: data.slot,
        originalScore: data.originalScore,
        atomEnabled: data.atomEnabled,
        hadImpact: data.hadImpact,
        runningDigest: Uint8Array.from(data.newRevokeDigest) as Uint8Array<ArrayBuffer>,
        revokeCount: data.newRevokeCount,
        txSignature: ctx.signature,
        txIndex: ctx.txIndex ?? null,
        eventOrdinal: ctx.eventOrdinal ?? null,
        revocationId: revokeStatus === "ORPHANED" ? null : (assignedRevocationId ?? null),
        status: revokeStatus,
        createdAt: ctx.blockTime,
      },
      update: {
        feedbackHash: eventSealHash,
        slot: data.slot,
        originalScore: data.originalScore,
        atomEnabled: data.atomEnabled,
        hadImpact: data.hadImpact,
        runningDigest: Uint8Array.from(data.newRevokeDigest) as Uint8Array<ArrayBuffer>,
        revokeCount: data.newRevokeCount,
        txSignature: ctx.signature,
        txIndex: ctx.txIndex ?? null,
        eventOrdinal: ctx.eventOrdinal ?? null,
        status: revokeStatus,
        ...(revokeStatus === "ORPHANED"
          ? {}
          : assignedRevocationId !== undefined
            ? { revocationId: assignedRevocationId }
            : {}),
      },
    });
  });

  if (revokeStatus !== "ORPHANED") {
    await syncAgentFeedbackStatsTx(
      tx,
      assetId,
      ctx.blockTime,
      data.atomEnabled && data.hadImpact
        ? {
            trustTier: data.newTrustTier,
            qualityScore: data.newQualityScore,
            confidence: data.newConfidence,
          }
        : undefined
    );
  }

  logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString(), orphan: !feedback, sealMismatch }, "Feedback revoked");
}

async function handleFeedbackRevoked(
  prisma: PrismaClient,
  data: FeedbackRevoked,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const clientAddress = data.clientAddress.toBase58();
  const eventSealHash = normalizeHash(data.sealHash);

  const feedback = await prisma.feedback.findUnique({
    where: {
      agentId_client_feedbackIndex: {
        agentId: assetId,
        client: clientAddress,
        feedbackIndex: data.feedbackIndex,
      },
    },
    select: { feedbackHash: true },
  });

  let sealMismatch = false;
  if (!feedback) {
    logger.warn(
      { assetId, client: clientAddress, feedbackIndex: data.feedbackIndex.toString() },
      "Feedback not found for revocation (orphan revoke)"
    );
  } else if (!hashesMatch(eventSealHash, feedback.feedbackHash)) {
    sealMismatch = true;
    logger.warn(
      { assetId, client: clientAddress, feedbackIndex: data.feedbackIndex.toString() },
      "seal_hash mismatch: revocation sealHash does not match stored feedbackHash"
    );
  }

  // Parity with Supabase batch path:
  // - missing feedback => ORPHANED
  // - seal mismatch with existing feedback => warn, keep PENDING
  const revokeStatus = classifyRevocationStatus(Boolean(feedback));
  if (revokeStatus !== "ORPHANED") {
    await prisma.feedback.updateMany({
      where: { agentId: assetId, client: clientAddress, feedbackIndex: data.feedbackIndex },
      data: { revoked: true, revokedTxSignature: ctx.signature, revokedSlot: ctx.slot },
    });
  }
  await withRevocationIdAssignmentLock(async () => {
    const existingRevocation = await prisma.revocation.findUnique({
      where: {
        agentId_client_feedbackIndex: {
          agentId: assetId,
          client: clientAddress,
          feedbackIndex: data.feedbackIndex,
        },
      },
      select: { revocationId: true },
    });

    let assignedRevocationId: bigint | undefined;
    if (revokeStatus !== "ORPHANED" && (!existingRevocation || existingRevocation.revocationId === null)) {
      const highestAssigned = await prisma.revocation.findMany({
        where: { agentId: assetId, revocationId: { not: null } },
        select: { revocationId: true },
        orderBy: { revocationId: "desc" },
        take: 1,
      });
      assignedRevocationId = asBigInt(highestAssigned[0]?.revocationId) + 1n;
    }

    await prisma.revocation.upsert({
      where: { agentId_client_feedbackIndex: { agentId: assetId, client: clientAddress, feedbackIndex: data.feedbackIndex } },
      create: {
        agentId: assetId,
        client: clientAddress,
        feedbackIndex: data.feedbackIndex,
        feedbackHash: eventSealHash,
        slot: data.slot,
        originalScore: data.originalScore,
        atomEnabled: data.atomEnabled,
        hadImpact: data.hadImpact,
        runningDigest: Uint8Array.from(data.newRevokeDigest) as Uint8Array<ArrayBuffer>,
        revokeCount: data.newRevokeCount,
        txSignature: ctx.signature,
        txIndex: ctx.txIndex ?? null,
        eventOrdinal: ctx.eventOrdinal ?? null,
        revocationId: revokeStatus === "ORPHANED" ? null : (assignedRevocationId ?? null),
        status: revokeStatus,
        createdAt: ctx.blockTime,
      },
      update: {
        feedbackHash: eventSealHash,
        slot: data.slot,
        originalScore: data.originalScore,
        atomEnabled: data.atomEnabled,
        hadImpact: data.hadImpact,
        runningDigest: Uint8Array.from(data.newRevokeDigest) as Uint8Array<ArrayBuffer>,
        revokeCount: data.newRevokeCount,
        txSignature: ctx.signature,
        txIndex: ctx.txIndex ?? null,
        eventOrdinal: ctx.eventOrdinal ?? null,
        status: revokeStatus,
        ...(revokeStatus === "ORPHANED"
          ? {}
          : assignedRevocationId !== undefined
            ? { revocationId: assignedRevocationId }
            : {}),
      },
    });
  });

  if (revokeStatus !== "ORPHANED") {
    await syncAgentFeedbackStats(
      prisma,
      assetId,
      ctx.blockTime,
      data.atomEnabled && data.hadImpact
        ? {
            trustTier: data.newTrustTier,
            qualityScore: data.newQualityScore,
            confidence: data.newConfidence,
          }
        : undefined
    );
  }

  logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString(), orphan: !feedback, sealMismatch }, "Feedback revoked");
}

async function handleResponseAppendedTx(
  tx: PrismaTransactionClient,
  data: ResponseAppended,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const clientAddress = data.client.toBase58();
  const responder = data.responder.toBase58();
  const feedback = await tx.feedback.findUnique({
    where: {
      agentId_client_feedbackIndex: {
        agentId: assetId,
        client: clientAddress,
        feedbackIndex: data.feedbackIndex,
      },
    },
  });
  if (!feedback) {
    logger.warn(
      { assetId, client: clientAddress, feedbackIndex: data.feedbackIndex.toString() },
      "Feedback not found, storing as orphan response"
    );
    await tx.orphanResponse.upsert({
      where: {
        agentId_client_feedbackIndex_responder_txSignature: {
          agentId: assetId,
          client: clientAddress,
          feedbackIndex: data.feedbackIndex,
          responder,
          txSignature: ctx.signature,
        },
      },
      create: {
        agentId: assetId,
        client: clientAddress,
        feedbackIndex: data.feedbackIndex,
        responder,
        responseUri: data.responseUri,
        responseHash: normalizeHash(data.responseHash),
        sealHash: normalizeHash(data.sealHash),
        runningDigest: data.newResponseDigest ? Uint8Array.from(data.newResponseDigest) as Uint8Array<ArrayBuffer> : null,
        responseCount: data.newResponseCount,
        txSignature: ctx.signature,
        slot: ctx.slot,
        txIndex: ctx.txIndex ?? null,
        eventOrdinal: ctx.eventOrdinal ?? null,
        createdAt: ctx.blockTime,
      },
      update: {
        responseUri: data.responseUri,
        responseHash: normalizeHash(data.responseHash),
        sealHash: normalizeHash(data.sealHash),
        runningDigest: data.newResponseDigest ? Uint8Array.from(data.newResponseDigest) as Uint8Array<ArrayBuffer> : null,
        responseCount: data.newResponseCount,
        slot: ctx.slot,
        txIndex: ctx.txIndex ?? null,
        eventOrdinal: ctx.eventOrdinal ?? null,
        createdAt: ctx.blockTime,
      },
    });
    logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString() }, "Orphan response stored");
    return;
  }

  const eventSealHash = normalizeHash(data.sealHash);
  const sealMismatch = !hashesMatch(eventSealHash, feedback.feedbackHash);
  if (sealMismatch) {
    logger.warn(
      { assetId, client: clientAddress, feedbackIndex: data.feedbackIndex.toString() },
      "seal_hash mismatch: response sealHash does not match stored feedbackHash"
    );
  }

  const responseStatus = sealMismatch ? "ORPHANED" as const : DEFAULT_STATUS;

  await withResponseIdAssignmentLock(async () => {
    const existingResponse = await tx.feedbackResponse.findUnique({
      where: {
        feedbackId_responder_txSignature: {
          feedbackId: feedback.id,
          responder,
          txSignature: ctx.signature,
        },
      },
      select: { responseId: true },
    });

    let assignedResponseId: bigint | undefined;
    if (responseStatus !== "ORPHANED" && (!existingResponse || existingResponse.responseId === null)) {
      const highestAssigned = await tx.feedbackResponse.findMany({
        where: { feedbackId: feedback.id, responseId: { not: null } },
        select: { responseId: true },
        orderBy: { responseId: "desc" },
        take: 1,
      });
      assignedResponseId = asBigInt(highestAssigned[0]?.responseId) + 1n;
    }

    await tx.feedbackResponse.upsert({
      where: {
        feedbackId_responder_txSignature: {
          feedbackId: feedback.id,
          responder,
          txSignature: ctx.signature,
        },
      },
      create: {
        feedbackId: feedback.id,
        responder,
        responseUri: data.responseUri,
        responseHash: normalizeHash(data.responseHash),
        runningDigest: Uint8Array.from(data.newResponseDigest) as Uint8Array<ArrayBuffer>,
        responseCount: data.newResponseCount,
        txSignature: ctx.signature,
        slot: ctx.slot,
        txIndex: ctx.txIndex ?? null,
        eventOrdinal: ctx.eventOrdinal ?? null,
        responseId: responseStatus === "ORPHANED" ? null : (assignedResponseId ?? null),
        status: responseStatus,
        createdAt: ctx.blockTime,
      },
      update: {
        ...(responseStatus === "ORPHANED"
          ? { status: responseStatus }
          : assignedResponseId !== undefined
            ? { responseId: assignedResponseId, status: responseStatus }
            : { status: responseStatus }),
      },
    });
  });
  logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString() }, "Response appended");
}

async function handleResponseAppended(
  prisma: PrismaClient,
  data: ResponseAppended,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const clientAddress = data.client.toBase58();
  const responder = data.responder.toBase58();

  const feedback = await prisma.feedback.findUnique({
    where: {
      agentId_client_feedbackIndex: {
        agentId: assetId,
        client: clientAddress,
        feedbackIndex: data.feedbackIndex,
      },
    },
  });

  if (!feedback) {
    logger.warn(
      { assetId, client: clientAddress, feedbackIndex: data.feedbackIndex.toString() },
      "Feedback not found, storing as orphan response"
    );

    await prisma.orphanResponse.upsert({
      where: {
        agentId_client_feedbackIndex_responder_txSignature: {
          agentId: assetId,
          client: clientAddress,
          feedbackIndex: data.feedbackIndex,
          responder,
          txSignature: ctx.signature,
        },
      },
      create: {
        agentId: assetId,
        client: clientAddress,
        feedbackIndex: data.feedbackIndex,
        responder,
        responseUri: data.responseUri,
        responseHash: normalizeHash(data.responseHash),
        sealHash: normalizeHash(data.sealHash),
        runningDigest: data.newResponseDigest ? Uint8Array.from(data.newResponseDigest) as Uint8Array<ArrayBuffer> : null,
        responseCount: data.newResponseCount,
        txSignature: ctx.signature,
        slot: ctx.slot,
        txIndex: ctx.txIndex ?? null,
        eventOrdinal: ctx.eventOrdinal ?? null,
        createdAt: ctx.blockTime,
      },
      update: {
        responseUri: data.responseUri,
        responseHash: normalizeHash(data.responseHash),
        sealHash: normalizeHash(data.sealHash),
        runningDigest: data.newResponseDigest ? Uint8Array.from(data.newResponseDigest) as Uint8Array<ArrayBuffer> : null,
        responseCount: data.newResponseCount,
        slot: ctx.slot,
        txIndex: ctx.txIndex ?? null,
        eventOrdinal: ctx.eventOrdinal ?? null,
        createdAt: ctx.blockTime,
      },
    });

    logger.info(
      { assetId, feedbackIndex: data.feedbackIndex.toString() },
      "Orphan response stored"
    );
    return;
  }

  const eventSealHash = normalizeHash(data.sealHash);
  const sealMismatch = !hashesMatch(eventSealHash, feedback.feedbackHash);
  if (sealMismatch) {
    logger.warn(
      { assetId, client: clientAddress, feedbackIndex: data.feedbackIndex.toString() },
      "seal_hash mismatch: response sealHash does not match stored feedbackHash"
    );
  }

  const responseStatus = sealMismatch ? "ORPHANED" as const : DEFAULT_STATUS;

  await withResponseIdAssignmentLock(async () => {
    const existingResponse = await prisma.feedbackResponse.findUnique({
      where: {
        feedbackId_responder_txSignature: {
          feedbackId: feedback.id,
          responder,
          txSignature: ctx.signature,
        },
      },
      select: { responseId: true },
    });

    let assignedResponseId: bigint | undefined;
    if (responseStatus !== "ORPHANED" && (!existingResponse || existingResponse.responseId === null)) {
      const highestAssigned = await prisma.feedbackResponse.findMany({
        where: { feedbackId: feedback.id, responseId: { not: null } },
        select: { responseId: true },
        orderBy: { responseId: "desc" },
        take: 1,
      });
      assignedResponseId = asBigInt(highestAssigned[0]?.responseId) + 1n;
    }

    await prisma.feedbackResponse.upsert({
      where: {
        feedbackId_responder_txSignature: {
          feedbackId: feedback.id,
          responder,
          txSignature: ctx.signature,
        },
      },
      create: {
        feedbackId: feedback.id,
        responder,
        responseUri: data.responseUri,
        responseHash: normalizeHash(data.responseHash),
        runningDigest: Uint8Array.from(data.newResponseDigest) as Uint8Array<ArrayBuffer>,
        responseCount: data.newResponseCount,
        txSignature: ctx.signature,
        slot: ctx.slot,
        txIndex: ctx.txIndex ?? null,
        eventOrdinal: ctx.eventOrdinal ?? null,
        responseId: responseStatus === "ORPHANED" ? null : (assignedResponseId ?? null),
        status: responseStatus,
        createdAt: ctx.blockTime,
      },
      update: {
        ...(responseStatus === "ORPHANED"
          ? { status: responseStatus }
          : assignedResponseId !== undefined
            ? { responseId: assignedResponseId, status: responseStatus }
            : { status: responseStatus }),
      },
    });
  });

  logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString() }, "Response appended");
}

async function handleValidationRequestedTx(
  tx: PrismaTransactionClient,
  data: ValidationRequested,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  await tx.validation.upsert({
    where: {
      agentId_validator_nonce: {
        agentId: assetId,
        validator: data.validatorAddress.toBase58(),
        nonce: data.nonce,
      },
    },
    create: {
      agentId: assetId,
      validator: data.validatorAddress.toBase58(),
      requester: data.requester.toBase58(),
      nonce: data.nonce,
      requestUri: data.requestUri,
      requestHash: normalizeHash(data.requestHash),
      requestTxSignature: ctx.signature,
      requestSlot: ctx.slot,
      requestTxIndex: ctx.txIndex ?? null,
      requestEventOrdinal: ctx.eventOrdinal ?? null,
      chainStatus: DEFAULT_STATUS,
    },
    update: {
      requester: data.requester.toBase58(),
      requestUri: data.requestUri,
      requestHash: normalizeHash(data.requestHash),
      requestTxSignature: ctx.signature,
      requestSlot: ctx.slot,
      requestTxIndex: ctx.txIndex ?? null,
      requestEventOrdinal: ctx.eventOrdinal ?? null,
    },
  });
  logger.info({ assetId, validator: data.validatorAddress.toBase58(), nonce: data.nonce }, "Validation requested");
}

async function handleValidationRequested(
  prisma: PrismaClient,
  data: ValidationRequested,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();

  await prisma.validation.upsert({
    where: {
      agentId_validator_nonce: {
        agentId: assetId,
        validator: data.validatorAddress.toBase58(),
        nonce: data.nonce,
      },
    },
    create: {
      agentId: assetId,
      validator: data.validatorAddress.toBase58(),
      requester: data.requester.toBase58(),
      nonce: data.nonce,
      requestUri: data.requestUri,
      requestHash: normalizeHash(data.requestHash),
      requestTxSignature: ctx.signature,
      requestSlot: ctx.slot,
      requestTxIndex: ctx.txIndex ?? null,
      requestEventOrdinal: ctx.eventOrdinal ?? null,
      chainStatus: DEFAULT_STATUS,
    },
    update: {
      // Backfill request fields if response was indexed first
      requester: data.requester.toBase58(),
      requestUri: data.requestUri,
      requestHash: normalizeHash(data.requestHash),
      requestTxSignature: ctx.signature,
      requestSlot: ctx.slot,
      requestTxIndex: ctx.txIndex ?? null,
      requestEventOrdinal: ctx.eventOrdinal ?? null,
    },
  });

  logger.info(
    {
      assetId,
      validator: data.validatorAddress.toBase58(),
      nonce: data.nonce,
    },
    "Validation requested"
  );
}

async function handleValidationRespondedTx(
  tx: PrismaTransactionClient,
  data: ValidationResponded,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  await tx.validation.upsert({
    where: {
      agentId_validator_nonce: {
        agentId: assetId,
        validator: data.validatorAddress.toBase58(),
        nonce: data.nonce,
      },
    },
    create: {
      agentId: assetId,
      validator: data.validatorAddress.toBase58(),
      nonce: data.nonce,
      requester: "unknown",
      requestUri: null,
      requestHash: null,
      requestTxSignature: ctx.signature,
      requestSlot: ctx.slot,
      requestTxIndex: ctx.txIndex ?? null,
      requestEventOrdinal: ctx.eventOrdinal ?? null,
      response: data.response,
      responseUri: data.responseUri,
      responseHash: normalizeHash(data.responseHash),
      tag: data.tag,
      respondedAt: ctx.blockTime,
      responseTxSignature: ctx.signature,
      responseSlot: ctx.slot,
      responseTxIndex: ctx.txIndex ?? null,
      responseEventOrdinal: ctx.eventOrdinal ?? null,
      chainStatus: DEFAULT_STATUS,
    },
    update: {
      response: data.response,
      responseUri: data.responseUri,
      responseHash: normalizeHash(data.responseHash),
      tag: data.tag,
      respondedAt: ctx.blockTime,
      responseTxSignature: ctx.signature,
      responseSlot: ctx.slot,
      responseTxIndex: ctx.txIndex ?? null,
      responseEventOrdinal: ctx.eventOrdinal ?? null,
    },
  });
  logger.info({ assetId, validator: data.validatorAddress.toBase58(), nonce: data.nonce, response: data.response }, "Validation responded");
}

async function handleValidationResponded(
  prisma: PrismaClient,
  data: ValidationResponded,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();

  await prisma.validation.upsert({
    where: {
      agentId_validator_nonce: {
        agentId: assetId,
        validator: data.validatorAddress.toBase58(),
        nonce: data.nonce,
      },
    },
    create: {
      agentId: assetId,
      validator: data.validatorAddress.toBase58(),
      nonce: data.nonce,
      requester: "unknown",
      requestUri: null,
      requestHash: null,
      requestTxSignature: ctx.signature,
      requestSlot: ctx.slot,
      requestTxIndex: ctx.txIndex ?? null,
      requestEventOrdinal: ctx.eventOrdinal ?? null,
      response: data.response,
      responseUri: data.responseUri,
      responseHash: normalizeHash(data.responseHash),
      tag: data.tag,
      respondedAt: ctx.blockTime,
      responseTxSignature: ctx.signature,
      responseSlot: ctx.slot,
      responseTxIndex: ctx.txIndex ?? null,
      responseEventOrdinal: ctx.eventOrdinal ?? null,
      chainStatus: DEFAULT_STATUS,
    },
    update: {
      response: data.response,
      responseUri: data.responseUri,
      responseHash: normalizeHash(data.responseHash),
      tag: data.tag,
      respondedAt: ctx.blockTime,
      responseTxSignature: ctx.signature,
      responseSlot: ctx.slot,
      responseTxIndex: ctx.txIndex ?? null,
      responseEventOrdinal: ctx.eventOrdinal ?? null,
    },
  });

  logger.info(
    {
      assetId,
      validator: data.validatorAddress.toBase58(),
      nonce: data.nonce,
      response: data.response,
    },
    "Validation responded"
  );
}

/**
 * Fetch, digest, and store URI metadata for an agent (local/Prisma mode)
 *
 * RACE CONDITION PROTECTION: Because URI fetches are queued and network latency varies,
 * two consecutive URI updates (block N and N+1) might complete out of order.
 * We check if the agent's current URI matches before writing to prevent stale overwrites.
 */
async function digestAndStoreUriMetadataLocal(
  prisma: PrismaClient,
  assetId: string,
  uri: string,
  verifiedAt?: Date
): Promise<void> {
  if (config.metadataIndexMode === "off") {
    return;
  }

  // RACE CONDITION CHECK: Verify URI hasn't changed while we were queued/fetching
  // This prevents stale data from overwriting newer data due to out-of-order completion
  const agent = await prisma.agent.findUnique({
    where: { id: assetId },
    select: { uri: true, updatedAt: true, createdAt: true },
  });

  if (!agent) {
    logger.debug({ assetId, uri }, "Agent no longer exists, skipping URI digest");
    return;
  }

  if (agent.uri !== uri) {
    logger.debug({
      assetId,
      expectedUri: uri,
      currentUri: agent.uri
    }, "Agent URI changed while processing, skipping stale write");
    return;
  }

  const result = await digestUri(uri);

  // Re-check URI freshness after network fetch (TOCTOU protection)
  const agentRecheck = await prisma.agent.findUnique({
    where: { id: assetId },
    select: { uri: true, updatedAt: true, createdAt: true },
  });
  if (!agentRecheck || agentRecheck.uri !== uri) {
    logger.debug({ assetId, uri }, "URI changed during fetch, discarding stale results");
    return;
  }
  const metadataVerifiedAt = verifiedAt ?? resolveDeterministicMetadataVerifiedAt(
    agentRecheck.updatedAt,
    agentRecheck.createdAt
  );

  // Purge old URI-derived metadata before storing new ones
  try {
    await prisma.agentMetadata.deleteMany({
      where: {
        agentId: assetId,
        key: { startsWith: "_uri:" },
        NOT: { key: "_uri:_source" },
        immutable: false,
      },
    });
    logger.debug({ assetId }, "Purged old URI metadata");
  } catch (error: any) {
    logger.warn({ assetId, error: error.message }, "Failed to purge old URI metadata");
  }

  // Keep extraction source for restart recovery and URI mismatch detection.
  await storeUriMetadataLocal(prisma, assetId, "_uri:_source", uri, metadataVerifiedAt);

  if (result.status !== "ok" || !result.fields) {
    logger.debug({ assetId, uri, status: result.status, error: result.error }, "URI digest failed or empty");
    // Store error status as metadata
    await storeUriMetadataLocal(
      prisma,
      assetId,
      "_uri:_status",
      JSON.stringify(toDeterministicUriStatus(result)),
      metadataVerifiedAt
    );
    return;
  }

  // Store each extracted field
  const maxValueBytes = config.metadataMaxValueBytes;
  for (const [key, value] of Object.entries(result.fields)) {
    const serialized = serializeValue(value, maxValueBytes);

    if (serialized.oversize) {
      // Store metadata about oversize field
      await storeUriMetadataLocal(
        prisma,
        assetId,
        `${key}_meta`,
        JSON.stringify({
          status: "oversize",
          bytes: serialized.bytes,
          sha256: result.hash,
        }),
        metadataVerifiedAt
      );
    } else {
      await storeUriMetadataLocal(prisma, assetId, key, serialized.value, metadataVerifiedAt);
    }
  }

  // Store success status with truncation info
  await storeUriMetadataLocal(
    prisma,
    assetId,
    "_uri:_status",
    JSON.stringify(toDeterministicUriStatus(result)),
    metadataVerifiedAt
  );

  // Sync nftName from _uri:name if not already set
  const uriName = result.fields["_uri:name"];
  if (uriName && typeof uriName === "string") {
    try {
      // Check current value first, then update if empty
      const agent = await prisma.agent.findUnique({ where: { id: assetId }, select: { nftName: true } });
      if (!agent?.nftName) {
        await prisma.agent.update({
          where: { id: assetId },
          data: { nftName: uriName },
        });
        logger.debug({ assetId, name: uriName }, "Synced nftName from URI metadata");
      }
    } catch (error: any) {
      logger.warn({ assetId, error: error.message }, "Failed to sync nftName");
    }
  }

  logger.info({ assetId, uri, fieldCount: Object.keys(result.fields).length }, "URI metadata indexed");
}

/**
 * Fetch, digest, and store collection metadata from canonical pointer (local/Prisma mode).
 * The `parent` field is intentionally ignored because parent linkage is on-chain authoritative.
 */
async function digestAndStoreCollectionMetadataLocal(
  prisma: PrismaClient,
  assetId: string,
  pointer: string
): Promise<void> {
  if (!config.collectionMetadataIndexEnabled) {
    return;
  }

  const agent = await prisma.agent.findUnique({
    where: { id: assetId },
    select: { collectionPointer: true, creator: true, owner: true },
  });

  if (!agent) {
    logger.debug({ assetId, col: pointer }, "Agent no longer exists, skipping collection digest");
    return;
  }

  if (agent.collectionPointer !== pointer) {
    logger.debug(
      { assetId, expectedCol: pointer, currentCol: agent.collectionPointer },
      "Collection pointer changed while processing, skipping stale digest"
    );
    return;
  }

  const creator = agent.creator ?? agent.owner;
  const result = await digestCollectionPointerDoc(pointer);
  const collectionRow = await prisma.collection.findUnique({
    where: { col_creator: { col: pointer, creator } },
    select: { lastSeenAt: true },
  });
  const baseUpdate = {
    metadataUpdatedAt: collectionRow?.lastSeenAt ?? null,
    metadataStatus: result.status,
    metadataHash: result.hash ?? null,
    metadataBytes: result.bytes ?? null,
  };

  if (result.status !== "ok" || !result.fields) {
    const failed = await prisma.collection.updateMany({
      where: { col: pointer, creator },
      data: baseUpdate,
    });
    if (failed.count === 0) {
      logger.warn({ assetId, col: pointer, creator }, "Collection row not found for digest status update");
    }
    return;
  }

  const fields = result.fields;
  const updated = await prisma.collection.updateMany({
    where: { col: pointer, creator },
    data: {
      ...baseUpdate,
      version: fields.version,
      name: fields.name,
      symbol: fields.symbol,
      description: fields.description,
      image: fields.image,
      bannerImage: fields.bannerImage,
      socialWebsite: fields.socialWebsite,
      socialX: fields.socialX,
      socialDiscord: fields.socialDiscord,
    },
  });

  if (updated.count === 0) {
    logger.warn({ assetId, col: pointer, creator }, "Collection row not found for metadata update");
    return;
  }

  logger.info({ assetId, col: pointer, creator }, "Collection metadata indexed");
}

/**
 * Store a single URI metadata entry (local/Prisma mode)
 * Applies compression parity with Supabase mode:
 * - Standard URI fields: RAW with 0x00 prefix
 * - Custom fields: ZSTD compressed if > 256 bytes
 */
async function storeUriMetadataLocal(
  prisma: PrismaClient,
  assetId: string,
  key: string,
  value: string,
  verifiedAt: Date
): Promise<void> {
  try {
    const existing = await prisma.agentMetadata.findUnique({
      where: {
        agentId_key: {
          agentId: assetId,
          key,
        },
      },
      select: { immutable: true },
    });
    if (existing?.immutable) {
      logger.debug({ assetId, key }, "Skipping URI metadata overwrite for immutable entry");
      return;
    }

    // Apply compression parity with Supabase mode
    const shouldCompress = !STANDARD_URI_FIELDS.has(key) && key !== "_uri:_source";
    const storedBuffer = shouldCompress
      ? await compressForStorage(Buffer.from(value))
      : Buffer.concat([Buffer.from([0x00]), Buffer.from(value)]); // PREFIX_RAW
    // Convert to Uint8Array for Prisma compatibility
    const storedValue = new Uint8Array(storedBuffer);

    await prisma.agentMetadata.upsert({
      where: {
        agentId_key: {
          agentId: assetId,
          key,
        },
      },
      create: {
        agentId: assetId,
        key,
        value: storedValue,
        immutable: false,
        slot: 0n,
        status: "FINALIZED",
        verifiedAt,
      },
      update: {
        value: storedValue,
        status: "FINALIZED",
        verifiedAt,
      },
    });
  } catch (error: any) {
    logger.error({ error: error.message, assetId, key }, "Failed to store URI metadata");
    throw error;
  }
}

/**
 * Cleanup old orphan responses (> maxAgeMinutes old)
 * Call periodically or at startup to prevent table pollution
 * Orphan responses should be reconciled within seconds, 30 min default is generous
 */
export async function cleanupOrphanResponses(
  prisma: PrismaClient,
  maxAgeMinutes: number = 30
): Promise<number> {
  const state = await prisma.indexerState.findUnique({
    where: { id: "main" },
    select: { lastSlot: true },
  });
  if (state?.lastSlot === null || state?.lastSlot === undefined) {
    return 0;
  }
  const slotsToKeep = BigInt(Math.max(1, Math.ceil((maxAgeMinutes * 60_000) / 400)));
  const cutoffSlot = state.lastSlot > slotsToKeep ? state.lastSlot - slotsToKeep : 0n;

  const result = await prisma.orphanResponse.deleteMany({
    where: { slot: { not: null, lt: cutoffSlot } },
  });

  if (result.count > 0) {
    logger.info({ deleted: result.count, maxAgeMinutes }, "Cleaned up old orphan responses");
  }

  return result.count;
}
