import { PrismaClient } from "@prisma/client";
import PQueue from "p-queue";
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
import { digestUri, serializeValue } from "../indexer/uriDigest.js";
import { digestCollectionPointerDoc } from "../indexer/collectionDigest.js";
import { compressForStorage } from "../utils/compression.js";
import { stripNullBytes } from "../utils/sanitize.js";
import { DEFAULT_PUBKEY, STANDARD_URI_FIELDS } from "../constants.js";

const logger = createChildLogger("db-handlers");

// Global concurrency limiter for URI metadata fetching
// Prevents OOM from unbounded fire-and-forget digest operations
const MAX_URI_FETCH_CONCURRENT = 5;
const MAX_URI_FETCH_QUEUE = 100;
const uriDigestQueue = new PQueue({ concurrency: MAX_URI_FETCH_CONCURRENT });
let uriDigestDroppedCount = 0;
const MAX_COLLECTION_FETCH_CONCURRENT = 3;
const MAX_COLLECTION_FETCH_QUEUE = 200;
const collectionDigestQueue = new PQueue({ concurrency: MAX_COLLECTION_FETCH_CONCURRENT });
let collectionDigestDroppedCount = 0;

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
  return Uint8Array.from(hash) as Uint8Array<ArrayBuffer>;
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

function toRoundedScore(avg: number | null | undefined): number {
  if (avg === null || avg === undefined || Number.isNaN(avg)) return 0;
  return Math.round(avg);
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

  await prisma.$transaction(async (tx) => {
    // 1. Handle event
    await handleEventInner(tx, event, ctx);

    // 2. Update cursor atomically with monotonic guard
    await updateCursorAtomic(tx, ctx);
  });

  // 3. Trigger derived metadata extraction AFTER transaction (fire-and-forget)
  // This is outside the transaction to avoid blocking event processing
  await triggerDerivedDigestsIfNeeded(prisma, event);
}

/**
 * Update indexer cursor with monotonic guard
 * Only advances if the new slot is greater than the current slot
 */
async function updateCursorAtomic(
  tx: PrismaTransactionClient,
  ctx: EventContext
): Promise<void> {
  const current = await tx.indexerState.findUnique({
    where: { id: "main" },
    select: { lastSlot: true },
  });

  // Monotonic guard: reject backward slot movement
  if (current && current.lastSlot !== null && ctx.slot < current.lastSlot) {
    return;
  }

  await tx.indexerState.upsert({
    where: { id: "main" },
    create: {
      id: "main",
      lastSignature: ctx.signature,
      lastSlot: ctx.slot,
      source: ctx.source || "poller",
    },
    update: {
      lastSignature: ctx.signature,
      lastSlot: ctx.slot,
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
  event: ProgramEvent
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
    // Use bounded queue to prevent OOM from unbounded concurrent fetches
    if (uriDigestQueue.size >= MAX_URI_FETCH_QUEUE) {
      uriDigestDroppedCount++;
      if (uriDigestDroppedCount % 10 === 1) {
        logger.warn({ assetId: uriAssetId, queueSize: uriDigestQueue.size, dropped: uriDigestDroppedCount }, "URI digest queue full, dropping request");
      }
    } else {
      uriDigestQueue.add(async () => {
        try {
          await digestAndStoreUriMetadataLocal(prisma, uriAssetId!, uri!);
        } catch (err: any) {
          logger.warn({ assetId: uriAssetId, uri, error: err.message }, "Failed to digest URI metadata");
        }
      });
    }
  }

  if (event.type === "CollectionPointerSet" && config.collectionMetadataIndexEnabled) {
    const assetId = event.data.asset.toBase58();
    const pointer = event.data.col;
    if (collectionDigestQueue.size >= MAX_COLLECTION_FETCH_QUEUE) {
      collectionDigestDroppedCount++;
      if (collectionDigestDroppedCount % 10 === 1) {
        logger.warn(
          { assetId, queueSize: collectionDigestQueue.size, dropped: collectionDigestDroppedCount },
          "Collection digest queue full, dropping request"
        );
      }
      return;
    }

    collectionDigestQueue.add(async () => {
      try {
        await digestAndStoreCollectionMetadataLocal(prisma, assetId, pointer);
      } catch (err: any) {
        logger.warn({ assetId, col: pointer, error: err.message }, "Failed to digest collection metadata");
      }
    });
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
      await handleAgentRegisteredCore(tx, event.data, ctx);
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
      await handleValidationRequestedTx(tx, event.data, ctx);
      break;
    case "ValidationResponded":
      await handleValidationRespondedTx(tx, event.data, ctx);
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
      await handleValidationRequested(prisma, event.data, ctx);
      break;

    case "ValidationResponded":
      await handleValidationResponded(prisma, event.data, ctx);
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

  await client.agent.upsert({
    where: { id: assetId },
    create: {
      id: assetId,
      owner: data.owner.toBase58(),
      creator: data.owner.toBase58(),
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
      status: DEFAULT_STATUS,
    },
    update: {
      collection: collectionId,
      registry: collectionId, // v0.6.0: registry = collection
      atomEnabled: data.atomEnabled,
      uri: agentUri,
      creator: data.owner.toBase58(),
      txIndex: ctx.txIndex ?? null,
      eventOrdinal: ctx.eventOrdinal ?? null,
    },
  });

  logger.info({ assetId, owner: data.owner.toBase58(), uri: agentUri }, "Agent registered");
}

// Non-atomic wrapper with URI digest side effect
async function handleAgentRegistered(
  prisma: PrismaClient,
  data: AgentRegistered,
  ctx: EventContext
): Promise<void> {
  await handleAgentRegisteredCore(prisma, data, ctx);
  const assetId = data.asset.toBase58();
  const agentUri = data.agentUri || "";

  // Trigger URI metadata extraction if configured and URI is present
  // Uses bounded queue to prevent OOM from unbounded concurrent fetches
  if (agentUri && config.metadataIndexMode !== "off") {
    if (uriDigestQueue.size >= MAX_URI_FETCH_QUEUE) {
      uriDigestDroppedCount++;
      if (uriDigestDroppedCount % 10 === 1) {
        logger.warn({ assetId, queueSize: uriDigestQueue.size, dropped: uriDigestDroppedCount }, "URI digest queue full, dropping request");
      }
    } else {
      uriDigestQueue.add(async () => {
        try {
          await digestAndStoreUriMetadataLocal(prisma, assetId, agentUri);
        } catch (err: any) {
          logger.warn({ assetId, uri: agentUri, error: err.message }, "Failed to digest URI metadata");
        }
      });
    }
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
  // Uses bounded queue to prevent OOM from unbounded concurrent fetches
  if (newUri && config.metadataIndexMode !== "off") {
    if (uriDigestQueue.size >= MAX_URI_FETCH_QUEUE) {
      uriDigestDroppedCount++;
      if (uriDigestDroppedCount % 10 === 1) {
        logger.warn({ assetId, queueSize: uriDigestQueue.size, dropped: uriDigestDroppedCount }, "URI digest queue full, dropping request");
      }
    } else {
      uriDigestQueue.add(async () => {
        try {
          await digestAndStoreUriMetadataLocal(prisma, assetId, newUri);
        } catch (err: any) {
          logger.warn({ assetId, uri: newUri, error: err.message }, "Failed to digest URI metadata");
        }
      });
    }
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

  await tx.collection.upsert({
    where: { col_creator: { col: pointer, creator } },
    create: {
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
    },
    update: {
      lastSeenAt: ctx.blockTime,
      lastSeenSlot: ctx.slot,
      lastSeenTxSignature: ctx.signature,
    },
  });

  const result = await tx.agent.updateMany({
    where: { id: assetId },
    data: {
      collectionPointer: pointer,
      creator,
      updatedAt: ctx.blockTime,
    },
  });

  if (result.count > 0) {
    const previousPointer = existing?.collectionPointer ?? "";
    const previousCreator = existing?.creator ?? creator;
    if (previousPointer !== pointer) {
      if (previousPointer !== "") {
        await tx.collection.updateMany({
          where: {
            col: previousPointer,
            creator: previousCreator,
            assetCount: { gt: 0n },
          },
          data: {
            assetCount: { decrement: 1n },
          },
        });
      }

      await tx.collection.update({
        where: { col_creator: { col: pointer, creator } },
        data: {
          assetCount: { increment: 1n },
        },
      });
    }
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

  await prisma.collection.upsert({
    where: { col_creator: { col: pointer, creator } },
    create: {
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
    },
    update: {
      lastSeenAt: ctx.blockTime,
      lastSeenSlot: ctx.slot,
      lastSeenTxSignature: ctx.signature,
    },
  });

  const result = await prisma.agent.updateMany({
    where: { id: assetId },
    data: {
      collectionPointer: pointer,
      creator,
      updatedAt: ctx.blockTime,
    },
  });

  if (result.count > 0) {
    const previousPointer = existing?.collectionPointer ?? "";
    const previousCreator = existing?.creator ?? creator;
    if (previousPointer !== pointer) {
      if (previousPointer !== "") {
        await prisma.collection.updateMany({
          where: {
            col: previousPointer,
            creator: previousCreator,
            assetCount: { gt: 0n },
          },
          data: {
            assetCount: { decrement: 1n },
          },
        });
      }

      await prisma.collection.update({
        where: { col_creator: { col: pointer, creator } },
        data: {
          assetCount: { increment: 1n },
        },
      });
    }
  } else {
    logger.warn({ assetId }, "Agent not found for collection pointer set, event may be out of order");
  }

  logger.info({ assetId, col: pointer, setBy }, "Collection pointer set");

  if (config.collectionMetadataIndexEnabled) {
    if (collectionDigestQueue.size >= MAX_COLLECTION_FETCH_QUEUE) {
      collectionDigestDroppedCount++;
      if (collectionDigestDroppedCount % 10 === 1) {
        logger.warn(
          { assetId, queueSize: collectionDigestQueue.size, dropped: collectionDigestDroppedCount },
          "Collection digest queue full, dropping request"
        );
      }
      return;
    }

    collectionDigestQueue.add(async () => {
      try {
        await digestAndStoreCollectionMetadataLocal(prisma, assetId, pointer);
      } catch (err: any) {
        logger.warn({ assetId, col: pointer, error: err.message }, "Failed to digest collection metadata");
      }
    });
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
  const feedback = await tx.feedback.upsert({
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
      status: DEFAULT_STATUS,
    },
    update: {},
  });
  // Reconcile orphan responses
  const orphans = await tx.orphanResponse.findMany({
    where: { agentId: assetId, client: clientAddress, feedbackIndex: data.feedbackIndex },
  });
  for (const orphan of orphans) {
    await tx.feedbackResponse.upsert({
      where: {
        feedbackId_responder_txSignature: {
          feedbackId: feedback.id,
          responder: orphan.responder,
          txSignature: orphan.txSignature ?? "",
        },
      },
      create: {
        feedbackId: feedback.id,
        responder: orphan.responder,
        responseUri: orphan.responseUri,
        responseHash: orphan.responseHash,
        runningDigest: orphan.runningDigest,
        txSignature: orphan.txSignature,
        slot: orphan.slot,
        status: DEFAULT_STATUS,
      },
      update: {},
    });
    await tx.orphanResponse.delete({ where: { id: orphan.id } });
  }
  if (orphans.length > 0) {
    logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString(), count: orphans.length }, "Reconciled orphan responses");
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

  const feedback = await prisma.feedback.upsert({
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
      status: DEFAULT_STATUS,
    },
    update: {},
  });

  // Reconcile orphan responses
  const orphans = await prisma.orphanResponse.findMany({
    where: { agentId: assetId, client: clientAddress, feedbackIndex: data.feedbackIndex },
  });

  for (const orphan of orphans) {
    await prisma.feedbackResponse.upsert({
      where: {
        feedbackId_responder_txSignature: {
          feedbackId: feedback.id,
          responder: orphan.responder,
          txSignature: orphan.txSignature ?? "",
        },
      },
      create: {
        feedbackId: feedback.id,
        responder: orphan.responder,
        responseUri: orphan.responseUri,
        responseHash: orphan.responseHash,
        runningDigest: orphan.runningDigest,
        txSignature: orphan.txSignature,
        slot: orphan.slot,
        status: DEFAULT_STATUS,
      },
      update: {},
    });
    await prisma.orphanResponse.delete({ where: { id: orphan.id } });
  }

  if (orphans.length > 0) {
    logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString(), count: orphans.length }, "Reconciled orphan responses");
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

  await tx.feedback.updateMany({
    where: { agentId: assetId, client: clientAddress, feedbackIndex: data.feedbackIndex },
    data: { revoked: true, revokedTxSignature: ctx.signature, revokedSlot: ctx.slot },
  });

  const revokeStatus = !feedback || sealMismatch ? "ORPHANED" as const : DEFAULT_STATUS;
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
      status: revokeStatus,
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
    },
  });

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

  await prisma.feedback.updateMany({
    where: { agentId: assetId, client: clientAddress, feedbackIndex: data.feedbackIndex },
    data: { revoked: true, revokedTxSignature: ctx.signature, revokedSlot: ctx.slot },
  });

  const revokeStatus = !feedback || sealMismatch ? "ORPHANED" as const : DEFAULT_STATUS;
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
      status: revokeStatus,
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
    },
  });

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
        runningDigest: data.newResponseDigest ? Uint8Array.from(data.newResponseDigest) as Uint8Array<ArrayBuffer> : null,
        txSignature: ctx.signature,
        slot: ctx.slot,
      },
      update: {},
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
      status: responseStatus,
    },
    update: {},
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
        runningDigest: data.newResponseDigest ? Uint8Array.from(data.newResponseDigest) as Uint8Array<ArrayBuffer> : null,
        txSignature: ctx.signature,
        slot: ctx.slot,
      },
      update: {},
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
      status: responseStatus,
    },
    update: {},
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
  uri: string
): Promise<void> {
  if (config.metadataIndexMode === "off") {
    return;
  }

  // RACE CONDITION CHECK: Verify URI hasn't changed while we were queued/fetching
  // This prevents stale data from overwriting newer data due to out-of-order completion
  const agent = await prisma.agent.findUnique({
    where: { id: assetId },
    select: { uri: true },
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
    select: { uri: true },
  });
  if (!agentRecheck || agentRecheck.uri !== uri) {
    logger.debug({ assetId, uri }, "URI changed during fetch, discarding stale results");
    return;
  }

  // Purge old URI-derived metadata before storing new ones
  try {
    await prisma.agentMetadata.deleteMany({
      where: {
        agentId: assetId,
        key: { startsWith: "_uri:" },
      },
    });
    logger.debug({ assetId }, "Purged old URI metadata");
  } catch (error: any) {
    logger.warn({ assetId, error: error.message }, "Failed to purge old URI metadata");
  }

  if (result.status !== "ok" || !result.fields) {
    logger.debug({ assetId, uri, status: result.status, error: result.error }, "URI digest failed or empty");
    // Store error status as metadata
    await storeUriMetadataLocal(prisma, assetId, "_uri:_status", JSON.stringify({
      status: result.status,
      error: result.error,
      bytes: result.bytes,
      hash: result.hash,
    }));
    return;
  }

  // Store each extracted field
  const maxValueBytes = config.metadataMaxValueBytes;
  for (const [key, value] of Object.entries(result.fields)) {
    const serialized = serializeValue(value, maxValueBytes);

    if (serialized.oversize) {
      // Store metadata about oversize field
      await storeUriMetadataLocal(prisma, assetId, `${key}_meta`, JSON.stringify({
        status: "oversize",
        bytes: serialized.bytes,
        sha256: result.hash,
      }));
    } else {
      await storeUriMetadataLocal(prisma, assetId, key, serialized.value);
    }
  }

  // Store success status with truncation info
  await storeUriMetadataLocal(prisma, assetId, "_uri:_status", JSON.stringify({
    status: "ok",
    bytes: result.bytes,
    hash: result.hash,
    fieldCount: Object.keys(result.fields).length,
    truncatedKeys: result.truncatedKeys || false,
  }));

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
  const baseUpdate = {
    metadataUpdatedAt: new Date(),
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
  value: string
): Promise<void> {
  try {
    // Apply compression parity with Supabase mode
    const shouldCompress = !STANDARD_URI_FIELDS.has(key);
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
      },
      update: {
        value: storedValue,
      },
    });
  } catch (error: any) {
    logger.error({ error: error.message, assetId, key }, "Failed to store URI metadata");
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
  const cutoff = new Date(Date.now() - maxAgeMinutes * 60 * 1000);

  const result = await prisma.orphanResponse.deleteMany({
    where: { createdAt: { lt: cutoff } },
  });

  if (result.count > 0) {
    logger.info({ deleted: result.count, maxAgeMinutes }, "Cleaned up old orphan responses");
  }

  return result.count;
}
