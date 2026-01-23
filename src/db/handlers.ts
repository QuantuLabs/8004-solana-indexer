import { PrismaClient } from "@prisma/client";
import {
  ProgramEvent,
  AgentRegisteredInRegistry,
  AtomEnabled,
  AgentOwnerSynced,
  UriUpdated,
  WalletUpdated,
  MetadataSet,
  MetadataDeleted,
  BaseRegistryCreated,
  UserRegistryCreated,
  NewFeedback,
  FeedbackRevoked,
  ResponseAppended,
  ValidationRequested,
  ValidationResponded,
} from "../parser/types.js";
import { createChildLogger } from "../logger.js";
import { config } from "../config.js";
import * as supabaseHandlers from "./supabase.js";
import { digestUri, serializeValue } from "../indexer/uriDigest.js";
import { compressForStorage } from "../utils/compression.js";

const logger = createChildLogger("db-handlers");

// Standard URI fields - never compressed for fast reads (parity with supabase.ts)
const STANDARD_URI_FIELDS = new Set([
  "uri:type",
  "uri:name",
  "uri:description",
  "uri:image",
  "uri:endpoints",
  "uri:registrations",
  "uri:supported_trusts",
  "uri:active",
  "uri:x402_support",
  "uri:skills",
  "uri:domains",
  "uri:status",
]);

export interface EventContext {
  signature: string;
  slot: bigint;
  blockTime: Date;
  txIndex?: number; // Transaction index within the block (for deterministic ordering)
}

/**
 * Dual-mode event handler
 * Routes to Supabase or Prisma (SQLite) based on DB_MODE config
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
    case "AgentRegisteredInRegistry":
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

    case "MetadataSet":
      await handleMetadataSet(prisma, event.data, ctx);
      break;

    case "MetadataDeleted":
      await handleMetadataDeleted(prisma, event.data, ctx);
      break;

    case "BaseRegistryCreated":
      await handleBaseRegistryCreated(prisma, event.data, ctx);
      break;

    case "UserRegistryCreated":
      await handleUserRegistryCreated(prisma, event.data, ctx);
      break;

    case "BaseRegistryRotated":
      // Just log for now - could update a state table
      logger.info({ event: event.data }, "Base registry rotated");
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

async function handleAgentRegistered(
  prisma: PrismaClient,
  data: AgentRegisteredInRegistry,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const agentUri = data.agentUri || "";

  await prisma.agent.upsert({
    where: { id: assetId },
    create: {
      id: assetId,
      owner: data.owner.toBase58(),
      uri: agentUri,
      nftName: "",
      collection: data.collection.toBase58(),
      registry: data.registry.toBase58(),
      atomEnabled: data.atomEnabled,
      createdTxSignature: ctx.signature,
      createdSlot: ctx.slot,
    },
    update: {
      collection: data.collection.toBase58(),
      registry: data.registry.toBase58(),
      atomEnabled: data.atomEnabled,
      uri: agentUri,
    },
  });

  logger.info({ assetId, owner: data.owner.toBase58(), uri: agentUri }, "Agent registered");

  // Trigger URI metadata extraction if configured and URI is present
  if (agentUri && config.metadataIndexMode !== "off") {
    digestAndStoreUriMetadataLocal(prisma, assetId, agentUri).catch((err) => {
      logger.warn({ assetId, uri: agentUri, error: err.message }, "Failed to digest URI metadata");
    });
  }
}

async function handleAgentOwnerSynced(
  prisma: PrismaClient,
  data: AgentOwnerSynced,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();

  await prisma.agent.update({
    where: { id: assetId },
    data: {
      owner: data.newOwner.toBase58(),
      updatedAt: ctx.blockTime,
    },
  });

  logger.info(
    {
      assetId,
      oldOwner: data.oldOwner.toBase58(),
      newOwner: data.newOwner.toBase58(),
    },
    "Agent owner synced"
  );
}

async function handleAtomEnabled(
  prisma: PrismaClient,
  data: AtomEnabled,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();

  await prisma.agent.update({
    where: { id: assetId },
    data: {
      atomEnabled: true,
      updatedAt: ctx.blockTime,
    },
  });

  logger.info({ assetId, enabledBy: data.enabledBy.toBase58() }, "ATOM enabled");
}

async function handleUriUpdated(
  prisma: PrismaClient,
  data: UriUpdated,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const newUri = data.newUri || "";

  await prisma.agent.update({
    where: { id: assetId },
    data: {
      uri: newUri,
      updatedAt: ctx.blockTime,
    },
  });

  logger.info({ assetId, newUri }, "Agent URI updated");

  // Trigger URI metadata extraction if configured and URI is present
  if (newUri && config.metadataIndexMode !== "off") {
    digestAndStoreUriMetadataLocal(prisma, assetId, newUri).catch((err) => {
      logger.warn({ assetId, uri: newUri, error: err.message }, "Failed to digest URI metadata");
    });
  }
}

async function handleWalletUpdated(
  prisma: PrismaClient,
  data: WalletUpdated,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();

  await prisma.agent.update({
    where: { id: assetId },
    data: {
      wallet: data.newWallet.toBase58(),
      updatedAt: ctx.blockTime,
    },
  });

  logger.info(
    { assetId, newWallet: data.newWallet.toBase58() },
    "Agent wallet updated"
  );
}

async function handleMetadataSet(
  prisma: PrismaClient,
  data: MetadataSet,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();

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
      value: Buffer.from(data.value),
      immutable: data.immutable,
      txSignature: ctx.signature,
      slot: ctx.slot,
    },
    update: {
      value: Buffer.from(data.value),
      // Immutable flag stays true once set
      txSignature: ctx.signature,
      slot: ctx.slot,
    },
  });

  logger.info({ assetId, key: data.key }, "Metadata set");
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

async function handleBaseRegistryCreated(
  prisma: PrismaClient,
  data: BaseRegistryCreated,
  ctx: EventContext
): Promise<void> {
  await prisma.registry.upsert({
    where: { id: data.registry.toBase58() },
    create: {
      id: data.registry.toBase58(),
      collection: data.collection.toBase58(),
      registryType: "Base",
      authority: data.createdBy.toBase58(),
      baseIndex: data.baseIndex,
      txSignature: ctx.signature,
      slot: ctx.slot,
    },
    update: {},
  });

  logger.info(
    { registryId: data.registry.toBase58(), baseIndex: data.baseIndex },
    "Base registry created"
  );
}

async function handleUserRegistryCreated(
  prisma: PrismaClient,
  data: UserRegistryCreated,
  ctx: EventContext
): Promise<void> {
  await prisma.registry.upsert({
    where: { id: data.registry.toBase58() },
    create: {
      id: data.registry.toBase58(),
      collection: data.collection.toBase58(),
      registryType: "User",
      authority: data.owner.toBase58(),
      txSignature: ctx.signature,
      slot: ctx.slot,
    },
    update: {},
  });

  logger.info(
    { registryId: data.registry.toBase58(), owner: data.owner.toBase58() },
    "User registry created"
  );
}

async function handleNewFeedback(
  prisma: PrismaClient,
  data: NewFeedback,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();

  await prisma.feedback.upsert({
    where: {
      agentId_client_feedbackIndex: {
        agentId: assetId,
        client: data.clientAddress.toBase58(),
        feedbackIndex: data.feedbackIndex,
      },
    },
    create: {
      agentId: assetId,
      client: data.clientAddress.toBase58(),
      feedbackIndex: data.feedbackIndex,
      score: data.score,
      tag1: data.tag1,
      tag2: data.tag2,
      endpoint: data.endpoint,
      feedbackUri: data.feedbackUri,
      feedbackHash: Buffer.from(data.feedbackHash),
      createdTxSignature: ctx.signature,
      createdSlot: ctx.slot,
    },
    update: {},
  });

  logger.info(
    {
      assetId,
      feedbackIndex: data.feedbackIndex.toString(),
      score: data.score,
    },
    "New feedback"
  );
}

async function handleFeedbackRevoked(
  prisma: PrismaClient,
  data: FeedbackRevoked,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();

  await prisma.feedback.updateMany({
    where: {
      agentId: assetId,
      client: data.clientAddress.toBase58(),
      feedbackIndex: data.feedbackIndex,
    },
    data: {
      revoked: true,
      revokedTxSignature: ctx.signature,
      revokedSlot: ctx.slot,
    },
  });

  logger.info(
    { assetId, feedbackIndex: data.feedbackIndex.toString() },
    "Feedback revoked"
  );
}

async function handleResponseAppended(
  prisma: PrismaClient,
  data: ResponseAppended,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const clientAddress = data.client.toBase58();

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
      "Feedback not found for response"
    );
    return;
  }

  // Multiple responses per responder allowed (ERC-8004)
  // Use upsert with txSignature to avoid duplicates during re-indexing
  await prisma.feedbackResponse.upsert({
    where: {
      feedbackId_responder_txSignature: {
        feedbackId: feedback.id,
        responder: data.responder.toBase58(),
        txSignature: ctx.signature,
      },
    },
    create: {
      feedbackId: feedback.id,
      responder: data.responder.toBase58(),
      responseUri: data.responseUri,
      responseHash: Buffer.from(data.responseHash),
      txSignature: ctx.signature,
      slot: ctx.slot,
    },
    update: {},
  });

  logger.info(
    { assetId, feedbackIndex: data.feedbackIndex.toString() },
    "Response appended"
  );
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
      requestHash: Buffer.from(data.requestHash),
      requestTxSignature: ctx.signature,
      requestSlot: ctx.slot,
    },
    update: {},
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

async function handleValidationResponded(
  prisma: PrismaClient,
  data: ValidationResponded,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();

  await prisma.validation.updateMany({
    where: {
      agentId: assetId,
      validator: data.validatorAddress.toBase58(),
      nonce: data.nonce,
    },
    data: {
      response: data.response,
      responseUri: data.responseUri,
      responseHash: Buffer.from(data.responseHash),
      tag: data.tag,
      respondedAt: ctx.blockTime,
      responseTxSignature: ctx.signature,
      responseSlot: ctx.slot,
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
 */
async function digestAndStoreUriMetadataLocal(
  prisma: PrismaClient,
  assetId: string,
  uri: string
): Promise<void> {
  if (config.metadataIndexMode === "off") {
    return;
  }

  const result = await digestUri(uri);

  if (result.status !== "ok" || !result.fields) {
    logger.debug({ assetId, uri, status: result.status, error: result.error }, "URI digest failed or empty");
    // Store error status as metadata
    await storeUriMetadataLocal(prisma, assetId, "uri:status", JSON.stringify({
      status: result.status,
      error: result.error,
      bytes: result.bytes,
      hash: result.hash,
    }));
    return;
  }

  // Store each extracted field
  const maxValueBytes = 10000; // 10KB max per field
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

  // Store success status
  await storeUriMetadataLocal(prisma, assetId, "uri:status", JSON.stringify({
    status: "ok",
    bytes: result.bytes,
    hash: result.hash,
    fieldCount: Object.keys(result.fields).length,
  }));

  logger.info({ assetId, uri, fieldCount: Object.keys(result.fields).length }, "URI metadata indexed");
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
      },
      update: {
        value: storedValue,
      },
    });
  } catch (error: any) {
    logger.error({ error: error.message, assetId, key }, "Failed to store URI metadata");
  }
}
