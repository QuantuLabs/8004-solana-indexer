import { PrismaClient } from "@prisma/client";
import {
  ProgramEvent,
  AgentRegisteredInRegistry,
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

const logger = createChildLogger("db-handlers");

export interface EventContext {
  signature: string;
  slot: bigint;
  blockTime: Date;
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

  await prisma.agent.upsert({
    where: { id: assetId },
    create: {
      id: assetId,
      owner: data.owner.toBase58(),
      uri: "",
      nftName: "",
      collection: data.collection.toBase58(),
      registry: data.registry.toBase58(),
      createdTxSignature: ctx.signature,
      createdSlot: ctx.slot,
    },
    update: {
      collection: data.collection.toBase58(),
      registry: data.registry.toBase58(),
    },
  });

  logger.info({ assetId, owner: data.owner.toBase58() }, "Agent registered");
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

async function handleUriUpdated(
  prisma: PrismaClient,
  data: UriUpdated,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();

  await prisma.agent.update({
    where: { id: assetId },
    data: {
      uri: data.newUri,
      updatedAt: ctx.blockTime,
    },
  });

  logger.info({ assetId, newUri: data.newUri }, "Agent URI updated");
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
      agentId_feedbackIndex: {
        agentId: assetId,
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

  const feedback = await prisma.feedback.findUnique({
    where: {
      agentId_feedbackIndex: {
        agentId: assetId,
        feedbackIndex: data.feedbackIndex,
      },
    },
  });

  if (!feedback) {
    logger.warn(
      { assetId, feedbackIndex: data.feedbackIndex.toString() },
      "Feedback not found for response"
    );
    return;
  }

  await prisma.feedbackResponse.create({
    data: {
      feedbackId: feedback.id,
      responder: data.responder.toBase58(),
      responseUri: data.responseUri,
      responseHash: Buffer.from(data.responseHash),
      txSignature: ctx.signature,
      slot: ctx.slot,
    },
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
