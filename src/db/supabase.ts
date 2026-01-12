/**
 * Supabase database handlers for production mode
 * Writes events directly to Supabase PostgreSQL
 */

import { createClient, SupabaseClient } from "@supabase/supabase-js";
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

const logger = createChildLogger("supabase-handlers");

export interface EventContext {
  signature: string;
  slot: bigint;
  blockTime: Date;
}

let supabaseClient: SupabaseClient | null = null;
const seenCollections = new Set<string>();

function getSupabase(): SupabaseClient {
  if (!supabaseClient) {
    if (!config.supabaseUrl || !config.supabaseKey) {
      throw new Error("Supabase URL and Key required");
    }
    supabaseClient = createClient(config.supabaseUrl, config.supabaseKey);
  }
  return supabaseClient;
}

export async function handleEvent(
  event: ProgramEvent,
  ctx: EventContext
): Promise<void> {
  switch (event.type) {
    case "AgentRegisteredInRegistry":
      await handleAgentRegistered(event.data, ctx);
      break;

    case "AgentOwnerSynced":
      await handleAgentOwnerSynced(event.data, ctx);
      break;

    case "UriUpdated":
      await handleUriUpdated(event.data, ctx);
      break;

    case "WalletUpdated":
      await handleWalletUpdated(event.data, ctx);
      break;

    case "MetadataSet":
      await handleMetadataSet(event.data, ctx);
      break;

    case "MetadataDeleted":
      await handleMetadataDeleted(event.data, ctx);
      break;

    case "BaseRegistryCreated":
      await handleBaseRegistryCreated(event.data, ctx);
      break;

    case "UserRegistryCreated":
      await handleUserRegistryCreated(event.data, ctx);
      break;

    case "BaseRegistryRotated":
      logger.info({ event: event.data }, "Base registry rotated");
      break;

    case "NewFeedback":
      await handleNewFeedback(event.data, ctx);
      break;

    case "FeedbackRevoked":
      await handleFeedbackRevoked(event.data, ctx);
      break;

    case "ResponseAppended":
      await handleResponseAppended(event.data, ctx);
      break;

    case "ValidationRequested":
      await handleValidationRequested(event.data, ctx);
      break;

    case "ValidationResponded":
      await handleValidationResponded(event.data, ctx);
      break;

    default:
      logger.warn({ event }, "Unhandled event type");
  }
}

async function ensureCollection(collection: string): Promise<void> {
  if (seenCollections.has(collection)) return;
  seenCollections.add(collection);

  const supabase = getSupabase();
  const { error } = await supabase.from("collections").upsert(
    {
      collection,
      registry_type: "BASE",
      created_at: new Date().toISOString(),
    },
    { onConflict: "collection" }
  );

  if (error) {
    logger.error({ error, collection }, "Failed to ensure collection");
  }
}

async function handleAgentRegistered(
  data: AgentRegisteredInRegistry,
  ctx: EventContext
): Promise<void> {
  const supabase = getSupabase();
  const assetId = data.asset.toBase58();
  const collection = data.collection.toBase58();

  // Ensure collection exists
  await ensureCollection(collection);

  const { error } = await supabase.from("agents").upsert({
    asset: assetId,
    owner: data.owner.toBase58(),
    agent_uri: null,
    collection,
    block_slot: Number(ctx.slot),
    tx_signature: ctx.signature,
    created_at: ctx.blockTime.toISOString(),
  });

  if (error) {
    logger.error({ error, assetId }, "Failed to register agent");
  } else {
    logger.info({ assetId, owner: data.owner.toBase58() }, "Agent registered");
  }
}

async function handleAgentOwnerSynced(
  data: AgentOwnerSynced,
  ctx: EventContext
): Promise<void> {
  const supabase = getSupabase();
  const assetId = data.asset.toBase58();

  const { error } = await supabase
    .from("agents")
    .update({
      owner: data.newOwner.toBase58(),
      block_slot: Number(ctx.slot),
      updated_at: ctx.blockTime.toISOString(),
    })
    .eq("asset", assetId);

  if (error) {
    logger.error({ error, assetId }, "Failed to sync owner");
  } else {
    logger.info(
      {
        assetId,
        oldOwner: data.oldOwner.toBase58(),
        newOwner: data.newOwner.toBase58(),
      },
      "Agent owner synced"
    );
  }
}

async function handleUriUpdated(
  data: UriUpdated,
  ctx: EventContext
): Promise<void> {
  const supabase = getSupabase();
  const assetId = data.asset.toBase58();

  const { error } = await supabase
    .from("agents")
    .update({
      agent_uri: data.newUri,
      block_slot: Number(ctx.slot),
      updated_at: ctx.blockTime.toISOString(),
    })
    .eq("asset", assetId);

  if (error) {
    logger.error({ error, assetId }, "Failed to update URI");
  } else {
    logger.info({ assetId, newUri: data.newUri }, "Agent URI updated");
  }
}

async function handleWalletUpdated(
  data: WalletUpdated,
  ctx: EventContext
): Promise<void> {
  const supabase = getSupabase();
  const assetId = data.asset.toBase58();

  const { error } = await supabase
    .from("agents")
    .update({
      agent_wallet: data.newWallet.toBase58(),
      block_slot: Number(ctx.slot),
      updated_at: ctx.blockTime.toISOString(),
    })
    .eq("asset", assetId);

  if (error) {
    logger.error({ error, assetId }, "Failed to update wallet");
  } else {
    logger.info(
      { assetId, newWallet: data.newWallet.toBase58() },
      "Agent wallet updated"
    );
  }
}

async function handleMetadataSet(
  data: MetadataSet,
  ctx: EventContext
): Promise<void> {
  const supabase = getSupabase();
  const assetId = data.asset.toBase58();
  const keyHash = Buffer.from(data.value).slice(0, 16).toString("hex");
  const id = `${assetId}:${keyHash}`;

  const { error } = await supabase.from("metadata").upsert({
    id,
    asset: assetId,
    key: data.key,
    key_hash: keyHash,
    value: Buffer.from(data.value).toString("base64"),
    immutable: data.immutable,
    block_slot: Number(ctx.slot),
    tx_signature: ctx.signature,
    updated_at: ctx.blockTime.toISOString(),
  });

  if (error) {
    logger.error({ error, assetId, key: data.key }, "Failed to set metadata");
  } else {
    logger.info({ assetId, key: data.key }, "Metadata set");
  }
}

async function handleMetadataDeleted(
  data: MetadataDeleted,
  _ctx: EventContext
): Promise<void> {
  const supabase = getSupabase();
  const assetId = data.asset.toBase58();

  // Delete all metadata entries for this asset+key
  const { error } = await supabase
    .from("metadata")
    .delete()
    .eq("asset", assetId)
    .eq("key", data.key);

  if (error) {
    logger.error(
      { error, assetId, key: data.key },
      "Failed to delete metadata"
    );
  } else {
    logger.info({ assetId, key: data.key }, "Metadata deleted");
  }
}

async function handleBaseRegistryCreated(
  data: BaseRegistryCreated,
  ctx: EventContext
): Promise<void> {
  const supabase = getSupabase();
  const collection = data.collection.toBase58();

  const { error } = await supabase.from("collections").upsert(
    {
      collection,
      registry_type: "BASE",
      authority: data.createdBy.toBase58(),
      base_index: data.baseIndex,
      created_at: ctx.blockTime.toISOString(),
    },
    { onConflict: "collection" }
  );

  if (error) {
    logger.error({ error, collection }, "Failed to create base registry");
  } else {
    logger.info(
      { registryId: data.registry.toBase58(), baseIndex: data.baseIndex },
      "Base registry created"
    );
  }
}

async function handleUserRegistryCreated(
  data: UserRegistryCreated,
  ctx: EventContext
): Promise<void> {
  const supabase = getSupabase();
  const collection = data.collection.toBase58();

  const { error } = await supabase.from("collections").upsert(
    {
      collection,
      registry_type: "USER",
      authority: data.owner.toBase58(),
      created_at: ctx.blockTime.toISOString(),
    },
    { onConflict: "collection" }
  );

  if (error) {
    logger.error({ error, collection }, "Failed to create user registry");
  } else {
    logger.info(
      { registryId: data.registry.toBase58(), owner: data.owner.toBase58() },
      "User registry created"
    );
  }
}

async function handleNewFeedback(
  data: NewFeedback,
  ctx: EventContext
): Promise<void> {
  const supabase = getSupabase();
  const assetId = data.asset.toBase58();
  const clientAddress = data.clientAddress.toBase58();
  const id = `${assetId}:${clientAddress}:${data.feedbackIndex}`;

  const { error } = await supabase.from("feedbacks").upsert({
    id,
    asset: assetId,
    client_address: clientAddress,
    feedback_index: Number(data.feedbackIndex),
    score: data.score,
    tag1: data.tag1 || null,
    tag2: data.tag2 || null,
    endpoint: data.endpoint || null,
    feedback_uri: data.feedbackUri || null,
    feedback_hash: data.feedbackHash
      ? Buffer.from(data.feedbackHash).toString("hex")
      : null,
    // ATOM enriched fields (v0.4.0)
    new_trust_tier: data.newTrustTier,
    new_quality_score: data.newQualityScore,
    new_confidence: data.newConfidence,
    new_risk_score: data.newRiskScore,
    new_diversity_ratio: data.newDiversityRatio,
    is_unique_client: data.isUniqueClient,
    is_revoked: false,
    block_slot: Number(ctx.slot),
    tx_signature: ctx.signature,
    created_at: ctx.blockTime.toISOString(),
  });

  if (error) {
    logger.error({ error, assetId, feedbackIndex: data.feedbackIndex }, "Failed to save feedback");
  } else {
    logger.info(
      {
        assetId,
        feedbackIndex: data.feedbackIndex.toString(),
        score: data.score,
      },
      "New feedback"
    );
  }
}

async function handleFeedbackRevoked(
  data: FeedbackRevoked,
  ctx: EventContext
): Promise<void> {
  const supabase = getSupabase();
  const assetId = data.asset.toBase58();
  const clientAddress = data.clientAddress.toBase58();
  const id = `${assetId}:${clientAddress}:${data.feedbackIndex}`;

  const { error } = await supabase
    .from("feedbacks")
    .update({
      is_revoked: true,
      revoked_at: ctx.blockTime.toISOString(),
      // ATOM enriched fields (v0.4.0) - final stats after revoke
      revoke_original_score: data.originalScore,
      revoke_had_impact: data.hadImpact,
      revoke_new_trust_tier: data.newTrustTier,
      revoke_new_quality_score: data.newQualityScore,
      revoke_new_confidence: data.newConfidence,
    })
    .eq("id", id);

  if (error) {
    logger.error({ error, assetId, feedbackIndex: data.feedbackIndex }, "Failed to revoke feedback");
  } else {
    logger.info(
      { assetId, feedbackIndex: data.feedbackIndex.toString() },
      "Feedback revoked"
    );
  }
}

async function handleResponseAppended(
  data: ResponseAppended,
  ctx: EventContext
): Promise<void> {
  const supabase = getSupabase();
  const assetId = data.asset.toBase58();
  const responder = data.responder.toBase58();
  const id = `${assetId}:${data.feedbackIndex}:${responder}`;

  const { error } = await supabase.from("feedback_responses").upsert({
    id,
    asset: assetId,
    feedback_index: Number(data.feedbackIndex),
    responder,
    response_uri: data.responseUri || null,
    response_hash: data.responseHash
      ? Buffer.from(data.responseHash).toString("hex")
      : null,
    block_slot: Number(ctx.slot),
    tx_signature: ctx.signature,
    created_at: ctx.blockTime.toISOString(),
  });

  if (error) {
    logger.error({ error, assetId, feedbackIndex: data.feedbackIndex }, "Failed to append response");
  } else {
    logger.info(
      { assetId, feedbackIndex: data.feedbackIndex.toString() },
      "Response appended"
    );
  }
}

async function handleValidationRequested(
  data: ValidationRequested,
  ctx: EventContext
): Promise<void> {
  const supabase = getSupabase();
  const assetId = data.asset.toBase58();
  const validatorAddress = data.validatorAddress.toBase58();
  const id = `${assetId}:${validatorAddress}:${data.nonce}`;

  const { error } = await supabase.from("validations").upsert({
    id,
    asset: assetId,
    validator_address: validatorAddress,
    nonce: data.nonce,
    requester: data.requester.toBase58(),
    request_uri: data.requestUri || null,
    request_hash: data.requestHash
      ? Buffer.from(data.requestHash).toString("hex")
      : null,
    status: "PENDING",
    block_slot: Number(ctx.slot),
    tx_signature: ctx.signature,
    created_at: ctx.blockTime.toISOString(),
  });

  if (error) {
    logger.error({ error, assetId, nonce: data.nonce }, "Failed to request validation");
  } else {
    logger.info(
      {
        assetId,
        validator: validatorAddress,
        nonce: data.nonce,
      },
      "Validation requested"
    );
  }
}

async function handleValidationResponded(
  data: ValidationResponded,
  ctx: EventContext
): Promise<void> {
  const supabase = getSupabase();
  const assetId = data.asset.toBase58();
  const validatorAddress = data.validatorAddress.toBase58();
  const id = `${assetId}:${validatorAddress}:${data.nonce}`;

  const { error } = await supabase
    .from("validations")
    .update({
      response: data.response,
      response_uri: data.responseUri || null,
      response_hash: data.responseHash
        ? Buffer.from(data.responseHash).toString("hex")
        : null,
      tag: data.tag || null,
      status: "RESPONDED",
      updated_at: ctx.blockTime.toISOString(),
    })
    .eq("id", id);

  if (error) {
    logger.error({ error, assetId, nonce: data.nonce }, "Failed to respond to validation");
  } else {
    logger.info(
      {
        assetId,
        validator: validatorAddress,
        nonce: data.nonce,
        response: data.response,
      },
      "Validation responded"
    );
  }
}
