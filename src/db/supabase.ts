/**
 * Supabase database handlers for production mode
 * Writes events directly to Supabase PostgreSQL via pg client
 */

import { Pool } from "pg";
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

let pool: Pool | null = null;
const seenCollections = new Set<string>();

function getPool(): Pool {
  if (!pool) {
    if (!config.supabaseDsn) {
      throw new Error("SUPABASE_DSN required for supabase mode");
    }
    pool = new Pool({
      connectionString: config.supabaseDsn,
      ssl: { rejectUnauthorized: false },
      max: 10,
    });
  }
  return pool;
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

  const db = getPool();
  try {
    await db.query(
      `INSERT INTO collections (collection, registry_type, created_at)
       VALUES ($1, $2, $3)
       ON CONFLICT (collection) DO NOTHING`,
      [collection, "BASE", new Date().toISOString()]
    );
  } catch (error: any) {
    logger.error({ error: error.message, collection }, "Failed to ensure collection");
  }
}

async function handleAgentRegistered(
  data: AgentRegisteredInRegistry,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const assetId = data.asset.toBase58();
  const collection = data.collection.toBase58();

  await ensureCollection(collection);

  try {
    await db.query(
      `INSERT INTO agents (asset, owner, agent_uri, collection, block_slot, tx_signature, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (asset) DO UPDATE SET
         owner = EXCLUDED.owner,
         block_slot = EXCLUDED.block_slot,
         updated_at = EXCLUDED.created_at`,
      [assetId, data.owner.toBase58(), null, collection, Number(ctx.slot), ctx.signature, ctx.blockTime.toISOString()]
    );
    logger.info({ assetId, owner: data.owner.toBase58() }, "Agent registered");
  } catch (error: any) {
    logger.error({ error: error.message, assetId }, "Failed to register agent");
  }
}

async function handleAgentOwnerSynced(
  data: AgentOwnerSynced,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const assetId = data.asset.toBase58();

  try {
    await db.query(
      `UPDATE agents SET owner = $1, block_slot = $2, updated_at = $3 WHERE asset = $4`,
      [data.newOwner.toBase58(), Number(ctx.slot), ctx.blockTime.toISOString(), assetId]
    );
    logger.info({ assetId, oldOwner: data.oldOwner.toBase58(), newOwner: data.newOwner.toBase58() }, "Agent owner synced");
  } catch (error: any) {
    logger.error({ error: error.message, assetId }, "Failed to sync owner");
  }
}

async function handleUriUpdated(
  data: UriUpdated,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const assetId = data.asset.toBase58();

  try {
    await db.query(
      `UPDATE agents SET agent_uri = $1, block_slot = $2, updated_at = $3 WHERE asset = $4`,
      [data.newUri, Number(ctx.slot), ctx.blockTime.toISOString(), assetId]
    );
    logger.info({ assetId, newUri: data.newUri }, "Agent URI updated");
  } catch (error: any) {
    logger.error({ error: error.message, assetId }, "Failed to update URI");
  }
}

async function handleWalletUpdated(
  data: WalletUpdated,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const assetId = data.asset.toBase58();

  try {
    await db.query(
      `UPDATE agents SET agent_wallet = $1, block_slot = $2, updated_at = $3 WHERE asset = $4`,
      [data.newWallet.toBase58(), Number(ctx.slot), ctx.blockTime.toISOString(), assetId]
    );
    logger.info({ assetId, newWallet: data.newWallet.toBase58() }, "Agent wallet updated");
  } catch (error: any) {
    logger.error({ error: error.message, assetId }, "Failed to update wallet");
  }
}

async function handleMetadataSet(
  data: MetadataSet,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const assetId = data.asset.toBase58();
  const keyHash = Buffer.from(data.value).slice(0, 16).toString("hex");
  const id = `${assetId}:${keyHash}`;

  try {
    await db.query(
      `INSERT INTO metadata (id, asset, key, key_hash, value, immutable, block_slot, tx_signature, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       ON CONFLICT (id) DO UPDATE SET
         value = EXCLUDED.value,
         block_slot = EXCLUDED.block_slot,
         updated_at = EXCLUDED.updated_at`,
      [id, assetId, data.key, keyHash, Buffer.from(data.value).toString("base64"), data.immutable, Number(ctx.slot), ctx.signature, ctx.blockTime.toISOString()]
    );
    logger.info({ assetId, key: data.key }, "Metadata set");
  } catch (error: any) {
    logger.error({ error: error.message, assetId, key: data.key }, "Failed to set metadata");
  }
}

async function handleMetadataDeleted(
  data: MetadataDeleted,
  _ctx: EventContext
): Promise<void> {
  const db = getPool();
  const assetId = data.asset.toBase58();

  try {
    await db.query(`DELETE FROM metadata WHERE asset = $1 AND key = $2`, [assetId, data.key]);
    logger.info({ assetId, key: data.key }, "Metadata deleted");
  } catch (error: any) {
    logger.error({ error: error.message, assetId, key: data.key }, "Failed to delete metadata");
  }
}

async function handleBaseRegistryCreated(
  data: BaseRegistryCreated,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const collection = data.collection.toBase58();

  try {
    await db.query(
      `INSERT INTO collections (collection, registry_type, authority, base_index, created_at)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (collection) DO UPDATE SET
         authority = EXCLUDED.authority,
         base_index = EXCLUDED.base_index`,
      [collection, "BASE", data.createdBy.toBase58(), data.baseIndex, ctx.blockTime.toISOString()]
    );
    logger.info({ registryId: data.registry.toBase58(), baseIndex: data.baseIndex }, "Base registry created");
  } catch (error: any) {
    logger.error({ error: error.message, collection }, "Failed to create base registry");
  }
}

async function handleUserRegistryCreated(
  data: UserRegistryCreated,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const collection = data.collection.toBase58();

  try {
    await db.query(
      `INSERT INTO collections (collection, registry_type, authority, created_at)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (collection) DO UPDATE SET
         authority = EXCLUDED.authority`,
      [collection, "USER", data.owner.toBase58(), ctx.blockTime.toISOString()]
    );
    logger.info({ registryId: data.registry.toBase58(), owner: data.owner.toBase58() }, "User registry created");
  } catch (error: any) {
    logger.error({ error: error.message, collection }, "Failed to create user registry");
  }
}

async function handleNewFeedback(
  data: NewFeedback,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const assetId = data.asset.toBase58();
  const clientAddress = data.clientAddress.toBase58();
  const id = `${assetId}:${clientAddress}:${data.feedbackIndex}`;

  try {
    // Insert feedback record (ATOM stats go in agents table, not here)
    await db.query(
      `INSERT INTO feedbacks (id, asset, client_address, feedback_index, score, tag1, tag2, endpoint, feedback_uri, feedback_hash,
         is_revoked, block_slot, tx_signature, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
       ON CONFLICT (id) DO UPDATE SET
         score = EXCLUDED.score,
         block_slot = EXCLUDED.block_slot`,
      [
        id, assetId, clientAddress, Number(data.feedbackIndex), data.score,
        data.tag1 || null, data.tag2 || null, data.endpoint || null, data.feedbackUri || null,
        data.feedbackHash ? Buffer.from(data.feedbackHash).toString("hex") : null,
        false, Number(ctx.slot), ctx.signature, ctx.blockTime.toISOString()
      ]
    );

    // Update agent's current ATOM stats
    await db.query(
      `UPDATE agents SET
         trust_tier = $1,
         quality_score = $2,
         confidence = $3,
         risk_score = $4,
         diversity_ratio = $5,
         feedback_count = feedback_count + 1,
         updated_at = $6
       WHERE asset = $7`,
      [data.newTrustTier, data.newQualityScore, data.newConfidence, data.newRiskScore, data.newDiversityRatio, ctx.blockTime.toISOString(), assetId]
    );

    logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString(), score: data.score, trustTier: data.newTrustTier }, "New feedback");
  } catch (error: any) {
    logger.error({ error: error.message, assetId, feedbackIndex: data.feedbackIndex }, "Failed to save feedback");
  }
}

async function handleFeedbackRevoked(
  data: FeedbackRevoked,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const assetId = data.asset.toBase58();
  const clientAddress = data.clientAddress.toBase58();
  const id = `${assetId}:${clientAddress}:${data.feedbackIndex}`;

  try {
    // Update feedback record (mark as revoked, ATOM stats update goes to agents table)
    await db.query(
      `UPDATE feedbacks SET
         is_revoked = true,
         revoked_at = $1
       WHERE id = $2`,
      [ctx.blockTime.toISOString(), id]
    );

    // Update agent's current ATOM stats (revoke may change them)
    if (data.hadImpact) {
      await db.query(
        `UPDATE agents SET
           trust_tier = $1,
           quality_score = $2,
           confidence = $3,
           feedback_count = GREATEST(feedback_count - 1, 0),
           updated_at = $4
         WHERE asset = $5`,
        [data.newTrustTier, data.newQualityScore, data.newConfidence, ctx.blockTime.toISOString(), assetId]
      );
    }

    logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString(), hadImpact: data.hadImpact }, "Feedback revoked");
  } catch (error: any) {
    logger.error({ error: error.message, assetId, feedbackIndex: data.feedbackIndex }, "Failed to revoke feedback");
  }
}

async function handleResponseAppended(
  data: ResponseAppended,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const assetId = data.asset.toBase58();
  const responder = data.responder.toBase58();
  const id = `${assetId}:${data.feedbackIndex}:${responder}`;

  try {
    await db.query(
      `INSERT INTO feedback_responses (id, asset, feedback_index, responder, response_uri, response_hash, block_slot, tx_signature, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       ON CONFLICT (id) DO NOTHING`,
      [id, assetId, Number(data.feedbackIndex), responder, data.responseUri || null,
       data.responseHash ? Buffer.from(data.responseHash).toString("hex") : null,
       Number(ctx.slot), ctx.signature, ctx.blockTime.toISOString()]
    );
    logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString() }, "Response appended");
  } catch (error: any) {
    logger.error({ error: error.message, assetId, feedbackIndex: data.feedbackIndex }, "Failed to append response");
  }
}

async function handleValidationRequested(
  data: ValidationRequested,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const assetId = data.asset.toBase58();
  const validatorAddress = data.validatorAddress.toBase58();
  const id = `${assetId}:${validatorAddress}:${data.nonce}`;

  try {
    await db.query(
      `INSERT INTO validations (id, asset, validator_address, nonce, requester, request_uri, request_hash, status, block_slot, tx_signature, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
       ON CONFLICT (id) DO NOTHING`,
      [id, assetId, validatorAddress, data.nonce, data.requester.toBase58(),
       data.requestUri || null, data.requestHash ? Buffer.from(data.requestHash).toString("hex") : null,
       "PENDING", Number(ctx.slot), ctx.signature, ctx.blockTime.toISOString()]
    );
    logger.info({ assetId, validator: validatorAddress, nonce: data.nonce }, "Validation requested");
  } catch (error: any) {
    logger.error({ error: error.message, assetId, nonce: data.nonce }, "Failed to request validation");
  }
}

async function handleValidationResponded(
  data: ValidationResponded,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const assetId = data.asset.toBase58();
  const validatorAddress = data.validatorAddress.toBase58();
  const id = `${assetId}:${validatorAddress}:${data.nonce}`;

  try {
    await db.query(
      `UPDATE validations SET
         response = $1,
         response_uri = $2,
         response_hash = $3,
         tag = $4,
         status = $5,
         updated_at = $6
       WHERE id = $7`,
      [data.response, data.responseUri || null,
       data.responseHash ? Buffer.from(data.responseHash).toString("hex") : null,
       data.tag || null, "RESPONDED", ctx.blockTime.toISOString(), id]
    );
    logger.info({ assetId, validator: validatorAddress, nonce: data.nonce, response: data.response }, "Validation responded");
  } catch (error: any) {
    logger.error({ error: error.message, assetId, nonce: data.nonce }, "Failed to respond to validation");
  }
}
