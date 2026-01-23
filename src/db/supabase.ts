/**
 * Supabase database handlers for production mode
 * Writes events directly to Supabase PostgreSQL via pg client
 */

import { Pool } from "pg";
import { createHash } from "crypto";
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

const logger = createChildLogger("supabase-handlers");

export interface EventContext {
  signature: string;
  slot: bigint;
  blockTime: Date;
  txIndex?: number; // Transaction index within the block (for deterministic ordering)
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
      connectionTimeoutMillis: 10000, // 10s timeout
      idleTimeoutMillis: 30000,
    });
    pool.on('error', (err) => {
      logger.error({ error: err.message }, 'Unexpected pool error');
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

    case "AtomEnabled":
      await handleAtomEnabled(event.data, ctx);
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
  const agentUri = data.agentUri || null;

  await ensureCollection(collection);

  try {
    await db.query(
      `INSERT INTO agents (asset, owner, agent_uri, collection, atom_enabled, block_slot, tx_index, tx_signature, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       ON CONFLICT (asset) DO UPDATE SET
         owner = EXCLUDED.owner,
         agent_uri = EXCLUDED.agent_uri,
         atom_enabled = EXCLUDED.atom_enabled,
         block_slot = EXCLUDED.block_slot,
         tx_index = EXCLUDED.tx_index,
         updated_at = EXCLUDED.created_at`,
      [assetId, data.owner.toBase58(), agentUri, collection, data.atomEnabled, Number(ctx.slot), ctx.txIndex ?? null, ctx.signature, ctx.blockTime.toISOString()]
    );
    logger.info({ assetId, owner: data.owner.toBase58(), uri: agentUri }, "Agent registered");

    // Trigger URI metadata extraction if configured and URI is present
    if (agentUri && config.metadataIndexMode !== "off") {
      digestAndStoreUriMetadata(assetId, agentUri).catch((err) => {
        logger.warn({ assetId, uri: agentUri, error: err.message }, "Failed to digest URI metadata");
      });
    }
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

async function handleAtomEnabled(
  data: AtomEnabled,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const assetId = data.asset.toBase58();

  try {
    await db.query(
      `UPDATE agents SET atom_enabled = true, block_slot = $1, updated_at = $2 WHERE asset = $3`,
      [Number(ctx.slot), ctx.blockTime.toISOString(), assetId]
    );
    logger.info({ assetId, enabledBy: data.enabledBy.toBase58() }, "ATOM enabled");
  } catch (error: any) {
    logger.error({ error: error.message, assetId }, "Failed to enable ATOM");
  }
}

async function handleUriUpdated(
  data: UriUpdated,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const assetId = data.asset.toBase58();
  const newUri = data.newUri || null;

  try {
    await db.query(
      `UPDATE agents SET agent_uri = $1, block_slot = $2, updated_at = $3 WHERE asset = $4`,
      [newUri, Number(ctx.slot), ctx.blockTime.toISOString(), assetId]
    );
    logger.info({ assetId, newUri }, "Agent URI updated");

    // Trigger URI metadata extraction if configured and URI is present
    if (newUri && config.metadataIndexMode !== "off") {
      digestAndStoreUriMetadata(assetId, newUri).catch((err) => {
        logger.warn({ assetId, uri: newUri, error: err.message }, "Failed to digest URI metadata");
      });
    }
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
  // FIX: Calculate key_hash from key (sha256(key)[0..16]), not from value
  const keyHash = createHash("sha256").update(data.key).digest().slice(0, 16).toString("hex");
  const id = `${assetId}:${keyHash}`;

  try {
    // Compress value for storage (threshold: 256 bytes)
    const compressedValue = await compressForStorage(Buffer.from(data.value));

    await db.query(
      `INSERT INTO metadata (id, asset, key, key_hash, value, immutable, block_slot, tx_index, tx_signature, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
       ON CONFLICT (id) DO UPDATE SET
         value = EXCLUDED.value,
         block_slot = EXCLUDED.block_slot,
         tx_index = EXCLUDED.tx_index,
         updated_at = EXCLUDED.updated_at`,
      [id, assetId, data.key, keyHash, compressedValue, data.immutable, Number(ctx.slot), ctx.txIndex ?? null, ctx.signature, ctx.blockTime.toISOString()]
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
         registry_type = EXCLUDED.registry_type,
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
         registry_type = EXCLUDED.registry_type,
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
    // Normalize optional hash: all-zero means "no hash" for IPFS
    const isAllZeroHash = data.feedbackHash && data.feedbackHash.every(b => b === 0);
    const feedbackHash = (data.feedbackHash && !isAllZeroHash)
      ? Buffer.from(data.feedbackHash).toString("hex")
      : null;

    const insertResult = await db.query(
      `INSERT INTO feedbacks (id, asset, client_address, feedback_index, score, tag1, tag2, endpoint, feedback_uri, feedback_hash,
         is_revoked, block_slot, tx_index, tx_signature, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
       ON CONFLICT (id) DO NOTHING`,
      [
        id, assetId, clientAddress, data.feedbackIndex.toString(), data.score,
        data.tag1 || null, data.tag2 || null, data.endpoint || null, data.feedbackUri || null,
        feedbackHash,
        false, Number(ctx.slot), ctx.txIndex ?? null, ctx.signature, ctx.blockTime.toISOString()
      ]
    );

    if (insertResult.rowCount === 0) {
      logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString() }, "Duplicate feedback ignored");
      return;
    }

    const baseUpdate = `
      feedback_count = COALESCE((
        SELECT COUNT(*)::int
        FROM feedbacks
        WHERE asset = $2 AND NOT is_revoked
      ), 0),
      raw_avg_score = COALESCE((
        SELECT ROUND(AVG(score))::smallint
        FROM feedbacks
        WHERE asset = $2 AND NOT is_revoked
      ), 0),
      updated_at = $1
    `;

    if (data.atomEnabled) {
      await db.query(
        `UPDATE agents SET
           trust_tier = $3,
           quality_score = $4,
           confidence = $5,
           risk_score = $6,
           diversity_ratio = $7,
           ${baseUpdate}
         WHERE asset = $2`,
        [
          ctx.blockTime.toISOString(),
          assetId,
          data.newTrustTier,
          data.newQualityScore,
          data.newConfidence,
          data.newRiskScore,
          data.newDiversityRatio,
        ]
      );
    } else {
      await db.query(
        `UPDATE agents SET
           ${baseUpdate}
         WHERE asset = $2`,
        [ctx.blockTime.toISOString(), assetId]
      );
    }

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

    const baseUpdate = `
      feedback_count = COALESCE((
        SELECT COUNT(*)::int
        FROM feedbacks
        WHERE asset = $2 AND NOT is_revoked
      ), 0),
      raw_avg_score = COALESCE((
        SELECT ROUND(AVG(score))::smallint
        FROM feedbacks
        WHERE asset = $2 AND NOT is_revoked
      ), 0),
      updated_at = $1
    `;

    await db.query(
      `UPDATE agents SET
         ${baseUpdate}
       WHERE asset = $2`,
      [ctx.blockTime.toISOString(), assetId]
    );

    if (data.atomEnabled && data.hadImpact) {
      await db.query(
        `UPDATE agents SET
           trust_tier = $3,
           quality_score = $4,
           confidence = $5,
           updated_at = $1
         WHERE asset = $2`,
        [ctx.blockTime.toISOString(), assetId, data.newTrustTier, data.newQualityScore, data.newConfidence]
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
  const clientAddress = data.client.toBase58();
  const responder = data.responder.toBase58();
  const id = `${assetId}:${clientAddress}:${data.feedbackIndex}:${responder}`;

  try {
    const feedbackCheck = await db.query(
      `SELECT id FROM feedbacks WHERE asset = $1 AND client_address = $2 AND feedback_index = $3 LIMIT 1`,
      [assetId, clientAddress, data.feedbackIndex.toString()]
    );
    if (feedbackCheck.rowCount === 0) {
      logger.warn({ assetId, client: clientAddress, feedbackIndex: data.feedbackIndex.toString() }, "Feedback not found for response");
      return;
    }

    // Normalize optional hash: all-zero means "no hash" for IPFS
    const isAllZeroHash = data.responseHash && data.responseHash.every(b => b === 0);
    const responseHash = (data.responseHash && !isAllZeroHash)
      ? Buffer.from(data.responseHash).toString("hex")
      : null;

    await db.query(
      `INSERT INTO feedback_responses (id, asset, client_address, feedback_index, responder, response_uri, response_hash, block_slot, tx_index, tx_signature, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
       ON CONFLICT (id) DO NOTHING`,
      [id, assetId, clientAddress, data.feedbackIndex.toString(), responder, data.responseUri || null,
       responseHash,
       Number(ctx.slot), ctx.txIndex ?? null, ctx.signature, ctx.blockTime.toISOString()]
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
      `INSERT INTO validations (id, asset, validator_address, nonce, requester, request_uri, request_hash, status, block_slot, tx_index, tx_signature, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
       ON CONFLICT (id) DO NOTHING`,
      [id, assetId, validatorAddress, data.nonce, data.requester.toBase58(),
       data.requestUri || null, data.requestHash ? Buffer.from(data.requestHash).toString("hex") : null,
       "PENDING", Number(ctx.slot), ctx.txIndex ?? null, ctx.signature, ctx.blockTime.toISOString()]
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

// =============================================
// INDEXER STATE PERSISTENCE
// =============================================

export interface IndexerState {
  lastSignature: string | null;
  lastSlot: bigint | null;
}

// Fallback: first transaction signature of the new program deployment
// Kept for reference - backfill mode will fetch all transactions anyway
// DEPLOYMENT_SIGNATURE = "6PXQkP5ihC2UMHD3xbm1Z9Ry8HXxSuKFAaVNGNktLBKXsgCQcqi7CM74SgoGkxvaiMVPMEP8REtGVbZK92Wsigt"
// DEPLOYMENT_SLOT = 434717355

/**
 * Load indexer state (cursor) from Supabase
 */
export async function loadIndexerState(): Promise<IndexerState> {
  logger.info("Loading indexer state from Supabase...");
  try {
    const db = getPool();
    const result = await db.query(
      `SELECT last_signature, last_slot FROM indexer_state WHERE id = 'main'`
    );
    if (result.rows.length > 0) {
      const row = result.rows[0];
      // If signature is null in DB, use deployment fallback
      if (!row.last_signature) {
        logger.info("DB has null signature, using deployment fallback");
        return { lastSignature: null, lastSlot: null };
      }
      logger.info({ lastSignature: row.last_signature, lastSlot: row.last_slot }, "Loaded indexer state");
      return {
        lastSignature: row.last_signature,
        lastSlot: row.last_slot ? BigInt(row.last_slot) : null,
      };
    }
    logger.info("No saved indexer state found, will start from beginning");
  } catch (error: any) {
    logger.warn({ error: error.message }, "Failed to load indexer state, using deployment fallback");
  }
  // Fallback: return null to start from beginning (fetches all transactions)
  return { lastSignature: null, lastSlot: null };
}

/**
 * Save indexer state (cursor) to Supabase
 */
export async function saveIndexerState(signature: string, slot: bigint): Promise<void> {
  const db = getPool();
  try {
    await db.query(
      `INSERT INTO indexer_state (id, last_signature, last_slot, updated_at)
       VALUES ('main', $1, $2, NOW())
       ON CONFLICT (id) DO UPDATE SET
         last_signature = EXCLUDED.last_signature,
         last_slot = EXCLUDED.last_slot,
         updated_at = NOW()`,
      [signature, Number(slot)]
    );
  } catch (error: any) {
    logger.error({ error: error.message }, "Failed to save indexer state");
  }
}

// =============================================
// URI METADATA EXTRACTION
// =============================================

import { digestUri, serializeValue } from "../indexer/uriDigest.js";
import { compressForStorage } from "../utils/compression.js";

/**
 * Fetch, digest, and store URI metadata for an agent
 * Called asynchronously after agent registration or URI update
 */
async function digestAndStoreUriMetadata(assetId: string, uri: string): Promise<void> {
  if (config.metadataIndexMode === "off") {
    return;
  }

  const result = await digestUri(uri);

  if (result.status !== "ok" || !result.fields) {
    logger.debug({ assetId, uri, status: result.status, error: result.error }, "URI digest failed or empty");
    // Store error status as metadata
    await storeUriMetadata(assetId, "uri:status", JSON.stringify({
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
      await storeUriMetadata(assetId, `${key}_meta`, JSON.stringify({
        status: "oversize",
        bytes: serialized.bytes,
        sha256: result.hash,
      }));
    } else {
      await storeUriMetadata(assetId, key, serialized.value);
    }
  }

  // Store success status
  await storeUriMetadata(assetId, "uri:status", JSON.stringify({
    status: "ok",
    bytes: result.bytes,
    hash: result.hash,
    fieldCount: Object.keys(result.fields).length,
  }));

  logger.info({ assetId, uri, fieldCount: Object.keys(result.fields).length }, "URI metadata indexed");
}

// Standard URI fields that should NOT be compressed (frequently read)
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

/**
 * Store a single URI metadata entry
 * Standard fields are stored raw (no compression) for fast reads
 * Extra/custom fields are compressed with ZSTD if > 256 bytes
 */
async function storeUriMetadata(assetId: string, key: string, value: string): Promise<void> {
  const db = getPool();
  const keyHash = createHash("sha256").update(key).digest().slice(0, 16).toString("hex");
  const id = `${assetId}:${keyHash}`;

  try {
    // Only compress non-standard fields (custom/extra data)
    // Standard fields are read frequently and shouldn't incur decompression cost
    const shouldCompress = !STANDARD_URI_FIELDS.has(key);
    const storedValue = shouldCompress
      ? await compressForStorage(Buffer.from(value))
      : Buffer.concat([Buffer.from([0x00]), Buffer.from(value)]); // PREFIX_RAW

    await db.query(
      `INSERT INTO metadata (id, asset, key, key_hash, value, immutable, block_slot, tx_signature, updated_at)
       VALUES ($1, $2, $3, $4, $5, false, 0, 'uri_derived', NOW())
       ON CONFLICT (id) DO UPDATE SET
         value = EXCLUDED.value,
         updated_at = NOW()`,
      [id, assetId, key, keyHash, storedValue]
    );
  } catch (error: any) {
    logger.error({ error: error.message, assetId, key }, "Failed to store URI metadata");
  }
}
