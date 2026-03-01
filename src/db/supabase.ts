/**
 * Supabase database handlers for production mode
 * Writes events directly to Supabase PostgreSQL via pg client
 */

import { Pool } from "pg";
import { createHash } from "crypto";
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
import { DEFAULT_PUBKEY } from "../constants.js";
import type { PoolClient } from "pg";

const logger = createChildLogger("supabase-handlers");

// Default status for new records (will be verified later)
const DEFAULT_STATUS: ChainStatus = "PENDING";

export interface EventContext {
  signature: string;
  slot: bigint;
  blockTime: Date;
  txIndex?: number; // Transaction index within the block (captured as metadata/tie-break context)
  eventOrdinal?: number; // Event index within parsed transaction events (deterministic intra-tx tie-break)
}

let pool: Pool | null = null;

// LRU-limited collection cache to prevent unbounded memory growth
const MAX_SEEN_COLLECTIONS = 1000;
const seenCollections = new Map<string, number>(); // collection -> timestamp

// Check if collection is in cache (read-only, does NOT add)
function hasSeenCollection(collection: string): boolean {
  if (seenCollections.has(collection)) {
    // Update access time for LRU
    seenCollections.set(collection, Date.now());
    return true;
  }
  return false;
}

// Mark collection as seen (call AFTER successful DB operation)
function markCollectionSeen(collection: string): void {
  // Evict oldest entries if at capacity
  if (seenCollections.size >= MAX_SEEN_COLLECTIONS) {
    let oldestKey: string | null = null;
    let oldestTime = Infinity;

    for (const [key, time] of seenCollections) {
      if (time < oldestTime) {
        oldestTime = time;
        oldestKey = key;
      }
    }

    if (oldestKey) {
      seenCollections.delete(oldestKey);
      logger.debug({ evictedCollection: oldestKey, cacheSize: seenCollections.size }, "Evicted oldest collection from cache");
    }
  }

  seenCollections.set(collection, Date.now());
}

// Stats tracking
let eventStats = {
  agentRegistered: 0,
  feedbackReceived: 0,
  validationRequested: 0,
  validationResponded: 0,
  metadataSet: 0,
  errors: 0,
  lastLogTime: Date.now(),
};

const ZERO_HASH_HEX = "0".repeat(64);

function hashesMatchHex(stored: string | null, event: string | null): boolean {
  const sEmpty = !stored || stored === ZERO_HASH_HEX;
  const eEmpty = !event || event === ZERO_HASH_HEX;
  if (sEmpty && eEmpty) return true;
  if (sEmpty || eEmpty) return false;
  return stored === event;
}

function logStatsIfNeeded(): void {
  const now = Date.now();
  // Log stats every 60 seconds
  if (now - eventStats.lastLogTime > 60000) {
    logger.info({
      ...eventStats,
      collectionCacheSize: seenCollections.size,
    }, "Supabase handler stats (60s)");
    eventStats.lastLogTime = now;
  }
}

export function getPool(): Pool {
  if (!pool) {
    if (!config.supabaseDsn) {
      throw new Error("SUPABASE_DSN required for supabase mode");
    }
    logger.info({ maxConnections: 10, sslVerify: config.supabaseSslVerify }, "Creating PostgreSQL connection pool");
    if (!config.supabaseSslVerify) {
      logger.warn("SSL certificate verification disabled - not recommended for production");
    }
    pool = new Pool({
      connectionString: config.supabaseDsn,
      ssl: config.supabaseSslVerify ? { rejectUnauthorized: true } : false,
      max: 10,
      connectionTimeoutMillis: 10000, // 10s timeout
      idleTimeoutMillis: 30000,
    });
    pool.on('error', (err) => {
      eventStats.errors++;
      logger.error({ error: err.message, stack: err.stack }, 'Unexpected pool error');
    });
    pool.on('connect', () => {
      logger.debug("New database connection established");
    });
    // Initialize metadata queue with same pool
    metadataQueue.setPool(pool);
    collectionMetadataQueue.setPool(pool);
    logger.info({ metadataMode: config.metadataIndexMode }, "Metadata extraction queue initialized");
  }
  return pool;
}

/**
 * @deprecated Use handleEventAtomic instead. This non-atomic handler does not
 * wrap event processing + cursor update in a single transaction.
 */
export async function handleEvent(
  event: ProgramEvent,
  ctx: EventContext
): Promise<void> {
  const startTime = Date.now();

  try {
    switch (event.type) {
      case "AgentRegistered":
        await handleAgentRegistered(event.data, ctx);
        eventStats.agentRegistered++;
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
      case "WalletResetOnOwnerSync":
        await handleWalletResetOnOwnerSync(event.data, ctx);
        break;

      case "MetadataSet":
        await handleMetadataSet(event.data, ctx);
        eventStats.metadataSet++;
        break;

      case "MetadataDeleted":
        await handleMetadataDeleted(event.data, ctx);
        break;

      case "RegistryInitialized":
        await handleRegistryInitialized(event.data, ctx);
        break;
      case "CollectionPointerSet":
        await handleCollectionPointerSet(event.data, ctx);
        break;
      case "ParentAssetSet":
        await handleParentAssetSet(event.data, ctx);
        break;

      case "NewFeedback":
        await handleNewFeedback(event.data, ctx);
        eventStats.feedbackReceived++;
        break;

      case "FeedbackRevoked":
        await handleFeedbackRevoked(event.data, ctx);
        break;

      case "ResponseAppended":
        await handleResponseAppended(event.data, ctx);
        break;

      case "ValidationRequested":
        if (config.validationIndexEnabled) {
          await handleValidationRequested(event.data, ctx);
          eventStats.validationRequested++;
        }
        break;

      case "ValidationResponded":
        if (config.validationIndexEnabled) {
          await handleValidationResponded(event.data, ctx);
          eventStats.validationResponded++;
        }
        break;

      default:
        logger.warn({ event }, "Unhandled event type");
    }

    const duration = Date.now() - startTime;
    if (duration > 1000) {
      logger.warn({ eventType: event.type, duration, signature: ctx.signature }, "Slow event processing");
    }

    logStatsIfNeeded();
  } catch (error: any) {
    eventStats.errors++;
    logger.error({
      error: error.message,
      eventType: event.type,
      signature: ctx.signature,
      slot: ctx.slot.toString()
    }, "Error handling event");
    throw error; // Re-throw to let caller handle
  }
}

/**
 * Atomic event handler - wraps event processing and cursor update in a single PostgreSQL transaction
 * This ensures crash/reorg resilience: either both succeed or both fail
 */
export async function handleEventAtomic(
  event: ProgramEvent,
  ctx: EventContext & { source?: "poller" | "websocket" | "substreams" }
): Promise<void> {
  const db = getPool();
  const client = await db.connect();

  try {
    await client.query("BEGIN");

    // Handle event inside transaction
    await handleEventInTx(client, event, ctx);

    // Update cursor atomically with monotonic guard
    await updateCursorAtomic(client, ctx);

    await client.query("COMMIT");
  } catch (error: any) {
    await client.query("ROLLBACK");
    eventStats.errors++;
    logger.error({
      error: error.message,
      eventType: event.type,
      signature: ctx.signature,
      slot: ctx.slot.toString(),
    }, "Atomic event handling failed, rolled back");
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Update indexer cursor with monotonic guard
 * Only advances if the new slot is greater than the current slot
 */
async function updateCursorAtomic(
  client: PoolClient,
  ctx: EventContext & { source?: string }
): Promise<void> {
  await client.query(
    `INSERT INTO indexer_state (id, last_signature, last_slot, source, updated_at)
     VALUES ('main', $1, $2, $3, NOW())
     ON CONFLICT (id) DO UPDATE SET
       last_signature = EXCLUDED.last_signature,
       last_slot = EXCLUDED.last_slot,
       source = EXCLUDED.source,
       updated_at = NOW()
     WHERE indexer_state.last_slot IS NULL OR indexer_state.last_slot <= EXCLUDED.last_slot`,
    [ctx.signature, ctx.slot.toString(), ctx.source || "poller"]
  );
}

/**
 * Inner event handler - runs inside transaction
 */
async function handleEventInTx(
  client: PoolClient,
  event: ProgramEvent,
  ctx: EventContext
): Promise<void> {
  switch (event.type) {
    case "AgentRegistered":
      await handleAgentRegisteredTx(client, event.data, ctx);
      eventStats.agentRegistered++;
      break;
    case "AgentOwnerSynced":
      await handleAgentOwnerSyncedTx(client, event.data, ctx);
      break;
    case "AtomEnabled":
      await handleAtomEnabledTx(client, event.data, ctx);
      break;
    case "UriUpdated":
      await handleUriUpdatedTx(client, event.data, ctx);
      break;
    case "WalletUpdated":
      await handleWalletUpdatedTx(client, event.data, ctx);
      break;
    case "WalletResetOnOwnerSync":
      await handleWalletResetOnOwnerSyncTx(client, event.data, ctx);
      break;
    case "MetadataSet":
      await handleMetadataSetTx(client, event.data, ctx);
      eventStats.metadataSet++;
      break;
    case "MetadataDeleted":
      await handleMetadataDeletedTx(client, event.data, ctx);
      break;
    case "RegistryInitialized":
      await handleRegistryInitializedTx(client, event.data, ctx);
      break;
    case "CollectionPointerSet":
      await handleCollectionPointerSetTx(client, event.data, ctx);
      break;
    case "ParentAssetSet":
      await handleParentAssetSetTx(client, event.data, ctx);
      break;
    case "NewFeedback":
      await handleNewFeedbackTx(client, event.data, ctx);
      eventStats.feedbackReceived++;
      break;
    case "FeedbackRevoked":
      await handleFeedbackRevokedTx(client, event.data, ctx);
      break;
    case "ResponseAppended":
      await handleResponseAppendedTx(client, event.data, ctx);
      break;
    case "ValidationRequested":
      if (config.validationIndexEnabled) {
        await handleValidationRequestedTx(client, event.data, ctx);
        eventStats.validationRequested++;
      }
      break;
    case "ValidationResponded":
      if (config.validationIndexEnabled) {
        await handleValidationRespondedTx(client, event.data, ctx);
        eventStats.validationResponded++;
      }
      break;
    default:
      logger.warn({ event }, "Unhandled event type");
  }
}

async function ensureCollection(collection: string): Promise<void> {
  // Use LRU cache to check if we've seen this collection recently
  if (hasSeenCollection(collection)) return;

  const db = getPool();
  try {
    await db.query(
      `INSERT INTO collections (collection, registry_type, created_at, status)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (collection) DO NOTHING`,
      [collection, "BASE", new Date().toISOString(), DEFAULT_STATUS]
    );
    // Only cache after successful DB operation
    markCollectionSeen(collection);
    logger.debug({ collection }, "Ensured collection exists");
  } catch (error: any) {
    // Don't cache on failure - allow retry on next event
    eventStats.errors++;
    logger.error({ error: error.message, collection }, "Failed to ensure collection");
  }
}

async function ensureCollectionTx(client: PoolClient, collection: string): Promise<void> {
  if (hasSeenCollection(collection)) return;
  try {
    await client.query(
      `INSERT INTO collections (collection, registry_type, created_at, status)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (collection) DO NOTHING`,
      [collection, "BASE", new Date().toISOString(), DEFAULT_STATUS]
    );
    markCollectionSeen(collection);
  } catch (error: any) {
    eventStats.errors++;
    logger.error({ error: error.message, collection }, "Failed to ensure collection");
    throw error;
  }
}

// Transaction-aware handlers for atomic ingestion

async function handleAgentRegisteredTx(
  client: PoolClient,
  data: AgentRegistered,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const collection = data.collection.toBase58();
  const agentUri = data.agentUri || null;
  await ensureCollectionTx(client, collection);
  await client.query(
    `INSERT INTO agents (asset, owner, creator, agent_uri, collection, canonical_col, col_locked, parent_asset, parent_creator, parent_locked, atom_enabled, block_slot, tx_index, event_ordinal, tx_signature, created_at, status)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
     ON CONFLICT (asset) DO UPDATE SET
       owner = EXCLUDED.owner,
       creator = COALESCE(agents.creator, EXCLUDED.creator),
       agent_uri = EXCLUDED.agent_uri,
       atom_enabled = EXCLUDED.atom_enabled,
       block_slot = EXCLUDED.block_slot,
       tx_index = EXCLUDED.tx_index,
       event_ordinal = EXCLUDED.event_ordinal,
       updated_at = EXCLUDED.created_at`,
    [
      assetId,
      data.owner.toBase58(),
      data.owner.toBase58(),
      agentUri,
      collection,
      "",
      false,
      null,
      null,
      false,
      data.atomEnabled,
      ctx.slot.toString(),
      ctx.txIndex ?? null,
      ctx.eventOrdinal ?? null,
      ctx.signature,
      ctx.blockTime.toISOString(),
      DEFAULT_STATUS,
    ]
  );
  logger.info({ assetId, owner: data.owner.toBase58(), uri: agentUri }, "Agent registered");

  // Queue URI metadata extraction (fire-and-forget, runs after transaction commits)
  if (agentUri && config.metadataIndexMode !== "off") {
    metadataQueue.add(assetId, agentUri);
  }
}

async function handleAgentOwnerSyncedTx(
  client: PoolClient,
  data: AgentOwnerSynced,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  await client.query(
    `UPDATE agents SET owner = $1, block_slot = $2, updated_at = $3 WHERE asset = $4`,
    [data.newOwner.toBase58(), ctx.slot.toString(), ctx.blockTime.toISOString(), assetId]
  );
  logger.info({ assetId, oldOwner: data.oldOwner.toBase58(), newOwner: data.newOwner.toBase58() }, "Agent owner synced");
}

async function handleAtomEnabledTx(
  client: PoolClient,
  data: AtomEnabled,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  await client.query(
    `UPDATE agents SET atom_enabled = true, block_slot = $1, updated_at = $2 WHERE asset = $3`,
    [ctx.slot.toString(), ctx.blockTime.toISOString(), assetId]
  );
  logger.info({ assetId, enabledBy: data.enabledBy.toBase58() }, "ATOM enabled");
}

async function handleUriUpdatedTx(
  client: PoolClient,
  data: UriUpdated,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const newUri = data.newUri || null;
  await client.query(
    `UPDATE agents SET agent_uri = $1, block_slot = $2, updated_at = $3 WHERE asset = $4`,
    [newUri, ctx.slot.toString(), ctx.blockTime.toISOString(), assetId]
  );
  logger.info({ assetId, newUri }, "Agent URI updated");

  // Queue URI metadata extraction (fire-and-forget, runs after transaction commits)
  if (newUri && config.metadataIndexMode !== "off") {
    metadataQueue.add(assetId, newUri);
  }
}

async function handleWalletUpdatedTx(
  client: PoolClient,
  data: WalletUpdated,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const newWalletRaw = data.newWallet.toBase58();
  const newWallet = newWalletRaw === DEFAULT_PUBKEY ? null : newWalletRaw;
  await client.query(
    `UPDATE agents SET agent_wallet = $1, block_slot = $2, updated_at = $3 WHERE asset = $4`,
    [newWallet, ctx.slot.toString(), ctx.blockTime.toISOString(), assetId]
  );
  logger.info({ assetId, newWallet: newWallet ?? "(reset)" }, "Agent wallet updated");
}

async function handleWalletResetOnOwnerSyncTx(
  client: PoolClient,
  data: WalletResetOnOwnerSync,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const newWalletRaw = data.newWallet.toBase58();
  const newWallet = newWalletRaw === DEFAULT_PUBKEY ? null : newWalletRaw;
  await client.query(
    `UPDATE agents SET owner = $1, agent_wallet = $2, block_slot = $3, updated_at = $4 WHERE asset = $5`,
    [data.ownerAfterSync.toBase58(), newWallet, ctx.slot.toString(), ctx.blockTime.toISOString(), assetId]
  );
  logger.info({ assetId, ownerAfterSync: data.ownerAfterSync.toBase58(), newWallet: newWallet ?? "(reset)" }, "Wallet reset on owner sync");
}

async function handleCollectionPointerSetTx(
  client: PoolClient,
  data: CollectionPointerSet,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const pointer = data.col;
  const setBy = data.setBy.toBase58();
  const lock = typeof data.lock === "boolean" ? data.lock : null;
  await client.query(
    `WITH previous AS (
       SELECT canonical_col AS prev_col, COALESCE(creator, owner) AS prev_creator
       FROM agents
       WHERE asset = $1
     ),
     updated AS (
       UPDATE agents
       SET canonical_col = $2,
           creator = COALESCE(creator, $3),
           block_slot = $4,
           updated_at = $5,
           col_locked = COALESCE($7, col_locked)
       WHERE asset = $1
       RETURNING 1
     ),
     inserted AS (
       INSERT INTO collection_pointers (
         col, creator, first_seen_asset, first_seen_at, first_seen_slot, first_seen_tx_signature,
         last_seen_at, last_seen_slot, last_seen_tx_signature, asset_count
       )
       SELECT $2, $3, $1, $5, $4, $6, $5, $4, $6, 1
       WHERE EXISTS (SELECT 1 FROM updated)
       ON CONFLICT (col, creator) DO NOTHING
     ),
     dec_old AS (
       UPDATE collection_pointers cp
       SET asset_count = GREATEST(0, cp.asset_count - 1)
       WHERE cp.col = COALESCE((SELECT prev_col FROM previous), '')
         AND cp.creator = COALESCE((SELECT prev_creator FROM previous), '')
         AND cp.col <> ''
         AND (cp.col <> $2 OR cp.creator <> $3)
         AND EXISTS (SELECT 1 FROM updated)
       RETURNING 1
     )
     UPDATE collection_pointers cp
     SET last_seen_at = $5,
         last_seen_slot = $4,
         last_seen_tx_signature = $6,
         asset_count = cp.asset_count + CASE
           WHEN COALESCE((SELECT prev_col FROM previous), '') <> $2
             OR COALESCE((SELECT prev_creator FROM previous), '') <> $3
           THEN 1 ELSE 0 END
     WHERE cp.col = $2
       AND cp.creator = $3
       AND EXISTS (SELECT 1 FROM updated)`,
     [assetId, pointer, setBy, ctx.slot.toString(), ctx.blockTime.toISOString(), ctx.signature, lock]
   );
  logger.info({ assetId, col: pointer, setBy }, "Collection pointer set");

  if (config.collectionMetadataIndexEnabled) {
    collectionMetadataQueue.add(assetId, pointer);
  }
}

async function handleParentAssetSetTx(
  client: PoolClient,
  data: ParentAssetSet,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const lock = typeof data.lock === "boolean" ? data.lock : null;
  await client.query(
    `UPDATE agents
     SET parent_asset = $1,
         parent_creator = $2,
         block_slot = $3,
         updated_at = $4,
         parent_locked = COALESCE($6, parent_locked)
     WHERE asset = $5`,
    [
      data.parentAsset.toBase58(),
      data.parentCreator.toBase58(),
      ctx.slot.toString(),
      ctx.blockTime.toISOString(),
      assetId,
      lock,
    ]
  );
  logger.info({
    assetId,
    parentAsset: data.parentAsset.toBase58(),
    parentCreator: data.parentCreator.toBase58(),
    setBy: data.setBy.toBase58(),
  }, "Parent asset set");
}

async function handleMetadataSetTx(
  client: PoolClient,
  data: MetadataSet,
  ctx: EventContext
): Promise<void> {
  if (data.key.startsWith("_uri:")) {
    logger.warn({ assetId: data.asset.toBase58(), key: data.key }, "Skipping reserved _uri: prefix");
    return;
  }
  const assetId = data.asset.toBase58();
  const keyHash = createHash("sha256").update(data.key).digest().slice(0, 16).toString("hex");
  const id = `${assetId}:${keyHash}`;
  const compressedValue = await compressForStorage(stripNullBytes(data.value));
  await client.query(
    `INSERT INTO metadata (id, asset, key, key_hash, value, immutable, block_slot, tx_index, event_ordinal, tx_signature, updated_at, status)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
     ON CONFLICT (id) DO UPDATE SET
       value = EXCLUDED.value,
       immutable = metadata.immutable OR EXCLUDED.immutable,
       block_slot = EXCLUDED.block_slot,
       tx_index = EXCLUDED.tx_index,
       event_ordinal = EXCLUDED.event_ordinal,
       updated_at = EXCLUDED.updated_at
     WHERE NOT metadata.immutable`,
    [id, assetId, data.key, keyHash, compressedValue, data.immutable, ctx.slot.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString(), DEFAULT_STATUS]
  );
  logger.info({ assetId, key: data.key }, "Metadata set");
}

async function handleMetadataDeletedTx(
  client: PoolClient,
  data: MetadataDeleted,
  _ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  await client.query(`DELETE FROM metadata WHERE asset = $1 AND key = $2`, [assetId, data.key]);
  logger.info({ assetId, key: data.key }, "Metadata deleted");
}

// v0.6.0: RegistryInitialized replaces BaseRegistryCreated/UserRegistryCreated
async function handleRegistryInitializedTx(
  client: PoolClient,
  data: RegistryInitialized,
  ctx: EventContext
): Promise<void> {
  const collection = data.collection.toBase58();
  await client.query(
    `INSERT INTO collections (collection, registry_type, authority, created_at, status)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (collection) DO UPDATE SET
       registry_type = EXCLUDED.registry_type,
       authority = EXCLUDED.authority`,
    [collection, "BASE", data.authority.toBase58(), ctx.blockTime.toISOString(), DEFAULT_STATUS]
  );
  logger.info({ collection }, "Registry initialized");
}

async function handleNewFeedbackTx(
  client: PoolClient,
  data: NewFeedback,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const clientAddress = data.clientAddress.toBase58();
  const id = `${assetId}:${clientAddress}:${data.feedbackIndex}`;
  // SEAL v1: sealHash is computed on-chain, stored in feedback_hash column
  const feedbackHash = data.sealHash
    ? Buffer.from(data.sealHash).toString("hex")
    : null;
  const runningDigest = data.newFeedbackDigest
    ? Buffer.from(data.newFeedbackDigest)
    : null;

  const insertResult = await client.query(
    `INSERT INTO feedbacks (id, feedback_id, asset, client_address, feedback_index, value, value_decimals, score, tag1, tag2, endpoint, feedback_uri, feedback_hash,
       running_digest, is_revoked, block_slot, tx_index, event_ordinal, tx_signature, created_at, status)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
     ON CONFLICT (id) DO NOTHING`,
    [
      id, null, assetId, clientAddress, data.feedbackIndex.toString(),
      data.value.toString(), data.valueDecimals, data.score,
      data.tag1 || null, data.tag2 || null, data.endpoint || null, data.feedbackUri || null,
      feedbackHash, runningDigest,
      false, ctx.slot.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString(), DEFAULT_STATUS
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
    await client.query(
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
    await client.query(
      `UPDATE agents SET
         ${baseUpdate}
       WHERE asset = $2`,
      [ctx.blockTime.toISOString(), assetId]
    );
  }
  logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString(), score: data.score, trustTier: data.newTrustTier }, "New feedback");
}

async function handleFeedbackRevokedTx(
  client: PoolClient,
  data: FeedbackRevoked,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const clientAddress = data.clientAddress.toBase58();
  const id = `${assetId}:${clientAddress}:${data.feedbackIndex}`;

  // Check feedback exists and validate seal_hash
  const feedbackCheck = await client.query(
    `SELECT id, feedback_hash FROM feedbacks WHERE id = $1 LIMIT 1`,
    [id]
  );
  if (feedbackCheck.rowCount === 0) {
    logger.warn(
      { assetId, client: clientAddress, feedbackIndex: data.feedbackIndex.toString() },
      "Feedback not found for revocation (orphan revoke)"
    );
  } else {
    const storedHash = feedbackCheck.rows[0].feedback_hash;
    const eventHash = data.sealHash
      ? Buffer.from(data.sealHash).toString("hex")
      : null;
    if (!hashesMatchHex(storedHash, eventHash)) {
      logger.warn(
        { assetId, client: clientAddress, feedbackIndex: data.feedbackIndex.toString() },
        "seal_hash mismatch: revocation sealHash does not match stored feedbackHash"
      );
    }
  }

  await client.query(
    `UPDATE feedbacks SET
       is_revoked = true,
       revoked_at = $1
     WHERE id = $2`,
    [ctx.blockTime.toISOString(), id]
  );

  const isOrphan = feedbackCheck.rowCount === 0;
  const revokeDigest = data.newRevokeDigest
    ? Buffer.from(data.newRevokeDigest)
    : null;
  const revokeSealHash = data.sealHash
    ? Buffer.from(data.sealHash).toString("hex")
    : null;
  const revokeId = `${assetId}:${clientAddress}:${data.feedbackIndex}`;
  await client.query(
    `INSERT INTO revocations (id, revocation_id, asset, client_address, feedback_index, feedback_hash, slot, original_score, atom_enabled, had_impact, running_digest, revoke_count, tx_index, event_ordinal, tx_signature, created_at, status)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
     ON CONFLICT (asset, client_address, feedback_index) DO UPDATE SET
       feedback_hash = EXCLUDED.feedback_hash,
       slot = EXCLUDED.slot,
       original_score = EXCLUDED.original_score,
       atom_enabled = EXCLUDED.atom_enabled,
       had_impact = EXCLUDED.had_impact,
       running_digest = EXCLUDED.running_digest,
       revoke_count = EXCLUDED.revoke_count,
       tx_index = EXCLUDED.tx_index,
       event_ordinal = EXCLUDED.event_ordinal,
       tx_signature = EXCLUDED.tx_signature,
       status = EXCLUDED.status`,
    [revokeId, null, assetId, clientAddress, data.feedbackIndex.toString(), revokeSealHash,
     data.slot.toString(), data.originalScore, data.atomEnabled, data.hadImpact,
     revokeDigest, data.newRevokeCount.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString(),
     isOrphan ? "ORPHANED" : DEFAULT_STATUS]
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
  await client.query(
    `UPDATE agents SET
       ${baseUpdate}
     WHERE asset = $2`,
    [ctx.blockTime.toISOString(), assetId]
  );
  if (data.atomEnabled && data.hadImpact) {
    await client.query(
      `UPDATE agents SET
         trust_tier = $3,
         quality_score = $4,
         confidence = $5,
         updated_at = $1
       WHERE asset = $2`,
      [ctx.blockTime.toISOString(), assetId, data.newTrustTier, data.newQualityScore, data.newConfidence]
    );
  }
  logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString(), hadImpact: data.hadImpact, orphan: isOrphan }, "Feedback revoked");
}

async function handleResponseAppendedTx(
  client: PoolClient,
  data: ResponseAppended,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const clientAddress = data.client.toBase58();
  const responder = data.responder.toBase58();
  const id = `${assetId}:${clientAddress}:${data.feedbackIndex}:${responder}:${ctx.signature}`;
  const feedbackCheck = await client.query(
    `SELECT id, feedback_hash FROM feedbacks WHERE asset = $1 AND client_address = $2 AND feedback_index = $3 LIMIT 1`,
    [assetId, clientAddress, data.feedbackIndex.toString()]
  );

  const responseHash = data.responseHash
    ? Buffer.from(data.responseHash).toString("hex")
    : null;
  const responseRunningDigest = data.newResponseDigest
    ? Buffer.from(data.newResponseDigest)
    : null;

  if (feedbackCheck.rowCount === 0) {
    logger.warn({ assetId, client: clientAddress, feedbackIndex: data.feedbackIndex.toString() },
      "Feedback not found for response - storing as orphan");
    await client.query(
      `INSERT INTO feedback_responses (id, response_id, asset, client_address, feedback_index, responder, response_uri, response_hash, running_digest, response_count, block_slot, tx_index, event_ordinal, tx_signature, created_at, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
       ON CONFLICT (id) DO NOTHING`,
      [id, null, assetId, clientAddress, data.feedbackIndex.toString(), responder, data.responseUri || null,
       responseHash, responseRunningDigest, data.newResponseCount.toString(),
       ctx.slot.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString(), "ORPHANED"]
    );
    logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString(), responder }, "Orphan response stored");
    return;
  }

  // Validate seal_hash matches stored feedback_hash
  const storedHash = feedbackCheck.rows[0].feedback_hash;
  const eventHash = data.sealHash
    ? Buffer.from(data.sealHash).toString("hex")
    : null;
  const sealMismatch = !hashesMatchHex(storedHash, eventHash);
  if (sealMismatch) {
    logger.warn(
      { assetId, client: clientAddress, feedbackIndex: data.feedbackIndex.toString() },
      "seal_hash mismatch: response sealHash does not match stored feedbackHash"
    );
  }
  const responseStatus = sealMismatch ? "ORPHANED" : DEFAULT_STATUS;
  await client.query(
    `INSERT INTO feedback_responses (id, response_id, asset, client_address, feedback_index, responder, response_uri, response_hash, running_digest, response_count, block_slot, tx_index, event_ordinal, tx_signature, created_at, status)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
     ON CONFLICT (id) DO UPDATE SET
       response_uri = EXCLUDED.response_uri,
       response_hash = EXCLUDED.response_hash,
       running_digest = EXCLUDED.running_digest,
       response_count = EXCLUDED.response_count,
       block_slot = EXCLUDED.block_slot,
       tx_index = EXCLUDED.tx_index,
       event_ordinal = EXCLUDED.event_ordinal,
       tx_signature = EXCLUDED.tx_signature,
       status = EXCLUDED.status`,
    [id, null, assetId, clientAddress, data.feedbackIndex.toString(), responder, data.responseUri || null,
     responseHash, responseRunningDigest, data.newResponseCount.toString(),
     ctx.slot.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString(), responseStatus]
  );
  logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString(), responder }, "Response appended");
}

async function handleValidationRequestedTx(
  client: PoolClient,
  data: ValidationRequested,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const validatorAddress = data.validatorAddress.toBase58();
  const id = `${assetId}:${validatorAddress}:${data.nonce}`;
  await client.query(
    `INSERT INTO validations (id, asset, validator_address, nonce, requester, request_uri, request_hash, status, block_slot, tx_index, event_ordinal, tx_signature, created_at, chain_status)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
     ON CONFLICT (id) DO UPDATE SET
       requester = EXCLUDED.requester,
       request_uri = EXCLUDED.request_uri,
       request_hash = EXCLUDED.request_hash,
       block_slot = EXCLUDED.block_slot,
       tx_index = EXCLUDED.tx_index,
       event_ordinal = EXCLUDED.event_ordinal,
       tx_signature = EXCLUDED.tx_signature`,
    [id, assetId, validatorAddress, data.nonce.toString(), data.requester.toBase58(),
     data.requestUri || null, data.requestHash ? Buffer.from(data.requestHash).toString("hex") : null,
     "PENDING", ctx.slot.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString(), DEFAULT_STATUS]
  );
  logger.info({ assetId, validator: validatorAddress, nonce: data.nonce }, "Validation requested");
}

async function handleValidationRespondedTx(
  client: PoolClient,
  data: ValidationResponded,
  ctx: EventContext
): Promise<void> {
  const assetId = data.asset.toBase58();
  const validatorAddress = data.validatorAddress.toBase58();
  const id = `${assetId}:${validatorAddress}:${data.nonce}`;
  await client.query(
    `INSERT INTO validations (id, asset, validator_address, nonce, response, response_uri, response_hash, tag, status, block_slot, tx_index, event_ordinal, tx_signature, created_at, updated_at, chain_status)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14, $15)
     ON CONFLICT (id) DO UPDATE SET
       response = EXCLUDED.response,
       response_uri = EXCLUDED.response_uri,
       response_hash = EXCLUDED.response_hash,
       tag = EXCLUDED.tag,
       status = EXCLUDED.status,
       block_slot = EXCLUDED.block_slot,
       tx_index = EXCLUDED.tx_index,
       event_ordinal = EXCLUDED.event_ordinal,
       tx_signature = EXCLUDED.tx_signature,
       updated_at = EXCLUDED.updated_at`,
    [id, assetId, validatorAddress, data.nonce.toString(), data.response,
     data.responseUri || null,
     data.responseHash ? Buffer.from(data.responseHash).toString("hex") : null,
     data.tag || null, "RESPONDED",
     ctx.slot.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString(), DEFAULT_STATUS]
  );
  logger.info({ assetId, validator: validatorAddress, nonce: data.nonce, response: data.response }, "Validation responded");
}

async function handleAgentRegistered(
  data: AgentRegistered,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const assetId = data.asset.toBase58();
  const collection = data.collection.toBase58();
  const agentUri = data.agentUri || null;

  await ensureCollection(collection);

  try {
    await db.query(
      `INSERT INTO agents (asset, owner, creator, agent_uri, collection, canonical_col, col_locked, parent_asset, parent_creator, parent_locked, atom_enabled, block_slot, tx_index, event_ordinal, tx_signature, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
       ON CONFLICT (asset) DO UPDATE SET
         owner = EXCLUDED.owner,
         creator = COALESCE(agents.creator, EXCLUDED.creator),
         agent_uri = EXCLUDED.agent_uri,
         atom_enabled = EXCLUDED.atom_enabled,
         block_slot = EXCLUDED.block_slot,
         tx_index = EXCLUDED.tx_index,
         event_ordinal = EXCLUDED.event_ordinal,
         updated_at = EXCLUDED.created_at`,
      [
        assetId,
        data.owner.toBase58(),
        data.owner.toBase58(),
        agentUri,
        collection,
        "",
        false,
        null,
        null,
        false,
        data.atomEnabled,
        ctx.slot.toString(),
        ctx.txIndex ?? null,
        ctx.eventOrdinal ?? null,
        ctx.signature,
        ctx.blockTime.toISOString(),
      ]
    );
    logger.info({ assetId, owner: data.owner.toBase58(), uri: agentUri }, "Agent registered");

    // Trigger URI metadata extraction if configured and URI is present
    if (agentUri && config.metadataIndexMode !== "off") {
      metadataQueue.add(assetId, agentUri);
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
      [data.newOwner.toBase58(), ctx.slot.toString(), ctx.blockTime.toISOString(), assetId]
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
      [ctx.slot.toString(), ctx.blockTime.toISOString(), assetId]
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
      [newUri, ctx.slot.toString(), ctx.blockTime.toISOString(), assetId]
    );
    logger.info({ assetId, newUri }, "Agent URI updated");

    // Trigger URI metadata extraction if configured and URI is present
    if (newUri && config.metadataIndexMode !== "off") {
      metadataQueue.add(assetId, newUri);
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
  // Convert default pubkey to NULL (wallet reset semantics)
  const newWalletRaw = data.newWallet.toBase58();
  const newWallet = newWalletRaw === DEFAULT_PUBKEY ? null : newWalletRaw;

  try {
    await db.query(
      `UPDATE agents SET agent_wallet = $1, block_slot = $2, updated_at = $3 WHERE asset = $4`,
      [newWallet, ctx.slot.toString(), ctx.blockTime.toISOString(), assetId]
    );
    logger.info({ assetId, newWallet: newWallet ?? "(reset)" }, "Agent wallet updated");
  } catch (error: any) {
    logger.error({ error: error.message, assetId }, "Failed to update wallet");
  }
}

async function handleWalletResetOnOwnerSync(
  data: WalletResetOnOwnerSync,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const assetId = data.asset.toBase58();
  const newWalletRaw = data.newWallet.toBase58();
  const newWallet = newWalletRaw === DEFAULT_PUBKEY ? null : newWalletRaw;

  try {
    await db.query(
      `UPDATE agents SET owner = $1, agent_wallet = $2, block_slot = $3, updated_at = $4 WHERE asset = $5`,
      [data.ownerAfterSync.toBase58(), newWallet, ctx.slot.toString(), ctx.blockTime.toISOString(), assetId]
    );
    logger.info({ assetId, ownerAfterSync: data.ownerAfterSync.toBase58(), newWallet: newWallet ?? "(reset)" }, "Wallet reset on owner sync");
  } catch (error: any) {
    logger.error({ error: error.message, assetId }, "Failed wallet reset on owner sync");
  }
}

async function handleCollectionPointerSet(
  data: CollectionPointerSet,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const assetId = data.asset.toBase58();
  const pointer = data.col;
  const setBy = data.setBy.toBase58();
  const lock = typeof data.lock === "boolean" ? data.lock : null;

  try {
    await db.query(
      `WITH previous AS (
         SELECT canonical_col AS prev_col, COALESCE(creator, owner) AS prev_creator
         FROM agents
         WHERE asset = $1
       ),
       updated AS (
         UPDATE agents
         SET canonical_col = $2,
             creator = COALESCE(creator, $3),
             block_slot = $4,
             updated_at = $5,
             col_locked = COALESCE($7, col_locked)
         WHERE asset = $1
         RETURNING 1
       ),
       inserted AS (
         INSERT INTO collection_pointers (
           col, creator, first_seen_asset, first_seen_at, first_seen_slot, first_seen_tx_signature,
           last_seen_at, last_seen_slot, last_seen_tx_signature, asset_count
         )
         SELECT $2, $3, $1, $5, $4, $6, $5, $4, $6, 1
         WHERE EXISTS (SELECT 1 FROM updated)
         ON CONFLICT (col, creator) DO NOTHING
       ),
       dec_old AS (
         UPDATE collection_pointers cp
         SET asset_count = GREATEST(0, cp.asset_count - 1)
         WHERE cp.col = COALESCE((SELECT prev_col FROM previous), '')
           AND cp.creator = COALESCE((SELECT prev_creator FROM previous), '')
           AND cp.col <> ''
           AND (cp.col <> $2 OR cp.creator <> $3)
           AND EXISTS (SELECT 1 FROM updated)
         RETURNING 1
       )
       UPDATE collection_pointers cp
       SET last_seen_at = $5,
           last_seen_slot = $4,
           last_seen_tx_signature = $6,
           asset_count = cp.asset_count + CASE
             WHEN COALESCE((SELECT prev_col FROM previous), '') <> $2
               OR COALESCE((SELECT prev_creator FROM previous), '') <> $3
             THEN 1 ELSE 0 END
       WHERE cp.col = $2
         AND cp.creator = $3
         AND EXISTS (SELECT 1 FROM updated)`,
      [assetId, pointer, setBy, ctx.slot.toString(), ctx.blockTime.toISOString(), ctx.signature, lock]
    );
    logger.info({ assetId, col: pointer, setBy }, "Collection pointer set");

    if (config.collectionMetadataIndexEnabled) {
      collectionMetadataQueue.add(assetId, pointer);
    }
  } catch (error: any) {
    logger.error({ error: error.message, assetId }, "Failed collection pointer set");
  }
}

async function handleParentAssetSet(
  data: ParentAssetSet,
  ctx: EventContext
): Promise<void> {
  const db = getPool();
  const assetId = data.asset.toBase58();
  const lock = typeof data.lock === "boolean" ? data.lock : null;

  try {
    await db.query(
      `UPDATE agents
       SET parent_asset = $1,
           parent_creator = $2,
           block_slot = $3,
           updated_at = $4,
           parent_locked = COALESCE($6, parent_locked)
       WHERE asset = $5`,
      [
        data.parentAsset.toBase58(),
        data.parentCreator.toBase58(),
        ctx.slot.toString(),
        ctx.blockTime.toISOString(),
        assetId,
        lock,
      ]
    );
    logger.info({
      assetId,
      parentAsset: data.parentAsset.toBase58(),
      parentCreator: data.parentCreator.toBase58(),
      setBy: data.setBy.toBase58(),
    }, "Parent asset set");
  } catch (error: any) {
    logger.error({ error: error.message, assetId }, "Failed parent asset set");
  }
}

async function handleMetadataSet(
  data: MetadataSet,
  ctx: EventContext
): Promise<void> {
  // Skip _uri: prefix (reserved for indexer-derived metadata)
  if (data.key.startsWith("_uri:")) {
    logger.warn({ assetId: data.asset.toBase58(), key: data.key }, "Skipping reserved _uri: prefix");
    return;
  }

  const db = getPool();
  const assetId = data.asset.toBase58();
  // FIX: Calculate key_hash from key (sha256(key)[0..16]), not from value
  const keyHash = createHash("sha256").update(data.key).digest().slice(0, 16).toString("hex");
  const id = `${assetId}:${keyHash}`;

  try {
    // Compress value for storage (threshold: 256 bytes)
    const compressedValue = await compressForStorage(stripNullBytes(data.value));

    await db.query(
      `INSERT INTO metadata (id, asset, key, key_hash, value, immutable, block_slot, tx_index, event_ordinal, tx_signature, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
       ON CONFLICT (id) DO UPDATE SET
         value = EXCLUDED.value,
         immutable = metadata.immutable OR EXCLUDED.immutable,
         block_slot = EXCLUDED.block_slot,
         tx_index = EXCLUDED.tx_index,
         event_ordinal = EXCLUDED.event_ordinal,
         updated_at = EXCLUDED.updated_at
       WHERE NOT metadata.immutable`,
      [id, assetId, data.key, keyHash, compressedValue, data.immutable, ctx.slot.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString()]
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

// v0.6.0: RegistryInitialized replaces BaseRegistryCreated/UserRegistryCreated
async function handleRegistryInitialized(
  data: RegistryInitialized,
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
      [collection, "BASE", data.authority.toBase58(), ctx.blockTime.toISOString()]
    );
    logger.info({ collection }, "Registry initialized");
  } catch (error: any) {
    logger.error({ error: error.message, collection }, "Failed to initialize registry");
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
    const feedbackHash = data.sealHash
      ? Buffer.from(data.sealHash).toString("hex")
      : null;

    const runningDigest = data.newFeedbackDigest
      ? Buffer.from(data.newFeedbackDigest)
      : null;

    const insertResult = await db.query(
      `INSERT INTO feedbacks (id, feedback_id, asset, client_address, feedback_index, value, value_decimals, score, tag1, tag2, endpoint, feedback_uri, feedback_hash,
         running_digest, is_revoked, block_slot, tx_index, event_ordinal, tx_signature, created_at, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
       ON CONFLICT (id) DO NOTHING`,
      [
        id, null, assetId, clientAddress, data.feedbackIndex.toString(),
        data.value.toString(), data.valueDecimals, data.score,
        data.tag1 || null, data.tag2 || null, data.endpoint || null, data.feedbackUri || null,
        feedbackHash, runningDigest,
        false, ctx.slot.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString(), DEFAULT_STATUS
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
    // Check feedback exists and validate seal_hash
    const feedbackCheck = await db.query(
      `SELECT id, feedback_hash FROM feedbacks WHERE id = $1 LIMIT 1`,
      [id]
    );
    if (feedbackCheck.rowCount === 0) {
      logger.warn(
        { assetId, client: clientAddress, feedbackIndex: data.feedbackIndex.toString() },
        "Feedback not found for revocation (orphan revoke)"
      );
    } else {
      const storedHash = feedbackCheck.rows[0].feedback_hash;
      const eventHash = data.sealHash
        ? Buffer.from(data.sealHash).toString("hex")
        : null;
      if (!hashesMatchHex(storedHash, eventHash)) {
        logger.warn(
          { assetId, client: clientAddress, feedbackIndex: data.feedbackIndex.toString() },
          "seal_hash mismatch: revocation sealHash does not match stored feedbackHash"
        );
      }
    }

    await db.query(
      `UPDATE feedbacks SET
         is_revoked = true,
         revoked_at = $1
       WHERE id = $2`,
      [ctx.blockTime.toISOString(), id]
    );

    const isOrphan = feedbackCheck.rowCount === 0;
    const revokeDigest = data.newRevokeDigest
      ? Buffer.from(data.newRevokeDigest)
      : null;
    const revokeSealHash = data.sealHash
      ? Buffer.from(data.sealHash).toString("hex")
      : null;
    await db.query(
      `INSERT INTO revocations (id, revocation_id, asset, client_address, feedback_index, feedback_hash, slot, original_score, atom_enabled, had_impact, running_digest, revoke_count, tx_index, event_ordinal, tx_signature, created_at, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
       ON CONFLICT (asset, client_address, feedback_index) DO UPDATE SET
         feedback_hash = EXCLUDED.feedback_hash,
         slot = EXCLUDED.slot,
         original_score = EXCLUDED.original_score,
         atom_enabled = EXCLUDED.atom_enabled,
         had_impact = EXCLUDED.had_impact,
         running_digest = EXCLUDED.running_digest,
         revoke_count = EXCLUDED.revoke_count,
         tx_index = EXCLUDED.tx_index,
         event_ordinal = EXCLUDED.event_ordinal,
         tx_signature = EXCLUDED.tx_signature,
         status = EXCLUDED.status`,
      [id, null, assetId, clientAddress, data.feedbackIndex.toString(), revokeSealHash,
       data.slot.toString(), data.originalScore, data.atomEnabled, data.hadImpact,
       revokeDigest, data.newRevokeCount.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString(),
       isOrphan ? "ORPHANED" : DEFAULT_STATUS]
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

    logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString(), hadImpact: data.hadImpact, orphan: isOrphan }, "Feedback revoked");
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
  const id = `${assetId}:${clientAddress}:${data.feedbackIndex}:${responder}:${ctx.signature}`;

  try {
    const feedbackCheck = await db.query(
      `SELECT id, feedback_hash FROM feedbacks WHERE asset = $1 AND client_address = $2 AND feedback_index = $3 LIMIT 1`,
      [assetId, clientAddress, data.feedbackIndex.toString()]
    );

    const responseHash = data.responseHash
      ? Buffer.from(data.responseHash).toString("hex")
      : null;
    const responseRunningDigest = data.newResponseDigest
      ? Buffer.from(data.newResponseDigest)
      : null;

    if (feedbackCheck.rowCount === 0) {
      logger.warn({ assetId, client: clientAddress, feedbackIndex: data.feedbackIndex.toString() },
        "Feedback not found for response - storing as orphan");
      await db.query(
        `INSERT INTO feedback_responses (id, response_id, asset, client_address, feedback_index, responder, response_uri, response_hash, running_digest, response_count, block_slot, tx_index, event_ordinal, tx_signature, created_at, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
         ON CONFLICT (id) DO NOTHING`,
        [id, null, assetId, clientAddress, data.feedbackIndex.toString(), responder, data.responseUri || null,
         responseHash, responseRunningDigest, data.newResponseCount.toString(),
         ctx.slot.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString(), "ORPHANED"]
      );
      logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString(), responder }, "Orphan response stored");
      return;
    }

    // Validate seal_hash matches stored feedback_hash
    const storedHash = feedbackCheck.rows[0].feedback_hash;
    const eventHash = data.sealHash
      ? Buffer.from(data.sealHash).toString("hex")
      : null;
    const sealMismatch = !hashesMatchHex(storedHash, eventHash);
    if (sealMismatch) {
      logger.warn(
        { assetId, client: clientAddress, feedbackIndex: data.feedbackIndex.toString() },
        "seal_hash mismatch: response sealHash does not match stored feedbackHash"
      );
    }
    const responseStatus = sealMismatch ? "ORPHANED" : DEFAULT_STATUS;
    await db.query(
      `INSERT INTO feedback_responses (id, response_id, asset, client_address, feedback_index, responder, response_uri, response_hash, running_digest, response_count, block_slot, tx_index, event_ordinal, tx_signature, created_at, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
       ON CONFLICT (id) DO UPDATE SET
         response_uri = EXCLUDED.response_uri,
         response_hash = EXCLUDED.response_hash,
         running_digest = EXCLUDED.running_digest,
         response_count = EXCLUDED.response_count,
         block_slot = EXCLUDED.block_slot,
         tx_index = EXCLUDED.tx_index,
         event_ordinal = EXCLUDED.event_ordinal,
         tx_signature = EXCLUDED.tx_signature,
         status = EXCLUDED.status`,
      [id, null, assetId, clientAddress, data.feedbackIndex.toString(), responder, data.responseUri || null,
       responseHash, responseRunningDigest, data.newResponseCount.toString(),
       ctx.slot.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString(), responseStatus]
    );
    logger.info({ assetId, feedbackIndex: data.feedbackIndex.toString(), responder }, "Response appended");
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
      `INSERT INTO validations (id, asset, validator_address, nonce, requester, request_uri, request_hash, status, block_slot, tx_index, event_ordinal, tx_signature, created_at, chain_status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
       ON CONFLICT (id) DO UPDATE SET
         requester = EXCLUDED.requester,
         request_uri = EXCLUDED.request_uri,
         request_hash = EXCLUDED.request_hash,
         block_slot = EXCLUDED.block_slot,
         tx_index = EXCLUDED.tx_index,
         event_ordinal = EXCLUDED.event_ordinal,
         tx_signature = EXCLUDED.tx_signature`,
      [id, assetId, validatorAddress, data.nonce.toString(), data.requester.toBase58(),
       data.requestUri || null, data.requestHash ? Buffer.from(data.requestHash).toString("hex") : null,
       "PENDING", ctx.slot.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString(), DEFAULT_STATUS]
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
    // Use UPSERT to handle case where request wasn't indexed (DB reset, late start, etc.)
    await db.query(
      `INSERT INTO validations (id, asset, validator_address, nonce, response, response_uri, response_hash, tag, status, block_slot, tx_index, event_ordinal, tx_signature, created_at, updated_at, chain_status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14, $15)
       ON CONFLICT (id) DO UPDATE SET
         response = EXCLUDED.response,
         response_uri = EXCLUDED.response_uri,
         response_hash = EXCLUDED.response_hash,
         tag = EXCLUDED.tag,
         status = EXCLUDED.status,
         block_slot = EXCLUDED.block_slot,
         tx_index = EXCLUDED.tx_index,
         event_ordinal = EXCLUDED.event_ordinal,
         tx_signature = EXCLUDED.tx_signature,
         updated_at = EXCLUDED.updated_at`,
      [id, assetId, validatorAddress, data.nonce.toString(), data.response,
       data.responseUri || null,
       data.responseHash ? Buffer.from(data.responseHash).toString("hex") : null,
       data.tag || null, "RESPONDED",
       ctx.slot.toString(), ctx.txIndex ?? null, ctx.eventOrdinal ?? null, ctx.signature, ctx.blockTime.toISOString(), DEFAULT_STATUS]
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
export async function saveIndexerState(
  signature: string,
  slot: bigint,
  source: "poller" | "websocket" | "substreams" = "poller"
): Promise<void> {
  const db = getPool();
  try {
    await db.query(
      `INSERT INTO indexer_state (id, last_signature, last_slot, source, updated_at)
       VALUES ('main', $1, $2, $3, NOW())
       ON CONFLICT (id) DO UPDATE SET
         last_signature = EXCLUDED.last_signature,
         last_slot = EXCLUDED.last_slot,
         source = EXCLUDED.source,
         updated_at = NOW()
       WHERE indexer_state.last_slot IS NULL OR indexer_state.last_slot <= EXCLUDED.last_slot`,
      [signature, slot.toString(), source]
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
import { stripNullBytes } from "../utils/sanitize.js";
import { metadataQueue } from "../indexer/metadata-queue.js";
import { collectionMetadataQueue } from "../indexer/collection-metadata-queue.js";

/**
 * Fetch, digest, and store URI metadata for an agent
 * Called asynchronously after agent registration or URI update
 *
 * RACE CONDITION PROTECTION: Because URI fetches are queued and network latency varies,
 * two consecutive URI updates (block N and N+1) might complete out of order.
 * We check if the agent's current URI matches before writing to prevent stale overwrites.
 */
export async function digestAndStoreUriMetadata(assetId: string, uri: string): Promise<void> {
  if (config.metadataIndexMode === "off") {
    return;
  }

  const db = getPool();

  // RACE CONDITION CHECK: Verify URI hasn't changed while we were queued/fetching
  // This prevents stale data from overwriting newer data due to out-of-order completion
  try {
    const agentResult = await db.query(
      `SELECT agent_uri FROM agents WHERE asset = $1`,
      [assetId]
    );
    if (agentResult.rows.length === 0) {
      logger.debug({ assetId, uri }, "Agent no longer exists, skipping URI digest");
      return;
    }
    if (agentResult.rows[0].agent_uri !== uri) {
      logger.debug({
        assetId,
        expectedUri: uri,
        currentUri: agentResult.rows[0].agent_uri
      }, "Agent URI changed while processing, skipping stale write");
      return;
    }
  } catch (error: any) {
    logger.warn({ assetId, error: error.message }, "Failed to check agent URI freshness, aborting to prevent stale overwrite");
    return;
  }

  // Purge old URI-derived metadata before storing new ones
  // Uses "_uri:" prefix to avoid collision with user's on-chain metadata
  try {
    await db.query(
      `DELETE FROM metadata
       WHERE asset = $1
         AND key LIKE '\\_uri:%' ESCAPE '\\'
         AND NOT immutable`,
      [assetId]
    );
    logger.debug({ assetId }, "Purged old URI metadata");
  } catch (error: any) {
    logger.warn({ assetId, error: error.message }, "Failed to purge old URI metadata");
  }

  const result = await digestUri(uri);

  if (result.status !== "ok" || !result.fields) {
    logger.debug({ assetId, uri, status: result.status, error: result.error }, "URI digest failed or empty");
    // Store error status as metadata
    await storeUriMetadata(assetId, "_uri:_status", JSON.stringify({
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
      await storeUriMetadata(assetId, `${key}_meta`, JSON.stringify({
        status: "oversize",
        bytes: serialized.bytes,
        sha256: result.hash,
      }));
    } else {
      await storeUriMetadata(assetId, key, serialized.value);
    }
  }

  // Store success status with truncation info
  await storeUriMetadata(assetId, "_uri:_status", JSON.stringify({
    status: "ok",
    bytes: result.bytes,
    hash: result.hash,
    fieldCount: Object.keys(result.fields).length,
    truncatedKeys: result.truncatedKeys || false,
  }));

  // Sync nft_name from _uri:name if not already set
  const uriName = result.fields["_uri:name"];
  if (uriName && typeof uriName === "string") {
    try {
      await db.query(
        `UPDATE agents SET nft_name = $1 WHERE asset = $2 AND (nft_name IS NULL OR nft_name = '')`,
        [uriName, assetId]
      );
      logger.debug({ assetId, name: uriName }, "Synced nft_name from URI metadata");
    } catch (error: any) {
      logger.warn({ assetId, error: error.message }, "Failed to sync nft_name");
    }
  }

  logger.info({ assetId, uri, fieldCount: Object.keys(result.fields).length }, "URI metadata indexed");
}

// Re-export from shared constants for URI metadata storage
import { STANDARD_URI_FIELDS } from "../constants.js";

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
         updated_at = NOW()
       WHERE NOT metadata.immutable`,
      [id, assetId, key, keyHash, storedValue]
    );
  } catch (error: any) {
    logger.error({ error: error.message, assetId, key }, "Failed to store URI metadata");
    throw error;
  }
}
