import { config } from "../config.js";
import { getPool } from "./supabase.js";
import type {
  ProofPassFeedbackCandidate,
  ProofPassFeedbackMatch,
} from "../extras/proofpass.js";

export const MISSING_PROOFPASS_SCHEMA_FATAL_MESSAGE =
  "ENABLE_PROOFPASS requires the extra_proofpass_feedbacks PostgreSQL schema. Apply supabase/migrations/20260309192500_add_extra_proofpass_feedbacks.sql before starting the indexer.";
export const MISSING_PROOFPASS_SCHEMA_WRITE_PERMISSIONS_FATAL_MESSAGE =
  "ENABLE_PROOFPASS requires INSERT and UPDATE privileges on public.extra_proofpass_feedbacks before starting the indexer.";
export const MISSING_PROOFPASS_SCHEMA_CONFLICT_TARGET_FATAL_MESSAGE =
  "ENABLE_PROOFPASS requires a UNIQUE(asset, client_address, feedback_index, tx_signature) constraint on public.extra_proofpass_feedbacks before starting the indexer.";

const PROOFPASS_BACKFILL_STATE_ID = "proofpass-backfill";
const PROOFPASS_BACKFILL_RETRY_STATE_PREFIX = "proofpass-backfill-retry:";

export interface ProofPassBackfillCursor {
  lastSlot: string | null;
  lastTxIndex: number | null;
  lastSignature: string | null;
}

export interface ProofPassBackfillTx {
  blockSlot: string;
  txIndex: number;
  txSignature: string;
}

function makeProofPassBackfillRetryStateId(tx: ProofPassBackfillTx): string {
  return `${PROOFPASS_BACKFILL_RETRY_STATE_PREFIX}${tx.blockSlot}:${tx.txIndex}:${tx.txSignature}`;
}

export async function assertProofPassSchema(): Promise<void> {
  if (!config.enableProofPass) {
    return;
  }

  if (config.dbMode !== "supabase") {
    throw new Error("ENABLE_PROOFPASS requires DB_MODE=supabase");
  }

  const pool = getPool();
  const client = await pool.connect();
  try {
    await client.query(
      `SELECT
         proofpass_session,
         context_type,
         context_ref_hash,
         block_slot
       FROM extra_proofpass_feedbacks
       LIMIT 0`
    );

    const privileges = await client.query<{ canWrite: boolean }>(
      `SELECT
         has_table_privilege(current_user, 'public.extra_proofpass_feedbacks', 'INSERT')
         AND has_table_privilege(current_user, 'public.extra_proofpass_feedbacks', 'UPDATE') AS "canWrite"`
    );

    if (!privileges.rows[0]?.canWrite) {
      throw new Error(MISSING_PROOFPASS_SCHEMA_WRITE_PERMISSIONS_FATAL_MESSAGE);
    }

    const uniqueConstraint = await client.query<{ hasConflictTarget: boolean }>(
      `SELECT EXISTS (
         SELECT 1
         FROM pg_constraint c
         JOIN pg_class t ON t.oid = c.conrelid
         JOIN pg_namespace n ON n.oid = t.relnamespace
         JOIN unnest(c.conkey) WITH ORDINALITY AS cols(attnum, ord) ON TRUE
         JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = cols.attnum
         WHERE n.nspname = 'public'
           AND t.relname = 'extra_proofpass_feedbacks'
           AND c.contype = 'u'
         GROUP BY c.oid
         HAVING array_agg(a.attname::text ORDER BY cols.ord)
           = ARRAY['asset', 'client_address', 'feedback_index', 'tx_signature']::text[]
       ) AS "hasConflictTarget"`
    );

    if (!uniqueConstraint.rows[0]?.hasConflictTarget) {
      throw new Error(MISSING_PROOFPASS_SCHEMA_CONFLICT_TARGET_FATAL_MESSAGE);
    }
  } catch (error) {
    if (
      error instanceof Error &&
      (
        error.message === MISSING_PROOFPASS_SCHEMA_WRITE_PERMISSIONS_FATAL_MESSAGE
        || error.message === MISSING_PROOFPASS_SCHEMA_CONFLICT_TARGET_FATAL_MESSAGE
      )
    ) {
      throw error;
    }
    throw new Error(MISSING_PROOFPASS_SCHEMA_FATAL_MESSAGE, { cause: error });
  } finally {
    client.release();
  }
}

export async function upsertProofPassMatches(
  matches: ProofPassFeedbackMatch[]
): Promise<void> {
  if (!config.enableProofPass || matches.length === 0) {
    return;
  }

  if (config.dbMode !== "supabase") {
    throw new Error("ENABLE_PROOFPASS requires DB_MODE=supabase");
  }

  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    for (const match of matches) {
      await client.query(
        `INSERT INTO extra_proofpass_feedbacks (
           id,
           asset,
           client_address,
           feedback_index,
           tx_signature,
           feedback_hash,
           proofpass_session,
           context_type,
           context_ref_hash,
           block_slot,
           created_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
         ON CONFLICT (asset, client_address, feedback_index, tx_signature)
         DO UPDATE SET
           feedback_hash = EXCLUDED.feedback_hash,
           proofpass_session = EXCLUDED.proofpass_session,
           context_type = EXCLUDED.context_type,
           context_ref_hash = EXCLUDED.context_ref_hash,
           block_slot = EXCLUDED.block_slot`,
        [
          match.id,
          match.asset,
          match.clientAddress,
          match.feedbackIndex,
          match.txSignature,
          match.feedbackHash,
          match.proofpassSession,
          match.contextType,
          match.contextRefHash,
          match.blockSlot,
        ]
      );
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function getProofPassBackfillCursor(): Promise<ProofPassBackfillCursor> {
  const pool = getPool();
  const result = await pool.query<{
    lastSlot: string | null;
    lastTxIndex: number | null;
    lastSignature: string | null;
  }>(
    `SELECT
       last_slot AS "lastSlot",
       last_tx_index AS "lastTxIndex",
       last_signature AS "lastSignature"
     FROM indexer_state
     WHERE id = $1`,
    [PROOFPASS_BACKFILL_STATE_ID]
  );

  return result.rows[0] ?? {
    lastSlot: null,
    lastTxIndex: null,
    lastSignature: null,
  };
}

export async function listNextProofPassBackfillTxs(
  limit: number,
  cursor: ProofPassBackfillCursor
): Promise<ProofPassBackfillTx[]> {
  const pool = getPool();
  const result = await pool.query<ProofPassBackfillTx>(
    `SELECT DISTINCT ON (f.block_slot, COALESCE(f.tx_index, -1), f.tx_signature)
       f.block_slot::text AS "blockSlot",
       COALESCE(f.tx_index, -1) AS "txIndex",
       f.tx_signature AS "txSignature"
     FROM feedbacks f
     WHERE f.status != 'ORPHANED'
       AND f.tx_signature IS NOT NULL
       AND f.feedback_hash IS NOT NULL
       AND NOT EXISTS (
         SELECT 1
         FROM extra_proofpass_feedbacks e
         WHERE e.asset = f.asset
           AND e.client_address = f.client_address
           AND e.feedback_index = f.feedback_index
           AND e.tx_signature = f.tx_signature
           AND e.feedback_hash = f.feedback_hash
       )
       AND (
         $1::bigint IS NULL
         OR f.block_slot > $1::bigint
         OR (
           f.block_slot = $1::bigint
           AND COALESCE(f.tx_index, -1) > COALESCE($2::int, -1)
         )
         OR (
           f.block_slot = $1::bigint
           AND COALESCE(f.tx_index, -1) = COALESCE($2::int, -1)
           AND f.tx_signature > COALESCE($3::text, '')
         )
       )
     ORDER BY f.block_slot ASC, COALESCE(f.tx_index, -1) ASC, f.tx_signature ASC
     LIMIT $4`,
    [cursor.lastSlot, cursor.lastTxIndex, cursor.lastSignature, limit]
  );

  return result.rows;
}

export async function listMissingProofPassFeedbackCandidatesBySignature(
  signatures: string[]
): Promise<ProofPassFeedbackCandidate[]> {
  if (signatures.length === 0) {
    return [];
  }

  const pool = getPool();
  const result = await pool.query<ProofPassFeedbackCandidate>(
    `SELECT
       f.asset,
       f.client_address AS "clientAddress",
       f.feedback_index::text AS "feedbackIndex",
       f.tx_signature AS "txSignature",
       f.block_slot::text AS "blockSlot",
       LOWER(f.feedback_hash) AS "feedbackHash"
     FROM feedbacks f
     WHERE f.status != 'ORPHANED'
       AND f.tx_signature = ANY($1::text[])
       AND f.feedback_hash IS NOT NULL
       AND NOT EXISTS (
         SELECT 1
         FROM extra_proofpass_feedbacks e
         WHERE e.asset = f.asset
           AND e.client_address = f.client_address
           AND e.feedback_index = f.feedback_index
           AND e.tx_signature = f.tx_signature
           AND e.feedback_hash = f.feedback_hash
       )
     ORDER BY f.block_slot ASC, COALESCE(f.tx_index, -1) ASC, COALESCE(f.event_ordinal, -1) ASC, f.tx_signature ASC`,
    [signatures]
  );

  return result.rows;
}

export async function listProofPassBackfillRetryTxs(
  limit: number
): Promise<ProofPassBackfillTx[]> {
  if (limit <= 0) {
    return [];
  }

  const pool = getPool();
  const result = await pool.query<ProofPassBackfillTx>(
    `SELECT
       last_slot::text AS "blockSlot",
       COALESCE(last_tx_index, -1) AS "txIndex",
       last_signature AS "txSignature"
     FROM indexer_state
     WHERE id LIKE $1
       AND last_slot IS NOT NULL
       AND last_signature IS NOT NULL
     ORDER BY updated_at ASC, last_slot ASC, COALESCE(last_tx_index, -1) ASC, last_signature ASC
     LIMIT $2`,
    [`${PROOFPASS_BACKFILL_RETRY_STATE_PREFIX}%`, limit]
  );

  return result.rows;
}

export async function saveProofPassBackfillRetryTx(
  tx: ProofPassBackfillTx
): Promise<void> {
  const pool = getPool();
  await pool.query(
    `INSERT INTO indexer_state (
       id,
       last_signature,
       last_slot,
       last_tx_index,
       source,
       updated_at
     )
     VALUES ($1, $2, $3::bigint, $4, 'proofpass-backfill-retry', NOW())
     ON CONFLICT (id)
     DO UPDATE SET
       last_signature = EXCLUDED.last_signature,
       last_slot = EXCLUDED.last_slot,
       last_tx_index = EXCLUDED.last_tx_index,
       source = EXCLUDED.source,
       updated_at = NOW()`,
    [makeProofPassBackfillRetryStateId(tx), tx.txSignature, tx.blockSlot, tx.txIndex]
  );
}

export async function deleteProofPassBackfillRetryTx(
  tx: ProofPassBackfillTx
): Promise<void> {
  const pool = getPool();
  await pool.query(`DELETE FROM indexer_state WHERE id = $1`, [
    makeProofPassBackfillRetryStateId(tx),
  ]);
}

export async function saveProofPassBackfillCursor(
  tx: ProofPassBackfillTx
): Promise<void> {
  const pool = getPool();
  await pool.query(
    `INSERT INTO indexer_state (
       id,
       last_signature,
       last_slot,
       last_tx_index,
       source,
       updated_at
     )
     VALUES ($1, $2, $3::bigint, $4, 'proofpass-backfill', NOW())
     ON CONFLICT (id)
     DO UPDATE SET
       last_signature = EXCLUDED.last_signature,
       last_slot = EXCLUDED.last_slot,
       last_tx_index = EXCLUDED.last_tx_index,
       source = EXCLUDED.source,
       updated_at = NOW()`,
    [PROOFPASS_BACKFILL_STATE_ID, tx.txSignature, tx.blockSlot, tx.txIndex]
  );
}
