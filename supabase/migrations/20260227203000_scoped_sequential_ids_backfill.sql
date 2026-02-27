-- Add scoped sequential IDs for feedback, responses, and revocations.
-- Guarantee: all non-orphan rows have a non-null scoped sequential ID.

ALTER TABLE feedbacks
ADD COLUMN IF NOT EXISTS feedback_id BIGINT;

ALTER TABLE feedback_responses
ADD COLUMN IF NOT EXISTS response_id BIGINT;

ALTER TABLE revocations
ADD COLUMN IF NOT EXISTS revocation_id BIGINT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_feedbacks_asset_feedback_id_unique
ON feedbacks(asset, feedback_id);

CREATE INDEX IF NOT EXISTS idx_feedbacks_asset_feedback_id
ON feedbacks(asset, feedback_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_responses_scope_response_id_unique
ON feedback_responses(asset, client_address, feedback_index, response_id);

CREATE INDEX IF NOT EXISTS idx_responses_scope_response_id
ON feedback_responses(asset, client_address, feedback_index, response_id);

CREATE INDEX IF NOT EXISTS idx_responses_response_id
ON feedback_responses(response_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_revocations_asset_revocation_id
ON revocations(asset, revocation_id)
WHERE revocation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_revocations_asset_revocation_id_lookup
ON revocations(asset, revocation_id);

-- Backfill feedback_id deterministically per agent (asset), preserving existing IDs.
WITH feedback_seed AS (
  SELECT asset, COALESCE(MAX(feedback_id), 0) AS max_id
  FROM feedbacks
  WHERE feedback_id IS NOT NULL
  GROUP BY asset
),
feedback_pending AS (
  SELECT
    f.id,
    f.asset,
    ROW_NUMBER() OVER (
      PARTITION BY f.asset
      ORDER BY
        f.block_slot ASC NULLS LAST,
        f.tx_signature ASC NULLS LAST,
        f.tx_index ASC NULLS LAST,
        f.event_ordinal ASC NULLS LAST,
        f.client_address ASC,
        f.feedback_index ASC,
        f.id ASC
    ) AS rn
  FROM feedbacks f
  WHERE f.status != 'ORPHANED'
    AND f.feedback_id IS NULL
),
feedback_assigned AS (
  SELECT
    p.id,
    (COALESCE(s.max_id, 0) + p.rn)::bigint AS feedback_id
  FROM feedback_pending p
  LEFT JOIN feedback_seed s ON s.asset = p.asset
)
UPDATE feedbacks f
SET feedback_id = a.feedback_id
FROM feedback_assigned a
WHERE f.id = a.id;

-- Backfill response_id deterministically per feedback scope, preserving existing IDs.
WITH response_seed AS (
  SELECT
    asset,
    client_address,
    feedback_index,
    COALESCE(MAX(response_id), 0) AS max_id
  FROM feedback_responses
  WHERE response_id IS NOT NULL
  GROUP BY asset, client_address, feedback_index
),
response_pending AS (
  SELECT
    fr.id,
    fr.asset,
    fr.client_address,
    fr.feedback_index,
    ROW_NUMBER() OVER (
      PARTITION BY fr.asset, fr.client_address, fr.feedback_index
      ORDER BY
        fr.response_count ASC NULLS LAST,
        fr.block_slot ASC NULLS LAST,
        fr.tx_signature ASC NULLS LAST,
        fr.tx_index ASC NULLS LAST,
        fr.event_ordinal ASC NULLS LAST,
        fr.responder ASC,
        fr.id ASC
    ) AS rn
  FROM feedback_responses fr
  WHERE fr.status != 'ORPHANED'
    AND fr.response_id IS NULL
),
response_assigned AS (
  SELECT
    p.id,
    (COALESCE(s.max_id, 0) + p.rn)::bigint AS response_id
  FROM response_pending p
  LEFT JOIN response_seed s
    ON s.asset = p.asset
   AND s.client_address = p.client_address
   AND s.feedback_index = p.feedback_index
)
UPDATE feedback_responses fr
SET response_id = a.response_id
FROM response_assigned a
WHERE fr.id = a.id;

-- Backfill revocation_id deterministically per agent (asset), preserving existing IDs.
WITH revocation_seed AS (
  SELECT asset, COALESCE(MAX(revocation_id), 0) AS max_id
  FROM revocations
  WHERE revocation_id IS NOT NULL
  GROUP BY asset
),
revocation_pending AS (
  SELECT
    r.id,
    r.asset,
    ROW_NUMBER() OVER (
      PARTITION BY r.asset
      ORDER BY
        r.revoke_count ASC NULLS LAST,
        r.slot ASC NULLS LAST,
        r.tx_signature ASC NULLS LAST,
        r.tx_index ASC NULLS LAST,
        r.event_ordinal ASC NULLS LAST,
        r.client_address ASC,
        r.feedback_index ASC,
        r.id ASC
    ) AS rn
  FROM revocations r
  WHERE r.status != 'ORPHANED'
    AND r.revocation_id IS NULL
),
revocation_assigned AS (
  SELECT
    p.id,
    (COALESCE(s.max_id, 0) + p.rn)::bigint AS revocation_id
  FROM revocation_pending p
  LEFT JOIN revocation_seed s ON s.asset = p.asset
)
UPDATE revocations r
SET revocation_id = a.revocation_id
FROM revocation_assigned a
WHERE r.id = a.id;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'feedbacks_non_orphan_feedback_id_required'
  ) THEN
    ALTER TABLE feedbacks
      ADD CONSTRAINT feedbacks_non_orphan_feedback_id_required
      CHECK (status = 'ORPHANED' OR feedback_id IS NOT NULL) NOT VALID;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'feedback_responses_non_orphan_response_id_required'
  ) THEN
    ALTER TABLE feedback_responses
      ADD CONSTRAINT feedback_responses_non_orphan_response_id_required
      CHECK (status = 'ORPHANED' OR response_id IS NOT NULL) NOT VALID;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'revocations_non_orphan_revocation_id_required'
  ) THEN
    ALTER TABLE revocations
      ADD CONSTRAINT revocations_non_orphan_revocation_id_required
      CHECK (status = 'ORPHANED' OR revocation_id IS NOT NULL) NOT VALID;
  END IF;
END
$$;

ALTER TABLE feedbacks
  VALIDATE CONSTRAINT feedbacks_non_orphan_feedback_id_required;

ALTER TABLE feedback_responses
  VALIDATE CONSTRAINT feedback_responses_non_orphan_response_id_required;

ALTER TABLE revocations
  VALIDATE CONSTRAINT revocations_non_orphan_revocation_id_required;
