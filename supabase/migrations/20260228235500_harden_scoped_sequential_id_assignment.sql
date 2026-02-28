-- Harden scoped sequential IDs at DB level for multi-writer safety.
-- Covers feedback_id (per asset), response_id (per asset+client+feedback_index),
-- and revocation_id (per asset).

DROP TRIGGER IF EXISTS trg_assign_feedback_id ON feedbacks;
DROP TRIGGER IF EXISTS trg_assign_response_id ON feedback_responses;
DROP TRIGGER IF EXISTS trg_assign_revocation_id ON revocations;

DROP FUNCTION IF EXISTS assign_feedback_id();
DROP FUNCTION IF EXISTS assign_response_id();
DROP FUNCTION IF EXISTS assign_revocation_id();

CREATE OR REPLACE FUNCTION assign_feedback_id()
RETURNS TRIGGER AS $$
DECLARE
  existing_feedback_id BIGINT;
BEGIN
  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    NEW.feedback_id := NULL;
    RETURN NEW;
  END IF;

  IF TG_OP = 'UPDATE' THEN
    IF OLD.feedback_id IS NOT NULL THEN
      NEW.feedback_id := OLD.feedback_id;
      RETURN NEW;
    END IF;

    IF NEW.feedback_id IS NOT NULL THEN
      RETURN NEW;
    END IF;
  END IF;

  -- Serialize per-asset assignment across processes.
  PERFORM pg_advisory_xact_lock(hashtextextended('feedback:' || NEW.asset, 0));

  SELECT feedback_id
  INTO existing_feedback_id
  FROM feedbacks
  WHERE id = NEW.id
  LIMIT 1;

  IF FOUND AND existing_feedback_id IS NOT NULL THEN
    NEW.feedback_id := existing_feedback_id;
    RETURN NEW;
  END IF;

  SELECT COALESCE(MAX(feedback_id), 0) + 1
  INTO NEW.feedback_id
  FROM feedbacks
  WHERE asset = NEW.asset
    AND feedback_id IS NOT NULL;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_response_id()
RETURNS TRIGGER AS $$
DECLARE
  existing_response_id BIGINT;
BEGIN
  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    NEW.response_id := NULL;
    RETURN NEW;
  END IF;

  IF TG_OP = 'UPDATE' THEN
    IF OLD.response_id IS NOT NULL THEN
      NEW.response_id := OLD.response_id;
      RETURN NEW;
    END IF;

    IF NEW.response_id IS NOT NULL THEN
      RETURN NEW;
    END IF;
  END IF;

  -- Serialize per-feedback-scope assignment across processes.
  PERFORM pg_advisory_xact_lock(
    hashtextextended(
      'response:' || NEW.asset || ':' || NEW.client_address || ':' || NEW.feedback_index::text,
      0
    )
  );

  SELECT response_id
  INTO existing_response_id
  FROM feedback_responses
  WHERE id = NEW.id
  LIMIT 1;

  IF FOUND AND existing_response_id IS NOT NULL THEN
    NEW.response_id := existing_response_id;
    RETURN NEW;
  END IF;

  SELECT COALESCE(MAX(response_id), 0) + 1
  INTO NEW.response_id
  FROM feedback_responses
  WHERE asset = NEW.asset
    AND client_address = NEW.client_address
    AND feedback_index = NEW.feedback_index
    AND response_id IS NOT NULL;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_revocation_id()
RETURNS TRIGGER AS $$
DECLARE
  existing_revocation_id BIGINT;
BEGIN
  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    NEW.revocation_id := NULL;
    RETURN NEW;
  END IF;

  IF TG_OP = 'UPDATE' THEN
    IF OLD.revocation_id IS NOT NULL THEN
      NEW.revocation_id := OLD.revocation_id;
      RETURN NEW;
    END IF;

    IF NEW.revocation_id IS NOT NULL THEN
      RETURN NEW;
    END IF;
  END IF;

  -- Serialize per-asset assignment across processes.
  PERFORM pg_advisory_xact_lock(hashtextextended('revocation:' || NEW.asset, 0));

  SELECT revocation_id
  INTO existing_revocation_id
  FROM revocations
  WHERE id = NEW.id
  LIMIT 1;

  IF FOUND AND existing_revocation_id IS NOT NULL THEN
    NEW.revocation_id := existing_revocation_id;
    RETURN NEW;
  END IF;

  SELECT COALESCE(MAX(revocation_id), 0) + 1
  INTO NEW.revocation_id
  FROM revocations
  WHERE asset = NEW.asset
    AND revocation_id IS NOT NULL;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_assign_feedback_id
  BEFORE INSERT OR UPDATE ON feedbacks
  FOR EACH ROW
  EXECUTE FUNCTION assign_feedback_id();

CREATE TRIGGER trg_assign_response_id
  BEFORE INSERT OR UPDATE ON feedback_responses
  FOR EACH ROW
  EXECUTE FUNCTION assign_response_id();

CREATE TRIGGER trg_assign_revocation_id
  BEFORE INSERT OR UPDATE ON revocations
  FOR EACH ROW
  EXECUTE FUNCTION assign_revocation_id();

-- Deterministically backfill non-orphan rows still missing scoped sequential IDs.
WITH feedback_scope_max AS (
  SELECT
    asset,
    COALESCE(MAX(feedback_id), 0) AS max_id
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
        f.created_at ASC NULLS LAST,
        f.block_slot ASC NULLS LAST,
        f.tx_signature ASC NULLS LAST,
        f.tx_index ASC NULLS LAST,
        f.event_ordinal ASC NULLS LAST,
        f.id ASC
    ) AS rn
  FROM feedbacks f
  WHERE f.feedback_id IS NULL
    AND (f.status IS NULL OR f.status != 'ORPHANED')
),
feedback_assigned AS (
  SELECT
    p.id,
    (COALESCE(m.max_id, 0) + p.rn)::bigint AS assigned_feedback_id
  FROM feedback_pending p
  LEFT JOIN feedback_scope_max m ON m.asset = p.asset
)
UPDATE feedbacks f
SET feedback_id = a.assigned_feedback_id
FROM feedback_assigned a
WHERE f.id = a.id
  AND f.feedback_id IS NULL
  AND (f.status IS NULL OR f.status != 'ORPHANED');

WITH response_scope_max AS (
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
        fr.created_at ASC NULLS LAST,
        fr.block_slot ASC NULLS LAST,
        fr.tx_signature ASC NULLS LAST,
        fr.tx_index ASC NULLS LAST,
        fr.event_ordinal ASC NULLS LAST,
        fr.id ASC
    ) AS rn
  FROM feedback_responses fr
  WHERE fr.response_id IS NULL
    AND (fr.status IS NULL OR fr.status != 'ORPHANED')
),
response_assigned AS (
  SELECT
    p.id,
    (COALESCE(m.max_id, 0) + p.rn)::bigint AS assigned_response_id
  FROM response_pending p
  LEFT JOIN response_scope_max m
    ON m.asset = p.asset
   AND m.client_address = p.client_address
   AND m.feedback_index = p.feedback_index
)
UPDATE feedback_responses fr
SET response_id = a.assigned_response_id
FROM response_assigned a
WHERE fr.id = a.id
  AND fr.response_id IS NULL
  AND (fr.status IS NULL OR fr.status != 'ORPHANED');

WITH revocation_scope_max AS (
  SELECT
    asset,
    COALESCE(MAX(revocation_id), 0) AS max_id
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
        r.created_at ASC NULLS LAST,
        r.slot ASC NULLS LAST,
        r.tx_signature ASC NULLS LAST,
        r.tx_index ASC NULLS LAST,
        r.event_ordinal ASC NULLS LAST,
        r.id ASC
    ) AS rn
  FROM revocations r
  WHERE r.revocation_id IS NULL
    AND (r.status IS NULL OR r.status != 'ORPHANED')
),
revocation_assigned AS (
  SELECT
    p.id,
    (COALESCE(m.max_id, 0) + p.rn)::bigint AS assigned_revocation_id
  FROM revocation_pending p
  LEFT JOIN revocation_scope_max m ON m.asset = p.asset
)
UPDATE revocations r
SET revocation_id = a.assigned_revocation_id
FROM revocation_assigned a
WHERE r.id = a.id
  AND r.revocation_id IS NULL
  AND (r.status IS NULL OR r.status != 'ORPHANED');
