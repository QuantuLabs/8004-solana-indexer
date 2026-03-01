-- Enforce gapless sequential IDs across all exposed sequential identifiers.
-- Scopes:
-- - agents.agent_id (global)
-- - feedbacks.feedback_id (per asset)
-- - feedback_responses.response_id (per asset+client+feedback_index)
-- - revocations.revocation_id (per asset)
--
-- Strategy:
-- 1) Replace non-transactional/scan allocators with transactional counters (rollback-safe).
-- 2) Keep replay idempotence by locking canonical identity before allocation.
-- 3) Compact existing IDs densely to remove historical gaps.

BEGIN;

CREATE TABLE IF NOT EXISTS id_counters (
  scope TEXT PRIMARY KEY,
  next_value BIGINT NOT NULL CHECK (next_value >= 1),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION alloc_gapless_id(p_scope TEXT)
RETURNS BIGINT AS $$
DECLARE
  allocated BIGINT;
BEGIN
  INSERT INTO id_counters (scope, next_value, updated_at)
  VALUES (p_scope, 2, NOW())
  ON CONFLICT (scope) DO UPDATE
    SET next_value = id_counters.next_value + 1,
        updated_at = NOW()
  RETURNING next_value - 1 INTO allocated;

  RETURN allocated;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_assign_agent_id ON agents;
DROP TRIGGER IF EXISTS trg_assign_feedback_id ON feedbacks;
DROP TRIGGER IF EXISTS trg_assign_response_id ON feedback_responses;
DROP TRIGGER IF EXISTS trg_assign_revocation_id ON revocations;

DROP FUNCTION IF EXISTS assign_agent_id();
DROP FUNCTION IF EXISTS assign_feedback_id();
DROP FUNCTION IF EXISTS assign_response_id();
DROP FUNCTION IF EXISTS assign_revocation_id();

CREATE OR REPLACE FUNCTION assign_agent_id()
RETURNS TRIGGER AS $$
DECLARE
  existing_agent_id BIGINT;
  existing_status TEXT;
BEGIN
  IF TG_OP = 'UPDATE' AND OLD.agent_id IS NOT NULL THEN
    NEW.agent_id := OLD.agent_id;
    RETURN NEW;
  END IF;

  IF NEW.agent_id IS NOT NULL THEN
    NEW.agent_id := NULL;
  END IF;

  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    RETURN NEW;
  END IF;

  PERFORM pg_advisory_xact_lock(hashtextextended('agent:id:' || NEW.asset, 0));

  SELECT agent_id, status
  INTO existing_agent_id, existing_status
  FROM agents
  WHERE asset = NEW.asset
  LIMIT 1;

  IF FOUND THEN
    IF existing_agent_id IS NOT NULL THEN
      NEW.agent_id := existing_agent_id;
      RETURN NEW;
    END IF;

    IF existing_status IS NULL OR existing_status != 'ORPHANED' THEN
      NEW.agent_id := alloc_gapless_id('agent:global');
      UPDATE agents
      SET agent_id = NEW.agent_id
      WHERE asset = NEW.asset
        AND agent_id IS NULL
        AND (status IS NULL OR status != 'ORPHANED');
    END IF;

    RETURN NEW;
  END IF;

  NEW.agent_id := alloc_gapless_id('agent:global');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_feedback_id()
RETURNS TRIGGER AS $$
DECLARE
  existing_feedback_id BIGINT;
BEGIN
  IF TG_OP = 'UPDATE' AND OLD.feedback_id IS NOT NULL THEN
    NEW.feedback_id := OLD.feedback_id;
    RETURN NEW;
  END IF;

  IF NEW.feedback_id IS NOT NULL THEN
    NEW.feedback_id := NULL;
  END IF;

  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    RETURN NEW;
  END IF;

  PERFORM pg_advisory_xact_lock(
    hashtextextended(
      'feedback:id:' || NEW.asset || ':' || NEW.client_address || ':' || NEW.feedback_index::text,
      0
    )
  );

  SELECT feedback_id
  INTO existing_feedback_id
  FROM feedbacks
  WHERE asset = NEW.asset
    AND client_address = NEW.client_address
    AND feedback_index = NEW.feedback_index
  LIMIT 1;

  IF FOUND AND existing_feedback_id IS NOT NULL THEN
    NEW.feedback_id := existing_feedback_id;
    RETURN NEW;
  END IF;

  NEW.feedback_id := alloc_gapless_id('feedback:' || NEW.asset);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_response_id()
RETURNS TRIGGER AS $$
DECLARE
  existing_response_id BIGINT;
BEGIN
  IF TG_OP = 'UPDATE' AND OLD.response_id IS NOT NULL THEN
    NEW.response_id := OLD.response_id;
    RETURN NEW;
  END IF;

  IF NEW.response_id IS NOT NULL THEN
    NEW.response_id := NULL;
  END IF;

  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    RETURN NEW;
  END IF;

  PERFORM pg_advisory_xact_lock(
    hashtextextended(
      'response:id:' || NEW.asset || ':' || NEW.client_address || ':' || NEW.feedback_index::text || ':' || NEW.responder || ':' || COALESCE(NEW.tx_signature, ''),
      0
    )
  );

  SELECT response_id
  INTO existing_response_id
  FROM feedback_responses
  WHERE asset = NEW.asset
    AND client_address = NEW.client_address
    AND feedback_index = NEW.feedback_index
    AND responder = NEW.responder
    AND tx_signature = NEW.tx_signature
  LIMIT 1;

  IF FOUND AND existing_response_id IS NOT NULL THEN
    NEW.response_id := existing_response_id;
    RETURN NEW;
  END IF;

  NEW.response_id := alloc_gapless_id(
    'response:' || NEW.asset || ':' || NEW.client_address || ':' || NEW.feedback_index::text
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_revocation_id()
RETURNS TRIGGER AS $$
DECLARE
  existing_revocation_id BIGINT;
BEGIN
  IF TG_OP = 'UPDATE' AND OLD.revocation_id IS NOT NULL THEN
    NEW.revocation_id := OLD.revocation_id;
    RETURN NEW;
  END IF;

  IF NEW.revocation_id IS NOT NULL THEN
    NEW.revocation_id := NULL;
  END IF;

  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    RETURN NEW;
  END IF;

  PERFORM pg_advisory_xact_lock(
    hashtextextended(
      'revocation:id:' || NEW.asset || ':' || NEW.client_address || ':' || NEW.feedback_index::text,
      0
    )
  );

  SELECT revocation_id
  INTO existing_revocation_id
  FROM revocations
  WHERE asset = NEW.asset
    AND client_address = NEW.client_address
    AND feedback_index = NEW.feedback_index
  LIMIT 1;

  IF FOUND AND existing_revocation_id IS NOT NULL THEN
    NEW.revocation_id := existing_revocation_id;
    RETURN NEW;
  END IF;

  NEW.revocation_id := alloc_gapless_id('revocation:' || NEW.asset);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_assign_agent_id
  BEFORE INSERT OR UPDATE ON agents
  FOR EACH ROW
  EXECUTE FUNCTION assign_agent_id();

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

-- Historical compaction (remove existing gaps).
-- Phase 1: temporary offset to avoid uniqueness conflicts during in-place renumbering.
WITH mx AS (
  SELECT GREATEST(COALESCE(MAX(agent_id), 0), 1) + 1000000 AS off
  FROM agents
)
UPDATE agents
SET agent_id = agent_id + (SELECT off FROM mx)
WHERE agent_id IS NOT NULL;

WITH ordered AS (
  SELECT
    asset,
    ROW_NUMBER() OVER (
      ORDER BY
        CASE WHEN status = 'ORPHANED' THEN 1 ELSE 0 END,
        created_at ASC NULLS LAST,
        block_slot ASC NULLS LAST,
        tx_signature ASC NULLS LAST,
        tx_index ASC NULLS LAST,
        event_ordinal ASC NULLS LAST,
        asset ASC
    )::bigint AS dense_id
  FROM agents
)
UPDATE agents a
SET agent_id = o.dense_id
FROM ordered o
WHERE a.asset = o.asset;

WITH mx AS (
  SELECT GREATEST(COALESCE(MAX(feedback_id), 0), 1) + 1000000 AS off
  FROM feedbacks
)
UPDATE feedbacks
SET feedback_id = feedback_id + (SELECT off FROM mx)
WHERE feedback_id IS NOT NULL;

WITH ordered AS (
  SELECT
    id,
    ROW_NUMBER() OVER (
      PARTITION BY asset
      ORDER BY
        CASE WHEN status = 'ORPHANED' THEN 1 ELSE 0 END,
        created_at ASC NULLS LAST,
        block_slot ASC NULLS LAST,
        tx_signature ASC NULLS LAST,
        tx_index ASC NULLS LAST,
        event_ordinal ASC NULLS LAST,
        client_address ASC,
        feedback_index ASC,
        id ASC
    )::bigint AS dense_id
  FROM feedbacks
)
UPDATE feedbacks f
SET feedback_id = o.dense_id
FROM ordered o
WHERE f.id = o.id;

WITH mx AS (
  SELECT GREATEST(COALESCE(MAX(response_id), 0), 1) + 1000000 AS off
  FROM feedback_responses
)
UPDATE feedback_responses
SET response_id = response_id + (SELECT off FROM mx)
WHERE response_id IS NOT NULL;

WITH ordered AS (
  SELECT
    id,
    ROW_NUMBER() OVER (
      PARTITION BY asset, client_address, feedback_index
      ORDER BY
        CASE WHEN status = 'ORPHANED' THEN 1 ELSE 0 END,
        created_at ASC NULLS LAST,
        block_slot ASC NULLS LAST,
        tx_signature ASC NULLS LAST,
        tx_index ASC NULLS LAST,
        event_ordinal ASC NULLS LAST,
        responder ASC,
        id ASC
    )::bigint AS dense_id
  FROM feedback_responses
)
UPDATE feedback_responses r
SET response_id = o.dense_id
FROM ordered o
WHERE r.id = o.id;

WITH mx AS (
  SELECT GREATEST(COALESCE(MAX(revocation_id), 0), 1) + 1000000 AS off
  FROM revocations
)
UPDATE revocations
SET revocation_id = revocation_id + (SELECT off FROM mx)
WHERE revocation_id IS NOT NULL;

WITH ordered AS (
  SELECT
    id,
    ROW_NUMBER() OVER (
      PARTITION BY asset
      ORDER BY
        CASE WHEN status = 'ORPHANED' THEN 1 ELSE 0 END,
        created_at ASC NULLS LAST,
        slot ASC NULLS LAST,
        tx_signature ASC NULLS LAST,
        tx_index ASC NULLS LAST,
        event_ordinal ASC NULLS LAST,
        client_address ASC,
        feedback_index ASC,
        id ASC
    )::bigint AS dense_id
  FROM revocations
)
UPDATE revocations r
SET revocation_id = o.dense_id
FROM ordered o
WHERE r.id = o.id;

-- Rebuild counter checkpoints from compacted state.
DELETE FROM id_counters
WHERE scope = 'agent:global'
   OR scope LIKE 'feedback:%'
   OR scope LIKE 'response:%'
   OR scope LIKE 'revocation:%';

INSERT INTO id_counters (scope, next_value, updated_at)
SELECT 'agent:global', COALESCE(MAX(agent_id), 0) + 1, NOW()
FROM agents
ON CONFLICT (scope) DO UPDATE
SET next_value = EXCLUDED.next_value,
    updated_at = NOW();

INSERT INTO id_counters (scope, next_value, updated_at)
SELECT 'feedback:' || asset, COALESCE(MAX(feedback_id), 0) + 1, NOW()
FROM feedbacks
GROUP BY asset
ON CONFLICT (scope) DO UPDATE
SET next_value = EXCLUDED.next_value,
    updated_at = NOW();

INSERT INTO id_counters (scope, next_value, updated_at)
SELECT 'response:' || asset || ':' || client_address || ':' || feedback_index::text,
       COALESCE(MAX(response_id), 0) + 1,
       NOW()
FROM feedback_responses
GROUP BY asset, client_address, feedback_index
ON CONFLICT (scope) DO UPDATE
SET next_value = EXCLUDED.next_value,
    updated_at = NOW();

INSERT INTO id_counters (scope, next_value, updated_at)
SELECT 'revocation:' || asset, COALESCE(MAX(revocation_id), 0) + 1, NOW()
FROM revocations
GROUP BY asset
ON CONFLICT (scope) DO UPDATE
SET next_value = EXCLUDED.next_value,
    updated_at = NOW();

DROP SEQUENCE IF EXISTS agent_id_seq;

COMMIT;
