-- Harden sequential IDs for immutability and trigger bypass resistance.
-- Also restore explicit grants for stats views after DROP/CREATE migrations.

CREATE SEQUENCE IF NOT EXISTS agent_id_seq START 1;

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

  -- Ignore caller-provided IDs and assign under DB lock instead.
  IF NEW.agent_id IS NOT NULL THEN
    NEW.agent_id := NULL;
  END IF;

  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    RETURN NEW;
  END IF;

  -- Serialize per-asset ID assignment so duplicate upserts/replays do not burn sequence values.
  PERFORM pg_advisory_xact_lock(hashtextextended(NEW.asset, 0));

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
      UPDATE agents
      SET agent_id = nextval('agent_id_seq')
      WHERE asset = NEW.asset
        AND agent_id IS NULL
        AND (status IS NULL OR status != 'ORPHANED')
      RETURNING agent_id INTO NEW.agent_id;
    END IF;

    RETURN NEW;
  END IF;

  NEW.agent_id := nextval('agent_id_seq');
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

  -- Ignore caller-provided IDs and assign under DB lock instead.
  IF NEW.feedback_id IS NOT NULL THEN
    NEW.feedback_id := NULL;
  END IF;

  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    RETURN NEW;
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
  IF TG_OP = 'UPDATE' AND OLD.response_id IS NOT NULL THEN
    NEW.response_id := OLD.response_id;
    RETURN NEW;
  END IF;

  -- Ignore caller-provided IDs and assign under DB lock instead.
  IF NEW.response_id IS NOT NULL THEN
    NEW.response_id := NULL;
  END IF;

  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    RETURN NEW;
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
  IF TG_OP = 'UPDATE' AND OLD.revocation_id IS NOT NULL THEN
    NEW.revocation_id := OLD.revocation_id;
    RETURN NEW;
  END IF;

  -- Ignore caller-provided IDs and assign under DB lock instead.
  IF NEW.revocation_id IS NOT NULL THEN
    NEW.revocation_id := NULL;
  END IF;

  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    RETURN NEW;
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

-- Backfill missing non-orphan IDs so constraints can be enforced safely.
UPDATE agents
SET agent_id = nextval('agent_id_seq')
WHERE agent_id IS NULL
  AND (status IS NULL OR status != 'ORPHANED');

SELECT setval(
  'agent_id_seq',
  GREATEST(COALESCE((SELECT MAX(agent_id) FROM agents), 1), 1),
  true
);

UPDATE feedbacks
SET feedback_id = NULL
WHERE feedback_id IS NULL
  AND (status IS NULL OR status != 'ORPHANED');

UPDATE feedback_responses
SET response_id = NULL
WHERE response_id IS NULL
  AND (status IS NULL OR status != 'ORPHANED');

UPDATE revocations
SET revocation_id = NULL
WHERE revocation_id IS NULL
  AND (status IS NULL OR status != 'ORPHANED');

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'agents_non_orphan_agent_id_required'
      AND conrelid = 'agents'::regclass
  ) THEN
    ALTER TABLE agents
      ADD CONSTRAINT agents_non_orphan_agent_id_required
      CHECK (status = 'ORPHANED' OR agent_id IS NOT NULL) NOT VALID;
  END IF;
END;
$$;

ALTER TABLE agents VALIDATE CONSTRAINT agents_non_orphan_agent_id_required;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
    GRANT SELECT ON global_stats TO anon;
    GRANT SELECT ON verification_stats TO anon;
  END IF;

  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
    GRANT SELECT ON global_stats TO authenticated;
    GRANT SELECT ON verification_stats TO authenticated;
  END IF;
END;
$$;
