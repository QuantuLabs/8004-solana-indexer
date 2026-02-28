-- Prevent agent_id sequence burn for duplicate/replayed agent upserts.
-- Keep assignment deterministic and repair legacy non-orphan rows missing agent_id.

DROP TRIGGER IF EXISTS trg_assign_agent_id ON agents;
DROP FUNCTION IF EXISTS assign_agent_id();

CREATE SEQUENCE IF NOT EXISTS agent_id_seq START 1;

DO $$
BEGIN
  ALTER SEQUENCE agent_id_seq OWNED BY agents.agent_id;
EXCEPTION
  WHEN undefined_table OR undefined_column THEN
    NULL;
END
$$;

CREATE OR REPLACE FUNCTION assign_agent_id()
RETURNS TRIGGER AS $$
DECLARE
  existing_agent_id BIGINT;
  existing_status TEXT;
BEGIN
  IF NEW.agent_id IS NOT NULL OR (NEW.status IS NOT NULL AND NEW.status = 'ORPHANED') THEN
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

CREATE TRIGGER trg_assign_agent_id
  BEFORE INSERT ON agents
  FOR EACH ROW
  EXECUTE FUNCTION assign_agent_id();

-- Deterministically backfill legacy non-orphan rows that still have NULL agent_id.
WITH repair_seed AS (
  SELECT COALESCE(MAX(agent_id), 0) AS max_agent_id
  FROM agents
),
repair_pending AS (
  SELECT
    a.asset,
    ROW_NUMBER() OVER (
      ORDER BY
        a.created_at ASC NULLS LAST,
        a.block_slot ASC NULLS LAST,
        a.tx_signature ASC NULLS LAST,
        a.tx_index ASC NULLS LAST,
        a.event_ordinal ASC NULLS LAST,
        a.asset ASC
    ) AS rn
  FROM agents a
  WHERE a.agent_id IS NULL
    AND (a.status IS NULL OR a.status != 'ORPHANED')
),
repair_assigned AS (
  SELECT
    p.asset,
    (s.max_agent_id + p.rn)::bigint AS repaired_agent_id
  FROM repair_pending p
  CROSS JOIN repair_seed s
)
UPDATE agents a
SET agent_id = r.repaired_agent_id
FROM repair_assigned r
WHERE a.asset = r.asset
  AND a.agent_id IS NULL
  AND (a.status IS NULL OR a.status != 'ORPHANED');

SELECT setval('agent_id_seq', COALESCE((SELECT MAX(agent_id) FROM agents), 0));
