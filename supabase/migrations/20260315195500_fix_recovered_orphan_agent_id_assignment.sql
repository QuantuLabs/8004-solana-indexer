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

    IF existing_status IS NULL
      OR existing_status != 'ORPHANED'
      OR (TG_OP = 'UPDATE' AND OLD.status = 'ORPHANED')
    THEN
      NEW.agent_id := alloc_gapless_id('agent:global', COALESCE(NEW.created_at, NEW.updated_at, NOW()));
      UPDATE agents
      SET agent_id = NEW.agent_id
      WHERE asset = NEW.asset
        AND agent_id IS NULL
        AND (status IS NULL OR status != 'ORPHANED');
    END IF;

    RETURN NEW;
  END IF;

  NEW.agent_id := alloc_gapless_id('agent:global', COALESCE(NEW.created_at, NEW.updated_at, NOW()));
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
