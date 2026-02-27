-- Rename internal deterministic registration key from global_id to agent_id
-- This is an internal naming cleanup; API fields remain unchanged.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'agents'
      AND column_name = 'global_id'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'agents'
      AND column_name = 'agent_id'
  ) THEN
    ALTER TABLE agents RENAME COLUMN global_id TO agent_id;
  END IF;
END
$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_class WHERE relkind = 'S' AND relname = 'agent_global_id_seq'
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_class WHERE relkind = 'S' AND relname = 'agent_id_seq'
  ) THEN
    ALTER SEQUENCE agent_global_id_seq RENAME TO agent_id_seq;
  END IF;
END
$$;

CREATE SEQUENCE IF NOT EXISTS agent_id_seq START 1;

DO $$
BEGIN
  ALTER SEQUENCE agent_id_seq OWNED BY agents.agent_id;
EXCEPTION
  WHEN undefined_table OR undefined_column THEN
    NULL;
END
$$;

SELECT setval('agent_id_seq', COALESCE((SELECT MAX(agent_id) FROM agents), 0));

DROP TRIGGER IF EXISTS trg_assign_agent_global_id ON agents;
DROP TRIGGER IF EXISTS trg_assign_agent_id ON agents;
DROP FUNCTION IF EXISTS assign_agent_global_id();
DROP FUNCTION IF EXISTS assign_agent_id();

CREATE OR REPLACE FUNCTION assign_agent_id()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.agent_id IS NULL AND (NEW.status IS NULL OR NEW.status != 'ORPHANED') THEN
    NEW.agent_id := nextval('agent_id_seq');
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_assign_agent_id
  BEFORE INSERT ON agents
  FOR EACH ROW
  EXECUTE FUNCTION assign_agent_id();

DROP INDEX IF EXISTS idx_agents_global_id;
DROP INDEX IF EXISTS idx_agents_global_id_active;
DROP INDEX IF EXISTS idx_agents_agent_id;
DROP INDEX IF EXISTS idx_agents_agent_id_active;

CREATE UNIQUE INDEX idx_agents_agent_id ON agents(agent_id) WHERE agent_id IS NOT NULL;
CREATE INDEX idx_agents_agent_id_active
  ON agents(agent_id ASC)
  WHERE status != 'ORPHANED' AND agent_id IS NOT NULL;
