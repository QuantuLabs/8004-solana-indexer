-- =============================================
-- 8004 Agent Registry - Reintroduce Global Agent ID
-- Migration: 2026-02-26
-- =============================================
-- New approach: PostgreSQL sequence + BEFORE INSERT trigger.
-- - ID is permanent once assigned (no shifting)
-- - Gaps are acceptable (orphaned agents keep their ID slot)
-- =============================================

-- Add global_id column to agents table
ALTER TABLE agents ADD COLUMN IF NOT EXISTS global_id BIGINT;

-- Create sequence for global IDs
CREATE SEQUENCE IF NOT EXISTS agent_global_id_seq START 1;

-- Backfill existing agents in deterministic order:
--   1) block_slot
--   2) tx_signature
--   3) tx_index NULLS LAST
--   4) event_ordinal NULLS LAST
--   5) asset (technical tie-breaker)
WITH ordered_agents AS (
  SELECT asset, ROW_NUMBER() OVER (
    ORDER BY block_slot, tx_signature, tx_index NULLS LAST, event_ordinal NULLS LAST, asset
  ) AS rn
  FROM agents
  WHERE status != 'ORPHANED'
)
UPDATE agents a
SET global_id = oa.rn
FROM ordered_agents oa
WHERE a.asset = oa.asset;

-- Set sequence to next value after backfill
SELECT setval('agent_global_id_seq', COALESCE((SELECT MAX(global_id) FROM agents), 0));

-- Create trigger to auto-assign global_id on INSERT
CREATE OR REPLACE FUNCTION assign_agent_global_id()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.global_id IS NULL AND (NEW.status IS NULL OR NEW.status != 'ORPHANED') THEN
    NEW.global_id := nextval('agent_global_id_seq');
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_assign_agent_global_id ON agents;
CREATE TRIGGER trg_assign_agent_global_id
  BEFORE INSERT ON agents
  FOR EACH ROW
  EXECUTE FUNCTION assign_agent_global_id();

-- Unique index (allows NULLs for orphaned agents without global_id)
CREATE UNIQUE INDEX IF NOT EXISTS idx_agents_global_id
  ON agents(global_id) WHERE global_id IS NOT NULL;

-- Index for ordering/filtering by global_id
CREATE INDEX IF NOT EXISTS idx_agents_global_id_active
  ON agents(global_id ASC) WHERE status != 'ORPHANED' AND global_id IS NOT NULL;

-- Helper function: format global_id with adaptive padding
CREATE OR REPLACE FUNCTION format_global_id(p_global_id BIGINT)
RETURNS TEXT AS $$
  SELECT '#' || LPAD(p_global_id::TEXT,
    CASE
      WHEN p_global_id < 1000 THEN 3
      WHEN p_global_id < 10000 THEN 4
      WHEN p_global_id < 100000 THEN 5
      ELSE 6
    END, '0');
$$ LANGUAGE SQL IMMUTABLE;
