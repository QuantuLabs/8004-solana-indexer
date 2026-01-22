-- =============================================
-- Add tx_index for accurate intra-slot ordering
-- Migration: 2026-01-22 (part 2)
-- =============================================

-- Add tx_index column to agents (nullable for existing data)
ALTER TABLE agents ADD COLUMN IF NOT EXISTS tx_index INTEGER;

-- Add tx_index column to feedbacks
ALTER TABLE feedbacks ADD COLUMN IF NOT EXISTS tx_index INTEGER;

-- Add tx_index column to feedback_responses
ALTER TABLE feedback_responses ADD COLUMN IF NOT EXISTS tx_index INTEGER;

-- Add tx_index column to validations
ALTER TABLE validations ADD COLUMN IF NOT EXISTS tx_index INTEGER;

-- Add tx_index column to metadata
ALTER TABLE metadata ADD COLUMN IF NOT EXISTS tx_index INTEGER;

-- Update deterministic ordering index to use tx_index when available
DROP INDEX IF EXISTS idx_feedbacks_deterministic_order;
CREATE INDEX idx_feedbacks_deterministic_order
ON feedbacks(asset, client_address, block_slot, COALESCE(tx_index, 0), tx_signature);

DROP INDEX IF EXISTS idx_feedbacks_global_order;
CREATE INDEX idx_feedbacks_global_order
ON feedbacks(asset, block_slot, COALESCE(tx_index, 0), tx_signature);

-- Update agent_global_ids view to use tx_index
DROP MATERIALIZED VIEW IF EXISTS agent_global_ids;

CREATE MATERIALIZED VIEW agent_global_ids AS
SELECT
  asset,
  collection,
  owner,
  nft_name,
  ROW_NUMBER() OVER (
    ORDER BY block_slot, COALESCE(tx_index, 0), tx_signature
  ) AS global_id,
  block_slot,
  tx_index,
  tx_signature,
  created_at
FROM agents
ORDER BY block_slot, COALESCE(tx_index, 0), tx_signature;

-- Recreate indexes
CREATE UNIQUE INDEX idx_agent_global_ids_global_id ON agent_global_ids(global_id);
CREATE UNIQUE INDEX idx_agent_global_ids_asset ON agent_global_ids(asset);
CREATE INDEX idx_agent_global_ids_collection ON agent_global_ids(collection);

-- Grant permissions
GRANT SELECT ON agent_global_ids TO anon;
GRANT SELECT ON agent_global_ids TO authenticated;

-- Refresh view
SELECT refresh_agent_global_ids();
