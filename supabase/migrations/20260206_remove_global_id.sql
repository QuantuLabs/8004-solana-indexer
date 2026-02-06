-- =============================================
-- 8004 Agent Registry - Remove Global Agent ID
-- Migration: 2026-02-06
-- =============================================
-- The global_id (ROW_NUMBER based on block_slot) cannot be constant:
-- - New agents with earlier slots shift all subsequent IDs
-- - Re-indexing can produce different numbering
-- - Not suitable as stable identifier
-- =============================================

-- Drop materialized view and indexes
DROP MATERIALIZED VIEW IF EXISTS agent_global_ids CASCADE;

-- Drop helper functions
DROP FUNCTION IF EXISTS get_agent_global_id(TEXT);
DROP FUNCTION IF EXISTS format_global_id(BIGINT);
DROP FUNCTION IF EXISTS refresh_agent_global_ids();

-- Restore get_agent_details without global_id
DROP FUNCTION IF EXISTS get_agent_details(TEXT);

CREATE OR REPLACE FUNCTION get_agent_details(p_asset TEXT)
RETURNS TABLE (
  asset TEXT,
  owner TEXT,
  collection TEXT,
  nft_name TEXT,
  agent_uri TEXT,
  agent_wallet TEXT,
  trust_tier SMALLINT,
  quality_score INTEGER,
  confidence INTEGER,
  risk_score SMALLINT,
  diversity_ratio SMALLINT,
  feedback_count INTEGER,
  sort_key BIGINT,
  created_at TIMESTAMPTZ
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    a.asset, a.owner, a.collection, a.nft_name, a.agent_uri, a.agent_wallet,
    a.trust_tier, a.quality_score, a.confidence, a.risk_score,
    a.diversity_ratio, a.feedback_count, a.sort_key, a.created_at
  FROM agents a
  WHERE a.asset = p_asset;
END;
$$ LANGUAGE plpgsql STABLE;

-- Note: Deterministic ordering indexes (idx_feedbacks_deterministic_order,
-- idx_feedbacks_global_order) are kept - they're useful for re-indexing.
