-- =============================================
-- 8004 Agent Registry - Deterministic Ordering + Global Agent ID
-- Migration: 2026-01-22
-- =============================================

-- =============================================
-- ISSUE 1: Deterministic Feedback Ordering
-- Use (block_slot, tx_signature) for consistent re-indexing order
-- =============================================

-- Index for deterministic feedback ordering
-- Replaces reliance on feedback_index (client-provided, not guaranteed)
CREATE INDEX IF NOT EXISTS idx_feedbacks_deterministic_order
ON feedbacks(asset, client_address, block_slot, tx_signature);

-- Index for global feedback ordering (across all clients)
CREATE INDEX IF NOT EXISTS idx_feedbacks_global_order
ON feedbacks(asset, block_slot, tx_signature);

-- =============================================
-- ISSUE 7: Global Agent ID (Cosmetic/Gamification)
-- Sequential ID based on registration order (slot, tx_signature)
-- Deterministic: re-indexing produces same global_id
-- =============================================

-- Drop existing view if exists
DROP MATERIALIZED VIEW IF EXISTS agent_global_ids;

-- Create materialized view for global agent IDs
-- Uses ROW_NUMBER() with deterministic ordering by (block_slot, tx_signature)
CREATE MATERIALIZED VIEW agent_global_ids AS
SELECT
  asset,
  collection,
  owner,
  nft_name,
  ROW_NUMBER() OVER (ORDER BY block_slot, tx_signature) AS global_id,
  block_slot,
  tx_signature,
  created_at
FROM agents
ORDER BY block_slot, tx_signature;

-- Indexes for fast lookups
CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_global_ids_global_id ON agent_global_ids(global_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_global_ids_asset ON agent_global_ids(asset);
CREATE INDEX IF NOT EXISTS idx_agent_global_ids_collection ON agent_global_ids(collection);

-- =============================================
-- HELPER FUNCTION: Get Global Agent ID
-- =============================================

-- Function to get global_id for a single agent
CREATE OR REPLACE FUNCTION get_agent_global_id(p_asset TEXT)
RETURNS BIGINT AS $$
  SELECT global_id FROM agent_global_ids WHERE asset = p_asset;
$$ LANGUAGE SQL STABLE;

-- Function to format global_id with padding (e.g., "#042")
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

-- =============================================
-- ENHANCED AGENT DETAILS VIEW
-- Includes global_id for API responses
-- =============================================

-- Drop existing function to recreate with global_id
DROP FUNCTION IF EXISTS get_agent_details(TEXT);

-- Recreate with global_id
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
  global_id BIGINT,
  global_id_formatted TEXT,
  created_at TIMESTAMPTZ
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    a.asset, a.owner, a.collection, a.nft_name, a.agent_uri, a.agent_wallet,
    a.trust_tier, a.quality_score, a.confidence, a.risk_score,
    a.diversity_ratio, a.feedback_count, a.sort_key,
    g.global_id,
    format_global_id(g.global_id) AS global_id_formatted,
    a.created_at
  FROM agents a
  LEFT JOIN agent_global_ids g ON a.asset = g.asset
  WHERE a.asset = p_asset;
END;
$$ LANGUAGE plpgsql STABLE;

-- =============================================
-- REFRESH FUNCTION (call after new agents indexed)
-- =============================================

-- Function to refresh global_ids view
-- Call this periodically or after batch inserts
CREATE OR REPLACE FUNCTION refresh_agent_global_ids()
RETURNS void AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY agent_global_ids;
END;
$$ LANGUAGE plpgsql;

-- RLS for materialized view (read-only public access)
-- Note: Materialized views don't support RLS directly, but the underlying
-- table (agents) has RLS enabled, and the view only reads from it.

-- Grant select on the view to anon/authenticated roles
GRANT SELECT ON agent_global_ids TO anon;
GRANT SELECT ON agent_global_ids TO authenticated;

-- Modified:
-- - Added deterministic ordering indexes for feedbacks
-- - Added agent_global_ids materialized view
-- - Added helper functions for global_id formatting and badges
-- - Enhanced get_agent_details to include global_id
