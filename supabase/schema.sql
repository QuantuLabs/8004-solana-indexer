-- =============================================
-- 8004 Agent Registry - Supabase Schema v2.0
-- Compatible: PostgreSQL only OR PostgreSQL + Redis
-- Run this in Supabase SQL Editor
-- =============================================

-- =============================================
-- DROP EXISTING (clean slate)
-- =============================================
DROP VIEW IF EXISTS global_stats CASCADE;
DROP VIEW IF EXISTS collection_stats CASCADE;
DROP VIEW IF EXISTS leaderboard CASCADE;
DROP FUNCTION IF EXISTS get_leaderboard CASCADE;
DROP TABLE IF EXISTS atom_config CASCADE;
DROP TABLE IF EXISTS validations CASCADE;
DROP TABLE IF EXISTS feedback_responses CASCADE;
DROP TABLE IF EXISTS feedbacks CASCADE;
DROP TABLE IF EXISTS metadata CASCADE;
DROP TABLE IF EXISTS agents CASCADE;
DROP TABLE IF EXISTS collections CASCADE;

-- =============================================
-- COLLECTIONS
-- =============================================
CREATE TABLE collections (
  collection TEXT PRIMARY KEY,
  registry_type TEXT CHECK (registry_type IN ('BASE', 'USER')),
  authority TEXT,
  base_index INTEGER,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_collections_authority ON collections(authority);

-- =============================================
-- AGENTS (Identity + ATOM Stats + Leaderboard)
-- =============================================
CREATE TABLE agents (
  asset TEXT PRIMARY KEY,
  owner TEXT NOT NULL,
  agent_uri TEXT,
  agent_wallet TEXT,
  atom_enabled BOOLEAN DEFAULT TRUE,
  collection TEXT REFERENCES collections(collection),
  nft_name TEXT,

  -- ATOM Stats (from chain) --
  trust_tier SMALLINT DEFAULT 0 CHECK (trust_tier >= 0 AND trust_tier <= 4),
  quality_score INTEGER DEFAULT 0 CHECK (quality_score >= 0 AND quality_score <= 10000),
  confidence INTEGER DEFAULT 0 CHECK (confidence >= 0 AND confidence <= 10000),
  risk_score SMALLINT DEFAULT 0 CHECK (risk_score >= 0 AND risk_score <= 100),
  diversity_ratio SMALLINT DEFAULT 0 CHECK (diversity_ratio >= 0 AND diversity_ratio <= 255),
  feedback_count INTEGER DEFAULT 0,
  -- Raw average (simple arithmetic mean, for when ATOM not enabled)
  raw_avg_score SMALLINT DEFAULT 0 CHECK (raw_avg_score >= 0 AND raw_avg_score <= 100),

  -- Leaderboard sort key (computed, Redis-compatible ≤2^53) --
  -- Formula: mixed-radix packing with deterministic tie-breaker
  -- Radices: tier(5) × quality(10001) × confidence(10001) × tie(10M)
  -- Max ≈ 5×10^15 (fits in IEEE-754 double for Redis ZSET)
  sort_key BIGINT GENERATED ALWAYS AS (
    (trust_tier::bigint * 1000200010000000) +      -- tier × 10001 × 10001 × 10M
    (quality_score::bigint * 100010000000) +       -- quality × 10001 × 10M
    (confidence::bigint * 10000000) +              -- confidence × 10M
    (abs(hashtext(asset)) % 10000000)              -- tie-breaker (0-9,999,999)
  ) STORED,

  -- Chain reference --
  block_slot BIGINT NOT NULL,
  tx_signature TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Standard indexes
CREATE INDEX idx_agents_owner ON agents(owner);
CREATE INDEX idx_agents_collection ON agents(collection);
CREATE INDEX idx_agents_wallet ON agents(agent_wallet);

-- LEADERBOARD: Partial index (only top tiers = small, fast)
CREATE INDEX idx_agents_leaderboard_top ON agents(sort_key DESC)
WHERE trust_tier >= 2;  -- Silver, Gold, Platinum only

-- LEADERBOARD: Per-collection partial index
CREATE INDEX idx_agents_collection_leaderboard ON agents(collection, sort_key DESC)
WHERE trust_tier >= 1;  -- Bronze+ per collection

-- Full leaderboard (if needed for all tiers)
CREATE INDEX idx_agents_sort_key ON agents(sort_key DESC);

-- =============================================
-- METADATA (key-value per agent)
-- =============================================
CREATE TABLE metadata (
  id TEXT PRIMARY KEY,
  asset TEXT NOT NULL REFERENCES agents(asset) ON DELETE CASCADE,
  key TEXT NOT NULL,
  key_hash TEXT NOT NULL,
  value BYTEA,  -- binary with compression prefix (0x00=raw, 0x01=zstd)
  immutable BOOLEAN DEFAULT FALSE,
  block_slot BIGINT NOT NULL,
  tx_signature TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(asset, key_hash)
);

CREATE INDEX idx_metadata_asset ON metadata(asset);
CREATE INDEX idx_metadata_key ON metadata(key);

-- =============================================
-- FEEDBACKS (immutable log - raw data only)
-- =============================================
CREATE TABLE feedbacks (
  id TEXT PRIMARY KEY,
  asset TEXT NOT NULL REFERENCES agents(asset) ON DELETE CASCADE,
  client_address TEXT NOT NULL,
  feedback_index BIGINT NOT NULL,
  score SMALLINT NOT NULL CHECK (score >= 0 AND score <= 100),
  tag1 TEXT,
  tag2 TEXT,
  endpoint TEXT,
  feedback_uri TEXT,
  feedback_hash TEXT,
  is_revoked BOOLEAN DEFAULT FALSE,
  revoked_at TIMESTAMPTZ,
  block_slot BIGINT NOT NULL,
  tx_signature TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(asset, client_address, feedback_index)
);

CREATE INDEX idx_feedbacks_asset ON feedbacks(asset);
CREATE INDEX idx_feedbacks_client ON feedbacks(client_address);
CREATE INDEX idx_feedbacks_tag1 ON feedbacks(tag1) WHERE tag1 IS NOT NULL;
CREATE INDEX idx_feedbacks_endpoint ON feedbacks(endpoint) WHERE endpoint IS NOT NULL;
CREATE INDEX idx_feedbacks_not_revoked ON feedbacks(asset, created_at DESC) WHERE NOT is_revoked;

-- =============================================
-- FEEDBACK_RESPONSES
-- =============================================
CREATE TABLE feedback_responses (
  id TEXT PRIMARY KEY,
  asset TEXT NOT NULL REFERENCES agents(asset) ON DELETE CASCADE,
  client_address TEXT NOT NULL,
  feedback_index BIGINT NOT NULL,
  responder TEXT NOT NULL,
  response_uri TEXT,
  response_hash TEXT,
  block_slot BIGINT NOT NULL,
  tx_signature TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(asset, client_address, feedback_index, responder)
);

CREATE INDEX idx_responses_asset ON feedback_responses(asset);
CREATE INDEX idx_responses_lookup ON feedback_responses(asset, client_address, feedback_index);

-- =============================================
-- VALIDATIONS
-- =============================================
CREATE TABLE validations (
  id TEXT PRIMARY KEY,
  asset TEXT NOT NULL REFERENCES agents(asset) ON DELETE CASCADE,
  validator_address TEXT NOT NULL,
  nonce BIGINT NOT NULL,  -- BIGINT: Solana u32 can exceed INTEGER max (2^31-1)
  requester TEXT,
  request_uri TEXT,
  request_hash TEXT,
  response SMALLINT,
  response_uri TEXT,
  response_hash TEXT,
  tag TEXT,
  status TEXT DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'RESPONDED')),
  block_slot BIGINT NOT NULL,
  tx_signature TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(asset, validator_address, nonce)
);

CREATE INDEX idx_validations_asset ON validations(asset);
CREATE INDEX idx_validations_validator ON validations(validator_address);
CREATE INDEX idx_validations_status ON validations(status);
CREATE INDEX idx_validations_pending ON validations(validator_address, created_at DESC)
WHERE status = 'PENDING';

-- =============================================
-- ATOM_CONFIG (singleton - ATOM Engine config)
-- =============================================
CREATE TABLE atom_config (
  id TEXT PRIMARY KEY DEFAULT 'main',
  authority TEXT NOT NULL,
  agent_registry_program TEXT NOT NULL,
  version SMALLINT DEFAULT 1,
  block_slot BIGINT NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================
-- INDEXER_STATE (cursor persistence)
-- =============================================
CREATE TABLE indexer_state (
  id TEXT PRIMARY KEY DEFAULT 'main',
  last_signature TEXT,
  last_slot BIGINT,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================
-- VIEWS (for API)
-- =============================================

-- Global leaderboard (top tiers, uses partial index)
CREATE OR REPLACE VIEW leaderboard AS
SELECT
  asset, owner, collection, nft_name, agent_uri,
  trust_tier, quality_score, confidence, risk_score,
  diversity_ratio, feedback_count, sort_key
FROM agents
WHERE trust_tier >= 2
ORDER BY sort_key DESC;

-- Collection stats
CREATE OR REPLACE VIEW collection_stats AS
SELECT
  c.collection,
  c.registry_type,
  c.authority,
  COUNT(a.asset) AS agent_count,
  COUNT(a.asset) FILTER (WHERE a.trust_tier >= 3) AS top_agents,
  ROUND(AVG(a.quality_score) FILTER (WHERE a.feedback_count > 0), 0) AS avg_quality
FROM collections c
LEFT JOIN agents a ON c.collection = a.collection
GROUP BY c.collection, c.registry_type, c.authority;

-- Global stats
CREATE OR REPLACE VIEW global_stats AS
SELECT
  (SELECT COUNT(*) FROM agents) AS total_agents,
  (SELECT COUNT(*) FROM collections) AS total_collections,
  (SELECT COUNT(*) FROM feedbacks WHERE NOT is_revoked) AS total_feedbacks,
  (SELECT COUNT(*) FROM validations) AS total_validations,
  (SELECT COUNT(*) FROM agents WHERE trust_tier = 4) AS platinum_agents,
  (SELECT COUNT(*) FROM agents WHERE trust_tier = 3) AS gold_agents,
  (SELECT ROUND(AVG(quality_score), 0) FROM agents WHERE feedback_count > 0) AS avg_quality;

-- =============================================
-- RPC FUNCTIONS
-- =============================================

-- Get leaderboard with pagination (keyset pagination for scale)
CREATE OR REPLACE FUNCTION get_leaderboard(
  p_collection TEXT DEFAULT NULL,
  p_min_tier INT DEFAULT 0,
  p_limit INT DEFAULT 50,
  p_cursor_sort_key BIGINT DEFAULT NULL
)
RETURNS TABLE (
  asset TEXT,
  owner TEXT,
  collection TEXT,
  nft_name TEXT,
  trust_tier SMALLINT,
  quality_score INTEGER,
  confidence INTEGER,
  risk_score SMALLINT,
  feedback_count INTEGER,
  sort_key BIGINT
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    a.asset, a.owner, a.collection, a.nft_name,
    a.trust_tier, a.quality_score, a.confidence,
    a.risk_score, a.feedback_count, a.sort_key
  FROM agents a
  WHERE a.trust_tier >= p_min_tier
    AND (p_collection IS NULL OR a.collection = p_collection)
    AND (p_cursor_sort_key IS NULL OR a.sort_key < p_cursor_sort_key)
  ORDER BY a.sort_key DESC
  LIMIT p_limit;
END;
$$ LANGUAGE plpgsql STABLE;

-- Get agent with full details
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

-- =============================================
-- ROW LEVEL SECURITY (RLS)
-- =============================================

ALTER TABLE agents ENABLE ROW LEVEL SECURITY;
ALTER TABLE collections ENABLE ROW LEVEL SECURITY;
ALTER TABLE metadata ENABLE ROW LEVEL SECURITY;
ALTER TABLE feedbacks ENABLE ROW LEVEL SECURITY;
ALTER TABLE feedback_responses ENABLE ROW LEVEL SECURITY;
ALTER TABLE validations ENABLE ROW LEVEL SECURITY;
ALTER TABLE atom_config ENABLE ROW LEVEL SECURITY;
ALTER TABLE indexer_state ENABLE ROW LEVEL SECURITY;

-- Public read access
CREATE POLICY "Public read agents" ON agents FOR SELECT USING (true);
CREATE POLICY "Public read collections" ON collections FOR SELECT USING (true);
CREATE POLICY "Public read metadata" ON metadata FOR SELECT USING (true);
CREATE POLICY "Public read feedbacks" ON feedbacks FOR SELECT USING (true);
CREATE POLICY "Public read feedback_responses" ON feedback_responses FOR SELECT USING (true);
CREATE POLICY "Public read validations" ON validations FOR SELECT USING (true);
CREATE POLICY "Public read atom_config" ON atom_config FOR SELECT USING (true);

-- Service role write access (indexer uses SUPABASE_DSN with service_role)
-- No INSERT/UPDATE/DELETE policies = blocked for anon users

-- =============================================
-- DETERMINISTIC ORDERING INDEXES
-- Use (block_slot, tx_signature) for consistent re-indexing
-- =============================================

-- Feedback ordering: deterministic per client
CREATE INDEX idx_feedbacks_deterministic_order
ON feedbacks(asset, client_address, block_slot, tx_signature);

-- Feedback ordering: global within agent
CREATE INDEX idx_feedbacks_global_order
ON feedbacks(asset, block_slot, tx_signature);

-- =============================================
-- GLOBAL AGENT ID (Cosmetic/Gamification)
-- =============================================

-- Materialized view for global sequential IDs
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
CREATE UNIQUE INDEX idx_agent_global_ids_global_id ON agent_global_ids(global_id);
CREATE UNIQUE INDEX idx_agent_global_ids_asset ON agent_global_ids(asset);
CREATE INDEX idx_agent_global_ids_collection ON agent_global_ids(collection);

-- Helper function: format global_id with padding
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

-- Refresh function (call after new agents)
CREATE OR REPLACE FUNCTION refresh_agent_global_ids()
RETURNS void AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY agent_global_ids;
END;
$$ LANGUAGE plpgsql;

-- Grant read access to materialized view
GRANT SELECT ON agent_global_ids TO anon;
GRANT SELECT ON agent_global_ids TO authenticated;

-- Modified:
-- - feedback_responses table: Added client_address column
-- - Updated UNIQUE constraint to (asset, client_address, feedback_index, responder)
-- - Updated idx_responses_lookup index to include client_address
-- - Added deterministic ordering indexes for feedbacks
-- - Added agent_global_ids materialized view for gamification
