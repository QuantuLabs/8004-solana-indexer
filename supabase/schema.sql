-- =============================================
-- 8004 Agent Registry - Supabase Schema v2.0
-- Compatible: PostgreSQL only OR PostgreSQL + Redis
-- Run this in Supabase SQL Editor
-- =============================================

-- =============================================
-- SAFETY GUARD (destructive reset protection)
-- =============================================
-- schema.sql is for fresh initialization only.
-- If indexer tables already exist, this script aborts unless the session sets:
--   SET app.allow_destructive_schema_reset = 'on';
-- For upgrades, use supabase/migrations/*.sql instead.
DO $$
DECLARE
  existing_table_count INTEGER;
  allow_reset TEXT;
BEGIN
  allow_reset := COALESCE(current_setting('app.allow_destructive_schema_reset', true), '');

  SELECT COUNT(*) INTO existing_table_count
  FROM information_schema.tables
  WHERE table_schema = 'public'
    AND table_type = 'BASE TABLE'
    AND table_name = ANY (ARRAY[
      'collections',
      'collection_pointers',
      'agents',
      'metadata',
      'feedbacks',
      'feedback_responses',
      'revocations',
      'validations',
      'atom_config',
      'id_counters',
      'indexer_state',
      'agent_digest_cache',
      'Agent',
      'AgentMetadata',
      'Feedback',
      'FeedbackResponse',
      'Validation',
      'Registry',
      'IndexerState',
      'EventLog'
    ]);

  IF existing_table_count > 0 AND lower(allow_reset) NOT IN ('1', 'true', 'yes', 'on') THEN
    RAISE EXCEPTION
      'Refusing to run destructive supabase/schema.sql on a non-empty indexer database. Use supabase/migrations/*.sql for upgrades, or explicitly opt in with SET app.allow_destructive_schema_reset = ''on''.';
  END IF;
END;
$$;

-- =============================================
-- DROP EXISTING (clean slate)
-- =============================================
DROP VIEW IF EXISTS global_stats CASCADE;
DROP VIEW IF EXISTS collection_stats CASCADE;
DROP VIEW IF EXISTS leaderboard CASCADE;
DROP FUNCTION IF EXISTS get_leaderboard CASCADE;
DROP TABLE IF EXISTS atom_config CASCADE;
DROP TABLE IF EXISTS agent_digest_cache CASCADE;
DROP TABLE IF EXISTS indexer_state CASCADE;
DROP TABLE IF EXISTS id_counters CASCADE;
DROP TABLE IF EXISTS validations CASCADE;
DROP TABLE IF EXISTS feedback_responses CASCADE;
DROP TABLE IF EXISTS revocations CASCADE;
DROP TABLE IF EXISTS feedbacks CASCADE;
DROP TABLE IF EXISTS metadata CASCADE;
DROP TABLE IF EXISTS agents CASCADE;
DROP TABLE IF EXISTS collection_pointers CASCADE;
DROP TABLE IF EXISTS collections CASCADE;

-- Extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- =============================================
-- COLLECTIONS
-- =============================================
CREATE TABLE collections (
  collection TEXT PRIMARY KEY,
  registry_type TEXT CHECK (registry_type IN ('BASE', 'USER')),
  authority TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  -- Verification status (reorg resilience) --
  status TEXT DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'FINALIZED', 'ORPHANED')),
  verified_at TIMESTAMPTZ
);

CREATE INDEX idx_collections_authority ON collections(authority);
CREATE INDEX idx_collections_status ON collections(status) WHERE status = 'PENDING';

-- =============================================
-- CANONICAL COLLECTION POINTERS (c1:<cid>)
-- =============================================
CREATE TABLE collection_pointers (
  col TEXT NOT NULL,
  creator TEXT NOT NULL,
  first_seen_asset TEXT NOT NULL,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  first_seen_slot BIGINT NOT NULL,
  first_seen_tx_signature TEXT,
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_slot BIGINT NOT NULL,
  last_seen_tx_signature TEXT,
  asset_count BIGINT NOT NULL DEFAULT 0 CHECK (asset_count >= 0),
  version TEXT,
  name TEXT,
  symbol TEXT,
  description TEXT,
  image TEXT,
  banner_image TEXT,
  social_website TEXT,
  social_x TEXT,
  social_discord TEXT,
  metadata_status TEXT,
  metadata_hash TEXT,
  metadata_bytes INTEGER,
  metadata_updated_at TIMESTAMPTZ,
  PRIMARY KEY (col, creator)
);

CREATE INDEX idx_collection_pointers_creator ON collection_pointers(creator);
CREATE INDEX idx_collection_pointers_first_seen_at ON collection_pointers(first_seen_at DESC);
CREATE INDEX idx_collection_pointers_last_seen_at ON collection_pointers(last_seen_at DESC);

-- =============================================
-- AGENTS (Identity + ATOM Stats + Leaderboard)
-- =============================================
CREATE TABLE agents (
  asset TEXT PRIMARY KEY,
  owner TEXT NOT NULL,
  creator TEXT,
  agent_uri TEXT,
  agent_wallet TEXT,
  atom_enabled BOOLEAN DEFAULT TRUE,
  collection TEXT REFERENCES collections(collection),
  canonical_col TEXT DEFAULT '',
  col_locked BOOLEAN DEFAULT FALSE,
  parent_asset TEXT,
  parent_creator TEXT,
  parent_locked BOOLEAN DEFAULT FALSE,
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

  -- Sequential registration ID (auto-assigned by trigger, permanent) --
  agent_id BIGINT,

  -- Chain reference --
  block_slot BIGINT NOT NULL,
  tx_index INTEGER,
  event_ordinal INTEGER,
  tx_signature TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),

  -- Verification status (reorg resilience) --
  status TEXT DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'FINALIZED', 'ORPHANED')),
  verified_at TIMESTAMPTZ,
  verified_slot BIGINT,
  CHECK (status = 'ORPHANED' OR agent_id IS NOT NULL)
);

-- Transactional gapless counters (rollback-safe)
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

  -- Serialize per-agent identity for idempotent replay handling.
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

CREATE TRIGGER trg_assign_agent_id
  BEFORE INSERT OR UPDATE ON agents
  FOR EACH ROW
  EXECUTE FUNCTION assign_agent_id();

-- Global ID indexes
CREATE UNIQUE INDEX idx_agents_agent_id ON agents(agent_id) WHERE agent_id IS NOT NULL;
CREATE INDEX idx_agents_agent_id_active ON agents(agent_id ASC) WHERE status != 'ORPHANED' AND agent_id IS NOT NULL;

-- Standard indexes
CREATE INDEX idx_agents_owner ON agents(owner);
CREATE INDEX idx_agents_collection ON agents(collection);
CREATE INDEX idx_agents_canonical_col ON agents(canonical_col) WHERE canonical_col <> '';
CREATE INDEX idx_agents_col_creator_active ON agents(canonical_col, creator, created_at DESC, asset DESC)
  WHERE status != 'ORPHANED' AND canonical_col <> '';
CREATE INDEX idx_agents_wallet ON agents(agent_wallet);
CREATE INDEX idx_agents_status ON agents(status) WHERE status = 'PENDING';
CREATE INDEX idx_agents_creator ON agents(creator);
CREATE INDEX idx_agents_parent_asset ON agents(parent_asset);
CREATE INDEX idx_agents_parent_active ON agents(parent_asset, created_at DESC, asset DESC)
  WHERE status != 'ORPHANED' AND parent_asset IS NOT NULL;
CREATE INDEX idx_agents_graphql_created_asset ON agents(created_at DESC, asset DESC) WHERE status != 'ORPHANED';
CREATE INDEX idx_agents_graphql_nft_name_trgm ON agents USING gin (nft_name gin_trgm_ops) WHERE status != 'ORPHANED';
CREATE INDEX idx_agents_graphql_asset_trgm ON agents USING gin (asset gin_trgm_ops) WHERE status != 'ORPHANED';
CREATE INDEX idx_agents_graphql_owner_trgm ON agents USING gin (owner gin_trgm_ops) WHERE status != 'ORPHANED';

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
  tx_index INTEGER,
  event_ordinal INTEGER,
  tx_signature TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  -- Verification status (reorg resilience) --
  status TEXT DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'FINALIZED', 'ORPHANED')),
  verified_at TIMESTAMPTZ,
  UNIQUE(asset, key_hash)
);

CREATE INDEX idx_metadata_asset ON metadata(asset);
CREATE INDEX idx_metadata_key ON metadata(key);
CREATE INDEX idx_metadata_status ON metadata(status) WHERE status = 'PENDING';
CREATE INDEX idx_metadata_graphql_asset_key ON metadata(asset, key) WHERE status != 'ORPHANED';
CREATE INDEX idx_metadata_graphql_uri_asset_updated
ON metadata(asset, updated_at DESC)
WHERE status != 'ORPHANED' AND key LIKE '\_uri:%' ESCAPE '\';

-- =============================================
-- FEEDBACKS (immutable log - raw data only)
-- v0.5.0: Added value, value_decimals, score is now nullable
-- =============================================
CREATE TABLE feedbacks (
  id TEXT PRIMARY KEY,
  feedback_id BIGINT,
  asset TEXT NOT NULL REFERENCES agents(asset) ON DELETE CASCADE,
  client_address TEXT NOT NULL,
  feedback_index BIGINT NOT NULL,
  value NUMERIC(39,0) DEFAULT 0,  -- v0.6.0: i128 raw metric value
  value_decimals SMALLINT DEFAULT 0 CHECK (value_decimals >= 0 AND value_decimals <= 18),  -- v0.6.0: decimal precision
  score SMALLINT CHECK (score >= 0 AND score <= 100),  -- v0.5.0: nullable (NULL = ATOM skipped)
  tag1 TEXT,
  tag2 TEXT,
  endpoint TEXT,
  feedback_uri TEXT,
  feedback_hash TEXT,
  running_digest BYTEA,
  is_revoked BOOLEAN DEFAULT FALSE,
  revoked_at TIMESTAMPTZ,
  block_slot BIGINT NOT NULL,
  tx_index INTEGER,  -- metadata / tertiary tie-breaker for deterministic ordering
  event_ordinal INTEGER, -- quaternary tie-breaker for deterministic intra-tx ordering
  tx_signature TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  -- Verification status (reorg resilience) --
  status TEXT DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'FINALIZED', 'ORPHANED')),
  verified_at TIMESTAMPTZ,
  CHECK (status = 'ORPHANED' OR feedback_id IS NOT NULL),
  UNIQUE(asset, client_address, feedback_index)
);

CREATE INDEX idx_feedbacks_asset ON feedbacks(asset);
CREATE INDEX idx_feedbacks_client ON feedbacks(client_address);
CREATE INDEX idx_feedbacks_tag1 ON feedbacks(tag1) WHERE tag1 IS NOT NULL;
CREATE INDEX idx_feedbacks_endpoint ON feedbacks(endpoint) WHERE endpoint IS NOT NULL;
CREATE INDEX idx_feedbacks_not_revoked ON feedbacks(asset, created_at DESC) WHERE NOT is_revoked;
CREATE INDEX idx_feedbacks_status ON feedbacks(status) WHERE status = 'PENDING';
CREATE INDEX idx_feedbacks_graphql_created_asset ON feedbacks(created_at DESC, asset DESC) WHERE status != 'ORPHANED';
CREATE INDEX idx_feedbacks_graphql_value_asset ON feedbacks(value DESC, asset DESC) WHERE status != 'ORPHANED';
CREATE UNIQUE INDEX idx_feedbacks_asset_feedback_id_unique ON feedbacks(asset, feedback_id);
CREATE INDEX idx_feedbacks_asset_feedback_id ON feedbacks(asset, feedback_id);

-- =============================================
-- FEEDBACK_RESPONSES
-- =============================================
CREATE TABLE feedback_responses (
  id TEXT PRIMARY KEY,
  response_id BIGINT,
  asset TEXT NOT NULL REFERENCES agents(asset) ON DELETE CASCADE,
  client_address TEXT NOT NULL,
  feedback_index BIGINT NOT NULL,
  responder TEXT NOT NULL,
  response_uri TEXT,
  response_hash TEXT,
  running_digest BYTEA,
  response_count BIGINT NOT NULL DEFAULT 0,
  block_slot BIGINT NOT NULL,
  tx_index INTEGER,
  event_ordinal INTEGER,
  tx_signature TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  -- Verification status (reorg resilience) --
  status TEXT DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'FINALIZED', 'ORPHANED')),
  verified_at TIMESTAMPTZ,
  CHECK (status = 'ORPHANED' OR response_id IS NOT NULL),
  -- Multiple responses per responder allowed (ERC-8004)
  -- id format: asset:client:index:responder:tx_signature
  UNIQUE(asset, client_address, feedback_index, responder, tx_signature)
);

CREATE INDEX idx_responses_asset ON feedback_responses(asset);
CREATE INDEX idx_responses_lookup ON feedback_responses(asset, client_address, feedback_index);
CREATE INDEX idx_responses_status ON feedback_responses(status) WHERE status = 'PENDING';
CREATE INDEX idx_responses_graphql_created_id ON feedback_responses(created_at DESC, id DESC) WHERE status != 'ORPHANED';
CREATE UNIQUE INDEX idx_responses_scope_response_id_unique
ON feedback_responses(asset, client_address, feedback_index, response_id);
CREATE INDEX idx_responses_scope_response_id
ON feedback_responses(asset, client_address, feedback_index, response_id);
CREATE INDEX idx_responses_response_id ON feedback_responses(response_id);

-- =============================================
-- REVOCATIONS
-- =============================================
CREATE TABLE revocations (
  id TEXT PRIMARY KEY,
  asset TEXT NOT NULL REFERENCES agents(asset) ON DELETE CASCADE,
  client_address TEXT NOT NULL,
  revocation_id BIGINT, -- Sequential revocation ID (auto-assigned, permanent)
  feedback_index BIGINT NOT NULL,
  feedback_hash TEXT,
  slot BIGINT NOT NULL,
  original_score SMALLINT,
  atom_enabled BOOLEAN DEFAULT FALSE,
  had_impact BOOLEAN DEFAULT FALSE,
  running_digest BYTEA,
  revoke_count BIGINT NOT NULL,
  tx_signature TEXT,
  tx_index INTEGER,
  event_ordinal INTEGER,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  status TEXT DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'FINALIZED', 'ORPHANED')),
  verified_at TIMESTAMPTZ,
  CHECK (status = 'ORPHANED' OR revocation_id IS NOT NULL),
  UNIQUE(asset, client_address, feedback_index)
);

CREATE INDEX idx_revocations_asset ON revocations(asset);
CREATE INDEX idx_revocations_status ON revocations(status) WHERE status = 'PENDING';
CREATE UNIQUE INDEX idx_revocations_asset_revocation_id
ON revocations(asset, revocation_id)
WHERE revocation_id IS NOT NULL;
CREATE INDEX idx_revocations_asset_revocation_id_lookup
ON revocations(asset, revocation_id);

-- =============================================
-- Scoped sequential ID hardening (DB-level, multi-writer safe)
-- =============================================
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

  -- Serialize by canonical feedback identity to make duplicate upserts idempotent.
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

  -- Ignore caller-provided IDs and assign under DB lock instead.
  IF NEW.response_id IS NOT NULL THEN
    NEW.response_id := NULL;
  END IF;

  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    RETURN NEW;
  END IF;

  -- Serialize by canonical response identity to make duplicate upserts idempotent.
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

  -- Ignore caller-provided IDs and assign under DB lock instead.
  IF NEW.revocation_id IS NOT NULL THEN
    NEW.revocation_id := NULL;
  END IF;

  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    RETURN NEW;
  END IF;

  -- Serialize by canonical revocation identity to make duplicate upserts idempotent.
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
  tx_index INTEGER,
  event_ordinal INTEGER,
  tx_signature TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  -- Verification status (reorg resilience) - separate from response status --
  chain_status TEXT DEFAULT 'PENDING' CHECK (chain_status IN ('PENDING', 'FINALIZED', 'ORPHANED')),
  chain_verified_at TIMESTAMPTZ,
  UNIQUE(asset, validator_address, nonce)
);

CREATE INDEX idx_validations_asset ON validations(asset);
CREATE INDEX idx_validations_validator ON validations(validator_address);
CREATE INDEX idx_validations_chain_status ON validations(chain_status) WHERE chain_status = 'PENDING';
CREATE INDEX idx_validations_status ON validations(status);
CREATE INDEX idx_validations_pending ON validations(validator_address, created_at DESC)
WHERE status = 'PENDING';
CREATE INDEX idx_validations_graphql_created_id ON validations(created_at DESC, id DESC) WHERE chain_status != 'ORPHANED';

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
  source TEXT DEFAULT 'poller',
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================
-- AGENT_DIGEST_CACHE (hash-chain verification)
-- =============================================
CREATE TABLE agent_digest_cache (
  agent_id TEXT PRIMARY KEY,
  feedback_digest BYTEA,
  feedback_count BIGINT DEFAULT 0,
  response_digest BYTEA,
  response_count BIGINT DEFAULT 0,
  revoke_digest BYTEA,
  revoke_count BIGINT DEFAULT 0,
  last_verified_at TIMESTAMPTZ,
  last_verified_slot BIGINT,
  needs_gap_fill BOOLEAN DEFAULT FALSE,
  gap_fill_from_slot BIGINT
);

CREATE INDEX idx_agent_digest_cache_gap_fill ON agent_digest_cache(needs_gap_fill) WHERE needs_gap_fill = TRUE;

-- =============================================
-- VIEWS (for API)
-- =============================================

-- Metadata decoded (all formats) - use when you need all metadata
-- value_text may contain "_compressed:base64..." for ZSTD entries
CREATE OR REPLACE VIEW metadata_decoded
WITH (security_invoker = true) AS
SELECT
  id, asset, key, key_hash, immutable, block_slot, tx_signature, created_at, updated_at,
  CASE
    WHEN value IS NULL OR octet_length(value) = 0 THEN NULL
    WHEN get_byte(value, 0) = 0 THEN convert_from(substring(value from 2), 'utf8')
    WHEN get_byte(value, 0) = 1 THEN '_compressed:' || encode(value, 'base64')
    ELSE encode(value, 'base64')
  END AS value_text,
  CASE
    WHEN value IS NULL OR octet_length(value) = 0 THEN 'empty'
    WHEN get_byte(value, 0) = 0 THEN 'raw'
    WHEN get_byte(value, 0) = 1 THEN 'zstd'
    ELSE 'legacy'
  END AS encoding
FROM metadata;

-- Metadata decoded (RAW only) - JSON-safe guaranteed
-- Use this when you need JSON-parseable values (most common case)
CREATE OR REPLACE VIEW metadata_decoded_raw
WITH (security_invoker = true) AS
SELECT
  id, asset, key, key_hash, immutable, block_slot, tx_signature, created_at, updated_at,
  convert_from(substring(value from 2), 'utf8') AS value_text
FROM metadata
WHERE value IS NOT NULL
  AND octet_length(value) > 0
  AND get_byte(value, 0) = 0;

-- Global leaderboard (top tiers, uses partial index)
CREATE OR REPLACE VIEW leaderboard
WITH (security_invoker = true) AS
SELECT
  asset, owner, collection, nft_name, agent_uri,
  trust_tier, quality_score, confidence, risk_score,
  diversity_ratio, feedback_count, sort_key
FROM agents
WHERE trust_tier >= 2
ORDER BY sort_key DESC;

-- Collection stats
CREATE OR REPLACE VIEW collection_stats
WITH (security_invoker = true) AS
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
CREATE OR REPLACE VIEW global_stats
WITH (security_invoker = true) AS
SELECT
  (SELECT COUNT(*) FROM agents) AS total_agents,
  (SELECT COUNT(*) FROM collections WHERE status != 'ORPHANED' AND registry_type != 'BASE') AS total_collections,
  (SELECT COUNT(*) FROM feedbacks WHERE NOT is_revoked) AS total_feedbacks,
  (SELECT COUNT(*) FROM agents WHERE trust_tier = 4) AS platinum_agents,
  (SELECT COUNT(*) FROM agents WHERE trust_tier = 3) AS gold_agents,
  (SELECT ROUND(AVG(quality_score), 0) FROM agents WHERE feedback_count > 0) AS avg_quality;

-- Verification stats (reorg resilience)
CREATE OR REPLACE VIEW verification_stats
WITH (security_invoker = true) AS
SELECT
  'agents' AS model,
  COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_count,
  COUNT(*) FILTER (WHERE status = 'FINALIZED') AS finalized_count,
  COUNT(*) FILTER (WHERE status = 'ORPHANED') AS orphaned_count
FROM agents
UNION ALL
SELECT
  'collections' AS model,
  COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_count,
  COUNT(*) FILTER (WHERE status = 'FINALIZED') AS finalized_count,
  COUNT(*) FILTER (WHERE status = 'ORPHANED') AS orphaned_count
FROM collections
UNION ALL
SELECT
  'feedbacks' AS model,
  COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_count,
  COUNT(*) FILTER (WHERE status = 'FINALIZED') AS finalized_count,
  COUNT(*) FILTER (WHERE status = 'ORPHANED') AS orphaned_count
FROM feedbacks
UNION ALL
SELECT
  'feedback_responses' AS model,
  COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_count,
  COUNT(*) FILTER (WHERE status = 'FINALIZED') AS finalized_count,
  COUNT(*) FILTER (WHERE status = 'ORPHANED') AS orphaned_count
FROM feedback_responses
UNION ALL
SELECT
  'revocations' AS model,
  COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_count,
  COUNT(*) FILTER (WHERE status = 'FINALIZED') AS finalized_count,
  COUNT(*) FILTER (WHERE status = 'ORPHANED') AS orphaned_count
FROM revocations
UNION ALL
SELECT
  'metadata' AS model,
  COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_count,
  COUNT(*) FILTER (WHERE status = 'FINALIZED') AS finalized_count,
  COUNT(*) FILTER (WHERE status = 'ORPHANED') AS orphaned_count
FROM metadata;

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
ALTER TABLE revocations ENABLE ROW LEVEL SECURITY;
ALTER TABLE validations ENABLE ROW LEVEL SECURITY;
ALTER TABLE atom_config ENABLE ROW LEVEL SECURITY;
ALTER TABLE indexer_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_digest_cache ENABLE ROW LEVEL SECURITY;

-- Public read access
CREATE POLICY "Public read agents" ON agents FOR SELECT USING (true);
CREATE POLICY "Public read collections" ON collections FOR SELECT USING (true);
CREATE POLICY "Public read metadata" ON metadata FOR SELECT USING (true);
CREATE POLICY "Public read feedbacks" ON feedbacks FOR SELECT USING (true);
CREATE POLICY "Public read feedback_responses" ON feedback_responses FOR SELECT USING (true);
CREATE POLICY "Public read revocations" ON revocations FOR SELECT USING (true);
CREATE POLICY "Public read validations" ON validations FOR SELECT USING (true);
CREATE POLICY "Public read atom_config" ON atom_config FOR SELECT USING (true);
CREATE POLICY "Public read agent_digest_cache" ON agent_digest_cache FOR SELECT USING (true);

-- Service role write access (indexer uses SUPABASE_DSN with service_role)
-- No INSERT/UPDATE/DELETE policies = blocked for anon users

-- =============================================
-- DETERMINISTIC ORDERING INDEXES
-- Canonical order uses:
-- (block_slot, tx_signature, tx_index NULLS LAST, event_ordinal NULLS LAST, technical tie-breaker)
-- =============================================

-- Agent ordering: deterministic global-id/backfill key
CREATE INDEX idx_agents_ordering_canonical
ON agents(block_slot, tx_signature, tx_index NULLS LAST, event_ordinal NULLS LAST, asset);

-- Feedback ordering: deterministic per client
CREATE INDEX idx_feedbacks_deterministic_order
ON feedbacks(asset, client_address, block_slot, tx_signature, tx_index NULLS LAST, event_ordinal NULLS LAST, id);

-- Feedback ordering: global within agent
CREATE INDEX idx_feedbacks_global_order
ON feedbacks(asset, block_slot, tx_signature, tx_index NULLS LAST, event_ordinal NULLS LAST, id);

-- Feedback response ordering: canonical replay/head resolution
CREATE INDEX idx_feedback_responses_canonical_order
ON feedback_responses(asset, block_slot, tx_signature, tx_index NULLS LAST, event_ordinal NULLS LAST, id);

-- Revocation ordering: canonical replay/head resolution
CREATE INDEX idx_revocations_canonical_order
ON revocations(asset, slot, tx_signature, tx_index NULLS LAST, event_ordinal NULLS LAST, id);

-- Grant read access to metadata views
GRANT SELECT ON metadata_decoded TO anon;
GRANT SELECT ON metadata_decoded TO authenticated;
GRANT SELECT ON metadata_decoded_raw TO anon;
GRANT SELECT ON metadata_decoded_raw TO authenticated;

-- Grant read access to verification stats view
GRANT SELECT ON global_stats TO anon;
GRANT SELECT ON global_stats TO authenticated;
GRANT SELECT ON verification_stats TO anon;
GRANT SELECT ON verification_stats TO authenticated;

-- Modified 2026-01-24:
-- - Added metadata_decoded VIEW (all formats with encoding info)
-- - Added metadata_decoded_raw VIEW (RAW only, JSON-safe)

-- Modified 2026-01-29:
-- - Added reorg resilience: status/verified_at columns to agents, feedbacks, feedback_responses, validations, metadata
-- - Added chain_status/chain_verified_at to validations (separate from response status)
-- - Added agent_digest_cache table for hash-chain verification
-- - Added source column to indexer_state
-- - Added partial indexes for PENDING status queries

-- Modified 2026-02-27:
-- - Added revocations bootstrap parity to schema.sql (table/indexes/verification_stats/RLS/policy/canonical ordering index)
