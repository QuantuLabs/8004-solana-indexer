-- =============================================
-- Reorg Resilience Migration
-- Adds verification status tracking for all indexable data
-- =============================================

-- =============================================
-- 1. Add status columns to all indexable tables
-- =============================================

-- Agents: status for existence verification
ALTER TABLE agents ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PENDING';
ALTER TABLE agents ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ;
ALTER TABLE agents ADD COLUMN IF NOT EXISTS verified_slot BIGINT;

-- Feedbacks: status for hash-chain verification
ALTER TABLE feedbacks ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PENDING';
ALTER TABLE feedbacks ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ;

-- Feedback responses: status for hash-chain verification
ALTER TABLE feedback_responses ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PENDING';
ALTER TABLE feedback_responses ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ;

-- Validations: status for existence verification
ALTER TABLE validations ADD COLUMN IF NOT EXISTS chain_status TEXT DEFAULT 'PENDING';
ALTER TABLE validations ADD COLUMN IF NOT EXISTS chain_verified_at TIMESTAMPTZ;

-- Metadata: status for existence verification
ALTER TABLE metadata ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PENDING';
ALTER TABLE metadata ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ;

-- Collections: status for existence verification
ALTER TABLE collections ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PENDING';
ALTER TABLE collections ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ;

-- =============================================
-- 2. Update indexer_state with source tracking
-- =============================================

ALTER TABLE indexer_state ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'poller';

-- =============================================
-- 3. Create agent_digest_cache table
-- =============================================

CREATE TABLE IF NOT EXISTS agent_digest_cache (
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

-- =============================================
-- 4. Create partial indexes for efficient verification queries
-- =============================================

-- Agents pending verification (most common query)
CREATE INDEX IF NOT EXISTS idx_agents_status_pending
ON agents(status) WHERE status = 'PENDING';

-- Feedbacks pending verification
CREATE INDEX IF NOT EXISTS idx_feedbacks_status_pending
ON feedbacks(status) WHERE status = 'PENDING';

-- Feedback responses pending verification
CREATE INDEX IF NOT EXISTS idx_feedback_responses_status_pending
ON feedback_responses(status) WHERE status = 'PENDING';

-- Validations pending verification
CREATE INDEX IF NOT EXISTS idx_validations_chain_status_pending
ON validations(chain_status) WHERE chain_status = 'PENDING';

-- Metadata pending verification
CREATE INDEX IF NOT EXISTS idx_metadata_status_pending
ON metadata(status) WHERE status = 'PENDING';

-- Collections pending verification
CREATE INDEX IF NOT EXISTS idx_collections_status_pending
ON collections(status) WHERE status = 'PENDING';

-- Agent digest cache: agents needing gap fill
CREATE INDEX IF NOT EXISTS idx_agent_digest_cache_gap_fill
ON agent_digest_cache(needs_gap_fill) WHERE needs_gap_fill = TRUE;

-- =============================================
-- 5. Add CHECK constraints for status values
-- =============================================

ALTER TABLE agents DROP CONSTRAINT IF EXISTS agents_status_check;
ALTER TABLE agents ADD CONSTRAINT agents_status_check
CHECK (status IN ('PENDING', 'FINALIZED', 'ORPHANED'));

ALTER TABLE feedbacks DROP CONSTRAINT IF EXISTS feedbacks_status_check;
ALTER TABLE feedbacks ADD CONSTRAINT feedbacks_status_check
CHECK (status IN ('PENDING', 'FINALIZED', 'ORPHANED'));

ALTER TABLE feedback_responses DROP CONSTRAINT IF EXISTS feedback_responses_status_check;
ALTER TABLE feedback_responses ADD CONSTRAINT feedback_responses_status_check
CHECK (status IN ('PENDING', 'FINALIZED', 'ORPHANED'));

ALTER TABLE validations DROP CONSTRAINT IF EXISTS validations_chain_status_check;
ALTER TABLE validations ADD CONSTRAINT validations_chain_status_check
CHECK (chain_status IN ('PENDING', 'FINALIZED', 'ORPHANED'));

ALTER TABLE metadata DROP CONSTRAINT IF EXISTS metadata_status_check;
ALTER TABLE metadata ADD CONSTRAINT metadata_status_check
CHECK (status IN ('PENDING', 'FINALIZED', 'ORPHANED'));

ALTER TABLE collections DROP CONSTRAINT IF EXISTS collections_status_check;
ALTER TABLE collections ADD CONSTRAINT collections_status_check
CHECK (status IN ('PENDING', 'FINALIZED', 'ORPHANED'));

-- =============================================
-- 6. Update RLS for agent_digest_cache
-- =============================================

ALTER TABLE agent_digest_cache ENABLE ROW LEVEL SECURITY;

-- Public read access
CREATE POLICY "Public read agent_digest_cache"
ON agent_digest_cache FOR SELECT USING (true);

-- Service role write access (indexer uses SUPABASE_DSN with service_role)
-- No INSERT/UPDATE/DELETE policies = blocked for anon users

-- =============================================
-- 7. Create verification stats view
-- =============================================

CREATE OR REPLACE VIEW verification_stats AS
SELECT
  'agents' AS model,
  COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_count,
  COUNT(*) FILTER (WHERE status = 'FINALIZED') AS finalized_count,
  COUNT(*) FILTER (WHERE status = 'ORPHANED') AS orphaned_count
FROM agents
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
  'validations' AS model,
  COUNT(*) FILTER (WHERE chain_status = 'PENDING') AS pending_count,
  COUNT(*) FILTER (WHERE chain_status = 'FINALIZED') AS finalized_count,
  COUNT(*) FILTER (WHERE chain_status = 'ORPHANED') AS orphaned_count
FROM validations
UNION ALL
SELECT
  'metadata' AS model,
  COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_count,
  COUNT(*) FILTER (WHERE status = 'FINALIZED') AS finalized_count,
  COUNT(*) FILTER (WHERE status = 'ORPHANED') AS orphaned_count
FROM metadata
UNION ALL
SELECT
  'collections' AS model,
  COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_count,
  COUNT(*) FILTER (WHERE status = 'FINALIZED') AS finalized_count,
  COUNT(*) FILTER (WHERE status = 'ORPHANED') AS orphaned_count
FROM collections;

-- Grant read access to verification stats view
GRANT SELECT ON verification_stats TO anon;
GRANT SELECT ON verification_stats TO authenticated;
