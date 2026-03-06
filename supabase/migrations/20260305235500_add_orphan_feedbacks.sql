CREATE TABLE IF NOT EXISTS orphan_feedbacks (
  id TEXT PRIMARY KEY,
  asset TEXT NOT NULL,
  client_address TEXT NOT NULL,
  feedback_index BIGINT NOT NULL,
  value NUMERIC(39,0) DEFAULT 0,
  value_decimals SMALLINT DEFAULT 0 CHECK (value_decimals >= 0 AND value_decimals <= 18),
  score SMALLINT CHECK (score >= 0 AND score <= 100),
  tag1 TEXT,
  tag2 TEXT,
  endpoint TEXT,
  feedback_uri TEXT,
  feedback_hash TEXT,
  running_digest BYTEA,
  atom_enabled BOOLEAN DEFAULT FALSE,
  new_trust_tier SMALLINT DEFAULT 0,
  new_quality_score SMALLINT DEFAULT 0,
  new_confidence SMALLINT DEFAULT 0,
  new_risk_score SMALLINT DEFAULT 0,
  new_diversity_ratio SMALLINT DEFAULT 0,
  block_slot BIGINT,
  tx_index INTEGER,
  event_ordinal INTEGER,
  tx_signature TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(asset, client_address, feedback_index)
);

CREATE INDEX IF NOT EXISTS idx_orphan_feedbacks_asset
  ON orphan_feedbacks(asset);

CREATE INDEX IF NOT EXISTS idx_orphan_feedbacks_lookup
  ON orphan_feedbacks(asset, client_address, feedback_index);
