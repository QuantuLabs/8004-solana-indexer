ALTER TABLE feedbacks ADD COLUMN running_digest BYTEA;
ALTER TABLE feedback_responses ADD COLUMN running_digest BYTEA;

CREATE TABLE revocations (
  id TEXT PRIMARY KEY,
  asset TEXT NOT NULL REFERENCES agents(asset) ON DELETE CASCADE,
  client_address TEXT NOT NULL,
  feedback_index BIGINT NOT NULL,
  feedback_hash TEXT,
  slot BIGINT NOT NULL,
  original_score SMALLINT,
  atom_enabled BOOLEAN DEFAULT FALSE,
  had_impact BOOLEAN DEFAULT FALSE,
  running_digest BYTEA,
  revoke_count BIGINT NOT NULL,
  tx_signature TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  status TEXT DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'FINALIZED', 'ORPHANED')),
  verified_at TIMESTAMPTZ,
  UNIQUE(asset, client_address, feedback_index)
);

CREATE INDEX idx_revocations_asset ON revocations(asset);
CREATE INDEX idx_revocations_status ON revocations(status) WHERE status = 'PENDING';

ALTER TABLE revocations ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Public read revocations" ON revocations FOR SELECT USING (true);
