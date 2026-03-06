CREATE TABLE IF NOT EXISTS orphan_responses (
  id TEXT PRIMARY KEY,
  asset TEXT NOT NULL,
  client_address TEXT NOT NULL,
  feedback_index BIGINT NOT NULL,
  responder TEXT NOT NULL,
  response_uri TEXT,
  response_hash TEXT,
  seal_hash TEXT,
  running_digest BYTEA,
  response_count BIGINT NOT NULL DEFAULT 0,
  block_slot BIGINT,
  tx_index INTEGER,
  event_ordinal INTEGER,
  tx_signature TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(asset, client_address, feedback_index, responder, tx_signature)
);

CREATE INDEX IF NOT EXISTS idx_orphan_responses_asset
ON orphan_responses(asset);

CREATE INDEX IF NOT EXISTS idx_orphan_responses_lookup
ON orphan_responses(asset, client_address, feedback_index);
