CREATE TABLE IF NOT EXISTS extra_proofpass_feedbacks (
  id TEXT PRIMARY KEY,
  asset TEXT NOT NULL,
  client_address TEXT NOT NULL,
  feedback_index BIGINT NOT NULL,
  tx_signature TEXT NOT NULL,
  feedback_hash TEXT NOT NULL,
  proofpass_session TEXT NOT NULL,
  context_type SMALLINT NOT NULL CHECK (context_type >= 0 AND context_type <= 255),
  context_ref_hash TEXT NOT NULL,
  block_slot BIGINT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(asset, client_address, feedback_index, tx_signature)
);

CREATE INDEX IF NOT EXISTS idx_extra_proofpass_feedback_lookup
  ON extra_proofpass_feedbacks(asset, client_address, feedback_index, tx_signature);

CREATE INDEX IF NOT EXISTS idx_extra_proofpass_feedback_hash
  ON extra_proofpass_feedbacks(feedback_hash);
