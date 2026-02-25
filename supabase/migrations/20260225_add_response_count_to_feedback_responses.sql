-- Ensure response_count is persisted per response event (v0.6.0 parity)
ALTER TABLE feedback_responses
  ADD COLUMN IF NOT EXISTS response_count BIGINT NOT NULL DEFAULT 0;
