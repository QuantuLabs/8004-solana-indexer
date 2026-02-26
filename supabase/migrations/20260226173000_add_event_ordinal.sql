-- =============================================
-- Add event_ordinal for deterministic intra-transaction ordering
-- Migration: 2026-02-26
-- Canonical key: block_slot, tx_signature, tx_index NULLS LAST,
--                event_ordinal NULLS LAST, then technical tie-break (id/asset)
-- =============================================

-- Add event_ordinal columns for all tx_index-backed entities
ALTER TABLE agents ADD COLUMN IF NOT EXISTS event_ordinal INTEGER;
ALTER TABLE metadata ADD COLUMN IF NOT EXISTS event_ordinal INTEGER;
ALTER TABLE feedbacks ADD COLUMN IF NOT EXISTS event_ordinal INTEGER;
ALTER TABLE feedback_responses ADD COLUMN IF NOT EXISTS event_ordinal INTEGER;
ALTER TABLE revocations ADD COLUMN IF NOT EXISTS event_ordinal INTEGER;
ALTER TABLE validations ADD COLUMN IF NOT EXISTS event_ordinal INTEGER;

-- Ensure revocations has tx_index for canonical parity (older installs may miss it)
ALTER TABLE revocations ADD COLUMN IF NOT EXISTS tx_index INTEGER;

-- Canonical ordering indexes
DROP INDEX IF EXISTS idx_agents_ordering_canonical;
CREATE INDEX idx_agents_ordering_canonical
ON agents(block_slot, tx_signature, tx_index NULLS LAST, event_ordinal NULLS LAST, asset);

DROP INDEX IF EXISTS idx_feedbacks_deterministic_order;
CREATE INDEX idx_feedbacks_deterministic_order
ON feedbacks(asset, client_address, block_slot, tx_signature, tx_index NULLS LAST, event_ordinal NULLS LAST, id);

DROP INDEX IF EXISTS idx_feedbacks_global_order;
CREATE INDEX idx_feedbacks_global_order
ON feedbacks(asset, block_slot, tx_signature, tx_index NULLS LAST, event_ordinal NULLS LAST, id);

DROP INDEX IF EXISTS idx_feedback_responses_canonical_order;
CREATE INDEX idx_feedback_responses_canonical_order
ON feedback_responses(asset, block_slot, tx_signature, tx_index NULLS LAST, event_ordinal NULLS LAST, id);

DROP INDEX IF EXISTS idx_revocations_canonical_order;
CREATE INDEX idx_revocations_canonical_order
ON revocations(asset, slot, tx_signature, tx_index NULLS LAST, event_ordinal NULLS LAST, id);

DROP INDEX IF EXISTS idx_validations_canonical_order;
CREATE INDEX idx_validations_canonical_order
ON validations(asset, block_slot, tx_signature, tx_index NULLS LAST, event_ordinal NULLS LAST, id);
