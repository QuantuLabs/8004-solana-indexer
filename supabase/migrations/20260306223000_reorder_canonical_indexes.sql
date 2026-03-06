-- Reorder canonical deterministic indexes to match runtime event ordering
-- Canonical order: block/slot -> tx_index NULLS LAST -> event_ordinal NULLS LAST -> tx_signature

DROP INDEX IF EXISTS idx_agents_ordering_canonical;
CREATE INDEX idx_agents_ordering_canonical
ON agents(block_slot, tx_index NULLS LAST, event_ordinal NULLS LAST, tx_signature, asset);

DROP INDEX IF EXISTS idx_feedbacks_deterministic_order;
CREATE INDEX idx_feedbacks_deterministic_order
ON feedbacks(asset, client_address, block_slot, tx_index NULLS LAST, event_ordinal NULLS LAST, tx_signature, id);

DROP INDEX IF EXISTS idx_feedbacks_global_order;
CREATE INDEX idx_feedbacks_global_order
ON feedbacks(asset, block_slot, tx_index NULLS LAST, event_ordinal NULLS LAST, tx_signature, id);

DROP INDEX IF EXISTS idx_feedback_responses_canonical_order;
CREATE INDEX idx_feedback_responses_canonical_order
ON feedback_responses(asset, block_slot, tx_index NULLS LAST, event_ordinal NULLS LAST, tx_signature, id);

DROP INDEX IF EXISTS idx_revocations_canonical_order;
CREATE INDEX idx_revocations_canonical_order
ON revocations(asset, slot, tx_index NULLS LAST, event_ordinal NULLS LAST, tx_signature, id);
