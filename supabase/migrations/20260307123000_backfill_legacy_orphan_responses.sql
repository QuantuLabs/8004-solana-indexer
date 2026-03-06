-- Move legacy response-before-feedback rows into orphan_responses so the current
-- runtime can reconcile them when the parent feedback arrives later.
-- Important: only move rows that still have no parent feedback. Real mismatch
-- rows (parent exists, response_id stays NULL, status ORPHANED) must stay in
-- feedback_responses.

INSERT INTO orphan_responses (
  id,
  asset,
  client_address,
  feedback_index,
  responder,
  response_uri,
  response_hash,
  seal_hash,
  running_digest,
  response_count,
  block_slot,
  tx_index,
  event_ordinal,
  tx_signature,
  created_at
)
SELECT
  fr.id,
  fr.asset,
  fr.client_address,
  fr.feedback_index,
  fr.responder,
  fr.response_uri,
  fr.response_hash,
  NULL,
  fr.running_digest,
  fr.response_count,
  fr.block_slot,
  fr.tx_index,
  fr.event_ordinal,
  fr.tx_signature,
  fr.created_at
FROM feedback_responses fr
WHERE fr.status = 'ORPHANED'
  AND fr.response_id IS NULL
  AND NOT EXISTS (
    SELECT 1
    FROM feedbacks f
    WHERE f.asset = fr.asset
      AND f.client_address = fr.client_address
      AND f.feedback_index = fr.feedback_index
  )
ON CONFLICT (id) DO NOTHING;

DELETE FROM feedback_responses fr
WHERE fr.status = 'ORPHANED'
  AND fr.response_id IS NULL
  AND NOT EXISTS (
    SELECT 1
    FROM feedbacks f
    WHERE f.asset = fr.asset
      AND f.client_address = fr.client_address
      AND f.feedback_index = fr.feedback_index
  )
  AND EXISTS (
    SELECT 1
    FROM orphan_responses o
    WHERE o.id = fr.id
  );
