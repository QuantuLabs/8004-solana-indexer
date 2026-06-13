ALTER TABLE metadata ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE;
UPDATE metadata SET deleted = FALSE WHERE deleted IS NULL;
ALTER TABLE metadata ALTER COLUMN deleted SET DEFAULT FALSE;
ALTER TABLE metadata ALTER COLUMN deleted SET NOT NULL;

DROP INDEX IF EXISTS idx_metadata_status;
CREATE INDEX idx_metadata_status
ON metadata(status)
WHERE status = 'PENDING' AND NOT deleted;

DROP INDEX IF EXISTS idx_metadata_graphql_asset_key;
CREATE INDEX idx_metadata_graphql_asset_key
ON metadata(asset, key)
WHERE status != 'ORPHANED' AND NOT deleted;

DROP INDEX IF EXISTS idx_metadata_graphql_uri_asset_updated;
CREATE INDEX idx_metadata_graphql_uri_asset_updated
ON metadata(asset, updated_at DESC)
WHERE status != 'ORPHANED' AND NOT deleted AND key LIKE '\_uri:%' ESCAPE '\';

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
FROM metadata
WHERE NOT deleted;

CREATE OR REPLACE VIEW metadata_decoded_raw
WITH (security_invoker = true) AS
SELECT
  id, asset, key, key_hash, immutable, block_slot, tx_signature, created_at, updated_at,
  convert_from(substring(value from 2), 'utf8') AS value_text
FROM metadata
WHERE NOT deleted
  AND value IS NOT NULL
  AND octet_length(value) > 0
  AND get_byte(value, 0) = 0;

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
  (
    COUNT(*) FILTER (WHERE status = 'ORPHANED')
    + COALESCE((SELECT COUNT(*) FROM orphan_feedbacks), 0)
  ) AS orphaned_count
FROM feedbacks
UNION ALL
SELECT
  'feedback_responses' AS model,
  COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_count,
  COUNT(*) FILTER (WHERE status = 'FINALIZED') AS finalized_count,
  (
    COUNT(*) FILTER (WHERE status = 'ORPHANED')
    + COALESCE((SELECT COUNT(*) FROM orphan_responses), 0)
  ) AS orphaned_count
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
  COUNT(*) FILTER (WHERE status = 'PENDING' AND NOT deleted) AS pending_count,
  COUNT(*) FILTER (WHERE status = 'FINALIZED' AND NOT deleted) AS finalized_count,
  COUNT(*) FILTER (WHERE status = 'ORPHANED' AND NOT deleted) AS orphaned_count
FROM metadata
UNION ALL
SELECT
  'validations' AS model,
  COUNT(*) FILTER (WHERE chain_status = 'PENDING') AS pending_count,
  COUNT(*) FILTER (WHERE chain_status = 'FINALIZED') AS finalized_count,
  COUNT(*) FILTER (WHERE chain_status = 'ORPHANED') AS orphaned_count
FROM validations;

DROP POLICY IF EXISTS "Public read metadata" ON metadata;
CREATE POLICY "Public read metadata" ON metadata FOR SELECT USING (NOT deleted);

GRANT SELECT ON metadata_decoded TO anon;
GRANT SELECT ON metadata_decoded TO authenticated;
GRANT SELECT ON metadata_decoded_raw TO anon;
GRANT SELECT ON metadata_decoded_raw TO authenticated;
GRANT SELECT ON verification_stats TO anon;
GRANT SELECT ON verification_stats TO authenticated;
