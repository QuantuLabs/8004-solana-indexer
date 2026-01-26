-- Migration: Add metadata_decoded VIEWs for plug-and-play consumers
-- Allows SQL consumers to query decoded metadata without manual decompression
--
-- Format: BYTEA with compression prefix
--   0x00 + data = RAW (UTF-8 direct, JSON-safe)
--   0x01 + data = ZSTD compressed (rare, custom fields > 256 bytes)

-- Drop existing views if any
DROP VIEW IF EXISTS metadata_decoded CASCADE;
DROP VIEW IF EXISTS metadata_decoded_raw CASCADE;

-- VIEW 1: metadata_decoded - All formats
-- Returns value_text for RAW, '_compressed:base64...' for ZSTD
-- Use this when you need all metadata regardless of compression
CREATE OR REPLACE VIEW metadata_decoded AS
SELECT
  id,
  asset,
  key,
  key_hash,
  immutable,
  block_slot,
  tx_signature,
  created_at,
  updated_at,
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
FROM metadata;

-- VIEW 2: metadata_decoded_raw - JSON-safe only
-- Filters to RAW entries only, guarantees value_text is always valid UTF-8/JSON
-- Use this when you need JSON-parseable values (most common case)
CREATE OR REPLACE VIEW metadata_decoded_raw AS
SELECT
  id,
  asset,
  key,
  key_hash,
  immutable,
  block_slot,
  tx_signature,
  created_at,
  updated_at,
  convert_from(substring(value from 2), 'utf8') AS value_text
FROM metadata
WHERE value IS NOT NULL
  AND octet_length(value) > 0
  AND get_byte(value, 0) = 0;

-- Grant read access to views
GRANT SELECT ON metadata_decoded TO anon;
GRANT SELECT ON metadata_decoded TO authenticated;
GRANT SELECT ON metadata_decoded_raw TO anon;
GRANT SELECT ON metadata_decoded_raw TO authenticated;

COMMENT ON VIEW metadata_decoded IS 'Decoded metadata with encoding info. value_text may contain "_compressed:base64..." for ZSTD entries.';
COMMENT ON VIEW metadata_decoded_raw IS 'Decoded metadata (RAW only). value_text is always valid UTF-8, JSON-safe.';
