-- Migration: Convert metadata.value from TEXT (base64) to BYTEA
-- Saves 33% storage overhead (critical at scale: 330GB on 1TB)
--
-- IMPORTANT: Run this BEFORE deploying the new indexer code

-- Convert existing base64 TEXT values to BYTEA
ALTER TABLE metadata
ALTER COLUMN value TYPE BYTEA
USING decode(value, 'base64');

-- Update schema comment
COMMENT ON COLUMN metadata.value IS 'Binary metadata value (raw bytes, previously base64 TEXT)';
