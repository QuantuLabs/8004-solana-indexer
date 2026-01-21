-- Fix: Change nonce from INTEGER to BIGINT
-- Reason: Solana u32 nonces can exceed PostgreSQL INTEGER max (2^31-1)
-- Example: nonce 3773730093 > 2147483647

-- Drop the unique constraint first
ALTER TABLE validations DROP CONSTRAINT IF EXISTS validations_asset_validator_address_nonce_key;

-- Change column type
ALTER TABLE validations ALTER COLUMN nonce TYPE BIGINT;

-- Re-add the unique constraint
ALTER TABLE validations ADD CONSTRAINT validations_asset_validator_address_nonce_key
  UNIQUE (asset, validator_address, nonce);
