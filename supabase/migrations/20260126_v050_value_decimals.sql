-- v0.5.0: Add value, value_decimals columns and make score nullable
-- EVM-compatible feedback signature support

-- Add value column (i64 raw metric value)
ALTER TABLE feedbacks ADD COLUMN IF NOT EXISTS value BIGINT DEFAULT 0;

-- Add value_decimals column (u8 decimal precision 0-6)
ALTER TABLE feedbacks ADD COLUMN IF NOT EXISTS value_decimals SMALLINT DEFAULT 0;

-- Add check constraint for value_decimals range
ALTER TABLE feedbacks DROP CONSTRAINT IF EXISTS feedbacks_value_decimals_check;
ALTER TABLE feedbacks ADD CONSTRAINT feedbacks_value_decimals_check
  CHECK (value_decimals >= 0 AND value_decimals <= 6);

-- Make score nullable (Option<u8> - null means ATOM skipped)
-- First drop the existing NOT NULL constraint
ALTER TABLE feedbacks ALTER COLUMN score DROP NOT NULL;

-- Add tx_index column if not exists (for deterministic ordering)
ALTER TABLE feedbacks ADD COLUMN IF NOT EXISTS tx_index INTEGER;

-- Update the unique constraint comment to reflect the actual behavior
COMMENT ON COLUMN feedbacks.score IS 'Score 0-100, NULL if ATOM was skipped (v0.5.0)';
COMMENT ON COLUMN feedbacks.value IS 'Raw metric value as i64, e.g., profit in cents (v0.5.0)';
COMMENT ON COLUMN feedbacks.value_decimals IS 'Decimal precision 0-6 for value interpretation (v0.5.0)';
