-- Migration: Add client_address to feedback_responses
-- Date: 2026-01-19
-- Purpose: Fix response indexing to match ERC-8004 spec (asset, client, feedbackIndex, responder)

-- Step 1: Add client_address column (allow NULL temporarily for existing data)
ALTER TABLE feedback_responses ADD COLUMN IF NOT EXISTS client_address TEXT;

-- Step 2: Drop existing unique constraint
ALTER TABLE feedback_responses DROP CONSTRAINT IF EXISTS feedback_responses_asset_feedback_index_responder_key;

-- Step 3: For existing responses without client_address, we cannot backfill accurately.
-- Clean slate: wipe existing responses so all future rows include client_address.
TRUNCATE feedback_responses;

-- Step 4: Enforce client_address NOT NULL after wipe
ALTER TABLE feedback_responses ALTER COLUMN client_address SET NOT NULL;

-- Step 5: Add new unique constraint with client_address
ALTER TABLE feedback_responses ADD CONSTRAINT feedback_responses_unique
  UNIQUE(asset, client_address, feedback_index, responder);

-- Step 6: Drop old index and create new one with client_address
DROP INDEX IF EXISTS idx_responses_lookup;
CREATE INDEX idx_responses_lookup ON feedback_responses(asset, client_address, feedback_index);

-- Step 7: Verify
SELECT COUNT(*) as total_responses FROM feedback_responses;
SELECT COUNT(*) as responses_with_client FROM feedback_responses WHERE client_address IS NOT NULL;

-- Modified 2026-01-19:
-- - Added client_address column to feedback_responses
-- - Updated unique constraint to include client_address
-- - Updated index to include client_address
