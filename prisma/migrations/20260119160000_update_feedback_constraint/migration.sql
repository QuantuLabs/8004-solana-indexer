-- Migration: Update Feedback unique constraint to include client
-- This is a BREAKING CHANGE if there are duplicate (agentId, feedbackIndex) with different clients

-- Step 1: Drop the old unique constraint
DROP INDEX IF EXISTS "Feedback_agentId_feedbackIndex_key";

-- Step 2: Create the new unique constraint with client
CREATE UNIQUE INDEX "Feedback_agentId_client_feedbackIndex_key" ON "Feedback"("agentId", "client", "feedbackIndex");
