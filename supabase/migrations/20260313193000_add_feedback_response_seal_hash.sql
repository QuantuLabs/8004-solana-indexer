ALTER TABLE feedback_responses
ADD COLUMN IF NOT EXISTS seal_hash TEXT;
