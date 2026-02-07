-- Add updated_at column to feedbacks table (referenced by batch processor ON CONFLICT)
ALTER TABLE feedbacks ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
