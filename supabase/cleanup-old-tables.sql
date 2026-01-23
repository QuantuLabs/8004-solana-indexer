-- =============================================
-- Cleanup: Drop old PascalCase tables (Prisma legacy)
-- These tables are empty and replaced by snake_case tables
-- Run this in Supabase SQL Editor
-- =============================================

-- Disable RLS temporarily for cleanup
ALTER TABLE IF EXISTS "Agent" DISABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS "AgentMetadata" DISABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS "EventLog" DISABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS "Feedback" DISABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS "FeedbackResponse" DISABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS "IndexerState" DISABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS "Registry" DISABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS "Validation" DISABLE ROW LEVEL SECURITY;

-- Drop old PascalCase tables (all have 0 rows)
DROP TABLE IF EXISTS "FeedbackResponse" CASCADE;
DROP TABLE IF EXISTS "Feedback" CASCADE;
DROP TABLE IF EXISTS "Validation" CASCADE;
DROP TABLE IF EXISTS "AgentMetadata" CASCADE;
DROP TABLE IF EXISTS "Agent" CASCADE;
DROP TABLE IF EXISTS "Registry" CASCADE;
DROP TABLE IF EXISTS "EventLog" CASCADE;
DROP TABLE IF EXISTS "IndexerState" CASCADE;

-- Verify cleanup
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;
