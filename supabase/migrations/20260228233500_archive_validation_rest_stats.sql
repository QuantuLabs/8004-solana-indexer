-- Archive validation counters from REST-facing stats views.
-- Drop first because CREATE OR REPLACE VIEW cannot remove existing columns.
DROP VIEW IF EXISTS verification_stats;
DROP VIEW IF EXISTS global_stats;

CREATE VIEW global_stats
WITH (security_invoker = true) AS
SELECT
  (SELECT COUNT(*) FROM agents) AS total_agents,
  (SELECT COUNT(*) FROM collections WHERE status != 'ORPHANED' AND registry_type != 'BASE') AS total_collections,
  (SELECT COUNT(*) FROM feedbacks WHERE NOT is_revoked) AS total_feedbacks,
  (SELECT COUNT(*) FROM agents WHERE trust_tier = 4) AS platinum_agents,
  (SELECT COUNT(*) FROM agents WHERE trust_tier = 3) AS gold_agents,
  (SELECT ROUND(AVG(quality_score), 0) FROM agents WHERE feedback_count > 0) AS avg_quality;

CREATE VIEW verification_stats
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
  COUNT(*) FILTER (WHERE status = 'ORPHANED') AS orphaned_count
FROM feedbacks
UNION ALL
SELECT
  'feedback_responses' AS model,
  COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_count,
  COUNT(*) FILTER (WHERE status = 'FINALIZED') AS finalized_count,
  COUNT(*) FILTER (WHERE status = 'ORPHANED') AS orphaned_count
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
  COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_count,
  COUNT(*) FILTER (WHERE status = 'FINALIZED') AS finalized_count,
  COUNT(*) FILTER (WHERE status = 'ORPHANED') AS orphaned_count
FROM metadata;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
    GRANT SELECT ON global_stats TO anon;
    GRANT SELECT ON verification_stats TO anon;
  END IF;

  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
    GRANT SELECT ON global_stats TO authenticated;
    GRANT SELECT ON verification_stats TO authenticated;
  END IF;
END;
$$;
