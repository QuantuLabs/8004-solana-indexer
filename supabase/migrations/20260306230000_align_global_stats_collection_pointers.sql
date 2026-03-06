-- Align REST/PostgREST global_stats total_collections with canonical collection scopes.
CREATE OR REPLACE VIEW global_stats
WITH (security_invoker = true)
AS
SELECT
  (SELECT COUNT(*) FROM agents WHERE status != 'ORPHANED') AS total_agents,
  (SELECT COUNT(*) FROM collection_pointers) AS total_collections,
  (SELECT COUNT(*) FROM feedbacks WHERE status != 'ORPHANED') AS total_feedbacks,
  (SELECT COUNT(*) FROM agents WHERE trust_tier = 4) AS platinum_agents,
  (SELECT COUNT(*) FROM agents WHERE trust_tier = 3) AS gold_agents,
  (SELECT ROUND(AVG(quality_score), 0) FROM agents WHERE feedback_count > 0) AS avg_quality;

DO $$
BEGIN
  BEGIN
    GRANT SELECT ON global_stats TO anon;
  EXCEPTION WHEN undefined_object THEN NULL;
  END;
  BEGIN
    GRANT SELECT ON global_stats TO authenticated;
  EXCEPTION WHEN undefined_object THEN NULL;
  END;
END $$;
