-- Align global_stats total_collections semantics with REST/GraphQL:
-- exclude ORPHANED rows and BASE registry rows.

CREATE OR REPLACE VIEW global_stats
WITH (security_invoker = true) AS
SELECT
  (SELECT COUNT(*) FROM agents) AS total_agents,
  (SELECT COUNT(*) FROM collections WHERE status != 'ORPHANED' AND registry_type != 'BASE') AS total_collections,
  (SELECT COUNT(*) FROM feedbacks WHERE NOT is_revoked) AS total_feedbacks,
  (SELECT COUNT(*) FROM agents WHERE trust_tier = 4) AS platinum_agents,
  (SELECT COUNT(*) FROM agents WHERE trust_tier = 3) AS gold_agents,
  (SELECT ROUND(AVG(quality_score), 0) FROM agents WHERE feedback_count > 0) AS avg_quality;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
    GRANT SELECT ON global_stats TO anon;
  END IF;

  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
    GRANT SELECT ON global_stats TO authenticated;
  END IF;
END;
$$;
