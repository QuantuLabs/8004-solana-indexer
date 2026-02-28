-- Exclude synthetic/base registry rows from total_collections.
-- total_collections should represent user-created collections only.
CREATE OR REPLACE VIEW global_stats
WITH (security_invoker = true) AS
SELECT
  (SELECT COUNT(*) FROM agents) AS total_agents,
  (SELECT COUNT(*) FROM collections WHERE registry_type != 'BASE') AS total_collections,
  (SELECT COUNT(*) FROM feedbacks WHERE NOT is_revoked) AS total_feedbacks,
  (SELECT COUNT(*) FROM validations) AS total_validations,
  (SELECT COUNT(*) FROM agents WHERE trust_tier = 4) AS platinum_agents,
  (SELECT COUNT(*) FROM agents WHERE trust_tier = 3) AS gold_agents,
  (SELECT ROUND(AVG(quality_score), 0) FROM agents WHERE feedback_count > 0) AS avg_quality;
