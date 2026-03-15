CREATE OR REPLACE VIEW leaderboard
WITH (security_invoker = true) AS
SELECT
  asset, owner, collection, nft_name, agent_uri,
  trust_tier, quality_score, confidence, risk_score,
  diversity_ratio, feedback_count, sort_key
FROM agents
WHERE trust_tier >= 2
  AND status != 'ORPHANED'
ORDER BY sort_key DESC, asset ASC;

CREATE OR REPLACE VIEW global_stats
WITH (security_invoker = true) AS
SELECT
  (SELECT COUNT(*) FROM agents WHERE status != 'ORPHANED') AS total_agents,
  (SELECT COUNT(*) FROM collection_pointers) AS total_collections,
  (SELECT COUNT(*) FROM feedbacks WHERE status != 'ORPHANED') AS total_feedbacks,
  (SELECT COUNT(*) FROM agents WHERE status != 'ORPHANED' AND trust_tier = 4) AS platinum_agents,
  (SELECT COUNT(*) FROM agents WHERE status != 'ORPHANED' AND trust_tier = 3) AS gold_agents,
  (SELECT ROUND(AVG(quality_score), 0) FROM agents WHERE status != 'ORPHANED' AND feedback_count > 0) AS avg_quality;

GRANT EXECUTE ON FUNCTION get_leaderboard(TEXT, INT, INT, BIGINT) TO anon;
GRANT EXECUTE ON FUNCTION get_leaderboard(TEXT, INT, INT, BIGINT) TO authenticated;
