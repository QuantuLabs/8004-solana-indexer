CREATE OR REPLACE VIEW agent_reputation
WITH (security_invoker = true) AS
WITH feedback_stats AS (
  SELECT
    f.asset,
    COUNT(*) FILTER (
      WHERE f.status != 'ORPHANED'
        AND f.is_revoked = false
        AND f.score IS NOT NULL
    )::integer AS feedback_count,
    ROUND(AVG(f.score) FILTER (
      WHERE f.status != 'ORPHANED'
        AND f.is_revoked = false
        AND f.score IS NOT NULL
    ), 0) AS avg_score,
    COUNT(*) FILTER (
      WHERE f.status != 'ORPHANED'
        AND f.is_revoked = false
        AND f.score IS NOT NULL
        AND f.score >= 50
    )::integer AS positive_count,
    COUNT(*) FILTER (
      WHERE f.status != 'ORPHANED'
        AND f.is_revoked = false
        AND f.score IS NOT NULL
        AND f.score < 50
    )::integer AS negative_count
  FROM feedbacks f
  GROUP BY f.asset
),
validation_stats AS (
  SELECT
    v.asset,
    COUNT(*) FILTER (
      WHERE v.chain_status != 'ORPHANED'
    )::integer AS validation_count
  FROM validations v
  GROUP BY v.asset
)
SELECT
  a.asset,
  a.owner,
  a.collection,
  a.nft_name,
  a.agent_uri,
  COALESCE(fs.feedback_count, a.feedback_count, 0) AS feedback_count,
  CASE
    WHEN COALESCE(fs.feedback_count, a.feedback_count, 0) > 0
      THEN COALESCE(fs.avg_score, a.raw_avg_score::numeric)
    ELSE NULL
  END AS avg_score,
  COALESCE(fs.positive_count, 0) AS positive_count,
  COALESCE(fs.negative_count, 0) AS negative_count,
  COALESCE(vs.validation_count, 0) AS validation_count
FROM agents a
LEFT JOIN feedback_stats fs ON fs.asset = a.asset
LEFT JOIN validation_stats vs ON vs.asset = a.asset
WHERE a.status != 'ORPHANED';

CREATE OR REPLACE FUNCTION get_collection_agents(
  collection_id BIGINT,
  page_limit INT DEFAULT 20,
  page_offset INT DEFAULT 0
)
RETURNS TABLE (
  asset TEXT,
  owner TEXT,
  collection TEXT,
  nft_name TEXT,
  agent_uri TEXT,
  feedback_count INTEGER,
  avg_score NUMERIC,
  positive_count INTEGER,
  negative_count INTEGER,
  validation_count INTEGER
) AS $$
  WITH feedback_stats AS (
    SELECT
      f.asset,
      COUNT(*) FILTER (
        WHERE f.status != 'ORPHANED'
          AND f.is_revoked = false
          AND f.score IS NOT NULL
      )::integer AS feedback_count,
      ROUND(AVG(f.score) FILTER (
        WHERE f.status != 'ORPHANED'
          AND f.is_revoked = false
          AND f.score IS NOT NULL
      ), 0) AS avg_score,
      COUNT(*) FILTER (
        WHERE f.status != 'ORPHANED'
          AND f.is_revoked = false
          AND f.score IS NOT NULL
          AND f.score >= 50
      )::integer AS positive_count,
      COUNT(*) FILTER (
        WHERE f.status != 'ORPHANED'
          AND f.is_revoked = false
          AND f.score IS NOT NULL
          AND f.score < 50
      )::integer AS negative_count
    FROM feedbacks f
    GROUP BY f.asset
  ),
  validation_stats AS (
    SELECT
      v.asset,
      COUNT(*) FILTER (
        WHERE v.chain_status != 'ORPHANED'
      )::integer AS validation_count
    FROM validations v
    GROUP BY v.asset
  )
  SELECT
    a.asset,
    a.owner,
    a.collection,
    a.nft_name,
    a.agent_uri,
    COALESCE(fs.feedback_count, a.feedback_count, 0) AS feedback_count,
    CASE
      WHEN COALESCE(fs.feedback_count, a.feedback_count, 0) > 0
        THEN COALESCE(fs.avg_score, a.raw_avg_score::numeric)
      ELSE NULL
    END AS avg_score,
    COALESCE(fs.positive_count, 0) AS positive_count,
    COALESCE(fs.negative_count, 0) AS negative_count,
    COALESCE(vs.validation_count, 0) AS validation_count
  FROM collection_pointers cp
  JOIN agents a
    ON a.canonical_col = cp.col
   AND a.creator = cp.creator
  LEFT JOIN feedback_stats fs ON fs.asset = a.asset
  LEFT JOIN validation_stats vs ON vs.asset = a.asset
  WHERE cp.collection_id = $1
    AND a.status != 'ORPHANED'
  ORDER BY a.created_at DESC, a.asset DESC
  LIMIT GREATEST($2, 0)
  OFFSET GREATEST($3, 0);
$$ LANGUAGE sql STABLE;

GRANT SELECT ON agent_reputation TO anon;
GRANT SELECT ON agent_reputation TO authenticated;
GRANT EXECUTE ON FUNCTION get_collection_agents(BIGINT, INT, INT) TO anon;
GRANT EXECUTE ON FUNCTION get_collection_agents(BIGINT, INT, INT) TO authenticated;
