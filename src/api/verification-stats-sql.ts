export const VERIFICATION_STATS_SQL = `SELECT
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
  (
    COUNT(*) FILTER (WHERE status = 'ORPHANED')
    + COALESCE((SELECT COUNT(*) FROM orphan_feedbacks), 0)
  ) AS orphaned_count
FROM feedbacks
UNION ALL
SELECT
  'feedback_responses' AS model,
  COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_count,
  COUNT(*) FILTER (WHERE status = 'FINALIZED') AS finalized_count,
  (
    COUNT(*) FILTER (WHERE status = 'ORPHANED')
    + COALESCE((SELECT COUNT(*) FROM orphan_responses), 0)
  ) AS orphaned_count
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
FROM metadata
UNION ALL
SELECT
  'validations' AS model,
  COUNT(*) FILTER (WHERE chain_status = 'PENDING') AS pending_count,
  COUNT(*) FILTER (WHERE chain_status = 'FINALIZED') AS finalized_count,
  COUNT(*) FILTER (WHERE chain_status = 'ORPHANED') AS orphaned_count
FROM validations`;
