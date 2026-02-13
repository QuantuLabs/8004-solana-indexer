import { Pool } from 'pg';
import { performance } from 'node:perf_hooks';
import fs from 'node:fs';

const conn = process.env.BENCH_DSN;
if (!conn) {
  console.error('Missing BENCH_DSN. Set BENCH_DSN to your PostgreSQL DSN.');
  process.exit(1);
}
const outFile = process.env.BENCH_OUT || '/tmp/bench_graphql_sql_results.json';
const iterations = Number(process.env.BENCH_ITERS || 20);
const warmup = Number(process.env.BENCH_WARMUP || 3);

const tests = [
  {
    name: 'agents_list_created_at',
    sql: `SELECT asset, owner, agent_uri, agent_wallet, collection, nft_name,
                 atom_enabled, trust_tier, quality_score, confidence, risk_score,
                 diversity_ratio, sort_key, status, verified_at,
                 created_at, updated_at, tx_signature as created_tx_signature, block_slot as created_slot,
                 NULL::bytea as feedback_digest, NULL::bytea as response_digest, NULL::bytea as revoke_digest,
                 NULL::bigint as feedback_count, NULL::bigint as response_count, NULL::bigint as revoke_count
          FROM agents
          WHERE status != 'ORPHANED'
          ORDER BY created_at DESC, asset DESC
          LIMIT $1::int OFFSET $2::int`,
    params: [100, 0],
  },
  {
    name: 'agents_list_created_at_after_cursor',
    sql: `SELECT asset, owner, agent_uri
          FROM agents
          WHERE status != 'ORPHANED'
            AND (created_at, asset) < ($1::timestamptz, $2::text)
          ORDER BY created_at DESC, asset DESC
          LIMIT $3::int OFFSET $4::int`,
    params: [new Date(Date.now() - 5 * 24 * 3600 * 1000).toISOString(), 'asset_10000', 100, 0],
  },
  {
    name: 'feedbacks_list_created_at',
    sql: `SELECT id, asset, client_address, feedback_index, value, value_decimals,
                 score, tag1, tag2, endpoint, feedback_uri, feedback_hash,
                 running_digest, is_revoked, status, verified_at,
                 tx_signature, block_slot, created_at, revoked_at
          FROM feedbacks
          WHERE status != 'ORPHANED'
          ORDER BY created_at DESC, asset DESC, id DESC
          LIMIT $1::int OFFSET $2::int`,
    params: [100, 0],
  },
  {
    name: 'feedbacks_list_order_value',
    sql: `SELECT id, asset, value, created_at
          FROM feedbacks
          WHERE status != 'ORPHANED'
          ORDER BY value DESC, asset DESC, id DESC
          LIMIT $1::int OFFSET $2::int`,
    params: [100, 0],
  },
  {
    name: 'feedback_responses_by_feedback_filter',
    sql: `SELECT id, asset, client_address, feedback_index, responder, response_uri,
                 response_hash, running_digest, NULL::bigint AS response_count, status, verified_at,
                 tx_signature, block_slot, created_at
          FROM feedback_responses
          WHERE asset = $1 AND client_address = $2 AND feedback_index = $3::bigint
            AND status != 'ORPHANED'
          ORDER BY created_at DESC, id DESC
          LIMIT $4::int OFFSET $5::int`,
    params: ['asset_1', 'client_1', '1', 100, 0],
  },
  {
    name: 'validations_status_pending_filter',
    sql: `SELECT id, asset, validator_address AS validator, requester, nonce, request_uri,
                 request_hash, response, response_uri, response_hash, tag, chain_status,
                 created_at, updated_at AS responded_at, tx_signature
          FROM validations
          WHERE validator_address = $1 AND response IS NULL AND chain_status != 'ORPHANED'
          ORDER BY created_at DESC, id DESC
          LIMIT $2::int OFFSET $3::int`,
    params: ['validator_1', 100, 0],
  },
  {
    name: 'agent_registration_files_list',
    sql: `SELECT m.asset, MAX(m.updated_at) AS latest_updated_at
          FROM metadata m
          INNER JOIN agents a ON a.asset = m.asset
          WHERE m.key LIKE '\\_uri:%' ESCAPE '\\'
            AND m.status != 'ORPHANED'
            AND a.status != 'ORPHANED'
          GROUP BY m.asset
          ORDER BY latest_updated_at DESC NULLS LAST, m.asset DESC
          LIMIT $1::int OFFSET $2::int`,
    params: [100, 0],
  },
  {
    name: 'agent_search_ilike',
    sql: `SELECT asset, owner, agent_uri, nft_name, created_at
          FROM agents
          WHERE status != 'ORPHANED'
            AND (nft_name ILIKE $1 ESCAPE '\\' OR asset ILIKE $1 ESCAPE '\\' OR owner ILIKE $1 ESCAPE '\\')
          ORDER BY created_at DESC
          LIMIT $2::int`,
    params: ['%agent 1%', 20],
  },
  {
    name: 'loader_feedback_page_by_agent_batch_50',
    sql: `WITH requested AS (
            SELECT unnest($1::text[]) AS asset
          ), ranked AS (
            SELECT
              f.id,
              f.asset,
              f.client_address,
              f.feedback_index,
              f.created_at,
              ROW_NUMBER() OVER (
                PARTITION BY f.asset
                ORDER BY created_at DESC, f.id DESC
              ) AS rn
            FROM feedbacks f
            INNER JOIN requested r ON r.asset = f.asset
            WHERE f.status != 'ORPHANED'
          )
          SELECT id, asset, client_address, feedback_index, created_at
          FROM ranked
          WHERE rn > $2::int AND rn <= $3::int
          ORDER BY asset, rn`,
    params: [Array.from({ length: 50 }, (_, i) => `asset_${i + 1}`), 0, 10],
  },
  {
    name: 'loader_responses_page_by_feedback_batch_50',
    sql: `WITH requested AS (
            SELECT
              unnest($1::text[]) AS asset,
              unnest($2::text[]) AS client_address,
              unnest($3::bigint[]) AS feedback_index
          ), ranked AS (
            SELECT
              fr.id,
              fr.asset,
              fr.client_address,
              fr.feedback_index,
              fr.created_at,
              ROW_NUMBER() OVER (
                PARTITION BY fr.asset, fr.client_address, fr.feedback_index
                ORDER BY fr.created_at ASC, fr.id ASC
              ) AS rn
            FROM feedback_responses fr
            INNER JOIN requested r
              ON r.asset = fr.asset
             AND r.client_address = fr.client_address
             AND r.feedback_index = fr.feedback_index
            WHERE fr.status != 'ORPHANED'
          )
          SELECT id, asset, client_address, feedback_index, created_at
          FROM ranked
          WHERE rn > $4::int AND rn <= $5::int
          ORDER BY asset, client_address, feedback_index, rn`,
    params: [
      Array.from({ length: 50 }, (_, i) => `asset_${i + 1}`),
      Array.from({ length: 50 }, (_, i) => `client_${i + 1}`),
      Array.from({ length: 50 }, () => '1'),
      0,
      10,
    ],
  },
  {
    name: 'loader_validations_page_by_agent_batch_50',
    sql: `WITH requested AS (
            SELECT unnest($1::text[]) AS asset
          ), ranked AS (
            SELECT
              v.id,
              v.asset,
              v.created_at,
              ROW_NUMBER() OVER (
                PARTITION BY v.asset
                ORDER BY v.created_at DESC, v.id DESC
              ) AS rn
            FROM validations v
            INNER JOIN requested r ON r.asset = v.asset
            WHERE v.chain_status != 'ORPHANED'
          )
          SELECT id, asset, created_at
          FROM ranked
          WHERE rn > $2::int AND rn <= $3::int
          ORDER BY asset, rn`,
    params: [Array.from({ length: 50 }, (_, i) => `asset_${i + 1}`), 0, 10],
  },
  {
    name: 'global_stats_aggregate',
    sql: `SELECT
            (SELECT COUNT(*)::text FROM agents WHERE status != 'ORPHANED') AS total_agents,
            (SELECT COUNT(*)::text FROM feedbacks WHERE status != 'ORPHANED') AS total_feedback,
            (SELECT COUNT(*)::text FROM validations WHERE chain_status != 'ORPHANED') AS total_validations,
            COALESCE((
              SELECT ARRAY(
                SELECT tag FROM (
                  SELECT tag1 AS tag FROM feedbacks WHERE tag1 != '' AND status != 'ORPHANED'
                  UNION ALL
                  SELECT tag2 AS tag FROM feedbacks WHERE tag2 != '' AND status != 'ORPHANED'
                ) t
                GROUP BY tag
                LIMIT 100
              )
            ), ARRAY[]::text[]) AS tags`,
    params: [],
  },
];

function statsFromDurations(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const avg = sorted.reduce((a, b) => a + b, 0) / sorted.length;
  const p50 = sorted[Math.floor(sorted.length * 0.5)];
  const p95 = sorted[Math.floor(sorted.length * 0.95)];
  const p99 = sorted[Math.floor(sorted.length * 0.99)];
  return { avg_ms: avg, p50_ms: p50, p95_ms: p95, p99_ms: p99, min_ms: sorted[0], max_ms: sorted[sorted.length - 1] };
}

const pool = new Pool({ connectionString: conn });

async function run() {
  const client = await pool.connect();
  try {
    await client.query('SET statement_timeout = 0');
    const results = [];

    for (const test of tests) {
      for (let i = 0; i < warmup; i++) {
        await client.query(test.sql, test.params);
      }

      const durations = [];
      for (let i = 0; i < iterations; i++) {
        const start = performance.now();
        await client.query(test.sql, test.params);
        durations.push(performance.now() - start);
      }

      const stat = statsFromDurations(durations);
      results.push({ name: test.name, ...stat });
      console.log(`${test.name.padEnd(45)} avg=${stat.avg_ms.toFixed(2)}ms p50=${stat.p50_ms.toFixed(2)}ms p95=${stat.p95_ms.toFixed(2)}ms`);
    }

    fs.writeFileSync(outFile, JSON.stringify({ connection: conn, iterations, warmup, results }, null, 2));
  } finally {
    client.release();
    await pool.end();
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
