import { Pool } from 'pg';
import { performance } from 'node:perf_hooks';
import { createApiServer } from '../dist/api/server.js';

const dsn = process.env.SUPABASE_DSN;
if (!dsn) {
  throw new Error('SUPABASE_DSN is required');
}

const localConn =
  dsn.includes('localhost') || dsn.includes('127.0.0.1');
const pool = new Pool(
  localConn
    ? { connectionString: dsn }
    : { connectionString: dsn, ssl: { rejectUnauthorized: process.env.SUPABASE_SSL_VERIFY !== 'false' } }
);

function percentile(sorted, p) {
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor(sorted.length * p)));
  return sorted[idx];
}

function stats(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const avg = sorted.reduce((a, b) => a + b, 0) / sorted.length;
  return {
    avg_ms: Number(avg.toFixed(2)),
    p50_ms: Number(percentile(sorted, 0.5).toFixed(2)),
    p95_ms: Number(percentile(sorted, 0.95).toFixed(2)),
    min_ms: Number(sorted[0].toFixed(2)),
    max_ms: Number(sorted[sorted.length - 1].toFixed(2)),
  };
}

async function gql(url, query, variables = {}) {
  const res = await fetch(`${url}/v2/graphql`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ query, variables }),
  });
  const json = await res.json();
  if (!res.ok || json.errors) {
    throw new Error(`GraphQL error: ${JSON.stringify(json.errors || json)}`);
  }
  return json.data;
}

async function waitGraphql(url, timeoutMs = 10000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      await gql(url, 'query { __typename }');
      return;
    } catch {
      await new Promise((r) => setTimeout(r, 200));
    }
  }
  throw new Error('GraphQL endpoint did not become ready in time');
}

async function runPerf(url, query, variables, iterations = 30, warmup = 5) {
  for (let i = 0; i < warmup; i++) {
    await gql(url, query, variables);
  }
  const durations = [];
  for (let i = 0; i < iterations; i++) {
    const t0 = performance.now();
    await gql(url, query, variables);
    durations.push(performance.now() - t0);
  }
  return stats(durations);
}

async function main() {
  const app = createApiServer({ prisma: null, pool, port: 0 });
  const server = await new Promise((resolve) => {
    const s = app.listen(0, () => resolve(s));
  });

  const address = server.address();
  if (!address || typeof address === 'string') {
    throw new Error('Unable to determine server address');
  }
  const url = `http://127.0.0.1:${address.port}`;

  try {
    await waitGraphql(url);

    const countData = await gql(
      url,
      `query {
        globalStats {
          totalAgents
          totalFeedback
          totalValidations
          totalCollections
        }
      }`
    );

    const sqlCounts = await pool.query(
      `SELECT
         (SELECT COUNT(*)::text FROM agents WHERE status != 'ORPHANED') AS total_agents,
         (SELECT COUNT(*)::text FROM feedbacks WHERE status != 'ORPHANED') AS total_feedback,
         (SELECT COUNT(*)::text FROM validations WHERE chain_status != 'ORPHANED') AS total_validations,
         (SELECT COUNT(*)::text FROM collections WHERE status != 'ORPHANED') AS total_collections`
    );

    const g = countData.globalStats;
    const s = sqlCounts.rows[0];
    if (
      !g
      || g.totalAgents !== s.total_agents
      || g.totalFeedback !== s.total_feedback
      || g.totalValidations !== s.total_validations
      || g.totalCollections !== s.total_collections
    ) {
      throw new Error(`Count mismatch GraphQL vs SQL: gql=${JSON.stringify(g)} sql=${JSON.stringify(s)}`);
    }

    const gqlAgents = await gql(
      url,
      `query {
        agents(first: 20, orderBy: createdAt, orderDirection: desc) {
          solana { assetPubkey }
        }
      }`
    );

    const sqlAgents = await pool.query(
      `SELECT asset
       FROM agents
       WHERE status != 'ORPHANED'
       ORDER BY created_at DESC, asset DESC
       LIMIT 20`
    );

    const gqlAgentAssets = gqlAgents.agents.map((a) => a.solana.assetPubkey);
    const sqlAgentAssets = sqlAgents.rows.map((r) => r.asset);
    if (JSON.stringify(gqlAgentAssets) !== JSON.stringify(sqlAgentAssets)) {
      throw new Error('Agent ordering mismatch between GraphQL and SQL');
    }

    const gqlFeedbacks = await gql(
      url,
      `query {
        feedbacks(first: 20, orderBy: createdAt, orderDirection: desc) {
          agent { solana { assetPubkey } }
          clientAddress
          feedbackIndex
        }
      }`
    );

    const sqlFeedbacks = await pool.query(
      `SELECT asset, client_address, feedback_index::text AS feedback_index
       FROM feedbacks
       WHERE status != 'ORPHANED'
       ORDER BY created_at DESC, asset DESC, id DESC
       LIMIT 20`
    );

    const gqlFeedbackKeys = gqlFeedbacks.feedbacks.map((f) => `${f.agent.solana.assetPubkey}:${f.clientAddress}:${f.feedbackIndex}`);
    const sqlFeedbackKeys = sqlFeedbacks.rows.map((r) => `${r.asset}:${r.client_address}:${r.feedback_index}`);
    if (JSON.stringify(gqlFeedbackKeys) !== JSON.stringify(sqlFeedbackKeys)) {
      throw new Error('Feedback ordering mismatch between GraphQL and SQL');
    }

    const perfResults = {
      globalStats: await runPerf(
        url,
        `query { globalStats { totalAgents totalFeedback totalValidations totalCollections tags } }`,
        {}
      ),
      agentsPage: await runPerf(
        url,
        `query { agents(first: 100, orderBy: createdAt, orderDirection: desc) { id createdAt owner } }`,
        {}
      ),
      feedbacksPage: await runPerf(
        url,
        `query { feedbacks(first: 100, orderBy: createdAt, orderDirection: desc) { id createdAt clientAddress feedbackIndex } }`,
        {}
      ),
      agentSearch: await runPerf(
        url,
        `query($q: String!) { agentSearch(query: $q, first: 20) { id owner } }`,
        { q: 'agent' }
      ),
      registrationFiles: await runPerf(
        url,
        `query { agentRegistrationFiles(first: 100) { name active a2aEndpoint hasOASF } }`,
        {}
      ),
    };

    console.log('COHERENCE_CHECK=PASS');
    console.log(JSON.stringify({ counts: g, perf: perfResults }, null, 2));
  } finally {
    await new Promise((resolve) => server.close(resolve));
    await pool.end();
  }
}

main().catch((err) => {
  console.error('COHERENCE_CHECK=FAIL');
  console.error(err);
  process.exit(1);
});
