import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import type { AddressInfo } from 'node:net';
import type { Server } from 'node:http';
import pg from 'pg';

const pgHost = process.env.COLLECTION_STATS_TEST_PGHOST;
const describePostgres = pgHost ? describe : describe.skip;

describePostgres('pooled collection stats on PostgreSQL', () => {
  let client: pg.Client;
  let server: Server;
  let baseUrl: string;
  const originalEnv = process.env;

  beforeAll(async () => {
    process.env = {
      ...originalEnv,
      API_MODE: 'rest',
      ENABLE_GRAPHQL: 'false',
      SUPABASE_URL: 'http://127.0.0.1:9',
      SUPABASE_KEY: 'collection-stats-postgres-test',
    };
    vi.resetModules();

    client = new pg.Client({
      host: pgHost,
      port: Number(process.env.COLLECTION_STATS_TEST_PGPORT ?? 5432),
      user: process.env.COLLECTION_STATS_TEST_PGUSER ?? 'postgres',
      database: process.env.COLLECTION_STATS_TEST_PGDATABASE ?? 'postgres',
    });
    await client.connect();
    await client.query('BEGIN');
    await client.query(`
      CREATE TEMP TABLE collections (
        collection text PRIMARY KEY, registry_type text, authority text, created_at timestamptz, status text
      );
      CREATE TEMP TABLE agents (asset text PRIMARY KEY, collection text, status text);
      CREATE TEMP TABLE feedbacks (id bigint PRIMARY KEY, asset text, score smallint, status text);
      INSERT INTO collections VALUES
        ('active', 'BASE', 'authority', NOW(), 'FINALIZED'),
        ('empty', 'USER', NULL, NOW() - INTERVAL '1 second', 'FINALIZED'),
        ('orphaned', 'BASE', NULL, NOW() - INTERVAL '2 seconds', 'ORPHANED');
      INSERT INTO agents VALUES
        ('active-agent', 'active', 'FINALIZED'),
        ('orphan-agent', 'active', 'ORPHANED');
      INSERT INTO feedbacks VALUES
        (1, 'active-agent', 80, 'FINALIZED'),
        (2, 'active-agent', 20, 'ORPHANED'),
        (3, 'orphan-agent', 40, 'FINALIZED');
    `);

    const { createApiServer } = await import('../../../src/api/server.js');
    const app = createApiServer({ pool: { query: client.query.bind(client) } as pg.Pool, prisma: null });
    await new Promise<void>((resolve, reject) => {
      server = app.listen(0, '127.0.0.1', () => {
        baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
        resolve();
      });
      server.on('error', reject);
    });
  });

  afterAll(async () => {
    if (server?.listening) {
      await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    }
    if (client) {
      await client.query('ROLLBACK');
      await client.end();
    }
    process.env = originalEnv;
  });

  it('returns bigint counts as numbers while preserving filters and empty registries', async () => {
    const request = async (query: string) => {
      const response = await fetch(`${baseUrl}/rest/v1/collection_stats?${query}`);
      expect(response.status).toBe(200);
      return response.json() as Promise<Array<Record<string, unknown>>>;
    };

    const [active] = await request('collection=eq.active');
    expect(active).toEqual(expect.objectContaining({
      collection: 'active', agent_count: 1, total_feedbacks: 1,
    }));
    expect(active.avg_score).toBe(80);

    const [activeWithOrphans] = await request('collection=eq.active&includeOrphaned=true');
    expect(activeWithOrphans).toEqual(expect.objectContaining({
      collection: 'active', agent_count: 2, total_feedbacks: 3,
    }));
    expect(activeWithOrphans.avg_score).toBeCloseTo(140 / 3);
    await expect(request('collection=eq.empty')).resolves.toEqual([expect.objectContaining({
      collection: 'empty', agent_count: 0, total_feedbacks: 0, avg_score: null,
    })]);
    await expect(request('collection=eq.missing')).resolves.toEqual([]);

    const allActive = await request('order=agent_count.desc');
    expect(allActive.map(({ collection }) => collection)).toEqual(['active', 'empty']);
    expect(allActive.every(({ agent_count, total_feedbacks }) =>
      typeof agent_count === 'number' && typeof total_feedbacks === 'number')).toBe(true);

    const allWithOrphans = await request('includeOrphaned=true&order=agent_count.desc');
    expect(allWithOrphans.map(({ collection }) => collection)).toEqual(['active', 'empty', 'orphaned']);
  });
});
