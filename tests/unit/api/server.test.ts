import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import type { AddressInfo } from 'net';
import type { Express } from 'express';
import type { Server } from 'http';

vi.mock('../../../src/api/graphql/index.js', () => ({
  createGraphQLHandler: vi.fn(() => ({
    handle: (_req: unknown, res: { status: (code: number) => { json: (body: unknown) => void } }) => {
      res.status(200).json({ data: { ok: true } });
    },
  })),
}));

const originalEnv = process.env;

describe('API Server (GraphQL-only)', () => {
  let createApiServer: typeof import('../../../src/api/server.js').createApiServer;
  let app: Express;
  let server: Server;
  let baseUrl: string;
  let suiteEnv: NodeJS.ProcessEnv;

  beforeAll(async () => {
    vi.resetModules();
    suiteEnv = {
      ...originalEnv,
      API_MODE: 'both',
      ENABLE_GRAPHQL: 'true',
    };
    delete suiteEnv.SUPABASE_URL;
    delete suiteEnv.POSTGREST_URL;
    delete suiteEnv.POSTGREST_TOKEN;
    process.env = suiteEnv;

    ({ createApiServer } = await import('../../../src/api/server.js'));
    app = createApiServer({ pool: {} as any, prisma: null });

    await new Promise<void>((resolve, reject) => {
      server = app.listen(0, '127.0.0.1', () => {
        const addr = server.address() as AddressInfo;
        baseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
      server.on('error', reject);
    });
  });

  afterAll(async () => {
    process.env = originalEnv;
    if (server) {
      await new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      });
    }
  });

  it('returns health status', async () => {
    const res = await fetch(`${baseUrl}/health`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body).toEqual({ status: 'ok' });
  });

  it('returns ready status when the API is ready', async () => {
    const res = await fetch(`${baseUrl}/ready`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body).toEqual({ status: 'ready' });
  });

  it('serves /v2/graphql endpoint', async () => {
    const res = await fetch(`${baseUrl}/v2/graphql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ query: '{ stats { totalAgents } }' }),
    });

    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body).toEqual({ data: { ok: true } });
  });

  it('does not expose REST routes', async () => {
    const res = await fetch(`${baseUrl}/rest/v1/agents`);
    expect(res.status).toBe(410);
  });

  it('requires SUPABASE_KEY when API_MODE=rest is using Supabase REST proxy mode', async () => {
    vi.resetModules();
    const restEnv = {
      ...originalEnv,
      API_MODE: 'rest',
      ENABLE_GRAPHQL: 'false',
      SUPABASE_URL: 'https://proxy-test.supabase.co',
    };
    delete restEnv.SUPABASE_KEY;
    process.env = restEnv;

    const { createApiServer: createRestServer } = await import('../../../src/api/server.js');
    expect(() => createRestServer({ pool: {} as any, prisma: null as any })).toThrow(
      'SUPABASE_URL + SUPABASE_KEY'
    );

    process.env = suiteEnv;
  });

  it('accepts POSTGREST_URL + POSTGREST_TOKEN aliases when API_MODE=rest is using proxy mode', async () => {
    vi.resetModules();
    const restEnv = {
      ...originalEnv,
      API_MODE: 'rest',
      ENABLE_GRAPHQL: 'false',
      POSTGREST_URL: 'https://proxy-test.supabase.co',
      POSTGREST_TOKEN: 'alias-service-role-key',
    };
    delete restEnv.SUPABASE_URL;
    delete restEnv.SUPABASE_KEY;
    process.env = restEnv;

    const { createApiServer: createRestServer } = await import('../../../src/api/server.js');
    expect(() => createRestServer({ pool: {} as any, prisma: null as any })).not.toThrow();

    process.env = suiteEnv;
  });

  it('throws when no API backend is available', () => {
    expect(() => createApiServer({ pool: null as any, prisma: null as any })).toThrow(
      'No API backend available for API_MODE. Provide Prisma (REST), Supabase pool (GraphQL), or set API_MODE explicitly.'
    );
  });
});

describe('API Server readiness gate', () => {
  let createApiServer: typeof import('../../../src/api/server.js').createApiServer;
  let app: Express;
  let server: Server;
  let baseUrl: string;

  beforeAll(async () => {
    vi.resetModules();
    process.env = {
      ...originalEnv,
      API_MODE: 'both',
      ENABLE_GRAPHQL: 'true',
    };

    ({ createApiServer } = await import('../../../src/api/server.js'));
    app = createApiServer({
      pool: {} as any,
      prisma: {} as any,
      isReady: () => false,
    });

    await new Promise<void>((resolve, reject) => {
      server = app.listen(0, '127.0.0.1', () => {
        const addr = server.address() as AddressInfo;
        baseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
      server.on('error', reject);
    });
  });

  afterAll(async () => {
    process.env = originalEnv;
    if (server) {
      await new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      });
    }
  });

  it('keeps /health live while bootstrap is in progress', async () => {
    const res = await fetch(`${baseUrl}/health`);
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual({ status: 'ok' });
  });

  it('exposes /ready as 503 while bootstrap is in progress', async () => {
    const res = await fetch(`${baseUrl}/ready`);
    expect(res.status).toBe(503);
    await expect(res.json()).resolves.toEqual({ status: 'starting' });
  });

  it('returns 503 for business routes while bootstrap is in progress', async () => {
    const restRes = await fetch(`${baseUrl}/rest/v1/agents`);
    expect(restRes.status).toBe(503);
    expect(restRes.headers.get('retry-after')).toBe('5');
    await expect(restRes.json()).resolves.toEqual({
      error: 'Indexer bootstrap in progress. Retry shortly.',
    });

    const graphqlRes = await fetch(`${baseUrl}/v2/graphql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ query: '{ stats { totalAgents } }' }),
    });
    expect(graphqlRes.status).toBe(503);
    expect(graphqlRes.headers.get('retry-after')).toBe('5');
    await expect(graphqlRes.json()).resolves.toEqual({
      error: 'Indexer bootstrap in progress. Retry shortly.',
    });
  });
});

describe('API Server GraphQL rate limiting', () => {
  let createApiServer: typeof import('../../../src/api/server.js').createApiServer;
  let app: Express;
  let server: Server;
  let baseUrl: string;

  beforeAll(async () => {
    vi.resetModules();
    process.env = {
      ...originalEnv,
      API_MODE: 'graphql',
      ENABLE_GRAPHQL: 'true',
      GRAPHQL_RATE_LIMIT_MAX_REQUESTS: '1',
    };

    ({ createApiServer } = await import('../../../src/api/server.js'));
    app = createApiServer({ pool: {} as any, prisma: null });

    await new Promise<void>((resolve, reject) => {
      server = app.listen(0, '127.0.0.1', () => {
        const addr = server.address() as AddressInfo;
        baseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
      server.on('error', reject);
    });
  });

  afterAll(async () => {
    process.env = originalEnv;
    if (server) {
      await new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      });
    }
  });

  it('returns a rate limit message that matches the configured GraphQL limit', async () => {
    const first = await fetch(`${baseUrl}/v2/graphql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ query: '{ stats { totalAgents } }' }),
    });
    expect(first.status).toBe(200);

    const second = await fetch(`${baseUrl}/v2/graphql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ query: '{ stats { totalAgents } }' }),
    });

    expect(second.status).toBe(429);
    await expect(second.json()).resolves.toEqual({
      error: 'GraphQL rate limited. Max 1 requests per minute.',
    });
  });
});

describe('API Server replay-data status filtering', () => {
  let createApiServer: typeof import('../../../src/api/server.js').createApiServer;
  let app: Express;
  let server: Server;
  let baseUrl: string;
  const feedbackFindMany = vi.fn().mockResolvedValue([]);
  const responseFindMany = vi.fn().mockResolvedValue([]);
  const revocationFindMany = vi.fn().mockResolvedValue([]);

  beforeAll(async () => {
    vi.resetModules();
    process.env = {
      ...originalEnv,
      API_MODE: 'rest',
      ENABLE_GRAPHQL: 'false',
    };

    ({ createApiServer } = await import('../../../src/api/server.js'));
    app = createApiServer({
      prisma: {
        feedback: { findMany: feedbackFindMany },
        feedbackResponse: { findMany: responseFindMany },
        revocation: { findMany: revocationFindMany },
      } as any,
      pool: null,
    });

    await new Promise<void>((resolve, reject) => {
      server = app.listen(0, '127.0.0.1', () => {
        const addr = server.address() as AddressInfo;
        baseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
      server.on('error', reject);
    });
  });

  afterAll(async () => {
    process.env = originalEnv;
    if (server) {
      await new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      });
    }
  });

  beforeEach(() => {
    feedbackFindMany.mockClear();
    responseFindMany.mockClear();
    revocationFindMany.mockClear();
  });

  it('excludes ORPHANED feedbacks from replay-data queries', async () => {
    const asset = '11111111111111111111111111111111';
    const res = await fetch(`${baseUrl}/rest/v1/events/${asset}/replay-data?chainType=feedback`);
    expect(res.status).toBe(200);
    expect(feedbackFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          agentId: asset,
          status: { not: 'ORPHANED' },
        }),
      })
    );
  });

  it('excludes ORPHANED responses from replay-data queries', async () => {
    const asset = '11111111111111111111111111111111';
    const res = await fetch(`${baseUrl}/rest/v1/events/${asset}/replay-data?chainType=response`);
    expect(res.status).toBe(200);
    expect(responseFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          status: { not: 'ORPHANED' },
          feedback: {
            agentId: asset,
            status: { not: 'ORPHANED' },
          },
        }),
      })
    );
  });

  it('excludes ORPHANED revocations from replay-data queries', async () => {
    const asset = '11111111111111111111111111111111';
    const res = await fetch(`${baseUrl}/rest/v1/events/${asset}/replay-data?chainType=revoke`);
    expect(res.status).toBe(200);
    expect(revocationFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          agentId: asset,
          status: { not: 'ORPHANED' },
        }),
      })
    );
  });
});

describe('API Server feedback_id ambiguity handling', () => {
  let createApiServer: typeof import('../../../src/api/server.js').createApiServer;
  let app: Express;
  let server: Server;
  let baseUrl: string;
  const feedbackFindMany = vi.fn().mockResolvedValue([
    {
      id: 'row-1',
      agentId: 'asset-a',
      client: 'client-a',
      feedbackIndex: 1n,
      feedbackId: 7n,
      createdAt: new Date('2025-01-01T00:00:00Z'),
      createdSlot: 1n,
      createdTxSignature: '11111111111111111111111111111111',
      status: 'FINALIZED',
      value: '1',
      updatedAt: new Date('2025-01-01T00:00:00Z'),
      classification: null,
      category: null,
      tag1: null,
      tag2: null,
      endpoint: null,
      revoked: false,
      mint: null,
      nftName: null,
      feedbackHash: null,
      runningDigest: null,
      eventOrdinal: 0,
      txIndex: 0,
      blockTime: null,
      agentWallet: null,
    },
  ]);
  const feedbackCount = vi.fn().mockResolvedValue(2);

  beforeAll(async () => {
    vi.resetModules();
    process.env = {
      ...originalEnv,
      API_MODE: 'rest',
      ENABLE_GRAPHQL: 'false',
    };

    ({ createApiServer } = await import('../../../src/api/server.js'));
    app = createApiServer({
      prisma: {
        feedback: {
          findMany: feedbackFindMany,
          count: feedbackCount,
        },
      } as any,
      pool: null,
    });

    await new Promise<void>((resolve, reject) => {
      server = app.listen(0, '127.0.0.1', () => {
        const addr = server.address() as AddressInfo;
        baseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
      server.on('error', reject);
    });
  });

  afterAll(async () => {
    process.env = originalEnv;
    if (server) {
      await new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      });
    }
  });

  beforeEach(() => {
    feedbackFindMany.mockClear();
    feedbackCount.mockClear();
    feedbackCount.mockResolvedValue(2);
  });

  it('rejects ambiguous feedback_id even when the current page has a single row', async () => {
    const res = await fetch(`${baseUrl}/rest/v1/feedbacks?feedback_id=eq.7&limit=1`);

    expect(res.status).toBe(400);
    await expect(res.json()).resolves.toEqual({
      error: 'feedback_id is scoped by agent. Provide asset filter when ambiguous.',
    });
    expect(feedbackCount).toHaveBeenCalledTimes(1);
  });
});
