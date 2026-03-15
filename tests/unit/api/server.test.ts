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
    if (server?.listening) {
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
    expect(body).toEqual({
      status: 'ready',
      phase: 'catching_up',
      state: null,
    });
  });

  it('reports /ready as starting when the pool readiness probe fails', async () => {
    vi.resetModules();
    process.env = {
      ...suiteEnv,
      API_MODE: 'graphql',
      ENABLE_GRAPHQL: 'true',
    };

    ({ createApiServer } = await import('../../../src/api/server.js'));
    const failingPool = {
      query: vi.fn().mockRejectedValue(new Error('pool down')),
    } as any;
    const failingApp = createApiServer({ pool: failingPool, prisma: null });

    let failingServer: Server | undefined;
    let failingBaseUrl = '';
    await new Promise<void>((resolve, reject) => {
      failingServer = failingApp.listen(0, '127.0.0.1', () => {
        const addr = failingServer!.address() as AddressInfo;
        failingBaseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
      failingServer.on('error', reject);
    });

    const res = await fetch(`${failingBaseUrl}/ready`);
    expect(res.status).toBe(503);
    await expect(res.json()).resolves.toEqual({
      status: 'starting',
      phase: 'bootstrapping',
      state: null,
    });

    await new Promise<void>((resolve, reject) => {
      failingServer!.close((err) => (err ? reject(err) : resolve()));
    });

    process.env = suiteEnv;
  });

  it('returns 503 for business routes when the pool readiness probe fails', async () => {
    vi.resetModules();
    process.env = {
      ...suiteEnv,
      API_MODE: 'graphql',
      ENABLE_GRAPHQL: 'true',
    };

    ({ createApiServer } = await import('../../../src/api/server.js'));
    const failingPool = {
      query: vi.fn().mockRejectedValue(new Error('pool down')),
    } as any;
    const failingApp = createApiServer({ pool: failingPool, prisma: null });

    let failingServer: Server | undefined;
    let failingBaseUrl = '';
    await new Promise<void>((resolve, reject) => {
      failingServer = failingApp.listen(0, '127.0.0.1', () => {
        const addr = failingServer!.address() as AddressInfo;
        failingBaseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
      failingServer.on('error', reject);
    });

    const res = await fetch(`${failingBaseUrl}/v2/graphql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ query: '{ stats { totalAgents } }' }),
    });

    expect(res.status).toBe(503);
    await expect(res.json()).resolves.toEqual({
      error: 'Indexer bootstrap in progress. Retry shortly.',
    });

    await new Promise<void>((resolve, reject) => {
      failingServer!.close((err) => (err ? reject(err) : resolve()));
    });

    process.env = suiteEnv;
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

  it('serves /graphql alias endpoint', async () => {
    const res = await fetch(`${baseUrl}/graphql`, {
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
      'GraphQL mode requires Supabase PostgreSQL pool (DB_MODE=supabase)'
    );
  });

  it('fails fast when GraphQL handler creation throws', async () => {
    const graphqlModule = await import('../../../src/api/graphql/index.js');
    vi.mocked(graphqlModule.createGraphQLHandler).mockImplementationOnce(() => {
      throw new Error('GraphQL boot failed');
    });

    expect(() => createApiServer({ pool: {} as any, prisma: null })).toThrow('GraphQL boot failed');
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
    if (server?.listening) {
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
    await expect(res.json()).resolves.toEqual({
      status: 'starting',
      phase: 'bootstrapping',
      state: null,
    });
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

describe('API Server REST proxy readiness', () => {
  let createApiServer: typeof import('../../../src/api/server.js').createApiServer;
  let server: Server;
  let baseUrl: string;

  async function boot(checkRestProxyReady: () => Promise<boolean>) {
    vi.resetModules();
    process.env = {
      ...originalEnv,
      API_MODE: 'both',
      ENABLE_GRAPHQL: 'true',
      SUPABASE_URL: 'https://proxy-test.supabase.co',
      SUPABASE_KEY: 'service-role-key',
    };

    ({ createApiServer } = await import('../../../src/api/server.js'));
    const app = createApiServer({
      pool: {} as any,
      prisma: null,
      checkRestProxyReady,
    });

    await new Promise<void>((resolve, reject) => {
      server = app.listen(0, '127.0.0.1', () => {
        const addr = server.address() as AddressInfo;
        baseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
      server.on('error', reject);
    });
  }

  afterAll(async () => {
    process.env = originalEnv;
    if (server) {
      await new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      });
    }
  });

  it('reports /ready as starting when the REST proxy backend is unavailable', async () => {
    await boot(async () => false);
    const res = await fetch(`${baseUrl}/ready`);
    expect(res.status).toBe(503);
    await expect(res.json()).resolves.toEqual({
      status: 'starting',
      phase: 'bootstrapping',
      state: null,
    });
    await new Promise<void>((resolve, reject) => {
      server.close((err) => (err ? reject(err) : resolve()));
    });
    server = undefined as unknown as Server;
  });

  it('reports /ready as ready when the REST proxy backend is available', async () => {
    await boot(async () => true);
    const res = await fetch(`${baseUrl}/ready`);
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual({
      status: 'ready',
      phase: 'catching_up',
      state: null,
    });
  });

  it('coalesces concurrent /ready proxy probes into a single upstream probe cycle', async () => {
    vi.resetModules();
    process.env = {
      ...originalEnv,
      API_MODE: 'both',
      ENABLE_GRAPHQL: 'true',
      SUPABASE_URL: 'https://proxy-test.supabase.co',
      SUPABASE_KEY: 'service-role-key',
    };

    const nativeFetch = globalThis.fetch;
    let agentsCalls = 0;
    let leaderboardCalls = 0;
    const fetchSpy = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.startsWith('http://127.0.0.1:')) {
        return await nativeFetch(input, init);
      }
      if (url === 'https://proxy-test.supabase.co/rest/v1/agents?select=asset&limit=1') {
        agentsCalls += 1;
        await new Promise((resolve) => setTimeout(resolve, 25));
        return new Response(JSON.stringify([{ asset: 'ok' }]), { status: 200 });
      }
      if (url === 'https://proxy-test.supabase.co/rest/v1/leaderboard?select=asset&limit=1') {
        leaderboardCalls += 1;
        await new Promise((resolve) => setTimeout(resolve, 25));
        return new Response(JSON.stringify([{ asset: 'ok' }]), { status: 200 });
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });
    vi.stubGlobal('fetch', fetchSpy as typeof fetch);

    ({ createApiServer } = await import('../../../src/api/server.js'));
    const app = createApiServer({
      pool: {} as any,
      prisma: null,
    });

    await new Promise<void>((resolve, reject) => {
      server = app.listen(0, '127.0.0.1', () => {
        const addr = server.address() as AddressInfo;
        baseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
      server.on('error', reject);
    });

    const [first, second] = await Promise.all([
      nativeFetch(`${baseUrl}/ready`),
      nativeFetch(`${baseUrl}/ready`),
    ]);

    expect(first.status).toBe(200);
    expect(second.status).toBe(200);
    expect(agentsCalls).toBe(1);
    expect(leaderboardCalls).toBe(1);

    vi.unstubAllGlobals();
    await new Promise<void>((resolve, reject) => {
      server.close((err) => (err ? reject(err) : resolve()));
    });
    server = undefined as unknown as Server;
  });

  it('reports /ready as starting when leaderboard proxy probe fails', async () => {
    vi.resetModules();
    process.env = {
      ...originalEnv,
      API_MODE: 'both',
      ENABLE_GRAPHQL: 'true',
      SUPABASE_URL: 'https://proxy-test.supabase.co',
      SUPABASE_KEY: 'service-role-key',
    };

    const nativeFetch = globalThis.fetch;
    const fetchSpy = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.startsWith('http://127.0.0.1:')) {
        return await nativeFetch(input, init);
      }
      if (url === 'https://proxy-test.supabase.co/rest/v1/agents?select=asset&limit=1') {
        return new Response(JSON.stringify([{ asset: 'ok' }]), { status: 200 });
      }
      if (url === 'https://proxy-test.supabase.co/rest/v1/leaderboard?select=asset&limit=1') {
        return new Response('forbidden', { status: 403 });
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });
    vi.stubGlobal('fetch', fetchSpy as typeof fetch);

    ({ createApiServer } = await import('../../../src/api/server.js'));
    const app = createApiServer({
      pool: {} as any,
      prisma: null,
    });

    await new Promise<void>((resolve, reject) => {
      server = app.listen(0, '127.0.0.1', () => {
        const addr = server.address() as AddressInfo;
        baseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
      server.on('error', reject);
    });

    const res = await nativeFetch(`${baseUrl}/ready`);
    expect(res.status).toBe(503);
    await expect(res.json()).resolves.toEqual({
      status: 'starting',
      phase: 'bootstrapping',
      state: null,
    });
    expect(fetchSpy).toHaveBeenCalledWith(
      'https://proxy-test.supabase.co/rest/v1/leaderboard?select=asset&limit=1',
      expect.objectContaining({ method: 'GET' }),
    );

    vi.unstubAllGlobals();
    await new Promise<void>((resolve, reject) => {
      server.close((err) => (err ? reject(err) : resolve()));
    });
    server = undefined as unknown as Server;
  });

  it('includes runtime state details in /ready when a real Prisma backend is available', async () => {
    vi.resetModules();
    process.env = {
      ...originalEnv,
      API_MODE: 'both',
      ENABLE_GRAPHQL: 'true',
    };

    const prisma = {
      indexerState: {
        findUnique: vi.fn().mockResolvedValue({ lastSlot: 123n, lastTxIndex: 7 }),
      },
      agent: {
        count: vi.fn()
          .mockResolvedValueOnce(11)
          .mockResolvedValueOnce(2)
          .mockResolvedValueOnce(1),
      },
      feedback: {
        count: vi.fn()
          .mockResolvedValueOnce(22)
          .mockResolvedValueOnce(3)
          .mockResolvedValueOnce(1),
      },
      feedbackResponse: {
        count: vi.fn()
          .mockResolvedValueOnce(33)
          .mockResolvedValueOnce(4)
          .mockResolvedValueOnce(2),
      },
      revocation: {
        count: vi.fn()
          .mockResolvedValueOnce(44)
          .mockResolvedValueOnce(5)
          .mockResolvedValueOnce(0),
      },
      orphanFeedback: {
        count: vi.fn().mockResolvedValue(4),
      },
      orphanResponse: {
        count: vi.fn().mockResolvedValue(5),
      },
      agentMetadata: {
        count: vi.fn()
          .mockResolvedValueOnce(6)
          .mockResolvedValueOnce(2),
      },
      registry: {
        count: vi.fn()
          .mockResolvedValueOnce(7)
          .mockResolvedValueOnce(1),
      },
      validation: {
        count: vi.fn()
          .mockResolvedValueOnce(8)
          .mockResolvedValueOnce(3),
      },
      collection: { count: vi.fn().mockResolvedValue(6) },
    } as any;

    ({ createApiServer } = await import('../../../src/api/server.js'));
    const app = createApiServer({ prisma, pool: null as any });

    await new Promise<void>((resolve, reject) => {
      server = app.listen(0, '127.0.0.1', () => {
        const addr = server.address() as AddressInfo;
        baseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
      server.on('error', reject);
    });

    const res = await fetch(`${baseUrl}/ready`);
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual({
      status: 'ready',
      phase: 'catching_up',
      state: {
        mainSlot: 123,
        mainTxIndex: 7,
        counts: {
          agents: 11,
          feedbacks: 22,
          responses: 33,
          revocations: 44,
          collections: 6,
        },
        pending: {
          agents: 2,
          feedbacks: 3,
          responses: 4,
          revocations: 5,
          metadata: 6,
          registries: 7,
          validations: 8,
        },
        orphans: {
          agents: 1,
          feedbacks: 5,
          responses: 7,
          revocations: 0,
          metadata: 2,
          registries: 1,
          validations: 3,
        },
      },
    });

    await new Promise<void>((resolve, reject) => {
      server.close((err) => (err ? reject(err) : resolve()));
    });
    server = undefined as unknown as Server;
  });

  it('reports /ready as starting when runtime state details cannot be loaded from Prisma', async () => {
    vi.resetModules();
    process.env = {
      ...originalEnv,
      API_MODE: 'both',
      ENABLE_GRAPHQL: 'true',
    };

    const prisma = {
      indexerState: {
        findUnique: vi.fn().mockRejectedValue(new Error('state query failed')),
      },
    } as any;

    ({ createApiServer } = await import('../../../src/api/server.js'));
    const app = createApiServer({ prisma, pool: null as any });

    await new Promise<void>((resolve, reject) => {
      server = app.listen(0, '127.0.0.1', () => {
        const addr = server.address() as AddressInfo;
        baseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
      server.on('error', reject);
    });

    const res = await fetch(`${baseUrl}/ready`);
    expect(res.status).toBe(503);
    await expect(res.json()).resolves.toEqual({
      status: 'starting',
      phase: 'bootstrapping',
      state: null,
    });

    await new Promise<void>((resolve, reject) => {
      server.close((err) => (err ? reject(err) : resolve()));
    });
    server = undefined as unknown as Server;
  });

  it('keeps /ready in catching_up when runtime backlog is empty but the poller has not reached an idle frontier yet', async () => {
    vi.resetModules();
    process.env = {
      ...originalEnv,
      API_MODE: 'both',
      ENABLE_GRAPHQL: 'true',
    };

    const prisma = {
      indexerState: {
        findUnique: vi.fn().mockResolvedValue({ lastSlot: 123n, lastTxIndex: 7 }),
      },
      agent: {
        count: vi.fn()
          .mockResolvedValueOnce(11)
          .mockResolvedValueOnce(0)
          .mockResolvedValueOnce(0),
      },
      feedback: {
        count: vi.fn()
          .mockResolvedValueOnce(22)
          .mockResolvedValueOnce(0)
          .mockResolvedValueOnce(0),
      },
      feedbackResponse: {
        count: vi.fn()
          .mockResolvedValueOnce(33)
          .mockResolvedValueOnce(0)
          .mockResolvedValueOnce(0),
      },
      revocation: {
        count: vi.fn()
          .mockResolvedValueOnce(44)
          .mockResolvedValueOnce(0)
          .mockResolvedValueOnce(0),
      },
      orphanFeedback: { count: vi.fn().mockResolvedValue(0) },
      orphanResponse: { count: vi.fn().mockResolvedValue(0) },
      agentMetadata: {
        count: vi.fn()
          .mockResolvedValueOnce(0)
          .mockResolvedValueOnce(0),
      },
      registry: {
        count: vi.fn()
          .mockResolvedValueOnce(0)
          .mockResolvedValueOnce(0),
      },
      validation: {
        count: vi.fn()
          .mockResolvedValueOnce(0)
          .mockResolvedValueOnce(0),
      },
      collection: { count: vi.fn().mockResolvedValue(6) },
    } as any;

    ({ createApiServer } = await import('../../../src/api/server.js'));
    const app = createApiServer({ prisma, pool: null as any, isCaughtUp: () => false });

    await new Promise<void>((resolve, reject) => {
      server = app.listen(0, '127.0.0.1', () => {
        const addr = server.address() as AddressInfo;
        baseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
      server.on('error', reject);
    });

    const res = await fetch(`${baseUrl}/ready`);
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual(expect.objectContaining({
      status: 'ready',
      phase: 'catching_up',
    }));

    await new Promise<void>((resolve, reject) => {
      server.close((err) => (err ? reject(err) : resolve()));
    });
    server = undefined as unknown as Server;
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

  it('does not apply the global REST limiter to GraphQL routes', async () => {
    if (server) {
      await new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      });
    }
    vi.resetModules();
    process.env = {
      ...originalEnv,
      API_MODE: 'both',
      ENABLE_GRAPHQL: 'true',
      RATE_LIMIT_MAX_REQUESTS: '1',
      GRAPHQL_RATE_LIMIT_MAX_REQUESTS: '2',
    };

    ({ createApiServer } = await import('../../../src/api/server.js'));
    app = createApiServer({ pool: {} as any, prisma: {
      agent: { findMany: vi.fn().mockResolvedValue([]) },
    } as any });

    await new Promise<void>((resolve, reject) => {
      server = app.listen(0, '127.0.0.1', () => {
        const addr = server.address() as AddressInfo;
        baseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
      server.on('error', reject);
    });

    const rest = await fetch(`${baseUrl}/rest/v1/agents`);
    expect(rest.status).toBe(200);

    const gql = await fetch(`${baseUrl}/v2/graphql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ query: '{ stats { totalAgents } }' }),
    });
    expect(gql.status).toBe(200);

    await new Promise<void>((resolve, reject) => {
      server.close((err) => (err ? reject(err) : resolve()));
    });
    server = undefined as unknown as Server;
  });
});

describe('API Server REST agents payload parity', () => {
  let createApiServer: typeof import('../../../src/api/server.js').createApiServer;
  let app: Express;
  let server: Server;
  let baseUrl: string;

  beforeAll(async () => {
    vi.resetModules();
    process.env = {
      ...originalEnv,
      API_MODE: 'rest',
      ENABLE_GRAPHQL: 'false',
    };

    ({ createApiServer } = await import('../../../src/api/server.js'));
    app = createApiServer({
      pool: null as any,
      prisma: {
        agent: {
          findMany: vi.fn(async () => [
            {
              id: 'Agent1111111111111111111111111111111111111',
              owner: 'Owner1111111111111111111111111111111111111',
              creator: 'Creator11111111111111111111111111111111111',
              uri: 'https://example.com/agent.json',
              wallet: null,
              collection: 'Collection11111111111111111111111111111111',
              collectionPointer: 'c1:bafy...',
              colLocked: false,
              parentAsset: null,
              parentCreator: null,
              parentLocked: false,
              nftName: 'Agent One',
              atomEnabled: true,
              trustTier: 1,
              qualityScore: 80,
              confidence: 90,
              riskScore: 5,
              diversityRatio: 12,
              feedbackCount: 3,
              rawAvgScore: 88,
              agentId: 7n,
              status: 'FINALIZED',
              verifiedAt: new Date('2026-03-09T20:00:00.000Z'),
              verifiedSlot: 447346482n,
              createdAt: new Date('2026-03-09T19:00:00.000Z'),
              updatedAt: new Date('2026-03-09T19:10:00.000Z'),
              createdTxSignature: 'Sig111111111111111111111111111111111111111111111111111',
              createdSlot: 447346400n,
              txIndex: 3,
              eventOrdinal: 1,
            },
          ]),
        },
      } as any,
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

  it('includes chain reference fields exposed by the hosted devnet API', async () => {
    const res = await fetch(`${baseUrl}/rest/v1/agents?limit=1`);
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual([
      expect.objectContaining({
        asset: 'Agent1111111111111111111111111111111111111',
        agent_id: '7',
        block_slot: 447346400,
        tx_index: 3,
        event_ordinal: 1,
        tx_signature: 'Sig111111111111111111111111111111111111111111111111111',
        verified_slot: 447346482,
      }),
    ]);
  });
});

describe('API Server replay-data status filtering', () => {
  let createApiServer: typeof import('../../../src/api/server.js').createApiServer;
  let app: Express;
  let server: Server;
  let baseUrl: string;
  const feedbackFindMany = vi.fn().mockResolvedValue([]);
  const orphanFeedbackFindMany = vi.fn().mockResolvedValue([]);
  const responseFindMany = vi.fn().mockResolvedValue([]);
  const orphanResponseFindMany = vi.fn().mockResolvedValue([]);
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
        orphanFeedback: { findMany: orphanFeedbackFindMany },
        feedbackResponse: { findMany: responseFindMany },
        orphanResponse: { findMany: orphanResponseFindMany },
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
    orphanFeedbackFindMany.mockClear();
    responseFindMany.mockClear();
    orphanResponseFindMany.mockClear();
    revocationFindMany.mockClear();
  });

  it('includes orphan feedback staging when serving feedback replay-data locally', async () => {
    const asset = '11111111111111111111111111111111';
    const res = await fetch(`${baseUrl}/rest/v1/events/${asset}/replay-data?chainType=feedback`);
    expect(res.status).toBe(200);
    expect(feedbackFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          agentId: asset,
        }),
      })
    );
    expect(orphanFeedbackFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          agentId: asset,
        }),
      })
    );
  });

  it('includes orphan response staging when serving response replay-data locally', async () => {
    const asset = '11111111111111111111111111111111';
    const res = await fetch(`${baseUrl}/rest/v1/events/${asset}/replay-data?chainType=response`);
    expect(res.status).toBe(200);
    expect(responseFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          feedback: {
            agentId: asset,
          },
        }),
      })
    );
    expect(orphanResponseFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          agentId: asset,
        }),
      })
    );
  });

  it('does not synthesize response feedback_hash from the parent feedback when sealHash is null', async () => {
    const asset = '11111111111111111111111111111111';
    responseFindMany.mockResolvedValueOnce([{
      id: 'response-row-1',
      responder: 'Responder11111111111111111111111111111111',
      responseHash: Buffer.from('aa'.repeat(32), 'hex'),
      sealHash: null,
      runningDigest: Buffer.from('bb'.repeat(32), 'hex'),
      responseCount: 0n,
      slot: 123n,
      txIndex: 1,
      eventOrdinal: 0,
      txSignature: 'Sig111111111111111111111111111111111111111111111111111111',
      feedback: {
        agentId: asset,
        client: 'Client111111111111111111111111111111111',
        feedbackIndex: 0n,
        feedbackHash: Buffer.from('cc'.repeat(32), 'hex'),
      },
    }]);

    const res = await fetch(`${baseUrl}/rest/v1/events/${asset}/replay-data?chainType=response`);
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual({
      events: [{
        asset,
        client: 'Client111111111111111111111111111111111',
        feedback_index: '0',
        responder: 'Responder11111111111111111111111111111111',
        response_hash: 'aa'.repeat(32),
        feedback_hash: null,
        slot: 123,
        running_digest: 'bb'.repeat(32),
        response_count: 0,
      }],
      hasMore: false,
      nextFromCount: 1,
    });
  });

  it('includes canonical orphan revocations when serving revoke replay-data locally', async () => {
    const asset = '11111111111111111111111111111111';
    const res = await fetch(`${baseUrl}/rest/v1/events/${asset}/replay-data?chainType=revoke`);
    expect(res.status).toBe(200);
    expect(revocationFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          agentId: asset,
        }),
      })
    );
  });
});

describe('API Server verification stats staging parity', () => {
  let createApiServer: typeof import('../../../src/api/server.js').createApiServer;
  let app: Express;
  let server: Server;
  let baseUrl: string;

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
        agent: { groupBy: vi.fn().mockResolvedValue([]) },
        feedback: { groupBy: vi.fn().mockResolvedValue([{ status: 'FINALIZED', _count: { _all: 10 } }]) },
        registry: { groupBy: vi.fn().mockResolvedValue([]) },
        agentMetadata: { groupBy: vi.fn().mockResolvedValue([]) },
        validation: { groupBy: vi.fn().mockResolvedValue([{ chainStatus: 'ORPHANED', _count: { _all: 4 } }]) },
        feedbackResponse: { groupBy: vi.fn().mockResolvedValue([{ status: 'FINALIZED', _count: { _all: 4 } }]) },
        revocation: { groupBy: vi.fn().mockResolvedValue([{ status: 'ORPHANED', _count: { _all: 2 } }]) },
        orphanFeedback: { count: vi.fn().mockResolvedValue(3) },
        orphanResponse: { count: vi.fn().mockResolvedValue(5) },
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

  it('includes orphan staging tables in local verification stats', async () => {
    const res = await fetch(`${baseUrl}/rest/v1/stats/verification`);
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual({
      agents: { PENDING: 0, FINALIZED: 0, ORPHANED: 0 },
      feedbacks: { PENDING: 0, FINALIZED: 10, ORPHANED: 3 },
      registries: { PENDING: 0, FINALIZED: 0, ORPHANED: 0 },
      metadata: { PENDING: 0, FINALIZED: 0, ORPHANED: 0 },
      validations: { PENDING: 0, FINALIZED: 0, ORPHANED: 4 },
      feedback_responses: { PENDING: 0, FINALIZED: 4, ORPHANED: 5 },
      revocations: { PENDING: 0, FINALIZED: 0, ORPHANED: 2 },
    });
  });
});

describe('API Server local reputation/rpc parity', () => {
  let createApiServer: typeof import('../../../src/api/server.js').createApiServer;
  let app: Express;
  let server: Server;
  let baseUrl: string;

  const agentFindFirst = vi.fn().mockResolvedValue({
    id: 'Agent1111111111111111111111111111111111111',
    owner: 'Owner1111111111111111111111111111111111111',
    collection: 'Collection11111111111111111111111111111111',
    nftName: 'Agent One',
    uri: 'https://example.com/agent.json',
    feedbackCount: 3,
    rawAvgScore: 88,
  });
  const agentFindMany = vi.fn().mockResolvedValue([
    {
      id: 'Agent1111111111111111111111111111111111111',
      owner: 'Owner1111111111111111111111111111111111111',
      creator: 'Creator11111111111111111111111111111111111',
      uri: 'https://example.com/agent.json',
      wallet: null,
      collection: 'Collection11111111111111111111111111111111',
      collectionPointer: 'c1:bafy...',
      colLocked: false,
      parentAsset: null,
      parentCreator: null,
      parentLocked: false,
      nftName: 'Agent One',
      atomEnabled: true,
      trustTier: 3,
      qualityScore: 80,
      confidence: 90,
      riskScore: 5,
      diversityRatio: 12,
      feedbackCount: 3,
      rawAvgScore: 88,
      agentId: 7n,
      status: 'FINALIZED',
      verifiedAt: new Date('2026-03-09T20:00:00.000Z'),
      verifiedSlot: 447346482n,
      createdAt: new Date('2026-03-09T19:00:00.000Z'),
      updatedAt: new Date('2026-03-09T19:10:00.000Z'),
      createdTxSignature: 'Sig111111111111111111111111111111111111111111111111111',
      createdSlot: 447346400n,
      txIndex: 3,
      eventOrdinal: 1,
    },
  ]);
  const feedbackAggregate = vi.fn().mockResolvedValue({
    _count: { _all: 3 },
    _avg: { score: 82.5 },
  });
  const agentCount = vi.fn().mockResolvedValue(1);
  const feedbackCount = vi.fn()
    .mockResolvedValueOnce(2)
    .mockResolvedValueOnce(1);
  const feedbackGroupBy = vi.fn()
    .mockResolvedValueOnce([
      { agentId: 'Agent1111111111111111111111111111111111111', _count: 3, _avg: { score: 82.5 } },
    ])
    .mockResolvedValueOnce([
      { agentId: 'Agent1111111111111111111111111111111111111', _count: 2 },
    ])
    .mockResolvedValueOnce([
      { agentId: 'Agent1111111111111111111111111111111111111', _count: 1 },
    ]);
  const validationCount = vi.fn().mockResolvedValue(4);
  const validationGroupBy = vi.fn().mockResolvedValue([
    { agentId: 'Agent1111111111111111111111111111111111111', _count: 4 },
  ]);
  const collectionFindUnique = vi.fn().mockResolvedValue({
    col: 'c1:bafybeigdyrzt4x7n3z6l6zjptk5f5t5b4v5l5m5n5p5q5r5s5t5u5v5w5x',
    creator: 'Creator11111111111111111111111111111111111',
  });
  const registryFindFirst = vi.fn().mockResolvedValue({
    registryType: 'USER',
    authority: 'Authority111111111111111111111111111111111',
  });
  const agentDigestCacheFindMany = vi.fn().mockResolvedValue([]);

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
        agent: {
          findFirst: agentFindFirst,
          findMany: agentFindMany,
          count: agentCount,
        },
        feedback: {
          aggregate: feedbackAggregate,
          count: feedbackCount,
          groupBy: feedbackGroupBy,
        },
        validation: {
          count: validationCount,
          groupBy: validationGroupBy,
        },
        registry: {
          findFirst: registryFindFirst,
        },
        agentDigestCache: {
          findMany: agentDigestCacheFindMany,
        },
        collection: {
          findUnique: collectionFindUnique,
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
    agentFindFirst.mockClear();
    agentFindMany.mockClear();
    agentCount.mockClear();
    feedbackAggregate.mockClear();
    feedbackCount.mockClear();
    feedbackCount.mockResolvedValueOnce(2).mockResolvedValueOnce(1);
    feedbackAggregate.mockResolvedValue({
      _count: { _all: 3 },
      _avg: { score: 82.5 },
    });
    feedbackGroupBy.mockClear();
    feedbackGroupBy
      .mockResolvedValueOnce([
        { agentId: 'Agent1111111111111111111111111111111111111', _count: 3, _avg: { score: 82.5 } },
      ])
      .mockResolvedValueOnce([
        { agentId: 'Agent1111111111111111111111111111111111111', _count: 2 },
      ])
      .mockResolvedValueOnce([
        { agentId: 'Agent1111111111111111111111111111111111111', _count: 1 },
      ]);
    validationCount.mockClear();
    validationGroupBy.mockClear();
    collectionFindUnique.mockClear();
    registryFindFirst.mockClear();
    agentDigestCacheFindMany.mockClear();
    agentDigestCacheFindMany.mockResolvedValue([]);
  });

  it('serves local /agents with cumulative digest feedback_count and normalized nft_name', async () => {
    agentFindMany.mockResolvedValueOnce([
      {
        id: 'Agent1111111111111111111111111111111111111',
        owner: 'Owner111111111111111111111111111111111111',
        creator: 'Creator11111111111111111111111111111111111',
        uri: 'https://example.com/agent-111.json',
        wallet: 'Wallet11111111111111111111111111111111111',
        collection: 'Collection11111111111111111111111111111111',
        collectionPointer: 'c1:bafybeigdyrzt4x7n3z6l6zjptk5f5t5b4v5l5m5n5p5q5r5s5t5u5v5w5x',
        colLocked: false,
        parentAsset: null,
        parentCreator: null,
        parentLocked: false,
        nftName: '',
        atomEnabled: true,
        trustTier: 2,
        qualityScore: 8000,
        confidence: 9000,
        riskScore: 10,
        diversityRatio: 20,
        feedbackCount: 2,
        rawAvgScore: 58,
        agentId: 42n,
        status: 'FINALIZED',
        verifiedAt: new Date('2026-03-15T00:00:00.000Z'),
        verifiedSlot: 123n,
        createdAt: new Date('2026-03-15T00:00:00.000Z'),
        updatedAt: new Date('2026-03-15T00:00:00.000Z'),
        createdTxSignature: 'Sig111111111111111111111111111111111111111111111111',
        createdSlot: 123n,
        txIndex: 7,
        eventOrdinal: 0,
      },
    ]);
    agentDigestCacheFindMany.mockResolvedValueOnce([
      { agentId: 'Agent1111111111111111111111111111111111111', feedbackCount: 5n },
    ]);

    const res = await fetch(`${baseUrl}/rest/v1/agents?limit=1`);
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual([
      expect.objectContaining({
        asset: 'Agent1111111111111111111111111111111111111',
        agent_id: '42',
        feedback_count: 5,
        nft_name: null,
      }),
    ]);
  });

  it('serves local agent_reputation for asset lookups', async () => {
    const res = await fetch(`${baseUrl}/rest/v1/agent_reputation?asset=eq.Agent1111111111111111111111111111111111111`);
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual([
      expect.objectContaining({
        asset: 'Agent1111111111111111111111111111111111111',
        feedback_count: 3,
        avg_score: 83,
        positive_count: 2,
        negative_count: 1,
        validation_count: 4,
      }),
    ]);
  });

  it('serves local get_collection_agents RPC', async () => {
    const res = await fetch(
      `${baseUrl}/rest/v1/rpc/get_collection_agents?collection_id=eq.38&page_limit=eq.1&page_offset=eq.0`
    );
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual([
      expect.objectContaining({
        asset: 'Agent1111111111111111111111111111111111111',
        feedback_count: 3,
        avg_score: 83,
        positive_count: 2,
        negative_count: 1,
        validation_count: 4,
      }),
    ]);
    expect(collectionFindUnique).toHaveBeenCalledWith({
      where: { collectionId: 38n },
      select: { col: true, creator: true },
    });
    expect(agentFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          collectionPointer: 'c1:bafybeigdyrzt4x7n3z6l6zjptk5f5t5b4v5l5m5n5p5q5r5s5t5u5v5w5x',
          creator: 'Creator11111111111111111111111111111111111',
        }),
      })
    );
  });

  it('normalizes empty local agent_reputation nft_name to null', async () => {
    agentFindFirst.mockResolvedValueOnce({
      id: 'Agent1111111111111111111111111111111111111',
      owner: 'Owner111111111111111111111111111111111111',
      collection: 'Collection11111111111111111111111111111111',
      nftName: '',
      uri: 'https://example.com/agent-111.json',
      feedbackCount: 1,
      rawAvgScore: 58,
    });

    const res = await fetch(`${baseUrl}/rest/v1/agent_reputation?asset=eq.Agent1111111111111111111111111111111111111`);
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual([
      expect.objectContaining({
        asset: 'Agent1111111111111111111111111111111111111',
        nft_name: null,
      }),
    ]);
  });

  it('serves local collection_stats with Prisma aggregate count objects', async () => {
    const res = await fetch(
      `${baseUrl}/rest/v1/collection_stats?collection=eq.Collection11111111111111111111111111111111`
    );
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual([
      expect.objectContaining({
        collection: 'Collection11111111111111111111111111111111',
        registry_type: 'USER',
        authority: 'Authority111111111111111111111111111111111',
        agent_count: 1,
        total_feedbacks: 3,
        avg_score: 82.5,
      }),
    ]);
  });

  it('serves local get_leaderboard RPC with PostgreSQL-compatible sort keys', async () => {
    agentFindMany.mockResolvedValueOnce([
      {
        id: 'Agent1111111111111111111111111111111111111',
        owner: 'Owner111111111111111111111111111111111111',
        creator: 'Creator11111111111111111111111111111111111',
        uri: 'https://example.com/agent-111.json',
        wallet: 'Wallet11111111111111111111111111111111111',
        collection: 'Collection11111111111111111111111111111111',
        collectionPointer: 'c1:bafybeigdyrzt4x7n3z6l6zjptk5f5t5b4v5l5m5n5p5q5r5s5t5u5v5w5x',
        colLocked: false,
        parentAsset: null,
        parentCreator: null,
        parentLocked: false,
        nftName: 'Alpha Agent',
        atomEnabled: true,
        trustTier: 3,
        qualityScore: 80,
        confidence: 90,
        riskScore: 5,
        diversityRatio: 12,
        feedbackCount: 3,
        rawAvgScore: 88,
        agentId: 7n,
        status: 'FINALIZED',
        verifiedAt: new Date('2026-03-09T20:00:00.000Z'),
        verifiedSlot: 447346482n,
        createdAt: new Date('2026-03-09T19:00:00.000Z'),
        updatedAt: new Date('2026-03-09T19:10:00.000Z'),
        createdTxSignature: 'Sig111111111111111111111111111111111111111111111111111',
        createdSlot: 447346400n,
        txIndex: 3,
        eventOrdinal: 1,
      },
      {
        id: 'Agent2222222222222222222222222222222222222',
        owner: 'Owner222222222222222222222222222222222222',
        creator: 'Creator11111111111111111111111111111111111',
        uri: 'https://example.com/agent-222.json',
        wallet: 'Wallet22222222222222222222222222222222222',
        collection: 'Collection11111111111111111111111111111111',
        collectionPointer: 'c1:bafybeigdyrzt4x7n3z6l6zjptk5f5t5b4v5l5m5n5p5q5r5s5t5u5v5w5x',
        colLocked: false,
        parentAsset: null,
        parentCreator: null,
        parentLocked: false,
        nftName: 'Beta Agent',
        atomEnabled: true,
        trustTier: 3,
        qualityScore: 80,
        confidence: 90,
        riskScore: 5,
        diversityRatio: 10,
        feedbackCount: 2,
        rawAvgScore: 80,
        agentId: 8n,
        status: 'FINALIZED',
        verifiedAt: new Date('2026-03-09T20:00:00.000Z'),
        verifiedSlot: 447346482n,
        createdAt: new Date('2026-03-09T19:01:00.000Z'),
        updatedAt: new Date('2026-03-09T19:11:00.000Z'),
        createdTxSignature: 'Sig222222222222222222222222222222222222222222222222222',
        createdSlot: 447346401n,
        txIndex: 1,
        eventOrdinal: 0,
      },
    ]);

    const res = await fetch(`${baseUrl}/rest/v1/rpc/get_leaderboard`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ p_collection: null, p_min_tier: 2, p_limit: 2, p_cursor_sort_key: null }),
    });
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual([
      expect.objectContaining({
        asset: 'Agent1111111111111111111111111111111111111',
        trust_tier: 3,
        quality_score: 80,
        confidence: 90,
        risk_score: 5,
        feedback_count: 3,
        sort_key: '3008601739624372',
      }),
      expect.objectContaining({
        asset: 'Agent2222222222222222222222222222222222222',
        sort_key: '3008601730779997',
      }),
    ]);
  });

  it('excludes ORPHANED agents from local get_leaderboard RPC by default', async () => {
    agentFindMany.mockResolvedValueOnce([
      {
        id: 'Agent1111111111111111111111111111111111111',
        owner: 'Owner111111111111111111111111111111111111',
        creator: null,
        uri: 'https://example.com/agent-111.json',
        wallet: null,
        collection: null,
        collectionPointer: null,
        colLocked: false,
        parentAsset: null,
        parentCreator: null,
        parentLocked: false,
        nftName: 'Alpha Agent',
        atomEnabled: true,
        trustTier: 3,
        qualityScore: 80,
        confidence: 90,
        riskScore: 5,
        diversityRatio: 12,
        feedbackCount: 3,
        rawAvgScore: 88,
        agentId: 7n,
        status: 'FINALIZED',
        verifiedAt: null,
        verifiedSlot: null,
        createdAt: new Date('2026-03-09T19:00:00.000Z'),
        updatedAt: new Date('2026-03-09T19:10:00.000Z'),
        createdTxSignature: 'Sig111111111111111111111111111111111111111111111111111',
        createdSlot: 447346400n,
        txIndex: 3,
        eventOrdinal: 1,
      },
    ]);

    const res = await fetch(`${baseUrl}/rest/v1/rpc/get_leaderboard`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ p_limit: 1, p_min_tier: 0 }),
    });

    expect(res.status).toBe(200);
    expect(agentFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          status: { not: 'ORPHANED' },
        }),
      })
    );
  });

  it('serves local leaderboard view shape compatible with proxy mode ordering', async () => {
    agentFindMany.mockResolvedValueOnce([
      {
        id: 'Agent1111111111111111111111111111111111111',
        owner: 'Owner111111111111111111111111111111111111',
        collection: 'Collection11111111111111111111111111111111',
        nftName: 'Alpha Agent',
        uri: 'https://example.com/agent-111.json',
        trustTier: 3,
        qualityScore: 80,
        confidence: 90,
        riskScore: 5,
        diversityRatio: 12,
        feedbackCount: 3,
      },
      {
        id: 'Agent2222222222222222222222222222222222222',
        owner: 'Owner222222222222222222222222222222222222',
        collection: 'Collection11111111111111111111111111111111',
        nftName: 'Beta Agent',
        uri: 'https://example.com/agent-222.json',
        trustTier: 3,
        qualityScore: 80,
        confidence: 90,
        riskScore: 5,
        diversityRatio: 10,
        feedbackCount: 2,
      },
    ]);

    const res = await fetch(`${baseUrl}/rest/v1/leaderboard?limit=2`);
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual([
      expect.objectContaining({
        asset: 'Agent1111111111111111111111111111111111111',
        owner: 'Owner111111111111111111111111111111111111',
        collection: 'Collection11111111111111111111111111111111',
        nft_name: 'Alpha Agent',
        agent_uri: 'https://example.com/agent-111.json',
        trust_tier: 3,
        quality_score: 80,
        confidence: 90,
        risk_score: 5,
        diversity_ratio: 12,
        feedback_count: 3,
        sort_key: '3008601739624372',
      }),
      expect.objectContaining({
        asset: 'Agent2222222222222222222222222222222222222',
        sort_key: '3008601730779997',
      }),
    ]);
    expect(agentFindMany).toHaveBeenCalledWith({
      where: {
        trustTier: { gte: 2 },
        status: { not: 'ORPHANED' },
      },
      select: {
        id: true,
        owner: true,
        collection: true,
        nftName: true,
        uri: true,
        trustTier: true,
        qualityScore: true,
        confidence: true,
        riskScore: true,
        diversityRatio: true,
        feedbackCount: true,
      },
    });
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

describe('API Server REST cutoff/order parity', () => {
  let createApiServer: typeof import('../../../src/api/server.js').createApiServer;
  let app: Express;
  let server: Server;
  let baseUrl: string;
  const agentFindMany = vi.fn().mockResolvedValue([]);
  const feedbackFindMany = vi.fn().mockResolvedValue([
    {
      id: 'feedback-row',
      agentId: 'Agent1111111111111111111111111111111111111',
      client: 'Client111111111111111111111111111111111111',
      feedbackIndex: 2n,
      feedbackId: 9n,
      value: 1n,
      valueDecimals: 0,
      score: 88,
      tag1: null,
      tag2: null,
      endpoint: null,
      feedbackUri: null,
      feedbackHash: null,
      runningDigest: null,
      revoked: false,
      status: 'FINALIZED',
      verifiedAt: null,
      createdSlot: 42n,
      txIndex: 8,
      eventOrdinal: 2,
      createdTxSignature: 'SigFeedback111111111111111111111111111111111111',
      createdAt: new Date('2026-03-09T19:00:00.000Z'),
    },
  ]);
  const feedbackCount = vi.fn().mockResolvedValue(1);
  const responseFindMany = vi.fn().mockResolvedValue([
    {
      id: 'response-row',
      responseId: 4n,
      responder: 'Responder111111111111111111111111111111111',
      responseUri: null,
      responseHash: null,
      runningDigest: null,
      responseCount: 1n,
      status: 'FINALIZED',
      verifiedAt: null,
      slot: 77n,
      txIndex: 5,
      eventOrdinal: 1,
      txSignature: 'SigResponse111111111111111111111111111111111111',
      createdAt: new Date('2026-03-09T19:05:00.000Z'),
      feedback: {
        agentId: 'Agent1111111111111111111111111111111111111',
        client: 'Client111111111111111111111111111111111111',
        feedbackIndex: 2n,
        feedbackId: 9n,
        createdSlot: 42n,
        createdTxSignature: 'SigFeedback111111111111111111111111111111111111',
        status: 'FINALIZED',
      },
    },
  ]);
  const responseCount = vi.fn().mockResolvedValue(1);
  const revocationFindMany = vi.fn().mockResolvedValue([
    {
      id: 'revocation-row',
      revocationId: 6n,
      agentId: 'Agent1111111111111111111111111111111111111',
      client: 'Client111111111111111111111111111111111111',
      feedbackIndex: 2n,
      feedbackHash: null,
      slot: 66n,
      originalScore: 70,
      atomEnabled: true,
      hadImpact: true,
      runningDigest: null,
      revokeCount: 1n,
      txIndex: 4,
      eventOrdinal: 0,
      txSignature: 'SigRevoke11111111111111111111111111111111111111',
      status: 'FINALIZED',
      verifiedAt: null,
      createdAt: new Date('2026-03-09T19:06:00.000Z'),
    },
  ]);
  const revocationCount = vi.fn().mockResolvedValue(1);
  const collectionFindMany = vi.fn().mockResolvedValue([
    {
      collectionId: 3n,
      col: 'Collection11111111111111111111111111111111',
      creator: 'Creator11111111111111111111111111111111111',
      firstSeenAsset: 'Agent1111111111111111111111111111111111111',
      firstSeenAt: new Date('2026-03-09T18:00:00.000Z'),
      firstSeenSlot: 55n,
      firstSeenTxSignature: 'SigCollection1111111111111111111111111111111',
      lastSeenAt: new Date('2026-03-09T18:10:00.000Z'),
      lastSeenSlot: 56n,
      lastSeenTxIndex: null,
      lastSeenTxSignature: 'SigCollectionLast1111111111111111111111111111',
      assetCount: 1n,
      version: null,
      name: null,
      symbol: null,
      description: null,
      image: null,
      bannerImage: null,
      socialWebsite: null,
      socialX: null,
      socialDiscord: null,
      metadataStatus: null,
      metadataHash: null,
      metadataBytes: null,
      metadataUpdatedAt: null,
    },
  ]);
  const collectionCount = vi.fn().mockResolvedValue(1);
  const metadataFindMany = vi.fn().mockResolvedValue([
    {
      id: 'metadata-row',
      agentId: 'Agent1111111111111111111111111111111111111',
      key: 'profile',
      value: Buffer.from([0x00, 0x41, 0x42]),
      immutable: false,
      slot: 88n,
      txIndex: 7,
      eventOrdinal: 3,
      txSignature: 'SigMetadata11111111111111111111111111111111111',
      status: 'FINALIZED',
      verifiedAt: new Date('2026-03-09T19:10:00.000Z'),
      createdAt: new Date('2026-03-09T19:08:00.000Z'),
      updatedAt: new Date('2026-03-09T19:09:00.000Z'),
    },
  ]);
  const metadataCount = vi.fn().mockResolvedValue(1);

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
        agent: { findMany: agentFindMany },
        feedback: { findMany: feedbackFindMany, count: feedbackCount },
        feedbackResponse: { findMany: responseFindMany, count: responseCount },
        revocation: { findMany: revocationFindMany, count: revocationCount },
        collection: { findMany: collectionFindMany, count: collectionCount },
        agentMetadata: { findMany: metadataFindMany, count: metadataCount },
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
    agentFindMany.mockClear();
    feedbackFindMany.mockClear();
    feedbackCount.mockClear();
    responseFindMany.mockClear();
    responseCount.mockClear();
    revocationFindMany.mockClear();
    revocationCount.mockClear();
    collectionFindMany.mockClear();
    collectionCount.mockClear();
    metadataFindMany.mockClear();
    metadataCount.mockClear();
  });

  it('applies block_slot filters and deterministic ordering for agents', async () => {
    const res = await fetch(`${baseUrl}/rest/v1/agents?block_slot=lte.10&order=block_slot.asc&limit=1`);
    expect(res.status).toBe(200);
    expect(agentFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          createdSlot: { lte: 10n },
        }),
        orderBy: [
          { createdSlot: 'asc' },
          { txIndex: 'asc' },
          { eventOrdinal: 'asc' },
          { id: 'asc' },
        ],
      })
    );
  });

  it('applies block_slot filters on feedbacks and exposes tx ordering fields', async () => {
    const res = await fetch(`${baseUrl}/rest/v1/feedbacks?block_slot=lte.42&order=block_slot.asc&limit=1`);
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual([
      expect.objectContaining({
        block_slot: 42,
        tx_index: 8,
        event_ordinal: 2,
      }),
    ]);
    expect(feedbackFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          createdSlot: { lte: 42n },
        }),
      })
    );
  });

  it('applies block_slot filters on responses and exposes tx ordering fields', async () => {
    const res = await fetch(`${baseUrl}/rest/v1/responses?block_slot=lte.77&order=block_slot.asc&limit=1`);
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual([
      expect.objectContaining({
        block_slot: 77,
        tx_index: 5,
        event_ordinal: 1,
      }),
    ]);
    expect(responseFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          OR: [
            { slot: { lte: 77n } },
            {
              AND: [
                { slot: null },
                { feedback: { createdSlot: { lte: 77n } } },
              ],
            },
          ],
        }),
      })
    );
  });

  it('applies slot filters on revocations and accepts slot order aliases', async () => {
    const res = await fetch(`${baseUrl}/rest/v1/revocations?slot=lte.66&order=slot.asc&limit=1`);
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual([
      expect.objectContaining({
        slot: 66,
        tx_index: 4,
        event_ordinal: 0,
      }),
    ]);
    expect(revocationFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          slot: { lte: 66n },
        }),
        orderBy: [
          { slot: 'asc' },
          { agentId: 'asc' },
          { client: 'asc' },
          { feedbackIndex: 'asc' },
          { txIndex: 'asc' },
          { eventOrdinal: 'asc' },
          { txSignature: { sort: 'asc', nulls: 'last' } },
        ],
      })
    );
  });

  it('applies first_seen_slot filters and ordering on collections', async () => {
    const res = await fetch(`${baseUrl}/rest/v1/collections?first_seen_slot=lte.55&order=first_seen_slot.asc&limit=1`);
    expect(res.status).toBe(200);
    expect(collectionFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          firstSeenSlot: { lte: 55n },
        }),
        orderBy: [
          { firstSeenSlot: 'asc' },
          { col: 'asc' },
          { creator: 'asc' },
        ],
      })
    );
  });

  it('serves collection_pointers as an alias of collections', async () => {
    const res = await fetch(`${baseUrl}/rest/v1/collection_pointers?collection=eq.Collection11111111111111111111111111111111&limit=1`);
    expect(res.status).toBe(200);
    expect(collectionFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          col: 'Collection11111111111111111111111111111111',
        }),
        take: 1,
      })
    );
  });

  it('applies block_slot filters on metadata and returns Supabase-compatible fields', async () => {
    const res = await fetch(`${baseUrl}/rest/v1/metadata?asset=eq.Agent1111111111111111111111111111111111111&block_slot=lte.88&order=block_slot.asc&limit=1`);
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual([
      expect.objectContaining({
        id: 'Agent1111111111111111111111111111111111111:1900eab6c028483d7126599ee6f50de0',
        asset: 'Agent1111111111111111111111111111111111111',
        key: 'profile',
        key_hash: '1900eab6c028483d7126599ee6f50de0',
        value: 'AEFC',
        block_slot: 88,
        tx_index: 7,
        event_ordinal: 3,
        tx_signature: 'SigMetadata11111111111111111111111111111111111',
        status: 'FINALIZED',
      }),
    ]);
    expect(metadataFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          agentId: 'Agent1111111111111111111111111111111111111',
          slot: { lte: 88n },
        }),
        orderBy: [
          { slot: 'asc' },
          { txIndex: 'asc' },
          { eventOrdinal: 'asc' },
          { agentId: 'asc' },
          { key: 'asc' },
          { id: 'asc' },
        ],
      })
    );
  });
});

describe('API Server checkpoint JSON normalization', () => {
  let createApiServer: typeof import('../../../src/api/server.js').createApiServer;
  let app: Express;
  let server: Server;
  let baseUrl: string;
  const feedbackFindMany = vi.fn().mockResolvedValue([
    {
      id: 'fb-1',
      feedbackIndex: 999n,
      runningDigest: Buffer.from('11'.repeat(32), 'hex'),
      createdAt: new Date('2026-03-09T10:00:00.000Z'),
      createdSlot: 10n,
      txIndex: 1,
      eventOrdinal: 0,
      createdTxSignature: 'sig-feedback',
    },
  ]);
  const orphanFeedbackFindMany = vi.fn().mockResolvedValue([]);
  const feedbackResponseFindMany = vi.fn().mockResolvedValue([
    {
      id: 'resp-1',
      responseCount: 999n,
      runningDigest: Buffer.from('11'.repeat(32), 'hex'),
      createdAt: new Date('2026-03-09T10:00:00.000Z'),
      slot: 11n,
      txIndex: 1,
      eventOrdinal: 0,
      txSignature: 'sig-response',
    },
  ]);
  const orphanResponseFindMany = vi.fn().mockResolvedValue([]);
  const revocationFindMany = vi.fn().mockResolvedValue([
    {
      id: 'rev-1',
      revokeCount: 999n,
      runningDigest: Buffer.from('11'.repeat(32), 'hex'),
      createdAt: new Date('2026-03-09T10:00:00.000Z'),
      slot: 12n,
      txIndex: 1,
      eventOrdinal: 0,
      txSignature: 'sig-revoke',
    },
  ]);

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
        orphanFeedback: { findMany: orphanFeedbackFindMany },
        feedbackResponse: { findMany: feedbackResponseFindMany },
        orphanResponse: { findMany: orphanResponseFindMany },
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

  it('serializes local checkpoint event_count values as JSON numbers', async () => {
    const asset = 'Agent1111111111111111111111111111111111111';
    const listRes = await fetch(`${baseUrl}/rest/v1/checkpoints/${asset}?chainType=feedback`);
    expect(listRes.status).toBe(200);
    await expect(listRes.json()).resolves.toEqual([
      {
        agent_id: asset,
        chain_type: 'feedback',
        event_count: 1000,
        digest: '11'.repeat(32),
        created_at: '2026-03-09T10:00:00.000Z',
      },
    ]);

    const latestRes = await fetch(`${baseUrl}/rest/v1/checkpoints/${asset}/latest`);
    expect(latestRes.status).toBe(200);
    await expect(latestRes.json()).resolves.toEqual({
      feedback: {
        event_count: 1000,
        digest: '11'.repeat(32),
        created_at: '2026-03-09T10:00:00.000Z',
      },
      response: null,
      revoke: null,
    });
  });
});
