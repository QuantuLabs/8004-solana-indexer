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
