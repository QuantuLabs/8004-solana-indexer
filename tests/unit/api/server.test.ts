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

import { createApiServer } from '../../../src/api/server.js';

describe('API Server (GraphQL-only)', () => {
  let app: Express;
  let server: Server;
  let baseUrl: string;

  beforeAll(async () => {
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
    await new Promise<void>((resolve, reject) => {
      server.close((err) => (err ? reject(err) : resolve()));
    });
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

  it('throws when no API backend is available', () => {
    expect(() => createApiServer({ pool: null as any, prisma: null as any })).toThrow(
      'No API backend available for API_MODE. Provide Prisma (REST), Supabase pool (GraphQL), or set API_MODE explicitly.'
    );
  });
});
