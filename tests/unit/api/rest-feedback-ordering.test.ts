import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AddressInfo } from "net";
import type { Server } from "http";
import type { Express } from "express";

const originalEnv = process.env;

async function startServer(prisma: any): Promise<{ server: Server; baseUrl: string }> {
  const { createApiServer } = await import("../../../src/api/server.js");
  const app: Express = createApiServer({ prisma, pool: null });

  const server = await new Promise<Server>((resolve, reject) => {
    const started = app.listen(0, "127.0.0.1", () => resolve(started));
    started.on("error", reject);
  });

  const addr = server.address() as AddressInfo;
  return { server, baseUrl: `http://127.0.0.1:${addr.port}` };
}

async function stopServer(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((err) => (err ? reject(err) : resolve()));
  });
}

describe("REST feedback deterministic ordering", () => {
  beforeEach(() => {
    vi.resetModules();
    process.env = {
      ...originalEnv,
      API_MODE: "rest",
      ENABLE_GRAPHQL: "false",
    };
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  it("applies stable tie-breakers for created_at collisions", async () => {
    const prisma = {
      feedback: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/feedbacks?limit=5`);
      expect(res.status).toBe(200);

      expect(prisma.feedback.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          orderBy: [
            { createdAt: "desc" },
            { agentId: "asc" },
            { client: "asc" },
            { feedbackIndex: "asc" },
            { id: "asc" },
          ],
          take: 5,
          skip: 0,
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("maps revoked_at from revocation records when feedback is revoked", async () => {
    const createdAt = new Date("2026-01-01T00:00:00.000Z");
    const revokedAt = new Date("2026-01-02T03:04:05.000Z");
    const prisma = {
      feedback: {
        findMany: vi.fn().mockResolvedValue([
          {
            id: "fb-1",
            agentId: "asset-1",
            client: "client-1",
            feedbackIndex: 5n,
            value: "100",
            valueDecimals: 0,
            score: 80,
            tag1: "quality",
            tag2: "speed",
            endpoint: "/chat",
            feedbackUri: "ipfs://feedback",
            feedbackHash: null,
            runningDigest: null,
            revoked: true,
            status: "FINALIZED",
            verifiedAt: null,
            createdSlot: 123n,
            createdTxSignature: "sig-1",
            createdAt,
          },
        ]),
        count: vi.fn().mockResolvedValue(1),
      },
      revocation: {
        findMany: vi.fn().mockResolvedValue([
          {
            agentId: "asset-1",
            client: "client-1",
            feedbackIndex: 5n,
            createdAt: revokedAt,
          },
        ]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/feedbacks?limit=1`);
      expect(res.status).toBe(200);

      const body = await res.json() as Array<{ revoked_at: string | null }>;
      expect(body).toHaveLength(1);
      expect(body[0].revoked_at).toBe(revokedAt.toISOString());
      expect(prisma.revocation.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            OR: [
              {
                agentId: "asset-1",
                client: "client-1",
                feedbackIndex: 5n,
              },
            ],
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });
});
