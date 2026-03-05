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

  it("supports block_slot ordering on feedbacks endpoint", async () => {
    const prisma = {
      feedback: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/feedbacks?limit=5&order=block_slot.desc`);
      expect(res.status).toBe(200);

      expect(prisma.feedback.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          orderBy: [
            { createdSlot: "desc" },
            { createdAt: "desc" },
            { agentId: "asc" },
            { client: "asc" },
            { feedbackIndex: "asc" },
            { id: "asc" },
          ],
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

  it("applies feedback_index PostgREST IN filters on feedbacks endpoint", async () => {
    const prisma = {
      feedback: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const inFilter = encodeURIComponent("in.(1,5,10)");
      const res = await fetch(`${baseUrl}/rest/v1/feedbacks?feedback_index=${inFilter}&limit=5`);
      expect(res.status).toBe(200);

      expect(prisma.feedback.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            status: { not: "ORPHANED" },
            feedbackIndex: { in: [1n, 5n, 10n] },
          }),
          take: 5,
          skip: 0,
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("applies quoted feedback_index IN filters on feedbacks endpoint", async () => {
    const prisma = {
      feedback: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const inFilter = encodeURIComponent('in.("1","5","10")');
      const res = await fetch(`${baseUrl}/rest/v1/feedbacks?feedback_index=${inFilter}&limit=5`);
      expect(res.status).toBe(200);

      expect(prisma.feedback.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            feedbackIndex: { in: [1n, 5n, 10n] },
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("rejects malformed quoted feedback_index IN filters on feedbacks endpoint", async () => {
    const prisma = {
      feedback: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const malformedInFilter = encodeURIComponent('in.("1,5)');
      const res = await fetch(`${baseUrl}/rest/v1/feedbacks?feedback_index=${malformedInFilter}`);
      expect(res.status).toBe(400);
      const body = await res.json() as { error?: string };
      expect(body.error).toContain("Invalid feedback_index IN filter");
      expect(prisma.feedback.findMany).not.toHaveBeenCalled();
    } finally {
      await stopServer(server);
    }
  });

  it("applies feedback_index neq filters on feedbacks endpoint", async () => {
    const prisma = {
      feedback: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/feedbacks?feedback_index=neq.7`);
      expect(res.status).toBe(200);

      expect(prisma.feedback.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            feedbackIndex: { not: 7n },
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("applies feedback_index not.in filters on feedbacks endpoint", async () => {
    const prisma = {
      feedback: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const notInFilter = encodeURIComponent("not.in.(1,5,10)");
      const res = await fetch(`${baseUrl}/rest/v1/feedbacks?feedback_index=${notInFilter}`);
      expect(res.status).toBe(200);

      expect(prisma.feedback.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            feedbackIndex: { notIn: [1n, 5n, 10n] },
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("rejects malformed quoted feedback_index NOT IN filters on feedbacks endpoint", async () => {
    const prisma = {
      feedback: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const malformedNotInFilter = encodeURIComponent('not.in.("1,5)');
      const res = await fetch(`${baseUrl}/rest/v1/feedbacks?feedback_index=${malformedNotInFilter}`);
      expect(res.status).toBe(400);
      const body = await res.json() as { error?: string };
      expect(body.error).toContain("Invalid feedback_index NOT IN filter");
      expect(prisma.feedback.findMany).not.toHaveBeenCalled();
    } finally {
      await stopServer(server);
    }
  });

  it("applies tx_signature filters on feedbacks endpoint", async () => {
    const prisma = {
      feedback: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/feedbacks?tx_signature=eq.sig-feedback-1`);
      expect(res.status).toBe(200);

      expect(prisma.feedback.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            createdTxSignature: "sig-feedback-1",
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("rejects malformed tx_signature IN filters on feedbacks endpoint", async () => {
    const prisma = {
      feedback: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const malformedInFilter = encodeURIComponent('in.("sig-a,sig-b)');
      const res = await fetch(`${baseUrl}/rest/v1/feedbacks?tx_signature=${malformedInFilter}`);
      expect(res.status).toBe(400);
      const body = await res.json() as { error?: string };
      expect(body.error).toContain("Invalid tx_signature filter");
      expect(prisma.feedback.findMany).not.toHaveBeenCalled();
    } finally {
      await stopServer(server);
    }
  });

  it("applies revoke_count PostgREST IN filters on revocations endpoint", async () => {
    const prisma = {
      revocation: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const inFilter = encodeURIComponent("in.(1,5,10)");
      const res = await fetch(`${baseUrl}/rest/v1/revocations?asset=asset-1&revoke_count=${inFilter}&order=revoke_count.asc`);
      expect(res.status).toBe(200);

      expect(prisma.revocation.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            agentId: "asset-1",
            revokeCount: { in: [1n, 5n, 10n] },
          }),
          orderBy: [
            { revokeCount: "asc" },
            { agentId: "asc" },
            { client: "asc" },
            { feedbackIndex: "asc" },
            { txSignature: "asc" },
            { txIndex: "asc" },
            { eventOrdinal: "asc" },
          ],
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("supports revocation_id ordering on revocations endpoint", async () => {
    const prisma = {
      revocation: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/revocations?asset=asset-1&order=revocation_id.asc`);
      expect(res.status).toBe(200);

      expect(prisma.revocation.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({ agentId: "asset-1" }),
          orderBy: [
            { revocationId: "asc" },
            { agentId: "asc" },
            { client: "asc" },
            { feedbackIndex: "asc" },
            { txSignature: "asc" },
            { txIndex: "asc" },
            { eventOrdinal: "asc" },
          ],
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("supports revocation_id ordering without asset scope using deterministic tie-breakers", async () => {
    const prisma = {
      revocation: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/revocations?order=revocation_id.asc`);
      expect(res.status).toBe(200);
      expect(prisma.revocation.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          orderBy: [
            { revocationId: "asc" },
            { agentId: "asc" },
            { client: "asc" },
            { feedbackIndex: "asc" },
            { txSignature: "asc" },
            { txIndex: "asc" },
            { eventOrdinal: "asc" },
          ],
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("applies quoted revoke_count PostgREST IN filters on revocations endpoint", async () => {
    const prisma = {
      revocation: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const inFilter = encodeURIComponent('in.("1","5","10")');
      const res = await fetch(`${baseUrl}/rest/v1/revocations?asset=asset-1&revoke_count=${inFilter}&order=revoke_count.asc`);
      expect(res.status).toBe(200);

      expect(prisma.revocation.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            agentId: "asset-1",
            revokeCount: { in: [1n, 5n, 10n] },
          }),
          orderBy: [
            { revokeCount: "asc" },
            { agentId: "asc" },
            { client: "asc" },
            { feedbackIndex: "asc" },
            { txSignature: "asc" },
            { txIndex: "asc" },
            { eventOrdinal: "asc" },
          ],
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("applies scalar revoke_count filters on revocations endpoint", async () => {
    const prisma = {
      revocation: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/revocations?asset=asset-1&revoke_count=eq.9`);
      expect(res.status).toBe(200);

      expect(prisma.revocation.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            agentId: "asset-1",
            revokeCount: 9n,
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("applies revoke_count neq filters on revocations endpoint", async () => {
    const prisma = {
      revocation: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/revocations?asset=asset-1&revoke_count=neq.9`);
      expect(res.status).toBe(200);

      expect(prisma.revocation.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            agentId: "asset-1",
            revokeCount: { not: 9n },
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("applies revoke_count not.in filters on revocations endpoint", async () => {
    const prisma = {
      revocation: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const notInFilter = encodeURIComponent("not.in.(1,5,10)");
      const res = await fetch(`${baseUrl}/rest/v1/revocations?asset=asset-1&revoke_count=${notInFilter}`);
      expect(res.status).toBe(200);

      expect(prisma.revocation.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            agentId: "asset-1",
            revokeCount: { notIn: [1n, 5n, 10n] },
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("applies tx_signature neq filters on revocations endpoint", async () => {
    const prisma = {
      revocation: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/revocations?asset=asset-1&tx_signature=neq.sig-revoke-1`);
      expect(res.status).toBe(200);

      expect(prisma.revocation.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            agentId: "asset-1",
            txSignature: { not: "sig-revoke-1" },
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("applies feedback_index neq filters on revocations endpoint", async () => {
    const prisma = {
      revocation: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/revocations?asset=asset-1&feedback_index=neq.4`);
      expect(res.status).toBe(200);

      expect(prisma.revocation.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            agentId: "asset-1",
            feedbackIndex: { not: 4n },
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("applies feedback_index IN filters on revocations endpoint", async () => {
    const prisma = {
      revocation: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const inFilter = encodeURIComponent("in.(1,2)");
      const res = await fetch(`${baseUrl}/rest/v1/revocations?asset=asset-1&feedback_index=${inFilter}`);
      expect(res.status).toBe(200);

      expect(prisma.revocation.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            agentId: "asset-1",
            feedbackIndex: { in: [1n, 2n] },
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("rejects invalid revoke_count IN filters on revocations endpoint", async () => {
    const prisma = {
      revocation: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const invalidInFilter = encodeURIComponent("in.(1,nope)");
      const res = await fetch(`${baseUrl}/rest/v1/revocations?revoke_count=${invalidInFilter}`);
      expect(res.status).toBe(400);

      const body = await res.json() as { error?: string };
      expect(body.error).toContain("Invalid revoke_count IN filter");
      expect(prisma.revocation.findMany).not.toHaveBeenCalled();
    } finally {
      await stopServer(server);
    }
  });

  it("rejects malformed quoted revoke_count IN filters on revocations endpoint", async () => {
    const prisma = {
      revocation: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const malformedInFilter = encodeURIComponent('in.("1,5)');
      const res = await fetch(`${baseUrl}/rest/v1/revocations?revoke_count=${malformedInFilter}`);
      expect(res.status).toBe(400);

      const body = await res.json() as { error?: string };
      expect(body.error).toContain("Invalid revoke_count IN filter");
      expect(prisma.revocation.findMany).not.toHaveBeenCalled();
    } finally {
      await stopServer(server);
    }
  });

  it("rejects malformed quoted revoke_count NOT IN filters on revocations endpoint", async () => {
    const prisma = {
      revocation: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const malformedNotInFilter = encodeURIComponent('not.in.("1,5)');
      const res = await fetch(`${baseUrl}/rest/v1/revocations?revoke_count=${malformedNotInFilter}`);
      expect(res.status).toBe(400);

      const body = await res.json() as { error?: string };
      expect(body.error).toContain("Invalid revoke_count NOT IN filter");
      expect(prisma.revocation.findMany).not.toHaveBeenCalled();
    } finally {
      await stopServer(server);
    }
  });

  it("returns 400 when response_id is provided without canonical feedback scope", async () => {
    const prisma = {
      feedbackResponse: {
        findMany: vi.fn(),
        count: vi.fn(),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/responses?response_id=eq.9&limit=1`);
      expect(res.status).toBe(400);

      const body = await res.json() as { error?: string };
      expect(body.error).toContain("response_id requires canonical feedback scope");
      expect(prisma.feedbackResponse.findMany).not.toHaveBeenCalled();
      expect(prisma.feedbackResponse.count).not.toHaveBeenCalled();
    } finally {
      await stopServer(server);
    }
  });

  it("applies feedback_index neq filters on responses endpoint", async () => {
    const prisma = {
      feedbackResponse: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/responses?asset=asset-1&feedback_index=neq.0&limit=1`);
      expect(res.status).toBe(200);
      expect(prisma.feedbackResponse.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            status: { not: "ORPHANED" },
            feedback: {
              agentId: "asset-1",
              feedbackIndex: { not: 0n },
            },
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("applies feedback_index IN filters on responses endpoint", async () => {
    const prisma = {
      feedbackResponse: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const inFilter = encodeURIComponent("in.(0,1)");
      const res = await fetch(`${baseUrl}/rest/v1/responses?asset=asset-1&feedback_index=${inFilter}&limit=1`);
      expect(res.status).toBe(200);
      expect(prisma.feedbackResponse.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            status: { not: "ORPHANED" },
            feedback: {
              agentId: "asset-1",
              feedbackIndex: { in: [0n, 1n] },
            },
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("applies feedback_index NOT IN filters on responses endpoint", async () => {
    const prisma = {
      feedbackResponse: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const notInFilter = encodeURIComponent("not.in.(0,1)");
      const res = await fetch(`${baseUrl}/rest/v1/responses?asset=asset-1&feedback_index=${notInFilter}&limit=1`);
      expect(res.status).toBe(200);
      expect(prisma.feedbackResponse.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            status: { not: "ORPHANED" },
            feedback: {
              agentId: "asset-1",
              feedbackIndex: { notIn: [0n, 1n] },
            },
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("rejects malformed quoted feedback_index NOT IN filters on responses endpoint", async () => {
    const prisma = {
      feedbackResponse: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const malformedNotInFilter = encodeURIComponent('not.in.("0,1)');
      const res = await fetch(`${baseUrl}/rest/v1/responses?asset=asset-1&feedback_index=${malformedNotInFilter}&limit=1`);
      expect(res.status).toBe(400);
      const body = await res.json() as { error?: string };
      expect(body.error).toContain("Invalid feedback_index NOT IN filter");
      expect(prisma.feedbackResponse.findMany).not.toHaveBeenCalled();
    } finally {
      await stopServer(server);
    }
  });

  it("allows response_id when canonical feedback scope is provided", async () => {
    const createdAt = new Date("2026-01-03T00:00:00.000Z");
    const prisma = {
      feedbackResponse: {
        findMany: vi.fn().mockResolvedValue([
          {
            responseId: 9n,
            responder: "responder-a",
            responseUri: "ipfs://response-a",
            responseHash: null,
            runningDigest: null,
            responseCount: 1n,
            status: "FINALIZED",
            verifiedAt: null,
            slot: 11n,
            txSignature: "sig-a",
            createdAt,
            feedback: {
              feedbackId: 10n,
              agentId: "asset-1",
              client: "client-1",
              feedbackIndex: 1n,
              createdSlot: 11n,
              createdTxSignature: "sig-a",
            },
          },
        ]),
        count: vi.fn().mockResolvedValue(1),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/responses?asset=asset-1&feedback_id=eq.10&response_id=eq.9`);
      expect(res.status).toBe(200);

      expect(prisma.feedbackResponse.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            responseId: 9n,
            feedback: { agentId: "asset-1", feedbackId: 10n },
          }),
        })
      );
      expect(prisma.feedbackResponse.count).not.toHaveBeenCalled();
    } finally {
      await stopServer(server);
    }
  });

  it("applies revocation_id PostgREST filters to Prisma where.revocationId", async () => {
    const prisma = {
      revocation: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    };

    const cases: Array<{ filter: string; expected: unknown }> = [
      { filter: "eq.9", expected: 9n },
      { filter: "gt.9", expected: { gt: 9n } },
      { filter: "gte.9", expected: { gte: 9n } },
      { filter: "lt.9", expected: { lt: 9n } },
      { filter: "lte.9", expected: { lte: 9n } },
    ];

    const { server, baseUrl } = await startServer(prisma);
    try {
      for (const testCase of cases) {
        const res = await fetch(
          `${baseUrl}/rest/v1/revocations?asset=asset-1&revocation_id=${encodeURIComponent(testCase.filter)}`
        );
        expect(res.status).toBe(200);
      }

      cases.forEach((testCase, index) => {
        expect(prisma.revocation.findMany).toHaveBeenNthCalledWith(
          index + 1,
          expect.objectContaining({
            where: expect.objectContaining({
              agentId: "asset-1",
              revocationId: testCase.expected,
            }),
          })
        );
      });
    } finally {
      await stopServer(server);
    }
  });

  it("requires asset when using revocation_id filters", async () => {
    const prisma = {
      revocation: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/revocations?revocation_id=eq.7`);
      expect(res.status).toBe(400);

      const body = await res.json() as { error?: string };
      expect(body.error).toContain("revocation_id is scoped by agent");
      expect(prisma.revocation.findMany).not.toHaveBeenCalled();
    } finally {
      await stopServer(server);
    }
  });

  it("rejects invalid revocation_id filters", async () => {
    const prisma = {
      revocation: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/revocations?asset=asset-1&revocation_id=gt.not-a-number`);
      expect(res.status).toBe(400);

      const body = await res.json() as { error?: string };
      expect(body.error).toContain("Invalid revocation_id filter");
      expect(prisma.revocation.findMany).not.toHaveBeenCalled();
    } finally {
      await stopServer(server);
    }
  });

  it("applies status=neq.ORPHANED as NOT filter on revocations endpoint", async () => {
    const prisma = {
      revocation: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/revocations?status=neq.ORPHANED`);
      expect(res.status).toBe(200);

      expect(prisma.revocation.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            status: { not: "ORPHANED" },
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("applies status=neq.ORPHANED as NOT filter on feedback_responses endpoint", async () => {
    const prisma = {
      feedbackResponse: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/feedback_responses?status=neq.ORPHANED`);
      expect(res.status).toBe(200);

      expect(prisma.feedbackResponse.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            status: { not: "ORPHANED" },
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("maps orphan responses with digest/count fields and response_count ordering", async () => {
    const prisma = {
      feedback: {
        findFirst: vi.fn().mockResolvedValue(null),
      },
      orphanResponse: {
        findMany: vi.fn().mockResolvedValue([
          {
            agentId: "asset-1",
            client: "client-1",
            feedbackIndex: 0n,
            responder: "responder-1",
            responseUri: "ipfs://resp",
            responseHash: Buffer.from("ab", "hex"),
            runningDigest: Buffer.from("cd", "hex"),
            responseCount: 7n,
            slot: 42n,
            txSignature: "tx-1",
            createdAt: new Date("2026-01-05T00:00:00.000Z"),
          },
        ]),
      },
      feedbackResponse: {
        findMany: vi.fn(),
        count: vi.fn(),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(
        `${baseUrl}/rest/v1/responses?asset=asset-1&client_address=client-1&feedback_index=eq.0&order=response_count.desc`
      );
      expect(res.status).toBe(200);

      expect(prisma.orphanResponse.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { agentId: "asset-1", client: "client-1", feedbackIndex: 0n },
          orderBy: [
            { responseCount: "desc" },
            { responder: "asc" },
            { txSignature: "asc" },
            { slot: "asc" },
          ],
        })
      );
      expect(prisma.feedbackResponse.findMany).not.toHaveBeenCalled();

      const body = await res.json() as Array<Record<string, unknown>>;
      expect(body).toHaveLength(1);
      expect(body[0]).toMatchObject({
        id: null,
        feedback_id: null,
        response_id: null,
        asset: "asset-1",
        client_address: "client-1",
        feedback_index: "0",
        responder: "responder-1",
        response_uri: "ipfs://resp",
        response_hash: "ab",
        running_digest: "cd",
        response_count: "7",
        status: "PENDING",
        verified_at: null,
        block_slot: 42,
        tx_signature: "tx-1",
      });
    } finally {
      await stopServer(server);
    }
  });

  it("supports response_id ordering on responses endpoint", async () => {
    const prisma = {
      feedback: {
        findFirst: vi.fn().mockResolvedValue({ id: "fb-1" }),
      },
      feedbackResponse: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(
        `${baseUrl}/rest/v1/responses?asset=asset-1&client_address=client-1&feedback_index=eq.0&order=response_id.asc`
      );
      expect(res.status).toBe(200);

      expect(prisma.feedbackResponse.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          orderBy: [
            { responseId: "asc" },
            { feedback: { agentId: "asc" } },
            { feedback: { client: "asc" } },
            { feedback: { feedbackIndex: "asc" } },
            { responder: "asc" },
            { txSignature: "asc" },
            { txIndex: "asc" },
            { eventOrdinal: "asc" },
          ],
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("supports block_slot ordering on responses endpoint", async () => {
    const prisma = {
      feedback: {
        findFirst: vi.fn().mockResolvedValue({ id: "fb-1" }),
      },
      feedbackResponse: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(
        `${baseUrl}/rest/v1/responses?asset=asset-1&client_address=client-1&feedback_index=eq.0&order=block_slot.desc`
      );
      expect(res.status).toBe(200);

      expect(prisma.feedbackResponse.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          orderBy: [
            { slot: "desc" },
            { feedback: { agentId: "asc" } },
            { feedback: { client: "asc" } },
            { feedback: { feedbackIndex: "asc" } },
            { responder: "asc" },
            { txSignature: "asc" },
            { txIndex: "asc" },
            { eventOrdinal: "asc" },
          ],
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("supports response_id ordering without canonical feedback scope using deterministic tie-breakers", async () => {
    const prisma = {
      feedbackResponse: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/responses?order=response_id.asc`);
      expect(res.status).toBe(200);
      expect(prisma.feedbackResponse.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          orderBy: [
            { responseId: "asc" },
            { feedback: { agentId: "asc" } },
            { feedback: { client: "asc" } },
            { feedback: { feedbackIndex: "asc" } },
            { responder: "asc" },
            { txSignature: "asc" },
            { txIndex: "asc" },
            { eventOrdinal: "asc" },
          ],
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("applies tx_signature IN filters on responses endpoint", async () => {
    const prisma = {
      feedbackResponse: {
        findMany: vi.fn().mockResolvedValue([]),
        count: vi.fn().mockResolvedValue(0),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const inFilter = encodeURIComponent("in.(sig-resp-1,sig-resp-2)");
      const res = await fetch(`${baseUrl}/rest/v1/responses?tx_signature=${inFilter}`);
      expect(res.status).toBe(200);
      expect(prisma.feedbackResponse.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            txSignature: { in: ["sig-resp-1", "sig-resp-2"] },
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("returns empty list for orphan responses when status filter excludes PENDING", async () => {
    const prisma = {
      feedback: {
        findFirst: vi.fn().mockResolvedValue(null),
      },
      orphanResponse: {
        findMany: vi.fn(),
      },
      feedbackResponse: {
        findMany: vi.fn(),
        count: vi.fn(),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(
        `${baseUrl}/rest/v1/responses?asset=asset-1&client_address=client-1&feedback_index=eq.0&status=eq.FINALIZED`
      );
      expect(res.status).toBe(200);
      expect(await res.json()).toEqual([]);
      expect(prisma.orphanResponse.findMany).not.toHaveBeenCalled();
      expect(prisma.feedbackResponse.findMany).not.toHaveBeenCalled();
    } finally {
      await stopServer(server);
    }
  });

  it("treats empty status comparison as no status filter for orphan responses", async () => {
    const prisma = {
      feedback: {
        findFirst: vi.fn().mockResolvedValue(null),
      },
      orphanResponse: {
        findMany: vi.fn().mockResolvedValue([
          {
            agentId: "asset-1",
            client: "client-1",
            feedbackIndex: 0n,
            responder: "responder-1",
            responseUri: null,
            responseHash: null,
            runningDigest: null,
            responseCount: 1n,
            slot: 10n,
            txSignature: "tx-1",
            createdAt: new Date("2026-01-05T00:00:00.000Z"),
          },
        ]),
      },
      feedbackResponse: {
        findMany: vi.fn(),
        count: vi.fn(),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(
        `${baseUrl}/rest/v1/responses?asset=asset-1&client_address=client-1&feedback_index=eq.0&status=eq.`
      );
      expect(res.status).toBe(200);
      const body = await res.json() as Array<Record<string, unknown>>;
      expect(body).toHaveLength(1);
      expect(prisma.orphanResponse.findMany).toHaveBeenCalledTimes(1);
      expect(prisma.feedbackResponse.findMany).not.toHaveBeenCalled();
    } finally {
      await stopServer(server);
    }
  });

  it("returns empty list for orphan responses when response_id is provided", async () => {
    const prisma = {
      feedback: {
        findFirst: vi.fn().mockResolvedValue(null),
      },
      orphanResponse: {
        findMany: vi.fn(),
      },
      feedbackResponse: {
        findMany: vi.fn(),
        count: vi.fn(),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(
        `${baseUrl}/rest/v1/responses?asset=asset-1&client_address=client-1&feedback_index=eq.0&response_id=eq.9`
      );
      expect(res.status).toBe(200);
      expect(await res.json()).toEqual([]);
      expect(prisma.orphanResponse.findMany).not.toHaveBeenCalled();
      expect(prisma.feedbackResponse.findMany).not.toHaveBeenCalled();
    } finally {
      await stopServer(server);
    }
  });
});
