import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AddressInfo } from "net";
import type { Server } from "http";
import type { Express } from "express";

const originalEnv = process.env;

function rawStored(value: string): Buffer {
  return Buffer.concat([Buffer.from([0x00]), Buffer.from(value, "utf8")]);
}

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

describe("REST metadata pagination parity", () => {
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

  it("applies deterministic ordering with offset and metadata limit clamp", async () => {
    const prisma = {
      agentMetadata: {
        findMany: vi.fn().mockResolvedValue([
          {
            id: "m-1",
            agentId: "asset-1",
            key: "profile",
            value: rawStored("hello"),
            immutable: false,
            status: "FINALIZED",
            verifiedAt: null,
          },
        ]),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/metadata?asset=eq.asset-1&key=eq.profile&limit=1000&offset=7`);
      expect(res.status).toBe(200);
      await res.json();

      expect(prisma.agentMetadata.findMany).toHaveBeenCalledWith({
        where: { status: { not: "ORPHANED" }, agentId: "asset-1", key: "profile" },
        orderBy: [
          { slot: "desc" },
          { txIndex: "desc" },
          { eventOrdinal: "desc" },
          { agentId: "asc" },
          { key: "asc" },
          { id: "asc" },
        ],
        take: 100,
        skip: 7,
      });
    } finally {
      await stopServer(server);
    }
  });

  it("returns Content-Range with total count when Prefer: count=exact is requested", async () => {
    const prisma = {
      agentMetadata: {
        findMany: vi.fn().mockResolvedValue([
          {
            id: "m-2",
            agentId: "asset-2",
            key: "name",
            value: rawStored("world"),
            immutable: false,
            status: "FINALIZED",
            verifiedAt: new Date("2026-02-01T00:00:00.000Z"),
          },
        ]),
        count: vi.fn().mockResolvedValue(5),
      },
    };

    const { server, baseUrl } = await startServer(prisma);
    try {
      const res = await fetch(`${baseUrl}/rest/v1/metadata?limit=1&offset=2`, {
        headers: { Prefer: "count=exact" },
      });

      expect(res.status).toBe(200);
      expect(res.headers.get("content-range")).toBe("items 2-2/5");

      const body = (await res.json()) as Array<{ id: string; asset: string; key: string; value: string }>;
      expect(body).toHaveLength(1);
      expect(body[0]).toMatchObject({
        id: "asset-2:name",
        asset: "asset-2",
        key: "name",
        value: Buffer.from("world", "utf8").toString("base64"),
      });

      expect(prisma.agentMetadata.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          take: 1,
          skip: 2,
        })
      );
      expect(prisma.agentMetadata.count).toHaveBeenCalledWith({ where: { status: { not: "ORPHANED" } } });
    } finally {
      await stopServer(server);
    }
  });
});
