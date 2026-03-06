import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AddressInfo } from "net";
import type { Server } from "http";
import type { Express } from "express";

vi.mock("../../src/api/graphql/index.js", () => ({
  createGraphQLHandler: vi.fn(() => ({
    handle: (
      _req: unknown,
      res: { status: (code: number) => { json: (body: unknown) => void } }
    ) => {
      res.status(200).json({ data: { ok: true } });
    },
  })),
}));

vi.mock("../../src/utils/compression.js", () => ({
  decompressFromStorage: vi.fn(async (value: string | Buffer | null) => value),
}));

const originalEnv = process.env;
const ARCHIVED_VALIDATIONS_ERROR =
  "Validation endpoints are archived and no longer exposed. /rest/v1/validations has been retired.";
const BARE_COLLECTION_CID = "bafkreihvfphhye3jom6ewfrlvy4wx7itxmkjc6bjtzrlgfnmfs7dwxc7km";

function makePrismaStub() {
  return {
    agent: {
      findMany: vi.fn().mockResolvedValue([]),
      count: vi.fn().mockResolvedValue(0),
      groupBy: vi.fn().mockResolvedValue([]),
    },
    feedback: {
      groupBy: vi.fn().mockResolvedValue([]),
    },
    registry: {
      groupBy: vi.fn().mockResolvedValue([]),
    },
    agentMetadata: {
      groupBy: vi.fn().mockResolvedValue([]),
    },
    feedbackResponse: {
      groupBy: vi.fn().mockResolvedValue([]),
    },
    collection: {
      findMany: vi.fn().mockResolvedValue([]),
      count: vi.fn().mockResolvedValue(0),
    },
  };
}

async function startServer(options: { prisma: any; pool: any }) {
  const { createApiServer } = await import("../../src/api/server.js");
  const app: Express = createApiServer(options);

  const server = await new Promise<Server>((resolve, reject) => {
    const started = app.listen(0, "127.0.0.1", () => resolve(started));
    started.on("error", reject);
  });

  const addr = server.address() as AddressInfo;
  return {
    server,
    baseUrl: `http://127.0.0.1:${addr.port}`,
  };
}

async function stopServer(server: Server) {
  await new Promise<void>((resolve, reject) => {
    server.close((err) => (err ? reject(err) : resolve()));
  });
}

async function waitForGraphqlMount(baseUrl: string): Promise<void> {
  for (let i = 0; i < 20; i += 1) {
    const res = await fetch(`${baseUrl}/v2/graphql`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ query: "{ __typename }" }),
    });
    if (res.status === 200) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error("GraphQL endpoint was not mounted in time");
}

describe("API_MODE=both behavior", () => {
  beforeEach(() => {
    vi.resetModules();
    const nextEnv = {
      ...originalEnv,
      API_MODE: "both",
      ENABLE_GRAPHQL: "true",
    };
    delete nextEnv.SUPABASE_URL;
    delete nextEnv.SUPABASE_KEY;
    delete nextEnv.POSTGREST_URL;
    delete nextEnv.POSTGREST_TOKEN;
    process.env = nextEnv;
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  it("serves REST and leaves GraphQL unmounted when only Prisma is available", async () => {
    const { server, baseUrl } = await startServer({
      prisma: makePrismaStub() as any,
      pool: null as any,
    });

    try {
      const restRes = await fetch(`${baseUrl}/rest/v1/agents`);
      expect(restRes.status).toBe(200);

      const deprecatedCanonicalColRes = await fetch(`${baseUrl}/rest/v1/agents?canonical_col=eq.c1:ptr`);
      expect(deprecatedCanonicalColRes.status).toBe(200);

      const archivedValidationsRes = await fetch(`${baseUrl}/rest/v1/validations?limit=1`);
      expect(archivedValidationsRes.status).toBe(410);
      expect(await archivedValidationsRes.json()).toEqual({
        error: ARCHIVED_VALIDATIONS_ERROR,
      });

      const verificationRes = await fetch(`${baseUrl}/rest/v1/stats/verification`);
      expect(verificationRes.status).toBe(200);
      const verificationBody = await verificationRes.json();
      expect(verificationBody).not.toHaveProperty("validations");

      const gqlRes = await fetch(`${baseUrl}/v2/graphql`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ query: "{ stats { totalAgents } }" }),
      });
      expect(gqlRes.status).toBe(404);
    } finally {
      await stopServer(server);
    }
  });

  it("normalizes bare CID collection filters for local REST agents", async () => {
    const prisma = makePrismaStub();
    const { server, baseUrl } = await startServer({
      prisma: prisma as any,
      pool: null as any,
    });

    try {
      const res = await fetch(`${baseUrl}/rest/v1/agents?canonical_col=eq.${BARE_COLLECTION_CID}&limit=1`);
      expect(res.status).toBe(200);
      expect(prisma.agent.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            OR: [
              { collectionPointer: `c1:${BARE_COLLECTION_CID}` },
              { collectionPointer: BARE_COLLECTION_CID },
            ],
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("returns no matches for blank canonical_col filters in local REST agents", async () => {
    const prisma = makePrismaStub();
    const { server, baseUrl } = await startServer({
      prisma: prisma as any,
      pool: null as any,
    });

    try {
      const eqRes = await fetch(`${baseUrl}/rest/v1/agents?canonical_col=eq.%20%20&limit=1`);
      expect(eqRes.status).toBe(200);
      expect(await eqRes.json()).toEqual([]);

      const neqRes = await fetch(`${baseUrl}/rest/v1/agents?canonical_col=neq.%20%20&limit=1`);
      expect(neqRes.status).toBe(200);
      expect(await neqRes.json()).toEqual([]);
      expect(prisma.agent.findMany).not.toHaveBeenCalled();
    } finally {
      await stopServer(server);
    }
  });

  it("uses collection_pointer when canonical_col is explicitly empty in local REST agents", async () => {
    const prisma = makePrismaStub();
    const { server, baseUrl } = await startServer({
      prisma: prisma as any,
      pool: null as any,
    });

    try {
      const res = await fetch(
        `${baseUrl}/rest/v1/agents?canonical_col=&collection_pointer=eq.${BARE_COLLECTION_CID}&limit=1`
      );
      expect(res.status).toBe(200);
      expect(prisma.agent.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            OR: [
              { collectionPointer: `c1:${BARE_COLLECTION_CID}` },
              { collectionPointer: BARE_COLLECTION_CID },
            ],
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("applies canonical_col neq filters for local REST agents", async () => {
    const prisma = makePrismaStub();
    const { server, baseUrl } = await startServer({
      prisma: prisma as any,
      pool: null as any,
    });

    try {
      const res = await fetch(`${baseUrl}/rest/v1/agents?canonical_col=neq.${BARE_COLLECTION_CID}&limit=1`);
      expect(res.status).toBe(200);
      expect(prisma.agent.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            status: { not: "ORPHANED" },
            NOT: {
              collectionPointer: {
                in: [`c1:${BARE_COLLECTION_CID}`, BARE_COLLECTION_CID],
              },
            },
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("applies canonical_col in filters for local REST agents", async () => {
    const prisma = makePrismaStub();
    const { server, baseUrl } = await startServer({
      prisma: prisma as any,
      pool: null as any,
    });

    try {
      const res = await fetch(`${baseUrl}/rest/v1/agents?canonical_col=in.(${BARE_COLLECTION_CID},c1:ptr)&limit=1`);
      expect(res.status).toBe(200);
      expect(prisma.agent.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            status: { not: "ORPHANED" },
            OR: [
              { collectionPointer: `c1:${BARE_COLLECTION_CID}` },
              { collectionPointer: BARE_COLLECTION_CID },
              { collectionPointer: "c1:ptr" },
            ],
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("supports quoted canonical_col in filters with commas in local REST agents", async () => {
    const prisma = makePrismaStub();
    const { server, baseUrl } = await startServer({
      prisma: prisma as any,
      pool: null as any,
    });

    try {
      const inFilter = encodeURIComponent('in.("c1:ptr,extra",c1:ptr)');
      const res = await fetch(`${baseUrl}/rest/v1/agents?canonical_col=${inFilter}&limit=1`);
      expect(res.status).toBe(200);
      expect(prisma.agent.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            status: { not: "ORPHANED" },
            OR: [
              { collectionPointer: "c1:ptr,extra" },
              { collectionPointer: "c1:ptr" },
            ],
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("returns no matches for malformed quoted canonical_col IN in local REST agents", async () => {
    const prisma = makePrismaStub();
    const { server, baseUrl } = await startServer({
      prisma: prisma as any,
      pool: null as any,
    });

    try {
      const malformedInFilter = encodeURIComponent('in.("c1:ptr,extra)');
      const res = await fetch(`${baseUrl}/rest/v1/agents?canonical_col=${malformedInFilter}&limit=1`);
      expect(res.status).toBe(200);
      expect(await res.json()).toEqual([]);
      expect(prisma.agent.findMany).not.toHaveBeenCalled();
    } finally {
      await stopServer(server);
    }
  });

  it("applies canonical_col not.in filters for local REST agents", async () => {
    const prisma = makePrismaStub();
    const { server, baseUrl } = await startServer({
      prisma: prisma as any,
      pool: null as any,
    });

    try {
      const res = await fetch(`${baseUrl}/rest/v1/agents?canonical_col=not.in.(${BARE_COLLECTION_CID})&limit=1`);
      expect(res.status).toBe(200);
      expect(prisma.agent.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            status: { not: "ORPHANED" },
            NOT: {
              collectionPointer: {
                in: [`c1:${BARE_COLLECTION_CID}`, BARE_COLLECTION_CID],
              },
            },
          }),
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("matches legacy bare rows for local REST collection counts with CIDv1 filters", async () => {
    const creator = "Creator11111111111111111111111111111111";
    const prisma = makePrismaStub();
    prisma.agent.count.mockImplementation(async ({ where }: { where: any }) => (
      Array.isArray(where?.OR) ? 22 : 2
    ));

    const { server, baseUrl } = await startServer({
      prisma: prisma as any,
      pool: null as any,
    });

    try {
      const countRes = await fetch(
        `${baseUrl}/rest/v1/collection_asset_count?collection=eq.c1:${BARE_COLLECTION_CID}&creator=eq.${creator}`
      );
      expect(countRes.status).toBe(200);
      expect(await countRes.json()).toEqual({
        collection: `c1:${BARE_COLLECTION_CID}`,
        creator,
        asset_count: 22,
      });

      expect(prisma.agent.count).toHaveBeenCalledWith({
        where: {
          status: { not: "ORPHANED" },
          creator,
          OR: [
            { collectionPointer: `c1:${BARE_COLLECTION_CID}` },
            { collectionPointer: BARE_COLLECTION_CID },
          ],
        },
      });
    } finally {
      await stopServer(server);
    }
  });

  it("matches legacy bare rows for local REST collection assets with CIDv1 filters", async () => {
    const creator = "Creator11111111111111111111111111111111";
    const prisma = makePrismaStub();

    const { server, baseUrl } = await startServer({
      prisma: prisma as any,
      pool: null as any,
    });

    try {
      const assetsRes = await fetch(
        `${baseUrl}/rest/v1/collection_assets?collection=eq.c1:${BARE_COLLECTION_CID}&creator=eq.${creator}&limit=1`
      );
      expect(assetsRes.status).toBe(200);
      expect(await assetsRes.json()).toEqual([]);

      expect(prisma.agent.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: {
            status: { not: "ORPHANED" },
            creator,
            OR: [
              { collectionPointer: `c1:${BARE_COLLECTION_CID}` },
              { collectionPointer: BARE_COLLECTION_CID },
            ],
          },
        })
      );
    } finally {
      await stopServer(server);
    }
  });

  it("validates /rest/v1/collections collection_id filters and returns count headers", async () => {
    const prisma = makePrismaStub();
    prisma.collection.findMany.mockResolvedValue([
      {
        collectionId: 7n,
        col: `c1:${BARE_COLLECTION_CID}`,
        creator: "Creator11111111111111111111111111111111",
        firstSeenAsset: "Asset11111111111111111111111111111111111",
        firstSeenAt: new Date("2026-03-01T00:00:00.000Z"),
        firstSeenSlot: 100n,
        firstSeenTxSignature: "sig-first",
        lastSeenAt: new Date("2026-03-02T00:00:00.000Z"),
        lastSeenSlot: 200n,
        lastSeenTxSignature: "sig-last",
        assetCount: 1n,
        version: "1.0.0",
        name: "Test Collection",
        symbol: "TC",
        description: null,
        image: null,
        bannerImage: null,
        socialWebsite: null,
        socialX: null,
        socialDiscord: null,
        metadataStatus: "ok",
        metadataHash: null,
        metadataBytes: 128,
        metadataUpdatedAt: new Date("2026-03-02T00:00:00.000Z"),
      },
    ]);
    prisma.collection.count.mockResolvedValue(1);

    const { server, baseUrl } = await startServer({
      prisma: prisma as any,
      pool: null as any,
    });

    try {
      const invalidRes = await fetch(`${baseUrl}/rest/v1/collections?collection_id=eq.not-a-number`);
      expect(invalidRes.status).toBe(400);
      expect(await invalidRes.json()).toEqual({
        error: "Invalid collection_id filter: use eq/gt/gte/lt/lte with a valid integer",
      });

      const successRes = await fetch(`${baseUrl}/rest/v1/collections?collection_id=eq.7&limit=1&offset=0`, {
        headers: { Prefer: "count=exact" },
      });
      expect(successRes.status).toBe(200);
      const body = await successRes.json();
      expect(body).toHaveLength(1);
      expect(body[0].collection_id).toBe("7");
      expect(successRes.headers.get("content-range")).toBe("items 0-0/1");
    } finally {
      await stopServer(server);
    }
  });

  it("rejects blank creator for creator+collection scoped endpoints in local mode", async () => {
    const prisma = makePrismaStub();
    const { server, baseUrl } = await startServer({
      prisma: prisma as any,
      pool: null as any,
    });

    try {
      const countRes = await fetch(
        `${baseUrl}/rest/v1/collection_asset_count?collection=eq.${BARE_COLLECTION_CID}&creator=eq.%20%20%20`
      );
      expect(countRes.status).toBe(400);
      expect(await countRes.json()).toEqual({
        error: "Missing required query param: creator (scope is creator+collection)",
      });

      const assetsRes = await fetch(
        `${baseUrl}/rest/v1/collection_assets?collection=eq.${BARE_COLLECTION_CID}&creator=eq.%20%20%20&limit=1`
      );
      expect(assetsRes.status).toBe(400);
      expect(await assetsRes.json()).toEqual({
        error: "Missing required query param: creator (scope is creator+collection)",
      });
    } finally {
      await stopServer(server);
    }
  });

  it("rejects missing creator in proxy mode without upstream calls", async () => {
    process.env.SUPABASE_URL = "https://proxy-test.supabase.co";
    process.env.SUPABASE_KEY = "service-role-key";
    const realFetch = globalThis.fetch;
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input: any, init?: any) => {
      const url = typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : input?.url;
      if (typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/")) {
        return new Response(JSON.stringify([]), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      return realFetch(input, init);
    });

    const { server, baseUrl } = await startServer({
      prisma: null as any,
      pool: { query: vi.fn() } as any,
    });

    try {
      const countRes = await fetch(
        `${baseUrl}/rest/v1/collection_asset_count?collection=eq.${BARE_COLLECTION_CID}`
      );
      expect(countRes.status).toBe(400);

      const assetsRes = await fetch(
        `${baseUrl}/rest/v1/collection_assets?collection=eq.${BARE_COLLECTION_CID}&limit=1`
      );
      expect(assetsRes.status).toBe(400);

      const proxyCalls = fetchSpy.mock.calls.filter(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/");
      });
      expect(proxyCalls).toHaveLength(0);
    } finally {
      await stopServer(server);
    }
  });

  it("serves GraphQL and disables REST when only Supabase pool is available without Supabase REST URL", async () => {
    const { server, baseUrl } = await startServer({
      prisma: null as any,
      pool: { query: vi.fn() } as any,
    });

    try {
      const restRes = await fetch(`${baseUrl}/rest/v1/agents`);
      expect(restRes.status).toBe(410);

      const archivedValidationsRes = await fetch(`${baseUrl}/rest/v1/validations`);
      expect(archivedValidationsRes.status).toBe(410);
      expect(await archivedValidationsRes.json()).toEqual({
        error: ARCHIVED_VALIDATIONS_ERROR,
      });

      await waitForGraphqlMount(baseUrl);
      const gqlRes = await fetch(`${baseUrl}/v2/graphql`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ query: "{ stats { totalAgents } }" }),
      });
      expect(gqlRes.status).toBe(200);
      const body = await gqlRes.json();
      expect(body).toEqual({ data: { ok: true } });
    } finally {
      await stopServer(server);
    }
  });

  it("returns clear 503 and keeps GraphQL when SUPABASE_URL is set without SUPABASE_KEY", async () => {
    process.env.SUPABASE_URL = "https://proxy-test.supabase.co";
    const realFetch = globalThis.fetch;
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input: any, init?: any) => {
      const url = typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : input?.url;

      if (typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/")) {
        return new Response(JSON.stringify({ error: "should-not-be-called" }), { status: 500 });
      }

      return realFetch(input, init);
    });

    const { server, baseUrl } = await startServer({
      prisma: null as any,
      pool: { query: vi.fn() } as any,
    });

    try {
      const restRes = await fetch(`${baseUrl}/rest/v1/agents?limit=1`);
      expect(restRes.status).toBe(503);
      expect(await restRes.json()).toEqual({
        error: "REST proxy unavailable: SUPABASE_KEY (or POSTGREST_TOKEN) is not configured. Use /v2/graphql or set SUPABASE_KEY/POSTGREST_TOKEN.",
      });

      const archivedValidationsRes = await fetch(`${baseUrl}/rest/v1/validations?limit=1`);
      expect(archivedValidationsRes.status).toBe(410);
      expect(await archivedValidationsRes.json()).toEqual({
        error: ARCHIVED_VALIDATIONS_ERROR,
      });

      const proxiedUpstreamCall = fetchSpy.mock.calls.find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/");
      });
      expect(proxiedUpstreamCall).toBeUndefined();

      await waitForGraphqlMount(baseUrl);
      const gqlRes = await fetch(`${baseUrl}/v2/graphql`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ query: "{ stats { totalAgents } }" }),
      });
      expect(gqlRes.status).toBe(200);
    } finally {
      await stopServer(server);
    }
  });

  it("serves GraphQL and proxies REST when only Supabase pool is available with SUPABASE_URL + SUPABASE_KEY", async () => {
    process.env.SUPABASE_URL = "https://proxy-test.supabase.co";
    process.env.SUPABASE_KEY = "service-role-key";
    const poolQuery = vi.fn().mockResolvedValue({
      rows: [
        { model: "agents", pending_count: "2", finalized_count: "8", orphaned_count: "0" },
        { model: "feedbacks", pending_count: "0", finalized_count: "5", orphaned_count: "0" },
        { model: "collections", pending_count: "0", finalized_count: "0", orphaned_count: "1" },
        { model: "metadata", pending_count: "3", finalized_count: "0", orphaned_count: "0" },
        { model: "feedback_responses", pending_count: "0", finalized_count: "4", orphaned_count: "0" },
        { model: "revocations", pending_count: "9", finalized_count: "9", orphaned_count: "9" },
      ],
    });
    const realFetch = globalThis.fetch;
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input: any, init?: any) => {
      const url = typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : input?.url;

      if (typeof url === "string") {
        try {
          const parsed = new URL(url);
          if (
            parsed.pathname === "/rest/v1/agents"
            && parsed.searchParams.get("select") === "asset"
            && parsed.searchParams.get("limit") === "1"
            && parsed.searchParams.get("canonical_col")
              === `in.(c1:${BARE_COLLECTION_CID},${BARE_COLLECTION_CID})`
          ) {
            return new Response(
              JSON.stringify([{ asset: "ProxyAgentCid1111111111111111111111111111111" }]),
              {
                status: 200,
                headers: {
                  "content-type": "application/json",
                  "content-range": "items 0-0/22",
                },
              }
            );
          }
        } catch {
          // Non-URL values fall through to default mock behavior.
        }
      }

      if (typeof url === "string" && url.includes("/rest/v1/agents?") && url.includes("select=asset%2Ccanonical_col")) {
        return new Response(
          JSON.stringify([{ asset: "ProxyAgent11111111111111111111111111111111", canonical_col: "c1:proxy" }]),
          {
            status: 200,
            headers: {
              "content-type": "application/json",
              "content-range": "items 0-0/1",
            },
          }
        );
      }

      if (typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/")) {
        return new Response(
          JSON.stringify([{ asset: "ProxyAgent11111111111111111111111111111111" }]),
          {
            status: 200,
            headers: {
              "content-type": "application/json",
              "content-range": "items 0-0/1",
            },
          }
        );
      }

      return realFetch(input, init);
    });

    const { server, baseUrl } = await startServer({
      prisma: null as any,
      pool: { query: poolQuery } as any,
    });

    try {
      const restRes = await fetch(`${baseUrl}/rest/v1/agents?limit=1`, {
        headers: {
          Prefer: "count=exact",
          Authorization: "Bearer test-token",
        },
      });
      expect(restRes.status).toBe(200);
      expect(restRes.headers.get("content-range")).toBe("items 0-0/1");
      expect(await restRes.json()).toEqual([{ asset: "ProxyAgent11111111111111111111111111111111" }]);

      const pointerAliasRes = await fetch(
        `${baseUrl}/rest/v1/agents?limit=1&select=asset,canonical_col&includeOrphaned=true`
      );
      expect(pointerAliasRes.status).toBe(200);
      const pointerAliasBody = await pointerAliasRes.json();
      expect(pointerAliasBody).toEqual([{
        asset: "ProxyAgent11111111111111111111111111111111",
        canonical_col: "c1:proxy",
        collection_pointer: "c1:proxy",
      }]);

      const upstreamCall = fetchSpy.mock.calls.find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/agents?");
      });
      expect(upstreamCall).toBeTruthy();
      const upstreamUrlRaw = typeof upstreamCall?.[0] === "string"
        ? upstreamCall[0]
        : upstreamCall?.[0] instanceof URL
          ? upstreamCall[0].toString()
          : upstreamCall?.[0]?.url;
      expect(typeof upstreamUrlRaw).toBe("string");
      const upstreamUrl = new URL(upstreamUrlRaw as string);
      expect(upstreamUrl.pathname).toBe("/rest/v1/agents");
      expect(upstreamUrl.searchParams.get("limit")).toBe("1");
      expect(upstreamUrl.searchParams.get("status")).toBe("neq.ORPHANED");
      const upstreamInit = upstreamCall?.[1] as RequestInit | undefined;
      expect(upstreamInit?.method).toBe("GET");
      const forwardedHeaders = new Headers(upstreamInit?.headers);
      expect(forwardedHeaders.get("prefer")).toBe("count=exact");
      expect(forwardedHeaders.get("authorization")).toBe("Bearer test-token");
      expect(forwardedHeaders.get("apikey")).toBe("service-role-key");

      const includeOrphanedCallStart = fetchSpy.mock.calls.length;
      const includeOrphanedRes = await fetch(`${baseUrl}/rest/v1/agents?limit=1&includeOrphaned=true`);
      expect(includeOrphanedRes.status).toBe(200);
      const includeOrphanedUpstreamCall = fetchSpy.mock.calls.slice(includeOrphanedCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/agents?");
      });
      expect(includeOrphanedUpstreamCall).toBeTruthy();
      const includeOrphanedUrlRaw = typeof includeOrphanedUpstreamCall?.[0] === "string"
        ? includeOrphanedUpstreamCall[0]
        : includeOrphanedUpstreamCall?.[0] instanceof URL
          ? includeOrphanedUpstreamCall[0].toString()
          : includeOrphanedUpstreamCall?.[0]?.url;
      expect(typeof includeOrphanedUrlRaw).toBe("string");
      const includeOrphanedUrl = new URL(includeOrphanedUrlRaw as string);
      expect(includeOrphanedUrl.searchParams.get("includeOrphaned")).toBeNull();
      expect(includeOrphanedUrl.searchParams.get("status")).toBeNull();

      const collectionPointerCallStart = fetchSpy.mock.calls.length;
      const collectionPointerRes = await fetch(
        `${baseUrl}/rest/v1/agents?limit=1&collection_pointer=eq.c1:ptr`
      );
      expect(collectionPointerRes.status).toBe(200);
      const collectionPointerUpstreamCall = fetchSpy.mock.calls.slice(collectionPointerCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/agents?");
      });
      expect(collectionPointerUpstreamCall).toBeTruthy();
      const collectionPointerUrlRaw = typeof collectionPointerUpstreamCall?.[0] === "string"
        ? collectionPointerUpstreamCall[0]
        : collectionPointerUpstreamCall?.[0] instanceof URL
          ? collectionPointerUpstreamCall[0].toString()
          : collectionPointerUpstreamCall?.[0]?.url;
      expect(typeof collectionPointerUrlRaw).toBe("string");
      const collectionPointerUrl = new URL(collectionPointerUrlRaw as string);
      expect(collectionPointerUrl.searchParams.get("canonical_col")).toBe("eq.c1:ptr");
      expect(collectionPointerUrl.searchParams.get("collection_pointer")).toBeNull();

      const canonicalColAliasCallStart = fetchSpy.mock.calls.length;
      const canonicalColAliasRes = await fetch(
        `${baseUrl}/rest/v1/agents?limit=1&canonical_col=eq.c1:ptr`
      );
      expect(canonicalColAliasRes.status).toBe(200);
      const canonicalColAliasUpstreamCall = fetchSpy.mock.calls.slice(canonicalColAliasCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/agents?");
      });
      expect(canonicalColAliasUpstreamCall).toBeTruthy();
      const canonicalColAliasUrlRaw = typeof canonicalColAliasUpstreamCall?.[0] === "string"
        ? canonicalColAliasUpstreamCall[0]
        : canonicalColAliasUpstreamCall?.[0] instanceof URL
          ? canonicalColAliasUpstreamCall[0].toString()
          : canonicalColAliasUpstreamCall?.[0]?.url;
      expect(typeof canonicalColAliasUrlRaw).toBe("string");
      const canonicalColAliasUrl = new URL(canonicalColAliasUrlRaw as string);
      expect(canonicalColAliasUrl.searchParams.get("canonical_col")).toBe("eq.c1:ptr");
      expect(canonicalColAliasUrl.searchParams.get("collection_pointer")).toBeNull();

      const canonicalColNeqBareCallStart = fetchSpy.mock.calls.length;
      const canonicalColNeqBareRes = await fetch(
        `${baseUrl}/rest/v1/agents?limit=1&canonical_col=neq.${BARE_COLLECTION_CID}`
      );
      expect(canonicalColNeqBareRes.status).toBe(200);
      const canonicalColNeqBareUpstreamCall = fetchSpy.mock.calls.slice(canonicalColNeqBareCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/agents?");
      });
      expect(canonicalColNeqBareUpstreamCall).toBeTruthy();
      const canonicalColNeqBareUrlRaw = typeof canonicalColNeqBareUpstreamCall?.[0] === "string"
        ? canonicalColNeqBareUpstreamCall[0]
        : canonicalColNeqBareUpstreamCall?.[0] instanceof URL
          ? canonicalColNeqBareUpstreamCall[0].toString()
          : canonicalColNeqBareUpstreamCall?.[0]?.url;
      expect(typeof canonicalColNeqBareUrlRaw).toBe("string");
      const canonicalColNeqBareUrl = new URL(canonicalColNeqBareUrlRaw as string);
      expect(canonicalColNeqBareUrl.searchParams.get("canonical_col")).toBe(
        `not.in.(c1:${BARE_COLLECTION_CID},${BARE_COLLECTION_CID})`
      );
      expect(canonicalColNeqBareUrl.searchParams.get("collection_pointer")).toBeNull();

      const canonicalColNotInBareCallStart = fetchSpy.mock.calls.length;
      const canonicalColNotInBareRes = await fetch(
        `${baseUrl}/rest/v1/agents?limit=1&canonical_col=not.in.(${BARE_COLLECTION_CID})`
      );
      expect(canonicalColNotInBareRes.status).toBe(200);
      const canonicalColNotInBareUpstreamCall = fetchSpy.mock.calls.slice(canonicalColNotInBareCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/agents?");
      });
      expect(canonicalColNotInBareUpstreamCall).toBeTruthy();
      const canonicalColNotInBareUrlRaw = typeof canonicalColNotInBareUpstreamCall?.[0] === "string"
        ? canonicalColNotInBareUpstreamCall[0]
        : canonicalColNotInBareUpstreamCall?.[0] instanceof URL
          ? canonicalColNotInBareUpstreamCall[0].toString()
          : canonicalColNotInBareUpstreamCall?.[0]?.url;
      expect(typeof canonicalColNotInBareUrlRaw).toBe("string");
      const canonicalColNotInBareUrl = new URL(canonicalColNotInBareUrlRaw as string);
      expect(canonicalColNotInBareUrl.searchParams.get("canonical_col")).toBe(
        `not.in.(c1:${BARE_COLLECTION_CID},${BARE_COLLECTION_CID})`
      );
      expect(canonicalColNotInBareUrl.searchParams.get("collection_pointer")).toBeNull();

      const canonicalColInBareCallStart = fetchSpy.mock.calls.length;
      const canonicalColInBareRes = await fetch(
        `${baseUrl}/rest/v1/agents?limit=1&canonical_col=in.(${BARE_COLLECTION_CID},c1:ptr)`
      );
      expect(canonicalColInBareRes.status).toBe(200);
      const canonicalColInBareUpstreamCall = fetchSpy.mock.calls.slice(canonicalColInBareCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/agents?");
      });
      expect(canonicalColInBareUpstreamCall).toBeTruthy();
      const canonicalColInBareUrlRaw = typeof canonicalColInBareUpstreamCall?.[0] === "string"
        ? canonicalColInBareUpstreamCall[0]
        : canonicalColInBareUpstreamCall?.[0] instanceof URL
          ? canonicalColInBareUpstreamCall[0].toString()
          : canonicalColInBareUpstreamCall?.[0]?.url;
      expect(typeof canonicalColInBareUrlRaw).toBe("string");
      const canonicalColInBareUrl = new URL(canonicalColInBareUrlRaw as string);
      expect(canonicalColInBareUrl.searchParams.get("canonical_col")).toBe(`in.(c1:${BARE_COLLECTION_CID},${BARE_COLLECTION_CID},c1:ptr)`);
      expect(canonicalColInBareUrl.searchParams.get("collection_pointer")).toBeNull();

      const quotedCanonicalColInCallStart = fetchSpy.mock.calls.length;
      const quotedCanonicalColInFilter = encodeURIComponent('in.("c1:ptr,extra",c1:ptr)');
      const quotedCanonicalColInRes = await fetch(
        `${baseUrl}/rest/v1/agents?limit=1&canonical_col=${quotedCanonicalColInFilter}`
      );
      expect(quotedCanonicalColInRes.status).toBe(200);
      const quotedCanonicalColInUpstreamCall = fetchSpy.mock.calls.slice(quotedCanonicalColInCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/agents?");
      });
      expect(quotedCanonicalColInUpstreamCall).toBeTruthy();
      const quotedCanonicalColInUrlRaw = typeof quotedCanonicalColInUpstreamCall?.[0] === "string"
        ? quotedCanonicalColInUpstreamCall[0]
        : quotedCanonicalColInUpstreamCall?.[0] instanceof URL
          ? quotedCanonicalColInUpstreamCall[0].toString()
          : quotedCanonicalColInUpstreamCall?.[0]?.url;
      expect(typeof quotedCanonicalColInUrlRaw).toBe("string");
      const quotedCanonicalColInUrl = new URL(quotedCanonicalColInUrlRaw as string);
      expect(quotedCanonicalColInUrl.searchParams.get("canonical_col")).toBe('in.("c1:ptr,extra",c1:ptr)');
      expect(quotedCanonicalColInUrl.searchParams.get("collection_pointer")).toBeNull();

      const canonicalColTrailingSlashRes = await fetch(
        `${baseUrl}/rest/v1/agents/?limit=1&canonical_col=eq.c1:ptr`
      );
      expect(canonicalColTrailingSlashRes.status).toBe(200);

      const canonicalColBypassRes = await fetch(
        `${baseUrl}/rest/v1/agents?limit=1&collection_pointer=&canonical_col=eq.c1:ptr`
      );
      expect(canonicalColBypassRes.status).toBe(200);

      const canonicalEmptyWithPointerCallStart = fetchSpy.mock.calls.length;
      const canonicalEmptyWithPointerRes = await fetch(
        `${baseUrl}/rest/v1/agents?limit=1&canonical_col=&collection_pointer=eq.${BARE_COLLECTION_CID}`
      );
      expect(canonicalEmptyWithPointerRes.status).toBe(200);
      const canonicalEmptyWithPointerUpstreamCall = fetchSpy.mock.calls
        .slice(canonicalEmptyWithPointerCallStart)
        .find(([input]) => {
          const url = typeof input === "string"
            ? input
            : input instanceof URL
              ? input.toString()
              : input?.url;
          return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/agents?");
        });
      expect(canonicalEmptyWithPointerUpstreamCall).toBeTruthy();
      const canonicalEmptyWithPointerUrlRaw = typeof canonicalEmptyWithPointerUpstreamCall?.[0] === "string"
        ? canonicalEmptyWithPointerUpstreamCall[0]
        : canonicalEmptyWithPointerUpstreamCall?.[0] instanceof URL
          ? canonicalEmptyWithPointerUpstreamCall[0].toString()
          : canonicalEmptyWithPointerUpstreamCall?.[0]?.url;
      expect(typeof canonicalEmptyWithPointerUrlRaw).toBe("string");
      const canonicalEmptyWithPointerUrl = new URL(canonicalEmptyWithPointerUrlRaw as string);
      expect(canonicalEmptyWithPointerUrl.searchParams.get("canonical_col")).toBe(
        `in.(c1:${BARE_COLLECTION_CID},${BARE_COLLECTION_CID})`
      );
      expect(canonicalEmptyWithPointerUrl.searchParams.get("collection_pointer")).toBeNull();

      const canonicalColBlankCallStart = fetchSpy.mock.calls.length;
      const canonicalColBlankRes = await fetch(
        `${baseUrl}/rest/v1/agents?limit=1&canonical_col=eq.%20%20`
      );
      expect(canonicalColBlankRes.status).toBe(200);
      expect(await canonicalColBlankRes.json()).toEqual([]);
      const canonicalColBlankUpstreamCall = fetchSpy.mock.calls.slice(canonicalColBlankCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/agents?");
      });
      expect(canonicalColBlankUpstreamCall).toBeUndefined();

      const collectionPointerBlankCallStart = fetchSpy.mock.calls.length;
      const collectionPointerBlankRes = await fetch(
        `${baseUrl}/rest/v1/agents?limit=1&collection_pointer=%20%20`
      );
      expect(collectionPointerBlankRes.status).toBe(200);
      expect(await collectionPointerBlankRes.json()).toEqual([]);
      const collectionPointerBlankUpstreamCall = fetchSpy.mock.calls.slice(collectionPointerBlankCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/agents?");
      });
      expect(collectionPointerBlankUpstreamCall).toBeUndefined();

      const canonicalColBlankPostCallStart = fetchSpy.mock.calls.length;
      const canonicalColBlankPostRes = await fetch(
        `${baseUrl}/rest/v1/agents?canonical_col=eq.%20%20`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({}),
        }
      );
      expect(canonicalColBlankPostRes.status).toBe(405);
      expect(await canonicalColBlankPostRes.json()).toEqual({
        error: "REST proxy is read-only. Mutating methods are disabled.",
      });
      const canonicalColBlankPostUpstreamCall = fetchSpy.mock.calls.slice(canonicalColBlankPostCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/agents?");
      });
      expect(canonicalColBlankPostUpstreamCall).toBeUndefined();

      const malformedCanonicalColCallStart = fetchSpy.mock.calls.length;
      const malformedCanonicalColFilter = encodeURIComponent('in.("c1:ptr,extra)');
      const malformedCanonicalColRes = await fetch(
        `${baseUrl}/rest/v1/agents?limit=1&canonical_col=${malformedCanonicalColFilter}`
      );
      expect(malformedCanonicalColRes.status).toBe(200);
      expect(await malformedCanonicalColRes.json()).toEqual([]);
      const malformedCanonicalColUpstreamCall = fetchSpy.mock.calls.slice(malformedCanonicalColCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/agents?");
      });
      expect(malformedCanonicalColUpstreamCall).toBeUndefined();

      const statusDefaultProxyPaths = ["/feedbacks", "/responses", "/feedback_responses", "/revocations"];
      for (const path of statusDefaultProxyPaths) {
        const upstreamPath = path === "/responses" ? "/feedback_responses" : path;
        const defaultCallStart = fetchSpy.mock.calls.length;
        const defaultRes = await fetch(`${baseUrl}/rest/v1${path}?limit=1`);
        expect(defaultRes.status).toBe(200);
        const defaultUpstreamCall = fetchSpy.mock.calls.slice(defaultCallStart).find(([input]) => {
          const url = typeof input === "string"
            ? input
            : input instanceof URL
              ? input.toString()
              : input?.url;
          return typeof url === "string" && url.startsWith(`https://proxy-test.supabase.co/rest/v1${upstreamPath}?`);
        });
        expect(defaultUpstreamCall).toBeTruthy();
        const defaultUrlRaw = typeof defaultUpstreamCall?.[0] === "string"
          ? defaultUpstreamCall[0]
          : defaultUpstreamCall?.[0] instanceof URL
            ? defaultUpstreamCall[0].toString()
            : defaultUpstreamCall?.[0]?.url;
        expect(typeof defaultUrlRaw).toBe("string");
        const defaultUrl = new URL(defaultUrlRaw as string);
        expect(defaultUrl.searchParams.get("limit")).toBe("1");
        expect(defaultUrl.searchParams.get("status")).toBe("neq.ORPHANED");
        expect(defaultUrl.searchParams.get("includeOrphaned")).toBeNull();

        const includeCallStart = fetchSpy.mock.calls.length;
        const includeRes = await fetch(`${baseUrl}/rest/v1${path}?limit=1&includeOrphaned=true`);
        expect(includeRes.status).toBe(200);
        const includeUpstreamCall = fetchSpy.mock.calls.slice(includeCallStart).find(([input]) => {
          const url = typeof input === "string"
            ? input
            : input instanceof URL
              ? input.toString()
              : input?.url;
          return typeof url === "string" && url.startsWith(`https://proxy-test.supabase.co/rest/v1${upstreamPath}?`);
        });
        expect(includeUpstreamCall).toBeTruthy();
        const includeUrlRaw = typeof includeUpstreamCall?.[0] === "string"
          ? includeUpstreamCall[0]
          : includeUpstreamCall?.[0] instanceof URL
            ? includeUpstreamCall[0].toString()
            : includeUpstreamCall?.[0]?.url;
        expect(typeof includeUrlRaw).toBe("string");
        const includeUrl = new URL(includeUrlRaw as string);
        expect(includeUrl.searchParams.get("limit")).toBe("1");
        expect(includeUrl.searchParams.get("includeOrphaned")).toBeNull();
        expect(includeUrl.searchParams.get("status")).toBeNull();
      }

      const feedbackIndexNotInCallStart = fetchSpy.mock.calls.length;
      const feedbackIndexNotInFilter = encodeURIComponent("not.in.(1,2)");
      const feedbackIndexNotInRes = await fetch(
        `${baseUrl}/rest/v1/feedbacks?limit=1&feedback_index=${feedbackIndexNotInFilter}`
      );
      expect(feedbackIndexNotInRes.status).toBe(200);
      const feedbackIndexNotInUpstreamCall = fetchSpy.mock.calls.slice(feedbackIndexNotInCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/feedbacks?");
      });
      expect(feedbackIndexNotInUpstreamCall).toBeTruthy();
      const feedbackIndexNotInUrlRaw = typeof feedbackIndexNotInUpstreamCall?.[0] === "string"
        ? feedbackIndexNotInUpstreamCall[0]
        : feedbackIndexNotInUpstreamCall?.[0] instanceof URL
          ? feedbackIndexNotInUpstreamCall[0].toString()
          : feedbackIndexNotInUpstreamCall?.[0]?.url;
      expect(typeof feedbackIndexNotInUrlRaw).toBe("string");
      const feedbackIndexNotInUrl = new URL(feedbackIndexNotInUrlRaw as string);
      expect(feedbackIndexNotInUrl.searchParams.get("feedback_index")).toBe("not.in.(1,2)");
      expect(feedbackIndexNotInUrl.searchParams.get("status")).toBe("neq.ORPHANED");

      const feedbackIndexNeqCallStart = fetchSpy.mock.calls.length;
      const feedbackIndexNeqRes = await fetch(
        `${baseUrl}/rest/v1/feedbacks?limit=1&feedback_index=neq.7`
      );
      expect(feedbackIndexNeqRes.status).toBe(200);
      const feedbackIndexNeqUpstreamCall = fetchSpy.mock.calls.slice(feedbackIndexNeqCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/feedbacks?");
      });
      expect(feedbackIndexNeqUpstreamCall).toBeTruthy();
      const feedbackIndexNeqUrlRaw = typeof feedbackIndexNeqUpstreamCall?.[0] === "string"
        ? feedbackIndexNeqUpstreamCall[0]
        : feedbackIndexNeqUpstreamCall?.[0] instanceof URL
          ? feedbackIndexNeqUpstreamCall[0].toString()
          : feedbackIndexNeqUpstreamCall?.[0]?.url;
      expect(typeof feedbackIndexNeqUrlRaw).toBe("string");
      const feedbackIndexNeqUrl = new URL(feedbackIndexNeqUrlRaw as string);
      expect(feedbackIndexNeqUrl.searchParams.get("feedback_index")).toBe("neq.7");
      expect(feedbackIndexNeqUrl.searchParams.get("status")).toBe("neq.ORPHANED");

      const revokeCountNotInCallStart = fetchSpy.mock.calls.length;
      const revokeCountNotInFilter = encodeURIComponent("not.in.(1,2)");
      const revokeCountNotInRes = await fetch(
        `${baseUrl}/rest/v1/revocations?limit=1&revoke_count=${revokeCountNotInFilter}`
      );
      expect(revokeCountNotInRes.status).toBe(200);
      const revokeCountNotInUpstreamCall = fetchSpy.mock.calls.slice(revokeCountNotInCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/revocations?");
      });
      expect(revokeCountNotInUpstreamCall).toBeTruthy();
      const revokeCountNotInUrlRaw = typeof revokeCountNotInUpstreamCall?.[0] === "string"
        ? revokeCountNotInUpstreamCall[0]
        : revokeCountNotInUpstreamCall?.[0] instanceof URL
          ? revokeCountNotInUpstreamCall[0].toString()
          : revokeCountNotInUpstreamCall?.[0]?.url;
      expect(typeof revokeCountNotInUrlRaw).toBe("string");
      const revokeCountNotInUrl = new URL(revokeCountNotInUrlRaw as string);
      expect(revokeCountNotInUrl.searchParams.get("revoke_count")).toBe("not.in.(1,2)");
      expect(revokeCountNotInUrl.searchParams.get("status")).toBe("neq.ORPHANED");

      const revokeCountInCallStart = fetchSpy.mock.calls.length;
      const revokeCountInFilter = encodeURIComponent("in.(1,2)");
      const revokeCountInRes = await fetch(
        `${baseUrl}/rest/v1/revocations?limit=1&revoke_count=${revokeCountInFilter}`
      );
      expect(revokeCountInRes.status).toBe(200);
      const revokeCountInUpstreamCall = fetchSpy.mock.calls.slice(revokeCountInCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/revocations?");
      });
      expect(revokeCountInUpstreamCall).toBeTruthy();
      const revokeCountInUrlRaw = typeof revokeCountInUpstreamCall?.[0] === "string"
        ? revokeCountInUpstreamCall[0]
        : revokeCountInUpstreamCall?.[0] instanceof URL
          ? revokeCountInUpstreamCall[0].toString()
          : revokeCountInUpstreamCall?.[0]?.url;
      expect(typeof revokeCountInUrlRaw).toBe("string");
      const revokeCountInUrl = new URL(revokeCountInUrlRaw as string);
      expect(revokeCountInUrl.searchParams.get("revoke_count")).toBe("in.(1,2)");
      expect(revokeCountInUrl.searchParams.get("status")).toBe("neq.ORPHANED");

      const statsCallStart = fetchSpy.mock.calls.length;
      const statsRes = await fetch(`${baseUrl}/rest/v1/stats?limit=1`);
      expect(statsRes.status).toBe(200);
      const statsUpstreamCall = fetchSpy.mock.calls.slice(statsCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/global_stats");
      });
      expect(statsUpstreamCall).toBeTruthy();
      const statsUpstreamUrlRaw = typeof statsUpstreamCall?.[0] === "string"
        ? statsUpstreamCall[0]
        : statsUpstreamCall?.[0] instanceof URL
          ? statsUpstreamCall[0].toString()
          : statsUpstreamCall?.[0]?.url;
      expect(typeof statsUpstreamUrlRaw).toBe("string");
      const statsUpstreamUrl = new URL(statsUpstreamUrlRaw as string);
      expect(statsUpstreamUrl.pathname).toBe("/rest/v1/global_stats");
      expect(statsUpstreamUrl.searchParams.get("limit")).toBe("1");

      const statsIncludeOrphanedCallStart = fetchSpy.mock.calls.length;
      const statsIncludeOrphanedRes = await fetch(`${baseUrl}/rest/v1/stats?includeOrphaned=true&limit=2`);
      expect(statsIncludeOrphanedRes.status).toBe(200);
      const statsIncludeOrphanedUpstreamCall = fetchSpy.mock.calls.slice(statsIncludeOrphanedCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/global_stats");
      });
      expect(statsIncludeOrphanedUpstreamCall).toBeTruthy();
      const statsIncludeOrphanedUpstreamUrlRaw = typeof statsIncludeOrphanedUpstreamCall?.[0] === "string"
        ? statsIncludeOrphanedUpstreamCall[0]
        : statsIncludeOrphanedUpstreamCall?.[0] instanceof URL
          ? statsIncludeOrphanedUpstreamCall[0].toString()
          : statsIncludeOrphanedUpstreamCall?.[0]?.url;
      expect(typeof statsIncludeOrphanedUpstreamUrlRaw).toBe("string");
      const statsIncludeOrphanedUpstreamUrl = new URL(statsIncludeOrphanedUpstreamUrlRaw as string);
      expect(statsIncludeOrphanedUpstreamUrl.pathname).toBe("/rest/v1/global_stats");
      expect(statsIncludeOrphanedUpstreamUrl.searchParams.get("includeOrphaned")).toBeNull();
      expect(statsIncludeOrphanedUpstreamUrl.searchParams.get("limit")).toBe("2");

      const verificationCallStart = fetchSpy.mock.calls.length;
      const verificationPoolCallStart = poolQuery.mock.calls.length;
      const verificationRes = await fetch(`${baseUrl}/rest/v1/stats/verification`);
      expect(verificationRes.status).toBe(200);
      expect(await verificationRes.json()).toEqual({
        agents: { PENDING: 2, FINALIZED: 8, ORPHANED: 0 },
        feedbacks: { PENDING: 0, FINALIZED: 5, ORPHANED: 0 },
        registries: { PENDING: 0, FINALIZED: 0, ORPHANED: 1 },
        metadata: { PENDING: 3, FINALIZED: 0, ORPHANED: 0 },
        feedback_responses: { PENDING: 0, FINALIZED: 4, ORPHANED: 0 },
      });
      const verificationProxyCall = fetchSpy.mock.calls.slice(verificationCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/stats/verification");
      });
      expect(verificationProxyCall).toBeUndefined();
      const verificationPoolCall = poolQuery.mock.calls.slice(verificationPoolCallStart).find(([query]) =>
        typeof query === "string"
          && query.includes("FROM verification_stats")
      );
      expect(verificationPoolCall).toBeTruthy();
      const verificationSlashCallStart = fetchSpy.mock.calls.length;
      const verificationSlashRes = await fetch(`${baseUrl}/rest/v1/stats/verification/`);
      expect(verificationSlashRes.status).toBe(200);
      const verificationSlashProxyCall = fetchSpy.mock.calls.slice(verificationSlashCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/stats/verification/");
      });
      expect(verificationSlashProxyCall).toBeUndefined();
      const verificationWriteRes = await fetch(`${baseUrl}/rest/v1/stats/verification`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      });
      expect(verificationWriteRes.status).toBe(405);
      expect(await verificationWriteRes.json()).toEqual({
        error: "REST proxy is read-only. Mutating methods are disabled.",
      });

      const creator = "Creator11111111111111111111111111111111";
      const collectionCountRes = await fetch(
        `${baseUrl}/rest/v1/collection_asset_count?collection=eq.Collection11111111111111111111111111111111&creator=eq.${creator}`
      );
      expect(collectionCountRes.status).toBe(200);
      expect(await collectionCountRes.json()).toEqual({
        collection: "Collection11111111111111111111111111111111",
        creator,
        asset_count: 1,
      });

      const cidCollectionCountCallStart = fetchSpy.mock.calls.length;
      const cidCollectionCountRes = await fetch(
        `${baseUrl}/rest/v1/collection_asset_count?collection=eq.c1:${BARE_COLLECTION_CID}&creator=eq.${creator}`
      );
      expect(cidCollectionCountRes.status).toBe(200);
      expect(await cidCollectionCountRes.json()).toEqual({
        collection: `c1:${BARE_COLLECTION_CID}`,
        creator,
        asset_count: 22,
      });
      const cidCollectionCountUpstreamCall = fetchSpy.mock.calls.slice(cidCollectionCountCallStart).find(([input]) => {
        const raw = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        if (typeof raw !== "string") return false;
        const parsed = new URL(raw);
        return parsed.pathname === "/rest/v1/agents"
          && parsed.searchParams.get("canonical_col")
            === `in.(c1:${BARE_COLLECTION_CID},${BARE_COLLECTION_CID})`;
      });
      expect(cidCollectionCountUpstreamCall).toBeTruthy();

      const cidCollectionAssetsCallStart = fetchSpy.mock.calls.length;
      const cidCollectionAssetsRes = await fetch(
        `${baseUrl}/rest/v1/collection_assets?collection=eq.c1:${BARE_COLLECTION_CID}&creator=eq.${creator}&limit=1`
      );
      expect(cidCollectionAssetsRes.status).toBe(200);
      const cidCollectionAssetsUpstreamCall = fetchSpy.mock.calls.slice(cidCollectionAssetsCallStart).find(([input]) => {
        const raw = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        if (typeof raw !== "string") return false;
        const parsed = new URL(raw);
        return parsed.pathname === "/rest/v1/agents"
          && parsed.searchParams.get("canonical_col")
            === `in.(c1:${BARE_COLLECTION_CID},${BARE_COLLECTION_CID})`;
      });
      expect(cidCollectionAssetsUpstreamCall).toBeTruthy();

      const collectionAssetsRes = await fetch(
        `${baseUrl}/rest/v1/collection_assets?collection=eq.Collection11111111111111111111111111111111&creator=eq.${creator}&limit=1`
      );
      expect(collectionAssetsRes.status).toBe(200);
      expect(await collectionAssetsRes.json()).toEqual([{ asset: "ProxyAgent11111111111111111111111111111111" }]);

      const collectionCountStatusCallStart = fetchSpy.mock.calls.length;
      const collectionCountStatusRes = await fetch(
        `${baseUrl}/rest/v1/collection_asset_count?collection=eq.Collection11111111111111111111111111111111&creator=eq.${creator}&status=neq.ORPHANED`
      );
      expect(collectionCountStatusRes.status).toBe(200);
      const collectionCountStatusUpstreamCall = fetchSpy.mock.calls.slice(collectionCountStatusCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string"
          && url.startsWith("https://proxy-test.supabase.co/rest/v1/agents?")
          && url.includes("canonical_col=eq.Collection11111111111111111111111111111111");
      });
      expect(collectionCountStatusUpstreamCall).toBeTruthy();
      const collectionCountStatusUrlRaw = typeof collectionCountStatusUpstreamCall?.[0] === "string"
        ? collectionCountStatusUpstreamCall[0]
        : collectionCountStatusUpstreamCall?.[0] instanceof URL
          ? collectionCountStatusUpstreamCall[0].toString()
          : collectionCountStatusUpstreamCall?.[0]?.url;
      expect(typeof collectionCountStatusUrlRaw).toBe("string");
      const collectionCountStatusUrl = new URL(collectionCountStatusUrlRaw as string);
      expect(collectionCountStatusUrl.searchParams.get("status")).toBe("neq.ORPHANED");

      const collectionAssetsStatusCallStart = fetchSpy.mock.calls.length;
      const collectionAssetsStatusRes = await fetch(
        `${baseUrl}/rest/v1/collection_assets?collection=eq.Collection11111111111111111111111111111111&creator=eq.${creator}&limit=1&status=neq.ORPHANED`
      );
      expect(collectionAssetsStatusRes.status).toBe(200);
      const collectionAssetsStatusUpstreamCall = fetchSpy.mock.calls.slice(collectionAssetsStatusCallStart).find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string"
          && url.startsWith("https://proxy-test.supabase.co/rest/v1/agents?")
          && url.includes("canonical_col=eq.Collection11111111111111111111111111111111");
      });
      expect(collectionAssetsStatusUpstreamCall).toBeTruthy();
      const collectionAssetsStatusUrlRaw = typeof collectionAssetsStatusUpstreamCall?.[0] === "string"
        ? collectionAssetsStatusUpstreamCall[0]
        : collectionAssetsStatusUpstreamCall?.[0] instanceof URL
          ? collectionAssetsStatusUpstreamCall[0].toString()
          : collectionAssetsStatusUpstreamCall?.[0]?.url;
      expect(typeof collectionAssetsStatusUrlRaw).toBe("string");
      const collectionAssetsStatusUrl = new URL(collectionAssetsStatusUrlRaw as string);
      expect(collectionAssetsStatusUrl.searchParams.get("status")).toBe("neq.ORPHANED");

      const collectionCompatUpstreamCall = fetchSpy.mock.calls.find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string"
          && url.startsWith("https://proxy-test.supabase.co/rest/v1/agents?")
          && url.includes("canonical_col=eq.Collection11111111111111111111111111111111");
      });
      expect(collectionCompatUpstreamCall).toBeTruthy();

      const directCollectionCompatProxyCall = fetchSpy.mock.calls.find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string"
          && url.startsWith("https://proxy-test.supabase.co/rest/v1/collection_asset_count");
      });
      expect(directCollectionCompatProxyCall).toBeUndefined();

      const writeAttempt = await fetch(`${baseUrl}/rest/v1/agents`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ asset: "forbidden" }),
      });
      expect(writeAttempt.status).toBe(405);
      expect(await writeAttempt.json()).toEqual({
        error: "REST proxy is read-only. Mutating methods are disabled.",
      });

      const proxiedWriteCall = fetchSpy.mock.calls.find(([input, init]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        const method = (init as RequestInit | undefined)?.method?.toUpperCase() ?? "GET";
        return typeof url === "string"
          && url.startsWith("https://proxy-test.supabase.co/rest/v1/")
          && method === "POST";
      });
      expect(proxiedWriteCall).toBeUndefined();

      const forbiddenPathRes = await fetch(`${baseUrl}/rest/v1/pg_catalog.pg_tables?limit=1`);
      expect(forbiddenPathRes.status).toBe(403);
      expect(await forbiddenPathRes.json()).toEqual({
        error: "REST proxy path not allowed",
      });
      const traversalPathRes = await fetch(`${baseUrl}/rest/v1/agents/%2e%2e/%2e%2e/pg_catalog.pg_tables?limit=1`);
      expect([403, 404]).toContain(traversalPathRes.status);
      if (traversalPathRes.status === 403) {
        expect(await traversalPathRes.json()).toEqual({
          error: "REST proxy path not allowed",
        });
      }

      const forbiddenPathCall = fetchSpy.mock.calls.find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.includes("https://proxy-test.supabase.co/rest/v1/pg_catalog.pg_tables");
      });
      expect(forbiddenPathCall).toBeUndefined();
      const traversalPathCall = fetchSpy.mock.calls.find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.includes("https://proxy-test.supabase.co/rest/v1/agents/%2e%2e/%2e%2e/pg_catalog.pg_tables");
      });
      expect(traversalPathCall).toBeUndefined();

      const archivedValidationsRes = await fetch(`${baseUrl}/rest/v1/validations?limit=1`);
      expect(archivedValidationsRes.status).toBe(410);
      expect(await archivedValidationsRes.json()).toEqual({
        error: ARCHIVED_VALIDATIONS_ERROR,
      });

      const validationsProxyCall = fetchSpy.mock.calls.find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/rest/v1/validations");
      });
      expect(validationsProxyCall).toBeUndefined();

      await waitForGraphqlMount(baseUrl);
      const gqlRes = await fetch(`${baseUrl}/v2/graphql`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ query: "{ stats { totalAgents } }" }),
      });
      expect(gqlRes.status).toBe(200);
    } finally {
      await stopServer(server);
    }
  });

  it("serves GraphQL and proxies REST when only aliases POSTGREST_URL + POSTGREST_TOKEN are set", async () => {
    process.env.POSTGREST_URL = "https://proxy-test.supabase.co";
    process.env.POSTGREST_TOKEN = "alias-service-role-key";
    const realFetch = globalThis.fetch;
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input: any, init?: any) => {
      const url = typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : input?.url;

      if (typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/")) {
        return new Response(
          JSON.stringify([{ asset: "ProxyAgent11111111111111111111111111111111" }]),
          {
            status: 200,
            headers: {
              "content-type": "application/json",
              "content-range": "items 0-0/1",
            },
          }
        );
      }

      return realFetch(input, init);
    });

    const { server, baseUrl } = await startServer({
      prisma: null as any,
      pool: { query: vi.fn() } as any,
    });

    try {
      const restRes = await fetch(`${baseUrl}/rest/v1/agents?limit=1`, {
        headers: {
          Prefer: "count=exact",
        },
      });
      expect(restRes.status).toBe(200);
      expect(restRes.headers.get("content-range")).toBe("items 0-0/1");
      expect(await restRes.json()).toEqual([{ asset: "ProxyAgent11111111111111111111111111111111" }]);

      const upstreamCall = fetchSpy.mock.calls.find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/agents?");
      });
      expect(upstreamCall).toBeTruthy();
      const upstreamUrlRaw = typeof upstreamCall?.[0] === "string"
        ? upstreamCall[0]
        : upstreamCall?.[0] instanceof URL
          ? upstreamCall[0].toString()
          : upstreamCall?.[0]?.url;
      expect(typeof upstreamUrlRaw).toBe("string");
      const upstreamUrl = new URL(upstreamUrlRaw as string);
      expect(upstreamUrl.pathname).toBe("/agents");
      expect(upstreamUrl.searchParams.get("limit")).toBe("1");
      expect(upstreamUrl.searchParams.get("status")).toBe("neq.ORPHANED");
      const upstreamInit = upstreamCall?.[1] as RequestInit | undefined;
      const forwardedHeaders = new Headers(upstreamInit?.headers);
      expect(forwardedHeaders.get("prefer")).toBe("count=exact");
      expect(forwardedHeaders.get("apikey")).toBe("alias-service-role-key");
      expect(forwardedHeaders.get("authorization")).toBe("Bearer alias-service-role-key");

      await waitForGraphqlMount(baseUrl);
      const gqlRes = await fetch(`${baseUrl}/v2/graphql`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ query: "{ stats { totalAgents } }" }),
      });
      expect(gqlRes.status).toBe(200);
    } finally {
      await stopServer(server);
    }
  });

  it("falls back to derived stats when /global_stats relation is missing in REST proxy mode", async () => {
    process.env.POSTGREST_URL = "https://proxy-test.supabase.co";
    process.env.POSTGREST_TOKEN = "alias-service-role-key";
    const realFetch = globalThis.fetch;
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input: any, init?: any) => {
      const url = typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : input?.url;

      if (typeof url === "string") {
        const parsed = new URL(url);
        if (parsed.pathname === "/global_stats") {
          return new Response(
            JSON.stringify({ code: "42P01", message: "relation \"public.global_stats\" does not exist" }),
            { status: 404, headers: { "content-type": "application/json" } }
          );
        }
        if (parsed.pathname === "/agents") {
          return new Response(
            JSON.stringify([{ asset: "ProxyAgentFallback11111111111111111111111111" }]),
            { status: 206, headers: { "content-type": "application/json", "content-range": "0-0/10" } }
          );
        }
        if (parsed.pathname === "/feedbacks") {
          return new Response(
            JSON.stringify([{ id: "feedback-1" }]),
            { status: 206, headers: { "content-type": "application/json", "content-range": "0-0/7" } }
          );
        }
        if (parsed.pathname === "/collections") {
          return new Response(
            JSON.stringify([{ collection: "c1:bafy..." }]),
            { status: 206, headers: { "content-type": "application/json", "content-range": "0-0/3" } }
          );
        }
      }

      return realFetch(input, init);
    });

    const { server, baseUrl } = await startServer({
      prisma: null as any,
      pool: { query: vi.fn() } as any,
    });

    try {
      const restRes = await fetch(`${baseUrl}/rest/v1/stats?limit=1`);
      expect(restRes.status).toBe(200);
      expect(await restRes.json()).toEqual([{
        total_agents: 10,
        total_feedbacks: 7,
        total_collections: 3,
      }]);

      const upstreamGlobalStatsCall = fetchSpy.mock.calls.find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string" && url.startsWith("https://proxy-test.supabase.co/global_stats");
      });
      expect(upstreamGlobalStatsCall).toBeTruthy();

      const agentCountCall = fetchSpy.mock.calls.find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        if (typeof url !== "string") return false;
        const parsed = new URL(url);
        return parsed.pathname === "/agents" && parsed.searchParams.get("select") === "asset";
      });
      expect(agentCountCall).toBeTruthy();
      const agentCountUrl = new URL(
        (typeof agentCountCall?.[0] === "string"
          ? agentCountCall?.[0]
          : agentCountCall?.[0] instanceof URL
            ? agentCountCall?.[0].toString()
            : agentCountCall?.[0]?.url) as string
      );
      expect(agentCountUrl.searchParams.get("status")).toBe("neq.ORPHANED");
      const agentCountInit = agentCountCall?.[1] as RequestInit | undefined;
      const agentCountHeaders = new Headers(agentCountInit?.headers);
      expect(agentCountHeaders.get("prefer")).toBe("count=exact");
      expect(agentCountHeaders.get("range")).toBe("0-0");

      const feedbackCountCall = fetchSpy.mock.calls.find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        if (typeof url !== "string") return false;
        const parsed = new URL(url);
        return parsed.pathname === "/feedbacks" && parsed.searchParams.get("select") === "id";
      });
      expect(feedbackCountCall).toBeTruthy();
      const feedbackCountUrl = new URL(
        (typeof feedbackCountCall?.[0] === "string"
          ? feedbackCountCall?.[0]
          : feedbackCountCall?.[0] instanceof URL
            ? feedbackCountCall?.[0].toString()
            : feedbackCountCall?.[0]?.url) as string
      );
      expect(feedbackCountUrl.searchParams.get("status")).toBe("neq.ORPHANED");

      const collectionCountCall = fetchSpy.mock.calls.find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        if (typeof url !== "string") return false;
        const parsed = new URL(url);
        return parsed.pathname === "/collections" && parsed.searchParams.get("select") === "collection";
      });
      expect(collectionCountCall).toBeTruthy();
      const collectionCountUrl = new URL(
        (typeof collectionCountCall?.[0] === "string"
          ? collectionCountCall?.[0]
          : collectionCountCall?.[0] instanceof URL
            ? collectionCountCall?.[0].toString()
            : collectionCountCall?.[0]?.url) as string
      );
      expect(collectionCountUrl.searchParams.get("registry_type")).toBe("neq.BASE");
      expect(collectionCountUrl.searchParams.get("status")).toBe("neq.ORPHANED");

      await waitForGraphqlMount(baseUrl);
      const gqlRes = await fetch(`${baseUrl}/v2/graphql`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ query: "{ stats { totalAgents } }" }),
      });
      expect(gqlRes.status).toBe(200);
    } finally {
      await stopServer(server);
    }
  });
});
