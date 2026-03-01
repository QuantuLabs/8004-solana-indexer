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

const originalEnv = process.env;
const ARCHIVED_VALIDATIONS_ERROR =
  "Validation endpoints are archived and no longer exposed. /rest/v1/validations has been retired.";

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
          Authorization: "Bearer test-token",
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
      expect(includeOrphanedUrl.searchParams.get("includeOrphaned")).toBe("true");
      expect(includeOrphanedUrl.searchParams.get("status")).toBeNull();

      const statusDefaultProxyPaths = ["/feedbacks", "/feedback_responses", "/revocations"];
      for (const path of statusDefaultProxyPaths) {
        const defaultCallStart = fetchSpy.mock.calls.length;
        const defaultRes = await fetch(`${baseUrl}/rest/v1${path}?limit=1`);
        expect(defaultRes.status).toBe(200);
        const defaultUpstreamCall = fetchSpy.mock.calls.slice(defaultCallStart).find(([input]) => {
          const url = typeof input === "string"
            ? input
            : input instanceof URL
              ? input.toString()
              : input?.url;
          return typeof url === "string" && url.startsWith(`https://proxy-test.supabase.co/rest/v1${path}?`);
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
          return typeof url === "string" && url.startsWith(`https://proxy-test.supabase.co/rest/v1${path}?`);
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

      const collectionCountRes = await fetch(
        `${baseUrl}/rest/v1/collection_asset_count?collection=eq.Collection11111111111111111111111111111111`
      );
      expect(collectionCountRes.status).toBe(200);
      expect(await collectionCountRes.json()).toEqual({
        collection: "Collection11111111111111111111111111111111",
        creator: null,
        asset_count: 1,
      });

      const collectionAssetsRes = await fetch(
        `${baseUrl}/rest/v1/collection_assets?collection=eq.Collection11111111111111111111111111111111&limit=1`
      );
      expect(collectionAssetsRes.status).toBe(200);
      expect(await collectionAssetsRes.json()).toEqual([{ asset: "ProxyAgent11111111111111111111111111111111" }]);

      const collectionCompatUpstreamCall = fetchSpy.mock.calls.find(([input]) => {
        const url = typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input?.url;
        return typeof url === "string"
          && url.startsWith("https://proxy-test.supabase.co/rest/v1/agents?")
          && url.includes("collection_pointer=eq.Collection11111111111111111111111111111111");
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
});
