import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AddressInfo } from "net";
import type { Server } from "http";
import type { Express } from "express";

vi.mock("../../../src/api/graphql/index.js", () => ({
  createGraphQLHandler: vi.fn(() => ({
    handle: (_req: unknown, res: { status: (code: number) => { json: (body: unknown) => void } }) => {
      res.status(200).json({ data: { ok: true } });
    },
  })),
}));

const originalEnv = process.env;
let server: Server | null = null;
let baseUrl = "";

async function listen(app: Express): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server = app.listen(0, "127.0.0.1", () => {
      const addr = server!.address() as AddressInfo;
      baseUrl = `http://127.0.0.1:${addr.port}`;
      resolve();
    });
    server.on("error", reject);
  });
}

async function closeServer(): Promise<void> {
  if (!server) {
    return;
  }
  await new Promise<void>((resolve, reject) => {
    server!.close((err) => (err ? reject(err) : resolve()));
  });
  server = null;
}

describe("metrics endpoint", () => {
  beforeEach(() => {
    vi.resetModules();
    process.env = { ...originalEnv };
  });

  afterEach(async () => {
    await closeServer();
    process.env = originalEnv;
  });

  it("is disabled by default", async () => {
    const { createApiServer } = await import("../../../src/api/server.js");
    const app = createApiServer({ pool: {} as any, prisma: null });
    await listen(app);

    const res = await fetch(`${baseUrl}/metrics`);
    expect(res.status).toBe(404);
  });

  it("serves integrity metrics when enabled", async () => {
    process.env.METRICS_ENDPOINT_ENABLED = "true";

    const {
      incrementVerifyCycles,
      resetIntegrityMetrics,
      setLastVerifiedSlot,
      setMismatchCount,
      setOrphanCount,
      setVerifierActive,
    } = await import("../../../src/observability/integrity-metrics.js");

    resetIntegrityMetrics();
    incrementVerifyCycles();
    setMismatchCount(2);
    setOrphanCount(3);
    setLastVerifiedSlot(456n);
    setVerifierActive(true);

    const { createApiServer } = await import("../../../src/api/server.js");
    const app = createApiServer({ pool: {} as any, prisma: null });
    await listen(app);

    const res = await fetch(`${baseUrl}/metrics`);
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("text/plain");

    const body = await res.text();
    expect(body).toContain("integrity_verify_cycles_total 1");
    expect(body).toContain("integrity_mismatch_count 2");
    expect(body).toContain("integrity_orphan_count 3");
    expect(body).toContain("integrity_last_verified_slot 456");
    expect(body).toContain("integrity_verifier_active 1");
  });
});
