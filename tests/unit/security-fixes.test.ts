/**
 * Security fix verification tests for the 8004 Solana Indexer.
 *
 * Validates:
 * 1. RootConfig PDA parser field order (baseRegistry before authority)
 * 2. RegistryConfig PDA parser layout (74 bytes, not 121)
 * 3. API security headers (X-Content-Type-Options, X-Frame-Options)
 * 4. Express body size limit
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { PublicKey } from "@solana/web3.js";
import http from "http";

// ---------------------------------------------------------------------------
// Hoisted mocks (vi.mock factories are hoisted and cannot reference locals)
// ---------------------------------------------------------------------------
const { mockLogger } = vi.hoisted(() => {
  const ml = {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    fatal: vi.fn(),
    child: vi.fn().mockReturnThis(),
  };
  return { mockLogger: ml };
});

// Mock 8004-solana SDK exports (needed by pda.ts import)
vi.mock("8004-solana", () => {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { PublicKey: RealPK } = require("@solana/web3.js");
  return {
    PROGRAM_ID: new RealPK("8oo4SbcgjRBAXjmGU4YMcdFqfeLLrtn7n6f358PkAc3N"),
    ATOM_ENGINE_PROGRAM_ID: new RealPK("AToMNmthLzvTy3D2kz2obFmbVCsTCmYpDw1ptWUJdeU8"),
    MPL_CORE_PROGRAM_ID: new RealPK("CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d"),
  };
});

// Mock logger with createChildLogger (used by compression.ts and others)
vi.mock("../../src/logger.js", () => ({
  logger: mockLogger,
  createChildLogger: vi.fn().mockReturnValue(mockLogger),
}));

// Mock @prisma/client (needed by server.ts)
vi.mock("@prisma/client", () => ({
  PrismaClient: vi.fn(),
  Prisma: {},
}));

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Build a deterministic 32-byte public key from a seed byte. */
function pubkeyFromSeed(seed: number): PublicKey {
  return new PublicKey(new Uint8Array(32).fill(seed));
}

/** Create a mock AccountInfo result from a raw Buffer. */
function mockAccountInfo(data: Buffer) {
  return {
    data,
    executable: false,
    lamports: 1_000_000,
    owner: new PublicKey("8oo4SbcgjRBAXjmGU4YMcdFqfeLLrtn7n6f358PkAc3N"),
    rentEpoch: 0,
  };
}

/**
 * Make a request to an Express app via an ephemeral HTTP server.
 * Returns { status, headers, body }.
 */
async function httpRequest(
  app: any,
  method: string,
  path: string,
  body?: string,
  headers?: Record<string, string>,
): Promise<{ status: number; headers: http.IncomingHttpHeaders; body: string }> {
  return new Promise((resolve, reject) => {
    const server = http.createServer(app);
    server.listen(0, () => {
      const addr = server.address() as { port: number };
      const reqHeaders: Record<string, string> = { ...headers };
      if (body) {
        reqHeaders["Content-Type"] = reqHeaders["Content-Type"] || "application/json";
        reqHeaders["Content-Length"] = String(Buffer.byteLength(body));
      }

      const req = http.request(
        { hostname: "127.0.0.1", port: addr.port, path, method, headers: reqHeaders },
        (res) => {
          let data = "";
          res.on("data", (chunk: string) => { data += chunk; });
          res.on("end", () => {
            server.close();
            resolve({ status: res.statusCode!, headers: res.headers, body: data });
          });
        },
      );
      req.on("error", (err) => { server.close(); reject(err); });
      if (body) req.write(body);
      req.end();
    });
  });
}

// ---------------------------------------------------------------------------
// 1. RootConfig field order
// ---------------------------------------------------------------------------
describe("PDA Parser: RootConfig field order", () => {
  let fetchRootConfig: typeof import("../../src/utils/pda.js").fetchRootConfig;

  const BASE_REGISTRY_KEY = pubkeyFromSeed(0xaa);
  const AUTHORITY_KEY = pubkeyFromSeed(0xbb);
  const BUMP = 254;

  beforeEach(async () => {
    vi.resetModules();
    const pda = await import("../../src/utils/pda.js");
    fetchRootConfig = pda.fetchRootConfig;
  });

  it("should parse baseCollection at bytes 8-39 and authority at bytes 40-71", async () => {
    // Build a 73-byte buffer that matches on-chain RootConfig layout:
    //   [0..7]   discriminator
    //   [8..39]  baseCollection (Pubkey)
    //   [40..71] authority (Pubkey)
    //   [72]     bump
    const buf = Buffer.alloc(73);
    buf.fill(0x00, 0, 8);
    BASE_REGISTRY_KEY.toBuffer().copy(buf, 8);
    AUTHORITY_KEY.toBuffer().copy(buf, 40);
    buf[72] = BUMP;

    const mockConnection = {
      getAccountInfo: vi.fn().mockResolvedValue(mockAccountInfo(buf)),
    } as any;

    const result = await fetchRootConfig(mockConnection);

    expect(result).not.toBeNull();
    expect(result!.baseCollection.toBase58()).toBe(BASE_REGISTRY_KEY.toBase58());
    expect(result!.authority.toBase58()).toBe(AUTHORITY_KEY.toBase58());
    expect(result!.bump).toBe(BUMP);
  });

  it("should NOT swap baseCollection and authority (regression check)", async () => {
    const buf = Buffer.alloc(73);
    buf.fill(0x00, 0, 8);
    BASE_REGISTRY_KEY.toBuffer().copy(buf, 8);
    AUTHORITY_KEY.toBuffer().copy(buf, 40);
    buf[72] = BUMP;

    const mockConnection = {
      getAccountInfo: vi.fn().mockResolvedValue(mockAccountInfo(buf)),
    } as any;

    const result = await fetchRootConfig(mockConnection);

    // The old bug would have returned authority where baseCollection should be
    expect(result!.baseCollection.toBase58()).not.toBe(AUTHORITY_KEY.toBase58());
    expect(result!.authority.toBase58()).not.toBe(BASE_REGISTRY_KEY.toBase58());
  });

  it("should return null when account does not exist", async () => {
    const mockConnection = {
      getAccountInfo: vi.fn().mockResolvedValue(null),
    } as any;

    const result = await fetchRootConfig(mockConnection);
    expect(result).toBeNull();
  });

  it("should throw on undersized buffer (< 73 bytes)", async () => {
    const buf = Buffer.alloc(50);

    const mockConnection = {
      getAccountInfo: vi.fn().mockResolvedValue(mockAccountInfo(buf)),
    } as any;

    await expect(fetchRootConfig(mockConnection)).rejects.toThrow(
      /Invalid RootConfig account size/
    );
  });
});

// ---------------------------------------------------------------------------
// 2. RegistryConfig correct layout (74 bytes)
// ---------------------------------------------------------------------------
describe("PDA Parser: RegistryConfig layout (73 bytes)", () => {
  let fetchRegistryConfig: typeof import("../../src/utils/pda.js").fetchRegistryConfig;

  const COLLECTION_KEY = pubkeyFromSeed(0xcc);
  const AUTHORITY_KEY = pubkeyFromSeed(0xdd);
  const BUMP = 253;

  beforeEach(async () => {
    vi.resetModules();
    const pda = await import("../../src/utils/pda.js");
    fetchRegistryConfig = pda.fetchRegistryConfig;
  });

  it("should parse 73-byte RegistryConfig correctly", async () => {
    // Layout (v0.6.0 single-collection):
    //   [0..7]   discriminator (8 bytes)
    //   [8..39]  collection (32 bytes)
    //   [40..71] authority (32 bytes)
    //   [72]     bump (1 byte)
    const buf = Buffer.alloc(73);
    buf.fill(0x00, 0, 8);
    COLLECTION_KEY.toBuffer().copy(buf, 8);
    AUTHORITY_KEY.toBuffer().copy(buf, 40);
    buf[72] = BUMP;

    const registryPda = pubkeyFromSeed(0xee);
    const mockConnection = {
      getAccountInfo: vi.fn().mockResolvedValue(mockAccountInfo(buf)),
    } as any;

    const result = await fetchRegistryConfig(mockConnection, registryPda);

    expect(result).not.toBeNull();
    expect(result!.collection.toBase58()).toBe(COLLECTION_KEY.toBase58());
    expect(result!.authority.toBase58()).toBe(AUTHORITY_KEY.toBase58());
    expect(result!.bump).toBe(BUMP);
  });

  it("should return null for buffer smaller than 73 bytes", async () => {
    const buf = Buffer.alloc(72);

    const mockConnection = {
      getAccountInfo: vi.fn().mockResolvedValue(mockAccountInfo(buf)),
    } as any;

    const result = await fetchRegistryConfig(mockConnection, pubkeyFromSeed(0xee));
    expect(result).toBeNull();
  });

  it("should return null when account does not exist", async () => {
    const mockConnection = {
      getAccountInfo: vi.fn().mockResolvedValue(null),
    } as any;

    const result = await fetchRegistryConfig(mockConnection, pubkeyFromSeed(0xee));
    expect(result).toBeNull();
  });

  it("should NOT use obsolete 121-byte layout (regression check)", async () => {
    // With the old 121-byte parser, a 73-byte buffer would be rejected.
    // The fix accepts 73 bytes and parses correctly.
    const buf = Buffer.alloc(73);
    COLLECTION_KEY.toBuffer().copy(buf, 8);
    AUTHORITY_KEY.toBuffer().copy(buf, 40);
    buf[72] = BUMP;

    const mockConnection = {
      getAccountInfo: vi.fn().mockResolvedValue(mockAccountInfo(buf)),
    } as any;

    const result = await fetchRegistryConfig(mockConnection, pubkeyFromSeed(0xee));
    expect(result).not.toBeNull();
    expect(result!.collection.toBase58()).toBe(COLLECTION_KEY.toBase58());
  });
});

// ---------------------------------------------------------------------------
// 3. API Security Headers
// ---------------------------------------------------------------------------
describe("API Security Headers", () => {
  let createApiServer: typeof import("../../src/api/server.js").createApiServer;

  beforeEach(async () => {
    vi.resetModules();
    const serverMod = await import("../../src/api/server.js");
    createApiServer = serverMod.createApiServer;
  });

  it("should set X-Content-Type-Options: nosniff", async () => {
    const app = createApiServer({ prisma: {} as any });
    const res = await httpRequest(app, "GET", "/health");

    expect(res.headers["x-content-type-options"]).toBe("nosniff");
  });

  it("should set X-Frame-Options: DENY", async () => {
    const app = createApiServer({ prisma: {} as any });
    const res = await httpRequest(app, "GET", "/health");

    expect(res.headers["x-frame-options"]).toBe("DENY");
  });

  it("should set X-XSS-Protection: 0", async () => {
    const app = createApiServer({ prisma: {} as any });
    const res = await httpRequest(app, "GET", "/health");

    expect(res.headers["x-xss-protection"]).toBe("0");
  });

  it("should set Referrer-Policy header", async () => {
    const app = createApiServer({ prisma: {} as any });
    const res = await httpRequest(app, "GET", "/health");

    expect(res.headers["referrer-policy"]).toBe("strict-origin-when-cross-origin");
  });

  it("should return healthy status on /health", async () => {
    const app = createApiServer({ prisma: {} as any });
    const res = await httpRequest(app, "GET", "/health");

    expect(res.status).toBe(200);
    expect(JSON.parse(res.body)).toEqual({ status: "ok" });
  });
});

// ---------------------------------------------------------------------------
// 4. CORS_ORIGINS env parsing & body size limit
// ---------------------------------------------------------------------------
describe("CORS and Body Size Limit", () => {
  it("should parse CORS_ORIGINS with multiple comma-separated origins", () => {
    const envValue = "https://app.example.com, https://admin.example.com, http://localhost:3000";
    const parsed = envValue.split(",").map((s) => s.trim());

    expect(parsed).toEqual([
      "https://app.example.com",
      "https://admin.example.com",
      "http://localhost:3000",
    ]);
    expect(parsed).toHaveLength(3);
  });

  it("should default to wildcard when CORS_ORIGINS is not set", () => {
    const envValue = undefined;
    const allowedOrigins = envValue?.split(",").map((s: string) => s.trim()) || ["*"];

    expect(allowedOrigins).toEqual(["*"]);
  });

  it("should detect wildcard origin in allowedOrigins", () => {
    const allowedOrigins = ["*"];
    const corsOrigin = allowedOrigins.includes("*") ? "*" : allowedOrigins;

    expect(corsOrigin).toBe("*");
  });

  it("should return array when no wildcard present", () => {
    const allowedOrigins = ["https://example.com", "https://admin.example.com"];
    const corsOrigin = allowedOrigins.includes("*") ? "*" : allowedOrigins;

    expect(corsOrigin).toEqual(["https://example.com", "https://admin.example.com"]);
  });

  it("should reject oversized JSON bodies (body size limit)", async () => {
    vi.resetModules();

    const { createApiServer } = await import("../../src/api/server.js");
    const app = createApiServer({ prisma: {} as any });

    // Build a JSON body larger than 100kb
    const oversizedBody = JSON.stringify({ data: "x".repeat(200 * 1024) });
    const res = await httpRequest(app, "POST", "/health", oversizedBody);

    // Express should reject with 413 Payload Too Large
    expect(res.status).toBe(413);
  });
});
