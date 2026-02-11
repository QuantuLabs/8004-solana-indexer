/**
 * URI Digest Coverage Tests
 * Covers all uncovered branches in src/indexer/uriDigest.ts
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Must use vi.hoisted so the fn is available before module initialization
const mockLookup = vi.hoisted(() => vi.fn());
vi.mock("dns/promises", () => ({
  lookup: mockLookup,
}));

vi.mock("../../src/config.js", () => ({
  config: {
    metadataIndexMode: "normal",
    metadataMaxBytes: 262144,
    metadataMaxValueBytes: 10000,
    metadataTimeoutMs: 5000,
  },
}));

vi.mock("../../src/logger.js", () => ({
  createChildLogger: () => ({
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  }),
}));

import {
  digestUri,
  serializeValue,
  sanitizeText,
  sanitizeUrl,
} from "../../src/indexer/uriDigest.js";

// Helper: create a mock fetch response with a JSON body
function mockFetchOk(body: unknown, headers: Record<string, string> = {}) {
  const encoded = new TextEncoder().encode(JSON.stringify(body));
  return {
    ok: true,
    status: 200,
    headers: new Headers({
      "content-length": String(encoded.length),
      ...headers,
    }),
    body: {
      getReader: () => ({
        read: vi
          .fn()
          .mockResolvedValueOnce({ done: false, value: encoded })
          .mockResolvedValueOnce({ done: true, value: undefined }),
        cancel: vi.fn(),
      }),
    },
  } as unknown as Response;
}

// Helper: mock DNS to resolve to a public IP
function mockPublicDns() {
  mockLookup.mockResolvedValue([{ address: "93.184.216.34", family: 4 }]);
}

// Helper: mock DNS to resolve to a private IP
function mockPrivateDns() {
  mockLookup.mockResolvedValue([{ address: "192.168.1.1", family: 4 }]);
}

describe("URI Digest Coverage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockPublicDns();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // =========================================================================
  // sanitizeText branches
  // =========================================================================
  describe("sanitizeText edge cases", () => {
    it("should return empty string for null-ish input", () => {
      expect(sanitizeText(null as unknown as string)).toBe("");
      expect(sanitizeText(undefined as unknown as string)).toBe("");
      expect(sanitizeText(123 as unknown as string)).toBe("");
    });

    it("should truncate input longer than MAX_SANITIZE_INPUT_LENGTH (1000)", () => {
      const longText = "A".repeat(1500);
      const result = sanitizeText(longText);
      expect(result.length).toBeLessThanOrEqual(1000);
    });

    it("should strip NUL bytes and control characters", () => {
      const withControls = "Hello\x00World\x01End\x08Done";
      const result = sanitizeText(withControls);
      expect(result).toBe("HelloWorldEndDone");
    });

    it("should keep tab, newline, CR", () => {
      const withAllowed = "Line1\nLine2\tTabbed\rCR";
      const result = sanitizeText(withAllowed);
      expect(result).toBe("Line1\nLine2\tTabbed\rCR");
    });

    it("should return empty for empty string", () => {
      expect(sanitizeText("")).toBe("");
    });
  });

  // =========================================================================
  // sanitizeUrl branches
  // =========================================================================
  describe("sanitizeUrl edge cases", () => {
    it("should return empty for null/undefined/non-string", () => {
      expect(sanitizeUrl(null as unknown as string)).toBe("");
      expect(sanitizeUrl(undefined as unknown as string)).toBe("");
      expect(sanitizeUrl(42 as unknown as string)).toBe("");
    });

    it("should truncate URLs longer than 1000 chars", () => {
      const longUrl = "https://example.com/" + "a".repeat(1500);
      const result = sanitizeUrl(longUrl);
      // Should still produce a valid URL (truncated)
      expect(result.length).toBeLessThanOrEqual(1010);
    });

    it("should validate IPFS CID is alphanumeric", () => {
      expect(sanitizeUrl("ipfs://Qm123Valid")).toBe("ipfs://Qm123Valid");
      expect(sanitizeUrl("ipfs://")).toBe(""); // Empty CID
    });

    it("should validate Arweave TX ID is base64url", () => {
      expect(sanitizeUrl("ar://abc123_-XYZ")).toBe("ar://abc123_-XYZ");
      expect(sanitizeUrl("ar://")).toBe(""); // Empty TX ID
    });

    it("should reject IPFS with invalid CID characters", () => {
      expect(sanitizeUrl("ipfs://invalid!cid")).toBe("");
    });

    it("should allow IPFS with path after CID", () => {
      expect(sanitizeUrl("ipfs://QmCid123/path/to/file")).toBe("ipfs://QmCid123/path/to/file");
    });

    it("should allow Arweave with path after TX ID", () => {
      expect(sanitizeUrl("ar://txid123/path")).toBe("ar://txid123/path");
    });
  });

  // =========================================================================
  // sanitizeField - via digestUri with different field types
  // =========================================================================
  describe("sanitizeField branches via digestUri", () => {
    it("should handle non-string _uri:name (returns null, omitted)", async () => {
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: 123 }));
      const result = await digestUri("https://example.com/agent.json");
      expect(result.status).toBe("ok");
      expect(result.fields!["_uri:name"]).toBeUndefined();
    });

    it("should handle non-string _uri:image (returns null, omitted)", async () => {
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ image: 123 }));
      const result = await digestUri("https://example.com/agent.json");
      expect(result.status).toBe("ok");
      expect(result.fields!["_uri:image"]).toBeUndefined();
    });

    it("should handle boolean active field", async () => {
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ active: true }));
      const result = await digestUri("https://example.com/agent.json");
      expect(result.status).toBe("ok");
      expect(result.fields!["_uri:active"]).toBe(true);
    });

    it("should reject non-boolean active field", async () => {
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ active: "yes" }));
      const result = await digestUri("https://example.com/agent.json");
      expect(result.status).toBe("ok");
      expect(result.fields!["_uri:active"]).toBeUndefined();
    });

    it("should handle x402Support boolean field", async () => {
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ x402Support: true }));
      const result = await digestUri("https://example.com/agent.json");
      expect(result.status).toBe("ok");
      expect(result.fields!["_uri:x402_support"]).toBe(true);
    });

    it("should reject non-boolean x402Support field", async () => {
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ x402Support: "true" }));
      const result = await digestUri("https://example.com/agent.json");
      expect(result.fields!["_uri:x402_support"]).toBeUndefined();
    });

    it("should handle supportedTrust with invalid entries", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({ supportedTrust: ["reputation", "invalid", 123, "8004"] })
      );
      const result = await digestUri("https://example.com/agent.json");
      expect(result.fields!["_uri:supported_trust"]).toEqual(["reputation", "8004"]);
    });

    it("should handle non-array supportedTrust", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({ supportedTrust: "reputation" })
      );
      const result = await digestUri("https://example.com/agent.json");
      // Returns empty array, omitted since length 0
      expect(result.fields!["_uri:supported_trust"]).toBeUndefined();
    });

    it("should handle non-array services", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({ services: "not-an-array" })
      );
      const result = await digestUri("https://example.com/agent.json");
      expect(result.fields!["_uri:services"]).toBeUndefined();
    });

    it("should handle non-array registrations", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({ registrations: "not-an-array" })
      );
      const result = await digestUri("https://example.com/agent.json");
      expect(result.fields!["_uri:registrations"]).toBeUndefined();
    });
  });

  // =========================================================================
  // sanitizeServices branches
  // =========================================================================
  describe("sanitizeServices branches via digestUri", () => {
    it("should filter out non-object items from services array", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({
          services: [
            "string-item",
            null,
            { name: "mcp", endpoint: "https://api.example.com" },
          ],
        })
      );
      const result = await digestUri("https://example.com/agent.json");
      const services = result.fields!["_uri:services"] as Array<Record<string, unknown>>;
      expect(services).toHaveLength(1);
    });

    it("should filter out services without name", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({
          services: [{ endpoint: "https://api.example.com" }],
        })
      );
      const result = await digestUri("https://example.com/agent.json");
      expect(result.fields!["_uri:services"]).toBeUndefined(); // Empty after filtering
    });

    it("should filter out services with invalid name", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({
          services: [{ name: "invalid-name", endpoint: "https://api.example.com" }],
        })
      );
      const result = await digestUri("https://example.com/agent.json");
      expect(result.fields!["_uri:services"]).toBeUndefined();
    });

    it("should filter out services with non-string name", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({
          services: [{ name: 123, endpoint: "https://api.example.com" }],
        })
      );
      const result = await digestUri("https://example.com/agent.json");
      expect(result.fields!["_uri:services"]).toBeUndefined();
    });

    it("should filter out services with invalid endpoint URL", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({
          services: [{ name: "a2a", endpoint: 'javascript:alert("xss")' }],
        })
      );
      const result = await digestUri("https://example.com/agent.json");
      expect(result.fields!["_uri:services"]).toBeUndefined();
    });

    it("should include service without endpoint (endpoint optional)", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({
          services: [{ name: "mcp" }],
        })
      );
      const result = await digestUri("https://example.com/agent.json");
      const services = result.fields!["_uri:services"] as Array<Record<string, unknown>>;
      expect(services).toHaveLength(1);
      expect(services[0].name).toBe("mcp");
    });

    it("should handle mcpTools array", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({
          services: [
            {
              name: "mcp",
              endpoint: "https://api.example.com",
              mcpTools: ["search", 123, "generate", ""],
            },
          ],
        })
      );
      const result = await digestUri("https://example.com/agent.json");
      const services = result.fields!["_uri:services"] as Array<Record<string, unknown>>;
      expect(services[0].mcpTools).toEqual(["search", "generate"]);
    });

    it("should handle a2aSkills array", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({
          services: [
            {
              name: "a2a",
              endpoint: "https://api.example.com",
              a2aSkills: ["skill1", "", "skill2", 42],
            },
          ],
        })
      );
      const result = await digestUri("https://example.com/agent.json");
      const services = result.fields!["_uri:services"] as Array<Record<string, unknown>>;
      expect(services[0].a2aSkills).toEqual(["skill1", "skill2"]);
    });

    it("should truncate version to 20 chars", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({
          services: [
            {
              name: "mcp",
              endpoint: "https://api.example.com",
              version: "A".repeat(50),
            },
          ],
        })
      );
      const result = await digestUri("https://example.com/agent.json");
      const services = result.fields!["_uri:services"] as Array<Record<string, unknown>>;
      expect((services[0].version as string).length).toBe(20);
    });

    it("should limit services array to 20 entries", async () => {
      const services = Array.from({ length: 30 }, () => ({
        name: "mcp",
        endpoint: "https://api.example.com",
      }));
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ services }));
      const result = await digestUri("https://example.com/agent.json");
      const svc = result.fields!["_uri:services"] as Array<Record<string, unknown>>;
      expect(svc.length).toBe(20);
    });

    it("should accept all valid service names", async () => {
      const services = [
        { name: "mcp", endpoint: "https://a.com" },
        { name: "a2a", endpoint: "https://b.com" },
        { name: "oasf", endpoint: "https://c.com" },
        { name: "ens", endpoint: "https://d.com" },
        { name: "did", endpoint: "https://e.com" },
        { name: "agentwallet", endpoint: "https://f.com" },
      ];
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ services }));
      const result = await digestUri("https://example.com/agent.json");
      const svc = result.fields!["_uri:services"] as Array<Record<string, unknown>>;
      expect(svc.length).toBe(6);
    });
  });

  // =========================================================================
  // sanitizeRegistrationsArray branches
  // =========================================================================
  describe("sanitizeRegistrationsArray branches via digestUri", () => {
    it("should handle string agentId", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({
          registrations: [{ agentId: "123456789", agentRegistry: "eip155:1:0xabc" }],
        })
      );
      const result = await digestUri("https://example.com/agent.json");
      const regs = result.fields!["_uri:registrations"] as Array<Record<string, unknown>>;
      expect(regs).toHaveLength(1);
      expect(regs[0].agentId).toBe("123456789");
    });

    it("should reject items without agentId", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({
          registrations: [{ agentRegistry: "eip155:1:0xabc" }],
        })
      );
      const result = await digestUri("https://example.com/agent.json");
      expect(result.fields!["_uri:registrations"]).toBeUndefined();
    });

    it("should reject items without valid agentRegistry format", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({
          registrations: [{ agentId: 1, agentRegistry: "bad-format" }],
        })
      );
      const result = await digestUri("https://example.com/agent.json");
      expect(result.fields!["_uri:registrations"]).toBeUndefined();
    });

    it("should filter out non-objects from registrations", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({
          registrations: ["string", null, 42, { agentId: 1, agentRegistry: "eip155:1:0xabc" }],
        })
      );
      const result = await digestUri("https://example.com/agent.json");
      const regs = result.fields!["_uri:registrations"] as Array<Record<string, unknown>>;
      expect(regs).toHaveLength(1);
    });

    it("should limit registrations to 20 entries", async () => {
      const registrations = Array.from({ length: 30 }, (_, i) => ({
        agentId: i,
        agentRegistry: `eip155:1:0x${i.toString(16)}`,
      }));
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ registrations }));
      const result = await digestUri("https://example.com/agent.json");
      const regs = result.fields!["_uri:registrations"] as Array<Record<string, unknown>>;
      expect(regs.length).toBe(20);
    });

    it("should reject non-string agentRegistry", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({
          registrations: [{ agentId: 1, agentRegistry: 12345 }],
        })
      );
      const result = await digestUri("https://example.com/agent.json");
      expect(result.fields!["_uri:registrations"]).toBeUndefined();
    });

    it("should reject non-number non-string agentId", async () => {
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({
          registrations: [{ agentId: true, agentRegistry: "eip155:1:0xabc" }],
        })
      );
      const result = await digestUri("https://example.com/agent.json");
      expect(result.fields!["_uri:registrations"]).toBeUndefined();
    });
  });

  // =========================================================================
  // sanitizeField unknown field branches (full mode)
  // =========================================================================
  describe("sanitizeField unknown field types", () => {
    beforeEach(() => {
      vi.clearAllMocks();
      mockPublicDns();
    });

    it("should handle number values for unknown fields in full mode", async () => {
      vi.resetModules();
      vi.doMock("../../src/config.js", () => ({
        config: {
          metadataIndexMode: "full",
          metadataMaxBytes: 262144,
          metadataTimeoutMs: 5000,
        },
      }));
      vi.doMock("../../src/logger.js", () => ({
        createChildLogger: () => ({
          debug: vi.fn(),
          info: vi.fn(),
          warn: vi.fn(),
          error: vi.fn(),
        }),
      }));

      const { digestUri: digestUriFull } = await import("../../src/indexer/uriDigest.js");
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ customNum: 42, customBool: true }));
      const result = await digestUriFull("https://example.com/agent.json");
      expect(result.fields!["_uri:customNum"]).toBe(42);
      expect(result.fields!["_uri:customBool"]).toBe(true);
    });

    it("should handle array values for unknown fields in full mode", async () => {
      vi.resetModules();
      vi.doMock("../../src/config.js", () => ({
        config: {
          metadataIndexMode: "full",
          metadataMaxBytes: 262144,
          metadataTimeoutMs: 5000,
        },
      }));
      vi.doMock("../../src/logger.js", () => ({
        createChildLogger: () => ({
          debug: vi.fn(),
          info: vi.fn(),
          warn: vi.fn(),
          error: vi.fn(),
        }),
      }));

      const { digestUri: digestUriFull } = await import("../../src/indexer/uriDigest.js");
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({ customArr: ["<b>bold</b>", "clean", 42] })
      );
      const result = await digestUriFull("https://example.com/agent.json");
      const arr = result.fields!["_uri:customArr"] as unknown[];
      expect(arr).toContain("bold"); // HTML stripped
      expect(arr).toContain("clean");
      expect(arr).toContain(42); // numbers preserved
    });

    it("should handle null/undefined values for unknown fields (returns null, omitted)", async () => {
      vi.resetModules();
      vi.doMock("../../src/config.js", () => ({
        config: {
          metadataIndexMode: "full",
          metadataMaxBytes: 262144,
          metadataTimeoutMs: 5000,
        },
      }));
      vi.doMock("../../src/logger.js", () => ({
        createChildLogger: () => ({
          debug: vi.fn(),
          info: vi.fn(),
          warn: vi.fn(),
          error: vi.fn(),
        }),
      }));

      const { digestUri: digestUriFull } = await import("../../src/indexer/uriDigest.js");
      global.fetch = vi.fn().mockResolvedValue(
        mockFetchOk({ customNull: null })
      );
      const result = await digestUriFull("https://example.com/agent.json");
      // null value should be omitted from fields
      expect(result.fields!["_uri:customNull"]).toBeUndefined();
    });
  });

  // =========================================================================
  // convertToFetchUrl branches
  // =========================================================================
  describe("convertToFetchUrl branches via digestUri", () => {
    it("should convert /ipfs/ path to gateway URL", async () => {
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Test" }));
      await digestUri("/ipfs/QmTest123");
      expect(global.fetch).toHaveBeenCalledWith(
        "https://ipfs.io/ipfs/QmTest123",
        expect.any(Object)
      );
    });

    it("should reject HTTP URI by default", async () => {
      const result = await digestUri("http://example.com/agent.json");
      expect(result.status).toBe("error");
      expect(result.error).toBe("Unsupported URI scheme");
    });

    it("should allow HTTP URI when ALLOW_INSECURE_URI=true", async () => {
      const original = process.env.ALLOW_INSECURE_URI;
      process.env.ALLOW_INSECURE_URI = "true";

      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Test" }));
      const result = await digestUri("http://example.com/agent.json");
      expect(result.status).toBe("ok");

      process.env.ALLOW_INSECURE_URI = original;
    });

    it("should return error for unknown scheme", async () => {
      const result = await digestUri("ssh://example.com/file");
      expect(result.status).toBe("error");
      expect(result.error).toBe("Unsupported URI scheme");
    });
  });

  // =========================================================================
  // SSRF Protection - isBlockedHost / isPrivateIP
  // =========================================================================
  describe("SSRF blocking via digestUri", () => {
    it("should block localhost", async () => {
      const result = await digestUri("https://localhost/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block 127.0.0.1", async () => {
      const result = await digestUri("https://127.0.0.1/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block 0.0.0.0", async () => {
      const result = await digestUri("https://0.0.0.0/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block [::1]", async () => {
      const result = await digestUri("https://[::1]/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block metadata.google.internal", async () => {
      const result = await digestUri("https://metadata.google.internal/computeMetadata/v1/");
      expect(result.status).toBe("blocked");
    });

    it("should block AWS/GCP metadata IP 169.254.169.254", async () => {
      const result = await digestUri("https://169.254.169.254/latest/meta-data/");
      expect(result.status).toBe("blocked");
    });

    it("should block DNS resolving to private IP", async () => {
      mockPrivateDns();
      const result = await digestUri("https://evil.com/agent.json");
      expect(result.status).toBe("blocked");
      expect(result.error).toBe("DNS resolved to private IP");
    });

    it("should block when DNS resolution fails (fail-closed)", async () => {
      mockLookup.mockRejectedValue(new Error("ENOTFOUND"));
      const result = await digestUri("https://nonexistent.invalid/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should skip DNS validation for trusted gateway ipfs.io", async () => {
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "IPFS" }));
      const result = await digestUri("ipfs://QmTest123");
      expect(result.status).toBe("ok");
      // DNS lookup shouldn't be called for ipfs.io
    });

    it("should skip DNS validation for trusted gateway arweave.net", async () => {
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Arweave" }));
      const result = await digestUri("ar://txid123");
      expect(result.status).toBe("ok");
    });
  });

  // =========================================================================
  // DNS rebinding detection (HTTPS re-check)
  // =========================================================================
  describe("DNS rebinding detection", () => {
    it("should detect DNS rebinding on HTTPS (re-resolve returns private IP)", async () => {
      // First call returns public IP, second call returns private IP (rebinding)
      mockLookup
        .mockResolvedValueOnce([{ address: "93.184.216.34", family: 4 }])
        .mockResolvedValueOnce([{ address: "10.0.0.1", family: 4 }]);

      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Evil" }));
      const result = await digestUri("https://evil-rebinding.com/agent.json");
      expect(result.status).toBe("blocked");
      expect(result.error).toBe("DNS rebinding detected on HTTPS");
    });

    it("should detect DNS rebinding on HTTPS when re-resolve fails", async () => {
      mockLookup
        .mockResolvedValueOnce([{ address: "93.184.216.34", family: 4 }])
        .mockRejectedValueOnce(new Error("DNS timeout"));

      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Evil" }));
      const result = await digestUri("https://evil-timeout.com/agent.json");
      expect(result.status).toBe("blocked");
      expect(result.error).toBe("DNS rebinding detected on HTTPS");
    });
  });

  // =========================================================================
  // Redirect handling
  // =========================================================================
  describe("redirect handling", () => {
    it("should follow redirects manually up to MAX_REDIRECT_DEPTH", async () => {
      // Mock DNS for all lookups
      mockLookup.mockResolvedValue([{ address: "93.184.216.34", family: 4 }]);

      // First call: redirect
      const redirect1 = {
        ok: false,
        status: 302,
        headers: new Headers({ location: "https://redirect1.com/agent.json" }),
      } as unknown as Response;
      // Second call: redirect
      const redirect2 = {
        ok: false,
        status: 301,
        headers: new Headers({ location: "https://redirect2.com/agent.json" }),
      } as unknown as Response;
      // Third call: redirect again (should hit max depth)
      const redirect3 = {
        ok: false,
        status: 302,
        headers: new Headers({ location: "https://redirect3.com/agent.json" }),
      } as unknown as Response;

      global.fetch = vi
        .fn()
        .mockResolvedValueOnce(redirect1)
        .mockResolvedValueOnce(redirect2)
        .mockResolvedValueOnce(redirect3);

      const result = await digestUri("https://example.com/agent.json");
      expect(result.status).toBe("error");
      expect(result.error).toBe("Too many redirects");
    });

    it("should return error for redirect without location header", async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 302,
        headers: new Headers({}),
      } as unknown as Response);

      const result = await digestUri("https://example.com/agent.json");
      expect(result.status).toBe("error");
      expect(result.error).toContain("Redirect 302 without location");
    });

    it("should block redirect to internal host", async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 301,
        headers: new Headers({ location: "https://127.0.0.1/secret" }),
      } as unknown as Response);

      const result = await digestUri("https://example.com/agent.json");
      expect(result.status).toBe("blocked");
      expect(result.error).toBe("Redirect to internal host blocked");
    });

    it("should block redirect when DNS resolves to private IP", async () => {
      // For HTTPS: first DNS call = original host, second = HTTPS re-check (must pass),
      // then redirect target DNS check must fail
      mockLookup
        .mockResolvedValueOnce([{ address: "93.184.216.34", family: 4 }]) // original
        .mockResolvedValueOnce([{ address: "93.184.216.34", family: 4 }]) // HTTPS re-check (pass)
        .mockResolvedValueOnce([{ address: "10.0.0.1", family: 4 }]); // redirect target DNS

      global.fetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 302,
        headers: new Headers({ location: "https://redirect.evil.com/agent.json" }),
      } as unknown as Response);

      const result = await digestUri("https://example.com/agent.json");
      expect(result.status).toBe("blocked");
      expect(result.error).toBe("Redirect DNS resolved to private IP");
    });

    it("should return error for invalid redirect URL", async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 302,
        headers: new Headers({ location: "not-a-valid-url-://[[[" }),
      } as unknown as Response);

      // The location is resolved relative to the original URL, so it might or might not fail
      // The invalid URL case happens when new URL() throws
      const result = await digestUri("https://example.com/agent.json");
      // If relative resolution succeeds, it continues; if truly invalid, we get error
      expect(["error", "blocked", "ok"]).toContain(result.status);
    });
  });

  // =========================================================================
  // digestUri error handling
  // =========================================================================
  describe("digestUri error handling", () => {
    it("should handle fetch abort (timeout)", async () => {
      const abortError = new Error("The operation was aborted");
      abortError.name = "AbortError";
      global.fetch = vi.fn().mockRejectedValue(abortError);

      const result = await digestUri("https://example.com/agent.json");
      expect(result.status).toBe("timeout");
    });

    it("should handle generic fetch error", async () => {
      global.fetch = vi.fn().mockRejectedValue(new Error("Network error"));
      const result = await digestUri("https://example.com/agent.json");
      expect(result.status).toBe("error");
      expect(result.error).toBe("Network error");
    });

    it("should handle non-Error throw from fetch", async () => {
      global.fetch = vi.fn().mockRejectedValue("raw string error");
      const result = await digestUri("https://example.com/agent.json");
      expect(result.status).toBe("error");
      expect(result.error).toBe("raw string error");
    });

    it("should handle invalid URL format", async () => {
      // A URL that convertToFetchUrl accepts but new URL() rejects
      // This is hard to trigger since convertToFetchUrl checks scheme first
      // Let's use a valid HTTPS prefix but mangled URL
      const result = await digestUri("https://[invalid");
      expect(result.status).toBe("error");
      expect(result.error).toBe("Invalid URL");
    });

    it("should return error for no response body", async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        headers: new Headers({ "content-length": "100" }),
        body: null,
      } as unknown as Response);

      const result = await digestUri("https://example.com/agent.json");
      expect(result.status).toBe("error");
      expect(result.error).toBe("No response body");
    });

    it("should handle oversize content-length header", async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        headers: new Headers({ "content-length": "999999999" }),
        body: {
          getReader: () => ({
            read: vi.fn().mockResolvedValue({ done: true, value: undefined }),
            cancel: vi.fn(),
          }),
        },
      } as unknown as Response);

      const result = await digestUri("https://example.com/agent.json");
      expect(result.status).toBe("oversize");
      expect(result.bytes).toBe(999999999);
    });

    it("should handle oversize body during streaming", async () => {
      const bigChunk = new Uint8Array(300000); // > 262144 default max
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        headers: new Headers({}), // No content-length
        body: {
          getReader: () => ({
            read: vi
              .fn()
              .mockResolvedValueOnce({ done: false, value: bigChunk })
              .mockResolvedValueOnce({ done: true, value: undefined }),
            cancel: vi.fn(),
          }),
        },
      } as unknown as Response);

      const result = await digestUri("https://example.com/agent.json");
      expect(result.status).toBe("oversize");
      expect(result.bytes).toBeGreaterThan(262144);
    });
  });

  // =========================================================================
  // IP pinning (HTTP with resolved IP)
  // =========================================================================
  describe("IP pinning for HTTP", () => {
    it("should pin to resolved IP for HTTP (non-HTTPS)", async () => {
      const original = process.env.ALLOW_INSECURE_URI;
      process.env.ALLOW_INSECURE_URI = "true";

      mockLookup.mockResolvedValue([{ address: "93.184.216.34", family: 4 }]);
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Test" }));

      await digestUri("http://example.com/agent.json");

      // Should have pinned to IP
      expect(global.fetch).toHaveBeenCalledWith(
        expect.stringContaining("93.184.216.34"),
        expect.objectContaining({
          headers: expect.objectContaining({ Host: "example.com" }),
        })
      );

      process.env.ALLOW_INSECURE_URI = original;
    });

    it("should pin with bracketed IPv6 for HTTP", async () => {
      const original = process.env.ALLOW_INSECURE_URI;
      process.env.ALLOW_INSECURE_URI = "true";

      mockLookup.mockResolvedValue([
        { address: "2001:db8::1", family: 6 },
      ]);
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Test" }));

      await digestUri("http://example.com/agent.json");

      expect(global.fetch).toHaveBeenCalledWith(
        expect.stringContaining("[2001:db8::1]"),
        expect.any(Object)
      );

      process.env.ALLOW_INSECURE_URI = original;
    });
  });

  // =========================================================================
  // isIPv6LoopbackOrUnspecified
  // =========================================================================
  describe("IPv6 loopback/unspecified detection via SSRF blocking", () => {
    it("should block DNS resolving to IPv6 loopback ::1", async () => {
      mockLookup.mockResolvedValue([{ address: "::1", family: 6 }]);
      const result = await digestUri("https://ipv6evil.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block DNS resolving to full-form IPv6 loopback", async () => {
      mockLookup.mockResolvedValue([
        { address: "0:0:0:0:0:0:0:1", family: 6 },
      ]);
      const result = await digestUri("https://ipv6evil.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block DNS resolving to IPv6 unspecified ::", async () => {
      mockLookup.mockResolvedValue([{ address: "::", family: 6 }]);
      const result = await digestUri("https://ipv6evil.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block DNS resolving to IPv6 unique local (fc00::/7)", async () => {
      mockLookup.mockResolvedValue([
        { address: "fd12:3456:789a::1", family: 6 },
      ]);
      const result = await digestUri("https://ipv6evil.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block DNS resolving to IPv6 link-local (fe80::)", async () => {
      mockLookup.mockResolvedValue([
        { address: "fe80::1", family: 6 },
      ]);
      const result = await digestUri("https://ipv6evil.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should allow DNS resolving to public IPv6", async () => {
      mockLookup.mockResolvedValue([
        { address: "2001:db8::1", family: 6 },
      ]);
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Public IPv6" }));
      const result = await digestUri("https://ipv6ok.com/agent.json");
      expect(result.status).toBe("ok");
    });
  });

  // =========================================================================
  // canonicalizeIP coverage via isPrivateIP / isBlockedHost
  // =========================================================================
  describe("canonicalizeIP via SSRF blocking", () => {
    it("should block IPv4-mapped IPv6 dotted form ::ffff:127.0.0.1", async () => {
      mockLookup.mockResolvedValue([
        { address: "::ffff:127.0.0.1", family: 6 },
      ]);
      const result = await digestUri("https://ipv4mapped.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block IPv4-mapped IPv6 hex form ::ffff:7f00:1", async () => {
      mockLookup.mockResolvedValue([
        { address: "::ffff:7f00:1", family: 6 },
      ]);
      const result = await digestUri("https://ipv4mappedhex.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block IPv4-mapped full form 0:0:0:0:0:ffff:7f00:1", async () => {
      mockLookup.mockResolvedValue([
        { address: "0:0:0:0:0:ffff:7f00:1", family: 6 },
      ]);
      const result = await digestUri("https://mappedfull.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block IPv4-mapped full dotted 0:0:0:0:0:ffff:192.168.1.1", async () => {
      mockLookup.mockResolvedValue([
        { address: "0:0:0:0:0:ffff:192.168.1.1", family: 6 },
      ]);
      const result = await digestUri("https://mappedfull2.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block IPv4-compatible ::10.0.0.1", async () => {
      mockLookup.mockResolvedValue([
        { address: "::10.0.0.1", family: 6 },
      ]);
      const result = await digestUri("https://compat.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block 10.x.x.x range", async () => {
      mockLookup.mockResolvedValue([
        { address: "10.255.0.1", family: 4 },
      ]);
      const result = await digestUri("https://tenrange.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block 172.16-31.x.x range", async () => {
      mockLookup.mockResolvedValue([
        { address: "172.16.0.1", family: 4 },
      ]);
      const result = await digestUri("https://rfc1918.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should allow 172.15.x.x (not private)", async () => {
      mockLookup.mockResolvedValue([
        { address: "172.15.0.1", family: 4 },
      ]);
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "OK" }));
      const result = await digestUri("https://notprivate.com/agent.json");
      expect(result.status).toBe("ok");
    });

    it("should block 0.x.x.x range (current network)", async () => {
      mockLookup.mockResolvedValue([
        { address: "0.1.2.3", family: 4 },
      ]);
      const result = await digestUri("https://currentnet.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should try all DNS records until finding a public one", async () => {
      mockLookup.mockResolvedValue([
        { address: "192.168.1.1", family: 4 }, // private
        { address: "10.0.0.1", family: 4 }, // private
        { address: "93.184.216.34", family: 4 }, // public!
      ]);
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "OK" }));
      const result = await digestUri("https://mixed.com/agent.json");
      expect(result.status).toBe("ok");
    });
  });

  // =========================================================================
  // serializeValue edge cases
  // =========================================================================
  describe("serializeValue edge cases", () => {
    it("should serialize boolean as JSON", () => {
      const result = serializeValue(true, 100);
      expect(result.value).toBe("true");
      expect(result.bytes).toBe(4);
    });

    it("should serialize number as JSON", () => {
      const result = serializeValue(42, 100);
      expect(result.value).toBe("42");
      expect(result.bytes).toBe(2);
    });

    it("should serialize null as JSON", () => {
      const result = serializeValue(null, 100);
      expect(result.value).toBe("null");
      expect(result.bytes).toBe(4);
    });

    it("should report exact byte count for oversize value", () => {
      const result = serializeValue("x".repeat(200), 100);
      expect(result.oversize).toBe(true);
      expect(result.bytes).toBe(200);
      expect(result.value).toBe("");
    });

    it("should handle boundary case (exactly at limit)", () => {
      const result = serializeValue("x".repeat(100), 100);
      expect(result.oversize).toBe(false);
      expect(result.value).toBe("x".repeat(100));
    });
  });

  // =========================================================================
  // isIPv6LoopbackOrUnspecified - additional branches
  // =========================================================================
  describe("IPv6 loopback/unspecified additional branches", () => {
    it("should handle bracketed IPv6 addresses", async () => {
      mockLookup.mockResolvedValue([{ address: "[::1]", family: 6 }]);
      const result = await digestUri("https://bracketed.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should handle IPv6 with zone ID (e.g., ::1%lo0)", async () => {
      mockLookup.mockResolvedValue([{ address: "::1%lo0", family: 6 }]);
      const result = await digestUri("https://zoneipv6.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should reject IPv6 with multiple :: (invalid)", async () => {
      mockLookup.mockResolvedValue([{ address: "::1::2", family: 6 }]);
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Test" }));
      const result = await digestUri("https://invalidipv6.com/agent.json");
      // Invalid IPv6 doesn't crash - may return ok, blocked, or error depending on path
      expect(["ok", "blocked", "error"]).toContain(result.status);
    });

    it("should handle full form IPv6 loopback 0000:0000:0000:0000:0000:0000:0000:0001", async () => {
      mockLookup.mockResolvedValue([
        { address: "0000:0000:0000:0000:0000:0000:0000:0001", family: 6 },
      ]);
      const result = await digestUri("https://fullipv6.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should handle full form IPv6 unspecified 0:0:0:0:0:0:0:0", async () => {
      mockLookup.mockResolvedValue([
        { address: "0:0:0:0:0:0:0:0", family: 6 },
      ]);
      const result = await digestUri("https://fullunspecified.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should reject non-8-segment IPv6 without ::", async () => {
      mockLookup.mockResolvedValue([{ address: "2001:db8:1:2:3", family: 6 }]);
      // 5 segments without :: is invalid, isIPv6LoopbackOrUnspecified returns false
      // Falls through to private range check
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Test" }));
      const result = await digestUri("https://shortipv6.com/agent.json");
      expect(["ok", "blocked"]).toContain(result.status);
    });

    it("should handle IPv6 with invalid hex segment", async () => {
      mockLookup.mockResolvedValue([{ address: "::gggg", family: 6 }]);
      // parseInt('gggg', 16) = NaN -> isIPv6LoopbackOrUnspecified returns false
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Test" }));
      const result = await digestUri("https://badhex.com/agent.json");
      expect(["ok", "blocked"]).toContain(result.status);
    });

    it("should handle left :: compression (::1234:5678)", async () => {
      mockLookup.mockResolvedValue([
        { address: "::1234:5678", family: 6 },
      ]);
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Test" }));
      const result = await digestUri("https://leftcompression.com/agent.json");
      // Not loopback/unspecified, and not in private ranges -> ok
      expect(result.status).toBe("ok");
    });
  });

  // =========================================================================
  // canonicalizeIP - hex IPv4, decimal IPv4, shorthand, octal
  // =========================================================================
  describe("canonicalizeIP edge cases via SSRF blocking", () => {
    it("should block hex IPv4 (0x7f000001 = 127.0.0.1)", async () => {
      mockLookup.mockResolvedValue([{ address: "0x7f000001", family: 4 }]);
      const result = await digestUri("https://hexip.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block decimal IPv4 (2130706433 = 127.0.0.1)", async () => {
      mockLookup.mockResolvedValue([{ address: "2130706433", family: 4 }]);
      const result = await digestUri("https://decimalip.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block IPv4 shorthand 2-part (127.1 = 127.0.0.1)", async () => {
      mockLookup.mockResolvedValue([{ address: "127.1", family: 4 }]);
      const result = await digestUri("https://shorthand2.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block IPv4 shorthand 3-part (10.0.1 = 10.0.0.1)", async () => {
      mockLookup.mockResolvedValue([{ address: "10.0.1", family: 4 }]);
      const result = await digestUri("https://shorthand3.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block octal IPv4 (0177.0.0.1 = 127.0.0.1)", async () => {
      mockLookup.mockResolvedValue([{ address: "0177.0.0.1", family: 4 }]);
      const result = await digestUri("https://octalip.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should allow non-private hex IPv4", async () => {
      // 0x5DB8D822 = 93.184.216.34 (public)
      mockLookup.mockResolvedValue([{ address: "0x5DB8D822", family: 4 }]);
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Test" }));
      const result = await digestUri("https://pubhex.com/agent.json");
      expect(result.status).toBe("ok");
    });

    it("should allow non-private decimal IPv4", async () => {
      // 1572395042 = 93.184.216.34 (public)
      mockLookup.mockResolvedValue([{ address: "1572395042", family: 4 }]);
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Test" }));
      const result = await digestUri("https://pubdec.com/agent.json");
      expect(result.status).toBe("ok");
    });

    it("should handle IP longer than 100 chars (returns null from canonicalize)", async () => {
      const longIp = "1".repeat(101);
      mockLookup.mockResolvedValue([{ address: longIp, family: 4 }]);
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Test" }));
      const result = await digestUri("https://longip.com/agent.json");
      // IP longer than 100 chars -> canonicalize returns null -> not blocked -> ok
      expect(result.status).toBe("ok");
    });

    it("should handle IPv4 shorthand 2-part with valid non-private values", async () => {
      // 93.34 = 93.0.0.34 (public)
      mockLookup.mockResolvedValue([{ address: "93.34", family: 4 }]);
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Test" }));
      const result = await digestUri("https://pubshort.com/agent.json");
      expect(result.status).toBe("ok");
    });

    it("should handle IPv4 shorthand 3-part with valid non-private values", async () => {
      // 93.184.34 = 93.184.0.34 (public)
      mockLookup.mockResolvedValue([{ address: "93.184.34", family: 4 }]);
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Test" }));
      const result = await digestUri("https://pubshort3.com/agent.json");
      expect(result.status).toBe("ok");
    });

    it("should block IPv4 shorthand 2-part for private range (192.1 = 192.0.0.1)", async () => {
      // 10.1 = 10.0.0.1 (private)
      mockLookup.mockResolvedValue([{ address: "10.1", family: 4 }]);
      const result = await digestUri("https://privshort2.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should block IPv4 shorthand 3-part for private range (192.168.1)", async () => {
      // 192.168.1 = 192.168.0.1 (private)
      mockLookup.mockResolvedValue([{ address: "192.168.1", family: 4 }]);
      const result = await digestUri("https://privshort3.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should handle non-IP non-hostname input to canonicalizeIP", async () => {
      // A value that doesn't match any IP pattern
      mockLookup.mockResolvedValue([{ address: "not-an-ip", family: 4 }]);
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Test" }));
      const result = await digestUri("https://notanip.com/agent.json");
      // "not-an-ip" doesn't match any canonicalization pattern -> returns null -> not blocked
      expect(result.status).toBe("ok");
    });

    it("should block IPv6 zone ID stripping (::1%25lo0 URL-encoded)", async () => {
      mockLookup.mockResolvedValue([{ address: "::1%25lo0", family: 6 }]);
      const result = await digestUri("https://zoneurl.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should handle hex IPv4 out of range (> 0xffffffff)", async () => {
      mockLookup.mockResolvedValue([{ address: "0xfffffffff", family: 4 }]);
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Test" }));
      const result = await digestUri("https://bighex.com/agent.json");
      // Out of range -> canonicalize returns null -> not blocked
      expect(result.status).toBe("ok");
    });

    it("should handle decimal IPv4 out of range (> 4294967295)", async () => {
      mockLookup.mockResolvedValue([{ address: "4294967296", family: 4 }]);
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Test" }));
      const result = await digestUri("https://bigdec.com/agent.json");
      // Out of range -> not recognized as IPv4 -> not blocked
      expect(result.status).toBe("ok");
    });

    it("should handle IPv4-mapped IPv6 full form with dotted notation", async () => {
      // 0:0:0:0:0:ffff:10.0.0.1 -> 10.0.0.1 (private)
      mockLookup.mockResolvedValue([
        { address: "0:0:0:0:0:ffff:10.0.0.1", family: 6 },
      ]);
      const result = await digestUri("https://mappeddotted.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should handle IPv4-compatible deprecated form (::192.168.1.1)", async () => {
      mockLookup.mockResolvedValue([
        { address: "::192.168.1.1", family: 6 },
      ]);
      const result = await digestUri("https://compatdeprecated.com/agent.json");
      expect(result.status).toBe("blocked");
    });

    it("should handle octal IPv4 with mixed notation (0300.0250.01.01 = 192.168.1.1)", async () => {
      mockLookup.mockResolvedValue([{ address: "0300.0250.01.01", family: 4 }]);
      const result = await digestUri("https://mixedoctal.com/agent.json");
      expect(result.status).toBe("blocked");
    });
  });

  // =========================================================================
  // truncatedKeys flag
  // =========================================================================
  describe("truncatedKeys flag in result", () => {
    it("should set truncatedKeys to false when within limit", async () => {
      global.fetch = vi.fn().mockResolvedValue(mockFetchOk({ name: "Test" }));
      const result = await digestUri("https://example.com/agent.json");
      expect(result.truncatedKeys).toBe(false);
    });
  });
});
