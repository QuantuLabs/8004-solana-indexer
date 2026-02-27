/**
 * URI Digest Module Tests
 * Tests metadata extraction from agent URIs
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { digestUri, serializeValue, sanitizeText, sanitizeUrl } from "../../src/indexer/uriDigest.js";

// Mock the config module
vi.mock("../../src/config.js", () => ({
  config: {
    metadataIndexMode: "normal",
    metadataMaxBytes: 262144, // 256KB
    metadataTimeoutMs: 5000,
    ipfsGatewayBase: "https://ipfs.io",
    uriDigestTrustedHosts: [],
  },
}));

// Mock the logger
vi.mock("../../src/logger.js", () => ({
  createChildLogger: () => ({
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  }),
}));

describe("URI Digest Module", () => {
  describe("digestUri", () => {
    beforeEach(() => {
      vi.clearAllMocks();
    });

    afterEach(() => {
      vi.restoreAllMocks();
    });

    it("should return error for empty URI", async () => {
      const result = await digestUri("");
      expect(result.status).toBe("error");
      expect(result.error).toBe("Empty URI");
    });

    it("should return error for unsupported URI scheme", async () => {
      const result = await digestUri("ftp://example.com/file.json");
      expect(result.status).toBe("error");
      expect(result.error).toBe("Unsupported URI scheme");
    });

    it("should extract standard ERC-8004 fields from valid JSON", async () => {
      // ERC-8004 spec: https://github.com/erc-8004/best-practices/blob/main/Registration.md
      const mockJson = {
        type: "https://eips.ethereum.org/EIPS/eip-8004#registration-v1",
        name: "Test Agent",
        description: "A test agent",
        image: "https://example.com/image.png",
        active: true,
        x402Support: false,
        supportedTrust: ["reputation"],
        services: [
          {
            name: "mcp",
            endpoint: "https://api.example.com/mcp",
            version: "1.0",
            skills: ["web-search", "code-generation"],
            domains: ["finance", "tech"],
          }
        ],
        registrations: [
          { agentId: 123, agentRegistry: "eip155:1:0x1234567890abcdef" }
        ],
      };

      // Mock fetch
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        headers: new Headers({ "content-length": "500" }),
        body: {
          getReader: () => ({
            read: vi.fn()
              .mockResolvedValueOnce({
                done: false,
                value: new TextEncoder().encode(JSON.stringify(mockJson)),
              })
              .mockResolvedValueOnce({ done: true, value: undefined }),
            cancel: vi.fn(),
          }),
        },
      } as unknown as Response);

      const result = await digestUri("https://example.com/agent.json");

      expect(result.status).toBe("ok");
      expect(result.bytes).toBeGreaterThan(0);
      expect(result.hash).toBeDefined();
      expect(result.fields).toBeDefined();
      expect(result.fields!["_uri:type"]).toBe("https://eips.ethereum.org/EIPS/eip-8004#registration-v1");
      expect(result.fields!["_uri:name"]).toBe("Test Agent");
      expect(result.fields!["_uri:description"]).toBe("A test agent");
      expect(result.fields!["_uri:image"]).toBe("https://example.com/image.png");
      expect(result.fields!["_uri:active"]).toBe(true);
      expect(result.fields!["_uri:x402_support"]).toBe(false);
      expect(result.fields!["_uri:supported_trust"]).toEqual(["reputation"]);
      // Services with skills/domains inside
      const services = result.fields!["_uri:services"] as Array<Record<string, unknown>>;
      expect(services).toHaveLength(1);
      expect(services[0].name).toBe("mcp");
      expect(services[0].endpoint).toBe("https://api.example.com/mcp");
      expect(services[0].skills).toEqual(["web-search", "code-generation"]);
      expect(services[0].domains).toEqual(["finance", "tech"]);
      // Registrations
      const registrations = result.fields!["_uri:registrations"] as Array<Record<string, unknown>>;
      expect(registrations).toHaveLength(1);
      expect(registrations[0].agentId).toBe(123);
      expect(registrations[0].agentRegistry).toBe("eip155:1:0x1234567890abcdef");
    });

    it("should handle HTTP error responses", async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 404,
      } as Response);

      const result = await digestUri("https://example.com/notfound.json");

      expect(result.status).toBe("error");
      expect(result.error).toBe("HTTP 404");
    });

    it("should handle invalid JSON", async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        headers: new Headers({ "content-length": "20" }),
        body: {
          getReader: () => ({
            read: vi.fn()
              .mockResolvedValueOnce({
                done: false,
                value: new TextEncoder().encode("not valid json"),
              })
              .mockResolvedValueOnce({ done: true, value: undefined }),
            cancel: vi.fn(),
          }),
        },
      } as unknown as Response);

      const result = await digestUri("https://example.com/invalid.json");

      expect(result.status).toBe("invalid_json");
      expect(result.bytes).toBe(14);
      expect(result.hash).toBeDefined();
    });

    it("should convert IPFS URI to gateway URL", async () => {
      const mockJson = { name: "IPFS Agent" };

      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        headers: new Headers({ "content-length": "25" }),
        body: {
          getReader: () => ({
            read: vi.fn()
              .mockResolvedValueOnce({
                done: false,
                value: new TextEncoder().encode(JSON.stringify(mockJson)),
              })
              .mockResolvedValueOnce({ done: true, value: undefined }),
            cancel: vi.fn(),
          }),
        },
      } as unknown as Response);

      await digestUri("ipfs://QmTest123abc");

      expect(global.fetch).toHaveBeenCalledWith(
        "https://ipfs.io/ipfs/QmTest123abc",
        expect.any(Object)
      );
    });

    it("should convert Arweave URI to gateway URL", async () => {
      const mockJson = { name: "Arweave Agent" };

      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        headers: new Headers({ "content-length": "28" }),
        body: {
          getReader: () => ({
            read: vi.fn()
              .mockResolvedValueOnce({
                done: false,
                value: new TextEncoder().encode(JSON.stringify(mockJson)),
              })
              .mockResolvedValueOnce({ done: true, value: undefined }),
            cancel: vi.fn(),
          }),
        },
      } as unknown as Response);

      await digestUri("ar://arweave-tx-id-123");

      expect(global.fetch).toHaveBeenCalledWith(
        "https://arweave.net/arweave-tx-id-123",
        expect.any(Object)
      );
    });
  });

  describe("mode full - individual keys with limit", () => {
    beforeEach(() => {
      vi.clearAllMocks();
      // Override config for full mode tests
      vi.doMock("../../src/config.js", () => ({
        config: {
          metadataIndexMode: "full",
          metadataMaxBytes: 262144,
          metadataTimeoutMs: 5000,
          ipfsGatewayBase: "https://ipfs.io",
          uriDigestTrustedHosts: [],
        },
      }));
    });

    it("should store unknown keys individually as uri:<key>", async () => {
      // Re-import with full mode config
      vi.resetModules();
      vi.doMock("../../src/config.js", () => ({
        config: {
          metadataIndexMode: "full",
          metadataMaxBytes: 262144,
          metadataTimeoutMs: 5000,
          ipfsGatewayBase: "https://ipfs.io",
          uriDigestTrustedHosts: [],
        },
      }));

      const { digestUri: digestUriFull } = await import("../../src/indexer/uriDigest.js");

      const mockJson = {
        name: "Test Agent",
        customField1: "value1",
        customField2: "value2",
        nestedObject: { foo: "bar" },
      };

      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        headers: new Headers({ "content-length": "100" }),
        body: {
          getReader: () => ({
            read: vi.fn()
              .mockResolvedValueOnce({
                done: false,
                value: new TextEncoder().encode(JSON.stringify(mockJson)),
              })
              .mockResolvedValueOnce({ done: true, value: undefined }),
            cancel: vi.fn(),
          }),
        },
      } as unknown as Response);

      const result = await digestUriFull("https://example.com/agent.json");

      expect(result.status).toBe("ok");
      expect(result.fields).toBeDefined();
      // Standard field
      expect(result.fields!["_uri:name"]).toBe("Test Agent");
      // Custom fields stored with _uri: prefix
      expect(result.fields!["_uri:customField1"]).toBe("value1");
      expect(result.fields!["_uri:customField2"]).toBe("value2");
      // Nested objects are serialized to JSON for XSS protection
      expect(result.fields!["_uri:nestedObject"]).toBe('{"foo":"bar"}');
      // No _uri:raw blob
      expect(result.fields!["_uri:raw"]).toBeUndefined();
    });

    it("should limit extra keys to MAX_EXTRA_KEYS (50) for DoS protection", async () => {
      vi.resetModules();
      vi.doMock("../../src/config.js", () => ({
        config: {
          metadataIndexMode: "full",
          metadataMaxBytes: 262144,
          metadataTimeoutMs: 5000,
          ipfsGatewayBase: "https://ipfs.io",
          uriDigestTrustedHosts: [],
        },
      }));

      const { digestUri: digestUriFull } = await import("../../src/indexer/uriDigest.js");

      // Create JSON with 100 custom keys (beyond the 50 limit)
      const mockJson: Record<string, string> = { name: "Test" };
      for (let i = 0; i < 100; i++) {
        mockJson[`custom_${i}`] = `value_${i}`;
      }

      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        headers: new Headers({ "content-length": "5000" }),
        body: {
          getReader: () => ({
            read: vi.fn()
              .mockResolvedValueOnce({
                done: false,
                value: new TextEncoder().encode(JSON.stringify(mockJson)),
              })
              .mockResolvedValueOnce({ done: true, value: undefined }),
            cancel: vi.fn(),
          }),
        },
      } as unknown as Response);

      const result = await digestUriFull("https://example.com/agent.json");

      expect(result.status).toBe("ok");
      expect(result.fields).toBeDefined();

      // Count extra keys (excluding standard field _uri:name)
      const extraKeyCount = Object.keys(result.fields!).filter(
        k => k.startsWith("_uri:custom_")
      ).length;

      // Should be limited to 50
      expect(extraKeyCount).toBe(50);
    });
  });

  describe("XSS/Injection Protection", () => {
    describe("sanitizeText", () => {
      it("should strip HTML script tags", () => {
        const result = sanitizeText('<script>alert("xss")</script>');
        expect(result).toBe("");
      });

      it("should strip img onerror XSS", () => {
        const result = sanitizeText('<img src=x onerror="alert(1)">');
        expect(result).toBe("");
      });

      it("should strip SVG with script", () => {
        const result = sanitizeText('<svg onload="alert(1)"></svg>');
        expect(result).toBe("");
      });

      it("should preserve valid text content", () => {
        const result = sanitizeText("Test Agent - AI Assistant v2.0");
        expect(result).toBe("Test Agent - AI Assistant v2.0");
      });

      it("should preserve Unicode characters", () => {
        const result = sanitizeText("エージェント 🤖 代理人");
        expect(result).toBe("エージェント 🤖 代理人");
      });

      it("should handle nested XSS attempts", () => {
        const result = sanitizeText('<div><script>evil()</script><p>Text</p></div>');
        expect(result).toBe("Text");
      });

      it("should strip event handlers", () => {
        const result = sanitizeText('<div onclick="evil()">Click me</div>');
        expect(result).toBe("Click me");
      });

      it("should handle SQL injection strings (preserves as text)", () => {
        const input = "'; DROP TABLE agents; --";
        const result = sanitizeText(input);
        expect(result).toBe("'; DROP TABLE agents; --");
      });
    });

    describe("sanitizeUrl", () => {
      it("should allow https URLs", () => {
        const result = sanitizeUrl("https://example.com/image.png");
        expect(result).toBe("https://example.com/image.png");
      });

      it("should allow http URLs", () => {
        const result = sanitizeUrl("http://example.com/image.png");
        expect(result).toBe("http://example.com/image.png");
      });

      it("should allow IPFS URIs", () => {
        const result = sanitizeUrl("ipfs://QmTest123abc");
        expect(result).toBe("ipfs://QmTest123abc");
      });

      it("should allow Arweave URIs", () => {
        const result = sanitizeUrl("ar://arweave-tx-id-123");
        expect(result).toBe("ar://arweave-tx-id-123");
      });

      it("should block javascript: protocol", () => {
        const result = sanitizeUrl('javascript:alert("xss")');
        expect(result).toBe("");
      });

      it("should block data: protocol", () => {
        const result = sanitizeUrl("data:text/html,<script>alert(1)</script>");
        expect(result).toBe("");
      });

      it("should block vbscript: protocol", () => {
        const result = sanitizeUrl("vbscript:msgbox(1)");
        expect(result).toBe("");
      });

      it("should block file: protocol", () => {
        const result = sanitizeUrl("file:///etc/passwd");
        expect(result).toBe("");
      });

      it("should handle malformed URLs", () => {
        const result = sanitizeUrl("not a valid url");
        expect(result).toBe("");
      });

      it("should strip XSS from IPFS paths", () => {
        const result = sanitizeUrl('ipfs://<script>alert(1)</script>');
        expect(result).toBe("");
      });
    });

    describe("XSS in metadata extraction", () => {
      beforeEach(() => {
        vi.clearAllMocks();
      });

      it("should sanitize XSS in name field", async () => {
        const mockJson = {
          name: '<script>alert("xss")</script>Test Agent',
          description: "Normal description",
        };

        global.fetch = vi.fn().mockResolvedValue({
          ok: true,
          headers: new Headers({ "content-length": "100" }),
          body: {
            getReader: () => ({
              read: vi.fn()
                .mockResolvedValueOnce({
                  done: false,
                  value: new TextEncoder().encode(JSON.stringify(mockJson)),
                })
                .mockResolvedValueOnce({ done: true, value: undefined }),
              cancel: vi.fn(),
            }),
          },
        } as unknown as Response);

        const result = await digestUri("https://example.com/agent.json");

        expect(result.status).toBe("ok");
        // Script tag stripped, only plain text remains
        expect(result.fields!["_uri:name"]).toBe("Test Agent");
      });

      it("should sanitize XSS in description field", async () => {
        const mockJson = {
          name: "Test Agent",
          description: '<img src=x onerror="stealCookies()">A helpful assistant',
        };

        global.fetch = vi.fn().mockResolvedValue({
          ok: true,
          headers: new Headers({ "content-length": "100" }),
          body: {
            getReader: () => ({
              read: vi.fn()
                .mockResolvedValueOnce({
                  done: false,
                  value: new TextEncoder().encode(JSON.stringify(mockJson)),
                })
                .mockResolvedValueOnce({ done: true, value: undefined }),
              cancel: vi.fn(),
            }),
          },
        } as unknown as Response);

        const result = await digestUri("https://example.com/agent.json");

        expect(result.status).toBe("ok");
        expect(result.fields!["_uri:description"]).toBe("A helpful assistant");
      });

      it("should block dangerous image URLs", async () => {
        const mockJson = {
          name: "Test Agent",
          image: 'javascript:alert("xss")',
        };

        global.fetch = vi.fn().mockResolvedValue({
          ok: true,
          headers: new Headers({ "content-length": "100" }),
          body: {
            getReader: () => ({
              read: vi.fn()
                .mockResolvedValueOnce({
                  done: false,
                  value: new TextEncoder().encode(JSON.stringify(mockJson)),
                })
                .mockResolvedValueOnce({ done: true, value: undefined }),
              cancel: vi.fn(),
            }),
          },
        } as unknown as Response);

        const result = await digestUri("https://example.com/agent.json");

        expect(result.status).toBe("ok");
        // Dangerous URL is blocked (field will be missing or empty)
        expect(result.fields!["_uri:image"]).toBeUndefined();
      });

      it("should sanitize service endpoints (ERC-8004 format)", async () => {
        const mockJson = {
          name: "Test Agent",
          services: [
            { name: "mcp", endpoint: "https://api.example.com/mcp" },
            { name: "a2a", endpoint: 'javascript:alert("xss")' }, // Blocked - invalid endpoint
            { name: "mcp", endpoint: "http://localhost:3000/mcp" }, // Allowed (HTTP ok)
            { name: "invalid-type", endpoint: "https://valid.url" }, // Blocked - invalid service name
          ],
        };

        global.fetch = vi.fn().mockResolvedValue({
          ok: true,
          headers: new Headers({ "content-length": "300" }),
          body: {
            getReader: () => ({
              read: vi.fn()
                .mockResolvedValueOnce({
                  done: false,
                  value: new TextEncoder().encode(JSON.stringify(mockJson)),
                })
                .mockResolvedValueOnce({ done: true, value: undefined }),
              cancel: vi.fn(),
            }),
          },
        } as unknown as Response);

        const result = await digestUri("https://example.com/agent.json");

        expect(result.status).toBe("ok");
        const services = result.fields!["_uri:services"] as Array<Record<string, unknown>>;
        // Only valid services should remain
        expect(services.length).toBe(2);
        expect(services[0]).toEqual({ name: "mcp", endpoint: "https://api.example.com/mcp" });
        expect(services[1]).toEqual({ name: "mcp", endpoint: "http://localhost:3000/mcp" });
      });

      it("should validate skill slugs inside services (reject XSS)", async () => {
        const mockJson = {
          name: "Test Agent",
          services: [
            {
              name: "oasf",
              endpoint: "https://example.com/oasf",
              skills: [
                "web-search", // Valid
                '<script>alert(1)</script>', // Invalid
                "code-generation", // Valid
                "INVALID SLUG!", // Invalid (spaces, special chars)
              ],
              domains: [
                "finance", // Valid
                "<img onerror=alert(1)>", // Invalid
              ],
            },
          ],
        };

        global.fetch = vi.fn().mockResolvedValue({
          ok: true,
          headers: new Headers({ "content-length": "300" }),
          body: {
            getReader: () => ({
              read: vi.fn()
                .mockResolvedValueOnce({
                  done: false,
                  value: new TextEncoder().encode(JSON.stringify(mockJson)),
                })
                .mockResolvedValueOnce({ done: true, value: undefined }),
              cancel: vi.fn(),
            }),
          },
        } as unknown as Response);

        const result = await digestUri("https://example.com/agent.json");

        expect(result.status).toBe("ok");
        const services = result.fields!["_uri:services"] as Array<Record<string, unknown>>;
        expect(services.length).toBe(1);
        // Only valid slugs remain
        expect(services[0].skills).toEqual(["web-search", "code-generation"]);
        expect(services[0].domains).toEqual(["finance"]);
      });
    });
  });

  describe("serializeValue", () => {
    it("should serialize string values directly", () => {
      const result = serializeValue("hello", 100);
      expect(result.value).toBe("hello");
      expect(result.oversize).toBe(false);
      expect(result.bytes).toBe(5);
    });

    it("should serialize objects as JSON", () => {
      const obj = { key: "value" };
      const result = serializeValue(obj, 100);
      expect(result.value).toBe('{"key":"value"}');
      expect(result.oversize).toBe(false);
      expect(result.bytes).toBe(15);
    });

    it("should mark oversize values", () => {
      const longString = "x".repeat(100);
      const result = serializeValue(longString, 50);
      expect(result.value).toBe("");
      expect(result.oversize).toBe(true);
      expect(result.bytes).toBe(100);
    });

    it("should handle arrays", () => {
      const arr = ["a", "b", "c"];
      const result = serializeValue(arr, 100);
      expect(result.value).toBe('["a","b","c"]');
      expect(result.oversize).toBe(false);
    });

    it("should handle UTF-8 multi-byte characters", () => {
      const emoji = "🔥";
      const result = serializeValue(emoji, 100);
      expect(result.bytes).toBe(4); // 🔥 is 4 bytes in UTF-8
      expect(result.oversize).toBe(false);
    });
  });
});
