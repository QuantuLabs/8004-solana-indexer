/**
 * URI Digest Module Tests
 * Tests metadata extraction from agent URIs
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { digestUri, serializeValue } from "../../src/indexer/uriDigest.js";

// Mock the config module
vi.mock("../../src/config.js", () => ({
  config: {
    metadataIndexMode: "normal",
    metadataMaxBytes: 262144, // 256KB
    metadataTimeoutMs: 5000,
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
      const mockJson = {
        type: "ai-agent",
        name: "Test Agent",
        description: "A test agent",
        image: "https://example.com/image.png",
        skills: ["web-search", "code-generation"],
        domains: ["finance", "tech"],
        active: true,
        endpoints: [
          { type: "mcp", url: "https://api.example.com/mcp" }
        ],
      };

      // Mock fetch
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        headers: new Headers({ "content-length": "200" }),
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
      expect(result.fields!["uri:type"]).toBe("ai-agent");
      expect(result.fields!["uri:name"]).toBe("Test Agent");
      expect(result.fields!["uri:description"]).toBe("A test agent");
      expect(result.fields!["uri:image"]).toBe("https://example.com/image.png");
      expect(result.fields!["uri:skills"]).toEqual(["web-search", "code-generation"]);
      expect(result.fields!["uri:domains"]).toEqual(["finance", "tech"]);
      expect(result.fields!["uri:active"]).toBe(true);
      expect(result.fields!["uri:endpoints"]).toEqual([
        { type: "mcp", url: "https://api.example.com/mcp" }
      ]);
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
      expect(result.fields!["uri:name"]).toBe("Test Agent");
      // Custom fields stored with uri: prefix
      expect(result.fields!["uri:customField1"]).toBe("value1");
      expect(result.fields!["uri:customField2"]).toBe("value2");
      expect(result.fields!["uri:nestedObject"]).toEqual({ foo: "bar" });
      // No uri:raw blob
      expect(result.fields!["uri:raw"]).toBeUndefined();
    });

    it("should limit extra keys to MAX_EXTRA_KEYS (50) for DoS protection", async () => {
      vi.resetModules();
      vi.doMock("../../src/config.js", () => ({
        config: {
          metadataIndexMode: "full",
          metadataMaxBytes: 262144,
          metadataTimeoutMs: 5000,
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

      // Count extra keys (excluding standard field uri:name)
      const extraKeyCount = Object.keys(result.fields!).filter(
        k => k.startsWith("uri:custom_")
      ).length;

      // Should be limited to 50
      expect(extraKeyCount).toBe(50);
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
