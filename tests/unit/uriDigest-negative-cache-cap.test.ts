import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const mockLookup = vi.hoisted(() => vi.fn());
vi.mock("dns/promises", () => ({
  lookup: mockLookup,
}));

describe("URI Digest negative DNS cache cap", () => {
  const previousCap = process.env.URI_DIGEST_DNS_NEGATIVE_CACHE_MAX_ENTRIES;

  beforeEach(() => {
    vi.clearAllMocks();
    vi.resetModules();
  });

  afterEach(() => {
    if (previousCap === undefined) {
      delete process.env.URI_DIGEST_DNS_NEGATIVE_CACHE_MAX_ENTRIES;
    } else {
      process.env.URI_DIGEST_DNS_NEGATIVE_CACHE_MAX_ENTRIES = previousCap;
    }
    vi.resetModules();
  });

  it("should evict oldest cached DNS failures when cap is reached", async () => {
    const cap = 100;
    process.env.URI_DIGEST_DNS_NEGATIVE_CACHE_MAX_ENTRIES = String(cap);

    vi.doMock("../../src/config.js", () => ({
      config: {
        metadataIndexMode: "normal",
        metadataMaxBytes: 262144,
        metadataMaxValueBytes: 10000,
        metadataTimeoutMs: 5000,
        ipfsGatewayBase: "https://ipfs.io",
        uriDigestTrustedHosts: [],
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

    mockLookup.mockRejectedValue(new Error("ENOTFOUND"));

    const { digestUri } = await import("../../src/indexer/uriDigest.js");

    for (let i = 0; i < cap + 1; i++) {
      expect((await digestUri(`https://cap-${i}.invalid/agent.json`)).status).toBe("blocked");
    }
    // cap reached with one extra host => first host should have been evicted.
    expect((await digestUri("https://cap-0.invalid/agent.json")).status).toBe("blocked");

    expect(mockLookup).toHaveBeenCalledTimes(cap + 2);
  });
});
