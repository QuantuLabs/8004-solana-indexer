import { beforeEach, describe, expect, it, vi } from "vitest";

const mockWarn = vi.hoisted(() => vi.fn());

vi.mock("../../../src/config.js", () => ({
  config: {
    metadataMaxBytes: 1024,
    metadataTimeoutMs: 25,
  },
}));

vi.mock("../../../src/logger.js", () => ({
  createChildLogger: () => ({
    debug: vi.fn(),
    info: vi.fn(),
    warn: mockWarn,
    error: vi.fn(),
  }),
}));

import { digestCollectionPointerDoc } from "../../../src/indexer/collectionDigest.js";

function mockStreamResponse(body: string, headers: Record<string, string> = {}): Response {
  const encoded = new TextEncoder().encode(body);
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

describe("collectionDigest", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("returns error for invalid canonical pointer", async () => {
    const fetchSpy = vi.fn();
    global.fetch = fetchSpy as unknown as typeof fetch;

    const result = await digestCollectionPointerDoc("ipfs://bafy123");

    expect(result).toEqual({
      status: "error",
      error: "Invalid canonical collection pointer format (expected c1:<cid>)",
    });
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("returns ok with sanitized collection fields", async () => {
    const payload = {
      version: "<b>1.0.0</b>",
      name: "  <i>Collection Name</i>  ",
      symbol: "<b>COLL</b>",
      description: "<p>desc</p>",
      image: "https://example.com/image.png",
      banner_image: "not-a-url",
      socials: {
        website: "https://collection.example",
        x: " <b>@collection</b> ",
        discord: "https://discord.gg/collection",
      },
    };

    global.fetch = vi.fn().mockResolvedValue(mockStreamResponse(JSON.stringify(payload))) as unknown as typeof fetch;

    const result = await digestCollectionPointerDoc("c1:bafy123");

    expect(result.status).toBe("ok");
    expect(result.bytes).toBeGreaterThan(0);
    expect(result.hash).toMatch(/^[a-f0-9]{64}$/);
    expect(result.fields).toEqual({
      version: "1.0.0",
      name: "Collection Name",
      symbol: "COLL",
      description: "desc",
      image: "https://example.com/image.png",
      bannerImage: null,
      socialWebsite: "https://collection.example/",
      socialX: "@collection",
      socialDiscord: "https://discord.gg/collection",
    });
  });

  it("returns invalid_json when payload is not valid JSON", async () => {
    global.fetch = vi.fn().mockResolvedValue(mockStreamResponse("not-json")) as unknown as typeof fetch;

    const result = await digestCollectionPointerDoc("c1:bafy123");

    expect(result.status).toBe("invalid_json");
    expect(result.bytes).toBe(8);
    expect(result.hash).toMatch(/^[a-f0-9]{64}$/);
  });

  it("returns oversize when content-length exceeds configured maximum", async () => {
    const getReader = vi.fn();
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers({ "content-length": "1025" }),
      body: {
        getReader,
      },
    } as unknown as Response) as unknown as typeof fetch;

    const result = await digestCollectionPointerDoc("c1:bafy123");

    expect(result).toEqual({
      status: "oversize",
      bytes: 1025,
    });
    expect(getReader).not.toHaveBeenCalled();
  });

  it("returns timeout when fetch aborts", async () => {
    const abortError = new Error("aborted");
    abortError.name = "AbortError";
    global.fetch = vi.fn().mockRejectedValue(abortError) as unknown as typeof fetch;

    const result = await digestCollectionPointerDoc("c1:bafy123");

    expect(result).toEqual({ status: "timeout" });
  });

  it("returns error for unexpected fetch failures", async () => {
    global.fetch = vi.fn().mockRejectedValue(new Error("network down")) as unknown as typeof fetch;

    const result = await digestCollectionPointerDoc("c1:bafy123");

    expect(result.status).toBe("error");
    expect(result.error).toContain("network down");
    expect(mockWarn).toHaveBeenCalledTimes(1);
  });
});
