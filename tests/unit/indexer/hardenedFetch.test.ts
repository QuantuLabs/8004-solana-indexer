import { EventEmitter } from "node:events";
import https from "https";
import { Readable } from "node:stream";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mockLookup = vi.hoisted(() => vi.fn());

vi.mock("dns/promises", () => ({
  lookup: mockLookup,
}));

vi.mock("../../../src/config.js", () => ({
  config: {
    metadataMaxBytes: 1024,
    metadataPerHostFetchConcurrency: 2,
    metadataTimeoutMs: 1000,
    ipfsGatewayBase: "https://ipfs.io",
    uriDigestTrustedHosts: [],
  },
}));

vi.mock("../../../src/logger.js", () => ({
  createChildLogger: () => ({
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  }),
}));

import {
  createPinnedLookup,
  hardenedFetchBuffer,
  resetHardenedFetchStateForTests,
} from "../../../src/indexer/hardenedFetch.js";

describe("hardenedFetch", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    resetHardenedFetchStateForTests();
  });

  afterEach(() => {
    resetHardenedFetchStateForTests();
  });

  it("returns pinned address entries when lookup requests all addresses", () => {
    const lookup = createPinnedLookup({ ip: "93.184.216.34", family: 4 });
    let result: { error: Error | null; address: unknown; family: number | undefined } | undefined;

    lookup("metadata.example", { all: true }, (error, address, family) => {
      result = { error, address, family };
    });

    expect(result).toEqual({
      error: null,
      address: [{ address: "93.184.216.34", family: 4 }],
      family: undefined,
    });
  });

  it("returns a scalar pinned address when lookup does not request all addresses", () => {
    const lookup = createPinnedLookup({ ip: "2001:4860:4860::8888", family: 6 });
    const results: Array<{ error: Error | null; address: unknown; family: number | undefined }> = [];

    lookup("metadata.example", { all: false }, (error, address, family) => {
      results.push({ error, address, family });
    });
    lookup("metadata.example", undefined, (error, address, family) => {
      results.push({ error, address, family });
    });

    expect(results).toEqual([
      { error: null, address: "2001:4860:4860::8888", family: 6 },
      { error: null, address: "2001:4860:4860::8888", family: 6 },
    ]);
  });

  it("uses https.request with pinned lookup for initial and redirect requests outside Vitest transport mode", async () => {
    const originalVitest = process.env.VITEST;
    const body = Buffer.from("{\"name\":\"Pinned\"}");
    const lookupResults: Array<{ hostname: string; error: Error | null; address: unknown; family: number | undefined }> = [];
    const requestOptions: any[] = [];

    mockLookup
      .mockResolvedValueOnce([{ address: "93.184.216.34", family: 4 }])
      .mockResolvedValueOnce([{ address: "93.184.216.35", family: 4 }]);

    const requestSpy = vi.spyOn(https, "request").mockImplementation((options: any, callback: any) => {
      requestOptions.push(options);
      const request = new EventEmitter() as EventEmitter & {
        destroy: (error?: Error) => void;
        end: () => void;
      };

      request.destroy = (error?: Error): void => {
        if (error) request.emit("error", error);
        request.emit("close");
      };
      request.end = (): void => {
        options.lookup(options.hostname, { all: true }, (error: Error | null, address: unknown, family?: number) => {
          lookupResults.push({ hostname: options.hostname, error, address, family });
        });

        const responseBody = requestOptions.length === 1 ? Buffer.alloc(0) : body;
        const response = Readable.from(responseBody.length > 0 ? [responseBody] : []) as Readable & {
          statusCode: number;
          headers: Record<string, string>;
        };
        response.statusCode = requestOptions.length === 1 ? 302 : 200;
        response.headers = requestOptions.length === 1
          ? { location: "https://cdn.example/agent.json" }
          : { "content-length": String(body.length) };
        callback(response);
        request.emit("close");
      };

      return request as any;
    });

    delete process.env.VITEST;
    try {
      const result = await hardenedFetchBuffer("https://metadata.example/agent.json", {
        maxBytes: 1024,
        timeoutMs: 1000,
      });

      expect(result.status).toBe("ok");
      expect(requestSpy).toHaveBeenCalledTimes(2);
      expect(requestOptions.map((options) => options.hostname)).toEqual(["metadata.example", "cdn.example"]);
      expect(requestOptions.map((options) => options.servername)).toEqual(["metadata.example", "cdn.example"]);
      expect(requestOptions.every((options) => options.rejectUnauthorized !== false)).toBe(true);
      expect(lookupResults).toEqual([
        {
          hostname: "metadata.example",
          error: null,
          address: [{ address: "93.184.216.34", family: 4 }],
          family: undefined,
        },
        {
          hostname: "cdn.example",
          error: null,
          address: [{ address: "93.184.216.35", family: 4 }],
          family: undefined,
        },
      ]);
    } finally {
      if (originalVitest === undefined) {
        delete process.env.VITEST;
      } else {
        process.env.VITEST = originalVitest;
      }
    }
  });
});
