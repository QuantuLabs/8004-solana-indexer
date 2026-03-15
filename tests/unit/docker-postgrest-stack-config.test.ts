import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const stackContent = readFileSync(
  new URL("../../docker/stack/indexer-stack.postgrest.yml", import.meta.url),
  "utf8"
);

describe("Docker PostgREST Overlay Config", () => {
  it("forces API_MODE=both for the overlayed indexer", () => {
    expect(stackContent).toContain("API_MODE: both");
  });

  it("waits for postgrest health before starting the indexer", () => {
    expect(stackContent).toContain("postgrest:");
    expect(stackContent).toContain("condition: service_healthy");
  });

  it("defines a concrete postgrest HTTP healthcheck", () => {
    expect(stackContent).toContain("healthcheck:");
    expect(stackContent).toContain('PeerAddr => "127.0.0.1:3000"');
    expect(stackContent).toContain('GET / HTTP/1.0');
    expect(stackContent).toContain("HTTP/\\S+ 200");
  });
});
