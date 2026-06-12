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

  it("waits for postgrest startup before starting the indexer", () => {
    expect(stackContent).toContain("postgrest:");
    expect(stackContent).toContain("condition: service_started");
  });

  it("keeps postgrest healthcheck disabled", () => {
    expect(stackContent).toContain("healthcheck:");
    expect(stackContent).toContain("disable: true");
  });
});
