import { describe, expect, it, vi } from "vitest";
import type { IncomingMessage, ServerResponse } from "node:http";
import {
  RAILWAY_REDIRECT_TARGET,
  handleRailwayRedirect,
  resolveRailwayRedirectLocation,
  resolveRailwayRedirectPort,
} from "../../src/railway-redirect.js";

describe("railway redirect", () => {
  it("preserves path and query while forcing the canonical devnet host", () => {
    expect(resolveRailwayRedirectLocation("http://legacy.example/v2/graphql?agent=2037")).toBe(
      `${RAILWAY_REDIRECT_TARGET}/v2/graphql?agent=2037`,
    );
  });

  it("prefers PORT over API_PORT and falls back to 3001", () => {
    expect(resolveRailwayRedirectPort({ PORT: "8080", API_PORT: "3001" } as NodeJS.ProcessEnv)).toBe(8080);
    expect(resolveRailwayRedirectPort({ API_PORT: "4010" } as NodeJS.ProcessEnv)).toBe(4010);
    expect(resolveRailwayRedirectPort({ PORT: "invalid" } as NodeJS.ProcessEnv)).toBe(3001);
  });

  it("returns a 308 redirect for runtime requests", () => {
    const headers = new Map<string, string>();
    const response = {
      statusCode: 200,
      setHeader: vi.fn((name: string, value: string) => {
        headers.set(name.toLowerCase(), value);
        return response;
      }),
      end: vi.fn(),
    } as unknown as ServerResponse;

    handleRailwayRedirect(
      { url: "/rest/v1/agents?limit=10" } as IncomingMessage,
      response,
    );

    expect(response.statusCode).toBe(308);
    expect(headers.get("cache-control")).toBe("no-store");
    expect(headers.get("location")).toBe(`${RAILWAY_REDIRECT_TARGET}/rest/v1/agents?limit=10`);
    expect(response.end).toHaveBeenCalledOnce();
  });
});
