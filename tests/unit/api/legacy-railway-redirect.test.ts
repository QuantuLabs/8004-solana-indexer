import type { Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  handleRailwayRedirect,
  resolveRailwayRedirectLocation,
  RAILWAY_REDIRECT_TARGET,
} from "../../../src/railway-redirect.js";
import { createServer } from "node:http";

describe("legacy railway redirect location", () => {
  it("preserves the request path and query string", () => {
    expect(resolveRailwayRedirectLocation("/v2/graphql?foo=1&bar=2")).toBe(
      `${RAILWAY_REDIRECT_TARGET}/v2/graphql?foo=1&bar=2`
    );
  });

  it("normalizes empty requests to the target root", () => {
    expect(resolveRailwayRedirectLocation("")).toBe(`${RAILWAY_REDIRECT_TARGET}/`);
  });
});

describe("legacy railway redirect server", () => {
  let server: Server;
  let baseUrl: string;

  beforeAll(async () => {
    server = createServer((req, res) => handleRailwayRedirect(req, res));

    await new Promise<void>((resolve, reject) => {
      server.listen(0, "127.0.0.1", () => {
        const address = server.address() as AddressInfo;
        baseUrl = `http://127.0.0.1:${address.port}`;
        resolve();
      });
      server.on("error", reject);
    });
  });

  afterAll(async () => {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
  });

  it("redirects POST requests with a permanent method-preserving status", async () => {
    const response = await fetch(`${baseUrl}/v2/graphql?asset=2037`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ query: "{ __typename }" }),
      redirect: "manual",
    });

    expect(response.status).toBe(308);
    expect(response.headers.get("location")).toBe(`${RAILWAY_REDIRECT_TARGET}/v2/graphql?asset=2037`);
  });

  it("answers CORS preflight locally so browser clients can follow the redirected POST", async () => {
    const response = await fetch(`${baseUrl}/v2/graphql`, {
      method: "OPTIONS",
      headers: {
        origin: "https://minter-8004.web.app",
        "access-control-request-method": "POST",
        "access-control-request-headers": "content-type",
      },
      redirect: "manual",
    });

    expect(response.status).toBe(204);
    expect(response.headers.get("access-control-allow-origin")).toBe("*");
    expect(response.headers.get("access-control-allow-methods")).toBe("GET, HEAD, POST, OPTIONS");
    expect(response.headers.get("access-control-allow-headers")).toBe("Content-Type, Authorization, Prefer");
  });
});
