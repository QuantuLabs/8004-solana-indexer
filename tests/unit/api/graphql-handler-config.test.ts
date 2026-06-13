import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { createYogaMock, mockLogger } = vi.hoisted(() => {
  const yoga = vi.fn(() => ({ handle: vi.fn() }));
  const logger = {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    fatal: vi.fn(),
    child: vi.fn().mockReturnThis(),
  };
  return { createYogaMock: yoga, mockLogger: logger };
});

vi.mock("dotenv/config", () => ({}));

vi.mock("graphql-yoga", () => ({
  createYoga: createYogaMock,
}));

vi.mock("../../../src/logger.js", () => ({
  logger: mockLogger,
  createChildLogger: vi.fn(() => mockLogger),
}));

const originalEnv = process.env;

describe("GraphQL handler config", () => {
  beforeEach(() => {
    vi.resetModules();
    createYogaMock.mockClear();
    process.env = { ...originalEnv };
    delete process.env.GRAPHQL_DEBUG_ENABLED;
    delete process.env.NODE_ENV;
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  it("defaults to masked errors with GraphiQL disabled", async () => {
    const { createGraphQLHandler } = await import("../../../src/api/graphql/index.js");

    createGraphQLHandler({ pool: {} as any, prisma: null });

    expect(createYogaMock).toHaveBeenCalledOnce();
    const options = createYogaMock.mock.calls[0][0];
    expect(options.graphiql).toBe(false);
    expect(options.maskedErrors).toBe(true);
  });

  it("uses the explicit debug flag independent of NODE_ENV", async () => {
    process.env.NODE_ENV = "production";
    process.env.GRAPHQL_DEBUG_ENABLED = "true";

    const { createGraphQLHandler } = await import("../../../src/api/graphql/index.js");

    createGraphQLHandler({ pool: {} as any, prisma: null });

    expect(createYogaMock).toHaveBeenCalledOnce();
    const options = createYogaMock.mock.calls[0][0];
    expect(options.graphiql).toBe(true);
    expect(options.maskedErrors).toBe(false);
  });
});
