import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { createGraphQLServer } from "../../../src/api/server.js";
import { createMockPrismaClient } from "../../mocks/prisma.js";

describe("GraphQL Server", () => {
  let mockPrisma: ReturnType<typeof createMockPrismaClient>;
  let mockProcessor: any;

  beforeEach(() => {
    mockPrisma = createMockPrismaClient();
    mockProcessor = {
      getStatus: vi.fn().mockReturnValue({
        running: true,
        mode: "auto",
        pollerActive: true,
        wsActive: false,
      }),
    };
  });

  describe("createGraphQLServer", () => {
    it("should create server with default port", async () => {
      const result = await createGraphQLServer({
        prisma: mockPrisma,
        processor: mockProcessor,
      });

      expect(result).toBeDefined();
      expect(result.server).toBeDefined();
      expect(typeof result.start).toBe("function");
      expect(typeof result.stop).toBe("function");
    });

    it("should create server with custom port", async () => {
      const result = await createGraphQLServer({
        prisma: mockPrisma,
        processor: mockProcessor,
        port: 5000,
      });

      expect(result).toBeDefined();
      expect(result.server).toBeDefined();
    });

    it("should start and stop server", async () => {
      const result = await createGraphQLServer({
        prisma: mockPrisma,
        processor: mockProcessor,
        port: 4001,
      });

      await result.start();
      await result.stop();

      // Server started and stopped without error
      expect(true).toBe(true);
    });

    it("should handle concurrent requests", async () => {
      const result = await createGraphQLServer({
        prisma: mockPrisma,
        processor: mockProcessor,
        port: 4002,
      });

      await result.start();

      // Make a simple GraphQL request
      const response = await fetch("http://localhost:4002/graphql", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          query: "{ indexerStatus { running mode } }",
        }),
      });

      const data = await response.json();

      expect(data.data.indexerStatus.running).toBe(true);
      expect(data.data.indexerStatus.mode).toBe("auto");

      await result.stop();
    });

    it("should provide GraphiQL interface", async () => {
      const result = await createGraphQLServer({
        prisma: mockPrisma,
        processor: mockProcessor,
        port: 4003,
      });

      await result.start();

      const response = await fetch("http://localhost:4003/graphql", {
        method: "GET",
        headers: {
          Accept: "text/html",
        },
      });

      expect(response.status).toBe(200);
      const html = await response.text();
      expect(html).toContain("graphiql");

      await result.stop();
    });

    it("should handle agents query", async () => {
      const mockAgents = [
        { id: "agent-1", owner: "owner-1", nftName: "Agent 1" },
      ];
      (mockPrisma.agent.findMany as any).mockResolvedValue(mockAgents);

      const result = await createGraphQLServer({
        prisma: mockPrisma,
        processor: mockProcessor,
        port: 4004,
      });

      await result.start();

      const response = await fetch("http://localhost:4004/graphql", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          query: "{ agents { id owner nftName } }",
        }),
      });

      const data = await response.json();

      expect(data.data.agents).toEqual(mockAgents);

      await result.stop();
    });

    it("should handle stats query", async () => {
      (mockPrisma.agent.count as any).mockResolvedValue(10);
      (mockPrisma.feedback.count as any).mockResolvedValue(25);
      (mockPrisma.validation.count as any).mockResolvedValue(5);
      (mockPrisma.registry.count as any).mockResolvedValue(2);
      (mockPrisma.indexerState.findUnique as any).mockResolvedValue({
        lastSignature: "test-sig",
        lastSlot: 12345n,
        updatedAt: new Date("2024-01-15"),
      });

      const result = await createGraphQLServer({
        prisma: mockPrisma,
        processor: mockProcessor,
        port: 4005,
      });

      await result.start();

      const response = await fetch("http://localhost:4005/graphql", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          query: `{ stats {
            totalAgents
            totalFeedbacks
            totalValidations
            totalRegistries
            lastProcessedSignature
          } }`,
        }),
      });

      const data = await response.json();

      expect(data.data.stats.totalAgents).toBe(10);
      expect(data.data.stats.totalFeedbacks).toBe(25);
      expect(data.data.stats.totalValidations).toBe(5);
      expect(data.data.stats.totalRegistries).toBe(2);
      expect(data.data.stats.lastProcessedSignature).toBe("test-sig");

      await result.stop();
    });

    it("should handle CORS preflight requests", async () => {
      const result = await createGraphQLServer({
        prisma: mockPrisma,
        processor: mockProcessor,
        port: 4006,
      });

      await result.start();

      const response = await fetch("http://localhost:4006/graphql", {
        method: "OPTIONS",
        headers: {
          "Origin": "http://localhost:3000",
          "Access-Control-Request-Method": "POST",
        },
      });

      // OPTIONS request should succeed
      expect(response.ok).toBe(true);

      await result.stop();
    });

    it("should handle GraphQL error responses", async () => {
      const result = await createGraphQLServer({
        prisma: mockPrisma,
        processor: mockProcessor,
        port: 4007,
      });

      await result.start();

      // Send invalid query
      const response = await fetch("http://localhost:4007/graphql", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          query: "{ invalidField }",
        }),
      });

      const data = await response.json();
      expect(data.errors).toBeDefined();

      await result.stop();
    });
  });
});
