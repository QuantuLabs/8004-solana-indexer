import { describe, it, expect, vi, beforeEach } from "vitest";
import { resolvers, ResolverContext } from "../../../src/api/resolvers.js";
import { createMockPrismaClient } from "../../mocks/prisma.js";
import {
  TEST_ASSET,
  TEST_OWNER,
  TEST_COLLECTION,
  TEST_REGISTRY,
  TEST_CLIENT,
  TEST_VALIDATOR,
  TEST_SIGNATURE,
  TEST_SLOT,
  TEST_BLOCK_TIME,
} from "../../mocks/solana.js";

describe("GraphQL Resolvers", () => {
  let mockPrisma: ReturnType<typeof createMockPrismaClient>;
  let mockProcessor: any;
  let ctx: ResolverContext;

  beforeEach(() => {
    mockPrisma = createMockPrismaClient();
    mockProcessor = {
      getStatus: vi.fn().mockReturnValue({
        running: true,
        mode: "auto",
        pollerActive: true,
        wsActive: true,
      }),
    };
    ctx = { prisma: mockPrisma, processor: mockProcessor };
  });

  describe("Query", () => {
    describe("agent", () => {
      it("should return agent by id", async () => {
        const mockAgent = {
          id: TEST_ASSET.toBase58(),
          owner: TEST_OWNER.toBase58(),
        };
        (mockPrisma.agent.findUnique as any).mockResolvedValue(mockAgent);

        const result = await resolvers.Query.agent(
          null,
          { id: TEST_ASSET.toBase58() },
          ctx
        );

        expect(result).toEqual(mockAgent);
        expect(mockPrisma.agent.findUnique).toHaveBeenCalledWith({
          where: { id: TEST_ASSET.toBase58() },
        });
      });

      it("should return null for non-existent agent", async () => {
        (mockPrisma.agent.findUnique as any).mockResolvedValue(null);

        const result = await resolvers.Query.agent(
          null,
          { id: "non-existent" },
          ctx
        );

        expect(result).toBeNull();
      });
    });

    describe("agents", () => {
      it("should return all agents with default limit", async () => {
        const mockAgents = [{ id: "1" }, { id: "2" }];
        (mockPrisma.agent.findMany as any).mockResolvedValue(mockAgents);

        const result = await resolvers.Query.agents(null, {}, ctx);

        expect(result).toEqual(mockAgents);
        expect(mockPrisma.agent.findMany).toHaveBeenCalledWith({
          where: {},
          take: 50,
          skip: 0,
          orderBy: { createdAt: "desc" },
        });
      });

      it("should filter by owner", async () => {
        (mockPrisma.agent.findMany as any).mockResolvedValue([]);

        await resolvers.Query.agents(
          null,
          { owner: TEST_OWNER.toBase58() },
          ctx
        );

        expect(mockPrisma.agent.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            where: { owner: TEST_OWNER.toBase58() },
          })
        );
      });

      it("should filter by collection", async () => {
        (mockPrisma.agent.findMany as any).mockResolvedValue([]);

        await resolvers.Query.agents(
          null,
          { collection: TEST_COLLECTION.toBase58() },
          ctx
        );

        expect(mockPrisma.agent.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            where: { collection: TEST_COLLECTION.toBase58() },
          })
        );
      });

      it("should filter by registry", async () => {
        (mockPrisma.agent.findMany as any).mockResolvedValue([]);

        await resolvers.Query.agents(
          null,
          { registry: TEST_REGISTRY.toBase58() },
          ctx
        );

        expect(mockPrisma.agent.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            where: { registry: TEST_REGISTRY.toBase58() },
          })
        );
      });

      it("should apply ordering", async () => {
        (mockPrisma.agent.findMany as any).mockResolvedValue([]);

        await resolvers.Query.agents(
          null,
          { orderBy: "CREATED_AT_ASC" },
          ctx
        );

        expect(mockPrisma.agent.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            orderBy: { createdAt: "asc" },
          })
        );
      });

      it("should apply UPDATED_AT ordering", async () => {
        (mockPrisma.agent.findMany as any).mockResolvedValue([]);

        await resolvers.Query.agents(
          null,
          { orderBy: "UPDATED_AT_DESC" },
          ctx
        );

        expect(mockPrisma.agent.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            orderBy: { updatedAt: "desc" },
          })
        );
      });

      it("should apply UPDATED_AT_ASC ordering", async () => {
        (mockPrisma.agent.findMany as any).mockResolvedValue([]);

        await resolvers.Query.agents(
          null,
          { orderBy: "UPDATED_AT_ASC" },
          ctx
        );

        expect(mockPrisma.agent.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            orderBy: { updatedAt: "asc" },
          })
        );
      });

      it("should apply CREATED_AT_DESC ordering explicitly", async () => {
        (mockPrisma.agent.findMany as any).mockResolvedValue([]);

        await resolvers.Query.agents(
          null,
          { orderBy: "CREATED_AT_DESC" },
          ctx
        );

        expect(mockPrisma.agent.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            orderBy: { createdAt: "desc" },
          })
        );
      });
    });

    describe("feedback", () => {
      it("should return feedback by id", async () => {
        const mockFeedback = { id: "feedback-1", score: 85 };
        (mockPrisma.feedback.findUnique as any).mockResolvedValue(mockFeedback);

        const result = await resolvers.Query.feedback(
          null,
          { id: "feedback-1" },
          ctx
        );

        expect(result).toEqual(mockFeedback);
      });
    });

    describe("feedbacks", () => {
      it("should return feedbacks with filters", async () => {
        (mockPrisma.feedback.findMany as any).mockResolvedValue([]);

        await resolvers.Query.feedbacks(
          null,
          {
            agentId: TEST_ASSET.toBase58(),
            minScore: 70,
            maxScore: 100,
            tag: "quality",
            revoked: false,
          },
          ctx
        );

        expect(mockPrisma.feedback.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            where: expect.objectContaining({
              agentId: TEST_ASSET.toBase58(),
              revoked: false,
            }),
          })
        );
      });

      it("should filter by client", async () => {
        (mockPrisma.feedback.findMany as any).mockResolvedValue([]);

        await resolvers.Query.feedbacks(
          null,
          { client: TEST_CLIENT.toBase58() },
          ctx
        );

        expect(mockPrisma.feedback.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            where: { client: TEST_CLIENT.toBase58() },
          })
        );
      });

      it("should apply score ordering", async () => {
        (mockPrisma.feedback.findMany as any).mockResolvedValue([]);

        await resolvers.Query.feedbacks(
          null,
          { orderBy: "SCORE_DESC" },
          ctx
        );

        expect(mockPrisma.feedback.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            orderBy: { score: "desc" },
          })
        );
      });

      it("should apply SCORE_ASC ordering", async () => {
        (mockPrisma.feedback.findMany as any).mockResolvedValue([]);

        await resolvers.Query.feedbacks(
          null,
          { orderBy: "SCORE_ASC" },
          ctx
        );

        expect(mockPrisma.feedback.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            orderBy: { score: "asc" },
          })
        );
      });

      it("should apply CREATED_AT_ASC ordering", async () => {
        (mockPrisma.feedback.findMany as any).mockResolvedValue([]);

        await resolvers.Query.feedbacks(
          null,
          { orderBy: "CREATED_AT_ASC" },
          ctx
        );

        expect(mockPrisma.feedback.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            orderBy: { createdAt: "asc" },
          })
        );
      });

      it("should apply CREATED_AT_DESC ordering explicitly", async () => {
        (mockPrisma.feedback.findMany as any).mockResolvedValue([]);

        await resolvers.Query.feedbacks(
          null,
          { orderBy: "CREATED_AT_DESC" },
          ctx
        );

        expect(mockPrisma.feedback.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            orderBy: { createdAt: "desc" },
          })
        );
      });
    });

    describe("validation", () => {
      it("should return validation by id", async () => {
        const mockValidation = { id: "val-1", nonce: 1 };
        (mockPrisma.validation.findUnique as any).mockResolvedValue(
          mockValidation
        );

        const result = await resolvers.Query.validation(
          null,
          { id: "val-1" },
          ctx
        );

        expect(result).toEqual(mockValidation);
      });
    });

    describe("validations", () => {
      it("should filter by pending status", async () => {
        (mockPrisma.validation.findMany as any).mockResolvedValue([]);

        await resolvers.Query.validations(null, { pending: true }, ctx);

        expect(mockPrisma.validation.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            where: { response: null },
          })
        );
      });

      it("should filter by completed status", async () => {
        (mockPrisma.validation.findMany as any).mockResolvedValue([]);

        await resolvers.Query.validations(null, { pending: false }, ctx);

        expect(mockPrisma.validation.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            where: { response: { not: null } },
          })
        );
      });

      it("should filter by validator", async () => {
        (mockPrisma.validation.findMany as any).mockResolvedValue([]);

        await resolvers.Query.validations(
          null,
          { validator: TEST_VALIDATOR.toBase58() },
          ctx
        );

        expect(mockPrisma.validation.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            where: { validator: TEST_VALIDATOR.toBase58() },
          })
        );
      });

      it("should filter by requester", async () => {
        (mockPrisma.validation.findMany as any).mockResolvedValue([]);

        await resolvers.Query.validations(
          null,
          { requester: TEST_OWNER.toBase58() },
          ctx
        );

        expect(mockPrisma.validation.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            where: { requester: TEST_OWNER.toBase58() },
          })
        );
      });

      it("should filter by agentId", async () => {
        (mockPrisma.validation.findMany as any).mockResolvedValue([]);

        await resolvers.Query.validations(
          null,
          { agentId: TEST_ASSET.toBase58() },
          ctx
        );

        expect(mockPrisma.validation.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            where: { agentId: TEST_ASSET.toBase58() },
          })
        );
      });
    });

    describe("registry", () => {
      it("should return registry by id", async () => {
        const mockRegistry = { id: "reg-1", registryType: "Base" };
        (mockPrisma.registry.findUnique as any).mockResolvedValue(mockRegistry);

        const result = await resolvers.Query.registry(
          null,
          { id: "reg-1" },
          ctx
        );

        expect(result).toEqual(mockRegistry);
      });
    });

    describe("registries", () => {
      it("should filter by type", async () => {
        (mockPrisma.registry.findMany as any).mockResolvedValue([]);

        await resolvers.Query.registries(null, { registryType: "Base" }, ctx);

        expect(mockPrisma.registry.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            where: { registryType: "Base" },
          })
        );
      });

      it("should filter by authority", async () => {
        (mockPrisma.registry.findMany as any).mockResolvedValue([]);

        await resolvers.Query.registries(
          null,
          { authority: TEST_OWNER.toBase58() },
          ctx
        );

        expect(mockPrisma.registry.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            where: { authority: TEST_OWNER.toBase58() },
          })
        );
      });
    });

    describe("stats", () => {
      it("should return aggregated stats", async () => {
        (mockPrisma.agent.count as any).mockResolvedValue(10);
        (mockPrisma.feedback.count as any).mockResolvedValue(25);
        (mockPrisma.validation.count as any).mockResolvedValue(5);
        (mockPrisma.registry.count as any).mockResolvedValue(2);
        (mockPrisma.indexerState.findUnique as any).mockResolvedValue({
          lastSignature: TEST_SIGNATURE,
          lastSlot: TEST_SLOT,
          updatedAt: TEST_BLOCK_TIME,
        });

        const result = await resolvers.Query.stats(null, null, ctx);

        expect(result).toEqual({
          totalAgents: 10,
          totalFeedbacks: 25,
          totalValidations: 5,
          totalRegistries: 2,
          lastProcessedSignature: TEST_SIGNATURE,
          lastProcessedSlot: TEST_SLOT,
          updatedAt: TEST_BLOCK_TIME,
        });
      });

      it("should handle missing indexer state", async () => {
        (mockPrisma.agent.count as any).mockResolvedValue(0);
        (mockPrisma.feedback.count as any).mockResolvedValue(0);
        (mockPrisma.validation.count as any).mockResolvedValue(0);
        (mockPrisma.registry.count as any).mockResolvedValue(0);
        (mockPrisma.indexerState.findUnique as any).mockResolvedValue(null);

        const result = await resolvers.Query.stats(null, null, ctx);

        expect(result.lastProcessedSignature).toBeUndefined();
      });
    });

    describe("indexerStatus", () => {
      it("should return processor status", () => {
        const result = resolvers.Query.indexerStatus(null, null, ctx);

        expect(result).toEqual({
          running: true,
          mode: "auto",
          pollerActive: true,
          wsActive: true,
        });
      });
    });

    describe("searchAgents", () => {
      it("should search agents by query", async () => {
        (mockPrisma.agent.findMany as any).mockResolvedValue([]);

        await resolvers.Query.searchAgents(
          null,
          { query: "test", limit: 5 },
          ctx
        );

        expect(mockPrisma.agent.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            where: {
              OR: [
                { id: { contains: "test" } },
                { owner: { contains: "test" } },
                { nftName: { contains: "test", mode: "insensitive" } },
              ],
            },
            take: 5,
          })
        );
      });

      it("should use default limit when not specified", async () => {
        (mockPrisma.agent.findMany as any).mockResolvedValue([]);

        await resolvers.Query.searchAgents(null, { query: "test" }, ctx);

        expect(mockPrisma.agent.findMany).toHaveBeenCalledWith(
          expect.objectContaining({
            take: 10,
          })
        );
      });
    });
  });

  describe("Agent field resolvers", () => {
    const mockAgent = { id: TEST_ASSET.toBase58() };

    it("should resolve metadata", async () => {
      (mockPrisma.agentMetadata.findMany as any).mockResolvedValue([]);

      await resolvers.Agent.metadata(mockAgent, null, ctx);

      expect(mockPrisma.agentMetadata.findMany).toHaveBeenCalledWith({
        where: { agentId: mockAgent.id },
      });
    });

    it("should resolve feedbacks with filters", async () => {
      (mockPrisma.feedback.findMany as any).mockResolvedValue([]);

      await resolvers.Agent.feedbacks(
        mockAgent,
        { limit: 10, revoked: false },
        ctx
      );

      expect(mockPrisma.feedback.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { agentId: mockAgent.id, revoked: false },
          take: 10,
        })
      );
    });

    it("should resolve feedbacks with default limit", async () => {
      (mockPrisma.feedback.findMany as any).mockResolvedValue([]);

      await resolvers.Agent.feedbacks(mockAgent, {}, ctx);

      expect(mockPrisma.feedback.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          take: 50,
        })
      );
    });

    it("should resolve validations with pending true", async () => {
      (mockPrisma.validation.findMany as any).mockResolvedValue([]);

      await resolvers.Agent.validations(mockAgent, { pending: true }, ctx);

      expect(mockPrisma.validation.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { agentId: mockAgent.id, response: null },
        })
      );
    });

    it("should resolve validations with pending false", async () => {
      (mockPrisma.validation.findMany as any).mockResolvedValue([]);

      await resolvers.Agent.validations(mockAgent, { pending: false }, ctx);

      expect(mockPrisma.validation.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { agentId: mockAgent.id, response: { not: null } },
        })
      );
    });

    it("should resolve feedbackCount", async () => {
      (mockPrisma.feedback.count as any).mockResolvedValue(5);

      const result = await resolvers.Agent.feedbackCount(mockAgent, null, ctx);

      expect(result).toBe(5);
      expect(mockPrisma.feedback.count).toHaveBeenCalledWith({
        where: { agentId: mockAgent.id, revoked: false },
      });
    });

    it("should resolve averageScore", async () => {
      (mockPrisma.feedback.aggregate as any).mockResolvedValue({
        _avg: { score: 85.5 },
      });

      const result = await resolvers.Agent.averageScore(mockAgent, null, ctx);

      expect(result).toBe(85.5);
    });

    it("should resolve validationCount", async () => {
      (mockPrisma.validation.count as any).mockResolvedValue(3);

      const result = await resolvers.Agent.validationCount(
        mockAgent,
        null,
        ctx
      );

      expect(result).toBe(3);
    });
  });

  describe("Feedback field resolvers", () => {
    const mockFeedback = { id: "fb-1", agentId: TEST_ASSET.toBase58() };

    it("should resolve agent", async () => {
      const mockAgent = { id: mockFeedback.agentId };
      (mockPrisma.agent.findUnique as any).mockResolvedValue(mockAgent);

      const result = await resolvers.Feedback.agent(mockFeedback, null, ctx);

      expect(result).toEqual(mockAgent);
    });

    it("should resolve responses", async () => {
      (mockPrisma.feedbackResponse.findMany as any).mockResolvedValue([]);

      await resolvers.Feedback.responses(mockFeedback, null, ctx);

      expect(mockPrisma.feedbackResponse.findMany).toHaveBeenCalledWith({
        where: { feedbackId: mockFeedback.id },
        orderBy: { createdAt: "asc" },
      });
    });
  });

  describe("FeedbackResponse field resolvers", () => {
    it("should resolve feedback", async () => {
      const mockResponse = { feedbackId: "fb-1" };
      (mockPrisma.feedback.findUnique as any).mockResolvedValue({
        id: "fb-1",
      });

      await resolvers.FeedbackResponse.feedback(mockResponse, null, ctx);

      expect(mockPrisma.feedback.findUnique).toHaveBeenCalledWith({
        where: { id: "fb-1" },
      });
    });
  });

  describe("Validation field resolvers", () => {
    it("should resolve agent", async () => {
      const mockValidation = { agentId: TEST_ASSET.toBase58() };
      (mockPrisma.agent.findUnique as any).mockResolvedValue({ id: mockValidation.agentId });

      await resolvers.Validation.agent(mockValidation, null, ctx);

      expect(mockPrisma.agent.findUnique).toHaveBeenCalledWith({
        where: { id: mockValidation.agentId },
      });
    });

    it("should resolve isPending for pending validation", () => {
      const result = resolvers.Validation.isPending({ response: null });
      expect(result).toBe(true);
    });

    it("should resolve isPending for completed validation", () => {
      const result = resolvers.Validation.isPending({ response: 90 });
      expect(result).toBe(false);
    });
  });

  describe("Registry field resolvers", () => {
    it("should resolve agentCount", async () => {
      const mockRegistry = { collection: TEST_COLLECTION.toBase58() };
      (mockPrisma.agent.count as any).mockResolvedValue(10);

      const result = await resolvers.Registry.agentCount(
        mockRegistry,
        null,
        ctx
      );

      expect(result).toBe(10);
      expect(mockPrisma.agent.count).toHaveBeenCalledWith({
        where: { collection: mockRegistry.collection },
      });
    });
  });

  describe("Scalar resolvers", () => {
    describe("DateTime", () => {
      it("should serialize date to ISO string", () => {
        const date = new Date("2024-01-15T10:00:00Z");
        const result = resolvers.DateTime.serialize(date);
        expect(result).toBe("2024-01-15T10:00:00.000Z");
      });

      it("should parse ISO string to date", () => {
        const result = resolvers.DateTime.parseValue("2024-01-15T10:00:00Z");
        expect(result).toBeInstanceOf(Date);
      });
    });

    describe("BigInt", () => {
      it("should serialize bigint to string", () => {
        const result = resolvers.BigInt.serialize(12345678n);
        expect(result).toBe("12345678");
      });

      it("should parse string to bigint", () => {
        const result = resolvers.BigInt.parseValue("12345678");
        expect(result).toBe(12345678n);
      });
    });

    describe("Bytes", () => {
      it("should serialize buffer to hex string", () => {
        const buffer = Buffer.from([0xab, 0xcd, 0xef]);
        const result = resolvers.Bytes.serialize(buffer);
        expect(result).toBe("abcdef");
      });

      it("should parse hex string to buffer", () => {
        const result = resolvers.Bytes.parseValue("abcdef");
        expect(Buffer.isBuffer(result)).toBe(true);
        expect(result.toString("hex")).toBe("abcdef");
      });
    });
  });
});
