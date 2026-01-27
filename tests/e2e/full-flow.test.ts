import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { PrismaClient } from "@prisma/client";
import { PublicKey } from "@solana/web3.js";
import { handleEvent, EventContext } from "../../src/db/handlers.js";
import { createApiServer } from "../../src/api/server.js";
import type { ProgramEvent } from "../../src/parser/types.js";
import type { Express } from "express";
import type { Server } from "http";

// Test fixtures - valid base58 pubkeys (44 chars, no 0/I/O/l)
const TEST_AGENT_ID = new PublicKey(
  "AgentTest111111111111111111111111111111111A"
);
const TEST_OWNER = new PublicKey(
  "Testwner111111111111111111111111111111111B1"
);
const TEST_COLLECTION = new PublicKey(
  "CccLLectn11111111111111111111111111111111C1"
);
const TEST_REGISTRY = new PublicKey(
  "RegistryTst111111111111111111111111111111D1"
);
const TEST_CLIENT = new PublicKey(
  "CjientTest1111111111111111111111111111111E1"
);
const TEST_VALIDATOR = new PublicKey(
  "VaLidatr111111111111111111111111111111111F1"
);

describe("E2E: Full Indexer Flow", () => {
  let prisma: PrismaClient;
  let app: Express;
  let server: Server;
  const PORT = 4100;

  beforeAll(async () => {
    prisma = new PrismaClient();

    // Clean database
    try {
      await prisma.eventLog.deleteMany();
      await prisma.feedbackResponse.deleteMany();
      await prisma.orphanResponse.deleteMany();
      await prisma.validation.deleteMany();
      await prisma.feedback.deleteMany();
      await prisma.agentMetadata.deleteMany();
      await prisma.agent.deleteMany();
      await prisma.registry.deleteMany();
      await prisma.indexerState.deleteMany();
    } catch {
      // Tables may not exist
    }

    app = createApiServer({ prisma });
    server = app.listen(PORT);
  });

  afterAll(async () => {
    server?.close();
    await prisma.$disconnect();
  });

  const ctx: EventContext = {
    signature: "testSignature123",
    slot: 12345n,
    blockTime: new Date("2024-01-15T10:00:00Z"),
  };

  async function restGet(path: string) {
    const response = await fetch(`http://localhost:${PORT}${path}`);
    return response.json();
  }

  describe("Registry Creation Flow", () => {
    it("should create a base registry and query it via REST", async () => {
      // Simulate registry creation event
      const event: ProgramEvent = {
        type: "BaseRegistryCreated",
        data: {
          registry: TEST_REGISTRY,
          collection: TEST_COLLECTION,
          baseIndex: 0,
          createdBy: TEST_OWNER,
        },
      };

      await handleEvent(prisma, event, ctx);

      // Query via REST
      const result = await restGet(`/rest/v1/registries?collection=eq.${TEST_COLLECTION.toBase58()}`);

      expect(result).toHaveLength(1);
      expect(result[0].id).toBe(TEST_REGISTRY.toBase58());
      expect(result[0].collection).toBe(TEST_COLLECTION.toBase58());
      expect(result[0].registryType).toBe("Base");
    });

    it("should list registries", async () => {
      const result = await restGet("/rest/v1/registries");

      expect(result.length).toBeGreaterThan(0);
      expect(result[0].registryType).toBe("Base");
    });
  });

  describe("Agent Registration Flow", () => {
    it("should register an agent and query it", async () => {
      const event: ProgramEvent = {
        type: "AgentRegisteredInRegistry",
        data: {
          asset: TEST_AGENT_ID,
          registry: TEST_REGISTRY,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: true,
          agentUri: "https://example.com/agent.json",
        },
      };

      await handleEvent(prisma, event, ctx);

      const result = await restGet(`/rest/v1/agents?id=eq.${TEST_AGENT_ID.toBase58()}`);

      expect(result).toHaveLength(1);
      expect(result[0].asset).toBe(TEST_AGENT_ID.toBase58());
      expect(result[0].owner).toBe(TEST_OWNER.toBase58());
    });

    it("should list agents with filters", async () => {
      const result = await restGet(`/rest/v1/agents?owner=eq.${TEST_OWNER.toBase58()}`);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0].owner).toBe(TEST_OWNER.toBase58());
    });
  });

  describe("Metadata Flow", () => {
    it("should set metadata on agent", async () => {
      const event: ProgramEvent = {
        type: "MetadataSet",
        data: {
          asset: TEST_AGENT_ID,
          key: "description",
          value: Buffer.from("Test AI Agent"),
          immutable: false,
        },
      };

      await handleEvent(prisma, event, ctx);

      const result = await restGet(`/rest/v1/metadata?asset=eq.${TEST_AGENT_ID.toBase58()}`);

      expect(result.length).toBe(1);
      expect(result[0].key).toBe("description");
      expect(result[0].immutable).toBe(false);
    });

    it("should delete metadata", async () => {
      const event: ProgramEvent = {
        type: "MetadataDeleted",
        data: {
          asset: TEST_AGENT_ID,
          key: "description",
        },
      };

      await handleEvent(prisma, event, ctx);

      const result = await restGet(`/rest/v1/metadata?asset=eq.${TEST_AGENT_ID.toBase58()}`);

      expect(result.length).toBe(0);
    });
  });

  describe("Feedback Flow", () => {
    it("should create feedback on agent", async () => {
      const event: ProgramEvent = {
        type: "NewFeedback",
        data: {
          asset: TEST_AGENT_ID,
          clientAddress: TEST_CLIENT,
          feedbackIndex: 0n,
          value: 8500n,
          valueDecimals: 2,
          score: 85,
          tag1: "quality",
          tag2: "speed",
          endpoint: "/api/chat",
          feedbackUri: "ipfs://QmTest123",
          feedbackHash: Buffer.alloc(32).fill(1),
          atomEnabled: true,
          newTrustTier: 1,
          newQualityScore: 85,
          newConfidence: 50,
          newRiskScore: 10,
          newDiversityRatio: 100,
          isUniqueClient: true,
        },
      };

      await handleEvent(prisma, event, ctx);

      const result = await restGet(`/rest/v1/feedbacks?asset=eq.${TEST_AGENT_ID.toBase58()}`);

      expect(result.length).toBe(1);
      expect(result[0].score).toBe(85);
      expect(result[0].tag1).toBe("quality");
      expect(result[0].is_revoked).toBe(false);
    });

    it("should query feedbacks with filters", async () => {
      const result = await restGet(`/rest/v1/feedbacks?asset=eq.${TEST_AGENT_ID.toBase58()}`);

      expect(result.length).toBe(1);
      expect(result[0].score).toBe(85);
    });

    it("should add response to feedback", async () => {
      const event: ProgramEvent = {
        type: "ResponseAppended",
        data: {
          asset: TEST_AGENT_ID,
          client: TEST_CLIENT,
          feedbackIndex: 0n,
          responder: TEST_OWNER,
          responseUri: "ipfs://QmResponse123",
          responseHash: Buffer.alloc(32).fill(2),
        },
      };

      await handleEvent(prisma, event, ctx);

      const result = await restGet(
        `/rest/v1/responses?asset=eq.${TEST_AGENT_ID.toBase58()}&client_address=eq.${TEST_CLIENT.toBase58()}&feedback_index=eq.0`
      );

      expect(result.length).toBe(1);
      expect(result[0].responder).toBe(TEST_OWNER.toBase58());
    });

    it("should revoke feedback", async () => {
      const event: ProgramEvent = {
        type: "FeedbackRevoked",
        data: {
          asset: TEST_AGENT_ID,
          clientAddress: TEST_CLIENT,
          feedbackIndex: 0n,
          originalScore: 85,
          atomEnabled: true,
          hadImpact: true,
          newTrustTier: 0,
          newQualityScore: 0,
          newConfidence: 0,
        },
      };

      await handleEvent(prisma, event, ctx);

      const result = await restGet(`/rest/v1/feedbacks?asset=eq.${TEST_AGENT_ID.toBase58()}&is_revoked=eq.true`);

      expect(result.length).toBe(1);
      expect(result[0].is_revoked).toBe(true);
    });
  });

  describe("Validation Flow", () => {
    it("should create validation request", async () => {
      const event: ProgramEvent = {
        type: "ValidationRequested",
        data: {
          asset: TEST_AGENT_ID,
          validatorAddress: TEST_VALIDATOR,
          nonce: 1,
          requestUri: "ipfs://QmValidation123",
          requestHash: Buffer.alloc(32).fill(3),
          requester: TEST_OWNER,
        },
      };

      await handleEvent(prisma, event, ctx);

      const result = await restGet(`/rest/v1/validations?asset=eq.${TEST_AGENT_ID.toBase58()}&responded=eq.false`);

      expect(result.length).toBe(1);
      expect(result[0].nonce).toBe(1);
      expect(result[0].status).toBe("PENDING");
      expect(result[0].response).toBeNull();
    });

    it("should respond to validation", async () => {
      const event: ProgramEvent = {
        type: "ValidationResponded",
        data: {
          asset: TEST_AGENT_ID,
          validatorAddress: TEST_VALIDATOR,
          nonce: 1,
          response: 95,
          responseUri: "ipfs://QmValResponse123",
          responseHash: Buffer.alloc(32).fill(4),
          tag: "security",
        },
      };

      await handleEvent(prisma, event, ctx);

      const result = await restGet(`/rest/v1/validations?asset=eq.${TEST_AGENT_ID.toBase58()}&responded=eq.true`);

      expect(result.length).toBe(1);
      expect(result[0].response).toBe(95);
      expect(result[0].tag).toBe("security");
      expect(result[0].status).toBe("RESPONDED");
    });
  });

  describe("Stats and Health", () => {
    it("should return global stats", async () => {
      const result = await restGet("/rest/v1/stats");

      expect(result).toHaveLength(1);
      expect(result[0].total_agents).toBeGreaterThan(0);
      expect(result[0].total_feedbacks).toBeGreaterThan(0);
      expect(result[0].total_validations).toBeGreaterThan(0);
      expect(result[0].total_collections).toBeGreaterThan(0);
    });

    it("should return health check", async () => {
      const result = await restGet("/health");

      expect(result.status).toBe("ok");
    });
  });

  describe("Owner Sync Flow", () => {
    it("should sync owner change", async () => {
      const NEW_OWNER = new PublicKey(
        "Newwner1111111111111111111111111111111111G1"
      );

      const event: ProgramEvent = {
        type: "AgentOwnerSynced",
        data: {
          asset: TEST_AGENT_ID,
          oldOwner: TEST_OWNER,
          newOwner: NEW_OWNER,
        },
      };

      await handleEvent(prisma, event, ctx);

      const result = await restGet(`/rest/v1/agents?id=eq.${TEST_AGENT_ID.toBase58()}`);

      expect(result[0].owner).toBe(NEW_OWNER.toBase58());
    });
  });

  describe("Wallet Update Flow", () => {
    it("should update agent wallet", async () => {
      const WALLET = new PublicKey(
        "WaLLettTest111111111111111111111111111111H1"
      );

      const event: ProgramEvent = {
        type: "WalletUpdated",
        data: {
          asset: TEST_AGENT_ID,
          oldWallet: null,
          newWallet: WALLET,
          updatedBy: TEST_OWNER,
        },
      };

      await handleEvent(prisma, event, ctx);

      const result = await restGet(`/rest/v1/agents?id=eq.${TEST_AGENT_ID.toBase58()}`);

      expect(result[0].agent_wallet).toBe(WALLET.toBase58());
    });
  });

  describe("Leaderboard", () => {
    it("should return leaderboard", async () => {
      const result = await restGet("/rest/v1/leaderboard?limit=10");

      expect(Array.isArray(result)).toBe(true);
    });
  });

  describe("Collection Stats", () => {
    it("should return collection stats", async () => {
      const result = await restGet(`/rest/v1/collection_stats?collection=eq.${TEST_COLLECTION.toBase58()}`);

      expect(result).toHaveLength(1);
      expect(result[0].collection).toBe(TEST_COLLECTION.toBase58());
      expect(result[0].agent_count).toBeGreaterThan(0);
    });
  });
});
