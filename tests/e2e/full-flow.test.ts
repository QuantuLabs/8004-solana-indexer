import { describe, it, expect, vi, beforeAll, afterAll, beforeEach } from "vitest";
import { PrismaClient } from "@prisma/client";
import { PublicKey } from "@solana/web3.js";
import { handleEvent, EventContext } from "../../src/db/handlers.js";
import { createGraphQLServer } from "../../src/api/server.js";
import type { ProgramEvent } from "../../src/parser/types.js";

// Test fixtures
const TEST_AGENT_ID = new PublicKey(
  "AgentTestPubkey11111111111111111111111111111"
);
const TEST_OWNER = new PublicKey(
  "OwnerTestPubkey111111111111111111111111111111"
);
const TEST_COLLECTION = new PublicKey(
  "CollectionTest11111111111111111111111111111"
);
const TEST_REGISTRY = new PublicKey(
  "RegistryTestPbky11111111111111111111111111111"
);
const TEST_CLIENT = new PublicKey(
  "ClientTestPubkey11111111111111111111111111111"
);
const TEST_VALIDATOR = new PublicKey(
  "ValidatorPubkey1111111111111111111111111111111"
);

describe("E2E: Full Indexer Flow", () => {
  let prisma: PrismaClient;
  let server: Awaited<ReturnType<typeof createGraphQLServer>>;
  let mockProcessor: any;
  const PORT = 4100;

  beforeAll(async () => {
    prisma = new PrismaClient();

    // Clean database
    try {
      await prisma.eventLog.deleteMany();
      await prisma.feedbackResponse.deleteMany();
      await prisma.validation.deleteMany();
      await prisma.feedback.deleteMany();
      await prisma.agentMetadata.deleteMany();
      await prisma.agent.deleteMany();
      await prisma.registry.deleteMany();
      await prisma.indexerState.deleteMany();
    } catch (e) {
      // Tables may not exist
    }

    mockProcessor = {
      getStatus: vi.fn().mockReturnValue({
        running: true,
        mode: "polling",
        pollerActive: true,
        wsActive: false,
      }),
    };

    server = await createGraphQLServer({
      prisma,
      processor: mockProcessor,
      port: PORT,
    });

    await server.start();
  });

  afterAll(async () => {
    await server.stop();
    await prisma.$disconnect();
  });

  const ctx: EventContext = {
    signature: "testSignature123",
    slot: 12345n,
    blockTime: new Date("2024-01-15T10:00:00Z"),
  };

  async function graphqlQuery(query: string, variables?: any) {
    const response = await fetch(`http://localhost:${PORT}/graphql`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, variables }),
    });
    return response.json();
  }

  describe("Registry Creation Flow", () => {
    it("should create a base registry and query it via GraphQL", async () => {
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

      // Query via GraphQL
      const result = await graphqlQuery(`{
        registry(id: "${TEST_REGISTRY.toBase58()}") {
          id
          collection
          registryType
          baseIndex
        }
      }`);

      expect(result.data.registry).toEqual({
        id: TEST_REGISTRY.toBase58(),
        collection: TEST_COLLECTION.toBase58(),
        registryType: "Base",
        baseIndex: 0,
      });
    });

    it("should list registries", async () => {
      const result = await graphqlQuery(`{
        registries {
          id
          registryType
        }
      }`);

      expect(result.data.registries.length).toBeGreaterThan(0);
      expect(result.data.registries[0].registryType).toBe("Base");
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
        },
      };

      await handleEvent(prisma, event, ctx);

      const result = await graphqlQuery(`{
        agent(id: "${TEST_AGENT_ID.toBase58()}") {
          id
          owner
          registry
          collection
          feedbackCount
          validationCount
        }
      }`);

      expect(result.data.agent.id).toBe(TEST_AGENT_ID.toBase58());
      expect(result.data.agent.owner).toBe(TEST_OWNER.toBase58());
      expect(result.data.agent.feedbackCount).toBe(0);
    });

    it("should list agents with filters", async () => {
      const result = await graphqlQuery(`{
        agents(owner: "${TEST_OWNER.toBase58()}") {
          id
          owner
        }
      }`);

      expect(result.data.agents.length).toBeGreaterThan(0);
      expect(result.data.agents[0].owner).toBe(TEST_OWNER.toBase58());
    });

    it("should search agents", async () => {
      // First update the agent's URI to have searchable content
      const updateEvent: ProgramEvent = {
        type: "UriUpdated",
        data: {
          asset: TEST_AGENT_ID,
          newUri: "https://example.com/agent.json",
          updatedBy: TEST_OWNER,
        },
      };
      await handleEvent(prisma, updateEvent, ctx);

      const result = await graphqlQuery(`{
        searchAgents(query: "${TEST_AGENT_ID.toBase58().slice(0, 10)}", limit: 5) {
          id
          owner
        }
      }`);

      expect(result.data.searchAgents.length).toBeGreaterThan(0);
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

      const result = await graphqlQuery(`{
        agent(id: "${TEST_AGENT_ID.toBase58()}") {
          id
          metadata {
            key
            immutable
          }
        }
      }`);

      expect(result.data.agent.metadata.length).toBe(1);
      expect(result.data.agent.metadata[0].key).toBe("description");
      expect(result.data.agent.metadata[0].immutable).toBe(false);
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

      const result = await graphqlQuery(`{
        agent(id: "${TEST_AGENT_ID.toBase58()}") {
          metadata {
            key
          }
        }
      }`);

      expect(result.data.agent.metadata.length).toBe(0);
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
          score: 85,
          tag1: "quality",
          tag2: "speed",
          endpoint: "/api/chat",
          feedbackUri: "ipfs://QmTest123",
          feedbackHash: Buffer.alloc(32).fill(1),
        },
      };

      await handleEvent(prisma, event, ctx);

      const result = await graphqlQuery(`{
        agent(id: "${TEST_AGENT_ID.toBase58()}") {
          id
          feedbackCount
          averageScore
          feedbacks {
            score
            tag1
            tag2
            endpoint
            revoked
          }
        }
      }`);

      expect(result.data.agent.feedbackCount).toBe(1);
      expect(result.data.agent.averageScore).toBe(85);
      expect(result.data.agent.feedbacks[0].score).toBe(85);
      expect(result.data.agent.feedbacks[0].tag1).toBe("quality");
      expect(result.data.agent.feedbacks[0].revoked).toBe(false);
    });

    it("should query feedbacks directly", async () => {
      const result = await graphqlQuery(`{
        feedbacks(agentId: "${TEST_AGENT_ID.toBase58()}", minScore: 80) {
          id
          score
          tag1
          client
        }
      }`);

      expect(result.data.feedbacks.length).toBe(1);
      expect(result.data.feedbacks[0].score).toBe(85);
    });

    it("should add response to feedback", async () => {
      // First get the feedback ID
      const feedbacks = await graphqlQuery(`{
        feedbacks(agentId: "${TEST_AGENT_ID.toBase58()}") {
          id
        }
      }`);

      const event: ProgramEvent = {
        type: "ResponseAppended",
        data: {
          asset: TEST_AGENT_ID,
          feedbackIndex: 0n,
          responder: TEST_OWNER,
          responseUri: "ipfs://QmResponse123",
          responseHash: Buffer.alloc(32).fill(2),
        },
      };

      await handleEvent(prisma, event, ctx);

      const result = await graphqlQuery(`{
        feedbacks(agentId: "${TEST_AGENT_ID.toBase58()}") {
          id
          responses {
            responder
            responseUri
          }
        }
      }`);

      expect(result.data.feedbacks[0].responses.length).toBe(1);
      expect(result.data.feedbacks[0].responses[0].responder).toBe(
        TEST_OWNER.toBase58()
      );
    });

    it("should revoke feedback", async () => {
      const event: ProgramEvent = {
        type: "FeedbackRevoked",
        data: {
          asset: TEST_AGENT_ID,
          clientAddress: TEST_CLIENT,
          feedbackIndex: 0n,
        },
      };

      await handleEvent(prisma, event, ctx);

      const result = await graphqlQuery(`{
        feedbacks(agentId: "${TEST_AGENT_ID.toBase58()}", revoked: true) {
          id
          revoked
        }
      }`);

      expect(result.data.feedbacks[0].revoked).toBe(true);

      // Non-revoked should be empty now
      const activeResult = await graphqlQuery(`{
        feedbacks(agentId: "${TEST_AGENT_ID.toBase58()}", revoked: false) {
          id
        }
      }`);

      expect(activeResult.data.feedbacks.length).toBe(0);
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

      const result = await graphqlQuery(`{
        validations(agentId: "${TEST_AGENT_ID.toBase58()}", pending: true) {
          id
          validator
          nonce
          requestUri
          isPending
          response
        }
      }`);

      expect(result.data.validations.length).toBe(1);
      expect(result.data.validations[0].nonce).toBe(1);
      expect(result.data.validations[0].isPending).toBe(true);
      expect(result.data.validations[0].response).toBeNull();
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

      const result = await graphqlQuery(`{
        validations(agentId: "${TEST_AGENT_ID.toBase58()}", pending: false) {
          id
          validator
          response
          responseUri
          tag
          isPending
        }
      }`);

      expect(result.data.validations.length).toBe(1);
      expect(result.data.validations[0].response).toBe(95);
      expect(result.data.validations[0].tag).toBe("security");
      expect(result.data.validations[0].isPending).toBe(false);
    });

    it("should query agent validations", async () => {
      const result = await graphqlQuery(`{
        agent(id: "${TEST_AGENT_ID.toBase58()}") {
          validationCount
          validations {
            validator
            response
          }
        }
      }`);

      expect(result.data.agent.validationCount).toBe(1);
      expect(result.data.agent.validations[0].response).toBe(95);
    });
  });

  describe("Stats and Status", () => {
    it("should return indexer stats", async () => {
      const result = await graphqlQuery(`{
        stats {
          totalAgents
          totalFeedbacks
          totalValidations
          totalRegistries
        }
      }`);

      expect(result.data.stats.totalAgents).toBeGreaterThan(0);
      expect(result.data.stats.totalFeedbacks).toBeGreaterThan(0);
      expect(result.data.stats.totalValidations).toBeGreaterThan(0);
      expect(result.data.stats.totalRegistries).toBeGreaterThan(0);
    });

    it("should return indexer status", async () => {
      const result = await graphqlQuery(`{
        indexerStatus {
          running
          mode
          pollerActive
          wsActive
        }
      }`);

      expect(result.data.indexerStatus.running).toBe(true);
      expect(result.data.indexerStatus.mode).toBe("polling");
      expect(result.data.indexerStatus.pollerActive).toBe(true);
    });
  });

  describe("Owner Sync Flow", () => {
    it("should sync owner change", async () => {
      const NEW_OWNER = new PublicKey(
        "NewOwnerPubkey11111111111111111111111111111"
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

      const result = await graphqlQuery(`{
        agent(id: "${TEST_AGENT_ID.toBase58()}") {
          owner
        }
      }`);

      expect(result.data.agent.owner).toBe(NEW_OWNER.toBase58());
    });
  });

  describe("Wallet Update Flow", () => {
    it("should update agent wallet", async () => {
      const WALLET = new PublicKey(
        "WalletPubkeyTest1111111111111111111111111111"
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

      const result = await graphqlQuery(`{
        agent(id: "${TEST_AGENT_ID.toBase58()}") {
          wallet
        }
      }`);

      expect(result.data.agent.wallet).toBe(WALLET.toBase58());
    });
  });
});
