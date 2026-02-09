/**
 * E2E Tests for Reorg Resilience
 *
 * Tests cover:
 * 1. Status columns on all models (PENDING default)
 * 2. Atomic ingestion (event + cursor in single transaction)
 * 3. Verification worker (PENDING → FINALIZED / ORPHANED transitions)
 * 4. API status filtering (default excludes ORPHANED)
 * 5. Reentrancy guard for verifier
 * 6. Verification stats endpoint
 */

import { describe, it, expect, beforeAll, afterAll, vi } from "vitest";
import { PublicKey, Connection } from "@solana/web3.js";
import { handleEvent, handleEventAtomic, EventContext } from "../../src/db/handlers.js";
import { createApiServer } from "../../src/api/server.js";
import { DataVerifier } from "../../src/indexer/verifier.js";
import type { ProgramEvent } from "../../src/parser/types.js";
import type { Express } from "express";
import type { Server } from "http";
import { prisma } from "./setup.js";

// Test fixtures - valid generated pubkeys
const TEST_AGENT_REORG = new PublicKey("BKCosZCjMMBzUwpEfyqdZro4rqwoeaZpLVinquLq7XmU");
const TEST_AGENT_ORPHAN = new PublicKey("H2N5JEbQNyHJpve2c9fvY1eLCbQH8QS7QEhA9NHXv7bi");
const TEST_OWNER = new PublicKey("B3dfk9bKtYeDi3fiy69eHWgf2uketLa16JnENYWLoXVU");
const TEST_COLLECTION = new PublicKey("EnhL4ezhwEmCdfD9pAKkwWDARTiK3Gh8p4qoSGDwzsEM");
const TEST_REGISTRY = new PublicKey("H8H8MB6x7N1yniUsEUkDS3fDFqmjjUUHvVSAbkpWF7ZN");
const TEST_CLIENT = new PublicKey("J7iPcZnFVyHs7ipu7YVgzacYz4MMLhD7p57B8jRjmtqk");
const TEST_VALIDATOR = new PublicKey("EvpsgCGoqhMUjxFaYouj9KKhqQ4RoknmPjnvsrRUU2ND");

describe("E2E: Reorg Resilience", () => {
  let app: Express;
  let server: Server;
  const PORT = 4200;

  beforeAll(async () => {
    app = createApiServer({ prisma });
    server = app.listen(PORT);
  });

  afterAll(async () => {
    server?.close();
  });

  async function restGet(path: string) {
    const response = await fetch(`http://localhost:${PORT}${path}`);
    return response.json();
  }

  // =========================================================================
  // Phase 1: Status Columns - All models default to PENDING
  // =========================================================================

  describe("1. Status Columns - Default PENDING", () => {
    const ctx: EventContext = {
      signature: "reorgTestSig1",
      slot: 100000n,
      blockTime: new Date("2024-01-20T10:00:00Z"),
    };

    it("should create registry with status=PENDING", async () => {
      const event: ProgramEvent = {
        type: "RegistryInitialized",
        data: {
          collection: TEST_COLLECTION,
          authority: TEST_OWNER,
        },
      };

      await handleEvent(prisma, event, ctx);

      const registry = await prisma.registry.findUnique({
        where: { id: TEST_COLLECTION.toBase58() },
      });

      expect(registry).not.toBeNull();
      expect(registry!.status).toBe("PENDING");
      expect(registry!.verifiedAt).toBeNull();
    });

    it("should create agent with status=PENDING", async () => {
      const event: ProgramEvent = {
        type: "AgentRegistered",
        data: {
          asset: TEST_AGENT_REORG,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: true,
          agentUri: "https://example.com/reorg-agent.json",
        },
      };

      await handleEvent(prisma, event, ctx);

      const agent = await prisma.agent.findUnique({
        where: { id: TEST_AGENT_REORG.toBase58() },
      });

      expect(agent).not.toBeNull();
      expect(agent!.status).toBe("PENDING");
      expect(agent!.verifiedAt).toBeNull();
      expect(agent!.verifiedSlot).toBeNull();
    });

    it("should create metadata with status=PENDING", async () => {
      const event: ProgramEvent = {
        type: "MetadataSet",
        data: {
          asset: TEST_AGENT_REORG,
          key: "reorg_test_key",
          value: Buffer.from("test value"),
          immutable: false,
        },
      };

      await handleEvent(prisma, event, ctx);

      const metadata = await prisma.agentMetadata.findFirst({
        where: {
          agentId: TEST_AGENT_REORG.toBase58(),
          key: "reorg_test_key",
        },
      });

      expect(metadata).not.toBeNull();
      expect(metadata!.status).toBe("PENDING");
      expect(metadata!.verifiedAt).toBeNull();
    });

    it("should create feedback with status=PENDING", async () => {
      const event: ProgramEvent = {
        type: "NewFeedback",
        data: {
          asset: TEST_AGENT_REORG,
          clientAddress: TEST_CLIENT,
          feedbackIndex: 0n,
          slot: 100000n,
          value: 9000n,
          valueDecimals: 2,
          score: 90,
          tag1: "reorg_test",
          tag2: "test",
          endpoint: "/api/test",
          feedbackUri: "ipfs://QmReorgTest",
          feedbackFileHash: null,
          sealHash: new Uint8Array(32).fill(1),
          atomEnabled: true,
          newFeedbackDigest: new Uint8Array(32).fill(0xaa),
          newFeedbackCount: 1n,
          newTrustTier: 1,
          newQualityScore: 9000,
          newConfidence: 5000,
          newRiskScore: 5,
          newDiversityRatio: 100,
          isUniqueClient: true,
        },
      };

      await handleEvent(prisma, event, ctx);

      const feedback = await prisma.feedback.findFirst({
        where: {
          agentId: TEST_AGENT_REORG.toBase58(),
          client: TEST_CLIENT.toBase58(),
        },
      });

      expect(feedback).not.toBeNull();
      expect(feedback!.status).toBe("PENDING");
      expect(feedback!.verifiedAt).toBeNull();
    });

    it("should create feedback response with status=PENDING", async () => {
      const event: ProgramEvent = {
        type: "ResponseAppended",
        data: {
          asset: TEST_AGENT_REORG,
          client: TEST_CLIENT,
          feedbackIndex: 0n,
          slot: 100001n,
          responder: TEST_OWNER,
          responseUri: "ipfs://QmReorgResponse",
          responseHash: new Uint8Array(32).fill(2),
          sealHash: new Uint8Array(32).fill(1),
          newResponseDigest: new Uint8Array(32).fill(0xbb),
          newResponseCount: 1n,
        },
      };

      await handleEvent(prisma, event, ctx);

      const response = await prisma.feedbackResponse.findFirst({
        where: {
          feedback: {
            agentId: TEST_AGENT_REORG.toBase58(),
            client: TEST_CLIENT.toBase58(),
          },
        },
      });

      expect(response).not.toBeNull();
      expect(response!.status).toBe("PENDING");
      expect(response!.verifiedAt).toBeNull();
    });

    it("should create validation with chainStatus=PENDING", async () => {
      const event: ProgramEvent = {
        type: "ValidationRequested",
        data: {
          asset: TEST_AGENT_REORG,
          validatorAddress: TEST_VALIDATOR,
          nonce: 1n,
          requestUri: "ipfs://QmReorgValidation",
          requestHash: new Uint8Array(32).fill(3),
          requester: TEST_OWNER,
        },
      };

      await handleEvent(prisma, event, ctx);

      const validation = await prisma.validation.findFirst({
        where: {
          agentId: TEST_AGENT_REORG.toBase58(),
          validator: TEST_VALIDATOR.toBase58(),
          nonce: 1n,
        },
      });

      expect(validation).not.toBeNull();
      expect(validation!.chainStatus).toBe("PENDING");
      expect(validation!.chainVerifiedAt).toBeNull();
    });
  });

  // =========================================================================
  // Phase 2: Atomic Ingestion
  // =========================================================================

  describe("2. Atomic Ingestion", () => {
    it("should update cursor atomically with event", async () => {
      const ctx: EventContext = {
        signature: "atomicTestSig",
        slot: 200000n,
        blockTime: new Date("2024-01-21T10:00:00Z"),
      };

      const newAgent = new PublicKey("2Bip1P7aedFwmy1jbDuP3btdeQMAVWzgMHU4s6sbt1Mg");

      const event: ProgramEvent = {
        type: "AgentRegistered",
        data: {
          asset: newAgent,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: false,
          agentUri: "https://example.com/atomic-test.json",
        },
      };

      // Use atomic handler
      await handleEventAtomic(prisma, event, ctx);

      // Verify agent was created
      const agent = await prisma.agent.findUnique({
        where: { id: newAgent.toBase58() },
      });
      expect(agent).not.toBeNull();

      // Verify cursor was updated (uses IndexerState model)
      const cursor = await prisma.indexerState.findUnique({
        where: { id: "main" },
      });
      expect(cursor).not.toBeNull();
      expect(cursor!.lastSlot).toBe(200000n);
      expect(cursor!.lastSignature).toBe("atomicTestSig");
    });

    it("should enforce monotonic cursor (reject older slots)", async () => {
      // Try to insert event with older slot
      const ctx: EventContext = {
        signature: "oldSlotSig",
        slot: 100n, // Much older than 200000n from previous test
        blockTime: new Date("2024-01-01T10:00:00Z"),
      };

      const oldAgent = new PublicKey("8yGjVHBQaxZyDWEZ3Cj6UrKag4qDnUX3d7ygiJoCZYu1");

      const event: ProgramEvent = {
        type: "AgentRegistered",
        data: {
          asset: oldAgent,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: false,
          agentUri: "https://example.com/old-test.json",
        },
      };

      // Event should still be processed (we don't reject events)
      await handleEventAtomic(prisma, event, ctx);

      // Agent should exist
      const agent = await prisma.agent.findUnique({
        where: { id: oldAgent.toBase58() },
      });
      expect(agent).not.toBeNull();

      // But cursor should NOT have regressed (monotonic guard)
      const cursor = await prisma.indexerState.findUnique({
        where: { id: "main" },
      });
      expect(cursor!.lastSlot).toBe(200000n); // Still at higher slot
    });
  });

  // =========================================================================
  // Phase 3: Verification Worker - Status Transitions
  // =========================================================================

  describe("3. Verification Worker", () => {
    it("should finalize agent when it exists on-chain", async () => {
      // Mock connection that returns account info for TEST_AGENT_REORG
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(300000),
        getAccountInfo: vi.fn().mockImplementation(async (pubkey: PublicKey) => {
          const key = pubkey.toBase58();
          if (key === TEST_AGENT_REORG.toBase58()) {
            return { data: Buffer.alloc(100), lamports: 1000000 };
          }
          return null;
        }),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      // Set isRunning to allow verification to proceed
      (verifier as any).isRunning = true;

      // Update agent to ensure it's at an old slot (past cutoff)
      await prisma.agent.update({
        where: { id: TEST_AGENT_REORG.toBase58() },
        data: { status: "PENDING", createdSlot: 1000n },
      });

      // Run verification manually
      await (verifier as any).verifyAgents(290000n);

      const agent = await prisma.agent.findUnique({
        where: { id: TEST_AGENT_REORG.toBase58() },
      });

      expect(agent!.status).toBe("FINALIZED");
      expect(agent!.verifiedAt).not.toBeNull();
      expect(agent!.verifiedSlot).toBe(290000n);
    });

    it("should orphan agent when it does not exist on-chain", async () => {
      // Mock connection that returns null for all accounts
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(300000),
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      // Create agent that doesn't exist on-chain
      const orphanAgent = TEST_AGENT_ORPHAN.toBase58();

      await prisma.agent.upsert({
        where: { id: orphanAgent },
        create: {
          id: orphanAgent,
          owner: TEST_OWNER.toBase58(),
          collection: TEST_COLLECTION.toBase58(),
          registry: TEST_COLLECTION.toBase58(),
          nftName: "Orphan Agent",
          uri: "https://example.com/orphan.json",
          status: "PENDING",
          createdSlot: 1000n,
        },
        update: { status: "PENDING", createdSlot: 1000n },
      });

      // Run verification
      await (verifier as any).verifyAgents(290000n);

      const agent = await prisma.agent.findUnique({
        where: { id: orphanAgent },
      });

      expect(agent!.status).toBe("ORPHANED");
      expect(agent!.verifiedAt).not.toBeNull();
    });

    it("should not verify agents newer than cutoff slot", async () => {
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(300000),
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      const newAgent = new PublicKey("EzzcEdDFp6i4cinsHQFJb638ZG8dGQTSL2eCceFMNFG3");

      await prisma.agent.upsert({
        where: { id: newAgent.toBase58() },
        create: {
          id: newAgent.toBase58(),
          owner: TEST_OWNER.toBase58(),
          collection: TEST_COLLECTION.toBase58(),
          registry: TEST_COLLECTION.toBase58(),
          nftName: "New Agent",
          uri: "https://example.com/new.json",
          status: "PENDING",
          createdSlot: 295000n, // After cutoff (290000)
        },
        update: { status: "PENDING", createdSlot: 295000n },
      });

      // Run verification with cutoff at 290000
      await (verifier as any).verifyAgents(290000n);

      const agent = await prisma.agent.findUnique({
        where: { id: newAgent.toBase58() },
      });

      // Should still be PENDING (not verified yet)
      expect(agent!.status).toBe("PENDING");
    });

    it("should return stats after verification", async () => {
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(300000),
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      const stats = verifier.getStats();

      expect(stats).toHaveProperty("agentsVerified");
      expect(stats).toHaveProperty("agentsOrphaned");
      expect(stats).toHaveProperty("feedbacksVerified");
      expect(stats).toHaveProperty("validationsVerified");
      expect(stats).toHaveProperty("lastRunAt");
    });

    it("should have reentrancy guard (skip overlapping cycles)", async () => {
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(300000),
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      // Set verifyInProgress flag
      (verifier as any).verifyInProgress = true;

      // This call should be skipped
      await (verifier as any).verifyAll();

      // Should not have changed lastRunAt
      expect(verifier.getStats().lastRunAt).toBeNull();

      // Reset flag
      (verifier as any).verifyInProgress = false;
    });
  });

  // =========================================================================
  // Phase 4: API Status Filtering
  // =========================================================================

  describe("4. API Status Filtering", () => {
    beforeAll(async () => {
      // Ensure we have agents in different states
      // FINALIZED agent
      await prisma.agent.update({
        where: { id: TEST_AGENT_REORG.toBase58() },
        data: { status: "FINALIZED" },
      });

      // ORPHANED agent
      await prisma.agent.update({
        where: { id: TEST_AGENT_ORPHAN.toBase58() },
        data: { status: "ORPHANED" },
      });
    });

    it("should exclude ORPHANED agents by default", async () => {
      const result = await restGet("/rest/v1/agents");

      // Should not include orphaned agent (API maps id to asset)
      const orphanedAgent = result.find(
        (a: any) => a.asset === TEST_AGENT_ORPHAN.toBase58()
      );
      expect(orphanedAgent).toBeUndefined();

      // Should include finalized agent
      const finalizedAgent = result.find(
        (a: any) => a.asset === TEST_AGENT_REORG.toBase58()
      );
      expect(finalizedAgent).toBeDefined();
    });

    it("should filter by status=FINALIZED", async () => {
      const result = await restGet("/rest/v1/agents?status=eq.FINALIZED");

      // All returned agents should be FINALIZED
      for (const agent of result) {
        expect(agent.status).toBe("FINALIZED");
      }
    });

    it("should filter by status=PENDING", async () => {
      const result = await restGet("/rest/v1/agents?status=eq.PENDING");

      // All returned agents should be PENDING
      for (const agent of result) {
        expect(agent.status).toBe("PENDING");
      }
    });

    it("should include ORPHANED when explicitly requested", async () => {
      const result = await restGet("/rest/v1/agents?status=eq.ORPHANED");

      // Should only return orphaned agents
      expect(result.length).toBeGreaterThan(0);
      for (const agent of result) {
        expect(agent.status).toBe("ORPHANED");
      }
    });

    it("should filter feedbacks by status", async () => {
      // Update a feedback to FINALIZED
      await prisma.feedback.updateMany({
        where: { agentId: TEST_AGENT_REORG.toBase58() },
        data: { status: "FINALIZED" },
      });

      const result = await restGet(
        `/rest/v1/feedbacks?asset=eq.${TEST_AGENT_REORG.toBase58()}&status=eq.FINALIZED`
      );

      for (const feedback of result) {
        expect(feedback.status).toBe("FINALIZED");
      }
    });

    it("should filter validations by chainStatus", async () => {
      const result = await restGet("/rest/v1/validations?chain_status=eq.PENDING");

      for (const validation of result) {
        expect(validation.chain_status).toBe("PENDING");
      }
    });
  });

  // =========================================================================
  // Phase 5: Verification Stats
  // =========================================================================

  describe("5. Verification Stats", () => {
    it("should count records by status", async () => {
      // Check agent counts by status directly
      const pending = await prisma.agent.count({ where: { status: "PENDING" } });
      const finalized = await prisma.agent.count({ where: { status: "FINALIZED" } });
      const orphaned = await prisma.agent.count({ where: { status: "ORPHANED" } });

      expect(pending).toBeGreaterThanOrEqual(0);
      expect(finalized).toBeGreaterThanOrEqual(0);
      expect(orphaned).toBeGreaterThanOrEqual(0);
    });

    it("should track verification stats in verifier", async () => {
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(400000),
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      // Create a test agent to verify
      const testAgent = new PublicKey("6FT1svEzknCatpeTKu9CoWJ4D3GTaaBbeFGQRwmGPWMf");
      await prisma.agent.upsert({
        where: { id: testAgent.toBase58() },
        create: {
          id: testAgent.toBase58(),
          owner: TEST_OWNER.toBase58(),
          collection: TEST_COLLECTION.toBase58(),
          registry: TEST_COLLECTION.toBase58(),
          nftName: "Stats Test Agent",
          uri: "https://example.com/stats.json",
          status: "PENDING",
          createdSlot: 1000n,
        },
        update: { status: "PENDING", createdSlot: 1000n },
      });

      // Run verification
      await (verifier as any).verifyAgents(390000n);

      const stats = verifier.getStats();
      expect(stats.agentsOrphaned).toBeGreaterThan(0);
    });
  });

  // =========================================================================
  // Phase 6: Feedback/Response Cascade Orphaning
  // =========================================================================

  describe("6. Cascade Orphaning", () => {
    it("should orphan feedback when agent does not exist", async () => {
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(400000),
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;
      const orphanAgent = TEST_AGENT_ORPHAN.toBase58();

      // Create feedback for orphaned agent
      const feedback = await prisma.feedback.upsert({
        where: {
          agentId_client_feedbackIndex: {
            agentId: orphanAgent,
            client: TEST_CLIENT.toBase58(),
            feedbackIndex: 99n,
          },
        },
        create: {
          agentId: orphanAgent,
          client: TEST_CLIENT.toBase58(),
          feedbackIndex: 99n,
          score: 50,
          value: 5000n,
          valueDecimals: 2,
          tag1: "cascade_test",
          tag2: "",
          endpoint: "/test",
          feedbackUri: "ipfs://cascade",
          feedbackHash: Buffer.alloc(32).fill(5),
          status: "PENDING",
          createdSlot: 1000n,
        },
        update: { status: "PENDING" },
      });

      await (verifier as any).verifyFeedbacks(390000n);

      // Check feedback was orphaned
      const updatedFeedback = await prisma.feedback.findUnique({
        where: { id: feedback.id },
      });

      expect(updatedFeedback!.status).toBe("ORPHANED");
    });

    it("should orphan response when parent feedback is orphaned", async () => {
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(400000),
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;
      const orphanAgent = TEST_AGENT_ORPHAN.toBase58();

      // Find or create feedback and ensure it's orphaned
      const feedback = await prisma.feedback.findFirst({
        where: {
          agentId: orphanAgent,
          client: TEST_CLIENT.toBase58(),
        },
      });

      if (feedback) {
        await prisma.feedback.update({
          where: { id: feedback.id },
          data: { status: "ORPHANED" },
        });

        // Create response
        const response = await prisma.feedbackResponse.create({
          data: {
            feedbackId: feedback.id,
            responder: TEST_OWNER.toBase58(),
            responseUri: "ipfs://cascade_response",
            responseHash: Buffer.alloc(32).fill(6),
            status: "PENDING",
            slot: 1000n,
            txSignature: "cascadeResponseSig",
          },
        });

        await (verifier as any).verifyFeedbackResponses(390000n);

        // Check response was orphaned (cascade from parent)
        const updatedResponse = await prisma.feedbackResponse.findUnique({
          where: { id: response.id },
        });

        expect(updatedResponse!.status).toBe("ORPHANED");
      }
    });

    it("should finalize feedback when agent exists at finalized", async () => {
      // Mock connection where agent exists
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(400000),
        getAccountInfo: vi.fn().mockResolvedValue({ data: Buffer.alloc(100), lamports: 1000000 }),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      // Ensure agent exists first (for FK constraint)
      await prisma.agent.upsert({
        where: { id: TEST_AGENT_REORG.toBase58() },
        create: {
          id: TEST_AGENT_REORG.toBase58(),
          owner: TEST_OWNER.toBase58(),
          uri: "https://test.com/agent.json",
          nftName: "Finalize Test Agent",
          collection: TEST_COLLECTION.toBase58(),
          registry: TEST_COLLECTION.toBase58(),
          atomEnabled: true,
          status: "PENDING",
        },
        update: {},
      });

      // Create feedback for existing agent
      const feedback = await prisma.feedback.upsert({
        where: {
          agentId_client_feedbackIndex: {
            agentId: TEST_AGENT_REORG.toBase58(),
            client: TEST_CLIENT.toBase58(),
            feedbackIndex: 100n,
          },
        },
        create: {
          agentId: TEST_AGENT_REORG.toBase58(),
          client: TEST_CLIENT.toBase58(),
          feedbackIndex: 100n,
          score: 85,
          value: 8500n,
          valueDecimals: 2,
          tag1: "finalize_test",
          tag2: "",
          endpoint: "/test",
          feedbackUri: "ipfs://finalize",
          feedbackHash: Buffer.alloc(32).fill(7),
          status: "PENDING",
          createdSlot: 1000n,
        },
        update: { status: "PENDING", verifiedAt: null },
      });

      await (verifier as any).verifyFeedbacks(390000n);

      const updated = await prisma.feedback.findUnique({
        where: { id: feedback.id },
      });

      expect(updated!.status).toBe("FINALIZED");
      expect(updated!.verifiedAt).not.toBeNull();
    });

    it("should finalize response when parent feedback not orphaned and agent exists", async () => {
      // Mock connection where agent exists
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(400000),
        getAccountInfo: vi.fn().mockResolvedValue({ data: Buffer.alloc(100), lamports: 1000000 }),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      // Ensure agent exists first (for FK constraint)
      await prisma.agent.upsert({
        where: { id: TEST_AGENT_REORG.toBase58() },
        create: {
          id: TEST_AGENT_REORG.toBase58(),
          owner: TEST_OWNER.toBase58(),
          uri: "https://test.com/agent.json",
          nftName: "Response Test Agent",
          collection: TEST_COLLECTION.toBase58(),
          registry: TEST_COLLECTION.toBase58(),
          atomEnabled: true,
          status: "PENDING",
        },
        update: {},
      });

      // First ensure we have a finalized feedback
      const feedback = await prisma.feedback.upsert({
        where: {
          agentId_client_feedbackIndex: {
            agentId: TEST_AGENT_REORG.toBase58(),
            client: TEST_CLIENT.toBase58(),
            feedbackIndex: 101n,
          },
        },
        create: {
          agentId: TEST_AGENT_REORG.toBase58(),
          client: TEST_CLIENT.toBase58(),
          feedbackIndex: 101n,
          score: 90,
          value: 9000n,
          valueDecimals: 2,
          tag1: "response_finalize",
          tag2: "",
          endpoint: "/test",
          feedbackUri: "ipfs://response_finalize",
          feedbackHash: Buffer.alloc(32).fill(8),
          status: "FINALIZED",
          createdSlot: 1000n,
        },
        update: { status: "FINALIZED" },
      });

      // Create response for that feedback
      const response = await prisma.feedbackResponse.create({
        data: {
          feedbackId: feedback.id,
          responder: TEST_OWNER.toBase58(),
          responseUri: "ipfs://finalize_response",
          responseHash: Buffer.alloc(32).fill(9),
          status: "PENDING",
          slot: 1000n,
          txSignature: "finalizeResponseSig" + Date.now(),
        },
      });

      await (verifier as any).verifyFeedbackResponses(390000n);

      const updated = await prisma.feedbackResponse.findUnique({
        where: { id: response.id },
      });

      expect(updated!.status).toBe("FINALIZED");
      expect(updated!.verifiedAt).not.toBeNull();
    });

    it("should orphan response when agent missing (even if feedback not orphaned)", async () => {
      // Mock connection where agent does NOT exist
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(400000),
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      // Ensure agent exists in DB first (for FK constraint) but it won't exist on-chain
      await prisma.agent.upsert({
        where: { id: TEST_AGENT_ORPHAN.toBase58() },
        create: {
          id: TEST_AGENT_ORPHAN.toBase58(),
          owner: TEST_OWNER.toBase58(),
          uri: "https://test.com/orphan-agent.json",
          nftName: "Orphan Test Agent",
          collection: TEST_COLLECTION.toBase58(),
          registry: TEST_COLLECTION.toBase58(),
          atomEnabled: true,
          status: "PENDING",
        },
        update: {},
      });

      // Create feedback that is PENDING (not orphaned yet)
      const feedback = await prisma.feedback.upsert({
        where: {
          agentId_client_feedbackIndex: {
            agentId: TEST_AGENT_ORPHAN.toBase58(),
            client: TEST_CLIENT.toBase58(),
            feedbackIndex: 102n,
          },
        },
        create: {
          agentId: TEST_AGENT_ORPHAN.toBase58(),
          client: TEST_CLIENT.toBase58(),
          feedbackIndex: 102n,
          score: 70,
          value: 7000n,
          valueDecimals: 2,
          tag1: "agent_missing",
          tag2: "",
          endpoint: "/test",
          feedbackUri: "ipfs://agent_missing",
          feedbackHash: Buffer.alloc(32).fill(10),
          status: "PENDING", // Not orphaned!
          createdSlot: 1000n,
        },
        update: { status: "PENDING" },
      });

      // Create response
      const response = await prisma.feedbackResponse.create({
        data: {
          feedbackId: feedback.id,
          responder: TEST_OWNER.toBase58(),
          responseUri: "ipfs://orphan_response",
          responseHash: Buffer.alloc(32).fill(11),
          status: "PENDING",
          slot: 1000n,
          txSignature: "orphanResponseSig" + Date.now(),
        },
      });

      await (verifier as any).verifyFeedbackResponses(390000n);

      const updated = await prisma.feedbackResponse.findUnique({
        where: { id: response.id },
      });

      // Should be orphaned because agent doesn't exist (even though feedback is PENDING)
      expect(updated!.status).toBe("ORPHANED");
    });
  });

  // =========================================================================
  // Phase 7: Edge Cases
  // =========================================================================

  describe("7. Edge Cases", () => {
    it("should handle negative cutoff guard (new network)", async () => {
      // Mock connection with very low slot (simulating new network)
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(10), // Very low slot
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      // This should not throw even with cutoff < 0
      await expect((verifier as any).verifyAll()).resolves.not.toThrow();
    });

    it("should handle RPC failures gracefully with retry", async () => {
      let attempts = 0;
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(400000),
        getAccountInfo: vi.fn().mockImplementation(async () => {
          attempts++;
          if (attempts < 3) {
            throw new Error("RPC timeout");
          }
          return { data: Buffer.alloc(100), lamports: 1000000 };
        }),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      // verifyWithRetry should succeed after retries
      const result = await (verifier as any).verifyWithRetry(
        TEST_AGENT_REORG.toBase58(),
        "finalized",
        3
      );

      expect(result).toBe(true);
      expect(attempts).toBe(3);
    });

    it("should return false when all retries exhausted (persistent failure)", async () => {
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(400000),
        getAccountInfo: vi.fn().mockRejectedValue(new Error("Persistent RPC failure")),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      // verifyWithRetry should return false after all retries fail
      const result = await (verifier as any).verifyWithRetry(
        TEST_AGENT_REORG.toBase58(),
        "finalized",
        3
      );

      expect(result).toBe(false);
    });

    it("should return false when account consistently null (not just error)", async () => {
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(400000),
        getAccountInfo: vi.fn().mockResolvedValue(null), // Persistent null
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      const result = await (verifier as any).verifyWithRetry(
        TEST_AGENT_REORG.toBase58(),
        "finalized",
        3
      );

      expect(result).toBe(false);
    });

    it("should skip URI-derived metadata (not on-chain)", async () => {
      // Create URI-derived metadata
      const metadata = await prisma.agentMetadata.create({
        data: {
          agentId: TEST_AGENT_REORG.toBase58(),
          key: "_uri:name",
          value: Buffer.from([0x00, ...Buffer.from("Test Name")]),
          immutable: false,
          status: "PENDING",
          slot: 1000n,
          txSignature: "uriMetaSig",
        },
      });

      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(400000),
        getAccountInfo: vi.fn().mockResolvedValue(null), // Would fail if checked
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;
      await (verifier as any).verifyMetadata(390000n);

      // Should be FINALIZED without checking on-chain
      const updated = await prisma.agentMetadata.findUnique({
        where: { id: metadata.id },
      });

      expect(updated!.status).toBe("FINALIZED");
    });
  });

  // =========================================================================
  // Phase 8: Full Data Lifecycle
  // =========================================================================

  describe("8. Full Data Lifecycle", () => {
    it("should complete full lifecycle: PENDING → FINALIZED", async () => {
      const lifecycleAgent = new PublicKey("GeCUQM9DamYcqnpjWSGKeyqF9jMyvJoMqsjjjDqi27BM");

      const ctx: EventContext = {
        signature: "lifecycleSig",
        slot: 500000n,
        blockTime: new Date(),
      };

      // 1. Register agent (PENDING)
      await handleEvent(prisma, {
        type: "AgentRegistered",
        data: {
          asset: lifecycleAgent,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: true,
          agentUri: "https://example.com/lifecycle.json",
        },
      }, ctx);

      let agent = await prisma.agent.findUnique({
        where: { id: lifecycleAgent.toBase58() },
      });
      expect(agent!.status).toBe("PENDING");

      // 2. Simulate verification (agent exists on-chain)
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(600000),
        getAccountInfo: vi.fn().mockResolvedValue({
          data: Buffer.alloc(100),
          lamports: 1000000,
        }),
      } as unknown as Connection;

      // Update slot to be old enough for verification
      await prisma.agent.update({
        where: { id: lifecycleAgent.toBase58() },
        data: { createdSlot: 1000n },
      });

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;
      await (verifier as any).verifyAgents(590000n);

      // 3. Check FINALIZED
      agent = await prisma.agent.findUnique({
        where: { id: lifecycleAgent.toBase58() },
      });
      expect(agent!.status).toBe("FINALIZED");
      expect(agent!.verifiedAt).not.toBeNull();
      expect(agent!.verifiedSlot).toBe(590000n);
    });
  });

  // =========================================================================
  // Phase 9: Validation Verification (Missing from initial coverage)
  // =========================================================================

  describe("9. Validation Verification", () => {
    it("should finalize validation when PDA exists on-chain", async () => {
      // Mock that returns account info for validation PDA
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(500000),
        getAccountInfo: vi.fn().mockResolvedValue({
          data: Buffer.alloc(109), // ValidationRequest size
          lamports: 1000000,
        }),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      // Create validation with old slot
      const validation = await prisma.validation.upsert({
        where: {
          agentId_validator_nonce: {
            agentId: TEST_AGENT_REORG.toBase58(),
            validator: TEST_VALIDATOR.toBase58(),
            nonce: 100n,
          },
        },
        create: {
          agentId: TEST_AGENT_REORG.toBase58(),
          validator: TEST_VALIDATOR.toBase58(),
          nonce: 100n,
          requester: TEST_OWNER.toBase58(),
          requestUri: "ipfs://validation_test",
          requestHash: Buffer.alloc(32).fill(10),
          chainStatus: "PENDING",
          requestSlot: 1000n,
          requestTxSignature: "validationTestSig",
        },
        update: { chainStatus: "PENDING", requestSlot: 1000n },
      });

      await (verifier as any).verifyValidations(490000n);

      const updated = await prisma.validation.findUnique({
        where: { id: validation.id },
      });

      expect(updated!.chainStatus).toBe("FINALIZED");
      expect(updated!.chainVerifiedAt).not.toBeNull();
    });

    it("should orphan validation when PDA does not exist on-chain", async () => {
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(500000),
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      // Create validation that doesn't exist on-chain
      const validation = await prisma.validation.upsert({
        where: {
          agentId_validator_nonce: {
            agentId: TEST_AGENT_ORPHAN.toBase58(),
            validator: TEST_VALIDATOR.toBase58(),
            nonce: 101n,
          },
        },
        create: {
          agentId: TEST_AGENT_ORPHAN.toBase58(),
          validator: TEST_VALIDATOR.toBase58(),
          nonce: 101n,
          requester: TEST_OWNER.toBase58(),
          requestUri: "ipfs://orphan_validation",
          requestHash: Buffer.alloc(32).fill(11),
          chainStatus: "PENDING",
          requestSlot: 1000n,
          requestTxSignature: "orphanValidationSig",
        },
        update: { chainStatus: "PENDING", requestSlot: 1000n },
      });

      await (verifier as any).verifyValidations(490000n);

      const updated = await prisma.validation.findUnique({
        where: { id: validation.id },
      });

      expect(updated!.chainStatus).toBe("ORPHANED");
    });
  });

  // =========================================================================
  // Phase 10: Registry Verification (Missing from initial coverage)
  // =========================================================================

  describe("10. Registry Verification", () => {
    it("should finalize registry when PDA exists on-chain", async () => {
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(500000),
        getAccountInfo: vi.fn().mockResolvedValue({
          data: Buffer.alloc(200),
          lamports: 1000000,
        }),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      // Reset registry status
      await prisma.registry.update({
        where: { id: TEST_COLLECTION.toBase58() },
        data: { status: "PENDING", slot: 1000n },
      });

      await (verifier as any).verifyRegistries(490000n);

      const updated = await prisma.registry.findUnique({
        where: { id: TEST_COLLECTION.toBase58() },
      });

      expect(updated!.status).toBe("FINALIZED");
      expect(updated!.verifiedAt).not.toBeNull();
    });

    it("should orphan registry when PDA does not exist on-chain", async () => {
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(500000),
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      // Create orphan registry
      const orphanRegistry = new PublicKey("DwozKmSieenAM3dgnjLr1VpQBc5q75Bo9GtZqkQoQnqh");
      const orphanCollection = new PublicKey("7VYnBrCNadcBUKRfhk4o4kfZfDgrwzxRXcUWQ6z6mbJL");
      await prisma.registry.upsert({
        where: { id: orphanRegistry.toBase58() },
        create: {
          id: orphanRegistry.toBase58(),
          collection: orphanCollection.toBase58(),
          registryType: "User",
          authority: TEST_OWNER.toBase58(),
          status: "PENDING",
          slot: 1000n,
          txSignature: "orphanRegistrySig",
        },
        update: { status: "PENDING", slot: 1000n },
      });

      await (verifier as any).verifyRegistries(490000n);

      const updated = await prisma.registry.findUnique({
        where: { id: orphanRegistry.toBase58() },
      });

      expect(updated!.status).toBe("ORPHANED");
    });
  });

  // =========================================================================
  // Phase 11: On-Chain Metadata Verification (Missing from initial coverage)
  // =========================================================================

  describe("11. On-Chain Metadata Verification", () => {
    it("should finalize on-chain metadata when PDA exists", async () => {
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(500000),
        getAccountInfo: vi.fn().mockResolvedValue({
          data: Buffer.alloc(100),
          lamports: 1000000,
        }),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      // Create on-chain metadata (no _uri: prefix)
      const metadata = await prisma.agentMetadata.create({
        data: {
          agentId: TEST_AGENT_REORG.toBase58(),
          key: "onchain_test_key",
          value: Buffer.from("onchain value"),
          immutable: false,
          status: "PENDING",
          slot: 1000n,
          txSignature: "onchainMetaSig",
        },
      });

      await (verifier as any).verifyMetadata(490000n);

      const updated = await prisma.agentMetadata.findUnique({
        where: { id: metadata.id },
      });

      expect(updated!.status).toBe("FINALIZED");
    });

    it("should orphan on-chain metadata when PDA does not exist", async () => {
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(500000),
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      // Create on-chain metadata that doesn't exist
      const metadata = await prisma.agentMetadata.create({
        data: {
          agentId: TEST_AGENT_ORPHAN.toBase58(),
          key: "orphan_onchain_key",
          value: Buffer.from("orphan value"),
          immutable: false,
          status: "PENDING",
          slot: 1000n,
          txSignature: "orphanOnchainMetaSig",
        },
      });

      await (verifier as any).verifyMetadata(490000n);

      const updated = await prisma.agentMetadata.findUnique({
        where: { id: metadata.id },
      });

      expect(updated!.status).toBe("ORPHANED");
    });
  });

  // =========================================================================
  // Phase 12: fetchOnChainDigests (Missing from initial coverage)
  // =========================================================================

  describe("12. fetchOnChainDigests", () => {
    it("should return null when account not found", async () => {
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(500000),
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);

      const digests = await verifier.fetchOnChainDigests(TEST_AGENT_REORG.toBase58());
      expect(digests).toBeNull();
    });

    it("should return null when data is too small", async () => {
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(500000),
        getAccountInfo: vi.fn().mockResolvedValue({
          data: Buffer.alloc(100), // Too small (< 227 bytes)
          lamports: 1000000,
        }),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);

      const digests = await verifier.fetchOnChainDigests(TEST_AGENT_REORG.toBase58());
      expect(digests).toBeNull();
    });

    it("should parse valid AgentAccount data", async () => {
      // Create a buffer that simulates AgentAccount structure
      const data = Buffer.alloc(300);
      // Skip discriminator (8) + collection(32) + owner(32) + asset(32) + bump(1) + atom_enabled(1) = 106
      // Option tag = 0 (None) at offset 106
      data[106] = 0;
      // Digests start at offset 107
      const feedbackDigest = Buffer.alloc(32).fill(0xaa);
      feedbackDigest.copy(data, 107);
      // Feedback count at 139
      data.writeBigUInt64LE(5n, 139);
      // Response digest at 147
      const responseDigest = Buffer.alloc(32).fill(0xbb);
      responseDigest.copy(data, 147);
      // Response count at 179
      data.writeBigUInt64LE(3n, 179);
      // Revoke digest at 187
      const revokeDigest = Buffer.alloc(32).fill(0xcc);
      revokeDigest.copy(data, 187);
      // Revoke count at 219
      data.writeBigUInt64LE(1n, 219);

      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(500000),
        getAccountInfo: vi.fn().mockResolvedValue({
          data,
          lamports: 1000000,
        }),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);

      const digests = await verifier.fetchOnChainDigests(TEST_AGENT_REORG.toBase58());
      expect(digests).not.toBeNull();
      expect(digests!.feedbackCount).toBe(5n);
      expect(digests!.responseCount).toBe(3n);
      expect(digests!.revokeCount).toBe(1n);
    });
  });

  // =========================================================================
  // Phase 13: start()/stop() Lifecycle (Missing from initial coverage)
  // =========================================================================

  describe("13. Verifier Lifecycle", () => {
    it("should not start when verificationEnabled is false", async () => {
      const originalConfig = (await import("../../src/config.js")).config;
      const originalEnabled = originalConfig.verificationEnabled;

      // Temporarily disable verification
      (originalConfig as any).verificationEnabled = false;

      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(500000),
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      await verifier.start();

      // isRunning should still be false
      expect((verifier as any).isRunning).toBe(false);

      // Restore
      (originalConfig as any).verificationEnabled = originalEnabled;
    });

    it("should stop and clear interval", async () => {
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(500000),
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;
      (verifier as any).interval = setInterval(() => {}, 60000);

      await verifier.stop();

      expect((verifier as any).isRunning).toBe(false);
      expect((verifier as any).interval).toBeNull();
    });
  });

  // =========================================================================
  // Phase 14: Multiple Feedbacks Cascade (Missing from initial coverage)
  // =========================================================================

  describe("14. Multiple Feedbacks Cascade", () => {
    it("should orphan all feedbacks when agent does not exist", async () => {
      const mockConnection = {
        getSlot: vi.fn().mockResolvedValue(500000),
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const verifier = new DataVerifier(mockConnection, prisma, null, 60000);
      (verifier as any).isRunning = true;

      const orphanAgent = TEST_AGENT_ORPHAN.toBase58();

      // Create multiple feedbacks for the same orphan agent
      const feedbacks = [];
      for (let i = 200; i < 203; i++) {
        const feedback = await prisma.feedback.upsert({
          where: {
            agentId_client_feedbackIndex: {
              agentId: orphanAgent,
              client: TEST_CLIENT.toBase58(),
              feedbackIndex: BigInt(i),
            },
          },
          create: {
            agentId: orphanAgent,
            client: TEST_CLIENT.toBase58(),
            feedbackIndex: BigInt(i),
            score: 50 + i,
            value: BigInt(5000 + i),
            valueDecimals: 2,
            tag1: `multi_cascade_${i}`,
            tag2: "",
            endpoint: "/test",
            feedbackUri: `ipfs://multi_${i}`,
            feedbackHash: Buffer.alloc(32).fill(i),
            status: "PENDING",
            createdSlot: 1000n,
          },
          update: { status: "PENDING", createdSlot: 1000n },
        });
        feedbacks.push(feedback);
      }

      await (verifier as any).verifyFeedbacks(490000n);

      // All feedbacks should be orphaned
      for (const fb of feedbacks) {
        const updated = await prisma.feedback.findUnique({
          where: { id: fb.id },
        });
        expect(updated!.status).toBe("ORPHANED");
      }
    });
  });
});
