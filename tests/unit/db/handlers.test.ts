import { describe, it, expect, vi, beforeEach } from "vitest";
import { handleEvent, EventContext } from "../../../src/db/handlers.js";
import { ProgramEvent } from "../../../src/parser/types.js";
import { createMockPrismaClient, resetMockPrisma } from "../../mocks/prisma.js";
import {
  TEST_ASSET,
  TEST_OWNER,
  TEST_NEW_OWNER,
  TEST_COLLECTION,
  TEST_REGISTRY,
  TEST_CLIENT,
  TEST_VALIDATOR,
  TEST_WALLET,
  TEST_HASH,
  TEST_VALUE,
  TEST_SIGNATURE,
  TEST_SLOT,
  TEST_BLOCK_TIME,
} from "../../mocks/solana.js";

describe("DB Handlers", () => {
  let prisma: ReturnType<typeof createMockPrismaClient>;
  let ctx: EventContext;

  beforeEach(() => {
    prisma = createMockPrismaClient();
    ctx = {
      signature: TEST_SIGNATURE,
      slot: TEST_SLOT,
      blockTime: TEST_BLOCK_TIME,
    };
  });

  describe("handleEvent", () => {
    describe("AgentRegisteredInRegistry", () => {
      it("should upsert agent on registration", async () => {
        const event: ProgramEvent = {
          type: "AgentRegisteredInRegistry",
          data: {
            asset: TEST_ASSET,
            registry: TEST_REGISTRY,
            collection: TEST_COLLECTION,
            owner: TEST_OWNER,
            atomEnabled: true,
          },
        };

        await handleEvent(prisma, event, ctx);

        expect(prisma.agent.upsert).toHaveBeenCalledWith(
          expect.objectContaining({
            where: { id: TEST_ASSET.toBase58() },
            create: expect.objectContaining({
              id: TEST_ASSET.toBase58(),
              owner: TEST_OWNER.toBase58(),
              collection: TEST_COLLECTION.toBase58(),
              registry: TEST_REGISTRY.toBase58(),
              atomEnabled: true,
            }),
          })
        );
      });
    });

    describe("AgentOwnerSynced", () => {
      it("should update agent owner", async () => {
        const event: ProgramEvent = {
          type: "AgentOwnerSynced",
          data: {
            asset: TEST_ASSET,
            oldOwner: TEST_OWNER,
            newOwner: TEST_NEW_OWNER,
          },
        };

        await handleEvent(prisma, event, ctx);

        expect(prisma.agent.update).toHaveBeenCalledWith({
          where: { id: TEST_ASSET.toBase58() },
          data: {
            owner: TEST_NEW_OWNER.toBase58(),
            updatedAt: ctx.blockTime,
          },
        });
      });
    });

    describe("UriUpdated", () => {
      it("should update agent URI", async () => {
        const newUri = "https://example.com/agent.json";
        const event: ProgramEvent = {
          type: "UriUpdated",
          data: {
            asset: TEST_ASSET,
            newUri,
            updatedBy: TEST_OWNER,
          },
        };

        await handleEvent(prisma, event, ctx);

        expect(prisma.agent.update).toHaveBeenCalledWith({
          where: { id: TEST_ASSET.toBase58() },
          data: {
            uri: newUri,
            updatedAt: ctx.blockTime,
          },
        });
      });
    });

    describe("WalletUpdated", () => {
      it("should update agent wallet", async () => {
        const event: ProgramEvent = {
          type: "WalletUpdated",
          data: {
            asset: TEST_ASSET,
            oldWallet: null,
            newWallet: TEST_WALLET,
            updatedBy: TEST_OWNER,
          },
        };

        await handleEvent(prisma, event, ctx);

        expect(prisma.agent.update).toHaveBeenCalledWith({
          where: { id: TEST_ASSET.toBase58() },
          data: {
            wallet: TEST_WALLET.toBase58(),
            updatedAt: ctx.blockTime,
          },
        });
      });
    });

    describe("MetadataSet", () => {
      it("should upsert metadata entry", async () => {
        const event: ProgramEvent = {
          type: "MetadataSet",
          data: {
            asset: TEST_ASSET,
            key: "description",
            value: TEST_VALUE,
            immutable: false,
          },
        };

        await handleEvent(prisma, event, ctx);

        expect(prisma.agentMetadata.upsert).toHaveBeenCalledWith(
          expect.objectContaining({
            where: {
              agentId_key: {
                agentId: TEST_ASSET.toBase58(),
                key: "description",
              },
            },
            create: expect.objectContaining({
              agentId: TEST_ASSET.toBase58(),
              key: "description",
              immutable: false,
            }),
          })
        );
      });
    });

    describe("MetadataDeleted", () => {
      it("should delete metadata entry", async () => {
        const event: ProgramEvent = {
          type: "MetadataDeleted",
          data: {
            asset: TEST_ASSET,
            key: "description",
          },
        };

        await handleEvent(prisma, event, ctx);

        expect(prisma.agentMetadata.deleteMany).toHaveBeenCalledWith({
          where: {
            agentId: TEST_ASSET.toBase58(),
            key: "description",
          },
        });
      });
    });

    describe("BaseRegistryCreated", () => {
      it("should upsert base registry", async () => {
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

        expect(prisma.registry.upsert).toHaveBeenCalledWith(
          expect.objectContaining({
            where: { id: TEST_REGISTRY.toBase58() },
            create: expect.objectContaining({
              registryType: "Base",
              baseIndex: 0,
            }),
          })
        );
      });
    });

    describe("UserRegistryCreated", () => {
      it("should upsert user registry", async () => {
        const event: ProgramEvent = {
          type: "UserRegistryCreated",
          data: {
            registry: TEST_REGISTRY,
            collection: TEST_COLLECTION,
            owner: TEST_OWNER,
          },
        };

        await handleEvent(prisma, event, ctx);

        expect(prisma.registry.upsert).toHaveBeenCalledWith(
          expect.objectContaining({
            where: { id: TEST_REGISTRY.toBase58() },
            create: expect.objectContaining({
              registryType: "User",
              authority: TEST_OWNER.toBase58(),
            }),
          })
        );
      });
    });

    describe("BaseRegistryRotated", () => {
      it("should handle registry rotation (logs only)", async () => {
        const newRegistry = TEST_REGISTRY; // simplified
        const event: ProgramEvent = {
          type: "BaseRegistryRotated",
          data: {
            oldRegistry: TEST_REGISTRY,
            newRegistry: newRegistry,
            rotatedBy: TEST_OWNER,
          },
        };

        // Should not throw, just log
        await expect(handleEvent(prisma, event, ctx)).resolves.not.toThrow();
      });
    });

    describe("NewFeedback", () => {
      it("should upsert feedback", async () => {
        const event: ProgramEvent = {
          type: "NewFeedback",
          data: {
            asset: TEST_ASSET,
            clientAddress: TEST_CLIENT,
            feedbackIndex: 0n,
            score: 85,
            tag1: "quality",
            tag2: "speed",
            endpoint: "/api/chat",
            feedbackUri: "ipfs://QmXXX",
            feedbackHash: TEST_HASH,
            atomEnabled: true,
            newTrustTier: 0,
            newQualityScore: 0,
            newConfidence: 0,
            newRiskScore: 0,
            newDiversityRatio: 0,
            isUniqueClient: true,
          },
        };

        await handleEvent(prisma, event, ctx);

        expect(prisma.feedback.upsert).toHaveBeenCalledWith(
          expect.objectContaining({
            where: {
              agentId_client_feedbackIndex: {
                agentId: TEST_ASSET.toBase58(),
                client: TEST_CLIENT.toBase58(),
                feedbackIndex: 0n,
              },
            },
            create: expect.objectContaining({
              score: 85,
              tag1: "quality",
              tag2: "speed",
            }),
          })
        );
      });
    });

    describe("FeedbackRevoked", () => {
      it("should mark feedback as revoked", async () => {
        const event: ProgramEvent = {
          type: "FeedbackRevoked",
          data: {
            asset: TEST_ASSET,
            clientAddress: TEST_CLIENT,
            feedbackIndex: 0n,
            originalScore: 0,
            atomEnabled: true,
            hadImpact: false,
            newTrustTier: 0,
            newQualityScore: 0,
            newConfidence: 0,
          },
        };

        await handleEvent(prisma, event, ctx);

        expect(prisma.feedback.updateMany).toHaveBeenCalledWith({
          where: {
            agentId: TEST_ASSET.toBase58(),
            client: TEST_CLIENT.toBase58(),
            feedbackIndex: 0n,
          },
          data: expect.objectContaining({
            revoked: true,
          }),
        });
      });
    });

    describe("ResponseAppended", () => {
      it("should create feedback response when feedback exists", async () => {
        const mockFeedback = { id: "feedback-uuid", agentId: TEST_ASSET.toBase58() };
        (prisma.feedback.findUnique as any).mockResolvedValue(mockFeedback);

        const event: ProgramEvent = {
          type: "ResponseAppended",
          data: {
            asset: TEST_ASSET,
            client: TEST_CLIENT,
            feedbackIndex: 0n,
            responder: TEST_OWNER,
            responseUri: "ipfs://QmYYY",
            responseHash: TEST_HASH,
          },
        };

        await handleEvent(prisma, event, ctx);

        expect(prisma.feedbackResponse.upsert).toHaveBeenCalledWith(
          expect.objectContaining({
            create: expect.objectContaining({
              feedbackId: "feedback-uuid",
              responder: TEST_OWNER.toBase58(),
              responseUri: "ipfs://QmYYY",
            }),
          })
        );
      });

      it("should not create response when feedback not found", async () => {
        (prisma.feedback.findUnique as any).mockResolvedValue(null);

        const event: ProgramEvent = {
          type: "ResponseAppended",
          data: {
            asset: TEST_ASSET,
            client: TEST_CLIENT,
            feedbackIndex: 0n,
            responder: TEST_OWNER,
            responseUri: "ipfs://QmYYY",
            responseHash: TEST_HASH,
          },
        };

        await handleEvent(prisma, event, ctx);

        expect(prisma.feedbackResponse.upsert).not.toHaveBeenCalled();
      });
    });

    describe("ValidationRequested", () => {
      it("should upsert validation request", async () => {
        const event: ProgramEvent = {
          type: "ValidationRequested",
          data: {
            asset: TEST_ASSET,
            validatorAddress: TEST_VALIDATOR,
            nonce: 1,
            requestUri: "ipfs://QmZZZ",
            requestHash: TEST_HASH,
            requester: TEST_OWNER,
          },
        };

        await handleEvent(prisma, event, ctx);

        expect(prisma.validation.upsert).toHaveBeenCalledWith(
          expect.objectContaining({
            where: {
              agentId_validator_nonce: {
                agentId: TEST_ASSET.toBase58(),
                validator: TEST_VALIDATOR.toBase58(),
                nonce: 1,
              },
            },
            create: expect.objectContaining({
              nonce: 1,
              requestUri: "ipfs://QmZZZ",
            }),
          })
        );
      });
    });

    describe("ValidationResponded", () => {
      it("should update validation with response", async () => {
        const event: ProgramEvent = {
          type: "ValidationResponded",
          data: {
            asset: TEST_ASSET,
            validatorAddress: TEST_VALIDATOR,
            nonce: 1,
            response: 90,
            responseUri: "ipfs://QmAAA",
            responseHash: TEST_HASH,
            tag: "security",
          },
        };

        await handleEvent(prisma, event, ctx);

        expect(prisma.validation.updateMany).toHaveBeenCalledWith({
          where: {
            agentId: TEST_ASSET.toBase58(),
            validator: TEST_VALIDATOR.toBase58(),
            nonce: 1,
          },
          data: expect.objectContaining({
            response: 90,
            responseUri: "ipfs://QmAAA",
            tag: "security",
          }),
        });
      });
    });

    describe("Unknown event type", () => {
      it("should log warning for unhandled event type", async () => {
        const event = {
          type: "UnknownEvent",
          data: { foo: "bar" },
        } as unknown as ProgramEvent;

        // Should not throw, just log warning
        await expect(handleEvent(prisma, event, ctx)).resolves.not.toThrow();
      });
    });
  });
});
