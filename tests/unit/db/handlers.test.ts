import { describe, it, expect, vi, beforeEach } from "vitest";
import { handleEvent, EventContext } from "../../../src/db/handlers.js";
import { config } from "../../../src/config.js";
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
    describe("AgentRegistered", () => {
      it("should upsert agent on registration", async () => {
        const event: ProgramEvent = {
          type: "AgentRegistered",
          data: {
            asset: TEST_ASSET,
            collection: TEST_COLLECTION,
            owner: TEST_OWNER,
            atomEnabled: true,
            agentUri: "ipfs://QmTest",
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

        expect(prisma.agent.updateMany).toHaveBeenCalledWith({
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

        expect(prisma.agent.updateMany).toHaveBeenCalledWith(
          expect.objectContaining({
            where: { id: TEST_ASSET.toBase58() },
          })
        );
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

        expect(prisma.agent.updateMany).toHaveBeenCalledWith({
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

    describe("RegistryInitialized", () => {
      it("should upsert registry on initialization", async () => {
        const event: ProgramEvent = {
          type: "RegistryInitialized",
          data: {
            collection: TEST_COLLECTION,
            authority: TEST_OWNER,
          },
        };

        await handleEvent(prisma, event, ctx);

        expect(prisma.registry.upsert).toHaveBeenCalledWith(
          expect.objectContaining({
            create: expect.objectContaining({
              authority: TEST_OWNER.toBase58(),
            }),
          })
        );
      });
    });

    describe("NewFeedback", () => {
      it("should upsert feedback", async () => {
        // Mock agent.findMany to return existing agent for reconciliation
        (prisma.agent.findMany as any).mockResolvedValue([{ id: TEST_ASSET.toBase58() }]);

        const event: ProgramEvent = {
          type: "NewFeedback",
          data: {
            asset: TEST_ASSET,
            clientAddress: TEST_CLIENT,
            feedbackIndex: 0n,
            value: 9500n,
            valueDecimals: 2,
            score: 85,
            tag1: "quality",
            tag2: "speed",
            endpoint: "/api/chat",
            feedbackUri: "ipfs://QmXXX",
            feedbackFileHash: null,
            sealHash: TEST_HASH,
            slot: 123456n,
            atomEnabled: true,
            newFeedbackDigest: TEST_HASH,
            newFeedbackCount: 1n,
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
              value: "9500",
              valueDecimals: 2,
              tag1: "quality",
              tag2: "speed",
            }),
          })
        );
      });

      it("should reconcile orphan response with responseCount and assign missing responseId", async () => {
        const feedbackId = "feedback-uuid-reconcile";
        (prisma.feedback.upsert as any).mockResolvedValue({ id: feedbackId });
        (prisma.orphanResponse.findMany as any).mockResolvedValue([
          {
            id: "orphan-1",
            responder: TEST_OWNER.toBase58(),
            responseUri: "ipfs://QmOrphan",
            responseHash: Uint8Array.from(TEST_HASH),
            runningDigest: Uint8Array.from(TEST_HASH),
            responseCount: 3n,
            txSignature: "orphan-tx-sig",
            slot: 123456n,
          },
        ]);
        (prisma.feedbackResponse.findUnique as any).mockResolvedValue({ responseId: null });
        (prisma.feedbackResponse.findMany as any).mockResolvedValue([{ responseId: 2n }]);

        const event: ProgramEvent = {
          type: "NewFeedback",
          data: {
            asset: TEST_ASSET,
            clientAddress: TEST_CLIENT,
            feedbackIndex: 0n,
            value: 9500n,
            valueDecimals: 2,
            score: 85,
            tag1: "quality",
            tag2: "speed",
            endpoint: "/api/chat",
            feedbackUri: "ipfs://QmXXX",
            feedbackFileHash: null,
            sealHash: TEST_HASH,
            slot: 123456n,
            atomEnabled: true,
            newFeedbackDigest: TEST_HASH,
            newFeedbackCount: 1n,
            newTrustTier: 0,
            newQualityScore: 0,
            newConfidence: 0,
            newRiskScore: 0,
            newDiversityRatio: 0,
            isUniqueClient: true,
          },
        };

        await handleEvent(prisma, event, ctx);

        expect(prisma.feedbackResponse.upsert).toHaveBeenCalledWith(
          expect.objectContaining({
            create: expect.objectContaining({
              feedbackId,
              responseCount: 3n,
              responseId: 3n,
            }),
            update: expect.objectContaining({
              responseCount: 3n,
              responseId: 3n,
            }),
          })
        );
      });
    });

    describe("FeedbackRevoked", () => {
      it("should mark feedback as revoked when feedback exists", async () => {
        // Mock feedback lookup with matching hash
        (prisma.feedback.findUnique as any).mockResolvedValue({
          feedbackHash: Uint8Array.from(TEST_HASH),
        });

        const event: ProgramEvent = {
          type: "FeedbackRevoked",
          data: {
            asset: TEST_ASSET,
            clientAddress: TEST_CLIENT,
            feedbackIndex: 0n,
            sealHash: TEST_HASH,
            slot: 123456n,
            originalScore: 0,
            atomEnabled: true,
            hadImpact: false,
            newTrustTier: 0,
            newQualityScore: 0,
            newConfidence: 0,
            newRevokeDigest: TEST_HASH,
            newRevokeCount: 1n,
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

        // Revocation should be stored with PENDING status (feedback exists)
        expect(prisma.revocation.upsert).toHaveBeenCalledWith(
          expect.objectContaining({
            create: expect.objectContaining({
              status: "PENDING",
            }),
          })
        );
      });

      it("should store orphan revocation when feedback not found", async () => {
        (prisma.feedback.findUnique as any).mockResolvedValue(null);

        const event: ProgramEvent = {
          type: "FeedbackRevoked",
          data: {
            asset: TEST_ASSET,
            clientAddress: TEST_CLIENT,
            feedbackIndex: 99n,
            sealHash: TEST_HASH,
            slot: 123456n,
            originalScore: 85,
            atomEnabled: false,
            hadImpact: false,
            newTrustTier: 0,
            newQualityScore: 0,
            newConfidence: 0,
            newRevokeDigest: TEST_HASH,
            newRevokeCount: 1n,
          },
        };

        await handleEvent(prisma, event, ctx);

        // Revocation still stored, but with ORPHANED status
        expect(prisma.revocation.upsert).toHaveBeenCalledWith(
          expect.objectContaining({
            create: expect.objectContaining({
              feedbackIndex: 99n,
              status: "ORPHANED",
            }),
          })
        );
      });

      it("should detect seal_hash mismatch on revocation", async () => {
        const differentHash = new Uint8Array(32).fill(0xcd);
        (prisma.feedback.findUnique as any).mockResolvedValue({
          feedbackHash: Uint8Array.from(differentHash),
        });

        const event: ProgramEvent = {
          type: "FeedbackRevoked",
          data: {
            asset: TEST_ASSET,
            clientAddress: TEST_CLIENT,
            feedbackIndex: 0n,
            sealHash: TEST_HASH, // 0xab * 32
            slot: 123456n,
            originalScore: 0,
            atomEnabled: true,
            hadImpact: false,
            newTrustTier: 0,
            newQualityScore: 0,
            newConfidence: 0,
            newRevokeDigest: TEST_HASH,
            newRevokeCount: 1n,
          },
        };

        // Should not throw (still processes the revocation)
        await handleEvent(prisma, event, ctx);

        // updateMany + revocation still called despite mismatch
        expect(prisma.feedback.updateMany).toHaveBeenCalled();
        expect(prisma.revocation.upsert).toHaveBeenCalled();
      });
    });

    describe("ResponseAppended", () => {
      it("should create feedback response when feedback exists with matching seal_hash", async () => {
        const mockFeedback = {
          id: "feedback-uuid",
          agentId: TEST_ASSET.toBase58(),
          feedbackHash: Uint8Array.from(TEST_HASH),
        };
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
            sealHash: TEST_HASH,
            slot: 123456n,
            newResponseDigest: TEST_HASH,
            newResponseCount: 1n,
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

      it("should store orphan response when feedback not found", async () => {
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
            sealHash: TEST_HASH,
            slot: 123456n,
            newResponseDigest: TEST_HASH,
            newResponseCount: 1n,
          },
        };

        await handleEvent(prisma, event, ctx);

        // Should NOT create feedbackResponse
        expect(prisma.feedbackResponse.upsert).not.toHaveBeenCalled();

        // Should store as orphan
        expect(prisma.orphanResponse.upsert).toHaveBeenCalledWith(
          expect.objectContaining({
            create: expect.objectContaining({
              agentId: TEST_ASSET.toBase58(),
              client: TEST_CLIENT.toBase58(),
              feedbackIndex: 0n,
              responder: TEST_OWNER.toBase58(),
              responseUri: "ipfs://QmYYY",
              responseCount: 1n,
            }),
          })
        );
      });

      it("should still create response when seal_hash mismatches", async () => {
        const differentHash = new Uint8Array(32).fill(0xcd);
        const mockFeedback = {
          id: "feedback-uuid",
          agentId: TEST_ASSET.toBase58(),
          feedbackHash: Uint8Array.from(differentHash), // Different from event sealHash
        };
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
            sealHash: TEST_HASH, // 0xab, different from 0xcd
            slot: 123456n,
            newResponseDigest: TEST_HASH,
            newResponseCount: 1n,
          },
        };

        // Should not throw, still processes despite mismatch
        await handleEvent(prisma, event, ctx);

        // Response still created (mismatch is logged, not blocking)
        expect(prisma.feedbackResponse.upsert).toHaveBeenCalledWith(
          expect.objectContaining({
            create: expect.objectContaining({
              feedbackId: "feedback-uuid",
            }),
          })
        );
      });

      it("should handle null feedbackHash gracefully", async () => {
        const mockFeedback = {
          id: "feedback-uuid",
          agentId: TEST_ASSET.toBase58(),
          feedbackHash: null, // Feedback had no hash
        };
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
            sealHash: new Uint8Array(32).fill(0), // All-zero = normalized to null
            slot: 123456n,
            newResponseDigest: TEST_HASH,
            newResponseCount: 1n,
          },
        };

        await handleEvent(prisma, event, ctx);

        // Both null → match, no warning
        expect(prisma.feedbackResponse.upsert).toHaveBeenCalled();
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

      it("should no-op when validation indexing is disabled", async () => {
        const previous = (config as any).validationIndexEnabled;
        (config as any).validationIndexEnabled = false;

        try {
          const event: ProgramEvent = {
            type: "ValidationRequested",
            data: {
              asset: TEST_ASSET,
              validatorAddress: TEST_VALIDATOR,
              nonce: 99,
              requestUri: "ipfs://QmNoop",
              requestHash: TEST_HASH,
              requester: TEST_OWNER,
            },
          };

          await handleEvent(prisma, event, ctx);
          expect(prisma.validation.upsert).not.toHaveBeenCalled();
        } finally {
          (config as any).validationIndexEnabled = previous;
        }
      });
    });

    describe("ValidationResponded", () => {
      it("should upsert validation with response", async () => {
        const event: ProgramEvent = {
          type: "ValidationResponded",
          data: {
            asset: TEST_ASSET,
            validatorAddress: TEST_VALIDATOR,
            nonce: 1n,
            response: 90,
            responseUri: "ipfs://QmAAA",
            responseHash: TEST_HASH,
            tag: "security",
          },
        };

        await handleEvent(prisma, event, ctx);

        expect(prisma.validation.upsert).toHaveBeenCalledWith({
          where: {
            agentId_validator_nonce: {
              agentId: TEST_ASSET.toBase58(),
              validator: TEST_VALIDATOR.toBase58(),
              nonce: 1n,
            },
          },
          create: expect.objectContaining({
            agentId: TEST_ASSET.toBase58(),
            validator: TEST_VALIDATOR.toBase58(),
            nonce: 1n,
            response: 90,
            responseUri: "ipfs://QmAAA",
            tag: "security",
          }),
          update: expect.objectContaining({
            response: 90,
            responseUri: "ipfs://QmAAA",
            tag: "security",
          }),
        });
      });

      it("should no-op when validation indexing is disabled", async () => {
        const previous = (config as any).validationIndexEnabled;
        (config as any).validationIndexEnabled = false;

        try {
          const event: ProgramEvent = {
            type: "ValidationResponded",
            data: {
              asset: TEST_ASSET,
              validatorAddress: TEST_VALIDATOR,
              nonce: 2n,
              response: 10,
              responseUri: "ipfs://QmNoopResp",
              responseHash: TEST_HASH,
              tag: "noop",
            },
          };

          await handleEvent(prisma, event, ctx);
          expect(prisma.validation.upsert).not.toHaveBeenCalled();
        } finally {
          (config as any).validationIndexEnabled = previous;
        }
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
