import { describe, it, expect, vi, beforeEach } from "vitest";
import { createMockPrismaClient } from "../../mocks/prisma.js";
import {
  TEST_ASSET,
  TEST_OWNER,
  TEST_NEW_OWNER,
  TEST_COLLECTION,
  TEST_CLIENT,
  TEST_WALLET,
  TEST_HASH,
  TEST_SIGNATURE,
  TEST_SLOT,
  TEST_BLOCK_TIME,
} from "../../mocks/solana.js";
import { PublicKey } from "@solana/web3.js";

// vi.hoisted ensures these are available when vi.mock factories execute (hoisted)
const {
  mockConfig,
  mockSupabaseHandleEventAtomic,
  mockSupabaseHandleEvent,
  mockDigestUri,
  mockSerializeValue,
  mockCompressForStorage,
  mockStripNullBytes,
} = vi.hoisted(() => ({
  mockConfig: {
    dbMode: "local" as string,
    metadataIndexMode: "normal" as string,
    metadataMaxValueBytes: 10000,
    metadataMaxBytes: 262144,
    metadataTimeoutMs: 5000,
  },
  mockSupabaseHandleEventAtomic: vi.fn().mockResolvedValue(undefined),
  mockSupabaseHandleEvent: vi.fn().mockResolvedValue(undefined),
  mockDigestUri: vi.fn().mockResolvedValue({ status: "ok", fields: {}, bytes: 100, hash: "abc123" }),
  mockSerializeValue: vi.fn().mockImplementation((value: unknown, _maxBytes: number) => ({
    value: typeof value === "string" ? value : JSON.stringify(value),
    oversize: false,
    bytes: 10,
  })),
  mockCompressForStorage: vi.fn().mockImplementation(async (data: Buffer) =>
    Buffer.concat([Buffer.from([0x01]), data])
  ),
  mockStripNullBytes: vi.fn().mockImplementation((data: Uint8Array) => Buffer.from(data)),
}));

vi.mock("../../../src/config.js", () => ({
  config: mockConfig,
  runtimeConfig: { baseCollection: null, initialized: false },
}));

vi.mock("../../../src/db/supabase.js", () => ({
  handleEventAtomic: mockSupabaseHandleEventAtomic,
  handleEvent: mockSupabaseHandleEvent,
}));

vi.mock("../../../src/indexer/uriDigest.js", () => ({
  digestUri: mockDigestUri,
  serializeValue: mockSerializeValue,
}));

vi.mock("../../../src/utils/compression.js", () => ({
  compressForStorage: mockCompressForStorage,
}));

vi.mock("../../../src/utils/sanitize.js", () => ({
  stripNullBytes: mockStripNullBytes,
}));

import { handleEventAtomic, cleanupOrphanResponses, handleEvent, EventContext } from "../../../src/db/handlers.js";
import { ProgramEvent } from "../../../src/parser/types.js";

const DEFAULT_PUBKEY_STR = "11111111111111111111111111111111";

describe("DB Handlers Coverage", () => {
  let prisma: ReturnType<typeof createMockPrismaClient>;
  let ctx: EventContext;

  beforeEach(() => {
    prisma = createMockPrismaClient();
    ctx = {
      signature: TEST_SIGNATURE,
      slot: TEST_SLOT,
      blockTime: TEST_BLOCK_TIME,
      source: "poller",
    };
    // Reset config to defaults
    mockConfig.dbMode = "local";
    mockConfig.metadataIndexMode = "normal";
    mockConfig.metadataMaxValueBytes = 10000;

    // Re-set mock implementations (vi.restoreAllMocks in setup.ts resets them)
    mockDigestUri.mockResolvedValue({ status: "ok", fields: {}, bytes: 100, hash: "abc123" });
    mockSerializeValue.mockImplementation((value: unknown, _maxBytes: number) => ({
      value: typeof value === "string" ? value : JSON.stringify(value),
      oversize: false,
      bytes: 10,
    }));
    mockCompressForStorage.mockImplementation(async (data: Buffer) =>
      Buffer.concat([Buffer.from([0x01]), data])
    );
    mockStripNullBytes.mockImplementation((data: Uint8Array) => Buffer.from(data));
  });

  // ==========================================================================
  // 1. handleEventAtomic
  // ==========================================================================
  describe("handleEventAtomic", () => {
    it("should process AgentRegistered event atomically via $transaction", async () => {
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

      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.$transaction).toHaveBeenCalledTimes(1);
      expect(prisma.agent.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: TEST_ASSET.toBase58() },
        })
      );
      expect(prisma.indexerState.upsert).toHaveBeenCalled();
    });

    it("should trigger URI digest for AgentRegistered with URI", async () => {
      const event: ProgramEvent = {
        type: "AgentRegistered",
        data: {
          asset: TEST_ASSET,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: false,
          agentUri: "ipfs://QmTestUri",
        },
      };

      (prisma.agent.findUnique as any).mockResolvedValue({
        uri: "ipfs://QmTestUri",
        nftName: "",
      });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      await handleEventAtomic(prisma, event, ctx);
      await vi.waitFor(() => {
        expect(mockDigestUri).toHaveBeenCalledWith("ipfs://QmTestUri");
      }, { timeout: 500 });
    });

    it("should trigger URI digest for UriUpdated with URI", async () => {
      const event: ProgramEvent = {
        type: "UriUpdated",
        data: {
          asset: TEST_ASSET,
          newUri: "https://example.com/agent.json",
          updatedBy: TEST_OWNER,
        },
      };

      (prisma.agent.findUnique as any).mockResolvedValue({
        uri: "https://example.com/agent.json",
        nftName: "",
      });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      await handleEventAtomic(prisma, event, ctx);
      await vi.waitFor(() => {
        expect(mockDigestUri).toHaveBeenCalledWith("https://example.com/agent.json");
      }, { timeout: 500 });
    });

    it("should NOT trigger URI digest when metadataIndexMode is off", async () => {
      mockConfig.metadataIndexMode = "off";

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

      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      await handleEventAtomic(prisma, event, ctx);
      await new Promise((r) => setTimeout(r, 100));

      expect(mockDigestUri).not.toHaveBeenCalled();
    });

    it("should route to supabase when dbMode is supabase", async () => {
      mockConfig.dbMode = "supabase";

      const event: ProgramEvent = {
        type: "AgentRegistered",
        data: {
          asset: TEST_ASSET,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: true,
          agentUri: "",
        },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(mockSupabaseHandleEventAtomic).toHaveBeenCalledWith(event, ctx);
      expect(prisma.$transaction).not.toHaveBeenCalled();
    });

    it("should throw when prisma is null in local mode", async () => {
      const event: ProgramEvent = {
        type: "AgentRegistered",
        data: {
          asset: TEST_ASSET,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: true,
          agentUri: "",
        },
      };

      await expect(handleEventAtomic(null, event, ctx)).rejects.toThrow(
        "Prisma client required in local mode"
      );
    });

    it("should not trigger URI digest for non-URI events", async () => {
      const event: ProgramEvent = {
        type: "MetadataDeleted",
        data: {
          asset: TEST_ASSET,
          key: "description",
        },
      };

      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      await handleEventAtomic(prisma, event, ctx);
      await new Promise((r) => setTimeout(r, 100));

      expect(mockDigestUri).not.toHaveBeenCalled();
    });

    it("should not trigger URI digest for AgentRegistered without URI", async () => {
      const event: ProgramEvent = {
        type: "AgentRegistered",
        data: {
          asset: TEST_ASSET,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: true,
          agentUri: "",
        },
      };

      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      await handleEventAtomic(prisma, event, ctx);
      await new Promise((r) => setTimeout(r, 100));

      expect(mockDigestUri).not.toHaveBeenCalled();
    });
  });

  // ==========================================================================
  // 2. updateCursorAtomic (tested indirectly via handleEventAtomic)
  // ==========================================================================
  describe("updateCursorAtomic (via handleEventAtomic)", () => {
    const simpleEvent: ProgramEvent = {
      type: "MetadataDeleted",
      data: { asset: TEST_ASSET, key: "test" },
    };

    it("should create new cursor when none exists", async () => {
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      await handleEventAtomic(prisma, simpleEvent, ctx);

      expect(prisma.indexerState.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: "main" },
          create: expect.objectContaining({
            id: "main",
            lastSignature: ctx.signature,
            lastSlot: ctx.slot,
            source: "poller",
          }),
        })
      );
    });

    it("should advance cursor when new slot > current", async () => {
      (prisma.indexerState.findUnique as any).mockResolvedValue({
        lastSlot: 1000n,
      });

      const advancedCtx = { ...ctx, slot: 2000n };
      await handleEventAtomic(prisma, simpleEvent, advancedCtx);

      expect(prisma.indexerState.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          update: expect.objectContaining({
            lastSlot: 2000n,
          }),
        })
      );
    });

    it("should reject backward slot movement", async () => {
      (prisma.indexerState.findUnique as any).mockResolvedValue({
        lastSlot: 5000n,
      });

      const staleCtx = { ...ctx, slot: 3000n };
      await handleEventAtomic(prisma, simpleEvent, staleCtx);

      expect(prisma.indexerState.upsert).not.toHaveBeenCalled();
    });

    it("should default source to 'poller' when ctx.source is undefined", async () => {
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const noSourceCtx: EventContext = {
        signature: TEST_SIGNATURE,
        slot: TEST_SLOT,
        blockTime: TEST_BLOCK_TIME,
      };

      await handleEventAtomic(prisma, simpleEvent, noSourceCtx);

      expect(prisma.indexerState.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({
            source: "poller",
          }),
          update: expect.objectContaining({
            source: "poller",
          }),
        })
      );
    });

    it("should use ctx.source when provided", async () => {
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const wsCtx: EventContext = { ...ctx, source: "websocket" };
      await handleEventAtomic(prisma, simpleEvent, wsCtx);

      expect(prisma.indexerState.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({
            source: "websocket",
          }),
        })
      );
    });
  });

  // ==========================================================================
  // 3. handleAgentRegistered (non-atomic) with URI digest queue
  // ==========================================================================
  describe("handleAgentRegistered (non-atomic, handleEvent)", () => {
    it("should trigger URI digest queue for registration with URI", async () => {
      const event: ProgramEvent = {
        type: "AgentRegistered",
        data: {
          asset: TEST_ASSET,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: true,
          agentUri: "ipfs://QmNonAtomic",
        },
      };

      (prisma.agent.findUnique as any).mockResolvedValue({
        uri: "ipfs://QmNonAtomic",
        nftName: "",
      });

      await handleEvent(prisma, event, ctx);
      await vi.waitFor(() => {
        expect(mockDigestUri).toHaveBeenCalledWith("ipfs://QmNonAtomic");
      }, { timeout: 500 });

      expect(prisma.agent.upsert).toHaveBeenCalled();
    });

    it("should skip URI digest when metadataIndexMode is off (non-atomic)", async () => {
      mockConfig.metadataIndexMode = "off";

      const event: ProgramEvent = {
        type: "AgentRegistered",
        data: {
          asset: TEST_ASSET,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: true,
          agentUri: "ipfs://QmShouldNotFetch",
        },
      };

      await handleEvent(prisma, event, ctx);
      await new Promise((r) => setTimeout(r, 100));

      expect(mockDigestUri).not.toHaveBeenCalled();
    });

    it("should skip URI digest when agentUri is empty", async () => {
      const event: ProgramEvent = {
        type: "AgentRegistered",
        data: {
          asset: TEST_ASSET,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: true,
          agentUri: "",
        },
      };

      await handleEvent(prisma, event, ctx);
      await new Promise((r) => setTimeout(r, 100));

      expect(mockDigestUri).not.toHaveBeenCalled();
    });
  });

  // ==========================================================================
  // 4. Event handlers with count=0 (out-of-order events)
  // ==========================================================================
  describe("Out-of-order events (count=0 branches)", () => {
    it("handleAgentOwnerSyncedTx: agent not found", async () => {
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 0 });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = {
        type: "AgentOwnerSynced",
        data: { asset: TEST_ASSET, oldOwner: TEST_OWNER, newOwner: TEST_NEW_OWNER },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.agent.updateMany).toHaveBeenCalledWith({
        where: { id: TEST_ASSET.toBase58() },
        data: { owner: TEST_NEW_OWNER.toBase58(), updatedAt: ctx.blockTime },
      });
    });

    it("handleAtomEnabledTx: agent not found", async () => {
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 0 });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = {
        type: "AtomEnabled",
        data: { asset: TEST_ASSET, enabledBy: TEST_OWNER },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.agent.updateMany).toHaveBeenCalledWith({
        where: { id: TEST_ASSET.toBase58() },
        data: { atomEnabled: true, updatedAt: ctx.blockTime },
      });
    });

    it("handleUriUpdatedTx: agent not found", async () => {
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 0 });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/updated.json", updatedBy: TEST_OWNER },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.agent.updateMany).toHaveBeenCalledWith({
        where: { id: TEST_ASSET.toBase58() },
        data: { uri: "https://example.com/updated.json", updatedAt: ctx.blockTime },
      });
    });

    it("handleWalletUpdatedTx: agent not found", async () => {
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 0 });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = {
        type: "WalletUpdated",
        data: { asset: TEST_ASSET, oldWallet: null, newWallet: TEST_WALLET, updatedBy: TEST_OWNER },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.agent.updateMany).toHaveBeenCalledWith({
        where: { id: TEST_ASSET.toBase58() },
        data: { wallet: TEST_WALLET.toBase58(), updatedAt: ctx.blockTime },
      });
    });

    it("handleAtomEnabled (non-atomic): agent not found", async () => {
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 0 });

      const event: ProgramEvent = {
        type: "AtomEnabled",
        data: { asset: TEST_ASSET, enabledBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);

      expect(prisma.agent.updateMany).toHaveBeenCalledWith({
        where: { id: TEST_ASSET.toBase58() },
        data: { atomEnabled: true, updatedAt: ctx.blockTime },
      });
    });

    it("handleUriUpdated (non-atomic): agent not found", async () => {
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 0 });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/new.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);

      expect(prisma.agent.updateMany).toHaveBeenCalled();
    });

    it("handleWalletUpdated (non-atomic): agent not found", async () => {
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 0 });

      const event: ProgramEvent = {
        type: "WalletUpdated",
        data: { asset: TEST_ASSET, oldWallet: null, newWallet: TEST_WALLET, updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);

      expect(prisma.agent.updateMany).toHaveBeenCalled();
    });
  });

  // ==========================================================================
  // 5. handleMetadataSetTx
  // ==========================================================================
  describe("handleMetadataSetTx (via handleEventAtomic)", () => {
    it("should upsert metadata for normal key", async () => {
      const event: ProgramEvent = {
        type: "MetadataSet",
        data: {
          asset: TEST_ASSET,
          key: "description",
          value: new Uint8Array([72, 101, 108, 108, 111]),
          immutable: false,
        },
      };

      (prisma.agentMetadata.findUnique as any).mockResolvedValue(null);
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.agentMetadata.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { agentId_key: { agentId: TEST_ASSET.toBase58(), key: "description" } },
          create: expect.objectContaining({
            agentId: TEST_ASSET.toBase58(),
            key: "description",
            immutable: false,
          }),
        })
      );
    });

    it("should skip _uri: prefix keys", async () => {
      const event: ProgramEvent = {
        type: "MetadataSet",
        data: {
          asset: TEST_ASSET,
          key: "_uri:name",
          value: new Uint8Array([65, 66, 67]),
          immutable: false,
        },
      };

      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.agentMetadata.upsert).not.toHaveBeenCalled();
    });

    it("should skip update when existing metadata is immutable", async () => {
      const event: ProgramEvent = {
        type: "MetadataSet",
        data: {
          asset: TEST_ASSET,
          key: "locked-field",
          value: new Uint8Array([1, 2, 3]),
          immutable: false,
        },
      };

      (prisma.agentMetadata.findUnique as any).mockResolvedValue({ immutable: true });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.agentMetadata.upsert).not.toHaveBeenCalled();
    });

    it("should allow update when existing metadata is not immutable", async () => {
      const event: ProgramEvent = {
        type: "MetadataSet",
        data: {
          asset: TEST_ASSET,
          key: "mutable-field",
          value: new Uint8Array([4, 5, 6]),
          immutable: true,
        },
      };

      (prisma.agentMetadata.findUnique as any).mockResolvedValue({ immutable: false });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.agentMetadata.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({ immutable: true }),
        })
      );
    });
  });

  // ==========================================================================
  // 5b. handleMetadataSet (non-atomic)
  // ==========================================================================
  describe("handleMetadataSet (non-atomic, via handleEvent)", () => {
    it("should skip _uri: prefix keys", async () => {
      const event: ProgramEvent = {
        type: "MetadataSet",
        data: {
          asset: TEST_ASSET,
          key: "_uri:description",
          value: new Uint8Array([65]),
          immutable: false,
        },
      };

      await handleEvent(prisma, event, ctx);

      expect(prisma.agentMetadata.upsert).not.toHaveBeenCalled();
    });

    it("should skip when immutable", async () => {
      const event: ProgramEvent = {
        type: "MetadataSet",
        data: {
          asset: TEST_ASSET,
          key: "frozen",
          value: new Uint8Array([1]),
          immutable: false,
        },
      };

      (prisma.agentMetadata.findUnique as any).mockResolvedValue({ immutable: true });

      await handleEvent(prisma, event, ctx);

      expect(prisma.agentMetadata.upsert).not.toHaveBeenCalled();
    });
  });

  // ==========================================================================
  // 6. handleWalletUpdatedTx with DEFAULT_PUBKEY
  // ==========================================================================
  describe("handleWalletUpdatedTx with DEFAULT_PUBKEY", () => {
    it("should set wallet to null when newWallet is DEFAULT_PUBKEY (atomic)", async () => {
      const defaultPubkey = new PublicKey(DEFAULT_PUBKEY_STR);

      const event: ProgramEvent = {
        type: "WalletUpdated",
        data: { asset: TEST_ASSET, oldWallet: TEST_WALLET, newWallet: defaultPubkey, updatedBy: TEST_OWNER },
      };

      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.agent.updateMany).toHaveBeenCalledWith({
        where: { id: TEST_ASSET.toBase58() },
        data: { wallet: null, updatedAt: ctx.blockTime },
      });
    });

    it("should set wallet to null when DEFAULT_PUBKEY (non-atomic)", async () => {
      const defaultPubkey = new PublicKey(DEFAULT_PUBKEY_STR);

      const event: ProgramEvent = {
        type: "WalletUpdated",
        data: { asset: TEST_ASSET, oldWallet: TEST_WALLET, newWallet: defaultPubkey, updatedBy: TEST_OWNER },
      };

      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });

      await handleEvent(prisma, event, ctx);

      expect(prisma.agent.updateMany).toHaveBeenCalledWith({
        where: { id: TEST_ASSET.toBase58() },
        data: { wallet: null, updatedAt: ctx.blockTime },
      });
    });
  });

  // ==========================================================================
  // 7. handleFeedbackRevokedTx
  // ==========================================================================
  describe("handleFeedbackRevokedTx (via handleEventAtomic)", () => {
    const revokeEventData = {
      asset: TEST_ASSET,
      clientAddress: TEST_CLIENT,
      feedbackIndex: 0n,
      sealHash: TEST_HASH,
      slot: 123456n,
      originalScore: 85,
      atomEnabled: true,
      hadImpact: true,
      newTrustTier: 0,
      newQualityScore: 0,
      newConfidence: 0,
      newRevokeDigest: TEST_HASH,
      newRevokeCount: 1n,
    };

    it("should mark feedback as revoked and store PENDING revocation (matching hash)", async () => {
      (prisma.feedback.findUnique as any).mockResolvedValue({
        feedbackHash: Uint8Array.from(TEST_HASH),
      });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = { type: "FeedbackRevoked", data: revokeEventData };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.feedback.updateMany).toHaveBeenCalledWith({
        where: {
          agentId: TEST_ASSET.toBase58(),
          client: TEST_CLIENT.toBase58(),
          feedbackIndex: 0n,
        },
        data: expect.objectContaining({ revoked: true }),
      });

      expect(prisma.revocation.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({ status: "PENDING" }),
        })
      );
    });

    it("should mark as ORPHANED when seal_hash mismatches", async () => {
      const differentHash = new Uint8Array(32).fill(0xcd);
      (prisma.feedback.findUnique as any).mockResolvedValue({
        feedbackHash: Uint8Array.from(differentHash),
      });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = { type: "FeedbackRevoked", data: revokeEventData };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.revocation.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({ status: "ORPHANED" }),
        })
      );
    });

    it("should store orphan revocation when feedback not found", async () => {
      (prisma.feedback.findUnique as any).mockResolvedValue(null);
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = {
        type: "FeedbackRevoked",
        data: { ...revokeEventData, feedbackIndex: 99n },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.feedback.updateMany).toHaveBeenCalled();
      expect(prisma.revocation.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({ feedbackIndex: 99n, status: "ORPHANED" }),
        })
      );
    });
  });

  // ==========================================================================
  // 8. UriUpdated (non-atomic) with URI digest queue
  // ==========================================================================
  describe("handleUriUpdated (non-atomic) with digest", () => {
    it("should trigger URI digest when URI is present and mode is normal", async () => {
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.agent.findUnique as any).mockResolvedValue({
        uri: "https://example.com/agent.json",
        nftName: "",
      });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/agent.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      await vi.waitFor(() => {
        expect(mockDigestUri).toHaveBeenCalledWith("https://example.com/agent.json");
      }, { timeout: 500 });
    });

    it("should skip URI digest when metadataIndexMode is off", async () => {
      mockConfig.metadataIndexMode = "off";
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/agent.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      await new Promise((r) => setTimeout(r, 100));

      expect(mockDigestUri).not.toHaveBeenCalled();
    });

    it("should skip URI digest when newUri is empty", async () => {
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      await new Promise((r) => setTimeout(r, 100));

      expect(mockDigestUri).not.toHaveBeenCalled();
    });
  });

  // ==========================================================================
  // 9. digestAndStoreUriMetadataLocal
  // ==========================================================================
  describe("digestAndStoreUriMetadataLocal (via handleEvent triggers)", () => {
    it("should early return when metadataIndexMode is off", async () => {
      mockConfig.metadataIndexMode = "off";
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/agent.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      await new Promise((r) => setTimeout(r, 100));

      expect(mockDigestUri).not.toHaveBeenCalled();
    });

    it("should skip when agent not found", async () => {
      (prisma.agent.findUnique as any).mockResolvedValue(null);
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/agent.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      await new Promise((r) => setTimeout(r, 200));

      // digestUri should not have been called because agent not found
      expect(mockDigestUri).not.toHaveBeenCalled();
    });

    it("should skip when agent URI changed (race condition)", async () => {
      (prisma.agent.findUnique as any).mockResolvedValue({
        uri: "https://different-uri.com/agent.json",
        nftName: "",
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/agent.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      await new Promise((r) => setTimeout(r, 200));

      expect(mockDigestUri).not.toHaveBeenCalled();
    });

    it("should skip when URI changed during fetch (TOCTOU)", async () => {
      (prisma.agent.findUnique as any)
        .mockResolvedValueOnce({ uri: "https://example.com/agent.json", nftName: "" })
        .mockResolvedValueOnce({ uri: "https://other.com/agent.json", nftName: "" });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/agent.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      await vi.waitFor(() => {
        expect(mockDigestUri).toHaveBeenCalled();
      }, { timeout: 500 });

      // No metadata stored because recheck failed
      expect(prisma.agentMetadata.deleteMany).not.toHaveBeenCalled();
    });

    it("should store error status when digest result is not ok", async () => {
      (prisma.agent.findUnique as any).mockResolvedValue({
        uri: "https://example.com/bad.json",
        nftName: "",
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      mockDigestUri.mockResolvedValue({
        status: "error",
        error: "HTTP 404",
        bytes: 0,
        hash: null,
      });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/bad.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      await vi.waitFor(() => {
        expect(prisma.agentMetadata.upsert).toHaveBeenCalled();
      }, { timeout: 500 });

      expect(prisma.agentMetadata.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { agentId_key: { agentId: TEST_ASSET.toBase58(), key: "_uri:_status" } },
        })
      );
    });

    it("should store fields and success status when digest is ok", async () => {
      (prisma.agent.findUnique as any).mockResolvedValue({
        uri: "https://example.com/good.json",
        nftName: "",
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      mockDigestUri.mockResolvedValue({
        status: "ok",
        bytes: 512,
        hash: "abc123def",
        fields: {
          "_uri:name": "Test Agent",
          "_uri:description": "A test agent",
        },
        truncatedKeys: false,
      });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/good.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      await vi.waitFor(() => {
        expect(prisma.agentMetadata.deleteMany).toHaveBeenCalled();
      }, { timeout: 500 });

      // Should have purged old _uri: metadata
      expect(prisma.agentMetadata.deleteMany).toHaveBeenCalledWith({
        where: { agentId: TEST_ASSET.toBase58(), key: { startsWith: "_uri:" } },
      });

      // Should have stored each field + status
      expect(prisma.agentMetadata.upsert).toHaveBeenCalled();
    });

    it("should store oversize fields with _meta suffix", async () => {
      (prisma.agent.findUnique as any).mockResolvedValue({
        uri: "https://example.com/big.json",
        nftName: "",
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      mockDigestUri.mockResolvedValue({
        status: "ok",
        bytes: 1000,
        hash: "bighash",
        fields: { "_uri:description": "x".repeat(20000) },
        truncatedKeys: false,
      });
      mockSerializeValue.mockReturnValueOnce({
        value: "",
        oversize: true,
        bytes: 20000,
      });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/big.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      await vi.waitFor(() => {
        expect(prisma.agentMetadata.upsert).toHaveBeenCalled();
      }, { timeout: 500 });

      const upsertCalls = (prisma.agentMetadata.upsert as any).mock.calls;
      const metaCall = upsertCalls.find(
        (c: any) => c[0]?.where?.agentId_key?.key === "_uri:description_meta"
      );
      expect(metaCall).toBeDefined();
    });

    it("should sync nftName from _uri:name when not already set", async () => {
      (prisma.agent.findUnique as any)
        .mockResolvedValueOnce({ uri: "https://example.com/named.json", nftName: "" })
        .mockResolvedValueOnce({ uri: "https://example.com/named.json", nftName: "" })
        .mockResolvedValueOnce({ nftName: "" });

      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      mockDigestUri.mockResolvedValue({
        status: "ok",
        bytes: 100,
        hash: "namehash",
        fields: { "_uri:name": "My Cool Agent" },
        truncatedKeys: false,
      });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/named.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      await vi.waitFor(() => {
        expect(prisma.agent.update).toHaveBeenCalled();
      }, { timeout: 500 });

      expect(prisma.agent.update).toHaveBeenCalledWith({
        where: { id: TEST_ASSET.toBase58() },
        data: { nftName: "My Cool Agent" },
      });
    });

    it("should skip nftName sync when already set", async () => {
      (prisma.agent.findUnique as any)
        .mockResolvedValueOnce({ uri: "https://example.com/named.json", nftName: "" })
        .mockResolvedValueOnce({ uri: "https://example.com/named.json", nftName: "" })
        .mockResolvedValueOnce({ nftName: "Already Set" });

      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      mockDigestUri.mockResolvedValue({
        status: "ok",
        bytes: 100,
        hash: "namehash",
        fields: { "_uri:name": "New Name" },
        truncatedKeys: false,
      });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/named.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      // Wait for async queue to complete
      await vi.waitFor(() => {
        expect(prisma.agentMetadata.upsert).toHaveBeenCalled();
      }, { timeout: 500 });

      expect(prisma.agent.update).not.toHaveBeenCalled();
    });
  });

  // ==========================================================================
  // 10. storeUriMetadataLocal (tested indirectly via digest)
  // ==========================================================================
  describe("storeUriMetadataLocal (tested via digest pipeline)", () => {
    it("should store standard field with raw prefix (0x00)", async () => {
      (prisma.agent.findUnique as any).mockResolvedValue({
        uri: "https://example.com/std.json",
        nftName: "",
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      mockDigestUri.mockResolvedValue({
        status: "ok",
        bytes: 50,
        hash: "stdhash",
        fields: { "_uri:name": "Standard Agent" },
        truncatedKeys: false,
      });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/std.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      await vi.waitFor(() => {
        expect(prisma.agentMetadata.upsert).toHaveBeenCalled();
      }, { timeout: 500 });

      const upsertCalls = (prisma.agentMetadata.upsert as any).mock.calls;
      const nameCall = upsertCalls.find(
        (c: any) => c[0]?.where?.agentId_key?.key === "_uri:name"
      );
      expect(nameCall).toBeDefined();
    });

    it("should store non-standard field with compression", async () => {
      (prisma.agent.findUnique as any).mockResolvedValue({
        uri: "https://example.com/custom.json",
        nftName: "",
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      mockDigestUri.mockResolvedValue({
        status: "ok",
        bytes: 50,
        hash: "customhash",
        fields: { "custom_field": "Custom Value" },
        truncatedKeys: false,
      });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/custom.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      await vi.waitFor(() => {
        expect(mockCompressForStorage).toHaveBeenCalled();
      }, { timeout: 500 });
    });

    it("should handle storage error gracefully", async () => {
      (prisma.agent.findUnique as any).mockResolvedValue({
        uri: "https://example.com/errstore.json",
        nftName: "",
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      mockDigestUri.mockResolvedValue({
        status: "ok",
        bytes: 50,
        hash: "errhash",
        fields: { "_uri:name": "Error Agent" },
        truncatedKeys: false,
      });
      (prisma.agentMetadata.upsert as any).mockRejectedValueOnce(new Error("DB write failed"));

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/errstore.json", updatedBy: TEST_OWNER },
      };

      // Should not throw - error is caught internally
      await handleEvent(prisma, event, ctx);
      await new Promise((r) => setTimeout(r, 300));
    });
  });

  // ==========================================================================
  // 11. cleanupOrphanResponses
  // ==========================================================================
  describe("cleanupOrphanResponses", () => {
    it("should delete old orphans and return count", async () => {
      (prisma.orphanResponse.deleteMany as any).mockResolvedValue({ count: 5 });

      const result = await cleanupOrphanResponses(prisma, 30);

      expect(result).toBe(5);
      expect(prisma.orphanResponse.deleteMany).toHaveBeenCalledWith({
        where: { createdAt: { lt: expect.any(Date) } },
      });
    });

    it("should return 0 when no orphans to clean", async () => {
      (prisma.orphanResponse.deleteMany as any).mockResolvedValue({ count: 0 });

      const result = await cleanupOrphanResponses(prisma);

      expect(result).toBe(0);
    });

    it("should use default maxAgeMinutes of 30", async () => {
      (prisma.orphanResponse.deleteMany as any).mockResolvedValue({ count: 0 });

      const before = Date.now();
      await cleanupOrphanResponses(prisma);

      const call = (prisma.orphanResponse.deleteMany as any).mock.calls[0][0];
      const cutoff = call.where.createdAt.lt as Date;
      const expectedCutoff = before - 30 * 60 * 1000;

      expect(Math.abs(cutoff.getTime() - expectedCutoff)).toBeLessThan(1000);
    });

    it("should respect custom maxAgeMinutes", async () => {
      (prisma.orphanResponse.deleteMany as any).mockResolvedValue({ count: 3 });

      const before = Date.now();
      const result = await cleanupOrphanResponses(prisma, 60);

      expect(result).toBe(3);
      const call = (prisma.orphanResponse.deleteMany as any).mock.calls[0][0];
      const cutoff = call.where.createdAt.lt as Date;
      const expectedCutoff = before - 60 * 60 * 1000;

      expect(Math.abs(cutoff.getTime() - expectedCutoff)).toBeLessThan(1000);
    });
  });

  // ==========================================================================
  // 12. handleEventAtomic routes all event types through handleEventInner
  // ==========================================================================
  describe("handleEventAtomic routes all event types", () => {
    beforeEach(() => {
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);
    });

    it("should handle AtomEnabled atomically", async () => {
      const event: ProgramEvent = {
        type: "AtomEnabled",
        data: { asset: TEST_ASSET, enabledBy: TEST_OWNER },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.$transaction).toHaveBeenCalled();
      expect(prisma.agent.updateMany).toHaveBeenCalled();
    });

    it("should handle RegistryInitialized atomically", async () => {
      const event: ProgramEvent = {
        type: "RegistryInitialized",
        data: { collection: TEST_COLLECTION, authority: TEST_OWNER },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.$transaction).toHaveBeenCalled();
      expect(prisma.registry.upsert).toHaveBeenCalled();
    });

    it("should handle NewFeedback atomically", async () => {
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

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.$transaction).toHaveBeenCalled();
      expect(prisma.feedback.upsert).toHaveBeenCalled();
    });

    it("should store full i128 NewFeedback value as decimal string", async () => {
      const event: ProgramEvent = {
        type: "NewFeedback",
        data: {
          asset: TEST_ASSET,
          clientAddress: TEST_CLIENT,
          feedbackIndex: 0n,
          value: 170141183460469231731687303715884105727n, // i128 max
          valueDecimals: 0,
          score: 100,
          tag1: "max",
          tag2: "",
          endpoint: "",
          feedbackUri: "ipfs://QmLarge",
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

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.feedback.upsert).toHaveBeenCalled();
      const upsertArg = (prisma.feedback.upsert as any).mock.calls[0][0];
      expect(upsertArg.create.value).toBe("170141183460469231731687303715884105727");
    });

    it("should handle ResponseAppended atomically", async () => {
      (prisma.feedback.findUnique as any).mockResolvedValue({
        id: "fb-uuid",
        feedbackHash: Uint8Array.from(TEST_HASH),
      });

      const event: ProgramEvent = {
        type: "ResponseAppended",
        data: {
          asset: TEST_ASSET,
          client: TEST_CLIENT,
          feedbackIndex: 0n,
          responder: TEST_OWNER,
          responseUri: "ipfs://QmResp",
          responseHash: TEST_HASH,
          sealHash: TEST_HASH,
          slot: 123456n,
          newResponseDigest: TEST_HASH,
          newResponseCount: 1n,
        },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.$transaction).toHaveBeenCalled();
      expect(prisma.feedbackResponse.upsert).toHaveBeenCalled();
    });

    it("should handle ResponseAppended as orphan when feedback not found (atomic)", async () => {
      (prisma.feedback.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = {
        type: "ResponseAppended",
        data: {
          asset: TEST_ASSET,
          client: TEST_CLIENT,
          feedbackIndex: 99n,
          responder: TEST_OWNER,
          responseUri: "ipfs://QmOrphan",
          responseHash: TEST_HASH,
          sealHash: TEST_HASH,
          slot: 123456n,
          newResponseDigest: TEST_HASH,
          newResponseCount: 1n,
        },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.orphanResponse.upsert).toHaveBeenCalled();
      expect(prisma.feedbackResponse.upsert).not.toHaveBeenCalled();
    });

    it("should handle MetadataDeleted atomically", async () => {
      const event: ProgramEvent = {
        type: "MetadataDeleted",
        data: { asset: TEST_ASSET, key: "test-key" },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.$transaction).toHaveBeenCalled();
      expect(prisma.agentMetadata.deleteMany).toHaveBeenCalledWith({
        where: { agentId: TEST_ASSET.toBase58(), key: "test-key" },
      });
    });

    it("should handle unknown event type without throwing", async () => {
      const event = { type: "UnknownEvent", data: { foo: "bar" } } as unknown as ProgramEvent;

      await expect(handleEventAtomic(prisma, event, ctx)).resolves.not.toThrow();
    });

    it("should handle ValidationRequested atomically", async () => {
      const event: ProgramEvent = {
        type: "ValidationRequested",
        data: {
          asset: TEST_ASSET,
          validatorAddress: TEST_CLIENT,
          nonce: 1n,
          requestUri: "ipfs://QmVal",
          requestHash: TEST_HASH,
          requester: TEST_OWNER,
        },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.$transaction).toHaveBeenCalled();
      expect(prisma.validation.upsert).toHaveBeenCalled();
    });

    it("should handle ValidationResponded atomically", async () => {
      const event: ProgramEvent = {
        type: "ValidationResponded",
        data: {
          asset: TEST_ASSET,
          validatorAddress: TEST_CLIENT,
          nonce: 1n,
          response: 90,
          responseUri: "ipfs://QmValResp",
          responseHash: TEST_HASH,
          tag: "security",
        },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.$transaction).toHaveBeenCalled();
      expect(prisma.validation.upsert).toHaveBeenCalled();
    });
  });

  // ==========================================================================
  // 13. handleEvent supabase routing + null prisma (lines 254-261)
  // ==========================================================================
  describe("handleEvent supabase routing and null prisma", () => {
    it("should route to supabase when dbMode is supabase (non-atomic)", async () => {
      mockConfig.dbMode = "supabase";

      const event: ProgramEvent = {
        type: "AgentRegistered",
        data: {
          asset: TEST_ASSET,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: true,
          agentUri: "",
        },
      };

      await handleEvent(prisma, event, ctx);

      expect(mockSupabaseHandleEvent).toHaveBeenCalledWith(event, ctx);
      expect(prisma.agent.upsert).not.toHaveBeenCalled();
    });

    it("should throw when prisma is null in local mode (non-atomic)", async () => {
      const event: ProgramEvent = {
        type: "AgentRegistered",
        data: {
          asset: TEST_ASSET,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: true,
          agentUri: "",
        },
      };

      await expect(handleEvent(null, event, ctx)).rejects.toThrow(
        "PrismaClient required for local mode"
      );
    });
  });

  // ==========================================================================
  // 14. handleAgentOwnerSynced (non-atomic) success logging (lines 401-402, 420-422)
  // ==========================================================================
  describe("handleAgentOwnerSynced (non-atomic) success path", () => {
    it("should log success when agent is found for owner sync", async () => {
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });

      const event: ProgramEvent = {
        type: "AgentOwnerSynced",
        data: { asset: TEST_ASSET, oldOwner: TEST_OWNER, newOwner: TEST_NEW_OWNER },
      };

      await handleEvent(prisma, event, ctx);

      expect(prisma.agent.updateMany).toHaveBeenCalledWith({
        where: { id: TEST_ASSET.toBase58() },
        data: { owner: TEST_NEW_OWNER.toBase58(), updatedAt: ctx.blockTime },
      });
    });
  });

  // ==========================================================================
  // 15. handleAtomEnabled (non-atomic) success logging (lines 471-474)
  // ==========================================================================
  describe("handleAtomEnabled (non-atomic) success path", () => {
    it("should log success when agent is found for ATOM enable", async () => {
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });

      const event: ProgramEvent = {
        type: "AtomEnabled",
        data: { asset: TEST_ASSET, enabledBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);

      expect(prisma.agent.updateMany).toHaveBeenCalledWith({
        where: { id: TEST_ASSET.toBase58() },
        data: { atomEnabled: true, updatedAt: ctx.blockTime },
      });
    });
  });

  // ==========================================================================
  // 16. NewFeedback orphan reconciliation (lines 792-820, 862-892)
  // ==========================================================================
  describe("NewFeedback orphan reconciliation", () => {
    const newFeedbackData = {
      asset: TEST_ASSET,
      clientAddress: TEST_CLIENT,
      feedbackIndex: 5n,
      value: 9500n,
      valueDecimals: 2,
      score: 85,
      tag1: "quality",
      tag2: "speed",
      endpoint: "/api/chat",
      feedbackUri: "ipfs://QmFeedback",
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
    };

    it("should reconcile orphan responses in atomic path (lines 792-820)", async () => {
      const feedbackResult = { id: "fb-new-id", feedbackIndex: 5n };
      (prisma.feedback.upsert as any).mockResolvedValue(feedbackResult);
      (prisma.orphanResponse.findMany as any).mockResolvedValue([
        {
          id: "orphan-1",
          agentId: TEST_ASSET.toBase58(),
          client: TEST_CLIENT.toBase58(),
          feedbackIndex: 5n,
          responder: TEST_OWNER.toBase58(),
          responseUri: "ipfs://QmOrphan",
          responseHash: TEST_HASH,
          runningDigest: TEST_HASH,
          txSignature: "orphan-sig",
          slot: 123456n,
        },
      ]);
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = { type: "NewFeedback", data: newFeedbackData };
      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.feedbackResponse.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({
            feedbackId: "fb-new-id",
            responder: TEST_OWNER.toBase58(),
          }),
        })
      );
      expect(prisma.orphanResponse.delete).toHaveBeenCalledWith({ where: { id: "orphan-1" } });
    });

    it("should reconcile orphan responses in non-atomic path (lines 862-892)", async () => {
      const feedbackResult = { id: "fb-non-atomic-id", feedbackIndex: 5n };
      (prisma.feedback.upsert as any).mockResolvedValue(feedbackResult);
      (prisma.orphanResponse.findMany as any).mockResolvedValue([
        {
          id: "orphan-2",
          agentId: TEST_ASSET.toBase58(),
          client: TEST_CLIENT.toBase58(),
          feedbackIndex: 5n,
          responder: TEST_OWNER.toBase58(),
          responseUri: "ipfs://QmOrphan2",
          responseHash: TEST_HASH,
          runningDigest: TEST_HASH,
          txSignature: "orphan-sig-2",
          slot: 123456n,
        },
      ]);

      const event: ProgramEvent = { type: "NewFeedback", data: newFeedbackData };
      await handleEvent(prisma, event, ctx);

      expect(prisma.feedbackResponse.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({
            feedbackId: "fb-non-atomic-id",
            responder: TEST_OWNER.toBase58(),
          }),
        })
      );
      expect(prisma.orphanResponse.delete).toHaveBeenCalledWith({ where: { id: "orphan-2" } });
    });

    it("should skip reconciliation when no orphans found", async () => {
      (prisma.feedback.upsert as any).mockResolvedValue({ id: "fb-no-orphans", feedbackIndex: 5n });
      (prisma.orphanResponse.findMany as any).mockResolvedValue([]);

      const event: ProgramEvent = { type: "NewFeedback", data: newFeedbackData };
      await handleEvent(prisma, event, ctx);

      expect(prisma.feedbackResponse.upsert).not.toHaveBeenCalled();
      expect(prisma.orphanResponse.delete).not.toHaveBeenCalled();
    });
  });

  // ==========================================================================
  // 17. ResponseAppended seal_hash mismatch (lines 1079-1085)
  // ==========================================================================
  describe("ResponseAppended seal_hash mismatch in atomic path", () => {
    it("should log warning and store response as ORPHANED on seal_hash mismatch", async () => {
      const differentHash = new Uint8Array(32).fill(0xee);
      (prisma.feedback.findUnique as any).mockResolvedValue({
        id: "fb-mismatch",
        feedbackHash: Uint8Array.from(differentHash),
      });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = {
        type: "ResponseAppended",
        data: {
          asset: TEST_ASSET,
          client: TEST_CLIENT,
          feedbackIndex: 0n,
          responder: TEST_OWNER,
          responseUri: "ipfs://QmResp",
          responseHash: TEST_HASH,
          sealHash: TEST_HASH,
          slot: 123456n,
          newResponseDigest: TEST_HASH,
          newResponseCount: 1n,
        },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.feedbackResponse.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({
            feedbackId: "fb-mismatch",
            status: "ORPHANED",
          }),
        })
      );
    });
  });

  // ==========================================================================
  // 18. digestAndStoreUriMetadataLocal purge error (line 1443-1444)
  // ==========================================================================
  describe("digestAndStoreUriMetadataLocal purge error", () => {
    it("should continue when purging old URI metadata fails", async () => {
      (prisma.agent.findUnique as any)
        .mockResolvedValueOnce({ uri: "https://example.com/purge-err.json", nftName: "" })
        .mockResolvedValueOnce({ uri: "https://example.com/purge-err.json", nftName: "" });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.agentMetadata.deleteMany as any).mockRejectedValueOnce(new Error("Purge failed"));
      mockDigestUri.mockResolvedValue({
        status: "ok",
        bytes: 100,
        hash: "purgehash",
        fields: { "_uri:name": "Purge Test" },
        truncatedKeys: false,
      });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/purge-err.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      await vi.waitFor(() => {
        expect(prisma.agentMetadata.upsert).toHaveBeenCalled();
      }, { timeout: 500 });

      // Should still store metadata despite purge failure
      expect(prisma.agentMetadata.upsert).toHaveBeenCalled();
    });
  });

  // ==========================================================================
  // 19. nftName sync error (lines 1498-1499)
  // ==========================================================================
  describe("nftName sync error handling", () => {
    it("should handle error when syncing nftName fails", async () => {
      (prisma.agent.findUnique as any)
        .mockResolvedValueOnce({ uri: "https://example.com/name-err.json", nftName: "" })
        .mockResolvedValueOnce({ uri: "https://example.com/name-err.json", nftName: "" })
        .mockResolvedValueOnce({ nftName: "" }); // nftName check
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.agent.update as any).mockRejectedValueOnce(new Error("Update failed"));
      mockDigestUri.mockResolvedValue({
        status: "ok",
        bytes: 100,
        hash: "nameerrhash",
        fields: { "_uri:name": "Error Name Agent" },
        truncatedKeys: false,
      });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/name-err.json", updatedBy: TEST_OWNER },
      };

      // Should not throw - error is caught internally
      await handleEvent(prisma, event, ctx);
      await new Promise((r) => setTimeout(r, 300));
    });
  });

  // ==========================================================================
  // 20. handleEvent unknown event type (non-atomic) (line 316-317)
  // ==========================================================================
  describe("handleEvent unknown event type", () => {
    it("should handle unknown event type without throwing (non-atomic)", async () => {
      const event = { type: "UnknownEvent", data: { foo: "bar" } } as unknown as ProgramEvent;
      await expect(handleEvent(prisma, event, ctx)).resolves.not.toThrow();
    });
  });

  // ==========================================================================
  // 21. FeedbackRevoked non-atomic paths
  // ==========================================================================
  describe("FeedbackRevoked (non-atomic, via handleEvent)", () => {
    const revokeData = {
      asset: TEST_ASSET,
      clientAddress: TEST_CLIENT,
      feedbackIndex: 0n,
      sealHash: TEST_HASH,
      slot: 123456n,
      originalScore: 85,
      atomEnabled: true,
      hadImpact: true,
      newTrustTier: 0,
      newQualityScore: 0,
      newConfidence: 0,
      newRevokeDigest: TEST_HASH,
      newRevokeCount: 1n,
    };

    it("should handle revocation with matching hash (non-atomic)", async () => {
      (prisma.feedback.findUnique as any).mockResolvedValue({
        feedbackHash: Uint8Array.from(TEST_HASH),
      });

      const event: ProgramEvent = { type: "FeedbackRevoked", data: revokeData };
      await handleEvent(prisma, event, ctx);

      expect(prisma.revocation.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({ status: "PENDING" }),
        })
      );
    });

    it("should handle revocation as orphan when feedback not found (non-atomic)", async () => {
      (prisma.feedback.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = { type: "FeedbackRevoked", data: revokeData };
      await handleEvent(prisma, event, ctx);

      expect(prisma.revocation.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({ status: "ORPHANED" }),
        })
      );
    });

    it("should handle seal_hash mismatch (non-atomic)", async () => {
      const differentHash = new Uint8Array(32).fill(0xdd);
      (prisma.feedback.findUnique as any).mockResolvedValue({
        feedbackHash: Uint8Array.from(differentHash),
      });

      const event: ProgramEvent = { type: "FeedbackRevoked", data: revokeData };
      await handleEvent(prisma, event, ctx);

      expect(prisma.revocation.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({ status: "ORPHANED" }),
        })
      );
    });
  });

  // ==========================================================================
  // 22. ResponseAppended non-atomic paths
  // ==========================================================================
  describe("ResponseAppended (non-atomic, via handleEvent)", () => {
    const responseData = {
      asset: TEST_ASSET,
      client: TEST_CLIENT,
      feedbackIndex: 0n,
      responder: TEST_OWNER,
      responseUri: "ipfs://QmResp",
      responseHash: TEST_HASH,
      sealHash: TEST_HASH,
      slot: 123456n,
      newResponseDigest: TEST_HASH,
      newResponseCount: 1n,
    };

    it("should store response when feedback found (non-atomic)", async () => {
      (prisma.feedback.findUnique as any).mockResolvedValue({
        id: "fb-found",
        feedbackHash: Uint8Array.from(TEST_HASH),
      });

      const event: ProgramEvent = { type: "ResponseAppended", data: responseData };
      await handleEvent(prisma, event, ctx);

      expect(prisma.feedbackResponse.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({ feedbackId: "fb-found" }),
        })
      );
    });

    it("should store orphan when feedback not found (non-atomic)", async () => {
      (prisma.feedback.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = { type: "ResponseAppended", data: responseData };
      await handleEvent(prisma, event, ctx);

      expect(prisma.orphanResponse.upsert).toHaveBeenCalled();
      expect(prisma.feedbackResponse.upsert).not.toHaveBeenCalled();
    });

    it("should warn and store response as ORPHANED on seal_hash mismatch (non-atomic)", async () => {
      const differentHash = new Uint8Array(32).fill(0xcc);
      (prisma.feedback.findUnique as any).mockResolvedValue({
        id: "fb-mismatch-na",
        feedbackHash: Uint8Array.from(differentHash),
      });

      const event: ProgramEvent = { type: "ResponseAppended", data: responseData };
      await handleEvent(prisma, event, ctx);

      expect(prisma.feedbackResponse.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({
            feedbackId: "fb-mismatch-na",
            status: "ORPHANED",
          }),
        })
      );
    });
  });

  // ==========================================================================
  // 23. RegistryInitialized non-atomic (non-Tx path)
  // ==========================================================================
  describe("RegistryInitialized (non-atomic, via handleEvent)", () => {
    it("should upsert registry in non-atomic path", async () => {
      const event: ProgramEvent = {
        type: "RegistryInitialized",
        data: { collection: TEST_COLLECTION, authority: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);

      expect(prisma.registry.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({
            id: TEST_COLLECTION.toBase58(),
            registryType: "Base",
          }),
        })
      );
    });
  });

  // ==========================================================================
  // 24. ValidationRequested / ValidationResponded non-atomic
  // ==========================================================================
  describe("Validation events (non-atomic, via handleEvent)", () => {
    it("should handle ValidationRequested (non-atomic)", async () => {
      const event: ProgramEvent = {
        type: "ValidationRequested",
        data: {
          asset: TEST_ASSET,
          validatorAddress: TEST_CLIENT,
          nonce: 1n,
          requestUri: "ipfs://QmVal",
          requestHash: TEST_HASH,
          requester: TEST_OWNER,
        },
      };

      await handleEvent(prisma, event, ctx);

      expect(prisma.validation.upsert).toHaveBeenCalled();
    });

    it("should handle ValidationResponded (non-atomic)", async () => {
      const event: ProgramEvent = {
        type: "ValidationResponded",
        data: {
          asset: TEST_ASSET,
          validatorAddress: TEST_CLIENT,
          nonce: 1n,
          response: 90,
          responseUri: "ipfs://QmValResp",
          responseHash: TEST_HASH,
          tag: "security",
        },
      };

      await handleEvent(prisma, event, ctx);

      expect(prisma.validation.upsert).toHaveBeenCalled();
    });
  });

  // ==========================================================================
  // 25. MetadataDeleted non-atomic
  // ==========================================================================
  describe("MetadataDeleted (non-atomic, via handleEvent)", () => {
    it("should delete metadata in non-atomic path", async () => {
      const event: ProgramEvent = {
        type: "MetadataDeleted",
        data: { asset: TEST_ASSET, key: "test-key" },
      };

      await handleEvent(prisma, event, ctx);

      expect(prisma.agentMetadata.deleteMany).toHaveBeenCalledWith({
        where: { agentId: TEST_ASSET.toBase58(), key: "test-key" },
      });
    });
  });
});
