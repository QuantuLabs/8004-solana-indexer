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
    validationIndexEnabled: true,
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
  toDeterministicUriStatus: vi.fn().mockImplementation((result: any) => {
    if (result?.status === "ok") {
      return {
        status: "ok",
        bytes: result.bytes ?? null,
        hash: result.hash ?? null,
        fieldCount: Object.keys(result.fields ?? {}).length,
        truncatedKeys: Boolean(result.truncatedKeys),
      };
    }
    return { status: "error", kind: "fetch_failed", retryable: true };
  }),
}));

vi.mock("../../../src/utils/compression.js", () => ({
  compressForStorage: mockCompressForStorage,
}));

vi.mock("../../../src/utils/sanitize.js", () => ({
  stripNullBytes: mockStripNullBytes,
}));

import {
  handleEventAtomic,
  cleanupOrphanResponses,
  handleEvent,
  EventContext,
  enableLocalDerivedDigests,
  resetLocalDerivedDigestsForTests,
  suspendLocalDerivedDigests,
  resumeLocalDerivedDigests,
} from "../../../src/db/handlers.js";
import { ProgramEvent } from "../../../src/parser/types.js";

const DEFAULT_PUBKEY_STR = "11111111111111111111111111111111";

describe("DB Handlers Coverage", () => {
  let prisma: ReturnType<typeof createMockPrismaClient>;
  let ctx: EventContext;

  beforeEach(() => {
    resetLocalDerivedDigestsForTests(true);
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
    mockConfig.validationIndexEnabled = true;
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

    it("should retry atomic transaction once on collection_id unique conflict", async () => {
      (prisma as any).$executeRawUnsafe = undefined;
      const uniqueError = Object.assign(
        new Error("Unique constraint failed on the fields: (`collection_id`)"),
        { code: "P2002", meta: { target: ["collection_id"] } }
      );
      (prisma.$transaction as any)
        .mockRejectedValueOnce(uniqueError)
        .mockImplementationOnce(async (fn: (tx: any) => Promise<any>) => fn(prisma));

      (prisma.agent.findUnique as any).mockResolvedValue({
        collectionPointer: "c1:old-pointer",
        creator: TEST_OWNER.toBase58(),
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.collection.upsert as any).mockResolvedValue({});
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = {
        type: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET,
          setBy: TEST_NEW_OWNER,
          col: "c1:new-pointer",
          lock: true,
        },
      };

      await expect(handleEventAtomic(prisma, event, ctx)).resolves.not.toThrow();
      expect(prisma.$transaction).toHaveBeenCalledTimes(2);
      expect(prisma.collection.upsert).toHaveBeenCalled();
      expect(prisma.indexerState.upsert).toHaveBeenCalled();
    });

    it("should leave agentId null until deterministic verifier backfill assigns it", async () => {
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

      (prisma.agent.findUnique as any).mockResolvedValue(null);
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.agent.findMany).not.toHaveBeenCalled();
      expect(prisma.agent.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({ agentId: null }),
          update: expect.not.objectContaining({ agentId: expect.anything() }),
        })
      );
    });

    it("should preserve existing agentId without reassigning", async () => {
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

      (prisma.agent.findUnique as any).mockResolvedValue({
        agentId: 9n,
        creator: TEST_OWNER.toBase58(),
      });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.agent.findMany).not.toHaveBeenCalledWith(
        expect.objectContaining({ where: { agentId: { not: null } } })
      );
      const upsertArgs = (prisma.agent.upsert as any).mock.calls[0]?.[0];
      expect(upsertArgs?.create?.agentId).toBeNull();
      expect(upsertArgs?.update?.agentId).toBeUndefined();
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
        agentId: 9n,
        creator: TEST_OWNER.toBase58(),
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

    it("should reject same-slot cursor regression when signature is older", async () => {
      (prisma.indexerState.findUnique as any).mockResolvedValue({
        lastSlot: 5000n,
        lastSignature: "sig-z",
      });

      const staleCtx = { ...ctx, slot: 5000n, signature: "sig-a" };
      await handleEventAtomic(prisma, simpleEvent, staleCtx);

      expect(prisma.indexerState.upsert).not.toHaveBeenCalled();
    });

    it("should allow same-slot cursor advance when signature is newer", async () => {
      (prisma.indexerState.findUnique as any).mockResolvedValue({
        lastSlot: 5000n,
        lastSignature: "sig-a",
      });

      const advancedCtx = { ...ctx, slot: 5000n, signature: "sig-z" };
      await handleEventAtomic(prisma, simpleEvent, advancedCtx);

      expect(prisma.indexerState.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          update: expect.objectContaining({
            lastSignature: "sig-z",
            lastSlot: 5000n,
          }),
        })
      );
    });

    it("should allow same-slot cursor advance when persisted tx index is null and next tx index is resolved", async () => {
      (prisma.indexerState.findUnique as any).mockResolvedValue({
        lastSlot: 5000n,
        lastTxIndex: null,
        lastSignature: "sig-a",
      });

      const advancedCtx = { ...ctx, slot: 5000n, txIndex: 0, signature: "sig-b" };
      await handleEventAtomic(prisma, simpleEvent, advancedCtx);

      expect(prisma.indexerState.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          update: expect.objectContaining({
            lastSignature: "sig-b",
            lastSlot: 5000n,
            lastTxIndex: 0,
          }),
        })
      );
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
        agentId: 9n,
        creator: TEST_OWNER.toBase58(),
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
  // 5. MetadataSet via handleEventAtomic
  // ==========================================================================
  describe("MetadataSet via handleEventAtomic", () => {
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
      (prisma.$transaction as any).mockImplementationOnce(async (fn: (tx: any) => Promise<any>) => {
        expect(prisma.agentMetadata.upsert).toHaveBeenCalled();
        return fn(prisma);
      });

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
      expect(prisma.indexerState.upsert).toHaveBeenCalled();
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

    it("should not advance cursor when metadata write fails", async () => {
      const event: ProgramEvent = {
        type: "MetadataSet",
        data: {
          asset: TEST_ASSET,
          key: "description",
          value: new Uint8Array([72]),
          immutable: false,
        },
      };

      const writeError = new Error("metadata write failed");
      (prisma.agentMetadata.findUnique as any).mockResolvedValue(null);
      (prisma.agentMetadata.upsert as any).mockRejectedValue(writeError);

      await expect(handleEventAtomic(prisma, event, ctx)).rejects.toThrow("metadata write failed");
      expect(prisma.$transaction).not.toHaveBeenCalled();
      expect(prisma.indexerState.upsert).not.toHaveBeenCalled();
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

    it("should keep PENDING when seal_hash mismatches but feedback exists", async () => {
      const differentHash = new Uint8Array(32).fill(0xcd);
      (prisma.feedback.findUnique as any).mockResolvedValue({
        feedbackHash: Uint8Array.from(differentHash),
      });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = { type: "FeedbackRevoked", data: revokeEventData };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.revocation.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({ status: "PENDING" }),
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

      expect(prisma.feedback.updateMany).not.toHaveBeenCalled();
      expect(prisma.revocation.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({ feedbackIndex: 99n, status: "ORPHANED" }),
        })
      );
    });

    it("should preserve all-zero sealHash bytes for atomic revocations", async () => {
      const zeroBytes = new Uint8Array(32).fill(0);
      (prisma.feedback.findUnique as any).mockResolvedValue({
        feedbackHash: Uint8Array.from(TEST_HASH),
      });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = {
        type: "FeedbackRevoked",
        data: { ...revokeEventData, sealHash: zeroBytes },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.revocation.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({
            feedbackHash: zeroBytes,
          }),
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
        where: {
          agentId: TEST_ASSET.toBase58(),
          key: { startsWith: "_uri:" },
          NOT: { key: "_uri:_source" },
          immutable: false,
        },
      });

      // Should have stored each field + status
      expect(prisma.agentMetadata.upsert).toHaveBeenCalled();
    });

    it("should keep the frozen event timestamp for URI metadata", async () => {
      const updatedAt = new Date("2026-03-06T12:00:00.000Z");
      const createdAt = new Date("2026-03-06T11:00:00.000Z");
      (prisma.agent.findUnique as any)
        .mockResolvedValueOnce({
          uri: "https://example.com/deterministic.json",
          updatedAt,
          createdAt,
        })
        .mockResolvedValueOnce({
          uri: "https://example.com/deterministic.json",
          updatedAt,
          createdAt,
        });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      mockDigestUri.mockResolvedValue({
        status: "ok",
        bytes: 100,
        hash: "stablehash",
        fields: { "_uri:description": "Stable" },
        truncatedKeys: false,
      });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/deterministic.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      await vi.waitFor(() => {
        expect(prisma.agentMetadata.upsert).toHaveBeenCalled();
      }, { timeout: 500 });

      const upsertCalls = (prisma.agentMetadata.upsert as any).mock.calls;
      const statusCall = upsertCalls.find(
        (c: any) => c[0]?.where?.agentId_key?.key === "_uri:_status"
      );
      expect(statusCall).toBeDefined();
      expect(statusCall[0].create.status).toBe("FINALIZED");
      expect(statusCall[0].create.verifiedAt).toEqual(ctx.blockTime);
      expect(statusCall[0].update.status).toBe("FINALIZED");
      expect(statusCall[0].update.verifiedAt).toEqual(ctx.blockTime);
      expect(prisma.agent.update).not.toHaveBeenCalled();
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

    it("should queue URI digest work until local derived digests are enabled", async () => {
      resetLocalDerivedDigestsForTests(false);
      (prisma.agent.findUnique as any).mockResolvedValue({
        uri: "https://example.com/queued.json",
        nftName: "",
        updatedAt: ctx.blockTime,
        createdAt: ctx.blockTime,
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      mockDigestUri.mockResolvedValue({
        status: "ok",
        bytes: 100,
        hash: "queuedhash",
        fields: { "_uri:name": "Queued Agent" },
        truncatedKeys: false,
      });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/queued.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      await new Promise((r) => setTimeout(r, 100));

      expect(mockDigestUri).not.toHaveBeenCalled();
      expect(prisma.agentMetadata.upsert).not.toHaveBeenCalled();

      enableLocalDerivedDigests(prisma as any);

      await vi.waitFor(() => {
        expect(mockDigestUri).toHaveBeenCalledTimes(1);
        expect(prisma.agentMetadata.upsert).toHaveBeenCalled();
      }, { timeout: 500 });
    });

    it("should sweep URI recovery beyond the first recovery page", async () => {
      vi.useFakeTimers();
      resetLocalDerivedDigestsForTests(false);
      const previousNodeEnv = process.env.NODE_ENV;
      process.env.NODE_ENV = "production";

      const firstPage = Array.from({ length: 500 }, (_, index) => ({
        id: `agent-${String(index).padStart(3, "0")}`,
        uri: `https://example.com/${index}.json`,
      }));
      const legacyAgent = {
        id: "legacy-agent",
        uri: "https://example.com/legacy.json",
      };
      const okStatus = Buffer.concat([Buffer.from([0x00]), Buffer.from('{"status":"ok"}')]);

      (prisma.agent.findMany as any).mockImplementation(async (args: any) => {
        const cursor = args?.where?.id?.gt ?? null;
        if (!cursor) return firstPage;
        if (cursor === firstPage[firstPage.length - 1].id) return [legacyAgent];
        return [];
      });
      (prisma.agentMetadata.findMany as any).mockImplementation(async (args: any) => {
        const ids = args?.where?.agentId?.in ?? [];
        if (ids.includes(legacyAgent.id)) {
          return [];
        }
        return ids.flatMap((id: string) => {
          const agent = firstPage.find((entry) => entry.id === id);
          return [
            {
              agentId: id,
              key: "_uri:_source",
              value: Buffer.concat([Buffer.from([0x00]), Buffer.from(agent?.uri ?? "")]),
            },
            {
              agentId: id,
              key: "_uri:_status",
              value: okStatus,
            },
          ];
        });
      });
      mockDigestUri.mockResolvedValue({
        status: "ok",
        bytes: 123,
        hash: "legacyhash",
        fields: { "_uri:name": "Legacy Agent" },
        truncatedKeys: false,
      });

      try {
        enableLocalDerivedDigests(prisma as any);
        await vi.advanceTimersByTimeAsync(0);

        expect(prisma.agent.findMany).toHaveBeenCalledTimes(1);
        expect(mockDigestUri).not.toHaveBeenCalled();

        await vi.advanceTimersByTimeAsync(60_000);
        expect(prisma.agent.findMany).toHaveBeenCalledTimes(2);
        expect((prisma.agent.findMany as any).mock.calls[1][0]?.where?.id?.gt).toBe(firstPage[firstPage.length - 1].id);
        expect((prisma.agentMetadata.findMany as any).mock.calls.some((call: any[]) =>
          Array.isArray(call[0]?.where?.agentId?.in) && call[0].where.agentId.in.includes(legacyAgent.id)
        )).toBe(true);
      } finally {
        process.env.NODE_ENV = previousNodeEnv;
        resetLocalDerivedDigestsForTests(true);
        vi.useRealTimers();
      }
    });

    it("should defer queued URI digest execution while local ingestion is suspended", async () => {
      const event: ProgramEvent = {
        type: "AgentRegistered",
        data: {
          asset: TEST_ASSET,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: false,
          agentUri: "https://example.com/suspended.json",
        },
      };

      (prisma.indexerState.findUnique as any).mockResolvedValue(null);
      (prisma.agent.findUnique as any).mockResolvedValue({
        uri: "https://example.com/suspended.json",
        nftName: "",
        creator: TEST_OWNER.toBase58(),
        updatedAt: ctx.blockTime,
        createdAt: ctx.blockTime,
      });

      suspendLocalDerivedDigests();
      await handleEventAtomic(prisma, event, ctx);

      expect(mockDigestUri).not.toHaveBeenCalled();

      resumeLocalDerivedDigests();

      await vi.waitFor(() => {
        expect(mockDigestUri).toHaveBeenCalledTimes(1);
      }, { timeout: 500 });
    });

    it("should wait for an in-flight local URI digest before suspending", async () => {
      let releaseDigest: (() => void) | null = null;
      mockDigestUri.mockImplementationOnce(() => new Promise((resolve) => {
        releaseDigest = () => resolve({
          status: "ok",
          bytes: 100,
          hash: "inflight",
          fields: {},
          truncatedKeys: false,
        });
      }));

      const event: ProgramEvent = {
        type: "AgentRegistered",
        data: {
          asset: TEST_ASSET,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: false,
          agentUri: "https://example.com/inflight.json",
        },
      };

      (prisma.indexerState.findUnique as any).mockResolvedValue(null);
      (prisma.agent.findUnique as any).mockResolvedValue({
        uri: "https://example.com/inflight.json",
        nftName: "",
        creator: TEST_OWNER.toBase58(),
        updatedAt: ctx.blockTime,
        createdAt: ctx.blockTime,
      });

      await handleEventAtomic(prisma, event, ctx);
      await new Promise((r) => setTimeout(r, 0));

      const suspendPromise = suspendLocalDerivedDigests();
      let settled = false;
      void suspendPromise.then(() => {
        settled = true;
      });

      await new Promise((r) => setTimeout(r, 25));
      expect(settled).toBe(false);

      releaseDigest?.();
      await suspendPromise;

      expect(mockDigestUri).toHaveBeenCalledTimes(1);
      resumeLocalDerivedDigests();
    });

    it("should defer same-asset URI rollover queued during suspension", async () => {
      let releaseFirstDigest: (() => void) | null = null;
      let currentUri = "https://example.com/first.json";
      mockDigestUri
        .mockImplementationOnce(() => new Promise((resolve) => {
          releaseFirstDigest = () => resolve({
            status: "ok",
            bytes: 100,
            hash: "first",
            fields: {},
            truncatedKeys: false,
          });
        }))
        .mockResolvedValueOnce({
          status: "ok",
          bytes: 100,
          hash: "second",
          fields: {},
          truncatedKeys: false,
        });

      const makeEvent = (uri: string): ProgramEvent => ({
        type: "AgentRegistered",
        data: {
          asset: TEST_ASSET,
          collection: TEST_COLLECTION,
          owner: TEST_OWNER,
          atomEnabled: false,
          agentUri: uri,
        },
      });

      (prisma.indexerState.findUnique as any).mockResolvedValue(null);
      (prisma.agent.findUnique as any).mockImplementation(async () => ({
        uri: currentUri,
        nftName: "",
        creator: TEST_OWNER.toBase58(),
        updatedAt: ctx.blockTime,
        createdAt: ctx.blockTime,
      }));

      await handleEventAtomic(prisma, makeEvent(currentUri), ctx);
      await new Promise((r) => setTimeout(r, 0));

      const suspendPromise = suspendLocalDerivedDigests();
      let settled = false;
      void suspendPromise.then(() => {
        settled = true;
      });

      await new Promise((r) => setTimeout(r, 25));
      expect(settled).toBe(false);
      expect(mockDigestUri).toHaveBeenCalledTimes(1);

      currentUri = "https://example.com/second.json";
      await handleEventAtomic(prisma, makeEvent(currentUri), ctx);

      releaseFirstDigest?.();
      await suspendPromise;

      expect(mockDigestUri).toHaveBeenCalledTimes(1);

      resumeLocalDerivedDigests();

      await vi.waitFor(() => {
        expect(mockDigestUri).toHaveBeenCalledTimes(2);
      }, { timeout: 500 });
    });

    it("should keep only the latest queued URI digest per asset until local derived digests are enabled", async () => {
      resetLocalDerivedDigestsForTests(false);
      (prisma.agent.findUnique as any).mockResolvedValue({
        uri: "https://example.com/latest.json",
        nftName: "",
        updatedAt: ctx.blockTime,
        createdAt: ctx.blockTime,
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      mockDigestUri.mockResolvedValue({
        status: "ok",
        bytes: 101,
        hash: "latesthash",
        fields: { "_uri:name": "Latest Agent" },
        truncatedKeys: false,
      });

      await handleEvent(prisma, {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/stale.json", updatedBy: TEST_OWNER },
      }, ctx);
      await handleEvent(prisma, {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/latest.json", updatedBy: TEST_OWNER },
      }, { ...ctx, signature: "sig-latest" });

      await new Promise((r) => setTimeout(r, 100));

      expect(mockDigestUri).not.toHaveBeenCalled();
      expect(prisma.agentMetadata.upsert).not.toHaveBeenCalled();

      enableLocalDerivedDigests(prisma as any);

      await vi.waitFor(() => {
        expect(mockDigestUri).toHaveBeenCalledTimes(1);
        expect(mockDigestUri).toHaveBeenCalledWith("https://example.com/latest.json");
        expect(prisma.agentMetadata.upsert).toHaveBeenCalled();
      }, { timeout: 500 });
    });

    it("should not overwrite immutable URI-derived metadata entries", async () => {
      (prisma.agent.findUnique as any).mockResolvedValue({
        uri: "https://example.com/immutable.json",
        nftName: "",
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.agentMetadata.findUnique as any).mockImplementation((args: any) => {
        if (args?.where?.agentId_key?.key === "_uri:name") {
          return Promise.resolve({ immutable: true });
        }
        return Promise.resolve(null);
      });
      mockDigestUri.mockResolvedValue({
        status: "ok",
        bytes: 50,
        hash: "immutablehash",
        fields: { "_uri:name": "ShouldNotOverwrite" },
        truncatedKeys: false,
      });

      const event: ProgramEvent = {
        type: "UriUpdated",
        data: { asset: TEST_ASSET, newUri: "https://example.com/immutable.json", updatedBy: TEST_OWNER },
      };

      await handleEvent(prisma, event, ctx);
      await vi.waitFor(() => {
        expect(prisma.agentMetadata.upsert).toHaveBeenCalled();
      }, { timeout: 500 });

      const upsertCalls = (prisma.agentMetadata.upsert as any).mock.calls;
      const immutableOverwriteCall = upsertCalls.find(
        (c: any) => c[0]?.where?.agentId_key?.key === "_uri:name"
      );
      expect(immutableOverwriteCall).toBeUndefined();
    });
  });

  // ==========================================================================
  // 11. cleanupOrphanResponses
  // ==========================================================================
  describe("cleanupOrphanResponses", () => {
    it("should delete old orphans and return count", async () => {
      (prisma.orphanResponse.deleteMany as any).mockResolvedValue({ count: 5 });
      (prisma.indexerState.findUnique as any).mockResolvedValue({ lastSlot: 1000n });

      const result = await cleanupOrphanResponses(prisma, 30);

      expect(result).toBe(5);
      const call = (prisma.orphanResponse.deleteMany as any).mock.calls[0][0];
      expect(call.where.slot.not).toBeNull();
      expect(typeof call.where.slot.lt).toBe("bigint");
    });

    it("should return 0 when no orphans to clean", async () => {
      (prisma.orphanResponse.deleteMany as any).mockResolvedValue({ count: 0 });
      (prisma.indexerState.findUnique as any).mockResolvedValue({ lastSlot: 1000n });

      const result = await cleanupOrphanResponses(prisma);

      expect(result).toBe(0);
    });

    it("should use default maxAgeMinutes of 30", async () => {
      (prisma.orphanResponse.deleteMany as any).mockResolvedValue({ count: 0 });
      (prisma.indexerState.findUnique as any).mockResolvedValue({ lastSlot: 10_000n });
      await cleanupOrphanResponses(prisma);

      const call = (prisma.orphanResponse.deleteMany as any).mock.calls[0][0];
      const cutoff = call.where.slot.lt as bigint;
      const slotsToKeep = BigInt(Math.ceil((30 * 60_000) / 400));
      const expectedCutoff = 10_000n > slotsToKeep ? 10_000n - slotsToKeep : 0n;
      expect(cutoff).toBe(expectedCutoff);
    });

    it("should respect custom maxAgeMinutes", async () => {
      (prisma.orphanResponse.deleteMany as any).mockResolvedValue({ count: 3 });
      (prisma.indexerState.findUnique as any).mockResolvedValue({ lastSlot: 20_000n });
      const result = await cleanupOrphanResponses(prisma, 60);

      expect(result).toBe(3);
      const call = (prisma.orphanResponse.deleteMany as any).mock.calls[0][0];
      const cutoff = call.where.slot.lt as bigint;
      const slotsToKeep = BigInt(Math.ceil((60 * 60_000) / 400));
      const expectedCutoff = 20_000n > slotsToKeep ? 20_000n - slotsToKeep : 0n;
      expect(cutoff).toBe(expectedCutoff);
    });

    it("should skip cleanup when lastSlot is missing", async () => {
      (prisma.indexerState.findUnique as any).mockResolvedValue({ lastSlot: null });

      const result = await cleanupOrphanResponses(prisma, 30);

      expect(result).toBe(0);
      expect(prisma.orphanResponse.deleteMany).not.toHaveBeenCalled();
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

    it("should preserve all-zero responseHash bytes for atomic responses", async () => {
      const zeroBytes = new Uint8Array(32).fill(0);
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
          responseHash: zeroBytes,
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
            responseHash: zeroBytes,
          }),
        })
      );
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

    it("should handle MetadataDeleted before advancing cursor", async () => {
      const event: ProgramEvent = {
        type: "MetadataDeleted",
        data: { asset: TEST_ASSET, key: "test-key" },
      };

      (prisma.$transaction as any).mockImplementationOnce(async (fn: (tx: any) => Promise<any>) => {
        expect(prisma.agentMetadata.deleteMany).toHaveBeenCalledWith({
          where: { agentId: TEST_ASSET.toBase58(), key: "test-key" },
        });
        return fn(prisma);
      });

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.$transaction).toHaveBeenCalled();
      expect(prisma.indexerState.upsert).toHaveBeenCalled();
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

    it("should preserve all-zero sealHash bytes for non-atomic revocations", async () => {
      const zeroBytes = new Uint8Array(32).fill(0);
      (prisma.feedback.findUnique as any).mockResolvedValue({
        feedbackHash: Uint8Array.from(TEST_HASH),
      });

      const event: ProgramEvent = {
        type: "FeedbackRevoked",
        data: { ...revokeData, sealHash: zeroBytes },
      };
      await handleEvent(prisma, event, ctx);

      expect(prisma.revocation.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({ feedbackHash: zeroBytes }),
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

    it("should keep PENDING on seal_hash mismatch when feedback exists (non-atomic)", async () => {
      const differentHash = new Uint8Array(32).fill(0xdd);
      (prisma.feedback.findUnique as any).mockResolvedValue({
        feedbackHash: Uint8Array.from(differentHash),
      });

      const event: ProgramEvent = { type: "FeedbackRevoked", data: revokeData };
      await handleEvent(prisma, event, ctx);

      expect(prisma.revocation.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({ status: "PENDING" }),
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

    it("should preserve all-zero responseHash bytes for non-atomic responses", async () => {
      const zeroBytes = new Uint8Array(32).fill(0);
      (prisma.feedback.findUnique as any).mockResolvedValue({
        id: "fb-found",
        feedbackHash: Uint8Array.from(TEST_HASH),
      });

      const event: ProgramEvent = {
        type: "ResponseAppended",
        data: { ...responseData, responseHash: zeroBytes },
      };
      await handleEvent(prisma, event, ctx);

      expect(prisma.feedbackResponse.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({ responseHash: zeroBytes }),
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
            registryType: "BASE",
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

  // ==========================================================================
  // 26. Identity event coverage (wallet/collection/parent)
  // ==========================================================================
  describe("Identity event handling coverage", () => {
    it("WalletResetOnOwnerSync should update owner and reset wallet when DEFAULT_PUBKEY (atomic)", async () => {
      const defaultPubkey = new PublicKey(DEFAULT_PUBKEY_STR);
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = {
        type: "WalletResetOnOwnerSync",
        data: {
          asset: TEST_ASSET,
          oldWallet: TEST_WALLET,
          newWallet: defaultPubkey,
          ownerAfterSync: TEST_NEW_OWNER,
        },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.agent.updateMany).toHaveBeenCalledWith({
        where: { id: TEST_ASSET.toBase58() },
        data: {
          owner: TEST_NEW_OWNER.toBase58(),
          wallet: null,
          updatedAt: ctx.blockTime,
        },
      });
    });

    it("CollectionPointerSet should preserve existing creator and apply lock (atomic)", async () => {
      (prisma as any).$executeRawUnsafe = undefined;
      const immutableCreator = TEST_OWNER.toBase58();
      const setByDifferent = TEST_NEW_OWNER;
      (prisma.agent.findUnique as any).mockResolvedValue({
        collectionPointer: "c1:old-pointer",
        creator: immutableCreator,
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = {
        type: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET,
          setBy: setByDifferent,
          col: "c1:new-pointer",
          lock: true,
        },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.collection.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { col_creator: { col: "c1:new-pointer", creator: immutableCreator } },
        })
      );
      expect(prisma.agent.updateMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: TEST_ASSET.toBase58() },
          data: expect.objectContaining({
            collectionPointer: "c1:new-pointer",
            creator: immutableCreator,
            colLocked: true,
            updatedAt: ctx.blockTime,
          }),
        })
      );
    });

    it("CollectionPointerSet should not create a collection pointer when the agent is missing (atomic)", async () => {
      (prisma as any).$executeRawUnsafe = undefined;
      (prisma.agent.findUnique as any).mockResolvedValue(null);
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 0 });

      const event: ProgramEvent = {
        type: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET,
          setBy: TEST_NEW_OWNER,
          col: "c1:missing-agent",
          lock: true,
        },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.collection.upsert).not.toHaveBeenCalled();
      expect(prisma.collection.update).not.toHaveBeenCalled();
      expect(prisma.collection.updateMany).not.toHaveBeenCalled();
    });

    it("CollectionPointerSet should retry collection_id assignment on unique collision (atomic)", async () => {
      (prisma as any).$executeRawUnsafe = undefined;
      const immutableCreator = TEST_OWNER.toBase58();
      (prisma.agent.findUnique as any).mockResolvedValue({
        collectionPointer: "c1:old-pointer",
        creator: immutableCreator,
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);
      (prisma.collection.findMany as any).mockResolvedValue([{ collectionId: 41n }]);

      const uniqueError = Object.assign(
        new Error("Unique constraint failed on the fields: (`collection_id`)"),
        { code: "P2002", meta: { target: ["collection_id"] } }
      );
      (prisma.collection.upsert as any)
        .mockRejectedValueOnce(uniqueError)
        .mockResolvedValueOnce({});

      const event: ProgramEvent = {
        type: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET,
          setBy: TEST_NEW_OWNER,
          col: "c1:new-pointer",
          lock: true,
        },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.collection.upsert).toHaveBeenCalledTimes(2);
      expect(prisma.collection.findMany).toHaveBeenCalledTimes(2);
      expect(prisma.collection.upsert).toHaveBeenLastCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({ collectionId: 42n }),
          update: expect.objectContaining({ collectionId: 42n }),
        })
      );
    });

    it("CollectionPointerSet should use db-side allocation when raw SQL is available (atomic)", async () => {
      const immutableCreator = TEST_OWNER.toBase58();
      (prisma.agent.findUnique as any).mockResolvedValue({
        collectionPointer: "c1:old-pointer",
        creator: immutableCreator,
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const executeRawUnsafe = vi.fn().mockResolvedValue(1);
      (prisma as any).$executeRawUnsafe = executeRawUnsafe;

      const event: ProgramEvent = {
        type: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET,
          setBy: TEST_NEW_OWNER,
          col: "c1:new-pointer",
          lock: true,
        },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(executeRawUnsafe).toHaveBeenCalledTimes(1);
      const [sql, ...params] = (executeRawUnsafe as any).mock.calls[0] ?? [];
      expect(String(sql)).toContain("ON CONFLICT(\"col\", \"creator\") DO UPDATE");
      expect(String(sql)).toContain("\"lastSeenSlot\" = CASE");
      expect(String(sql)).toContain("\"lastSeenTxIndex\" = CASE");
      expect(String(sql)).toContain("\"lastSeenTxSignature\" = CASE");
      expect(params[0]).toBe("c1:new-pointer");
      expect(params[1]).toBe(immutableCreator);
      expect(prisma.collection.findMany).not.toHaveBeenCalled();
      expect(prisma.collection.upsert).not.toHaveBeenCalled();
    });

    it("CollectionPointerSet should fallback to max-scan assignment when db-side allocation fails (atomic)", async () => {
      const immutableCreator = TEST_OWNER.toBase58();
      (prisma.agent.findUnique as any).mockResolvedValue({
        collectionPointer: "c1:old-pointer",
        creator: immutableCreator,
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);
      (prisma.collection.findMany as any).mockResolvedValue([{ collectionId: 41n }]);
      (prisma.collection.upsert as any).mockResolvedValue({});

      const executeRawUnsafe = vi.fn().mockRejectedValueOnce(new Error("sqlite raw allocator failed"));
      (prisma as any).$executeRawUnsafe = executeRawUnsafe;

      const event: ProgramEvent = {
        type: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET,
          setBy: TEST_NEW_OWNER,
          col: "c1:new-pointer",
          lock: true,
        },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(executeRawUnsafe).toHaveBeenCalledTimes(1);
      expect(prisma.collection.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({ collectionId: 42n }),
          update: expect.objectContaining({ collectionId: 42n }),
        })
      );
    });

    it("CollectionPointerSet should not treat raw-SQL unique conflicts as missing schema", async () => {
      const immutableCreator = TEST_OWNER.toBase58();
      (prisma.agent.findUnique as any).mockResolvedValue({
        collectionPointer: "",
        creator: immutableCreator,
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      let maxCollectionId = 41n;
      (prisma.collection.findMany as any).mockImplementation(async () => [{ collectionId: maxCollectionId }]);
      (prisma.collection.upsert as any).mockImplementation(async (args: any) => {
        const assigned = args?.create?.collectionId;
        if (typeof assigned === "bigint" && assigned > maxCollectionId) {
          maxCollectionId = assigned;
        }
        return {};
      });

      const rawUniqueError = Object.assign(
        new Error("Raw query failed. Code: `2067`. Message: `UNIQUE constraint failed: CollectionPointer.collection_id`"),
        { code: "P2010" }
      );
      const executeRawUnsafe = vi.fn().mockRejectedValue(rawUniqueError);
      (prisma as any).$executeRawUnsafe = executeRawUnsafe;

      const eventA: ProgramEvent = {
        type: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET,
          setBy: TEST_NEW_OWNER,
          col: "c1:new-pointer-a",
          lock: true,
        },
      };
      const eventB: ProgramEvent = {
        type: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET,
          setBy: TEST_NEW_OWNER,
          col: "c1:new-pointer-b",
          lock: true,
        },
      };
      const ctxB: EventContext = {
        ...ctx,
        signature: `${TEST_SIGNATURE}-p2010`,
        slot: ctx.slot + 1n,
      };

      await handleEventAtomic(prisma, eventA, ctx);
      await handleEventAtomic(prisma, eventB, ctxB);

      expect(executeRawUnsafe).toHaveBeenCalledTimes(2);
      expect(prisma.collection.upsert).toHaveBeenCalled();
    });

    it("CollectionPointerSet should keep sequential collection_id under concurrent atomic calls in fallback mode", async () => {
      (prisma as any).$executeRawUnsafe = undefined;
      const immutableCreator = TEST_OWNER.toBase58();
      (prisma.agent.findUnique as any).mockResolvedValue({
        collectionPointer: "",
        creator: immutableCreator,
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      let maxCollectionId = 0n;
      (prisma.collection.findMany as any).mockImplementation(async () => [{ collectionId: maxCollectionId }]);
      (prisma.collection.upsert as any).mockImplementation(async (args: any) => {
        await new Promise((resolve) => setTimeout(resolve, 10));
        const assigned = args?.create?.collectionId;
        if (typeof assigned === "bigint" && assigned > maxCollectionId) {
          maxCollectionId = assigned;
        }
        return {};
      });

      const secondAsset = new PublicKey(new Uint8Array(32).fill(9));
      const eventA: ProgramEvent = {
        type: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET,
          setBy: TEST_NEW_OWNER,
          col: "c1:pointer-a",
          lock: true,
        },
      };
      const eventB: ProgramEvent = {
        type: "CollectionPointerSet",
        data: {
          asset: secondAsset,
          setBy: TEST_NEW_OWNER,
          col: "c1:pointer-b",
          lock: true,
        },
      };
      const ctxB: EventContext = {
        ...ctx,
        signature: `${TEST_SIGNATURE}-b`,
        slot: ctx.slot + 1n,
      };

      await Promise.all([
        handleEventAtomic(prisma, eventA, ctx),
        handleEventAtomic(prisma, eventB, ctxB),
      ]);

      const assignedIds = ((prisma.collection.upsert as any).mock.calls as Array<[any]>)
        .map((call) => call[0]?.create?.collectionId)
        .filter((value): value is bigint => typeof value === "bigint")
        .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));

      expect(assignedIds).toEqual([1n, 2n]);
    });

    it("CollectionPointerSet should not regress lastSeen fields for an older same-slot signature in fallback mode", async () => {
      (prisma as any).$executeRawUnsafe = undefined;
      const immutableCreator = TEST_OWNER.toBase58();
      (prisma.agent.findUnique as any).mockResolvedValue({
        collectionPointer: "",
        creator: immutableCreator,
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);
      (prisma.collection.findUnique as any).mockResolvedValue({
        collectionId: 41n,
        lastSeenSlot: ctx.slot,
        lastSeenTxIndex: 7,
        lastSeenTxSignature: "sig-z",
      });
      (prisma.collection.upsert as any).mockResolvedValue({});

      const event: ProgramEvent = {
        type: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET,
          setBy: TEST_NEW_OWNER,
          col: "c1:new-pointer",
          lock: true,
        },
      };
      const olderCtx: EventContext = {
        ...ctx,
        txIndex: 5,
        signature: "sig-a",
      };

      await handleEventAtomic(prisma, event, olderCtx);

      expect(prisma.collection.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          update: expect.not.objectContaining({
            lastSeenAt: olderCtx.blockTime,
            lastSeenSlot: olderCtx.slot,
            lastSeenTxIndex: olderCtx.txIndex,
            lastSeenTxSignature: olderCtx.signature,
          }),
        })
      );
    });

    it("CollectionPointerSet should advance lastSeen fields for a newer same-slot signature in fallback mode", async () => {
      (prisma as any).$executeRawUnsafe = undefined;
      const immutableCreator = TEST_OWNER.toBase58();
      (prisma.agent.findUnique as any).mockResolvedValue({
        collectionPointer: "",
        creator: immutableCreator,
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);
      (prisma.collection.findUnique as any).mockResolvedValue({
        collectionId: 41n,
        lastSeenSlot: ctx.slot,
        lastSeenTxIndex: 5,
        lastSeenTxSignature: "sig-a",
      });
      (prisma.collection.upsert as any).mockResolvedValue({});

      const event: ProgramEvent = {
        type: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET,
          setBy: TEST_NEW_OWNER,
          col: "c1:new-pointer",
          lock: true,
        },
      };
      const newerCtx: EventContext = {
        ...ctx,
        txIndex: 7,
        signature: "sig-z",
      };

      await handleEventAtomic(prisma, event, newerCtx);

      expect(prisma.collection.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          update: expect.objectContaining({
            lastSeenAt: newerCtx.blockTime,
            lastSeenSlot: newerCtx.slot,
            lastSeenTxIndex: newerCtx.txIndex,
            lastSeenTxSignature: newerCtx.signature,
          }),
        })
      );
    });

    it("CollectionPointerSet should advance lastSeen fields for a newer same-slot tx index even with a lexicographically older signature", async () => {
      (prisma as any).$executeRawUnsafe = undefined;
      const immutableCreator = TEST_OWNER.toBase58();
      (prisma.agent.findUnique as any).mockResolvedValue({
        collectionPointer: "",
        creator: immutableCreator,
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);
      (prisma.collection.findUnique as any).mockResolvedValue({
        collectionId: 41n,
        lastSeenSlot: ctx.slot,
        lastSeenTxIndex: 2,
        lastSeenTxSignature: "sig-z",
      });
      (prisma.collection.upsert as any).mockResolvedValue({});

      const event: ProgramEvent = {
        type: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET,
          setBy: TEST_NEW_OWNER,
          col: "c1:new-pointer",
          lock: true,
        },
      };
      const newerTxIndexCtx: EventContext = {
        ...ctx,
        txIndex: 9,
        signature: "sig-a",
      };

      await handleEventAtomic(prisma, event, newerTxIndexCtx);

      expect(prisma.collection.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          update: expect.objectContaining({
            lastSeenAt: newerTxIndexCtx.blockTime,
            lastSeenSlot: newerTxIndexCtx.slot,
            lastSeenTxIndex: newerTxIndexCtx.txIndex,
            lastSeenTxSignature: newerTxIndexCtx.signature,
          }),
        })
      );
    });

    it("CollectionPointerSet should decrement previous collection and increment new collection count", async () => {
      const immutableCreator = TEST_OWNER.toBase58();
      (prisma.agent.findUnique as any).mockResolvedValue({
        collectionPointer: "c1:old-pointer",
        creator: immutableCreator,
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.agent.count as any).mockResolvedValueOnce(4).mockResolvedValueOnce(9);
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = {
        type: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET,
          setBy: TEST_NEW_OWNER,
          col: "c1:new-pointer",
          lock: false,
        },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.collection.updateMany).toHaveBeenCalledWith({
        where: {
          col: "c1:old-pointer",
          creator: immutableCreator,
        },
        data: {
          assetCount: 4n,
        },
      });

      expect(prisma.collection.update).toHaveBeenCalledWith({
        where: { col_creator: { col: "c1:new-pointer", creator: immutableCreator } },
        data: {
          assetCount: 9n,
        },
      });
    });

    it("CollectionPointerSet should still refresh current collection count when pointer stays identical", async () => {
      const immutableCreator = TEST_OWNER.toBase58();
      (prisma.agent.findUnique as any).mockResolvedValue({
        collectionPointer: "c1:same-pointer",
        creator: immutableCreator,
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.agent.count as any).mockResolvedValueOnce(7);
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = {
        type: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET,
          setBy: TEST_NEW_OWNER,
          col: "c1:same-pointer",
          lock: true,
        },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.collection.updateMany).not.toHaveBeenCalled();
      expect(prisma.collection.update).toHaveBeenCalledWith({
        where: { col_creator: { col: "c1:same-pointer", creator: immutableCreator } },
        data: { assetCount: 7n },
      });
    });

    it("CollectionPointerSet should not mutate lock flag when lock is omitted (non-atomic)", async () => {
      (prisma.agent.findUnique as any).mockResolvedValue({
        collectionPointer: "c1:prev",
        creator: TEST_OWNER.toBase58(),
      });
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });

      const event: ProgramEvent = {
        type: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET,
          setBy: TEST_NEW_OWNER,
          col: "c1:next",
        },
      };

      await handleEvent(prisma, event, ctx);

      const updateCall = (prisma.agent.updateMany as any).mock.calls.at(-1)?.[0];
      expect(updateCall?.data.collectionPointer).toBe("c1:next");
      expect(updateCall?.data.creator).toBe(TEST_OWNER.toBase58());
      expect("colLocked" in updateCall?.data).toBe(false);
    });

    it("CollectionPointerSet should not create a collection pointer when the agent is missing (non-atomic)", async () => {
      (prisma.agent.findUnique as any).mockResolvedValue(null);
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 0 });

      const event: ProgramEvent = {
        type: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET,
          setBy: TEST_NEW_OWNER,
          col: "c1:missing-agent",
          lock: true,
        },
      };

      await handleEvent(prisma, event, ctx);

      expect(prisma.collection.upsert).not.toHaveBeenCalled();
      expect(prisma.collection.update).not.toHaveBeenCalled();
      expect(prisma.collection.updateMany).not.toHaveBeenCalled();
    });

    it("ParentAssetSet should apply parent lock when provided (atomic)", async () => {
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });
      (prisma.indexerState.findUnique as any).mockResolvedValue(null);

      const event: ProgramEvent = {
        type: "ParentAssetSet",
        data: {
          asset: TEST_ASSET,
          parentAsset: TEST_COLLECTION,
          parentCreator: TEST_OWNER,
          setBy: TEST_NEW_OWNER,
          lock: true,
        },
      };

      await handleEventAtomic(prisma, event, ctx);

      expect(prisma.agent.updateMany).toHaveBeenCalledWith({
        where: { id: TEST_ASSET.toBase58() },
        data: {
          parentAsset: TEST_COLLECTION.toBase58(),
          parentCreator: TEST_OWNER.toBase58(),
          parentLocked: true,
          updatedAt: ctx.blockTime,
        },
      });
    });

    it("ParentAssetSet should leave parentLocked unchanged when lock omitted (non-atomic)", async () => {
      (prisma.agent.updateMany as any).mockResolvedValue({ count: 1 });

      const event: ProgramEvent = {
        type: "ParentAssetSet",
        data: {
          asset: TEST_ASSET,
          parentAsset: TEST_COLLECTION,
          parentCreator: TEST_OWNER,
          setBy: TEST_NEW_OWNER,
        },
      };

      await handleEvent(prisma, event, ctx);

      const updateCall = (prisma.agent.updateMany as any).mock.calls.at(-1)?.[0];
      expect(updateCall?.data.parentAsset).toBe(TEST_COLLECTION.toBase58());
      expect(updateCall?.data.parentCreator).toBe(TEST_OWNER.toBase58());
      expect("parentLocked" in updateCall?.data).toBe(false);
    });
  });
});
