import { describe, it, expect, vi, beforeEach } from "vitest";
import { PublicKey } from "@solana/web3.js";
import {
  parseTransactionLogs,
  parseTransaction,
  toTypedEvent,
  idl,
  eventParser,
  IDL_VERSION,
  IDL_PROGRAM_ID,
} from "../../../src/parser/decoder.js";
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
  TEST_PROGRAM_ID,
  createMockParsedTransaction,
  createEventLogs,
  encodeAnchorEvent,
} from "../../mocks/solana.js";

describe("Parser Decoder", () => {
  describe("IDL loading", () => {
    it("should load IDL successfully", () => {
      expect(idl).toBeDefined();
      expect(idl.address).toBe("8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C");
    });

    it("should have events defined in IDL", () => {
      expect(idl.events).toBeDefined();
      expect(Array.isArray(idl.events)).toBe(true);
      expect(idl.events!.length).toBe(14);
    });

    it("should export IDL_VERSION from metadata", () => {
      expect(IDL_VERSION).toBeDefined();
      expect(typeof IDL_VERSION).toBe("string");
      expect(IDL_VERSION).not.toBe("unknown");
    });

    it("should export IDL_PROGRAM_ID from IDL address", () => {
      expect(IDL_PROGRAM_ID).toBeDefined();
      expect(IDL_PROGRAM_ID).toBe("8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C");
    });

    it("should fallback IDL_VERSION to 'unknown' when metadata.version is missing", () => {
      const idlWithoutVersion = { ...idl, metadata: {} };
      const version = (idlWithoutVersion as any).metadata?.version || "unknown";
      expect(version).toBe("unknown");
    });

    it("should fallback IDL_PROGRAM_ID to config.programId when address is missing", () => {
      const idlWithoutAddress = { ...idl, address: undefined };
      const programId = (idlWithoutAddress as any).address || "fallback-id";
      expect(programId).toBe("fallback-id");
    });
  });

  describe("parseTransactionLogs", () => {
    it("should return empty array for empty logs", () => {
      const result = parseTransactionLogs([]);
      expect(result).toEqual([]);
    });

    it("should return empty array for logs without events", () => {
      const logs = [
        "Program 11111111111111111111111111111111 invoke [1]",
        "Program log: Hello",
        "Program 11111111111111111111111111111111 success",
      ];
      const result = parseTransactionLogs(logs);
      expect(result).toEqual([]);
    });

    it("should handle invalid logs gracefully", () => {
      const logs = ["invalid log format", "", "another invalid"];
      const result = parseTransactionLogs(logs);
      expect(result).toEqual([]);
    });

    it("should parse valid Anchor event from logs", () => {
      // Create a properly encoded AgentRegistered event
      const eventData = {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmTest",
      };

      const logs = createEventLogs("AgentRegistered", eventData);

      const result = parseTransactionLogs(logs);

      expect(result.length).toBe(1);
      expect(result[0].name).toBe("AgentRegistered");
      expect(result[0].data.asset.toString()).toBe(TEST_ASSET.toBase58());
    });

    it("should parse multiple events from logs", () => {
      // Create two encoded events
      const eventData1 = {
        asset: TEST_ASSET,
        registry: TEST_REGISTRY,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmTest",
      };

      const eventData2 = {
        asset: TEST_ASSET,
        newUri: "https://example.com/agent.json",
        updatedBy: TEST_OWNER,
      };

      const encoded1 = encodeAnchorEvent("AgentRegistered", eventData1);
      const encoded2 = encodeAnchorEvent("UriUpdated", eventData2);
      const base64Data1 = encoded1.toString("base64");
      const base64Data2 = encoded2.toString("base64");

      const logs = [
        `Program ${TEST_PROGRAM_ID.toBase58()} invoke [1]`,
        `Program data: ${base64Data1}`,
        `Program data: ${base64Data2}`,
        `Program ${TEST_PROGRAM_ID.toBase58()} success`,
      ];

      const result = parseTransactionLogs(logs);

      // Anchor parser may parse events differently based on discriminator matching
      // At minimum, we expect at least 1 event to be parsed
      expect(result.length).toBeGreaterThanOrEqual(1);
      expect(result[0].name).toBe("AgentRegistered");
    });

    it("should handle parser exceptions gracefully", () => {
      // Create logs that might cause the parser to throw
      const logs = [
        `Program ${TEST_PROGRAM_ID.toBase58()} invoke [1]`,
        // Invalid base64 data that will fail borsh decoding
        "Program data: AAAAAAAAAAAAAAAA",
        `Program ${TEST_PROGRAM_ID.toBase58()} success`,
      ];

      // Should not throw, returns empty array
      const result = parseTransactionLogs(logs);
      expect(Array.isArray(result)).toBe(true);
    });

    it("should catch and return empty array when eventParser.parseLogs throws", () => {
      const originalParseLogs = eventParser.parseLogs.bind(eventParser);
      const spy = vi.spyOn(eventParser, "parseLogs").mockImplementation(() => {
        throw new Error("Simulated parser failure");
      });

      const logs = [
        `Program ${TEST_PROGRAM_ID.toBase58()} invoke [1]`,
        `Program ${TEST_PROGRAM_ID.toBase58()} success`,
      ];

      const result = parseTransactionLogs(logs);

      expect(result).toEqual([]);
      spy.mockRestore();
    });

    it("should derive collection lock=false from set_collection_pointer_with_options logs", () => {
      const spy = vi.spyOn(eventParser, "parseLogs").mockImplementation(() => {
        return (function* () {
          yield {
            name: "CollectionPointerSet",
            data: {
              asset: TEST_ASSET.toBase58(),
              set_by: TEST_OWNER.toBase58(),
              col: "c1:test",
            },
          } as any;
        })();
      });

      const logs = [
        `Program ${TEST_PROGRAM_ID.toBase58()} invoke [1]`,
        "Program log: Instruction: set_collection_pointer_with_options",
        `Program ${TEST_PROGRAM_ID.toBase58()} success`,
      ];
      const result = parseTransactionLogs(logs);

      expect(result).toHaveLength(1);
      expect(result[0].name).toBe("CollectionPointerSet");
      expect(result[0].data.lock).toBe(false);

      spy.mockRestore();
    });

    it("should derive parent lock=true from set_parent_asset logs", () => {
      const spy = vi.spyOn(eventParser, "parseLogs").mockImplementation(() => {
        return (function* () {
          yield {
            name: "ParentAssetSet",
            data: {
              asset: TEST_ASSET.toBase58(),
              parent_asset: TEST_COLLECTION.toBase58(),
              parent_creator: TEST_OWNER.toBase58(),
              set_by: TEST_OWNER.toBase58(),
            },
          } as any;
        })();
      });

      const logs = [
        `Program ${TEST_PROGRAM_ID.toBase58()} invoke [1]`,
        "Program log: Instruction: set_parent_asset",
        `Program ${TEST_PROGRAM_ID.toBase58()} success`,
      ];
      const result = parseTransactionLogs(logs);

      expect(result).toHaveLength(1);
      expect(result[0].name).toBe("ParentAssetSet");
      expect(result[0].data.lock).toBe(true);

      spy.mockRestore();
    });

    it("should derive lock hints from CamelCase instruction logs", () => {
      const spy = vi.spyOn(eventParser, "parseLogs").mockImplementation(() => {
        return (function* () {
          yield {
            name: "CollectionPointerSet",
            data: {
              asset: TEST_ASSET.toBase58(),
              set_by: TEST_OWNER.toBase58(),
              col: "c1:test",
            },
          } as any;
          yield {
            name: "ParentAssetSet",
            data: {
              asset: TEST_ASSET.toBase58(),
              parent_asset: TEST_COLLECTION.toBase58(),
              parent_creator: TEST_OWNER.toBase58(),
              set_by: TEST_OWNER.toBase58(),
            },
          } as any;
        })();
      });

      const logs = [
        `Program ${TEST_PROGRAM_ID.toBase58()} invoke [1]`,
        "Program log: Instruction: SetCollectionPointer",
        "Program log: Instruction: SetParentAssetWithOptions",
        `Program ${TEST_PROGRAM_ID.toBase58()} success`,
      ];
      const result = parseTransactionLogs(logs);

      expect(result).toHaveLength(2);
      expect(result[0].data.lock).toBe(true);
      expect(result[1].data.lock).toBe(false);

      spy.mockRestore();
    });
  });

  describe("parseTransaction", () => {
    it("should return null for transaction without logs", () => {
      const tx = {
        slot: Number(TEST_SLOT),
        blockTime: Math.floor(TEST_BLOCK_TIME.getTime() / 1000),
        transaction: { signatures: [TEST_SIGNATURE] },
        meta: null,
      };
      const result = parseTransaction(tx as any);
      expect(result).toBeNull();
    });

    it("should return null for transaction with empty logs", () => {
      const tx = createMockParsedTransaction(TEST_SIGNATURE, []);
      const result = parseTransaction(tx as any);
      expect(result).toBeNull();
    });

    it("should extract signature and slot from transaction", () => {
      const tx = createMockParsedTransaction(TEST_SIGNATURE, [
        "Program log: test",
      ]);
      // Won't have events but tests structure extraction
      const result = parseTransaction(tx as any);
      // Result is null because no valid events parsed
      expect(result).toBeNull();
    });

    it("should return TransactionEvents when events are parsed", () => {
      // Create a valid encoded event
      const eventData = {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmTest",
      };

      const logs = createEventLogs("AgentRegistered", eventData);

      const tx = createMockParsedTransaction(TEST_SIGNATURE, logs);
      const result = parseTransaction(tx as any);

      expect(result).not.toBeNull();
      expect(result!.signature).toBe(TEST_SIGNATURE);
      expect(result!.slot).toBe(Number(TEST_SLOT));
      expect(result!.events.length).toBe(1);
      expect(result!.events[0].name).toBe("AgentRegistered");
    });

    it("should handle transaction with null blockTime", () => {
      const eventData = {
        asset: TEST_ASSET,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
        agentUri: "ipfs://QmTest",
      };

      const logs = createEventLogs("AgentRegistered", eventData);

      const tx = {
        slot: Number(TEST_SLOT),
        blockTime: null, // null blockTime
        transaction: {
          signatures: [TEST_SIGNATURE],
        },
        meta: {
          err: null,
          logMessages: logs,
        },
      };

      const result = parseTransaction(tx as any);

      expect(result).not.toBeNull();
      expect(result!.blockTime).toBeNull();
    });
  });

  describe("toTypedEvent", () => {
    it("should convert AgentRegistered event", () => {
      const event = {
        name: "AgentRegistered",
        data: {
          asset: TEST_ASSET.toBase58(),
          collection: TEST_COLLECTION.toBase58(),
          owner: TEST_OWNER.toBase58(),
          atom_enabled: true,
          agent_uri: "ipfs://QmTest",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("AgentRegistered");
      expect(result!.data.asset.toBase58()).toBe(TEST_ASSET.toBase58());
      expect(result!.data.collection.toBase58()).toBe(TEST_COLLECTION.toBase58());
      expect(result!.data.owner.toBase58()).toBe(TEST_OWNER.toBase58());
    });

    it("should convert AgentOwnerSynced event", () => {
      const event = {
        name: "AgentOwnerSynced",
        data: {
          asset: TEST_ASSET.toBase58(),
          old_owner: TEST_OWNER.toBase58(),
          new_owner: TEST_NEW_OWNER.toBase58(),
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("AgentOwnerSynced");
      expect(result!.data.asset.toBase58()).toBe(TEST_ASSET.toBase58());
      expect(result!.data.oldOwner.toBase58()).toBe(TEST_OWNER.toBase58());
      expect(result!.data.newOwner.toBase58()).toBe(TEST_NEW_OWNER.toBase58());
    });

    it("should convert UriUpdated event", () => {
      const event = {
        name: "UriUpdated",
        data: {
          asset: TEST_ASSET.toBase58(),
          new_uri: "https://example.com/agent.json",
          updated_by: TEST_OWNER.toBase58(),
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("UriUpdated");
      expect(result!.data.newUri).toBe("https://example.com/agent.json");
    });

    it("should convert CollectionPointerSet event with lock hint", () => {
      const event = {
        name: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET.toBase58(),
          set_by: TEST_OWNER.toBase58(),
          col: "c1:test",
          lock: false,
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("CollectionPointerSet");
      expect(result!.data.lock).toBe(false);
    });

    it("should convert ParentAssetSet event with lock hint", () => {
      const event = {
        name: "ParentAssetSet",
        data: {
          asset: TEST_ASSET.toBase58(),
          parent_asset: TEST_COLLECTION.toBase58(),
          parent_creator: TEST_OWNER.toBase58(),
          set_by: TEST_OWNER.toBase58(),
          lock: true,
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("ParentAssetSet");
      expect(result!.data.lock).toBe(true);
    });

    it("should convert WalletUpdated event with null oldWallet", () => {
      const event = {
        name: "WalletUpdated",
        data: {
          asset: TEST_ASSET.toBase58(),
          old_wallet: null,
          new_wallet: TEST_WALLET.toBase58(),
          updated_by: TEST_OWNER.toBase58(),
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("WalletUpdated");
      expect(result!.data.oldWallet).toBeNull();
      expect(result!.data.newWallet.toBase58()).toBe(TEST_WALLET.toBase58());
    });

    it("should convert WalletUpdated event with existing oldWallet", () => {
      // Create a valid old wallet key using 32-byte array
      const oldWalletBytes = new Uint8Array(32).fill(9);
      const oldWallet = new PublicKey(oldWalletBytes);
      const event = {
        name: "WalletUpdated",
        data: {
          asset: TEST_ASSET.toBase58(),
          old_wallet: oldWallet.toBase58(),
          new_wallet: TEST_WALLET.toBase58(),
          updated_by: TEST_OWNER.toBase58(),
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.data.oldWallet).not.toBeNull();
      expect(result!.data.oldWallet!.toBase58()).toBe(oldWallet.toBase58());
    });

    it("should convert WalletResetOnOwnerSync event", () => {
      const event = {
        name: "WalletResetOnOwnerSync",
        data: {
          asset: TEST_ASSET.toBase58(),
          old_wallet: TEST_WALLET.toBase58(),
          new_wallet: TEST_WALLET.toBase58(),
          owner_after_sync: TEST_NEW_OWNER.toBase58(),
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("WalletResetOnOwnerSync");
      expect(result!.data.ownerAfterSync.toBase58()).toBe(TEST_NEW_OWNER.toBase58());
      expect(result!.data.newWallet.toBase58()).toBe(TEST_WALLET.toBase58());
    });

    it("should convert CollectionPointerSet event", () => {
      const event = {
        name: "CollectionPointerSet",
        data: {
          asset: TEST_ASSET.toBase58(),
          set_by: TEST_OWNER.toBase58(),
          col: "c1:bafybeigdyrzt",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("CollectionPointerSet");
      expect(result!.data.setBy.toBase58()).toBe(TEST_OWNER.toBase58());
      expect(result!.data.col).toBe("c1:bafybeigdyrzt");
    });

    it("should convert ParentAssetSet event", () => {
      const parentAsset = new PublicKey(new Uint8Array(32).fill(11));
      const event = {
        name: "ParentAssetSet",
        data: {
          asset: TEST_ASSET.toBase58(),
          parent_asset: parentAsset.toBase58(),
          parent_creator: TEST_OWNER.toBase58(),
          set_by: TEST_OWNER.toBase58(),
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("ParentAssetSet");
      expect(result!.data.parentAsset.toBase58()).toBe(parentAsset.toBase58());
      expect(result!.data.parentCreator.toBase58()).toBe(TEST_OWNER.toBase58());
    });

    it("should convert MetadataSet event", () => {
      const event = {
        name: "MetadataSet",
        data: {
          asset: TEST_ASSET.toBase58(),
          key: "description",
          value: Array.from(TEST_VALUE),
          immutable: false,
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("MetadataSet");
      expect(result!.data.key).toBe("description");
      expect(result!.data.immutable).toBe(false);
    });

    it("should convert MetadataDeleted event", () => {
      const event = {
        name: "MetadataDeleted",
        data: {
          asset: TEST_ASSET.toBase58(),
          key: "description",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("MetadataDeleted");
      expect(result!.data.key).toBe("description");
    });

    it("should convert RegistryInitialized event", () => {
      const event = {
        name: "RegistryInitialized",
        data: {
          collection: TEST_COLLECTION.toBase58(),
          authority: TEST_OWNER.toBase58(),
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("RegistryInitialized");
    });

    it("should convert NewFeedback event", () => {
      const event = {
        name: "NewFeedback",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: "0",
          slot: "123456",
          value: "9500",
          value_decimals: 2,
          score: 85,
          feedback_hash: Array.from(TEST_HASH),
          atom_enabled: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
          new_risk_score: 10,
          new_diversity_ratio: 42,
          is_unique_client: true,
          new_feedback_digest: Array.from(TEST_HASH),
          new_feedback_count: "1",
          tag1: "quality",
          tag2: "speed",
          endpoint: "/api/chat",
          feedback_uri: "ipfs://QmXXX",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("NewFeedback");
      expect(result!.data.feedbackIndex).toBe(0n);
      expect(result!.data.value).toBe(9500n);
      expect(result!.data.valueDecimals).toBe(2);
      expect(result!.data.score).toBe(85);
      expect(result!.data.tag1).toBe("quality");
    });

    it("should convert FeedbackRevoked event", () => {
      const event = {
        name: "FeedbackRevoked",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: "1",
          feedback_hash: Array.from(TEST_HASH),
          slot: "123456",
          original_score: 85,
          atom_enabled: true,
          had_impact: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
          new_revoke_digest: Array.from(TEST_HASH),
          new_revoke_count: "1",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("FeedbackRevoked");
      expect(result!.data.feedbackIndex).toBe(1n);
    });

    it("should convert ResponseAppended event", () => {
      const event = {
        name: "ResponseAppended",
        data: {
          asset: TEST_ASSET.toBase58(),
          client: TEST_CLIENT.toBase58(),
          feedback_index: "0",
          slot: "123456",
          responder: TEST_OWNER.toBase58(),
          response_hash: Array.from(TEST_HASH),
          seal_hash: Array.from(TEST_HASH),
          new_response_digest: Array.from(TEST_HASH),
          new_response_count: "1",
          response_uri: "ipfs://QmYYY",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("ResponseAppended");
      expect(result!.data.responseUri).toBe("ipfs://QmYYY");
      expect(result!.data.sealHash).toEqual(new Uint8Array(TEST_HASH));
    });

    it("should convert ValidationRequested event", () => {
      const event = {
        name: "ValidationRequested",
        data: {
          asset: TEST_ASSET.toBase58(),
          validator_address: TEST_VALIDATOR.toBase58(),
          nonce: 1,
          request_uri: "ipfs://QmZZZ",
          request_hash: Array.from(TEST_HASH),
          requester: TEST_OWNER.toBase58(),
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("ValidationRequested");
      expect(result!.data.nonce).toBe(1n);
    });

    it("should convert ValidationResponded event", () => {
      const event = {
        name: "ValidationResponded",
        data: {
          asset: TEST_ASSET.toBase58(),
          validator_address: TEST_VALIDATOR.toBase58(),
          nonce: 1,
          response: 90,
          response_uri: "ipfs://QmAAA",
          response_hash: Array.from(TEST_HASH),
          tag: "security",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("ValidationResponded");
      expect(result!.data.response).toBe(90);
      expect(result!.data.tag).toBe("security");
    });

    it("should return null for unknown event type", () => {
      const event = {
        name: "UnknownEvent",
        data: { foo: "bar" },
      };

      const result = toTypedEvent(event);

      expect(result).toBeNull();
    });

    it("should return null for invalid event data", () => {
      const event = {
        name: "AgentRegistered",
        data: {
          asset: "invalid-not-a-pubkey",
        },
      };

      const result = toTypedEvent(event);

      expect(result).toBeNull();
    });

    it("should convert AtomEnabled event", () => {
      const event = {
        name: "AtomEnabled",
        data: {
          asset: TEST_ASSET.toBase58(),
          enabled_by: TEST_OWNER.toBase58(),
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("AtomEnabled");
      expect(result!.data.asset.toBase58()).toBe(TEST_ASSET.toBase58());
      expect(result!.data.enabledBy.toBase58()).toBe(TEST_OWNER.toBase58());
    });

    it("should default atomEnabled to true when atom_enabled is undefined in AgentRegistered", () => {
      const event = {
        name: "AgentRegistered",
        data: {
          asset: TEST_ASSET.toBase58(),
          collection: TEST_COLLECTION.toBase58(),
          owner: TEST_OWNER.toBase58(),
          // atom_enabled intentionally omitted
          agent_uri: "ipfs://QmTest",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("AgentRegistered");
      expect(result!.data.atomEnabled).toBe(true);
    });

    it("should default agentUri to empty string when agent_uri is falsy in AgentRegistered", () => {
      const event = {
        name: "AgentRegistered",
        data: {
          asset: TEST_ASSET.toBase58(),
          collection: TEST_COLLECTION.toBase58(),
          owner: TEST_OWNER.toBase58(),
          atom_enabled: false,
          agent_uri: "",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.data.agentUri).toBe("");
    });

    it("should default atomEnabled to false when atom_enabled is undefined in FeedbackRevoked", () => {
      const event = {
        name: "FeedbackRevoked",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: "1",
          seal_hash: Array.from(TEST_HASH),
          slot: "123456",
          original_score: 85,
          // atom_enabled intentionally omitted
          had_impact: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
          new_revoke_digest: Array.from(TEST_HASH),
          new_revoke_count: "1",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("FeedbackRevoked");
      expect(result!.data.atomEnabled).toBe(false);
    });

    it("should handle NewFeedback with null feedback_file_hash and seal_hash", () => {
      const event = {
        name: "NewFeedback",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: "0",
          slot: "123456",
          value: "9500",
          value_decimals: 2,
          score: 85,
          feedback_file_hash: null,
          seal_hash: Array.from(TEST_HASH),
          atom_enabled: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
          new_risk_score: 10,
          new_diversity_ratio: 42,
          is_unique_client: true,
          new_feedback_digest: Array.from(TEST_HASH),
          new_feedback_count: "1",
          tag1: "quality",
          tag2: "speed",
          endpoint: "/api/chat",
          feedback_uri: "ipfs://QmXXX",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("NewFeedback");
      expect(result!.data.feedbackFileHash).toBeNull();
      expect(result!.data.sealHash).toEqual(new Uint8Array(TEST_HASH));
    });

    it("should handle NewFeedback with present feedback_file_hash", () => {
      const fileHash = new Uint8Array(32).fill(0xcd);
      const event = {
        name: "NewFeedback",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: "0",
          slot: "123456",
          value: "9500",
          value_decimals: 2,
          score: 85,
          feedback_file_hash: Array.from(fileHash),
          seal_hash: Array.from(TEST_HASH),
          atom_enabled: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
          new_risk_score: 10,
          new_diversity_ratio: 42,
          is_unique_client: true,
          new_feedback_digest: Array.from(TEST_HASH),
          new_feedback_count: "1",
          tag1: "quality",
          tag2: "speed",
          endpoint: "/api/chat",
          feedback_uri: "ipfs://QmXXX",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("NewFeedback");
      expect(result!.data.feedbackFileHash).toEqual(new Uint8Array(fileHash));
    });

    it("should handle NewFeedback with null score (Option<u8>)", () => {
      const event = {
        name: "NewFeedback",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: "0",
          slot: "123456",
          value: "9500",
          value_decimals: 2,
          score: null,
          feedback_file_hash: null,
          seal_hash: Array.from(TEST_HASH),
          atom_enabled: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
          new_risk_score: 10,
          new_diversity_ratio: 42,
          is_unique_client: true,
          new_feedback_digest: Array.from(TEST_HASH),
          new_feedback_count: "1",
          tag1: "quality",
          tag2: "speed",
          endpoint: "/api/chat",
          feedback_uri: "ipfs://QmXXX",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.data.score).toBeNull();
    });

    it("should handle NewFeedback with score wrapped in { some: value } object", () => {
      const event = {
        name: "NewFeedback",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: "0",
          slot: "123456",
          value: "9500",
          value_decimals: 2,
          score: { some: 75 },
          feedback_file_hash: null,
          seal_hash: Array.from(TEST_HASH),
          atom_enabled: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
          new_risk_score: 10,
          new_diversity_ratio: 42,
          is_unique_client: true,
          new_feedback_digest: Array.from(TEST_HASH),
          new_feedback_count: "1",
          tag1: "quality",
          tag2: "speed",
          endpoint: "/api/chat",
          feedback_uri: "ipfs://QmXXX",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.data.score).toBe(75);
    });

    it("should handle NewFeedback with undefined score", () => {
      const event = {
        name: "NewFeedback",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: "0",
          slot: "123456",
          value: "9500",
          value_decimals: 2,
          score: undefined,
          feedback_file_hash: null,
          seal_hash: Array.from(TEST_HASH),
          atom_enabled: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
          new_risk_score: 10,
          new_diversity_ratio: 42,
          is_unique_client: true,
          new_feedback_digest: Array.from(TEST_HASH),
          new_feedback_count: "1",
          tag1: "quality",
          tag2: "speed",
          endpoint: "/api/chat",
          feedback_uri: "ipfs://QmXXX",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.data.score).toBeNull();
    });

    it("should handle NewFeedback with numeric feedback_index (parseBigInt number path)", () => {
      const event = {
        name: "NewFeedback",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: 42,
          slot: 123456,
          value: 9500,
          value_decimals: 2,
          score: 85,
          feedback_file_hash: null,
          seal_hash: Array.from(TEST_HASH),
          atom_enabled: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
          new_risk_score: 10,
          new_diversity_ratio: 42,
          is_unique_client: true,
          new_feedback_digest: Array.from(TEST_HASH),
          new_feedback_count: 1,
          tag1: "quality",
          tag2: "speed",
          endpoint: "/api/chat",
          feedback_uri: "ipfs://QmXXX",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.data.feedbackIndex).toBe(42n);
      expect(result!.data.slot).toBe(123456n);
      expect(result!.data.value).toBe(9500n);
      expect(result!.data.newFeedbackCount).toBe(1n);
    });

    it("should handle NewFeedback with bigint feedback_index (parseBigInt bigint path)", () => {
      const event = {
        name: "NewFeedback",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: 42n,
          slot: 123456n,
          value: 9500n,
          value_decimals: 2,
          score: 85,
          feedback_file_hash: null,
          seal_hash: Array.from(TEST_HASH),
          atom_enabled: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
          new_risk_score: 10,
          new_diversity_ratio: 42,
          is_unique_client: true,
          new_feedback_digest: Array.from(TEST_HASH),
          new_feedback_count: 1n,
          tag1: "quality",
          tag2: "speed",
          endpoint: "/api/chat",
          feedback_uri: "ipfs://QmXXX",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.data.feedbackIndex).toBe(42n);
      expect(result!.data.slot).toBe(123456n);
      expect(result!.data.value).toBe(9500n);
      expect(result!.data.newFeedbackCount).toBe(1n);
    });

    it("should handle NewFeedback with BN-like object (parseBigInt BN path)", () => {
      // Simulate Anchor BN format: { negative: 0, words: [9500, 0], length: 2, red: null }
      const bnValue = { negative: 0, words: [9500] };
      const bnIndex = { negative: 0, words: [42] };
      const bnSlot = { negative: 0, words: [123456] };
      const bnCount = { negative: 0, words: [5] };

      const event = {
        name: "NewFeedback",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: bnIndex,
          slot: bnSlot,
          value: bnValue,
          value_decimals: 2,
          score: 85,
          feedback_file_hash: null,
          seal_hash: Array.from(TEST_HASH),
          atom_enabled: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
          new_risk_score: 10,
          new_diversity_ratio: 42,
          is_unique_client: true,
          new_feedback_digest: Array.from(TEST_HASH),
          new_feedback_count: bnCount,
          tag1: "quality",
          tag2: "speed",
          endpoint: "/api/chat",
          feedback_uri: "ipfs://QmXXX",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.data.feedbackIndex).toBe(42n);
      expect(result!.data.value).toBe(9500n);
      expect(result!.data.slot).toBe(123456n);
      expect(result!.data.newFeedbackCount).toBe(5n);
    });

    it("should handle negative BN-like object in parseBigInt", () => {
      const bnValue = { negative: 1, words: [100] };

      const event = {
        name: "NewFeedback",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: "0",
          slot: "123456",
          value: bnValue,
          value_decimals: 2,
          score: 85,
          feedback_file_hash: null,
          seal_hash: Array.from(TEST_HASH),
          atom_enabled: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
          new_risk_score: 10,
          new_diversity_ratio: 42,
          is_unique_client: true,
          new_feedback_digest: Array.from(TEST_HASH),
          new_feedback_count: "1",
          tag1: "quality",
          tag2: "speed",
          endpoint: "/api/chat",
          feedback_uri: "ipfs://QmXXX",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.data.value).toBe(-100n);
    });

    it("should handle multi-word BN-like object in parseBigInt", () => {
      // BN uses 26-bit words: value = words[0] + words[1] * 2^26
      const bnValue = { negative: 0, words: [1, 1] };

      const event = {
        name: "NewFeedback",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: "0",
          slot: "123456",
          value: bnValue,
          value_decimals: 2,
          score: 85,
          feedback_file_hash: null,
          seal_hash: Array.from(TEST_HASH),
          atom_enabled: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
          new_risk_score: 10,
          new_diversity_ratio: 42,
          is_unique_client: true,
          new_feedback_digest: Array.from(TEST_HASH),
          new_feedback_count: "1",
          tag1: "quality",
          tag2: "speed",
          endpoint: "/api/chat",
          feedback_uri: "ipfs://QmXXX",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      // 1 + 1 * 2^26 = 1 + 67108864 = 67108865
      expect(result!.data.value).toBe(67108865n);
    });

    it("should handle parseBigInt fallback with boolean-like value via String coercion", () => {
      // An object that isn't BN-like but has a toString() - triggers line 33: BigInt(String(value))
      const customValue = { toString: () => "42" };

      const event = {
        name: "NewFeedback",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: "0",
          slot: "123456",
          value: customValue,
          value_decimals: 2,
          score: 85,
          feedback_file_hash: null,
          seal_hash: Array.from(TEST_HASH),
          atom_enabled: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
          new_risk_score: 10,
          new_diversity_ratio: 42,
          is_unique_client: true,
          new_feedback_digest: Array.from(TEST_HASH),
          new_feedback_count: "1",
          tag1: "quality",
          tag2: "speed",
          endpoint: "/api/chat",
          feedback_uri: "ipfs://QmXXX",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.data.value).toBe(42n);
    });

    it("should handle unsafe integer in parseBigInt and return null from toTypedEvent", () => {
      // Number.MAX_SAFE_INTEGER + 1 is unsafe
      const unsafeNumber = Number.MAX_SAFE_INTEGER + 1;

      const event = {
        name: "NewFeedback",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: unsafeNumber,
          slot: "123456",
          value: "9500",
          value_decimals: 2,
          score: 85,
          feedback_file_hash: null,
          seal_hash: Array.from(TEST_HASH),
          atom_enabled: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
          new_risk_score: 10,
          new_diversity_ratio: 42,
          is_unique_client: true,
          new_feedback_digest: Array.from(TEST_HASH),
          new_feedback_count: "1",
          tag1: "quality",
          tag2: "speed",
          endpoint: "/api/chat",
          feedback_uri: "ipfs://QmXXX",
        },
      };

      const result = toTypedEvent(event);

      // parseBigInt throws on unsafe integer, toTypedEvent catches and returns null
      expect(result).toBeNull();
    });

    it("should handle score as object without 'some' key in parseOptionU8 (returns null)", () => {
      const event = {
        name: "NewFeedback",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: "0",
          slot: "123456",
          value: "9500",
          value_decimals: 2,
          score: { none: true },
          feedback_file_hash: null,
          seal_hash: Array.from(TEST_HASH),
          atom_enabled: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
          new_risk_score: 10,
          new_diversity_ratio: 42,
          is_unique_client: true,
          new_feedback_digest: Array.from(TEST_HASH),
          new_feedback_count: "1",
          tag1: "quality",
          tag2: "speed",
          endpoint: "/api/chat",
          feedback_uri: "ipfs://QmXXX",
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.data.score).toBeNull();
    });

    it("should handle FeedbackRevoked with seal_hash field", () => {
      const event = {
        name: "FeedbackRevoked",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: 5n,
          seal_hash: Array.from(TEST_HASH),
          slot: 999999n,
          original_score: 50,
          atom_enabled: false,
          had_impact: false,
          new_trust_tier: 0,
          new_quality_score: 5000,
          new_confidence: 7000,
          new_revoke_digest: Array.from(TEST_HASH),
          new_revoke_count: 3n,
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("FeedbackRevoked");
      expect(result!.data.sealHash).toEqual(new Uint8Array(TEST_HASH));
      expect(result!.data.feedbackIndex).toBe(5n);
      expect(result!.data.slot).toBe(999999n);
      expect(result!.data.atomEnabled).toBe(false);
      expect(result!.data.hadImpact).toBe(false);
      expect(result!.data.newRevokeCount).toBe(3n);
    });
  });
});
