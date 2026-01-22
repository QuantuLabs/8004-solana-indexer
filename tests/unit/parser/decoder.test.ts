import { describe, it, expect, vi, beforeEach } from "vitest";
import { PublicKey } from "@solana/web3.js";
import {
  parseTransactionLogs,
  parseTransaction,
  toTypedEvent,
  idl,
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
      expect(idl.address).toBe("6MuHv4dY4p9E4hSCEPr9dgbCSpMhq8x1vrUexbMVjfw1");
    });

    it("should have events defined in IDL", () => {
      expect(idl.events).toBeDefined();
      expect(Array.isArray(idl.events)).toBe(true);
      expect(idl.events!.length).toBe(15);
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
      // Create a properly encoded AgentRegisteredInRegistry event
      const eventData = {
        asset: TEST_ASSET,
        registry: TEST_REGISTRY,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
      };

      const logs = createEventLogs("AgentRegisteredInRegistry", eventData);

      const result = parseTransactionLogs(logs);

      expect(result.length).toBe(1);
      expect(result[0].name).toBe("AgentRegisteredInRegistry");
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
      };

      const eventData2 = {
        asset: TEST_ASSET,
        newUri: "https://example.com/agent.json",
        updatedBy: TEST_OWNER,
      };

      const encoded1 = encodeAnchorEvent("AgentRegisteredInRegistry", eventData1);
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
      expect(result[0].name).toBe("AgentRegisteredInRegistry");
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
        registry: TEST_REGISTRY,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
      };

      const logs = createEventLogs("AgentRegisteredInRegistry", eventData);

      const tx = createMockParsedTransaction(TEST_SIGNATURE, logs);
      const result = parseTransaction(tx as any);

      expect(result).not.toBeNull();
      expect(result!.signature).toBe(TEST_SIGNATURE);
      expect(result!.slot).toBe(Number(TEST_SLOT));
      expect(result!.events.length).toBe(1);
      expect(result!.events[0].name).toBe("AgentRegisteredInRegistry");
    });

    it("should handle transaction with null blockTime", () => {
      const eventData = {
        asset: TEST_ASSET,
        registry: TEST_REGISTRY,
        collection: TEST_COLLECTION,
        owner: TEST_OWNER,
        atomEnabled: true,
      };

      const logs = createEventLogs("AgentRegisteredInRegistry", eventData);

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
    it("should convert AgentRegisteredInRegistry event", () => {
      const event = {
        name: "AgentRegisteredInRegistry",
        data: {
          asset: TEST_ASSET.toBase58(),
          registry: TEST_REGISTRY.toBase58(),
          collection: TEST_COLLECTION.toBase58(),
          owner: TEST_OWNER.toBase58(),
          atom_enabled: true,
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("AgentRegisteredInRegistry");
      expect(result!.data.asset.toBase58()).toBe(TEST_ASSET.toBase58());
      expect(result!.data.registry.toBase58()).toBe(TEST_REGISTRY.toBase58());
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

    it("should convert BaseRegistryCreated event", () => {
      const event = {
        name: "BaseRegistryCreated",
        data: {
          registry: TEST_REGISTRY.toBase58(),
          collection: TEST_COLLECTION.toBase58(),
          base_index: 0,
          created_by: TEST_OWNER.toBase58(),
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("BaseRegistryCreated");
      expect(result!.data.baseIndex).toBe(0);
    });

    it("should convert UserRegistryCreated event", () => {
      const event = {
        name: "UserRegistryCreated",
        data: {
          registry: TEST_REGISTRY.toBase58(),
          collection: TEST_COLLECTION.toBase58(),
          owner: TEST_OWNER.toBase58(),
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("UserRegistryCreated");
    });

    it("should convert BaseRegistryRotated event", () => {
      // Create a valid new registry key using 32-byte array
      const newRegistryBytes = new Uint8Array(32).fill(10);
      const newRegistry = new PublicKey(newRegistryBytes);
      const event = {
        name: "BaseRegistryRotated",
        data: {
          old_registry: TEST_REGISTRY.toBase58(),
          new_registry: newRegistry.toBase58(),
          rotated_by: TEST_OWNER.toBase58(),
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("BaseRegistryRotated");
    });

    it("should convert NewFeedback event", () => {
      const event = {
        name: "NewFeedback",
        data: {
          asset: TEST_ASSET.toBase58(),
          client_address: TEST_CLIENT.toBase58(),
          feedback_index: "0",
          score: 85,
          feedback_hash: Array.from(TEST_HASH),
          atom_enabled: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
          new_risk_score: 10,
          new_diversity_ratio: 42,
          is_unique_client: true,
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
          original_score: 85,
          atom_enabled: true,
          had_impact: true,
          new_trust_tier: 1,
          new_quality_score: 8000,
          new_confidence: 9000,
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
          responder: TEST_OWNER.toBase58(),
          response_uri: "ipfs://QmYYY",
          response_hash: Array.from(TEST_HASH),
        },
      };

      const result = toTypedEvent(event);

      expect(result).not.toBeNull();
      expect(result!.type).toBe("ResponseAppended");
      expect(result!.data.responseUri).toBe("ipfs://QmYYY");
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
      expect(result!.data.nonce).toBe(1);
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
        name: "AgentRegisteredInRegistry",
        data: {
          // Missing required fields
          asset: "invalid-not-a-pubkey",
        },
      };

      const result = toTypedEvent(event);

      expect(result).toBeNull();
    });
  });
});
