import { vi } from "vitest";
import { PublicKey, Keypair } from "@solana/web3.js";

// Generate deterministic test keypairs using seed bytes
function createTestPubkey(seed: number): PublicKey {
  const bytes = new Uint8Array(32).fill(seed);
  return new PublicKey(bytes);
}

// Test keypairs - using valid 32-byte public keys
export const TEST_PROGRAM_ID = new PublicKey(
  "3GGkAWC3mYYdud8GVBsKXK5QC9siXtFkWVZFYtbueVbC"
);
export const TEST_ASSET = createTestPubkey(1);
export const TEST_OWNER = createTestPubkey(2);
export const TEST_NEW_OWNER = createTestPubkey(3);
export const TEST_COLLECTION = createTestPubkey(4);
export const TEST_REGISTRY = createTestPubkey(5);
export const TEST_CLIENT = createTestPubkey(6);
export const TEST_VALIDATOR = createTestPubkey(7);
export const TEST_WALLET = createTestPubkey(8);

export const TEST_SIGNATURE =
  "5wHu1qwD7q2ggbJqCPtxnHZ2TrLQfEV9B7NqcBYBqzXh9J6vQQYc4Kdb8ZnZJwZqNjKt1QZcJZGJ";
export const TEST_SLOT = 12345678n;
export const TEST_BLOCK_TIME = new Date("2024-01-15T10:00:00Z");

export const TEST_HASH = new Uint8Array(32).fill(0xab);
export const TEST_VALUE = new Uint8Array([1, 2, 3, 4, 5]);

export function createMockConnection() {
  return {
    getSlot: vi.fn().mockResolvedValue(Number(TEST_SLOT)),
    getSignaturesForAddress: vi.fn().mockResolvedValue([]),
    getParsedTransaction: vi.fn().mockResolvedValue(null),
    onLogs: vi.fn().mockReturnValue(1),
    removeOnLogsListener: vi.fn().mockResolvedValue(undefined),
  };
}

export function createMockSignatureInfo(
  signature: string = TEST_SIGNATURE,
  slot: number = Number(TEST_SLOT),
  err: null | object = null
) {
  return {
    signature,
    slot,
    err,
    blockTime: Math.floor(TEST_BLOCK_TIME.getTime() / 1000),
    memo: null,
    confirmationStatus: "finalized" as const,
  };
}

export function createMockParsedTransaction(
  signature: string = TEST_SIGNATURE,
  logs: string[] = []
) {
  return {
    slot: Number(TEST_SLOT),
    blockTime: Math.floor(TEST_BLOCK_TIME.getTime() / 1000),
    transaction: {
      signatures: [signature],
      message: {
        accountKeys: [],
        instructions: [],
        recentBlockhash: "11111111111111111111111111111111",
      },
    },
    meta: {
      err: null,
      logMessages: logs,
      preBalances: [],
      postBalances: [],
    },
  };
}

// Sample Anchor event logs for testing
export const SAMPLE_LOGS = {
  agentRegistered: [
    `Program ${TEST_PROGRAM_ID.toBase58()} invoke [1]`,
    "Program log: Instruction: Register",
    `Program data: 6/FX4gHfuq8${Buffer.from(TEST_ASSET.toBytes()).toString("base64")}`,
    `Program ${TEST_PROGRAM_ID.toBase58()} success`,
  ],
  newFeedback: [
    `Program ${TEST_PROGRAM_ID.toBase58()} invoke [1]`,
    "Program log: Instruction: GiveFeedback",
    "Program log: NewFeedback event",
    `Program ${TEST_PROGRAM_ID.toBase58()} success`,
  ],
};

/**
 * Encode an Anchor event manually for testing.
 * Anchor events are: discriminator (8 bytes) + borsh-serialized data
 * The discriminator is the first 8 bytes of SHA256("event:<EventName>")
 */
export function encodeAnchorEvent(eventName: string, data: Record<string, any>): Buffer {
  // Event discriminators from the IDL
  const EVENT_DISCRIMINATORS: Record<string, number[]> = {
    AgentOwnerSynced: [101, 228, 184, 252, 20, 185, 70, 249],
    AgentRegisteredInRegistry: [235, 241, 87, 226, 1, 223, 186, 175],
    BaseRegistryCreated: [135, 156, 231, 228, 36, 76, 0, 43],
    BaseRegistryRotated: [142, 184, 57, 194, 241, 29, 60, 124],
    FeedbackRevoked: [42, 164, 86, 229, 102, 57, 226, 107],
    MetadataDeleted: [133, 11, 160, 186, 17, 196, 66, 205],
    MetadataSet: [165, 149, 191, 46, 63, 144, 96, 71],
    NewFeedback: [203, 253, 87, 187, 39, 80, 205, 50],
    ResponseAppended: [38, 184, 90, 199, 56, 249, 194, 240],
    UriUpdated: [71, 228, 14, 198, 192, 78, 38, 106],
    UserRegistryCreated: [55, 76, 218, 186, 141, 93, 122, 124],
    ValidationRequested: [142, 196, 147, 233, 10, 190, 66, 180],
    ValidationResponded: [214, 183, 195, 169, 184, 142, 105, 84],
    WalletUpdated: [93, 90, 213, 6, 170, 101, 178, 197],
  };

  const discriminator = EVENT_DISCRIMINATORS[eventName];
  if (!discriminator) {
    throw new Error(`Unknown event: ${eventName}`);
  }

  // Manually serialize the event data based on event type
  // This is a simplified serialization for testing purposes
  const buffers: Buffer[] = [Buffer.from(discriminator)];

  switch (eventName) {
    case "AgentRegisteredInRegistry":
      // asset: Pubkey (32), registry: Pubkey (32), collection: Pubkey (32), owner: Pubkey (32)
      buffers.push(Buffer.from(data.asset.toBytes()));
      buffers.push(Buffer.from(data.registry.toBytes()));
      buffers.push(Buffer.from(data.collection.toBytes()));
      buffers.push(Buffer.from(data.owner.toBytes()));
      break;

    case "UriUpdated":
      // asset: Pubkey (32), newUri: String, updatedBy: Pubkey (32)
      buffers.push(Buffer.from(data.asset.toBytes()));
      const uriBytes = Buffer.from(data.newUri, "utf-8");
      const uriLenBuf = Buffer.alloc(4);
      uriLenBuf.writeUInt32LE(uriBytes.length);
      buffers.push(uriLenBuf);
      buffers.push(uriBytes);
      buffers.push(Buffer.from(data.updatedBy.toBytes()));
      break;

    default:
      throw new Error(`Encoding not implemented for event: ${eventName}`);
  }

  return Buffer.concat(buffers);
}

/**
 * Create valid Anchor event logs for testing
 */
export function createEventLogs(eventName: string, data: Record<string, any>): string[] {
  const encoded = encodeAnchorEvent(eventName, data);
  const base64Data = encoded.toString("base64");

  return [
    `Program ${TEST_PROGRAM_ID.toBase58()} invoke [1]`,
    `Program data: ${base64Data}`,
    `Program ${TEST_PROGRAM_ID.toBase58()} success`,
  ];
}
