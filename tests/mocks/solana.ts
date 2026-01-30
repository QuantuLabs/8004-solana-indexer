import { vi } from "vitest";
import { PublicKey, Keypair } from "@solana/web3.js";
import { createHash } from "crypto";

// Generate deterministic test keypairs using seed bytes
function createTestPubkey(seed: number): PublicKey {
  const bytes = new Uint8Array(32).fill(seed);
  return new PublicKey(bytes);
}

// Test keypairs - using valid 32-byte public keys
export const TEST_PROGRAM_ID = new PublicKey(
  "8oo4SbcgjRBAXjmGU4YMcdFqfeLLrtn7n6f358PkAc3N"
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

function getEventDiscriminator(eventName: string): Buffer {
  return createHash("sha256")
    .update(`event:${eventName}`)
    .digest()
    .subarray(0, 8);
}

/**
 * Encode an Anchor event manually for testing.
 * Anchor events are: discriminator (8 bytes) + borsh-serialized data
 * The discriminator is the first 8 bytes of SHA256("event:<EventName>")
 */
export function encodeAnchorEvent(eventName: string, data: Record<string, any>): Buffer {
  const discriminator = getEventDiscriminator(eventName);

  // Manually serialize the event data based on event type.
  const buffers: Buffer[] = [discriminator];

  switch (eventName) {
    case "AgentRegisteredInRegistry":
      // asset: Pubkey (32), registry: Pubkey (32), collection: Pubkey (32), owner: Pubkey (32), atomEnabled: bool, agentUri: String
      buffers.push(Buffer.from(data.asset.toBytes()));
      buffers.push(Buffer.from(data.registry.toBytes()));
      buffers.push(Buffer.from(data.collection.toBytes()));
      buffers.push(Buffer.from(data.owner.toBytes()));
      buffers.push(Buffer.from([data.atomEnabled ? 1 : 0]));
      // agent_uri: String (length prefix + bytes)
      const agentUri = data.agentUri || "";
      const agentUriBytes = Buffer.from(agentUri, "utf-8");
      const agentUriLenBuf = Buffer.alloc(4);
      agentUriLenBuf.writeUInt32LE(agentUriBytes.length);
      buffers.push(agentUriLenBuf);
      buffers.push(agentUriBytes);
      break;

    case "UriUpdated":
      // asset: Pubkey (32), updatedBy: Pubkey (32), newUri: String
      buffers.push(Buffer.from(data.asset.toBytes()));
      buffers.push(Buffer.from(data.updatedBy.toBytes()));
      const uriBytes = Buffer.from(data.newUri, "utf-8");
      const uriLenBuf = Buffer.alloc(4);
      uriLenBuf.writeUInt32LE(uriBytes.length);
      buffers.push(uriLenBuf);
      buffers.push(uriBytes);
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
