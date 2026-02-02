/**
 * PDA Derivation Utilities for 8004 Agent Registry
 * Used by verifier for on-chain existence checks
 */

import { Connection, PublicKey } from "@solana/web3.js";
import { createHash } from "crypto";
import { config } from "../config.js";

/**
 * RootConfig account structure (on-chain)
 * - discriminator: 8 bytes
 * - authority: 32 bytes (Pubkey)
 * - base_registry: 32 bytes (Pubkey) - The RegistryConfig PDA (NOT the collection!)
 * - bump: 1 byte
 */
export interface RootConfig {
  authority: PublicKey;
  baseRegistry: PublicKey; // This is RegistryConfig PDA, not collection
  bump: number;
}

/**
 * RegistryConfig account structure (on-chain)
 * - discriminator: 8 bytes
 * - collection: 32 bytes (Pubkey) - The actual Metaplex Core collection
 * - agent_count: 8 bytes (u64)
 * - owner: 32 bytes (Pubkey)
 * - fees_wallet: 32 bytes (Pubkey)
 * - register_fee: 8 bytes (u64)
 * - bump: 1 byte
 */
export interface RegistryConfig {
  collection: PublicKey;
  agentCount: bigint;
  owner: PublicKey;
  feesWallet: PublicKey;
  registerFee: bigint;
  bump: number;
}

/**
 * Fetch and parse RootConfig from on-chain
 */
export async function fetchRootConfig(
  connection: Connection,
  programId: PublicKey = AGENT_REGISTRY_PROGRAM_ID
): Promise<RootConfig | null> {
  const [rootConfigPda] = getRootConfigPda(programId);

  const accountInfo = await connection.getAccountInfo(rootConfigPda);

  if (!accountInfo) {
    return null;
  }

  // Parse RootConfig account data
  const data = accountInfo.data;
  if (data.length < 73) {
    throw new Error(`Invalid RootConfig account size: ${data.length}`);
  }

  return {
    authority: new PublicKey(data.slice(8, 40)),
    baseRegistry: new PublicKey(data.slice(40, 72)),
    bump: data[72],
  };
}

/**
 * Fetch and parse RegistryConfig from on-chain
 */
export async function fetchRegistryConfig(
  connection: Connection,
  registryConfigPda: PublicKey
): Promise<RegistryConfig | null> {
  const accountInfo = await connection.getAccountInfo(registryConfigPda);

  if (!accountInfo || accountInfo.data.length < 121) {
    return null;
  }

  const data = accountInfo.data;
  return {
    collection: new PublicKey(data.slice(8, 40)),
    agentCount: data.readBigUInt64LE(40),
    owner: new PublicKey(data.slice(48, 80)),
    feesWallet: new PublicKey(data.slice(80, 112)),
    registerFee: data.readBigUInt64LE(112),
    bump: data[120],
  };
}

/**
 * Fetch the actual base collection from on-chain
 * Follows: RootConfig.baseRegistry → RegistryConfig.collection
 */
export async function fetchBaseCollection(
  connection: Connection,
  programId: PublicKey = AGENT_REGISTRY_PROGRAM_ID
): Promise<PublicKey | null> {
  // 1. Get RootConfig
  const rootConfig = await fetchRootConfig(connection, programId);
  if (!rootConfig) {
    return null;
  }

  // 2. Get RegistryConfig from baseRegistry PDA
  const registryConfig = await fetchRegistryConfig(connection, rootConfig.baseRegistry);
  if (!registryConfig) {
    return null;
  }

  // 3. Return the actual collection
  return registryConfig.collection;
}

// Program IDs
export const AGENT_REGISTRY_PROGRAM_ID = new PublicKey(config.programId);
export const ATOM_ENGINE_PROGRAM_ID = new PublicKey(
  "AToMNmthLzvTy3D2kz2obFmbVCsTCmYpDw1ptWUJdeU8"
);
export const MPL_CORE_PROGRAM_ID = new PublicKey(
  "CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d"
);

/**
 * Convert a number to little-endian buffer
 */
function toLEBuffer(num: number | bigint, bytes: number): Buffer {
  const buf = Buffer.alloc(bytes);
  let n = BigInt(num);
  for (let i = 0; i < bytes; i++) {
    buf[i] = Number(n & 0xffn);
    n >>= 8n;
  }
  return buf;
}

/**
 * Derive root config PDA: ["root_config"]
 */
export function getRootConfigPda(
  programId: PublicKey = AGENT_REGISTRY_PROGRAM_ID
): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [Buffer.from("root_config")],
    programId
  );
}

/**
 * Derive registry config PDA: ["registry_config", collection]
 */
export function getRegistryConfigPda(
  collection: PublicKey,
  programId: PublicKey = AGENT_REGISTRY_PROGRAM_ID
): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [Buffer.from("registry_config"), collection.toBuffer()],
    programId
  );
}

/**
 * Derive ValidationConfig PDA: ["validation_config"]
 */
export function getValidationConfigPda(
  programId: PublicKey = AGENT_REGISTRY_PROGRAM_ID
): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [Buffer.from("validation_config")],
    programId
  );
}

/**
 * Derive agent PDA: ["agent", asset.key()]
 */
export function getAgentPda(
  asset: PublicKey,
  programId: PublicKey = AGENT_REGISTRY_PROGRAM_ID
): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [Buffer.from("agent"), asset.toBuffer()],
    programId
  );
}

/**
 * Derive validation request PDA: ["validation", asset.key(), validator, nonce (u32 LE)]
 */
export function getValidationRequestPda(
  asset: PublicKey,
  validator: PublicKey,
  nonce: number | bigint,
  programId: PublicKey = AGENT_REGISTRY_PROGRAM_ID
): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [
      Buffer.from("validation"),
      asset.toBuffer(),
      validator.toBuffer(),
      toLEBuffer(nonce, 4),
    ],
    programId
  );
}

/**
 * Derive metadata entry PDA: ["agent_meta", asset.key(), key_hash (16 bytes)]
 * key_hash is SHA256(key)[0..16] for collision resistance
 */
export function getMetadataEntryPda(
  asset: PublicKey,
  keyOrHash: string | Uint8Array,
  programId: PublicKey = AGENT_REGISTRY_PROGRAM_ID
): [PublicKey, number] {
  const keyHash = typeof keyOrHash === "string"
    ? computeKeyHash(keyOrHash)
    : keyOrHash.slice(0, 16);

  return PublicKey.findProgramAddressSync(
    [
      Buffer.from("agent_meta"),
      asset.toBuffer(),
      Buffer.from(keyHash),
    ],
    programId
  );
}

/**
 * Compute key hash for metadata PDA derivation
 * Returns first 16 bytes of SHA256(key)
 */
export function computeKeyHash(key: string): Uint8Array {
  const hash = createHash("sha256").update(key).digest();
  return new Uint8Array(hash.slice(0, 16));
}

/**
 * Derive AtomConfig PDA: ["atom_config"]
 */
export function getAtomConfigPda(
  programId: PublicKey = ATOM_ENGINE_PROGRAM_ID
): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [Buffer.from("atom_config")],
    programId
  );
}

/**
 * Derive AtomStats PDA: ["atom_stats", asset.key()]
 */
export function getAtomStatsPda(
  asset: PublicKey,
  programId: PublicKey = ATOM_ENGINE_PROGRAM_ID
): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [Buffer.from("atom_stats"), asset.toBuffer()],
    programId
  );
}

/**
 * Parse asset pubkey from string (base58)
 */
export function parseAssetPubkey(assetId: string): PublicKey {
  return new PublicKey(assetId);
}

/**
 * Check if a string is a valid base58 pubkey
 */
export function isValidPubkey(str: string): boolean {
  try {
    new PublicKey(str);
    return true;
  } catch {
    return false;
  }
}
