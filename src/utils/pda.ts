/**
 * PDA Derivation Utilities for 8004 Agent Registry
 * Used by verifier for on-chain existence checks
 */

import { Connection, PublicKey } from "@solana/web3.js";
import { createHash } from "crypto";
import {
  MPL_CORE_PROGRAM_ID as SDK_MPL_CORE_PROGRAM_ID,
} from "8004-solana";

/**
 * RootConfig account structure (on-chain)
 * Single-collection architecture (v0.6.0)
 * - discriminator: 8 bytes
 * - base_collection: 32 bytes (Pubkey) - The Metaplex Core collection
 * - authority: 32 bytes (Pubkey)
 * - bump: 1 byte
 * Total: 8 + 32 + 32 + 1 = 73 bytes
 */
export interface RootConfig {
  baseCollection: PublicKey;
  authority: PublicKey;
  bump: number;
}

/**
 * RegistryConfig account structure (on-chain)
 * Single-collection architecture (v0.6.0)
 * - discriminator: 8 bytes
 * - collection: 32 bytes (Pubkey) - The Metaplex Core collection
 * - authority: 32 bytes (Pubkey)
 * - bump: 1 byte
 * Total: 8 + 32 + 32 + 1 = 73 bytes
 */
export interface RegistryConfig {
  collection: PublicKey;
  authority: PublicKey;
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
    baseCollection: new PublicKey(data.slice(8, 40)),
    authority: new PublicKey(data.slice(40, 72)),
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

  if (!accountInfo || accountInfo.data.length < 73) {
    return null;
  }

  const data = accountInfo.data;
  return {
    collection: new PublicKey(data.slice(8, 40)),
    authority: new PublicKey(data.slice(40, 72)),
    bump: data[72],
  };
}

/**
 * Fetch the base collection from on-chain
 * Single-collection architecture: RootConfig.baseCollection is the collection directly
 */
export async function fetchBaseCollection(
  connection: Connection,
  programId: PublicKey = AGENT_REGISTRY_PROGRAM_ID
): Promise<PublicKey | null> {
  const rootConfig = await fetchRootConfig(connection, programId);
  if (!rootConfig) {
    return null;
  }
  return rootConfig.baseCollection;
}

const DEFAULT_REGISTRY_PROGRAM_ID = new PublicKey("8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C");
const DEFAULT_ATOM_ENGINE_PROGRAM_ID = new PublicKey("AToMufS4QD6hEXvcvBDg9m1AHeCLpmZQsyfYa5h9MwAF");

function resolveProgramId(envValue: string | undefined, fallback: PublicKey): PublicKey {
  if (!envValue || envValue.trim() === "") return fallback;
  return new PublicKey(envValue.trim());
}

export const AGENT_REGISTRY_PROGRAM_ID = resolveProgramId(
  process.env.PROGRAM_ID,
  DEFAULT_REGISTRY_PROGRAM_ID
);
export const ATOM_ENGINE_PROGRAM_ID = resolveProgramId(
  process.env.ATOM_ENGINE_PROGRAM_ID,
  DEFAULT_ATOM_ENGINE_PROGRAM_ID
);
export const MPL_CORE_PROGRAM_ID = SDK_MPL_CORE_PROGRAM_ID;

const solanaNetwork = (process.env.SOLANA_NETWORK || "").trim().toLowerCase();
if (solanaNetwork === "mainnet-beta" && ATOM_ENGINE_PROGRAM_ID.equals(DEFAULT_ATOM_ENGINE_PROGRAM_ID)) {
  console.warn(
    "[CONFIG WARNING] SOLANA_NETWORK=mainnet-beta but ATOM_ENGINE_PROGRAM_ID is still the default devnet ID. Set ATOM_ENGINE_PROGRAM_ID to the mainnet deployment before production use."
  );
}

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
