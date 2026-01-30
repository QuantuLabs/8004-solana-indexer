/**
 * PDA Derivation Utilities for 8004 Agent Registry
 * Used by verifier for on-chain existence checks
 */

import { PublicKey } from "@solana/web3.js";
import { createHash } from "crypto";
import { config } from "../config.js";

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
