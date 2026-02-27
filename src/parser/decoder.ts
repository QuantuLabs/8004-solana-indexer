import { BorshCoder, EventParser, Idl } from "@coral-xyz/anchor";
import { PublicKey, ParsedTransactionWithMeta } from "@solana/web3.js";
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { ProgramEvent } from "./types.js";
import { createChildLogger } from "../logger.js";
import { config } from "../config.js";

const logger = createChildLogger("decoder");

/**
 * Parse value to bigint (handles Anchor BN format, string, number)
 * Validates safe integer range to prevent silent precision loss
 */
function parseBigInt(value: unknown): bigint {
  if (typeof value === 'bigint') return value;
  if (typeof value === 'string') return BigInt(value);
  if (typeof value === 'number') {
    if (!Number.isSafeInteger(value)) {
      throw new Error(`Unsafe integer ${value} exceeds MAX_SAFE_INTEGER, use string or BN`);
    }
    return BigInt(Math.trunc(value));
  }
  if (value && typeof value === 'object' && 'negative' in value && 'words' in value) {
    const bn = value as { negative: number; words: number[] };
    let result = 0n;
    for (let i = 0; i < bn.words.length; i++) {
      result += BigInt(bn.words[i]) << BigInt(i * 26);
    }
    return bn.negative ? -result : result;
  }
  return BigInt(String(value));
}

/**
 * Parse signed integer value from event payload.
 * Supports full i128 range carried by the v0.6.0 feedback schema.
 */
function parseSignedInt(value: unknown): bigint {
  return parseBigInt(value);
}

/**
 * Parse Option<u8> - returns number or null
 */
function parseOptionU8(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') return value;
  if (value && typeof value === 'object' && 'some' in value) {
    return (value as { some: number }).some;
  }
  return null;
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const idlPath = join(__dirname, "../../idl/agent_registry_8004.json");
const idl: Idl = JSON.parse(readFileSync(idlPath, "utf-8"));
const coder = new BorshCoder(idl);
const eventParser = new EventParser(new PublicKey(config.programId), coder);

// IDL version info for startup validation
export const IDL_VERSION = (idl as any).metadata?.version || "unknown";
export const IDL_PROGRAM_ID = (idl as any).address || config.programId;

logger.info({ version: IDL_VERSION, programId: IDL_PROGRAM_ID }, "IDL loaded");

export interface ParsedEvent {
  name: string;
  data: Record<string, unknown>;
}

export interface TransactionEvents {
  signature: string;
  slot: number;
  blockTime: number | null;
  events: ParsedEvent[];
}

interface IdentityLockHints {
  collection: boolean[];
  parent: boolean[];
}

function extractIdentityLockHintsFromLogs(logs: string[]): IdentityLockHints {
  const hints: IdentityLockHints = {
    collection: [],
    parent: [],
  };

  for (const line of logs) {
    const normalized = line.toLowerCase();
    if (!normalized.includes("instruction:")) continue;

    const isCollectionWithOptions =
      normalized.includes("set_collection_pointer_with_options") ||
      normalized.includes("setcollectionpointerwithoptions");
    const isCollectionDefault =
      normalized.includes("set_collection_pointer") ||
      normalized.includes("setcollectionpointer");
    const isParentWithOptions =
      normalized.includes("set_parent_asset_with_options") ||
      normalized.includes("setparentassetwithoptions");
    const isParentDefault =
      normalized.includes("set_parent_asset") ||
      normalized.includes("setparentasset");

    if (isCollectionWithOptions) {
      // SDK path uses *_with_options for lock=false.
      hints.collection.push(false);
      continue;
    }
    if (isCollectionDefault) {
      hints.collection.push(true);
      continue;
    }
    if (isParentWithOptions) {
      // SDK path uses *_with_options for lock=false.
      hints.parent.push(false);
      continue;
    }
    if (isParentDefault) {
      hints.parent.push(true);
    }
  }

  return hints;
}

function applyIdentityLockHints(
  events: ParsedEvent[],
  hints: IdentityLockHints
): void {
  let collectionIdx = 0;
  let parentIdx = 0;

  for (const event of events) {
    if (event.name === "CollectionPointerSet") {
      const lock = hints.collection[collectionIdx];
      if (typeof lock === "boolean") {
        event.data.lock = lock;
      }
      collectionIdx += 1;
      continue;
    }
    if (event.name === "ParentAssetSet") {
      const lock = hints.parent[parentIdx];
      if (typeof lock === "boolean") {
        event.data.lock = lock;
      }
      parentIdx += 1;
    }
  }
}

/**
 * Parse events from a transaction's logs
 */
export function parseTransactionLogs(logs: string[]): ParsedEvent[] {
  const events: ParsedEvent[] = [];
  const lockHints = extractIdentityLockHintsFromLogs(logs);

  try {
    const generator = eventParser.parseLogs(logs);
    for (const event of generator) {
      events.push({
        name: event.name,
        data: event.data as Record<string, unknown>,
      });
    }
  } catch (error) {
    logger.debug({ error }, "Failed to parse some logs");
  }

  applyIdentityLockHints(events, lockHints);

  return events;
}

/**
 * Parse events from a parsed transaction
 */
export function parseTransaction(
  tx: ParsedTransactionWithMeta
): TransactionEvents | null {
  if (!tx.meta?.logMessages) {
    return null;
  }

  const events = parseTransactionLogs(tx.meta.logMessages);

  if (events.length === 0) {
    return null;
  }

  return {
    signature: tx.transaction.signatures[0],
    slot: tx.slot,
    blockTime: tx.blockTime ?? null,
    events,
  };
}

/**
 * Convert raw event data to typed event
 */
export function toTypedEvent(event: ParsedEvent): ProgramEvent | null {
  const { name, data } = event;

  try {
    switch (name) {
      // v0.6.0: AgentRegistered (was AgentRegisteredInRegistry, no registry field)
      case "AgentRegistered":
        return {
          type: "AgentRegistered",
          data: {
            asset: new PublicKey(data.asset as string),
            collection: new PublicKey(data.collection as string),
            owner: new PublicKey(data.owner as string),
            atomEnabled: data.atom_enabled === undefined
              ? true
              : (data.atom_enabled as boolean),
            agentUri: (data.agent_uri as string) || "",
          },
        };

      case "AtomEnabled":
        return {
          type: "AtomEnabled",
          data: {
            asset: new PublicKey(data.asset as string),
            enabledBy: new PublicKey(data.enabled_by as string), // snake_case from IDL
          },
        };

      case "AgentOwnerSynced":
        return {
          type: "AgentOwnerSynced",
          data: {
            asset: new PublicKey(data.asset as string),
            oldOwner: new PublicKey(data.old_owner as string),   // snake_case from IDL
            newOwner: new PublicKey(data.new_owner as string),   // snake_case from IDL
          },
        };

      case "UriUpdated":
        return {
          type: "UriUpdated",
          data: {
            asset: new PublicKey(data.asset as string),
            newUri: data.new_uri as string,                       // snake_case from IDL
            updatedBy: new PublicKey(data.updated_by as string), // snake_case from IDL
          },
        };

      case "WalletUpdated":
        return {
          type: "WalletUpdated",
          data: {
            asset: new PublicKey(data.asset as string),
            oldWallet: data.old_wallet                           // snake_case from IDL
              ? new PublicKey(data.old_wallet as string)
              : null,
            newWallet: new PublicKey(data.new_wallet as string), // snake_case from IDL
            updatedBy: new PublicKey(data.updated_by as string), // snake_case from IDL
          },
        };

      case "WalletResetOnOwnerSync":
        return {
          type: "WalletResetOnOwnerSync",
          data: {
            asset: new PublicKey(data.asset as string),
            oldWallet: data.old_wallet
              ? new PublicKey(data.old_wallet as string)
              : null,
            newWallet: new PublicKey(data.new_wallet as string),
            ownerAfterSync: new PublicKey(data.owner_after_sync as string),
          },
        };

      case "MetadataSet":
        return {
          type: "MetadataSet",
          data: {
            asset: new PublicKey(data.asset as string),
            key: data.key as string,
            value: new Uint8Array(data.value as number[]),
            immutable: data.immutable as boolean,
          },
        };

      case "MetadataDeleted":
        return {
          type: "MetadataDeleted",
          data: {
            asset: new PublicKey(data.asset as string),
            key: data.key as string,
          },
        };

      // v0.6.0: RegistryInitialized (replaces BaseRegistryCreated/UserRegistryCreated)
      case "RegistryInitialized":
        return {
          type: "RegistryInitialized",
          data: {
            collection: new PublicKey(data.collection as string),
            authority: new PublicKey(data.authority as string),
          },
        };

      case "CollectionPointerSet":
        return {
          type: "CollectionPointerSet",
          data: {
            asset: new PublicKey(data.asset as string),
            setBy: new PublicKey(data.set_by as string),
            col: data.col as string,
            lock: typeof data.lock === "boolean" ? (data.lock as boolean) : undefined,
          },
        };

      case "NewFeedback":
        return {
          type: "NewFeedback",
          data: {
            asset: new PublicKey(data.asset as string),
            clientAddress: new PublicKey(data.client_address as string),
            feedbackIndex: parseBigInt(data.feedback_index),
            slot: parseBigInt(data.slot),
            value: parseSignedInt(data.value),
            valueDecimals: data.value_decimals as number,
            score: parseOptionU8(data.score),
            // SEAL v1: feedback_file_hash is optional, seal_hash is computed on-chain
            feedbackFileHash: data.feedback_file_hash
              ? new Uint8Array(data.feedback_file_hash as number[])
              : null,
            sealHash: new Uint8Array(data.seal_hash as number[]),
            atomEnabled: data.atom_enabled as boolean,
            newTrustTier: data.new_trust_tier as number,
            newQualityScore: data.new_quality_score as number,
            newConfidence: data.new_confidence as number,
            newRiskScore: data.new_risk_score as number,
            newDiversityRatio: data.new_diversity_ratio as number,
            isUniqueClient: data.is_unique_client as boolean,
            newFeedbackDigest: new Uint8Array(data.new_feedback_digest as number[]),
            newFeedbackCount: parseBigInt(data.new_feedback_count),
            tag1: data.tag1 as string,
            tag2: data.tag2 as string,
            endpoint: data.endpoint as string,
            feedbackUri: data.feedback_uri as string,
          },
        };

      case "ParentAssetSet":
        return {
          type: "ParentAssetSet",
          data: {
            asset: new PublicKey(data.asset as string),
            parentAsset: new PublicKey(data.parent_asset as string),
            parentCreator: new PublicKey(data.parent_creator as string),
            setBy: new PublicKey(data.set_by as string),
            lock: typeof data.lock === "boolean" ? (data.lock as boolean) : undefined,
          },
        };

      case "FeedbackRevoked":
        return {
          type: "FeedbackRevoked",
          data: {
            asset: new PublicKey(data.asset as string),
            clientAddress: new PublicKey(data.client_address as string),
            feedbackIndex: parseBigInt(data.feedback_index),
            // SEAL v1: seal_hash (was feedback_hash)
            sealHash: new Uint8Array(data.seal_hash as number[]),
            slot: parseBigInt(data.slot),
            originalScore: data.original_score as number,
            atomEnabled: data.atom_enabled === undefined
              ? false
              : (data.atom_enabled as boolean),
            hadImpact: data.had_impact as boolean,
            newTrustTier: data.new_trust_tier as number,
            newQualityScore: data.new_quality_score as number,
            newConfidence: data.new_confidence as number,
            newRevokeDigest: new Uint8Array(data.new_revoke_digest as number[]),
            newRevokeCount: parseBigInt(data.new_revoke_count),
          },
        };

      case "ResponseAppended":
        return {
          type: "ResponseAppended",
          data: {
            asset: new PublicKey(data.asset as string),
            client: new PublicKey(data.client as string),
            feedbackIndex: parseBigInt(data.feedback_index),
            slot: parseBigInt(data.slot),
            responder: new PublicKey(data.responder as string),
            responseHash: new Uint8Array(data.response_hash as number[]),
            // SEAL v1: seal_hash (was feedback_hash)
            sealHash: new Uint8Array(data.seal_hash as number[]),
            newResponseDigest: new Uint8Array(data.new_response_digest as number[]),
            newResponseCount: parseBigInt(data.new_response_count),
            responseUri: data.response_uri as string,
          },
        };

      case "ValidationRequested":
        return {
          type: "ValidationRequested",
          data: {
            asset: new PublicKey(data.asset as string),
            validatorAddress: new PublicKey(data.validator_address as string), // snake_case from IDL
            nonce: BigInt(data.nonce as string | number),
            requestUri: data.request_uri as string,                            // snake_case from IDL
            requestHash: new Uint8Array(data.request_hash as number[]),        // snake_case from IDL
            requester: new PublicKey(data.requester as string),
          },
        };

      case "ValidationResponded":
        return {
          type: "ValidationResponded",
          data: {
            asset: new PublicKey(data.asset as string),
            validatorAddress: new PublicKey(data.validator_address as string), // snake_case from IDL
            nonce: BigInt(data.nonce as string | number),
            response: data.response as number,
            responseUri: data.response_uri as string,                          // snake_case from IDL
            responseHash: new Uint8Array(data.response_hash as number[]),      // snake_case from IDL
            tag: data.tag as string,
          },
        };

      default:
        logger.warn({ eventName: name }, "Unknown event type");
        return null;
    }
  } catch (error) {
    logger.error({ error, event }, "Failed to convert event to typed event");
    return null;
  }
}

export { idl, coder, eventParser };
