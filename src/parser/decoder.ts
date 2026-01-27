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
    // Warn if number exceeds safe integer range (potential precision loss)
    if (!Number.isSafeInteger(value)) {
      logger.warn({ value }, "Unsafe integer conversion to BigInt - potential precision loss");
    }
    return BigInt(Math.trunc(value)); // Use trunc to avoid fractional issues
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
 * Parse i64 value (signed)
 */
function parseI64(value: unknown): bigint {
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

/**
 * Parse events from a transaction's logs
 */
export function parseTransactionLogs(logs: string[]): ParsedEvent[] {
  const events: ParsedEvent[] = [];

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
      case "AgentRegisteredInRegistry":
        return {
          type: "AgentRegisteredInRegistry",
          data: {
            asset: new PublicKey(data.asset as string),
            registry: new PublicKey(data.registry as string),
            collection: new PublicKey(data.collection as string),
            owner: new PublicKey(data.owner as string),
            atomEnabled: data.atom_enabled === undefined
              ? true
              : (data.atom_enabled as boolean), // snake_case from IDL
            agentUri: (data.agent_uri as string) || "", // snake_case from IDL
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

      case "BaseRegistryCreated":
        return {
          type: "BaseRegistryCreated",
          data: {
            registry: new PublicKey(data.registry as string),
            collection: new PublicKey(data.collection as string),
            baseIndex: data.base_index as number,                // snake_case from IDL
            createdBy: new PublicKey(data.created_by as string), // snake_case from IDL
          },
        };

      case "UserRegistryCreated":
        return {
          type: "UserRegistryCreated",
          data: {
            registry: new PublicKey(data.registry as string),
            collection: new PublicKey(data.collection as string),
            owner: new PublicKey(data.owner as string),
          },
        };

      case "BaseRegistryRotated":
        return {
          type: "BaseRegistryRotated",
          data: {
            oldRegistry: new PublicKey(data.old_registry as string), // snake_case from IDL
            newRegistry: new PublicKey(data.new_registry as string), // snake_case from IDL
            rotatedBy: new PublicKey(data.rotated_by as string),     // snake_case from IDL
          },
        };

      case "NewFeedback":
        return {
          type: "NewFeedback",
          data: {
            asset: new PublicKey(data.asset as string),
            clientAddress: new PublicKey(data.client_address as string),
            feedbackIndex: parseBigInt(data.feedback_index),
            value: parseI64(data.value),
            valueDecimals: data.value_decimals as number,
            score: parseOptionU8(data.score),
            feedbackHash: new Uint8Array(data.feedback_hash as number[]),
            atomEnabled: data.atom_enabled as boolean,
            newTrustTier: data.new_trust_tier as number,
            newQualityScore: data.new_quality_score as number,
            newConfidence: data.new_confidence as number,
            newRiskScore: data.new_risk_score as number,
            newDiversityRatio: data.new_diversity_ratio as number,
            isUniqueClient: data.is_unique_client as boolean,
            tag1: data.tag1 as string,
            tag2: data.tag2 as string,
            endpoint: data.endpoint as string,
            feedbackUri: data.feedback_uri as string,
          },
        };

      case "FeedbackRevoked":
        return {
          type: "FeedbackRevoked",
          data: {
            asset: new PublicKey(data.asset as string),
            clientAddress: new PublicKey(data.client_address as string), // snake_case from IDL
            feedbackIndex: parseBigInt(data.feedback_index),             // snake_case from IDL
            // ATOM enriched fields (v0.4.0)
            originalScore: data.original_score as number,                // snake_case from IDL
            atomEnabled: data.atom_enabled === undefined
              ? false
              : (data.atom_enabled as boolean),                          // snake_case from IDL
            hadImpact: data.had_impact as boolean,                       // snake_case from IDL
            newTrustTier: data.new_trust_tier as number,                 // snake_case from IDL
            newQualityScore: data.new_quality_score as number,           // snake_case from IDL
            newConfidence: data.new_confidence as number,                // snake_case from IDL
          },
        };

      case "ResponseAppended":
        return {
          type: "ResponseAppended",
          data: {
            asset: new PublicKey(data.asset as string),
            client: new PublicKey(data.client as string),
            feedbackIndex: parseBigInt(data.feedback_index),             // snake_case from IDL
            responder: new PublicKey(data.responder as string),
            responseUri: data.response_uri as string,                    // snake_case from IDL
            responseHash: new Uint8Array(data.response_hash as number[]),// snake_case from IDL
          },
        };

      case "ValidationRequested":
        return {
          type: "ValidationRequested",
          data: {
            asset: new PublicKey(data.asset as string),
            validatorAddress: new PublicKey(data.validator_address as string), // snake_case from IDL
            nonce: data.nonce as number,
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
            nonce: data.nonce as number,
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
