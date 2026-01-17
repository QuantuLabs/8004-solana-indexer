import { PublicKey } from "@solana/web3.js";

// Event types matching the IDL
export interface AgentRegisteredInRegistry {
  asset: PublicKey;
  registry: PublicKey;
  collection: PublicKey;
  owner: PublicKey;
}

export interface AgentOwnerSynced {
  asset: PublicKey;
  oldOwner: PublicKey;
  newOwner: PublicKey;
}

export interface UriUpdated {
  asset: PublicKey;
  newUri: string;
  updatedBy: PublicKey;
}

export interface WalletUpdated {
  asset: PublicKey;
  oldWallet: PublicKey | null;
  newWallet: PublicKey;
  updatedBy: PublicKey;
}

export interface MetadataSet {
  asset: PublicKey;
  key: string;
  value: Uint8Array;
  immutable: boolean;
}

export interface MetadataDeleted {
  asset: PublicKey;
  key: string;
}

export interface BaseRegistryCreated {
  registry: PublicKey;
  collection: PublicKey;
  baseIndex: number;
  createdBy: PublicKey;
}

export interface UserRegistryCreated {
  registry: PublicKey;
  collection: PublicKey;
  owner: PublicKey;
}

export interface BaseRegistryRotated {
  oldRegistry: PublicKey;
  newRegistry: PublicKey;
  rotatedBy: PublicKey;
}

export interface NewFeedback {
  asset: PublicKey;
  clientAddress: PublicKey;
  feedbackIndex: bigint;
  score: number;
  feedbackHash: Uint8Array;
  // ATOM enabled flag
  atomEnabled: boolean;       // Whether ATOM Engine was used
  // ATOM enriched fields (v0.4.0) - 0 if atomEnabled=false
  newTrustTier: number;       // u8 (0-4)
  newQualityScore: number;    // u16 (0-10000)
  newConfidence: number;      // u16 (0-10000)
  newRiskScore: number;       // u8 (0-100)
  newDiversityRatio: number;  // u8 (0-255)
  isUniqueClient: boolean;
  // Variable-size fields
  tag1: string;
  tag2: string;
  endpoint: string;
  feedbackUri: string;
}

export interface FeedbackRevoked {
  asset: PublicKey;
  clientAddress: PublicKey;
  feedbackIndex: bigint;
  originalScore: number;      // u8
  // ATOM enabled flag
  atomEnabled: boolean;       // Whether ATOM Engine was used
  // ATOM enriched fields (v0.4.0) - 0 if atomEnabled=false
  hadImpact: boolean;
  newTrustTier: number;       // u8 (0-4)
  newQualityScore: number;    // u16 (0-10000)
  newConfidence: number;      // u16 (0-10000)
}

export interface ResponseAppended {
  asset: PublicKey;
  feedbackIndex: bigint;
  responder: PublicKey;
  responseUri: string;
  responseHash: Uint8Array;
}

export interface ValidationRequested {
  asset: PublicKey;
  validatorAddress: PublicKey;
  nonce: number;
  requestUri: string;
  requestHash: Uint8Array;
  requester: PublicKey;
}

export interface ValidationResponded {
  asset: PublicKey;
  validatorAddress: PublicKey;
  nonce: number;
  response: number;
  responseUri: string;
  responseHash: Uint8Array;
  tag: string;
}

// Union type for all events
export type ProgramEvent =
  | { type: "AgentRegisteredInRegistry"; data: AgentRegisteredInRegistry }
  | { type: "AgentOwnerSynced"; data: AgentOwnerSynced }
  | { type: "UriUpdated"; data: UriUpdated }
  | { type: "WalletUpdated"; data: WalletUpdated }
  | { type: "MetadataSet"; data: MetadataSet }
  | { type: "MetadataDeleted"; data: MetadataDeleted }
  | { type: "BaseRegistryCreated"; data: BaseRegistryCreated }
  | { type: "UserRegistryCreated"; data: UserRegistryCreated }
  | { type: "BaseRegistryRotated"; data: BaseRegistryRotated }
  | { type: "NewFeedback"; data: NewFeedback }
  | { type: "FeedbackRevoked"; data: FeedbackRevoked }
  | { type: "ResponseAppended"; data: ResponseAppended }
  | { type: "ValidationRequested"; data: ValidationRequested }
  | { type: "ValidationResponded"; data: ValidationResponded };

// Event discriminators from IDL (first 8 bytes of SHA256 of event name)
export const EVENT_DISCRIMINATORS: Record<string, string> = {
  AgentOwnerSynced: "65e4b8fc14b946f9",
  AgentRegisteredInRegistry: "ebf157e201dfbaaf",
  BaseRegistryCreated: "879ce7e4244c002b",
  BaseRegistryRotated: "8eb839c2f11d3c7c",
  FeedbackRevoked: "cd101f5e36651007",
  MetadataDeleted: "fbf4993f23fc8336",
  MetadataSet: "be7d47770e1f1ac5",
  NewFeedback: "0ea23ac2832a0b95",
  ResponseAppended: "a8a9d6c1ab01e87b",
  UriUpdated: "aac74ea731546602",
  UserRegistryCreated: "f58b689be5829872",
  ValidationRequested: "852afcc65287b741",
  ValidationResponded: "5d3ff665d4d035a7",
  WalletUpdated: "d7220a3b1872c981",
};

// Reverse lookup: discriminator -> event name
export const DISCRIMINATOR_TO_EVENT: Record<string, string> = Object.fromEntries(
  Object.entries(EVENT_DISCRIMINATORS).map(([name, disc]) => [disc, name])
);
