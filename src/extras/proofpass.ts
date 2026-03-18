import { PublicKey } from "@solana/web3.js";
import type { NewFeedback } from "../parser/types.js";

export interface ProofPassFinalizeLog {
  sessionHex: string;
  clientAddress: string;
  asset: string;
  feedbackIndex: string;
  contextType: number;
  contextRefHash: string;
  sealHash: string;
}

export interface ProofPassFeedbackMatch {
  id: string;
  asset: string;
  clientAddress: string;
  feedbackIndex: string;
  txSignature: string;
  blockSlot: string;
  feedbackHash: string;
  proofpassSession: string;
  contextType: number;
  contextRefHash: string;
}

export interface ProofPassFeedbackCandidate {
  asset: string;
  clientAddress: string;
  feedbackIndex: string;
  txSignature: string;
  blockSlot: string;
  feedbackHash: string;
}

const FINALIZE_PREFIX = "PP_FINALIZE|v=1|";
export const DEFAULT_PROOFPASS_PROGRAM_ID = "72WFGnAp9EjPok7JbadCEC1j83TZe3ti8k6ax25dQVzG";
const RAW_PROOFPASS_PROGRAM_ID = (process.env.PROOFPASS_PROGRAM_ID || DEFAULT_PROOFPASS_PROGRAM_ID).trim();
export const PROOFPASS_PROGRAM_ID = normalizeProofPassProgramId(
  RAW_PROOFPASS_PROGRAM_ID,
  isProofPassValidationEnabled()
);
const PROGRAM_INVOKE_RE = /^Program ([1-9A-HJ-NP-Za-km-z]+) invoke \[\d+\]$/;
const PROGRAM_SUCCESS_RE = /^Program ([1-9A-HJ-NP-Za-km-z]+) success$/;
const PROGRAM_FAILED_RE = /^Program ([1-9A-HJ-NP-Za-km-z]+) failed: /;

function scanProofPassFinalizeLogs(logs: string[]): {
  rows: ProofPassFinalizeLog[];
  sawInFrameFinalizeMarker: boolean;
} {
  const rows: ProofPassFinalizeLog[] = [];
  const executionStack: string[] = [];
  let sawInFrameFinalizeMarker = false;

  for (const line of logs) {
    const invokeMatch = line.match(PROGRAM_INVOKE_RE);
    if (invokeMatch) {
      executionStack.push(invokeMatch[1]);
      continue;
    }

    const successMatch = line.match(PROGRAM_SUCCESS_RE);
    if (successMatch) {
      popExecutionFrame(executionStack, successMatch[1]);
      continue;
    }

    const failedMatch = line.match(PROGRAM_FAILED_RE);
    if (failedMatch) {
      popExecutionFrame(executionStack, failedMatch[1]);
      continue;
    }

    if (executionStack[executionStack.length - 1] !== PROOFPASS_PROGRAM_ID) {
      continue;
    }

    const start = line.indexOf(FINALIZE_PREFIX);
    if (start === -1) continue;
    sawInFrameFinalizeMarker = true;

    const payload = line.slice(start);
    const fields = payload.split("|");
    const values = new Map<string, string>();

    for (const field of fields.slice(2)) {
      const separator = field.indexOf("=");
      if (separator === -1) continue;
      values.set(field.slice(0, separator), field.slice(separator + 1));
    }

    const sessionHex = normalizeHex32(values.get("session"));
    const clientHex = normalizeHex32(values.get("client"));
    const assetHex = normalizeHex32(values.get("asset"));
    const sealHash = normalizeHex32(values.get("seal"));
    const contextRefHash = normalizeHex32(values.get("context_ref"));
    const feedbackIndex = values.get("feedback_index");
    const contextType = values.get("context");

    if (
      sessionHex === null
      || clientHex === null
      || assetHex === null
      || sealHash === null
      || contextRefHash === null
      || feedbackIndex === undefined
      || !/^\d+$/.test(feedbackIndex)
      || contextType === undefined
      || !/^\d+$/.test(contextType)
    ) {
      continue;
    }

    rows.push({
      sessionHex,
      clientAddress: hex32ToPublicKeyBase58(clientHex),
      asset: hex32ToPublicKeyBase58(assetHex),
      feedbackIndex,
      contextType: Number.parseInt(contextType, 10),
      contextRefHash,
      sealHash,
    });
  }

  return { rows, sawInFrameFinalizeMarker };
}

export function parseProofPassFinalizeLogs(logs: string[]): ProofPassFinalizeLog[] {
  return scanProofPassFinalizeLogs(logs).rows;
}

export function matchProofPassFeedbacks(params: {
  logs: string[];
  signature: string;
  slot: bigint;
  feedbackEvents: NewFeedback[];
}): ProofPassFeedbackMatch[] {
  return matchProofPassFeedbackCandidates({
    logs: params.logs,
    signature: params.signature,
    slot: params.slot,
    feedbacks: params.feedbackEvents.map((feedback) => ({
      asset: feedback.asset.toBase58(),
      clientAddress: feedback.clientAddress.toBase58(),
      feedbackIndex: feedback.feedbackIndex.toString(),
      txSignature: params.signature,
      blockSlot: params.slot.toString(),
      feedbackHash: Buffer.from(feedback.sealHash).toString("hex").toLowerCase(),
    })),
  });
}

export function matchProofPassFeedbackCandidates(params: {
  logs: string[];
  signature: string;
  slot: bigint;
  feedbacks: ProofPassFeedbackCandidate[];
}): ProofPassFeedbackMatch[] {
  if (params.feedbacks.length === 0) {
    return [];
  }

  const { rows: finalizations, sawInFrameFinalizeMarker } = scanProofPassFinalizeLogs(params.logs);
  if (finalizations.length === 0) {
    if (sawInFrameFinalizeMarker) {
      throw new Error(
        `ProofPass transaction ${params.signature} emitted feedback without a parseable PP_FINALIZE log`
      );
    }
    return [];
  }

  const { matches, matchedFinalizationKeys } = matchParsedProofPassFinalizations(
    finalizations,
    params.feedbacks,
    params.signature
  );

  if (
    finalizations.some((finalize) =>
      !matchedFinalizationKeys.has(
        proofPassMatchKey(
          finalize.asset,
          finalize.clientAddress,
          finalize.feedbackIndex,
          finalize.sealHash
        )
      )
    )
  ) {
    throw new Error(
      `ProofPass transaction ${params.signature} has unmatched PP_FINALIZE logs; refusing silent downgrade`
    );
  }

  return matches;
}

export function extractProofPassFinalizeMatches(params: {
  logs: string[];
  signature: string;
  slot: bigint;
}): ProofPassFeedbackMatch[] {
  const finalizations = parseProofPassFinalizeLogs(params.logs);
  if (finalizations.length === 0) {
    return [];
  }

  const matches = new Map<string, ProofPassFeedbackMatch>();
  for (const finalize of finalizations) {
    const feedbackHash = finalize.sealHash.toLowerCase();
    const match: ProofPassFeedbackMatch = {
      id: `${finalize.asset}:${finalize.clientAddress}:${finalize.feedbackIndex}:${params.signature}`,
      asset: finalize.asset,
      clientAddress: finalize.clientAddress,
      feedbackIndex: finalize.feedbackIndex,
      txSignature: params.signature,
      blockSlot: params.slot.toString(),
      feedbackHash,
      proofpassSession: finalize.sessionHex,
      contextType: finalize.contextType,
      contextRefHash: finalize.contextRefHash,
    };
    matches.set(
      proofPassLookupKey(
        match.asset,
        match.clientAddress,
        match.feedbackIndex,
        match.txSignature,
        match.feedbackHash
      ),
      match
    );
  }

  return Array.from(matches.values());
}

export function getProofPassProgramKey(): PublicKey {
  return new PublicKey(PROOFPASS_PROGRAM_ID);
}

function matchParsedProofPassFinalizations(
  finalizations: ProofPassFinalizeLog[],
  feedbacks: ProofPassFeedbackCandidate[],
  signature: string
): {
  matches: ProofPassFeedbackMatch[];
  matchedFinalizationKeys: Set<string>;
} {
  const finalizationByKey = new Map<string, ProofPassFinalizeLog>();
  for (const finalize of finalizations) {
    finalizationByKey.set(
      proofPassMatchKey(
        finalize.asset,
        finalize.clientAddress,
        finalize.feedbackIndex,
        finalize.sealHash
      ),
      finalize
    );
  }

  const matches: ProofPassFeedbackMatch[] = [];
  const matchedFinalizationKeys = new Set<string>();
  for (const feedback of feedbacks) {
    const finalizationKey = proofPassMatchKey(
      feedback.asset,
      feedback.clientAddress,
      feedback.feedbackIndex,
      feedback.feedbackHash
    );
    const finalize = finalizationByKey.get(finalizationKey);
    if (!finalize) continue;
    matchedFinalizationKeys.add(finalizationKey);

    matches.push({
      id: `${feedback.asset}:${feedback.clientAddress}:${feedback.feedbackIndex}:${signature}`,
      asset: feedback.asset,
      clientAddress: feedback.clientAddress,
      feedbackIndex: feedback.feedbackIndex,
      txSignature: feedback.txSignature,
      blockSlot: feedback.blockSlot,
      feedbackHash: feedback.feedbackHash,
      proofpassSession: finalize.sessionHex,
      contextType: finalize.contextType,
      contextRefHash: finalize.contextRefHash,
    });
  }

  return { matches, matchedFinalizationKeys };
}

export function proofPassLookupKey(
  asset: string,
  clientAddress: string,
  feedbackIndex: string,
  txSignature: string,
  feedbackHash: string
): string {
  return `${asset}:${clientAddress}:${feedbackIndex}:${txSignature}:${feedbackHash.toLowerCase()}`;
}

function proofPassMatchKey(
  asset: string,
  clientAddress: string,
  feedbackIndex: string,
  feedbackHash: string
): string {
  return `${asset}:${clientAddress}:${feedbackIndex}:${feedbackHash.toLowerCase()}`;
}

function normalizeHex32(value: string | undefined): string | null {
  if (!value) return null;
  const normalized = value.trim().toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(normalized)) {
    return null;
  }
  return normalized;
}

function hex32ToPublicKeyBase58(value: string): string {
  return new PublicKey(Buffer.from(value, "hex")).toBase58();
}

function isProofPassValidationEnabled(): boolean {
  const raw = process.env.ENABLE_PROOFPASS;
  return raw !== undefined && /^(1|true|yes|on)$/i.test(raw.trim());
}

function normalizeProofPassProgramId(value: string, validate: boolean): string {
  if (!validate) {
    return value;
  }
  return new PublicKey(value).toBase58();
}

function popExecutionFrame(executionStack: string[], programId: string): void {
  if (executionStack.length === 0) {
    return;
  }

  if (executionStack[executionStack.length - 1] === programId) {
    executionStack.pop();
    return;
  }

  const recoveryIndex = executionStack.lastIndexOf(programId);
  if (recoveryIndex !== -1) {
    executionStack.length = recoveryIndex;
  }
}
