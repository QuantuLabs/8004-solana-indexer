import {
  Connection,
  PublicKey,
  ConfirmedSignatureInfo,
  ParsedTransactionWithMeta,
} from "@solana/web3.js";
import { PrismaClient } from "@prisma/client";
import { config } from "../config.js";
import { handleEventAtomic, EventContext, suspendLocalDerivedDigests, resumeLocalDerivedDigests } from "../db/handlers.js";
import { getPool } from "../db/supabase.js";
import { matchProofPassFeedbacks } from "../extras/proofpass.js";
import { upsertProofPassMatches } from "../db/proofpass.js";
import { parseTransaction, toTypedEvent, type ParsedEvent } from "../parser/decoder.js";
import type { NewFeedback, ProgramEvent } from "../parser/types.js";
import { compareCanonicalTransactionOrder } from "./canonical-order.js";
import { resolveEventBlockTime } from "./block-time.js";

export function hasProgramDataLogsForProgram(logs: string[], programId: string): boolean {
  const invokeStack: string[] = [];

  for (const log of logs) {
    const invokeMatch = /^Program ([1-9A-HJ-NP-Za-km-z]+) invoke \[\d+\]$/.exec(log);
    if (invokeMatch) {
      invokeStack.push(invokeMatch[1]);
      continue;
    }

    const completionMatch = /^Program ([1-9A-HJ-NP-Za-km-z]+) (success|failed:.*)$/.exec(log);
    if (completionMatch) {
      if (invokeStack[invokeStack.length - 1] === completionMatch[1]) {
        invokeStack.pop();
      }
      continue;
    }

    if (log.startsWith("Program data:") && invokeStack[invokeStack.length - 1] === programId) {
      return true;
    }
  }

  return false;
}

export async function maybeUpsertProofPassMatchesForLogs(params: {
  signature: string;
  slot: bigint;
  logs: string[];
  typedEvents: Array<{ typedEvent: ProgramEvent }>;
}): Promise<void> {
  if (!config.enableProofPass) {
    return;
  }

  const feedbackEvents = params.typedEvents
    .filter((entry): entry is { typedEvent: { type: "NewFeedback"; data: NewFeedback } } =>
      entry.typedEvent.type === "NewFeedback"
    )
    .map((entry) => entry.typedEvent.data);

  const matches = matchProofPassFeedbacks({
    logs: params.logs,
    signature: params.signature,
    slot: params.slot,
    feedbackEvents,
  });
  if (matches.length === 0) {
    return;
  }

  await upsertProofPassMatches(matches);
}

async function findIndexedProgramSignatures(
  prisma: PrismaClient | null,
  signatures: string[]
): Promise<Set<string>> {
  const uniqueSignatures = [...new Set(signatures)];
  const indexed = new Set<string>();
  if (uniqueSignatures.length === 0) {
    return indexed;
  }

  if (prisma) {
    const client = prisma as any;
    const rows = (await Promise.all([
      client.agent.findMany({
        where: { createdTxSignature: { in: uniqueSignatures } },
        select: { createdTxSignature: true },
      }),
      client.agentMetadata.findMany({
        where: { txSignature: { in: uniqueSignatures } },
        select: { txSignature: true },
      }),
      client.feedback.findMany({
        where: { OR: [{ createdTxSignature: { in: uniqueSignatures } }, { revokedTxSignature: { in: uniqueSignatures } }] },
        select: { createdTxSignature: true, revokedTxSignature: true },
      }),
      client.feedbackResponse.findMany({
        where: { txSignature: { in: uniqueSignatures } },
        select: { txSignature: true },
      }),
      client.revocation.findMany({
        where: { txSignature: { in: uniqueSignatures } },
        select: { txSignature: true },
      }),
      client.validation.findMany({
        where: { OR: [{ requestTxSignature: { in: uniqueSignatures } }, { responseTxSignature: { in: uniqueSignatures } }] },
        select: { requestTxSignature: true, responseTxSignature: true },
      }),
      client.registry.findMany({
        where: { txSignature: { in: uniqueSignatures } },
        select: { txSignature: true },
      }),
      client.collection.findMany({
        where: { OR: [{ firstSeenTxSignature: { in: uniqueSignatures } }, { lastSeenTxSignature: { in: uniqueSignatures } }] },
        select: { firstSeenTxSignature: true, lastSeenTxSignature: true },
      }),
      client.orphanFeedback.findMany({
        where: { txSignature: { in: uniqueSignatures } },
        select: { txSignature: true },
      }),
      client.orphanResponse.findMany({
        where: { txSignature: { in: uniqueSignatures } },
        select: { txSignature: true },
      }),
    ])).flat();

    for (const row of rows as Array<Record<string, unknown>>) {
      for (const value of Object.values(row)) {
        if (typeof value === "string") indexed.add(value);
      }
    }
    return indexed;
  }

  const result = await getPool().query(
    `SELECT DISTINCT signature
     FROM (
       SELECT tx_signature AS signature FROM agents WHERE tx_signature = ANY($1::text[])
       UNION SELECT tx_signature FROM metadata WHERE tx_signature = ANY($1::text[])
       UNION SELECT tx_signature FROM feedbacks WHERE tx_signature = ANY($1::text[])
       UNION SELECT tx_signature FROM feedback_responses WHERE tx_signature = ANY($1::text[])
       UNION SELECT tx_signature FROM revocations WHERE tx_signature = ANY($1::text[])
       UNION SELECT tx_signature FROM validations WHERE tx_signature = ANY($1::text[])
       UNION SELECT tx_signature FROM orphan_feedbacks WHERE tx_signature = ANY($1::text[])
       UNION SELECT tx_signature FROM orphan_responses WHERE tx_signature = ANY($1::text[])
       UNION SELECT first_seen_tx_signature FROM collection_pointers WHERE first_seen_tx_signature = ANY($1::text[])
       UNION SELECT last_seen_tx_signature FROM collection_pointers WHERE last_seen_tx_signature = ANY($1::text[])
     ) indexed
     WHERE signature IS NOT NULL`,
    [uniqueSignatures]
  );
  for (const row of result.rows as Array<{ signature?: unknown }>) {
    if (typeof row.signature === "string") indexed.add(row.signature);
  }
  return indexed;
}

export async function findMissingProgramSignatures(
  prisma: PrismaClient | null,
  signatures: ConfirmedSignatureInfo[]
): Promise<ConfirmedSignatureInfo[]> {
  const successful = signatures.filter((signature) => signature.err === null);
  const indexed = await findIndexedProgramSignatures(
    prisma,
    successful.map((signature) => signature.signature)
  );
  return successful.filter((signature) => !indexed.has(signature.signature));
}

export async function recoverMissingProgramSignatures(params: {
  connection: Connection;
  prisma: PrismaClient | null;
  programId: PublicKey;
  signatures: ConfirmedSignatureInfo[];
  isActive: () => boolean;
}): Promise<number> {
  const ordered = [...params.signatures].sort((a, b) => compareCanonicalTransactionOrder(
    { slot: a.slot, txIndex: null, signature: a.signature },
    { slot: b.slot, txIndex: null, signature: b.signature }
  ));
  let recovered = 0;

  for (const signature of ordered) {
    if (!params.isActive()) break;
    const tx = await params.connection.getParsedTransaction(signature.signature, {
      maxSupportedTransactionVersion: config.maxSupportedTransactionVersion,
    });
    if (!tx) {
      throw new Error(`Parsed transaction unavailable for reconciliation recovery: ${signature.signature}`);
    }
    await ingestReconciliationTransaction({ ...params, signature, tx });
    recovered++;
  }

  return recovered;
}

async function ingestReconciliationTransaction(params: {
  prisma: PrismaClient | null;
  programId: PublicKey;
  signature: ConfirmedSignatureInfo;
  tx: ParsedTransactionWithMeta;
  isActive: () => boolean;
}): Promise<void> {
  const blockTime = resolveEventBlockTime(params.signature.blockTime ?? params.tx.blockTime ?? undefined, params.signature.slot);
  const logs = params.tx.meta?.logMessages ?? [];
  const parsed = parseTransaction(params.tx);
  if (!parsed || parsed.events.length === 0) {
    if (hasProgramDataLogsForProgram(logs, params.programId.toBase58())) {
      throw new Error(`Failed to parse program events for reconciliation signature ${params.signature.signature}`);
    }
    return;
  }

  const typedEvents = parsed.events.map((rawEvent: ParsedEvent, eventOrdinal: number) => {
    const typedEvent = toTypedEvent(rawEvent);
    if (!typedEvent) {
      throw new Error(`Failed to convert parsed program event ${rawEvent.name} for reconciliation signature ${params.signature.signature}`);
    }
    return { typedEvent, eventOrdinal, rawEvent };
  });

  const suspendLocalDigests = Boolean(params.prisma && config.dbMode === "local");
  if (suspendLocalDigests) await suspendLocalDerivedDigests();
  try {
    for (const { typedEvent, eventOrdinal } of typedEvents) {
      if (!params.isActive()) return;
      const eventCtx: EventContext = {
        signature: params.signature.signature,
        slot: BigInt(params.signature.slot),
        blockTime,
        txIndex: undefined,
        eventOrdinal,
        source: "websocket",
        skipCursorUpdate: true,
      };
      await handleEventAtomic(params.prisma, typedEvent, eventCtx);
    }
  } finally {
    if (suspendLocalDigests) resumeLocalDerivedDigests();
  }

  await maybeUpsertProofPassMatchesForLogs({
    signature: params.signature.signature,
    slot: BigInt(params.signature.slot),
    logs,
    typedEvents,
  });
}
