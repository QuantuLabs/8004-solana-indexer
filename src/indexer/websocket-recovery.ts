import {
  Connection,
  PublicKey,
  ConfirmedSignatureInfo,
} from "@solana/web3.js";
import { PrismaClient } from "@prisma/client";
import { config } from "../config.js";
import { matchProofPassFeedbacks } from "../extras/proofpass.js";
import { upsertProofPassMatches } from "../db/proofpass.js";
import type { NewFeedback, ProgramEvent } from "../parser/types.js";
import { Poller } from "./poller.js";

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

export async function recoverMissingProgramSignatures(params: {
  connection: Connection;
  prisma: PrismaClient | null;
  programId: PublicKey;
  signatures: ConfirmedSignatureInfo[];
  isActive: () => boolean;
}): Promise<number> {
  if (!params.isActive()) {
    return 0;
  }
  const poller = new Poller({
    connection: params.connection,
    prisma: params.prisma,
    programId: params.programId,
    pollingInterval: config.pollingInterval,
    batchSize: Math.max(1, params.signatures.length),
  });
  return await poller.replaySignatures(
    params.signatures.filter((signature) => signature.err === null),
    { advanceCursor: false, suppressEventLogWrites: true }
  );
}
