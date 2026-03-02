import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { PrismaClient } from "@prisma/client";

type JsonRecord = Record<string, unknown>;

type AgentAction = {
  asset: string;
  txSignature: string;
};

type FeedbackAction = {
  asset: string;
  client: string;
  feedbackIndex: string;
  sealHash: string;
  txSignature: string;
};

type ResponseAction = {
  asset: string;
  client: string;
  feedbackIndex: string;
  responder: string;
  sealHash: string;
  txSignature: string;
};

type RevocationAction = {
  asset: string;
  client: string;
  feedbackIndex: string;
  sealHash: string;
  txSignature: string;
};

type CliOptions = {
  jsonlPath: string;
  outPath: string;
  databaseUrl: string;
  timeoutMs: number;
  pollMs: number;
};

type ParseOutput = {
  runId: string | null;
  lineCount: number;
  parsedCount: number;
  malformedLines: number;
  unmatchedResponsePayloads: number;
  unresolvedResponseClients: string[];
  agents: AgentAction[];
  feedbacks: FeedbackAction[];
  responses: ResponseAction[];
  revocations: RevocationAction[];
};

type DeterministicResult = {
  expected: number;
  indexed: number;
  missingKeys: string[];
  txSignatureMismatches: Array<{ key: string; expected: string; actual: string | null }>;
};

const DEFAULT_TIMEOUT_MS = 180_000;
const DEFAULT_POLL_MS = 2_000;

function usage(): string {
  return [
    "Usage:",
    "  bunx tsx scripts/localnet-websocket-integrity-report.ts \\",
    "    --jsonl /abs/path/actions.jsonl \\",
    "    --out /abs/path/integrity-report.json \\",
    "    [--database-url file:/abs/path/localnet.db] \\",
    `    [--timeout-ms ${DEFAULT_TIMEOUT_MS}] [--poll-ms ${DEFAULT_POLL_MS}]`,
  ].join("\n");
}

function getString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function getNumber(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

function parseArgs(argv: string[]): CliOptions {
  const argMap = new Map<string, string>();
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (!arg.startsWith("--")) continue;
    const [k, v] = arg.includes("=") ? arg.split("=", 2) : [arg, argv[i + 1]];
    if (!arg.includes("=") && v && !v.startsWith("--")) {
      argMap.set(k, v);
      i++;
      continue;
    }
    if (arg.includes("=")) {
      argMap.set(k, v ?? "");
    } else {
      argMap.set(k, "");
    }
  }

  const jsonlPath = argMap.get("--jsonl");
  const outPath = argMap.get("--out");
  const databaseUrl = argMap.get("--database-url") || process.env.DATABASE_URL;

  if (!jsonlPath || !outPath || !databaseUrl) {
    throw new Error(`${usage()}\n\nMissing required args (--jsonl, --out, database URL).`);
  }

  return {
    jsonlPath: path.resolve(jsonlPath),
    outPath: path.resolve(outPath),
    databaseUrl,
    timeoutMs: getNumber(argMap.get("--timeout-ms"), DEFAULT_TIMEOUT_MS),
    pollMs: getNumber(argMap.get("--poll-ms"), DEFAULT_POLL_MS),
  };
}

function actionKeyFeedback(asset: string, client: string, feedbackIndex: string): string {
  return `${asset}:${client}:${feedbackIndex}`;
}

function actionKeyResponse(
  asset: string,
  client: string,
  feedbackIndex: string,
  responder: string,
  txSignature: string
): string {
  return `${asset}:${client}:${feedbackIndex}:${responder}:${txSignature}`;
}

function parseJsonlInput(jsonlPath: string): ParseOutput {
  const raw = fs.readFileSync(jsonlPath, "utf8");
  const lines = raw.split(/\r?\n/).filter((line) => line.trim().length > 0);

  const agentsBySig = new Map<string, AgentAction>();
  const feedbackBySig = new Map<string, FeedbackAction>();
  const responsesBySig = new Map<string, ResponseAction>();
  const revocationsBySig = new Map<string, RevocationAction>();

  const feedbackClientByAssetIndexSeal = new Map<string, string[]>();
  const pendingResponsePayloads = new Map<string, string[]>();
  const unresolvedResponseClients: string[] = [];

  let runId: string | null = null;
  let parsedCount = 0;
  let malformedLines = 0;

  for (const line of lines) {
    let rec: JsonRecord;
    try {
      rec = JSON.parse(line) as JsonRecord;
      parsedCount++;
    } catch {
      malformedLines++;
      continue;
    }

    if (!runId) {
      runId = getString(rec.run_id);
    }

    const phase = getString(rec.phase);
    const event = getString(rec.event);

    if (phase === "response" && event === "payload") {
      const responder = getString(rec.wallet);
      const asset = getString(rec.asset);
      const feedbackIndex = getString(rec.feedback_index);
      const sealHash = getString(rec.seal_hash);
      const payload = rec.payload as JsonRecord | undefined;
      const client = payload ? getString(payload.client) : null;

      if (responder && asset && feedbackIndex && sealHash && client) {
        const k = `${responder}|${asset}|${feedbackIndex}|${sealHash}`;
        const queue = pendingResponsePayloads.get(k) ?? [];
        queue.push(client);
        pendingResponsePayloads.set(k, queue);
      }
    }

    if (event !== "result" || rec.success !== true) {
      continue;
    }

    if (phase === "register") {
      const asset = getString(rec.asset);
      const txSignature = getString(rec.tx_signature);
      if (!asset || !txSignature) continue;
      agentsBySig.set(txSignature, { asset, txSignature });
      continue;
    }

    if (phase === "feedback") {
      const asset = getString(rec.asset);
      const client = getString(rec.wallet);
      const feedbackIndex = getString(rec.feedback_index);
      const sealHash = getString(rec.seal_hash);
      const txSignature = getString(rec.tx_signature);
      if (!asset || !client || !feedbackIndex || !sealHash || !txSignature) continue;
      feedbackBySig.set(txSignature, {
        asset,
        client,
        feedbackIndex,
        sealHash,
        txSignature,
      });
      const lookupKey = `${asset}|${feedbackIndex}|${sealHash}`;
      const arr = feedbackClientByAssetIndexSeal.get(lookupKey) ?? [];
      if (!arr.includes(client)) {
        arr.push(client);
      }
      feedbackClientByAssetIndexSeal.set(lookupKey, arr);
      continue;
    }

    if (phase === "response") {
      const asset = getString(rec.asset);
      const responder = getString(rec.wallet);
      const feedbackIndex = getString(rec.feedback_index);
      const sealHash = getString(rec.seal_hash);
      const txSignature = getString(rec.tx_signature);
      if (!asset || !responder || !feedbackIndex || !sealHash || !txSignature) continue;

      let client: string | null = null;
      const payloadLookupKey = `${responder}|${asset}|${feedbackIndex}|${sealHash}`;
      const queue = pendingResponsePayloads.get(payloadLookupKey);
      if (queue && queue.length > 0) {
        client = queue.shift() ?? null;
        if (queue.length === 0) {
          pendingResponsePayloads.delete(payloadLookupKey);
        } else {
          pendingResponsePayloads.set(payloadLookupKey, queue);
        }
      }

      if (!client) {
        const feedbackLookupKey = `${asset}|${feedbackIndex}|${sealHash}`;
        const feedbackClients = feedbackClientByAssetIndexSeal.get(feedbackLookupKey);
        if (feedbackClients && feedbackClients.length === 1) {
          client = feedbackClients[0];
        }
      }

      if (!client) {
        unresolvedResponseClients.push(txSignature);
        continue;
      }

      responsesBySig.set(txSignature, {
        asset,
        client,
        feedbackIndex,
        responder,
        sealHash,
        txSignature,
      });
      continue;
    }

    if (phase === "revoke") {
      const asset = getString(rec.asset);
      const client = getString(rec.wallet);
      const feedbackIndex = getString(rec.feedback_index);
      const sealHash = getString(rec.seal_hash);
      const txSignature = getString(rec.tx_signature);
      if (!asset || !client || !feedbackIndex || !sealHash || !txSignature) continue;
      revocationsBySig.set(txSignature, {
        asset,
        client,
        feedbackIndex,
        sealHash,
        txSignature,
      });
    }
  }

  let unmatchedResponsePayloads = 0;
  for (const queue of pendingResponsePayloads.values()) {
    unmatchedResponsePayloads += queue.length;
  }

  return {
    runId,
    lineCount: lines.length,
    parsedCount,
    malformedLines,
    unmatchedResponsePayloads,
    unresolvedResponseClients,
    agents: Array.from(agentsBySig.values()),
    feedbacks: Array.from(feedbackBySig.values()),
    responses: Array.from(responsesBySig.values()),
    revocations: Array.from(revocationsBySig.values()),
  };
}

function buildDeterministicLookup<T>(
  actions: T[],
  keyFn: (action: T) => string,
  txFn: (action: T) => string
): Map<string, string> {
  const out = new Map<string, string>();
  for (const action of actions) {
    out.set(keyFn(action), txFn(action));
  }
  return out;
}

function evaluateDeterministic(
  expectedByKey: Map<string, string>,
  actualByKey: Map<string, string | null>
): DeterministicResult {
  const missingKeys: string[] = [];
  const txSignatureMismatches: Array<{ key: string; expected: string; actual: string | null }> = [];
  let indexed = 0;

  for (const [key, expectedTx] of expectedByKey.entries()) {
    if (!actualByKey.has(key)) {
      missingKeys.push(key);
      continue;
    }

    indexed++;
    const actualTx = actualByKey.get(key) ?? null;
    if (actualTx !== expectedTx) {
      txSignatureMismatches.push({ key, expected: expectedTx, actual: actualTx });
    }
  }

  return {
    expected: expectedByKey.size,
    indexed,
    missingKeys,
    txSignatureMismatches,
  };
}

async function collectSnapshot(prisma: PrismaClient, parsed: ParseOutput) {
  const expectedAgentByKey = buildDeterministicLookup(
    parsed.agents,
    (a) => a.asset,
    (a) => a.txSignature
  );
  const expectedFeedbackByKey = buildDeterministicLookup(
    parsed.feedbacks,
    (f) => actionKeyFeedback(f.asset, f.client, f.feedbackIndex),
    (f) => f.txSignature
  );
  const expectedResponseByKey = buildDeterministicLookup(
    parsed.responses,
    (r) => actionKeyResponse(r.asset, r.client, r.feedbackIndex, r.responder, r.txSignature),
    (r) => r.txSignature
  );
  const expectedRevocationByKey = buildDeterministicLookup(
    parsed.revocations,
    (r) => actionKeyFeedback(r.asset, r.client, r.feedbackIndex),
    (r) => r.txSignature
  );

  const actualAgentByKey = new Map<string, string | null>();
  if (parsed.agents.length > 0) {
    const rows = await prisma.agent.findMany({
      where: {
        createdTxSignature: {
          in: parsed.agents.map((a) => a.txSignature),
        },
      },
      select: {
        id: true,
        createdTxSignature: true,
      },
    });
    for (const row of rows) {
      actualAgentByKey.set(row.id, row.createdTxSignature ?? null);
    }
  }

  const actualFeedbackByKey = new Map<string, string | null>();
  if (parsed.feedbacks.length > 0) {
    const rows = await prisma.feedback.findMany({
      where: {
        createdTxSignature: {
          in: parsed.feedbacks.map((f) => f.txSignature),
        },
      },
      select: {
        agentId: true,
        client: true,
        feedbackIndex: true,
        createdTxSignature: true,
      },
    });
    for (const row of rows) {
      const key = actionKeyFeedback(row.agentId, row.client, row.feedbackIndex.toString());
      actualFeedbackByKey.set(key, row.createdTxSignature ?? null);
    }
  }

  const actualResponseByKey = new Map<string, string | null>();
  if (parsed.responses.length > 0) {
    const rows = await prisma.feedbackResponse.findMany({
      where: {
        txSignature: {
          in: parsed.responses.map((r) => r.txSignature),
        },
      },
      select: {
        responder: true,
        txSignature: true,
        feedback: {
          select: {
            agentId: true,
            client: true,
            feedbackIndex: true,
          },
        },
      },
    });
    for (const row of rows) {
      const key = actionKeyResponse(
        row.feedback.agentId,
        row.feedback.client,
        row.feedback.feedbackIndex.toString(),
        row.responder,
        row.txSignature ?? ""
      );
      actualResponseByKey.set(key, row.txSignature ?? null);
    }
  }

  const actualRevocationByKey = new Map<string, string | null>();
  if (parsed.revocations.length > 0) {
    const rows = await prisma.revocation.findMany({
      where: {
        txSignature: {
          in: parsed.revocations.map((r) => r.txSignature),
        },
      },
      select: {
        agentId: true,
        client: true,
        feedbackIndex: true,
        txSignature: true,
      },
    });
    for (const row of rows) {
      const key = actionKeyFeedback(row.agentId, row.client, row.feedbackIndex.toString());
      actualRevocationByKey.set(key, row.txSignature ?? null);
    }
  }

  const agents = evaluateDeterministic(expectedAgentByKey, actualAgentByKey);
  const feedbacks = evaluateDeterministic(expectedFeedbackByKey, actualFeedbackByKey);
  const responses = evaluateDeterministic(expectedResponseByKey, actualResponseByKey);
  const revocations = evaluateDeterministic(expectedRevocationByKey, actualRevocationByKey);

  const indexedBySignature = {
    agents:
      parsed.agents.length === 0
        ? 0
        : await prisma.agent.count({
            where: { createdTxSignature: { in: parsed.agents.map((a) => a.txSignature) } },
          }),
    feedbacks:
      parsed.feedbacks.length === 0
        ? 0
        : await prisma.feedback.count({
            where: { createdTxSignature: { in: parsed.feedbacks.map((f) => f.txSignature) } },
          }),
    responses:
      parsed.responses.length === 0
        ? 0
        : await prisma.feedbackResponse.count({
            where: { txSignature: { in: parsed.responses.map((r) => r.txSignature) } },
          }),
    revocations:
      parsed.revocations.length === 0
        ? 0
        : await prisma.revocation.count({
            where: { txSignature: { in: parsed.revocations.map((r) => r.txSignature) } },
          }),
  };

  return {
    deterministic: {
      agents,
      feedbacks,
      responses,
      revocations,
    },
    indexedBySignature,
  };
}

function allMatched(
  deterministic: {
    agents: DeterministicResult;
    feedbacks: DeterministicResult;
    responses: DeterministicResult;
    revocations: DeterministicResult;
  },
  unresolvedResponseClients: string[]
): boolean {
  const checks = Object.values(deterministic);
  return (
    unresolvedResponseClients.length === 0 &&
    checks.every((item) => item.missingKeys.length === 0 && item.txSignatureMismatches.length === 0)
  );
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  fs.mkdirSync(path.dirname(opts.outPath), { recursive: true });

  const parsed = parseJsonlInput(opts.jsonlPath);
  process.env.DATABASE_URL = opts.databaseUrl;
  const prisma = new PrismaClient();

  const start = Date.now();
  let polls = 0;
  let snapshot: Awaited<ReturnType<typeof collectSnapshot>> | null = null;

  try {
    await prisma.$connect();

    while (true) {
      polls++;
      snapshot = await collectSnapshot(prisma, parsed);
      const ok = allMatched(snapshot.deterministic, parsed.unresolvedResponseClients);
      const elapsed = Date.now() - start;
      if (ok) break;
      if (elapsed >= opts.timeoutMs) break;
      await sleep(opts.pollMs);
    }
  } finally {
    await prisma.$disconnect();
  }

  if (!snapshot) {
    throw new Error("No snapshot generated.");
  }

  const expectedCounts = {
    agents: parsed.agents.length,
    feedbacks: parsed.feedbacks.length,
    responses: parsed.responses.length,
    revocations: parsed.revocations.length,
  };

  const success = allMatched(snapshot.deterministic, parsed.unresolvedResponseClients);

  const report = {
    generatedAt: new Date().toISOString(),
    runId: parsed.runId,
    sourceJsonl: opts.jsonlPath,
    databaseUrl: opts.databaseUrl,
    parser: {
      lineCount: parsed.lineCount,
      parsedCount: parsed.parsedCount,
      malformedLines: parsed.malformedLines,
      unmatchedResponsePayloads: parsed.unmatchedResponsePayloads,
      unresolvedResponseClients: parsed.unresolvedResponseClients,
    },
    inputActions: {
      counts: expectedCounts,
      agents: parsed.agents,
      feedbacks: parsed.feedbacks,
      responses: parsed.responses,
      revocations: parsed.revocations,
    },
    comparison: {
      polls,
      timeoutMs: opts.timeoutMs,
      elapsedMs: Date.now() - start,
      indexedBySignature: snapshot.indexedBySignature,
      deterministic: snapshot.deterministic,
      success,
    },
  };

  fs.writeFileSync(opts.outPath, JSON.stringify(report, null, 2));
  console.log(`integrity_report=${opts.outPath}`);
  console.log(`integrity_success=${success ? "1" : "0"}`);

  if (!success) {
    process.exit(1);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
