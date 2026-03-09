import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { PrismaClient } from "@prisma/client";
import {
  Connection,
  Context,
  Logs,
  ParsedTransactionWithMeta,
  PublicKey,
  type ConfirmedSignatureInfo,
} from "@solana/web3.js";
import { beforeAll, describe, expect, it } from "vitest";
import { resetLocalDerivedDigestsForTests } from "../../src/db/handlers.js";
import { Processor } from "../../src/indexer/processor.js";
import { Poller } from "../../src/indexer/poller.js";
import { WebSocketIndexer, testWebSocketConnection } from "../../src/indexer/websocket.js";
import { parseTransaction } from "../../src/parser/decoder.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const repoRoot = resolve(__dirname, "../..");

const rpcUrl =
  process.env.HELIUS_DEVNET_URL ||
  process.env.DEVNET_RPC_URL ||
  process.env.RPC_URL ||
  "https://api.devnet.solana.com";
const wsUrl =
  process.env.WS_URL ||
  (rpcUrl.startsWith("https://") ? rpcUrl.replace("https://", "wss://") : "wss://api.devnet.solana.com");
const programId = new PublicKey(process.env.PROGRAM_ID || "8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C");

const DISCOVERY_LIMIT = parseInt(process.env.DEVNET_MODE_PARITY_DISCOVERY_LIMIT || "120", 10);
const TARGET_TX_COUNT = parseInt(process.env.DEVNET_MODE_PARITY_TARGET_TXS || "8", 10);
const RPC_RETRY_LIMIT = parseInt(process.env.DEVNET_MODE_PARITY_RPC_RETRIES || "6", 10);
const RPC_RETRY_BASE_MS = parseInt(process.env.DEVNET_MODE_PARITY_RPC_RETRY_BASE_MS || "400", 10);

type ReplayEntry = {
  signatureInfo: ConfirmedSignatureInfo;
  parsedTx: ParsedTransactionWithMeta;
  txIndex: number | null;
  eventCount: number;
};

type RpcCounters = {
  getSlot: number;
  getSignaturesForAddress: number;
  getParsedTransaction: number;
  getBlock: number;
  onLogs: number;
  removeOnLogsListener: number;
};

type ReplayResult = {
  snapshot: unknown;
  counters: RpcCounters;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor<T>(
  label: string,
  fn: () => Promise<T>,
  predicate: (value: T) => boolean,
  timeoutMs = 30000,
  intervalMs = 250
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let lastValue: T | null = null;

  while (Date.now() < deadline) {
    const value = await fn();
    lastValue = value;
    if (predicate(value)) {
      return value;
    }
    await sleep(intervalMs);
  }

  throw new Error(`[${label}] timed out after ${timeoutMs}ms with last value ${JSON.stringify(normalize(lastValue))}`);
}

function isRetryableRpcError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /429|Too Many Requests|503|504|ECONNRESET|ETIMEDOUT|fetch failed|socket hang up/i.test(message);
}

async function withRpcRetry<T>(label: string, fn: () => Promise<T>, maxRetries = RPC_RETRY_LIMIT): Promise<T> {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (attempt === maxRetries || !isRetryableRpcError(error)) {
        throw error;
      }
      const delayMs = RPC_RETRY_BASE_MS * (attempt + 1);
      console.log(`[devnet-mode-parity] retry ${attempt + 1}/${maxRetries} for ${label} after ${delayMs}ms`);
      await sleep(delayMs);
    }
  }
  throw new Error(`unreachable retry state for ${label}`);
}

function migrateFreshSqlite(dbPath: string): void {
  execFileSync("zsh", ["-lc", "bunx prisma migrate deploy"], {
    cwd: repoRoot,
    env: { ...process.env, DATABASE_URL: `file:${dbPath}` },
    stdio: "pipe",
  });
}

async function withTempPrisma<T>(prefix: string, run: (prisma: PrismaClient) => Promise<T>): Promise<T> {
  const dir = mkdtempSync(resolve(tmpdir(), `${prefix}-`));
  const dbPath = resolve(dir, "test.db");
  const prisma = new PrismaClient({
    datasources: {
      db: {
        url: `file:${dbPath}`,
      },
    },
  });

  try {
    migrateFreshSqlite(dbPath);
    resetLocalDerivedDigestsForTests(false);
    await prisma.$connect();
    return await run(prisma);
  } finally {
    resetLocalDerivedDigestsForTests(false);
    await prisma.$disconnect();
    if (existsSync(dir)) {
      rmSync(dir, { recursive: true, force: true });
    }
  }
}

function createInstrumentedConnection(base: Connection): { connection: Connection; counters: RpcCounters } {
  const counters: RpcCounters = {
    getSlot: 0,
    getSignaturesForAddress: 0,
    getParsedTransaction: 0,
    getBlock: 0,
    onLogs: 0,
    removeOnLogsListener: 0,
  };

  const connection = {
    getSlot: async (...args: Parameters<Connection["getSlot"]>) => {
      counters.getSlot += 1;
      return withRpcRetry("getSlot", () => base.getSlot(...args));
    },
    getSignaturesForAddress: async (...args: Parameters<Connection["getSignaturesForAddress"]>) => {
      counters.getSignaturesForAddress += 1;
      return withRpcRetry("getSignaturesForAddress", () => base.getSignaturesForAddress(...args));
    },
    getParsedTransaction: async (...args: Parameters<Connection["getParsedTransaction"]>) => {
      counters.getParsedTransaction += 1;
      return withRpcRetry("getParsedTransaction", () => base.getParsedTransaction(...args));
    },
    getBlock: async (...args: Parameters<Connection["getBlock"]>) => {
      counters.getBlock += 1;
      return withRpcRetry("getBlock", () => base.getBlock(...args));
    },
    onLogs: (...args: Parameters<Connection["onLogs"]>) => {
      counters.onLogs += 1;
      return base.onLogs(...args);
    },
    removeOnLogsListener: async (...args: Parameters<Connection["removeOnLogsListener"]>) => {
      counters.removeOnLogsListener += 1;
      return withRpcRetry("removeOnLogsListener", () => base.removeOnLogsListener(...args));
    },
  } as unknown as Connection;

  return { connection, counters };
}

function canonicalSort(entries: ReplayEntry[]): ReplayEntry[] {
  return [...entries].sort((a, b) => {
    if (a.signatureInfo.slot !== b.signatureInfo.slot) {
      return a.signatureInfo.slot - b.signatureInfo.slot;
    }
    const txA = a.txIndex ?? Number.MAX_SAFE_INTEGER;
    const txB = b.txIndex ?? Number.MAX_SAFE_INTEGER;
    if (txA !== txB) {
      return txA - txB;
    }
    return a.signatureInfo.signature.localeCompare(b.signatureInfo.signature);
  });
}

function reverseWithinSlots(entries: ReplayEntry[]): ReplayEntry[] {
  const grouped = new Map<number, ReplayEntry[]>();
  for (const entry of canonicalSort(entries)) {
    const slot = entry.signatureInfo.slot;
    if (!grouped.has(slot)) grouped.set(slot, []);
    grouped.get(slot)!.push(entry);
  }

  return [...grouped.entries()]
    .sort((a, b) => a[0] - b[0])
    .flatMap(([, slotEntries]) => [...slotEntries].reverse());
}

function normalize(value: unknown): unknown {
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (value instanceof Uint8Array || Buffer.isBuffer(value)) {
    return Buffer.from(value).toString("hex");
  }
  if (Array.isArray(value)) {
    return value.map((item) => normalize(item));
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, item]) => [key, normalize(item)])
    );
  }
  return value;
}

async function snapshotCore(prisma: PrismaClient): Promise<unknown> {
  const [
    agents,
    collections,
    metadata,
    feedbacks,
    orphanFeedbacks,
    responses,
    orphanResponses,
    revocations,
    validations,
    registries,
    indexerState,
    agentDigestCache,
    hashChainCheckpoints,
  ] = await Promise.all([
    prisma.agent.findMany({ orderBy: [{ createdSlot: "asc" }, { txIndex: "asc" }, { eventOrdinal: "asc" }, { id: "asc" }] }),
    prisma.collection.findMany({ orderBy: [{ col: "asc" }, { creator: "asc" }] }),
    prisma.agentMetadata.findMany({ orderBy: [{ agentId: "asc" }, { key: "asc" }] }),
    prisma.feedback.findMany({ orderBy: [{ agentId: "asc" }, { client: "asc" }, { feedbackIndex: "asc" }] }),
    prisma.orphanFeedback.findMany({ orderBy: [{ agentId: "asc" }, { client: "asc" }, { feedbackIndex: "asc" }] }),
    prisma.feedbackResponse.findMany({ orderBy: [{ feedbackId: "asc" }, { responder: "asc" }, { txSignature: "asc" }] }),
    prisma.orphanResponse.findMany({ orderBy: [{ agentId: "asc" }, { client: "asc" }, { feedbackIndex: "asc" }, { responder: "asc" }] }),
    prisma.revocation.findMany({ orderBy: [{ agentId: "asc" }, { client: "asc" }, { feedbackIndex: "asc" }] }),
    prisma.validation.findMany({ orderBy: [{ agentId: "asc" }, { validator: "asc" }, { nonce: "asc" }] }),
    prisma.registry.findMany({ orderBy: [{ id: "asc" }] }),
    prisma.indexerState.findUnique({ where: { id: "main" } }),
    prisma.agentDigestCache.findMany({ orderBy: [{ agentId: "asc" }] }),
    prisma.hashChainCheckpoint.findMany({ orderBy: [{ agentId: "asc" }, { chainType: "asc" }, { eventCount: "asc" }] }),
  ]);

  return normalize({
    agents,
    collections,
    metadata,
    feedbacks,
    orphanFeedbacks,
    responses,
    orphanResponses,
    revocations,
    validations,
    registries,
    indexerState: indexerState
      ? {
          lastSignature: indexerState.lastSignature,
          lastSlot: indexerState.lastSlot,
          lastTxIndex: indexerState.lastTxIndex,
        }
      : null,
    agentDigestCache,
    hashChainCheckpoints,
  });
}

async function seedCursor(prisma: PrismaClient, entry: ReplayEntry): Promise<void> {
  await prisma.indexerState.upsert({
    where: { id: "main" },
    update: {
      lastSignature: entry.signatureInfo.signature,
      lastSlot: BigInt(entry.signatureInfo.slot),
      lastTxIndex: entry.txIndex,
      source: "poller",
    },
    create: {
      id: "main",
      lastSignature: entry.signatureInfo.signature,
      lastSlot: BigInt(entry.signatureInfo.slot),
      lastTxIndex: entry.txIndex,
      source: "poller",
    },
  });
}

async function countIndexedRows(prisma: PrismaClient): Promise<number> {
  const [agents, feedbacks, responses, revocations, validations, registries] = await Promise.all([
    prisma.agent.count(),
    prisma.feedback.count(),
    prisma.feedbackResponse.count(),
    prisma.revocation.count(),
    prisma.validation.count(),
    prisma.registry.count(),
  ]);

  return agents + feedbacks + responses + revocations + validations + registries;
}

async function runProcessorModeSmoke(mode: "polling" | "websocket" | "auto", entries: ReplayEntry[]) {
  const seedEntry = entries[0];

  return withTempPrisma(`idx-mode-processor-${mode}`, async (prisma) => {
    await seedCursor(prisma, seedEntry);

    const processor = new Processor(prisma, null, { mode });

    try {
      await processor.start();

      await waitFor(
        `processor-${mode}-progress`,
        async () => {
          const [state, totalRows] = await Promise.all([
            prisma.indexerState.findUnique({ where: { id: "main" } }),
            countIndexedRows(prisma),
          ]);
          return {
            status: processor.getStatus(),
            state,
            totalRows,
          };
        },
        ({ state, totalRows }) =>
          Boolean(state?.lastSignature && state.lastSignature !== seedEntry.signatureInfo.signature && totalRows > 0),
        40000,
      );

      return {
        status: processor.getStatus(),
        totalRows: await countIndexedRows(prisma),
        state: await prisma.indexerState.findUnique({ where: { id: "main" } }),
      };
    } finally {
      await processor.stop();
    }
  });
}

async function fetchReplayWindow(): Promise<{ entries: ReplayEntry[]; multiSlotCount: number }> {
  const connection = new Connection(rpcUrl, "confirmed");
  const signatures = await withRpcRetry("window:getSignaturesForAddress", () =>
    connection.getSignaturesForAddress(programId, { limit: DISCOVERY_LIMIT })
  );
  const validSignatures = signatures.filter((signature) => signature.err === null);

  const slotCounts = new Map<number, number>();
  for (const signature of validSignatures) {
    slotCounts.set(signature.slot, (slotCounts.get(signature.slot) || 0) + 1);
  }

  const prioritized = [...validSignatures].sort((a, b) => {
    const aMulti = (slotCounts.get(a.slot) || 0) > 1 ? 0 : 1;
    const bMulti = (slotCounts.get(b.slot) || 0) > 1 ? 0 : 1;
    if (aMulti !== bMulti) return aMulti - bMulti;
    if (a.slot !== b.slot) return b.slot - a.slot;
    return a.signature.localeCompare(b.signature);
  });

  const selected: Array<{
    signatureInfo: ConfirmedSignatureInfo;
    parsedTx: ParsedTransactionWithMeta;
    eventCount: number;
  }> = [];

  for (const signatureInfo of prioritized) {
    const parsedTx = await withRpcRetry(`window:getParsedTransaction:${signatureInfo.signature}`, () =>
      connection.getParsedTransaction(signatureInfo.signature, {
        maxSupportedTransactionVersion: 0,
      })
    );

    if (!parsedTx?.meta?.logMessages) {
      await sleep(80);
      continue;
    }

    const parsed = parseTransaction(parsedTx);
    if (!parsed || parsed.events.length === 0) {
      await sleep(80);
      continue;
    }

    selected.push({
      signatureInfo,
      parsedTx,
      eventCount: parsed.events.length,
    });

    if (selected.length >= TARGET_TX_COUNT) {
      break;
    }

    await sleep(80);
  }

  if (selected.length < Math.min(TARGET_TX_COUNT, 4)) {
    throw new Error(`Expected at least 4 parseable devnet transactions, got ${selected.length}`);
  }

  const bySlot = new Map<number, typeof selected>();
  for (const entry of selected) {
    const slot = entry.signatureInfo.slot;
    if (!bySlot.has(slot)) bySlot.set(slot, []);
    bySlot.get(slot)!.push(entry);
  }

  const txIndexBySignature = new Map<string, number | null>();
  for (const [slot, slotEntries] of bySlot.entries()) {
    const block = await withRpcRetry(`window:getBlock:${slot}`, () =>
      connection.getBlock(slot, {
        maxSupportedTransactionVersion: 0,
        transactionDetails: "full",
      })
    );

    if (!block?.transactions) {
      for (const entry of slotEntries) {
        txIndexBySignature.set(entry.signatureInfo.signature, null);
      }
      continue;
    }

    for (const entry of slotEntries) {
      const txIndex = block.transactions.findIndex(
        (transaction) => transaction.transaction.signatures[0] === entry.signatureInfo.signature
      );
      txIndexBySignature.set(entry.signatureInfo.signature, txIndex >= 0 ? txIndex : null);
    }

    await sleep(80);
  }

  const entries = canonicalSort(
    selected.map((entry) => ({
      ...entry,
      txIndex: txIndexBySignature.get(entry.signatureInfo.signature) ?? null,
    }))
  );

  const multiSlotCount = [...bySlot.values()].filter((entriesForSlot) => entriesForSlot.length > 1).length;
  return { entries, multiSlotCount };
}

async function replayPolling(entries: ReplayEntry[]): Promise<ReplayResult> {
  return withTempPrisma("idx-mode-polling", async (prisma) => {
    const { connection, counters } = createInstrumentedConnection(new Connection(rpcUrl, "confirmed"));
    const poller = new Poller({
      connection,
      prisma,
      programId,
    });

    for (const entry of entries) {
      await (poller as any).processTransaction(
        entry.signatureInfo,
        entry.txIndex === null ? undefined : entry.txIndex
      );
    }

    return {
      snapshot: await snapshotCore(prisma),
      counters,
    };
  });
}

async function replayWebsocket(entries: ReplayEntry[]): Promise<ReplayResult> {
  return withTempPrisma("idx-mode-websocket", async (prisma) => {
    const { connection, counters } = createInstrumentedConnection(new Connection(rpcUrl, {
      wsEndpoint: wsUrl,
      commitment: "confirmed",
    }));
    const wsIndexer = new WebSocketIndexer({
      connection,
      prisma,
      programId,
    });

    for (const entry of entries) {
      await (wsIndexer as any).handleLogs(
        {
          err: null,
          logs: entry.parsedTx.meta?.logMessages || [],
          signature: entry.signatureInfo.signature,
        } as Logs,
        { slot: entry.signatureInfo.slot } as Context
      );
    }

    return {
      snapshot: await snapshotCore(prisma),
      counters,
    };
  });
}

async function replayAutoOverlap(entries: ReplayEntry[]): Promise<ReplayResult> {
  return withTempPrisma("idx-mode-auto", async (prisma) => {
    const { connection, counters } = createInstrumentedConnection(new Connection(rpcUrl, {
      wsEndpoint: wsUrl,
      commitment: "confirmed",
    }));
    const poller = new Poller({
      connection,
      prisma,
      programId,
    });
    const wsIndexer = new WebSocketIndexer({
      connection,
      prisma,
      programId,
    });

    const splitIndex = Math.max(1, Math.floor(entries.length * 0.6));
    const bootstrapEntries = entries.slice(0, splitIndex);
    const overlapEntries = entries.slice(splitIndex);

    for (const entry of bootstrapEntries) {
      await (poller as any).processTransaction(
        entry.signatureInfo,
        entry.txIndex === null ? undefined : entry.txIndex
      );
    }

    for (const entry of overlapEntries) {
      await (wsIndexer as any).handleLogs(
        {
          err: null,
          logs: entry.parsedTx.meta?.logMessages || [],
          signature: entry.signatureInfo.signature,
        } as Logs,
        { slot: entry.signatureInfo.slot } as Context
      );
    }

    for (const entry of overlapEntries) {
      await (poller as any).processTransaction(
        entry.signatureInfo,
        entry.txIndex === null ? undefined : entry.txIndex
      );
    }

    return {
      snapshot: await snapshotCore(prisma),
      counters,
    };
  });
}

describe.sequential("E2E: Devnet mode parity", () => {
  let replayEntries: ReplayEntry[] = [];
  let multiSlotCount = 0;
  let wsProbeAvailable = false;

  beforeAll(async () => {
    const connection = new Connection(rpcUrl, "confirmed");
    const slot = await withRpcRetry("preflight:getSlot", () => connection.getSlot());
    console.log(`[devnet-mode-parity] connected to ${rpcUrl} at slot ${slot}`);

    const replayWindow = await fetchReplayWindow();
    replayEntries = replayWindow.entries;
    multiSlotCount = replayWindow.multiSlotCount;
    wsProbeAvailable = await testWebSocketConnection(rpcUrl, wsUrl, programId.toBase58());

    console.log("[devnet-mode-parity] replay window", {
      entries: replayEntries.length,
      multiSlotCount,
      slots: [...new Set(replayEntries.map((entry) => entry.signatureInfo.slot))],
      signatures: replayEntries.map((entry) => ({
        signature: entry.signatureInfo.signature,
        slot: entry.signatureInfo.slot,
        txIndex: entry.txIndex,
        eventCount: entry.eventCount,
      })),
    });
  }, 120000);

  it("captures a bounded devnet window with parseable transactions", () => {
    expect(replayEntries.length).toBeGreaterThanOrEqual(4);
    expect(replayEntries.every((entry) => entry.eventCount > 0)).toBe(true);
    expect(replayEntries.every((entry) => entry.parsedTx.meta?.logMessages?.length)).toBe(true);
  });

  it("probes a live websocket logs subscription against devnet", async () => {
    expect(wsProbeAvailable).toBe(true);
  }, 30000);

  it("starts and stops a live websocket indexer subscription against devnet", async () => {
    await withTempPrisma("idx-mode-websocket-live", async (prisma) => {
      const connection = new Connection(rpcUrl, {
        wsEndpoint: wsUrl,
        commitment: "confirmed",
      });
      const wsIndexer = new WebSocketIndexer({
        connection,
        prisma,
        programId,
      });

      await wsIndexer.start();
      expect(wsIndexer.isActive()).toBe(true);

      await wsIndexer.stop();
      expect(wsIndexer.isActive()).toBe(false);
    });
  }, 30000);

  it("processor polling mode catches up from a recent saved cursor on devnet", async () => {
    const result = await runProcessorModeSmoke("polling", replayEntries);

    expect(result.status.configuredMode).toBe("polling");
    expect(result.status.mode).toBe("polling");
    expect(result.status.pollerActive).toBe(true);
    expect(result.status.wsActive).toBe(false);
    expect(result.totalRows).toBeGreaterThan(0);
    expect(result.state?.lastSignature).not.toBe(replayEntries[0].signatureInfo.signature);
  }, 60000);

  it("processor websocket mode performs catch-up and keeps a live websocket transport on devnet", async () => {
    expect(wsProbeAvailable).toBe(true);

    const result = await runProcessorModeSmoke("websocket", replayEntries);

    expect(result.status.configuredMode).toBe("websocket");
    expect(result.status.mode).toBe("websocket");
    expect(result.status.wsActive).toBe(true);
    expect(result.status.pollerActive).toBe(false);
    expect(result.totalRows).toBeGreaterThan(0);
    expect(result.state?.lastSignature).not.toBe(replayEntries[0].signatureInfo.signature);
  }, 60000);

  it("processor auto mode prefers websocket on devnet after catch-up", async () => {
    expect(wsProbeAvailable).toBe(true);

    const result = await runProcessorModeSmoke("auto", replayEntries);

    expect(result.status.configuredMode).toBe("auto");
    expect(result.status.mode).toBe("websocket");
    expect(result.status.wsActive).toBe(true);
    expect(result.status.pollerActive).toBe(false);
    expect(result.totalRows).toBeGreaterThan(0);
    expect(result.state?.lastSignature).not.toBe(replayEntries[0].signatureInfo.signature);
  }, 60000);

  it("polling and websocket produce identical core snapshots on the same devnet window", async () => {
    const polling = await replayPolling(replayEntries);
    const websocket = await replayWebsocket(replayEntries);

    expect(websocket.snapshot).toEqual(polling.snapshot);

    console.log("[devnet-mode-parity] polling counters", polling.counters);
    console.log("[devnet-mode-parity] websocket counters", websocket.counters);
  }, 120000);

  it("auto overlap converges to the same core snapshot as polling", async () => {
    const polling = await replayPolling(replayEntries);
    const auto = await replayAutoOverlap(replayEntries);

    expect(auto.snapshot).toEqual(polling.snapshot);

    console.log("[devnet-mode-parity] auto counters", auto.counters);
  }, 120000);

  it("websocket same-slot reverse-order replay preserves full local parity", async () => {
    if (multiSlotCount === 0) {
      console.log("[devnet-mode-parity] skipping reverse-order assertion; no multi-slot window available");
      return;
    }

    const polling = await replayPolling(replayEntries);
    const websocketReordered = await replayWebsocket(reverseWithinSlots(replayEntries));

    expect(websocketReordered.snapshot).toEqual(polling.snapshot);
  }, 120000);
});
