import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";
import dotenv from "dotenv";
import { PrismaClient } from "@prisma/client";
import { Connection, PublicKey } from "@solana/web3.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const repoRoot = resolve(__dirname, "..");

const REMOTE_BASE = process.env.REMOTE_BASE || "https://8004-indexer-dev.qnt.sh";
const PAGE_LIMIT = parseInt(process.env.COMPARE_PAGE_LIMIT || "500", 10);
const CUTOFF_MARGIN = parseInt(process.env.COMPARE_CUTOFF_MARGIN || "256", 10);
const START_SLOT = parseInt(process.env.COMPARE_START_SLOT || "0", 10);
const CUTOFF_SLOT_OVERRIDE = parseInt(process.env.COMPARE_CUTOFF_SLOT || "0", 10);
const READY_TIMEOUT_MS = parseInt(process.env.COMPARE_READY_TIMEOUT_MS || "900000", 10);
const CATCHUP_TIMEOUT_MS = parseInt(process.env.COMPARE_CATCHUP_TIMEOUT_MS || "900000", 10);
const POLL_INTERVAL_MS = parseInt(process.env.COMPARE_POLL_INTERVAL_MS || "5000", 10);
const STABLE_POLLS_REQUIRED = parseInt(process.env.COMPARE_STABLE_POLLS_REQUIRED || "7", 10);
const DB_DIR = process.env.COMPARE_DB_DIR
  ? resolve(repoRoot, process.env.COMPARE_DB_DIR)
  : resolve(repoRoot, ".tmp/devnet-mode-matrix");

const MODES = [
  { name: "polling", dbPath: resolve(DB_DIR, "polling.db"), baseUrl: "http://127.0.0.1:3401" },
  { name: "websocket", dbPath: resolve(DB_DIR, "websocket.db"), baseUrl: "http://127.0.0.1:3402" },
  { name: "auto", dbPath: resolve(DB_DIR, "auto.db"), baseUrl: "http://127.0.0.1:3403" },
];

function parseEnvFile(path) {
  return dotenv.parse(readFileSync(path, "utf8"));
}

const monitorEnv = parseEnvFile(resolve(repoRoot, ".env.devnet.monitor.alchemy"));
const exampleEnv = parseEnvFile(resolve(repoRoot, ".env.devnet.example"));
const runtimeEnv = { ...process.env, ...exampleEnv, ...monitorEnv };

const rpcUrl = process.env.COMPARE_RPC_URL || runtimeEnv.DEVNET_RPC_URL || runtimeEnv.RPC_URL;
const programId = process.env.COMPARE_PROGRAM_ID || runtimeEnv.PROGRAM_ID;

if (!rpcUrl || !programId) {
  throw new Error("RPC_URL/DEVNET_RPC_URL and PROGRAM_ID are required");
}

const connection = new Connection(rpcUrl, "confirmed");
const programPublicKey = new PublicKey(programId);

function asText(value) {
  if (value === null || value === undefined) return null;
  return String(value);
}

function asNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function bytesToHex(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    if (value.startsWith("\\x")) return value.slice(2);
    return value;
  }
  if (value instanceof Uint8Array || Buffer.isBuffer(value)) {
    return Buffer.from(value).toString("hex");
  }
  return String(value);
}

function normalizeMetadataValue(value) {
  if (typeof value !== "string") return value ?? null;
  if (value.startsWith("\\x")) {
    return Buffer.from(value.slice(2), "hex").toString("base64");
  }
  return value;
}

function computeKeyHash(key) {
  return createHash("sha256").update(key).digest().subarray(0, 16).toString("hex");
}

function buildUrl(baseUrl, path, params = {}) {
  const url = new URL(path, baseUrl);
  for (const [key, value] of Object.entries(params)) {
    if (value === null || value === undefined || value === "") continue;
    url.searchParams.set(key, String(value));
  }
  return url;
}

function isInSlotRange(value, startSlot, cutoffSlot) {
  if (value === null || value === undefined || value === "") return false;
  const slot = Number(value);
  return Number.isFinite(slot) && slot >= startSlot && slot <= cutoffSlot;
}

async function hasUncoveredSignatureAtOrBelowCutoff(lastSignature, cutoffSlot) {
  let before;

  while (true) {
    let page;
    let attempt = 0;
    while (true) {
      try {
        page = await connection.getSignaturesForAddress(programPublicKey, {
          until: lastSignature || undefined,
          before,
          limit: 1000,
        });
        break;
      } catch (error) {
        attempt += 1;
        if (attempt >= 5) {
          throw error;
        }
        await sleep(500 * attempt);
      }
    }

    if (page.length === 0) {
      return false;
    }

    if (page.some((sig) => sig.slot <= cutoffSlot)) {
      return true;
    }

    before = page[page.length - 1]?.signature;
    if (!before) {
      return false;
    }
  }
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${url}`);
  }
  return res.json();
}

async function waitForReady(baseUrl, timeoutMs = READY_TIMEOUT_MS) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const ready = await fetch(buildUrl(baseUrl, "/ready"));
      if (ready.ok) return;
    } catch {
      // ignore
    }
    await sleep(1000);
  }
  throw new Error(`Timed out waiting for ${baseUrl}/ready`);
}

function createPrisma(dbPath) {
  return new PrismaClient({
    datasources: {
      db: {
        url: `file:${dbPath}`,
      },
    },
  });
}

async function getPendingCounts(prisma, cutoffSlot) {
  const cutoff = BigInt(cutoffSlot);
  const [agents, feedbacks, responses, revocations, metadata] = await Promise.all([
    prisma.agent.count({
      where: {
        status: "PENDING",
        createdSlot: { lte: cutoff },
      },
    }),
    prisma.feedback.count({
      where: {
        status: "PENDING",
        createdSlot: { lte: cutoff },
      },
    }),
    prisma.feedbackResponse.findMany({
      where: { status: "PENDING" },
      select: {
        slot: true,
        feedback: {
          select: {
            createdSlot: true,
          },
        },
      },
    }).then((rows) => rows.filter((row) => Number(row.slot ?? row.feedback.createdSlot ?? 0n) <= cutoffSlot).length),
    prisma.revocation.count({
      where: {
        status: "PENDING",
        slot: { lte: cutoff },
      },
    }),
    prisma.agentMetadata.count({
      where: {
        status: "PENDING",
        slot: { lte: cutoff },
      },
    }),
  ]);

  return {
    agents,
    feedbacks,
    responses,
    revocations,
    metadata,
    total: agents + feedbacks + responses + revocations + metadata,
  };
}

async function getSnapshotCounts(prisma, cutoffSlot) {
  const cutoff = BigInt(cutoffSlot);
  const [agents, feedbacks, responses, revocations, collections, metadata] = await Promise.all([
    prisma.agent.count({
      where: { status: { not: "ORPHANED" }, createdSlot: { lte: cutoff } },
    }),
    prisma.feedback.count({
      where: { status: { not: "ORPHANED" }, createdSlot: { lte: cutoff } },
    }),
    prisma.feedbackResponse.findMany({
      where: { status: { not: "ORPHANED" } },
      select: { slot: true, feedback: { select: { createdSlot: true } } },
    }).then((rows) => rows.filter((row) => Number(row.slot ?? row.feedback.createdSlot ?? 0n) <= cutoffSlot).length),
    prisma.revocation.count({
      where: { status: { not: "ORPHANED" }, slot: { lte: cutoff } },
    }),
    prisma.collection.count({
      where: { firstSeenSlot: { lte: cutoff }, lastSeenSlot: { lte: cutoff } },
    }),
    prisma.agentMetadata.count({
      where: { status: { not: "ORPHANED" }, slot: { lte: cutoff } },
    }),
  ]);

  return { agents, feedbacks, responses, revocations, collections, metadata };
}

async function waitForCatchup(run, cutoffSlot) {
  const prisma = createPrisma(run.dbPath);
  await prisma.$connect();

  try {
    const deadline = Date.now() + CATCHUP_TIMEOUT_MS;
    let stablePolls = 0;
    let lastPendingKey = null;
    let lastSnapshotKey = null;

    while (Date.now() < deadline) {
      const [state, pending, snapshotCounts] = await Promise.all([
        prisma.indexerState.findUnique({ where: { id: "main" } }),
        getPendingCounts(prisma, cutoffSlot),
        getSnapshotCounts(prisma, cutoffSlot),
      ]);

      const lastSlot = Number(state?.lastSlot ?? 0n);
      const hasCoverageGap = await hasUncoveredSignatureAtOrBelowCutoff(state?.lastSignature ?? null, cutoffSlot);
      const pendingKey = JSON.stringify(pending);
      const snapshotKey = JSON.stringify(snapshotCounts);
      if (!hasCoverageGap) {
        stablePolls = pendingKey === lastPendingKey && snapshotKey === lastSnapshotKey ? stablePolls + 1 : 1;
        lastPendingKey = pendingKey;
        lastSnapshotKey = snapshotKey;
      } else {
        stablePolls = 0;
        lastPendingKey = null;
        lastSnapshotKey = null;
      }
      console.log(`[compare-devnet-local-modes] ${run.name} progress`, {
        lastSlot,
        cutoffSlot,
        hasCoverageGap,
        stablePolls,
        pending,
        snapshotCounts,
      });

      if (!hasCoverageGap && stablePolls >= STABLE_POLLS_REQUIRED) {
        return { lastSlot, pending };
      }

      await sleep(POLL_INTERVAL_MS);
    }

    throw new Error(`Timed out waiting for ${run.name} to reach cutoff ${cutoffSlot}`);
  } finally {
    await prisma.$disconnect();
  }
}

async function fetchPagedRemote(path, params = {}) {
  const out = [];
  for (let offset = 0; ; offset += PAGE_LIMIT) {
    const page = await fetchJson(buildUrl(REMOTE_BASE, path, {
      ...params,
      limit: PAGE_LIMIT,
      offset,
    }));
    if (!Array.isArray(page)) {
      throw new Error(`Expected array payload for ${path}`);
    }
    out.push(...page);
    if (page.length < PAGE_LIMIT) break;
  }
  return out;
}

function normalizeAgentRemote(row) {
  return {
    key: row.asset,
    asset: row.asset,
    owner: row.owner ?? null,
    creator: row.creator ?? null,
    agent_uri: row.agent_uri ?? null,
    agent_wallet: row.agent_wallet ?? null,
    collection: row.collection ?? null,
    collection_pointer: row.collection_pointer ?? "",
    col_locked: Boolean(row.col_locked),
    parent_asset: row.parent_asset ?? null,
    parent_creator: row.parent_creator ?? null,
    parent_locked: Boolean(row.parent_locked),
    atom_enabled: Boolean(row.atom_enabled),
    block_slot: asNumber(row.block_slot) ?? 0,
    tx_index: asNumber(row.tx_index),
    event_ordinal: asNumber(row.event_ordinal),
    tx_signature: row.tx_signature ?? null,
  };
}

function normalizeFeedbackRemote(row) {
  return {
    key: `${row.asset}|${row.client_address}|${row.feedback_index}`,
    asset: row.asset,
    client_address: row.client_address,
    feedback_index: asText(row.feedback_index),
    value: asText(row.value),
    value_decimals: asNumber(row.value_decimals) ?? 0,
    score: asNumber(row.score),
    tag1: row.tag1 ?? null,
    tag2: row.tag2 ?? null,
    endpoint: row.endpoint ?? null,
    feedback_uri: row.feedback_uri ?? null,
    feedback_hash: bytesToHex(row.feedback_hash),
    is_revoked: Boolean(row.is_revoked),
    block_slot: asNumber(row.block_slot) ?? 0,
    tx_index: asNumber(row.tx_index),
    event_ordinal: asNumber(row.event_ordinal),
    tx_signature: row.tx_signature ?? null,
  };
}

function normalizeResponseRemote(row) {
  return {
    key: `${row.asset}|${row.client_address}|${row.feedback_index}|${row.responder}|${row.tx_signature ?? ""}`,
    asset: row.asset,
    client_address: row.client_address,
    feedback_index: asText(row.feedback_index),
    responder: row.responder,
    response_uri: row.response_uri ?? null,
    response_hash: bytesToHex(row.response_hash),
    response_count: asText(row.response_count),
    block_slot: asNumber(row.block_slot) ?? 0,
    tx_index: asNumber(row.tx_index),
    event_ordinal: asNumber(row.event_ordinal),
    tx_signature: row.tx_signature ?? null,
  };
}

function normalizeRevocationRemote(row) {
  return {
    key: `${row.asset}|${row.client_address}|${row.feedback_index}`,
    asset: row.asset,
    client_address: row.client_address,
    feedback_index: asText(row.feedback_index),
    feedback_hash: bytesToHex(row.feedback_hash),
    slot: asNumber(row.slot) ?? 0,
    original_score: asNumber(row.original_score),
    atom_enabled: Boolean(row.atom_enabled),
    had_impact: Boolean(row.had_impact),
    revoke_count: asText(row.revoke_count),
    tx_signature: row.tx_signature ?? null,
    tx_index: asNumber(row.tx_index),
    event_ordinal: asNumber(row.event_ordinal),
  };
}

function normalizeCollectionRemote(row) {
  return {
    // collection_id is backend-specific sequential allocation; compare canonical identity only.
    key: `${row.collection}|${row.creator}`,
    collection: row.collection,
    creator: row.creator,
    first_seen_asset: row.first_seen_asset,
    first_seen_slot: asText(row.first_seen_slot),
    last_seen_slot: asText(row.last_seen_slot),
    asset_count: asText(row.asset_count),
  };
}

function normalizeMetadataRemote(row) {
  const blockSlot = asNumber(row.block_slot) ?? 0;
  return {
    key: row.id ?? `${row.asset}:${row.key_hash ?? computeKeyHash(row.key ?? "")}`,
    id: row.id ?? `${row.asset}:${row.key_hash ?? computeKeyHash(row.key ?? "")}`,
    asset: row.asset,
    key_name: row.key,
    key_hash: row.key_hash ?? computeKeyHash(row.key ?? ""),
    value: normalizeMetadataValue(row.value),
    immutable: Boolean(row.immutable),
    block_slot: blockSlot,
    tx_index: asNumber(row.tx_index),
    event_ordinal: asNumber(row.event_ordinal),
    tx_signature: row.tx_signature ?? (blockSlot === 0 ? "uri_derived" : null),
  };
}

async function fetchRemoteSnapshot(startSlot, cutoffSlot) {
  const [agentsRaw, feedbacksRaw, responsesRaw, revocationsRaw, collectionsRaw, metadataRaw] = await Promise.all([
    fetchPagedRemote("/rest/v1/agents", {
      block_slot: `gte.${startSlot}`,
      and: `(block_slot.lte.${cutoffSlot})`,
    }).then((rows) => rows.map(normalizeAgentRemote)),
    fetchPagedRemote("/rest/v1/feedbacks", {
      block_slot: `gte.${startSlot}`,
      and: `(block_slot.lte.${cutoffSlot})`,
    }).then((rows) => rows.map(normalizeFeedbackRemote)),
    fetchPagedRemote("/rest/v1/responses", {
      block_slot: `gte.${startSlot}`,
      and: `(block_slot.lte.${cutoffSlot})`,
    }).then((rows) => rows.map(normalizeResponseRemote)),
    fetchPagedRemote("/rest/v1/revocations", {
      slot: `gte.${startSlot}`,
      and: `(slot.lte.${cutoffSlot})`,
    }).then((rows) => rows.map(normalizeRevocationRemote)),
    fetchPagedRemote("/rest/v1/collections", {
      first_seen_slot: `gte.${startSlot}`,
      and: `(first_seen_slot.lte.${cutoffSlot},last_seen_slot.gte.${startSlot},last_seen_slot.lte.${cutoffSlot})`,
    }).then((rows) => rows.map(normalizeCollectionRemote)),
    fetchPagedRemote("/rest/v1/metadata", {
      block_slot: `gte.${startSlot}`,
      and: `(block_slot.lte.${cutoffSlot})`,
    }).then((rows) => rows.map(normalizeMetadataRemote)),
  ]);

  const agents = agentsRaw.filter((row) => isInSlotRange(row.block_slot, startSlot, cutoffSlot));
  const feedbacks = feedbacksRaw.filter((row) => isInSlotRange(row.block_slot, startSlot, cutoffSlot));
  const responses = responsesRaw.filter((row) => isInSlotRange(row.block_slot, startSlot, cutoffSlot));
  const revocations = revocationsRaw.filter((row) => isInSlotRange(row.slot, startSlot, cutoffSlot));
  const collections = collectionsRaw.filter((row) => {
    const firstSeenSlot = Number(row.first_seen_slot ?? 0);
    const lastSeenSlot = Number(row.last_seen_slot ?? 0);
    return firstSeenSlot >= startSlot && firstSeenSlot <= cutoffSlot && lastSeenSlot >= startSlot && lastSeenSlot <= cutoffSlot;
  });
  const metadata = metadataRaw.filter((row) => isInSlotRange(row.block_slot, startSlot, cutoffSlot));

  const trimmedCounts = {
    agents: agentsRaw.length - agents.length,
    feedbacks: feedbacksRaw.length - feedbacks.length,
    responses: responsesRaw.length - responses.length,
    revocations: revocationsRaw.length - revocations.length,
    collections: collectionsRaw.length - collections.length,
    metadata: metadataRaw.length - metadata.length,
  };

  return {
    agents,
    feedbacks,
    responses,
    revocations,
    collections,
    metadata,
    trimmedCounts,
  };
}

async function fetchStableRemoteSnapshot(startSlot, cutoffSlot) {
  let previous = null;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const snapshot = await fetchRemoteSnapshot(startSlot, cutoffSlot);
    const fingerprint = JSON.stringify(snapshot);
    if (fingerprint === previous) {
      return snapshot;
    }
    previous = fingerprint;
    await sleep(POLL_INTERVAL_MS);
  }
  return fetchRemoteSnapshot(startSlot, cutoffSlot);
}

function normalizeAgentLocal(row) {
  return {
    key: row.id,
    asset: row.id,
    owner: row.owner ?? null,
    creator: row.creator ?? null,
    agent_uri: row.uri ?? null,
    agent_wallet: row.wallet ?? null,
    collection: row.collection ?? null,
    collection_pointer: row.collectionPointer ?? "",
    col_locked: Boolean(row.colLocked),
    parent_asset: row.parentAsset ?? null,
    parent_creator: row.parentCreator ?? null,
    parent_locked: Boolean(row.parentLocked),
    atom_enabled: Boolean(row.atomEnabled),
    block_slot: row.createdSlot !== null && row.createdSlot !== undefined ? Number(row.createdSlot) : 0,
    tx_index: row.txIndex ?? null,
    event_ordinal: row.eventOrdinal ?? null,
    tx_signature: row.createdTxSignature ?? null,
  };
}

function normalizeFeedbackLocal(row) {
  return {
    key: `${row.agentId}|${row.client}|${row.feedbackIndex.toString()}`,
    asset: row.agentId,
    client_address: row.client,
    feedback_index: row.feedbackIndex.toString(),
    value: row.value,
    value_decimals: row.valueDecimals,
    score: row.score ?? null,
    tag1: row.tag1 ?? null,
    tag2: row.tag2 ?? null,
    endpoint: row.endpoint ?? null,
    feedback_uri: row.feedbackUri ?? null,
    feedback_hash: bytesToHex(row.feedbackHash),
    is_revoked: Boolean(row.revoked),
    block_slot: row.createdSlot !== null && row.createdSlot !== undefined ? Number(row.createdSlot) : 0,
    tx_index: row.txIndex ?? null,
    event_ordinal: row.eventOrdinal ?? null,
    tx_signature: row.createdTxSignature ?? null,
  };
}

function normalizeOrphanFeedbackLocal(row) {
  return {
    key: `${row.agentId}|${row.client}|${row.feedbackIndex.toString()}`,
    asset: row.agentId,
    client_address: row.client,
    feedback_index: row.feedbackIndex.toString(),
    value: row.value,
    value_decimals: row.valueDecimals,
    score: row.score ?? null,
    tag1: row.tag1 ?? null,
    tag2: row.tag2 ?? null,
    endpoint: row.endpoint ?? null,
    feedback_uri: row.feedbackUri ?? null,
    feedback_hash: bytesToHex(row.feedbackHash),
    is_revoked: false,
    block_slot: row.slot !== null && row.slot !== undefined ? Number(row.slot) : 0,
    tx_index: row.txIndex ?? null,
    event_ordinal: row.eventOrdinal ?? null,
    tx_signature: row.txSignature ?? null,
  };
}

function normalizeResponseLocal(row) {
  const blockSlot = row.slot ?? row.feedback.createdSlot ?? 0n;
  return {
    key: `${row.feedback.agentId}|${row.feedback.client}|${row.feedback.feedbackIndex.toString()}|${row.responder}|${row.txSignature ?? ""}`,
    asset: row.feedback.agentId,
    client_address: row.feedback.client,
    feedback_index: row.feedback.feedbackIndex.toString(),
    responder: row.responder,
    response_uri: row.responseUri ?? null,
    response_hash: bytesToHex(row.responseHash),
    response_count: row.responseCount !== null && row.responseCount !== undefined ? row.responseCount.toString() : null,
    block_slot: Number(blockSlot),
    tx_index: row.txIndex ?? null,
    event_ordinal: row.eventOrdinal ?? null,
    tx_signature: row.txSignature ?? null,
  };
}

function normalizeOrphanResponseLocal(row) {
  const blockSlot = row.slot ?? 0n;
  return {
    key: `${row.agentId}|${row.client}|${row.feedbackIndex.toString()}|${row.responder}|${row.txSignature ?? ""}`,
    asset: row.agentId,
    client_address: row.client,
    feedback_index: row.feedbackIndex.toString(),
    responder: row.responder,
    response_uri: row.responseUri ?? null,
    response_hash: bytesToHex(row.responseHash),
    response_count: row.responseCount !== null && row.responseCount !== undefined ? row.responseCount.toString() : null,
    block_slot: Number(blockSlot),
    tx_index: row.txIndex ?? null,
    event_ordinal: row.eventOrdinal ?? null,
    tx_signature: row.txSignature ?? null,
  };
}

function normalizeRevocationLocal(row) {
  return {
    key: `${row.agentId}|${row.client}|${row.feedbackIndex.toString()}`,
    asset: row.agentId,
    client_address: row.client,
    feedback_index: row.feedbackIndex.toString(),
    feedback_hash: bytesToHex(row.feedbackHash),
    slot: Number(row.slot),
    original_score: row.originalScore ?? null,
    atom_enabled: Boolean(row.atomEnabled),
    had_impact: Boolean(row.hadImpact),
    revoke_count: row.revokeCount.toString(),
    tx_signature: row.txSignature ?? null,
    tx_index: row.txIndex ?? null,
    event_ordinal: row.eventOrdinal ?? null,
  };
}

function normalizeCollectionLocal(row) {
  return {
    // collection_id is backend-specific sequential allocation; compare canonical identity only.
    key: `${row.col}|${row.creator}`,
    collection: row.col,
    creator: row.creator,
    first_seen_asset: row.firstSeenAsset,
    first_seen_slot: row.firstSeenSlot.toString(),
    last_seen_slot: row.lastSeenSlot.toString(),
    asset_count: row.assetCount.toString(),
  };
}

function normalizeMetadataLocal(row) {
  const blockSlot = Number(row.slot ?? 0n);
  const keyHash = computeKeyHash(row.key);
  return {
    key: `${row.agentId}:${keyHash}`,
    id: `${row.agentId}:${keyHash}`,
    asset: row.agentId,
    key_name: row.key,
    key_hash: keyHash,
    value: Buffer.from(row.value).toString("base64"),
    immutable: Boolean(row.immutable),
    block_slot: blockSlot,
    tx_index: row.txIndex ?? null,
    event_ordinal: row.eventOrdinal ?? null,
    tx_signature: row.txSignature ?? (blockSlot === 0 ? "uri_derived" : null),
  };
}

async function fetchLocalSnapshot(dbPath, startSlot, cutoffSlot) {
  const prisma = createPrisma(dbPath);
  const cutoff = BigInt(cutoffSlot);
  const start = BigInt(startSlot);
  await prisma.$connect();

  try {
    const [agents, feedbacks, orphanFeedbacks, responses, orphanResponses, revocations, collections, metadata] =
      await Promise.all([
      prisma.agent.findMany({
        where: {
          createdSlot: { gte: start, lte: cutoff },
        },
      }),
      prisma.feedback.findMany({
        where: {
          createdSlot: { gte: start, lte: cutoff },
        },
      }),
      prisma.orphanFeedback.findMany({
        where: {
          slot: { gte: start, lte: cutoff },
        },
      }),
      prisma.feedbackResponse.findMany({
        where: {},
        include: {
          feedback: {
            select: {
              agentId: true,
              client: true,
              feedbackIndex: true,
              createdSlot: true,
            },
          },
        },
      }).then((rows) =>
        rows.filter((row) => isInSlotRange(row.slot ?? row.feedback.createdSlot ?? 0n, startSlot, cutoffSlot))
      ),
      prisma.orphanResponse.findMany({
        where: {
          slot: { gte: start, lte: cutoff },
        },
      }),
      prisma.revocation.findMany({
        where: {
          slot: { gte: start, lte: cutoff },
        },
      }),
      prisma.collection.findMany({
        where: {
          firstSeenSlot: { gte: start, lte: cutoff },
          lastSeenSlot: { gte: start, lte: cutoff },
        },
      }),
      prisma.agentMetadata.findMany({
        where: {
          slot: { gte: start, lte: cutoff },
        },
      }),
    ]);

    const snapshotStats = {
      total_agents: agents.length,
      total_collections: collections.length,
      total_feedbacks: feedbacks.length + orphanFeedbacks.length,
      total_responses: responses.length + orphanResponses.length,
      total_revocations: revocations.length,
      total_metadata: metadata.length,
    };

    return {
      agents: agents.map(normalizeAgentLocal),
      feedbacks: [...feedbacks.map(normalizeFeedbackLocal), ...orphanFeedbacks.map(normalizeOrphanFeedbackLocal)],
      responses: [...responses.map(normalizeResponseLocal), ...orphanResponses.map(normalizeOrphanResponseLocal)],
      revocations: revocations.map(normalizeRevocationLocal),
      collections: collections.map(normalizeCollectionLocal),
      metadata: metadata.map(normalizeMetadataLocal),
      snapshotStats,
    };
  } finally {
    await prisma.$disconnect();
  }
}

function compareCategory(name, remoteRows, localRows) {
  const remoteMap = new Map(remoteRows.map((row) => [row.key, row]));
  const localMap = new Map(localRows.map((row) => [row.key, row]));

  const missing = [];
  const extra = [];
  const changed = [];

  for (const [key, remoteRow] of remoteMap.entries()) {
    const localRow = localMap.get(key);
    if (!localRow) {
      missing.push(key);
      continue;
    }
    if (JSON.stringify(localRow) !== JSON.stringify(remoteRow)) {
      changed.push({
        key,
        remote: remoteRow,
        local: localRow,
      });
    }
  }

  for (const key of localMap.keys()) {
    if (!remoteMap.has(key)) {
      extra.push(key);
    }
  }

  return {
    name,
    remoteCount: remoteRows.length,
    localCount: localRows.length,
    missing,
    extra,
    changed,
    ok: missing.length === 0 && extra.length === 0 && changed.length === 0,
  };
}

async function main() {
  const connection = new Connection(rpcUrl, "confirmed");
  const currentSlot = await connection.getSlot();
  const cutoffSlot = CUTOFF_SLOT_OVERRIDE > 0 ? CUTOFF_SLOT_OVERRIDE : currentSlot - CUTOFF_MARGIN;
  const startSlot = START_SLOT > 0 ? START_SLOT : 0;

  console.log("[compare-devnet-local-modes] comparison window", {
    rpcUrl,
    currentSlot,
    startSlot,
    cutoffSlot,
    remoteBase: REMOTE_BASE,
  });

  await Promise.all(MODES.map((mode) => waitForReady(mode.baseUrl)));
  await Promise.all(MODES.map((mode) => waitForCatchup(mode, cutoffSlot)));

  const remoteSnapshot = await fetchStableRemoteSnapshot(startSlot, cutoffSlot);
  console.log("[compare-devnet-local-modes] frozen remote snapshot counts", {
    agents: remoteSnapshot.agents.length,
    feedbacks: remoteSnapshot.feedbacks.length,
    responses: remoteSnapshot.responses.length,
    revocations: remoteSnapshot.revocations.length,
    collections: remoteSnapshot.collections.length,
    metadata: remoteSnapshot.metadata.length,
  });
  if (Object.values(remoteSnapshot.trimmedCounts).some((count) => count > 0)) {
    console.log("[compare-devnet-local-modes] trimmed remote rows outside cutoff window", remoteSnapshot.trimmedCounts);
  }

  let failed = false;

  for (const mode of MODES) {
    const localSnapshot = await fetchLocalSnapshot(mode.dbPath, startSlot, cutoffSlot);
    const results = [
      compareCategory("agents", remoteSnapshot.agents, localSnapshot.agents),
      compareCategory("feedbacks", remoteSnapshot.feedbacks, localSnapshot.feedbacks),
      compareCategory("responses", remoteSnapshot.responses, localSnapshot.responses),
      compareCategory("revocations", remoteSnapshot.revocations, localSnapshot.revocations),
      compareCategory("collections", remoteSnapshot.collections, localSnapshot.collections),
      compareCategory("metadata", remoteSnapshot.metadata, localSnapshot.metadata),
    ];

    console.log(`[compare-devnet-local-modes] ${mode.name} snapshot stats`, localSnapshot.snapshotStats);

    for (const result of results) {
      console.log(`[compare-devnet-local-modes] ${mode.name}:${result.name}`, {
        remoteCount: result.remoteCount,
        localCount: result.localCount,
        missing: result.missing.length,
        extra: result.extra.length,
        changed: result.changed.length,
      });

      if (!result.ok) {
        failed = true;
        console.log(`[compare-devnet-local-modes] ${mode.name}:${result.name} first diffs`, {
          missing: result.missing.slice(0, 5),
          extra: result.extra.slice(0, 5),
          changed: result.changed.slice(0, 2),
        });
      }
    }
  }

  if (failed) {
    process.exitCode = 1;
    return;
  }

  console.log("[compare-devnet-local-modes] all mode comparisons matched remote snapshot");
}

main().catch((error) => {
  console.error("[compare-devnet-local-modes] fatal", error);
  process.exitCode = 1;
});
