import { createHash } from "node:crypto";
import { writeFileSync } from "node:fs";
import { resolve } from "node:path";

const DEFAULTS = {
  network: "mainnet",
  pageSize: 100,
  rpm: 20,
  retries: 5,
  waitTimeoutMs: 120000,
  pollIntervalMs: 5000,
  targetBase: {
    mainnet: "http://127.0.0.1:4962",
    devnet: "http://127.0.0.1:3962",
  },
  peerBases: {
    mainnet: [
      "https://8004-indexer-main.qnt.sh",
      "https://8004-indexer-main2.qnt.sh",
    ],
    devnet: ["https://8004-indexer-dev.qnt.sh"],
  },
};

const SURFACES = [
  {
    name: "agents",
    countPath: ["verificationStats", "agents", "finalized"],
    query: `query Agents($first: Int!, $skip: Int!) {
      agents(first: $first, skip: $skip, orderBy: createdAt, orderDirection: asc) {
        id
        agentId
        createdAt
        updatedAt
      }
    }`,
    dataPath: ["agents"],
    normalize: (row) => ({
      asset: normalizeValue(row?.id),
      agent_id: normalizeValue(row?.agentId),
      created_at: normalizeValue(row?.createdAt),
      updated_at: normalizeValue(row?.updatedAt),
    }),
    keyOf: (row) => row.asset,
  },
  {
    name: "feedbacks",
    countPath: ["verificationStats", "feedbacks", "finalized"],
    query: `query Feedbacks($first: Int!, $skip: Int!) {
      feedbacks(first: $first, skip: $skip, orderBy: createdAt, orderDirection: asc) {
        id
        createdAt
        isRevoked
      }
    }`,
    dataPath: ["feedbacks"],
    normalize: (row) => ({
      id: normalizeValue(row?.id),
      created_at: normalizeValue(row?.createdAt),
      is_revoked: normalizeValue(row?.isRevoked),
    }),
    keyOf: (row) => row.id,
  },
  {
    name: "responses",
    countPath: ["verificationStats", "feedbackResponses", "finalized"],
    query: `query Responses($first: Int!, $skip: Int!) {
      feedbackResponses(first: $first, skip: $skip, orderBy: createdAt, orderDirection: asc) {
        id
        createdAt
      }
    }`,
    dataPath: ["feedbackResponses"],
    normalize: (row) => ({
      id: normalizeValue(row?.id),
      created_at: normalizeValue(row?.createdAt),
    }),
    keyOf: (row) => row.id,
  },
  {
    name: "revocations",
    countPath: ["verificationStats", "revocations", "finalized"],
    query: `query Revocations($first: Int!, $skip: Int!) {
      revocations(first: $first, skip: $skip, orderBy: createdAt, orderDirection: asc) {
        id
        createdAt
        atomEnabled
        hadImpact
      }
    }`,
    dataPath: ["revocations"],
    normalize: (row) => ({
      id: normalizeValue(row?.id),
      created_at: normalizeValue(row?.createdAt),
      atom_enabled: normalizeValue(row?.atomEnabled),
      had_impact: normalizeValue(row?.hadImpact),
    }),
    keyOf: (row) => row.id,
  },
  {
    name: "collections",
    countPath: ["globalStats", "totalCollections"],
    query: `query Collections($first: Int!, $skip: Int!) {
      collections(first: $first, skip: $skip) {
        collectionId
        collection
        creator
        firstSeenAsset
        firstSeenAt
        firstSeenSlot
        firstSeenTxSignature
        lastSeenAt
        lastSeenSlot
        lastSeenTxSignature
        assetCount
      }
    }`,
    dataPath: ["collections"],
    normalize: (row) => ({
      collection_id: normalizeValue(row?.collectionId),
      collection: normalizeValue(row?.collection),
      creator: normalizeValue(row?.creator),
      first_seen_asset: normalizeValue(row?.firstSeenAsset),
      first_seen_at: normalizeValue(row?.firstSeenAt),
      first_seen_slot: normalizeValue(row?.firstSeenSlot),
      first_seen_tx_signature: normalizeValue(row?.firstSeenTxSignature),
      last_seen_at: normalizeValue(row?.lastSeenAt),
      last_seen_slot: normalizeValue(row?.lastSeenSlot),
      last_seen_tx_signature: normalizeValue(row?.lastSeenTxSignature),
      asset_count: normalizeValue(row?.assetCount),
    }),
    keyOf: (row) => `${row.creator}:${row.collection}`,
  },
];

const COUNTS_QUERY = `query ParityCounts {
  globalStats {
    totalAgents
    totalFeedback
    totalCollections
  }
  verificationStats {
    agents { pending finalized orphaned }
    feedbacks { pending finalized orphaned }
    feedbackResponses { pending finalized orphaned }
    revocations { pending finalized orphaned }
  }
}`;

function parseArgs(argv) {
  const args = {
    network: DEFAULTS.network,
    targetBase: null,
    peerBases: [],
    pageSize: DEFAULTS.pageSize,
    rpm: DEFAULTS.rpm,
    retries: DEFAULTS.retries,
    waitTimeoutMs: DEFAULTS.waitTimeoutMs,
    pollIntervalMs: DEFAULTS.pollIntervalMs,
    jsonOut: null,
    verbose: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    switch (token) {
      case "--network":
        args.network = argv[++i] || args.network;
        break;
      case "--target-base":
      case "--base":
        args.targetBase = argv[++i] || null;
        break;
      case "--peer-base":
        if (argv[i + 1]) args.peerBases.push(argv[++i]);
        break;
      case "--page-size":
        args.pageSize = parsePositiveInt(argv[++i], DEFAULTS.pageSize);
        break;
      case "--rpm":
        args.rpm = parsePositiveInt(argv[++i], DEFAULTS.rpm);
        break;
      case "--retries":
        args.retries = parseNonNegativeInt(argv[++i], DEFAULTS.retries);
        break;
      case "--wait-timeout-ms":
        args.waitTimeoutMs = parsePositiveInt(argv[++i], DEFAULTS.waitTimeoutMs);
        break;
      case "--poll-interval-ms":
        args.pollIntervalMs = parsePositiveInt(argv[++i], DEFAULTS.pollIntervalMs);
        break;
      case "--json-out":
        args.jsonOut = argv[++i] || null;
        break;
      case "--verbose":
        args.verbose = true;
        break;
      case "--help":
      case "-h":
        printHelp();
        process.exit(0);
        break;
      default:
        throw new Error(`Unknown arg: ${token}`);
    }
  }

  if (!["mainnet", "devnet"].includes(args.network)) {
    throw new Error(`Invalid --network '${args.network}'. Use mainnet or devnet.`);
  }

  args.targetBase = normalizeBaseUrl(args.targetBase || DEFAULTS.targetBase[args.network]);
  args.peerBases = (args.peerBases.length > 0 ? args.peerBases : DEFAULTS.peerBases[args.network]).map(normalizeBaseUrl);
  return args;
}

function printHelp() {
  console.log(`Usage: node scripts/check-public-parity.mjs [options]

GraphQL-only public parity check. Compares one target indexer against live peers.
The script waits for a shared head and zero pending backlog before comparing public GraphQL surfaces.

Options:
  --network mainnet|devnet     Network profile. Default: mainnet
  --target-base URL            Target indexer base URL. Default depends on network.
  --peer-base URL              Reference peer base URL. Repeatable.
  --page-size N                GraphQL page size. Default: 500
  --rpm N                      Per-host request budget. Default: 20
  --retries N                  Retry count for 429/5xx. Default: 5
  --wait-timeout-ms N          Wait for shared head + zero pending. Default: 120000
  --poll-interval-ms N         Ready polling cadence. Default: 5000
  --json-out PATH              Write JSON report to file
  --verbose                    Print request progress
`);
}

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(value ?? "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parseNonNegativeInt(value, fallback) {
  const parsed = Number.parseInt(value ?? "", 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

function normalizeBaseUrl(value) {
  const url = new URL(String(value).trim());
  let pathname = url.pathname.replace(/\/+$/, "");
  if (pathname.endsWith("/v2/graphql")) {
    pathname = pathname.slice(0, -"/v2/graphql".length);
  } else if (pathname.endsWith("/rest/v1")) {
    pathname = pathname.slice(0, -"/rest/v1".length);
  }
  url.pathname = pathname;
  url.search = "";
  url.hash = "";
  return url.toString().replace(/\/+$/, "");
}

function buildReadyUrl(base) {
  return new URL("/ready", `${base}/`).toString();
}

function buildGraphqlUrl(base) {
  return new URL("/v2/graphql", `${base}/`).toString();
}

function hostKey(urlString) {
  const url = new URL(urlString);
  return `${url.protocol}//${url.host}`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

class RateLimitedFetcher {
  constructor({ rpm, retries, verbose }) {
    this.intervalMs = Math.ceil(60000 / Math.max(1, rpm));
    this.retries = retries;
    this.verbose = verbose;
    this.nextAllowedAt = new Map();
  }

  async json(url, options = {}) {
    const response = await this.fetch(url, options);
    const text = await response.text();
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      throw new Error(`Invalid JSON from ${url}: ${text.slice(0, 300)}`);
    }
    return { json, headers: response.headers, status: response.status };
  }

  async graphql(base, query, variables = {}) {
    const url = buildGraphqlUrl(base);
    const { json } = await this.json(url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ query, variables }),
    });

    if (json?.errors?.length) {
      const message = json.errors.map((entry) => entry?.message).filter(Boolean).join("; ");
      throw new Error(`GraphQL error from ${url}: ${message}`);
    }
    if (!json?.data) {
      throw new Error(`GraphQL response missing data from ${url}`);
    }
    return json.data;
  }

  async fetch(url, options = {}) {
    const key = hostKey(url);
    const baseDelay = this.intervalMs;

    for (let attempt = 0; attempt <= this.retries; attempt += 1) {
      const waitMs = Math.max(0, (this.nextAllowedAt.get(key) || 0) - Date.now());
      if (waitMs > 0) await sleep(waitMs);
      this.nextAllowedAt.set(key, Date.now() + baseDelay);

      if (this.verbose) {
        console.log(`[public-parity] ${options.method || "GET"} ${url}${attempt > 0 ? ` retry=${attempt}` : ""}`);
      }

      let response;
      try {
        response = await fetch(url, options);
      } catch (error) {
        if (attempt === this.retries) throw error;
        await sleep(baseDelay * (attempt + 1));
        continue;
      }

      if (response.ok) return response;

      if (![429, 500, 502, 503, 504].includes(response.status) || attempt === this.retries) {
        const text = await response.text().catch(() => "");
        throw new Error(`HTTP ${response.status} for ${url}: ${text.slice(0, 300)}`);
      }

      const retryAfterRaw = response.headers.get("retry-after");
      const retryAfter = retryAfterRaw ? Number.parseInt(retryAfterRaw, 10) : Number.NaN;
      const delayMs = Number.isFinite(retryAfter) && retryAfter > 0
        ? retryAfter * 1000
        : baseDelay * (attempt + 1);
      await sleep(delayMs);
    }

    throw new Error(`Unreachable fetch state for ${url}`);
  }
}

function toNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeReadyPayload(raw) {
  const state = raw?.state && typeof raw.state === "object" ? raw.state : raw || {};
  const pending = state.pending && typeof state.pending === "object" ? state.pending : raw?.pending || {};
  const orphans = state.orphans && typeof state.orphans === "object" ? state.orphans : raw?.orphans || {};

  return {
    status: String(raw?.status ?? state.status ?? ""),
    phase: String(raw?.phase ?? state.phase ?? ""),
    mainSlot: toNumber(state.mainSlot ?? raw?.mainSlot ?? state.slot ?? raw?.slot),
    mainTxIndex: toNumber(state.mainTxIndex ?? raw?.mainTxIndex ?? state.txIndex ?? raw?.txIndex),
    counts: {
      agents: toNumber(state.agents ?? raw?.agents) ?? 0,
      feedbacks: toNumber(state.feedbacks ?? raw?.feedbacks) ?? 0,
      responses: toNumber(state.responses ?? raw?.responses) ?? 0,
      revocations: toNumber(state.revocations ?? raw?.revocations) ?? 0,
      collections: toNumber(state.collections ?? raw?.collections) ?? 0,
    },
    pending: {
      agents: toNumber(pending.agents) ?? 0,
      feedbacks: toNumber(pending.feedbacks) ?? 0,
      responses: toNumber(pending.responses) ?? 0,
      revocations: toNumber(pending.revocations) ?? 0,
      metadata: toNumber(pending.metadata) ?? 0,
    },
    orphans: {
      feedbacks: toNumber(orphans.feedbacks) ?? 0,
      responses: toNumber(orphans.responses) ?? 0,
      revocations: toNumber(orphans.revocations) ?? 0,
    },
    raw,
  };
}

function normalizeValue(value) {
  return value === undefined ? null : value;
}

function stableStringify(value) {
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableStringify(entry)).join(",")}]`;
  }
  if (value && typeof value === "object") {
    const entries = Object.entries(value).sort(([a], [b]) => a.localeCompare(b));
    return `{${entries.map(([key, entry]) => `${JSON.stringify(key)}:${stableStringify(entry)}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function hashRows(rows) {
  return createHash("sha256").update(stableStringify(rows)).digest("hex");
}

function getByPath(object, path) {
  return path.reduce((current, key) => (current == null ? undefined : current[key]), object);
}

function normalizeCounts(raw) {
  return {
    globalStats: {
      totalAgents: toNumber(raw?.globalStats?.totalAgents) ?? 0,
      totalFeedback: toNumber(raw?.globalStats?.totalFeedback) ?? 0,
      totalCollections: toNumber(raw?.globalStats?.totalCollections) ?? 0,
    },
    verificationStats: {
      agents: {
        pending: toNumber(raw?.verificationStats?.agents?.pending) ?? 0,
        finalized: toNumber(raw?.verificationStats?.agents?.finalized) ?? 0,
        orphaned: toNumber(raw?.verificationStats?.agents?.orphaned) ?? 0,
      },
      feedbacks: {
        pending: toNumber(raw?.verificationStats?.feedbacks?.pending) ?? 0,
        finalized: toNumber(raw?.verificationStats?.feedbacks?.finalized) ?? 0,
        orphaned: toNumber(raw?.verificationStats?.feedbacks?.orphaned) ?? 0,
      },
      feedbackResponses: {
        pending: toNumber(raw?.verificationStats?.feedbackResponses?.pending) ?? 0,
        finalized: toNumber(raw?.verificationStats?.feedbackResponses?.finalized) ?? 0,
        orphaned: toNumber(raw?.verificationStats?.feedbackResponses?.orphaned) ?? 0,
      },
      revocations: {
        pending: toNumber(raw?.verificationStats?.revocations?.pending) ?? 0,
        finalized: toNumber(raw?.verificationStats?.revocations?.finalized) ?? 0,
        orphaned: toNumber(raw?.verificationStats?.revocations?.orphaned) ?? 0,
      },
    },
  };
}

function endpointLabel(base, index, role) {
  const host = new URL(base).host;
  return role === "target" ? `target:${host}` : `peer${index}:${host}`;
}

async function loadReady(fetcher, label, base) {
  const { json } = await fetcher.json(buildReadyUrl(base));
  return { label, base, ready: normalizeReadyPayload(json) };
}

async function waitForComparableWindow(fetcher, endpoints, waitTimeoutMs, pollIntervalMs) {
  const deadline = Date.now() + waitTimeoutMs;
  let lastSnapshot = null;

  while (Date.now() < deadline) {
    const snapshot = await Promise.all(endpoints.map((entry) => loadReady(fetcher, entry.label, entry.base)));
    lastSnapshot = snapshot;

    const allReady = snapshot.every((entry) => entry.ready.status === "ready");
    const sharedHead = snapshot.every((entry) =>
      entry.ready.mainSlot === snapshot[0].ready.mainSlot &&
      entry.ready.mainTxIndex === snapshot[0].ready.mainTxIndex
    );
    const zeroPending = snapshot.every((entry) =>
      Object.values(entry.ready.pending).every((value) => value === 0)
    );

    if (allReady && sharedHead && zeroPending) {
      return snapshot;
    }

    await sleep(pollIntervalMs);
  }

  throw new Error(
    `Timed out waiting for comparable window. Last ready snapshot: ${JSON.stringify(lastSnapshot?.map((entry) => ({
      label: entry.label,
      status: entry.ready.status,
      phase: entry.ready.phase,
      mainSlot: entry.ready.mainSlot,
      mainTxIndex: entry.ready.mainTxIndex,
      pending: entry.ready.pending,
    })), null, 2)}`
  );
}

async function fetchCounts(fetcher, base) {
  const raw = await fetcher.graphql(base, COUNTS_QUERY);
  return normalizeCounts(raw);
}

async function fetchAllSurfaceRows(fetcher, base, surface, pageSize, expectedTotal) {
  const rows = [];
  for (let skip = 0; skip < expectedTotal; skip += pageSize) {
    const data = await fetcher.graphql(base, surface.query, { first: pageSize, skip });
    const chunk = getByPath(data, surface.dataPath);
    if (!Array.isArray(chunk)) {
      throw new Error(`Invalid GraphQL payload for ${surface.name}`);
    }
    rows.push(...chunk.map((row) => surface.normalize(row)));
    if (chunk.length < pageSize) break;
  }
  return rows.sort((left, right) => surface.keyOf(left).localeCompare(surface.keyOf(right)));
}

function computeRowDiff(surface, baselineRows, candidateRows) {
  const baselineMap = new Map(baselineRows.map((row) => [surface.keyOf(row), row]));
  const candidateMap = new Map(candidateRows.map((row) => [surface.keyOf(row), row]));
  const allKeys = new Set([...baselineMap.keys(), ...candidateMap.keys()]);
  const missing = [];
  const extra = [];
  const fieldDiffs = [];

  for (const key of allKeys) {
    const left = baselineMap.get(key);
    const right = candidateMap.get(key);
    if (!left && right) {
      extra.push({ key, row: right });
      continue;
    }
    if (left && !right) {
      missing.push({ key, row: left });
      continue;
    }
    const changed = [];
    for (const field of Object.keys(left)) {
      if (stableStringify(left[field]) !== stableStringify(right[field])) {
        changed.push({ field, left: left[field], right: right[field] });
      }
    }
    if (changed.length > 0) {
      fieldDiffs.push({ key, changed });
    }
  }

  return { missing, extra, fieldDiffs };
}

function summarizeDiff(diff) {
  return {
    missing: diff.missing.length,
    extra: diff.extra.length,
    fieldDiffs: diff.fieldDiffs.length,
  };
}

function summarizeReadyComparison(endpoints) {
  const [baseline, ...rest] = endpoints;
  return rest.map((endpoint) => {
    const diffs = [];
    if (stableStringify(baseline.ready.status) !== stableStringify(endpoint.ready.status)) diffs.push("status");
    if (stableStringify(baseline.ready.phase) !== stableStringify(endpoint.ready.phase)) diffs.push("phase");
    if (stableStringify(baseline.ready.mainSlot) !== stableStringify(endpoint.ready.mainSlot)) diffs.push("mainSlot");
    if (stableStringify(baseline.ready.mainTxIndex) !== stableStringify(endpoint.ready.mainTxIndex)) diffs.push("mainTxIndex");
    if (stableStringify(baseline.ready.counts) !== stableStringify(endpoint.ready.counts)) diffs.push("counts");
    if (stableStringify(baseline.ready.pending) !== stableStringify(endpoint.ready.pending)) diffs.push("pending");
    if (stableStringify(baseline.ready.orphans) !== stableStringify(endpoint.ready.orphans)) diffs.push("orphans");
    return {
      baseline: baseline.label,
      candidate: endpoint.label,
      exact: diffs.length === 0,
      diffs,
    };
  });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const endpoints = [
    { label: endpointLabel(args.targetBase, 0, "target"), base: args.targetBase },
    ...args.peerBases.map((base, index) => ({ label: endpointLabel(base, index + 1, "peer"), base })),
  ];
  const fetcher = new RateLimitedFetcher({
    rpm: args.rpm,
    retries: args.retries,
    verbose: args.verbose,
  });

  const readySnapshot = await waitForComparableWindow(
    fetcher,
    endpoints,
    args.waitTimeoutMs,
    args.pollIntervalMs,
  );
  const hydratedEndpoints = readySnapshot.map((entry) => ({
    label: entry.label,
    base: entry.base,
    ready: entry.ready,
  }));

  const countsByEndpoint = {};
  for (const endpoint of hydratedEndpoints) {
    countsByEndpoint[endpoint.label] = await fetchCounts(fetcher, endpoint.base);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: "graphql-only",
    network: args.network,
    config: {
      targetBase: args.targetBase,
      peerBases: args.peerBases,
      pageSize: args.pageSize,
      rpm: args.rpm,
      retries: args.retries,
      waitTimeoutMs: args.waitTimeoutMs,
      pollIntervalMs: args.pollIntervalMs,
    },
    ready: Object.fromEntries(hydratedEndpoints.map((entry) => [entry.label, entry.ready])),
    readyComparison: summarizeReadyComparison(hydratedEndpoints),
    counts: countsByEndpoint,
    surfaces: {},
  };

  for (const surface of SURFACES) {
    const totals = Object.fromEntries(
      hydratedEndpoints.map((endpoint) => [
        endpoint.label,
        toNumber(getByPath(countsByEndpoint[endpoint.label], surface.countPath)) ?? 0,
      ])
    );
    const expectedTotal = Math.max(...Object.values(totals));
    const rowsByEndpoint = {};
    for (const endpoint of hydratedEndpoints) {
      rowsByEndpoint[endpoint.label] = await fetchAllSurfaceRows(
        fetcher,
        endpoint.base,
        surface,
        args.pageSize,
        totals[endpoint.label],
      );
    }

    const baselineRows = rowsByEndpoint[hydratedEndpoints[0].label];
    const baselineHash = hashRows(baselineRows);
    const endpointDiffs = [];

    for (const endpoint of hydratedEndpoints.slice(1)) {
      const candidateRows = rowsByEndpoint[endpoint.label];
      const candidateHash = hashRows(candidateRows);
      if (candidateHash !== baselineHash) {
        const diff = computeRowDiff(surface, baselineRows, candidateRows);
        endpointDiffs.push({
          baseline: hydratedEndpoints[0].label,
          candidate: endpoint.label,
          baselineHash,
          candidateHash,
          summary: summarizeDiff(diff),
          sample: {
            missing: diff.missing.slice(0, 3),
            extra: diff.extra.slice(0, 3),
            fieldDiffs: diff.fieldDiffs.slice(0, 3),
          },
        });
      }
    }

    report.surfaces[surface.name] = {
      name: surface.name,
      compared: true,
      totalByEndpoint: totals,
      exactMatch: endpointDiffs.length === 0,
      endpointDiffs,
    };
  }

  report.summary = {
    readyExact: report.readyComparison.every((entry) => entry.exact),
    surfaceExact: Object.values(report.surfaces).every((surface) => surface.exactMatch),
  };

  if (args.jsonOut) {
    writeFileSync(resolve(args.jsonOut), JSON.stringify(report, null, 2));
  }

  console.log("PUBLIC_PARITY_REPORT");
  console.log(JSON.stringify(report, null, 2));

  if (!report.summary.surfaceExact) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error("PUBLIC_PARITY_REPORT=FAIL");
  console.error(error);
  process.exit(1);
});
