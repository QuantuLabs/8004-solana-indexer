import "dotenv/config";

export type IndexerMode = "auto" | "polling" | "websocket";
export type DbMode = "local" | "supabase";
export type ApiMode = "graphql" | "rest" | "both";
export type MetadataIndexMode = "off" | "normal" | "full";
export type SolanaNetwork = "devnet" | "mainnet-beta" | "testnet" | "localnet";
export type ChainStatus = "PENDING" | "FINALIZED" | "ORPHANED";

const DEFAULT_PROGRAM_ID = "8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C";
const resolvedProgramId = (process.env.PROGRAM_ID || DEFAULT_PROGRAM_ID).trim();

const VALID_DB_MODES: DbMode[] = ["local", "supabase"];
const VALID_API_MODES: ApiMode[] = ["graphql", "rest", "both"];
const VALID_INDEXER_MODES: IndexerMode[] = ["auto", "polling", "websocket"];
const VALID_METADATA_MODES: MetadataIndexMode[] = ["off", "normal", "full"];
const VALID_SOLANA_NETWORKS: SolanaNetwork[] = ["devnet", "mainnet-beta", "testnet", "localnet"];

function parseSolanaNetwork(value: string | undefined): SolanaNetwork {
  const network = (value || "devnet").trim().toLowerCase();
  if (!VALID_SOLANA_NETWORKS.includes(network as SolanaNetwork)) {
    throw new Error(
      `Invalid SOLANA_NETWORK '${network}'. Must be one of: ${VALID_SOLANA_NETWORKS.join(", ")}`
    );
  }
  return network as SolanaNetwork;
}

function defaultRpcUrlForNetwork(network: SolanaNetwork): string {
  switch (network) {
    case "mainnet-beta":
      return "https://api.mainnet-beta.solana.com";
    case "testnet":
      return "https://api.testnet.solana.com";
    case "localnet":
      return "http://127.0.0.1:8899";
    case "devnet":
    default:
      return "https://api.devnet.solana.com";
  }
}

function defaultWsUrlForNetwork(network: SolanaNetwork): string {
  switch (network) {
    case "mainnet-beta":
      return "wss://api.mainnet-beta.solana.com";
    case "testnet":
      return "wss://api.testnet.solana.com";
    case "localnet":
      return "ws://127.0.0.1:8900";
    case "devnet":
    default:
      return "wss://api.devnet.solana.com";
  }
}

const resolvedSolanaNetwork = parseSolanaNetwork(process.env.SOLANA_NETWORK);

function parseDbMode(value: string | undefined): DbMode {
  const mode = value || "local";
  if (!VALID_DB_MODES.includes(mode as DbMode)) {
    throw new Error(`Invalid DB_MODE '${mode}'. Must be one of: ${VALID_DB_MODES.join(", ")}`);
  }
  return mode as DbMode;
}

function parseIndexerMode(value: string | undefined): IndexerMode {
  const mode = value || "auto";
  if (!VALID_INDEXER_MODES.includes(mode as IndexerMode)) {
    throw new Error(`Invalid INDEXER_MODE '${mode}'. Must be one of: ${VALID_INDEXER_MODES.join(", ")}`);
  }
  return mode as IndexerMode;
}

function parseApiMode(value: string | undefined): ApiMode {
  const rawMode = (value || "both").trim().toLowerCase();
  const mode = rawMode === "graph"
    ? "graphql"
    : rawMode === "hybrid"
      ? "both"
      : rawMode;
  if (!VALID_API_MODES.includes(mode as ApiMode)) {
    throw new Error(
      `Invalid API_MODE '${rawMode}'. Must be one of: ${VALID_API_MODES.join(", ")} (legacy alias: hybrid)`
    );
  }
  return mode as ApiMode;
}

function parseMetadataMode(value: string | undefined): MetadataIndexMode {
  const mode = value || "normal";
  if (!VALID_METADATA_MODES.includes(mode as MetadataIndexMode)) {
    throw new Error(`Invalid INDEX_METADATA '${mode}'. Must be one of: ${VALID_METADATA_MODES.join(", ")}`);
  }
  return mode as MetadataIndexMode;
}

function parseBoolean(value: string | undefined, fallback: boolean): boolean {
  if (!value || value.trim() === "") {
    return fallback;
  }
  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  return fallback;
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  if (!value || value.trim() === "") {
    return fallback;
  }
  const parsed = parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function parseNonNegativeInt(value: string | undefined, fallback: number): number {
  if (!value || value.trim() === "") {
    return fallback;
  }
  const parsed = parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallback;
  }
  return parsed;
}

/**
 * Runtime configuration (populated at startup from on-chain data via SDK)
 */
export const runtimeConfig: {
  baseCollection: string | null;
  initialized: boolean;
} = {
  baseCollection: null,
  initialized: false,
};

export const config = {
  // Database mode: "local" (SQLite/Prisma) | "supabase" (PostgreSQL via Supabase)
  dbMode: parseDbMode(process.env.DB_MODE),

  // Local database (SQLite via Prisma)
  databaseUrl: process.env.DATABASE_URL || "file:./data/indexer.db",

  // Supabase (production)
  supabaseUrl: process.env.SUPABASE_URL,
  supabaseKey: process.env.SUPABASE_KEY, // service_role key for writes
  supabaseDsn: process.env.SUPABASE_DSN, // PostgreSQL DSN for direct pg connection
  supabaseSslVerify: process.env.SUPABASE_SSL_VERIFY !== "false", // default: verify SSL certs

  // Solana network for default RPC/WS endpoint resolution
  solanaNetwork: resolvedSolanaNetwork,

  // Solana RPC (works with any provider)
  rpcUrl: process.env.RPC_URL || defaultRpcUrlForNetwork(resolvedSolanaNetwork),
  wsUrl: process.env.WS_URL || defaultWsUrlForNetwork(resolvedSolanaNetwork),
  // Highest Solana transaction version accepted by RPC parsers.
  maxSupportedTransactionVersion: parseNonNegativeInt(
    process.env.MAX_SUPPORTED_TRANSACTION_VERSION,
    0
  ),

  // Program ID from SDK (source of truth)
  programId: resolvedProgramId,

  // API mode: both (default) | graphql | rest
  apiMode: parseApiMode(process.env.API_MODE),
  // GraphQL requires Supabase pool and is enabled by default
  enableGraphql: parseBoolean(process.env.ENABLE_GRAPHQL, true),
  // Cache TTL for expensive GraphQL aggregated stats queries
  graphqlStatsCacheTtlMs: parsePositiveInt(process.env.GRAPHQL_STATS_CACHE_TTL_MS, 60000),

  // Indexer mode: "auto" | "polling" | "websocket"
  // auto = tries WebSocket first, falls back to polling if unavailable
  indexerMode: parseIndexerMode(process.env.INDEXER_MODE),

  // Polling config
  pollingInterval: parseInt(process.env.POLLING_INTERVAL || "5000", 10),
  batchSize: parseInt(process.env.BATCH_SIZE || "100", 10),

  // WebSocket config
  wsReconnectInterval: parseInt(
    process.env.WS_RECONNECT_INTERVAL || "3000",
    10
  ),
  wsMaxRetries: parseInt(process.env.WS_MAX_RETRIES || "5", 10),

  // Logging
  logLevel: process.env.LOG_LEVEL || "info",

  // URI Metadata indexing (fetch and extract fields from agent_uri)
  // off = don't fetch URIs, normal = extract standard fields, full = store entire JSON
  metadataIndexMode: parseMetadataMode(process.env.INDEX_METADATA),
  // Collection metadata indexing from canonical pointer (c1:<cid>)
  // true = fetch and parse collection JSON, false = keep on-chain only
  collectionMetadataIndexEnabled: parseBoolean(process.env.INDEX_COLLECTION_METADATA, true),
  // Validation module is archived on-chain in v0.6.x.
  // Keep ingestion off by default in runtime, on during tests for coverage.
  validationIndexEnabled: parseBoolean(process.env.INDEX_VALIDATIONS, process.env.NODE_ENV === "test" || process.env.VITEST !== undefined),
  // Maximum bytes to fetch from URI (prevents memory exhaustion)
  metadataMaxBytes: parseInt(process.env.METADATA_MAX_BYTES || "262144", 10), // 256KB
  // Maximum bytes per field value (prevents single oversize field)
  metadataMaxValueBytes: parseInt(process.env.METADATA_MAX_VALUE_BYTES || "10000", 10), // 10KB
  // Fixed timeout for URI fetch (security: no user-configurable timeout)
  metadataTimeoutMs: 5000,

  // Verification config (reorg resilience)
  // Enable/disable background verification worker
  verificationEnabled: process.env.VERIFICATION_ENABLED !== "false",
  // Expose Prometheus /metrics endpoint for integrity observability
  metricsEndpointEnabled: parseBoolean(process.env.METRICS_ENDPOINT_ENABLED, false),
  // Interval between verification cycles (ms)
  verifyIntervalMs: parseInt(process.env.VERIFY_INTERVAL_MS || "60000", 10), // 60s
  // Max items to verify per cycle (prevents RPC rate limiting)
  verifyBatchSize: parseInt(process.env.VERIFY_BATCH_SIZE || "100", 10),
  // Safety margin: slots behind finalized to wait before verifying
  verifySafetyMarginSlots: parseInt(process.env.VERIFY_SAFETY_MARGIN_SLOTS || "32", 10),
  // Max retries for existence checks before orphaning
  verifyMaxRetries: parseInt(process.env.VERIFY_MAX_RETRIES || "3", 10),
  // Run ORPHANED recovery every N verification cycles (0 = disabled)
  verifyRecoveryCycles: parseInt(process.env.VERIFY_RECOVERY_CYCLES || "10", 10),
  // Max ORPHANED records to re-check per recovery run
  verifyRecoveryBatchSize: parseInt(process.env.VERIFY_RECOVERY_BATCH_SIZE || "50", 10),
} as const;

export function validateConfig(): void {
  // Mode validations already done at parse time (parseDbMode, parseIndexerMode, parseMetadataMode)

  // Warn about disabled SSL verification
  if (!config.supabaseSslVerify) {
    console.warn('[SECURITY WARNING] SUPABASE_SSL_VERIFY=false — TLS certificate verification is disabled for database connections. This is vulnerable to MITM attacks. Do NOT use in production.');
  }

  if (config.solanaNetwork === "mainnet-beta" && config.programId === DEFAULT_PROGRAM_ID) {
    console.warn(
      "[CONFIG WARNING] SOLANA_NETWORK=mainnet-beta but PROGRAM_ID is still the default devnet ID. Set PROGRAM_ID to your mainnet deployment before production use."
    );
  }

  // Validate Supabase config when in supabase mode
  if (config.dbMode === "supabase") {
    if (!config.supabaseDsn) {
      throw new Error("SUPABASE_DSN required when DB_MODE=supabase");
    }
  }

  // Validate verification config
  if (config.verifyIntervalMs < 5000) {
    throw new Error("VERIFY_INTERVAL_MS must be at least 5000ms");
  }

  if (config.verifyBatchSize < 1 || config.verifyBatchSize > 1000) {
    throw new Error("VERIFY_BATCH_SIZE must be between 1 and 1000");
  }

  if (config.verifySafetyMarginSlots < 0 || config.verifySafetyMarginSlots > 150) {
    throw new Error("VERIFY_SAFETY_MARGIN_SLOTS must be between 0 and 150");
  }

  if (config.verifyRecoveryCycles < 0 || config.verifyRecoveryCycles > 1000) {
    throw new Error("VERIFY_RECOVERY_CYCLES must be between 0 and 1000");
  }

  if (config.verifyRecoveryBatchSize < 1 || config.verifyRecoveryBatchSize > 1000) {
    throw new Error("VERIFY_RECOVERY_BATCH_SIZE must be between 1 and 1000");
  }

  if (config.graphqlStatsCacheTtlMs < 1000 || config.graphqlStatsCacheTtlMs > 3600000) {
    throw new Error("GRAPHQL_STATS_CACHE_TTL_MS must be between 1000 and 3600000");
  }

  if (config.maxSupportedTransactionVersion < 0) {
    throw new Error("MAX_SUPPORTED_TRANSACTION_VERSION must be >= 0");
  }
}
