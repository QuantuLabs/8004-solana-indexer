import "dotenv/config";
import { PROGRAM_ID } from "8004-solana";

export type IndexerMode = "auto" | "polling" | "websocket";
export type DbMode = "local" | "supabase";
export type MetadataIndexMode = "off" | "normal" | "full";
export type ChainStatus = "PENDING" | "FINALIZED" | "ORPHANED";

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
  dbMode: (process.env.DB_MODE || "local") as DbMode,

  // Local database (SQLite via Prisma)
  databaseUrl: process.env.DATABASE_URL || "file:./data/indexer.db",

  // Supabase (production)
  supabaseUrl: process.env.SUPABASE_URL,
  supabaseKey: process.env.SUPABASE_KEY, // service_role key for writes
  supabaseDsn: process.env.SUPABASE_DSN, // PostgreSQL DSN for direct pg connection

  // Solana RPC (works with any provider)
  rpcUrl: process.env.RPC_URL || "https://api.devnet.solana.com",
  wsUrl: process.env.WS_URL || "wss://api.devnet.solana.com",

  // Program ID from SDK (source of truth)
  programId: PROGRAM_ID.toBase58(),

  // Indexer mode: "auto" | "polling" | "websocket"
  // auto = tries WebSocket first, falls back to polling if unavailable
  indexerMode: (process.env.INDEXER_MODE || "auto") as IndexerMode,

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
  metadataIndexMode: (process.env.INDEX_METADATA || "normal") as MetadataIndexMode,
  // Maximum bytes to fetch from URI (prevents memory exhaustion)
  metadataMaxBytes: parseInt(process.env.METADATA_MAX_BYTES || "262144", 10), // 256KB
  // Maximum bytes per field value (prevents single oversize field)
  metadataMaxValueBytes: parseInt(process.env.METADATA_MAX_VALUE_BYTES || "10000", 10), // 10KB
  // Fixed timeout for URI fetch (security: no user-configurable timeout)
  metadataTimeoutMs: 5000,

  // Verification config (reorg resilience)
  // Enable/disable background verification worker
  verificationEnabled: process.env.VERIFICATION_ENABLED !== "false",
  // Interval between verification cycles (ms)
  verifyIntervalMs: parseInt(process.env.VERIFY_INTERVAL_MS || "60000", 10), // 60s
  // Max items to verify per cycle (prevents RPC rate limiting)
  verifyBatchSize: parseInt(process.env.VERIFY_BATCH_SIZE || "100", 10),
  // Safety margin: slots behind finalized to wait before verifying
  verifySafetyMarginSlots: parseInt(process.env.VERIFY_SAFETY_MARGIN_SLOTS || "32", 10),
  // Max retries for existence checks before orphaning
  verifyMaxRetries: parseInt(process.env.VERIFY_MAX_RETRIES || "3", 10),
} as const;

export function validateConfig(): void {
  if (!["auto", "polling", "websocket"].includes(config.indexerMode)) {
    throw new Error("INDEXER_MODE must be 'auto', 'polling', or 'websocket'");
  }

  if (!["local", "supabase"].includes(config.dbMode)) {
    throw new Error("DB_MODE must be 'local' or 'supabase'");
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
}
