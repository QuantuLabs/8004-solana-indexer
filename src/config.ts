import "dotenv/config";

export type IndexerMode = "auto" | "polling" | "websocket";

export const config = {
  // Database
  databaseUrl:
    process.env.DATABASE_URL ||
    "postgresql://postgres:postgres@localhost:5432/indexer8004?schema=public",

  // Solana RPC (works with any provider)
  rpcUrl: process.env.RPC_URL || "https://api.devnet.solana.com",
  wsUrl: process.env.WS_URL || "wss://api.devnet.solana.com",

  // Program ID (8004 Agent Registry)
  programId:
    process.env.PROGRAM_ID || "3GGkAWC3mYYdud8GVBsKXK5QC9siXtFkWVZFYtbueVbC",

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

  // GraphQL API
  graphqlPort: parseInt(process.env.GRAPHQL_PORT || "4000", 10),

  // Logging
  logLevel: process.env.LOG_LEVEL || "info",
} as const;

export function validateConfig(): void {
  // Note: All config values have defaults, so these checks validate the format/type
  // rather than presence. The indexerMode check is the only meaningful one.
  if (!["auto", "polling", "websocket"].includes(config.indexerMode)) {
    throw new Error("INDEXER_MODE must be 'auto', 'polling', or 'websocket'");
  }
}
