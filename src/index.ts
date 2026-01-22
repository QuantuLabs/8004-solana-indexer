import { PrismaClient } from "@prisma/client";
import { config, validateConfig } from "./config.js";
import { logger } from "./logger.js";
import { Processor } from "./indexer/processor.js";
import { startApiServer } from "./api/server.js";

async function main() {
  try {
    validateConfig();
  } catch (error) {
    logger.fatal({ error }, "Configuration validation failed");
    process.exit(1);
  }

  logger.info(
    {
      programId: config.programId,
      rpcUrl: config.rpcUrl,
      indexerMode: config.indexerMode,
      dbMode: config.dbMode,
    },
    "Starting 8004 Solana Indexer"
  );

  // Initialize Prisma only for local mode
  let prisma: PrismaClient | null = null;

  if (config.dbMode === "local") {
    prisma = new PrismaClient();
    try {
      await prisma.$connect();
      logger.info("Database connected (SQLite via Prisma)");
    } catch (error) {
      logger.fatal({ error }, "Failed to connect to database");
      process.exit(1);
    }
  } else {
    logger.info(
      { supabaseUrl: config.supabaseUrl },
      "Using Supabase for database (API via REST)"
    );
  }

  const processor = new Processor(prisma);

  await processor.start();

  // Start REST API server in local mode
  if (config.dbMode === "local" && prisma) {
    const apiPort = parseInt(process.env.API_PORT || "3001");
    await startApiServer({ prisma, port: apiPort });
    logger.info({ apiPort }, "REST API available at http://localhost:" + apiPort + "/rest/v1");
  }

  const shutdown = async (signal: string) => {
    logger.info({ signal }, "Shutdown signal received");

    try {
      await processor.stop();
      if (prisma) {
        await prisma.$disconnect();
      }
      logger.info("Shutdown complete");
      process.exit(0);
    } catch (error) {
      logger.error({ error }, "Error during shutdown");
      process.exit(1);
    }
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  logger.info("8004 Solana Indexer is running");
  logger.info("API available via Supabase REST: " + (config.supabaseUrl || "N/A"));
}

main().catch((error) => {
  logger.fatal({ error }, "Unhandled error");
  process.exit(1);
});
