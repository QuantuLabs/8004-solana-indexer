import { PrismaClient } from "@prisma/client";
import { Connection } from "@solana/web3.js";
import { Server } from "http";
import { getBaseCollection } from "8004-solana";
import { config, validateConfig, runtimeConfig } from "./config.js";
import { logger } from "./logger.js";
import { Processor } from "./indexer/processor.js";
import { startApiServer } from "./api/server.js";
import { cleanupOrphanResponses } from "./db/handlers.js";
import { getPool } from "./db/supabase.js";
import { IDL_VERSION, IDL_PROGRAM_ID } from "./parser/decoder.js";

async function main() {
  try {
    validateConfig();
  } catch (error) {
    logger.fatal({ error }, "Configuration validation failed");
    process.exit(1);
  }

  // IDL/SDK version validation
  if (IDL_PROGRAM_ID !== config.programId) {
    logger.warn(
      { idlProgramId: IDL_PROGRAM_ID, configProgramId: config.programId },
      "IDL program ID mismatch - events may fail to parse"
    );
  }
  logger.info(
    {
      programId: config.programId,
      idlVersion: IDL_VERSION,
      rpcUrl: config.rpcUrl,
      indexerMode: config.indexerMode,
      dbMode: config.dbMode,
    },
    "Starting 8004 Solana Indexer"
  );

  // Fetch base collection from on-chain using SDK
  const connection = new Connection(config.rpcUrl, "confirmed");
  try {
    const baseCollection = await getBaseCollection(connection);

    if (baseCollection) {
      runtimeConfig.baseCollection = baseCollection.toBase58();
      runtimeConfig.initialized = true;
      logger.info(
        { baseCollection: runtimeConfig.baseCollection },
        "Fetched base collection from on-chain via SDK"
      );
    } else {
      logger.warn("Base collection not found on-chain - indexing all collections");
    }
  } catch (error) {
    logger.error({ error }, "Failed to fetch base collection from on-chain");
  }

  // Initialize Prisma only for local mode
  let prisma: PrismaClient | null = null;

  if (config.dbMode === "local") {
    prisma = new PrismaClient();
    try {
      await prisma.$connect();
      logger.info("Database connected (SQLite via Prisma)");
      // Cleanup old orphan responses at startup (> 30 min)
      await cleanupOrphanResponses(prisma);
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

  const pool = config.dbMode === "supabase" ? getPool() : null;
  const processor = new Processor(prisma, pool);

  await processor.start();

  // Start REST API server in local mode
  let apiServer: Server | null = null;
  if (config.dbMode === "local" && prisma) {
    const apiPort = parseInt(process.env.API_PORT || "3001");
    apiServer = await startApiServer({ prisma, port: apiPort });
    logger.info({ apiPort }, "REST API available at http://localhost:" + apiPort + "/rest/v1");
  }

  const shutdown = async (signal: string) => {
    logger.info({ signal }, "Shutdown signal received");

    try {
      await processor.stop();
      if (apiServer) {
        await new Promise<void>((resolve, reject) => {
          apiServer!.close((err) => err ? reject(err) : resolve());
        });
        logger.info("API server closed");
      }
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
