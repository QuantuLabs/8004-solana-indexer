import { PrismaClient } from "@prisma/client";
import { Connection } from "@solana/web3.js";
import { Server } from "http";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { getBaseCollection } from "8004-solana";
import { config, validateConfig, runtimeConfig } from "./config.js";
import { logger } from "./logger.js";
import { Processor } from "./indexer/processor.js";
import { startApiServer } from "./api/server.js";
import { getPool } from "./db/supabase.js";
import { IDL_VERSION, IDL_PROGRAM_ID } from "./parser/decoder.js";
import { metadataQueue } from "./indexer/metadata-queue.js";
import { collectionMetadataQueue } from "./indexer/collection-metadata-queue.js";
import {
  MISSING_COLLECTION_ID_SCHEMA_FATAL_MESSAGE,
  assertLocalCollectionIdSchema,
  isMissingCollectionIdSchemaError,
  repairLocalCollectionIdSchema,
} from "./db/local-collection-id-schema.js";

export async function main() {
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
  const connection = new Connection(config.rpcUrl, {
    commitment: "confirmed",
    disableRetryOnRateLimit: true,
  });
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
      let missingSequentialSchema = false;
      try {
        await assertLocalCollectionIdSchema(prisma);
      } catch (error) {
        if (!isMissingCollectionIdSchemaError(error)) {
          throw error;
        }
        missingSequentialSchema = true;
        logger.warn(
          { error },
          "Local sequential-id schema missing; applying idempotent SQLite repair"
        );
      }
      await repairLocalCollectionIdSchema(prisma);
      await assertLocalCollectionIdSchema(prisma);
      if (!missingSequentialSchema) {
        logger.info("Applied idempotent SQLite sequential-id repair");
      }
    } catch (error) {
      if (isMissingCollectionIdSchemaError(error)) {
        logger.fatal({ error }, MISSING_COLLECTION_ID_SCHEMA_FATAL_MESSAGE);
      } else {
        logger.fatal({ error }, "Failed to connect to database");
      }
      process.exit(1);
    }
  } else {
    logger.info(
      { supabaseUrl: config.supabaseUrl },
      "Using Supabase for database (API via GraphQL)"
    );
  }

  const pool = config.dbMode === "supabase" ? getPool() : null;
  const processor = new Processor(prisma, pool);
  let apiReady = false;

  const wantsRest = config.apiMode !== "graphql";
  const wantsGraphql = config.apiMode !== "rest" && config.enableGraphql;
  const hasSupabaseRestUrl = typeof config.supabaseUrl === "string" && config.supabaseUrl.trim().length > 0;
  const hasSupabaseKey = typeof config.supabaseKey === "string" && config.supabaseKey.trim().length > 0;
  const restProxyMissingKey = wantsRest && !prisma && !!pool && hasSupabaseRestUrl && !hasSupabaseKey;
  const restProxyEnabled = wantsRest && !prisma && !!pool && hasSupabaseRestUrl && hasSupabaseKey;
  const canServeRest = wantsRest && (!!prisma || restProxyEnabled);
  const canServeGraphql = wantsGraphql && !!pool;

  if (config.apiMode === "rest" && !canServeRest) {
    logger.fatal(
      {
        apiMode: config.apiMode,
        dbMode: config.dbMode,
        hasSupabaseUrl: hasSupabaseRestUrl,
        hasSupabaseKey,
      },
      "REST mode requires Prisma (DB_MODE=local) or Supabase PostgREST proxy auth (SUPABASE_URL + SUPABASE_KEY, or POSTGREST_URL + POSTGREST_TOKEN)"
    );
    process.exit(1);
  }

  if (config.apiMode === "graphql" && !pool) {
    logger.fatal(
      { apiMode: config.apiMode, dbMode: config.dbMode },
      "GraphQL mode requires DB_MODE=supabase (PostgreSQL pool)"
    );
    process.exit(1);
  }

  if (config.apiMode === "both" && wantsRest && !canServeRest) {
    logger.warn(
      {
        apiMode: config.apiMode,
        dbMode: config.dbMode,
        hasPool: !!pool,
        hasSupabaseUrl: hasSupabaseRestUrl,
        hasSupabaseKey,
        restProxyMissingKey,
      },
      restProxyMissingKey
        ? "REST disabled in API_MODE=both because SUPABASE_KEY (or POSTGREST_TOKEN) is missing for Supabase REST proxy"
        : "REST disabled in API_MODE=both because no REST backend is available"
    );
  }

  if (config.apiMode === "both" && wantsGraphql && !pool) {
    logger.warn(
      { apiMode: config.apiMode, dbMode: config.dbMode },
      "GraphQL disabled in API_MODE=both because Supabase pool is unavailable"
    );
  }

  let apiServer: Server | null = null;

  try {
    if (canServeRest || canServeGraphql) {
      const apiPort = parseInt(process.env.API_PORT || "3001");
      apiServer = await startApiServer({
        prisma,
        pool,
        port: apiPort,
        isReady: () => apiReady,
        isCaughtUp: () => processor.isCaughtUp(),
      });
      logger.info(
        {
          apiPort,
          apiMode: config.apiMode,
          restEnabled: canServeRest,
          restProxyEnabled,
          graphqlEnabled: canServeGraphql,
        },
        "API available"
      );
      if (canServeGraphql) {
        logger.info({ apiPort }, `GraphQL endpoint: http://localhost:${apiPort}/v2/graphql`);
      }
    }

    await processor.start();
    apiReady = true;
  } catch (error) {
    if (apiServer) {
      try {
        await new Promise<void>((resolve, reject) => {
          apiServer!.close((err) => err ? reject(err) : resolve());
        });
      } catch (closeError) {
        logger.error({ error: closeError }, "Failed to close API server after startup error");
      }
      apiServer = null;
    }

    if (prisma) {
      try {
        await prisma.$disconnect();
      } catch (disconnectError) {
        logger.error({ error: disconnectError }, "Failed to disconnect Prisma after startup error");
      }
    }

    throw error;
  }

  const shutdown = async (signal: string) => {
    logger.info({ signal }, "Shutdown signal received");

    try {
      metadataQueue.shutdown();
      collectionMetadataQueue.shutdown();
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
  if (canServeGraphql) {
    logger.info("API available via GraphQL endpoint");
  }
}

function isDirectExecution(): boolean {
  const entryPath = process.argv[1];
  if (!entryPath) return false;
  return resolve(entryPath) === fileURLToPath(import.meta.url);
}

if (isDirectExecution()) {
  main().catch((error) => {
    logger.fatal({ error }, "Unhandled error");
    process.exit(1);
  });
}
