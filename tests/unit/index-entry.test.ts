import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const originalEnv = process.env;

const mockConnect = vi.fn();
const mockDisconnect = vi.fn();
const mockGetBaseCollection = vi.fn();
const mockValidateConfig = vi.fn();
const mockProcessorStart = vi.fn();
const mockProcessorStop = vi.fn();
const mockStartApiServer = vi.fn();
const mockAssertLocalCollectionIdSchema = vi.fn();
const mockRepairLocalCollectionIdSchema = vi.fn();
const mockLogger = {
  info: vi.fn(),
  warn: vi.fn(),
  error: vi.fn(),
  debug: vi.fn(),
  fatal: vi.fn(),
};

vi.mock("@prisma/client", () => ({
  PrismaClient: vi.fn(function MockPrismaClient() {
    return {
      $connect: mockConnect,
      $disconnect: mockDisconnect,
    };
  }),
}));

vi.mock("@solana/web3.js", () => ({
  Connection: vi.fn(function MockConnection() {
    return {};
  }),
}));

vi.mock("8004-solana", () => ({
  getBaseCollection: mockGetBaseCollection,
}));

vi.mock("../../src/logger.js", () => ({
  logger: mockLogger,
}));

vi.mock("../../src/indexer/processor.js", () => ({
  Processor: vi.fn(function MockProcessor() {
    return {
      start: mockProcessorStart,
      stop: mockProcessorStop,
    };
  }),
}));

vi.mock("../../src/api/server.js", () => ({
  startApiServer: mockStartApiServer,
}));

vi.mock("../../src/db/local-collection-id-schema.js", () => ({
  MISSING_COLLECTION_ID_SCHEMA_FATAL_MESSAGE:
    "Missing collection_id schema in local database. Apply Prisma migrations (prisma migrate deploy) before starting the indexer.",
  isMissingCollectionIdSchemaError: (error: unknown) => String(error).includes("Missing collection_id schema"),
  assertLocalCollectionIdSchema: mockAssertLocalCollectionIdSchema,
  repairLocalCollectionIdSchema: mockRepairLocalCollectionIdSchema,
}));

vi.mock("../../src/db/supabase.js", () => ({
  getPool: vi.fn(() => ({ query: vi.fn() })),
}));

vi.mock("../../src/parser/decoder.js", () => ({
  IDL_VERSION: "test-idl",
  IDL_PROGRAM_ID: "8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C",
}));

vi.mock("../../src/indexer/metadata-queue.js", () => ({
  metadataQueue: { shutdown: vi.fn() },
}));

vi.mock("../../src/indexer/collection-metadata-queue.js", () => ({
  collectionMetadataQueue: { shutdown: vi.fn() },
}));

vi.mock("../../src/config.js", () => ({
  config: {
    programId: "8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C",
    rpcUrl: "https://api.devnet.solana.com",
    indexerMode: "polling",
    dbMode: "local",
    apiMode: "rest",
    enableGraphql: false,
    apiPort: 3001,
    supabaseUrl: "",
    supabaseKey: "",
  },
  runtimeConfig: {
    baseCollection: null,
    initialized: false,
  },
  validateConfig: mockValidateConfig,
}));

describe("index.ts bootstrap", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = {
      ...originalEnv,
      API_MODE: "rest",
      DB_MODE: "local",
      ENABLE_GRAPHQL: "false",
      RPC_URL: "https://api.devnet.solana.com",
      PROGRAM_ID: "8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C",
      LOG_LEVEL: "silent",
    };
    mockConnect.mockResolvedValue(undefined);
    mockDisconnect.mockResolvedValue(undefined);
    mockGetBaseCollection.mockResolvedValue(null);
    mockValidateConfig.mockReturnValue(undefined);
    mockProcessorStart.mockResolvedValue(undefined);
    mockProcessorStop.mockResolvedValue(undefined);
    mockStartApiServer.mockResolvedValue({
      close: (cb: (err?: Error | null) => void) => cb(null),
    });
    mockAssertLocalCollectionIdSchema.mockResolvedValue(undefined);
    mockRepairLocalCollectionIdSchema.mockResolvedValue(undefined);
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  it("repairs missing local collection_id SQLite objects before starting the processor", async () => {
    mockAssertLocalCollectionIdSchema
      .mockRejectedValueOnce(new Error("Missing collection_id schema in local database. Apply Prisma migrations (prisma migrate deploy) before starting the indexer."))
      .mockResolvedValueOnce(undefined);

    const { main } = await import("../../src/index.js");
    await main();

    expect(mockRepairLocalCollectionIdSchema).toHaveBeenCalledTimes(1);
    expect(mockAssertLocalCollectionIdSchema).toHaveBeenCalledTimes(2);
    expect(mockStartApiServer).toHaveBeenCalledTimes(1);
    expect(mockProcessorStart).toHaveBeenCalledTimes(1);
    expect(mockRepairLocalCollectionIdSchema.mock.invocationCallOrder[0]).toBeLessThan(
      mockProcessorStart.mock.invocationCallOrder[0]
    );
  });

  it("runs the idempotent local sequential-id repair even when the schema is already present", async () => {
    const { main } = await import("../../src/index.js");
    await main();

    expect(mockAssertLocalCollectionIdSchema).toHaveBeenCalledTimes(2);
    expect(mockRepairLocalCollectionIdSchema).toHaveBeenCalledTimes(1);
    expect(mockRepairLocalCollectionIdSchema.mock.invocationCallOrder[0]).toBeLessThan(
      mockProcessorStart.mock.invocationCallOrder[0]
    );
  });
});
