import { vi, beforeAll, afterAll, beforeEach } from "vitest";
import { PrismaClient } from "@prisma/client";

// Mock environment for e2e tests
process.env.DATABASE_URL =
  process.env.DATABASE_URL ||
  "postgresql://postgres:postgres@localhost:5432/indexer8004_test?schema=public";
process.env.RPC_URL =
  process.env.RPC_URL || "https://api.devnet.solana.com";
process.env.WS_URL = process.env.WS_URL || "wss://api.devnet.solana.com";
process.env.PROGRAM_ID =
  process.env.PROGRAM_ID || "3GGkAWC3mYYdud8GVBsKXK5QC9siXtFkWVZFYtbueVbC";
process.env.LOG_LEVEL = "silent";
process.env.INDEXER_MODE = "polling";

// Mock pino logger to be silent during tests
vi.mock("pino", () => {
  const mockLogger = {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    fatal: vi.fn(),
    child: vi.fn(() => mockLogger),
  };
  return { default: vi.fn(() => mockLogger) };
});

// Create test prisma client
let prisma: PrismaClient;

beforeAll(async () => {
  prisma = new PrismaClient();

  // Clean database before tests
  try {
    await prisma.$executeRaw`TRUNCATE TABLE "EventLog" CASCADE`;
    await prisma.$executeRaw`TRUNCATE TABLE "FeedbackResponse" CASCADE`;
    await prisma.$executeRaw`TRUNCATE TABLE "Validation" CASCADE`;
    await prisma.$executeRaw`TRUNCATE TABLE "Feedback" CASCADE`;
    await prisma.$executeRaw`TRUNCATE TABLE "AgentMetadata" CASCADE`;
    await prisma.$executeRaw`TRUNCATE TABLE "Agent" CASCADE`;
    await prisma.$executeRaw`TRUNCATE TABLE "Registry" CASCADE`;
    await prisma.$executeRaw`TRUNCATE TABLE "IndexerState" CASCADE`;
  } catch {
    // Tables may not exist yet, that's fine
  }
});

afterAll(async () => {
  await prisma.$disconnect();
});

beforeEach(() => {
  vi.clearAllMocks();
});

export { prisma };
