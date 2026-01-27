import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const testDbPath = join(__dirname, '../../prisma/test.db');

// Set environment BEFORE any other imports
process.env.DATABASE_URL = process.env.DATABASE_URL || `file:${testDbPath}`;
process.env.RPC_URL = process.env.RPC_URL || "https://api.devnet.solana.com";
process.env.WS_URL = process.env.WS_URL || "wss://api.devnet.solana.com";
process.env.PROGRAM_ID = process.env.PROGRAM_ID || "3GGkAWC3mYYdud8GVBsKXK5QC9siXtFkWVZFYtbueVbC";
process.env.LOG_LEVEL = "silent";
process.env.INDEXER_MODE = "polling";

// Now import dependencies
import { vi, beforeAll, afterAll, beforeEach } from "vitest";
import { PrismaClient } from "@prisma/client";

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

  // Clean database before tests (SQLite uses DELETE, not TRUNCATE)
  try {
    await prisma.eventLog.deleteMany();
    await prisma.feedbackResponse.deleteMany();
    await prisma.orphanResponse.deleteMany();
    await prisma.validation.deleteMany();
    await prisma.feedback.deleteMany();
    await prisma.agentMetadata.deleteMany();
    await prisma.agent.deleteMany();
    await prisma.registry.deleteMany();
    await prisma.indexerState.deleteMany();
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
