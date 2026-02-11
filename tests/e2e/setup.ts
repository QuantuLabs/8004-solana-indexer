import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { execSync } from 'child_process';
import { unlinkSync, existsSync } from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const testDbPath = join(__dirname, '../../prisma/test.db');

// Set environment BEFORE any other imports
process.env.DATABASE_URL = `file:${testDbPath}`;
process.env.RPC_URL =
  process.env.HELIUS_DEVNET_URL ||
  process.env.DEVNET_RPC_URL ||
  process.env.RPC_URL ||
  "https://api.devnet.solana.com";
process.env.WS_URL = process.env.WS_URL || "wss://api.devnet.solana.com";
process.env.PROGRAM_ID = process.env.PROGRAM_ID || "3GGkAWC3mYYdud8GVBsKXK5QC9siXtFkWVZFYtbueVbC";
process.env.LOG_LEVEL = "silent";
process.env.INDEXER_MODE = "polling";
process.env.DB_MODE = "local";

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
  // Remove old test database if exists
  if (existsSync(testDbPath)) {
    unlinkSync(testDbPath);
  }

  // Push schema to create fresh test database
  execSync('bunx prisma db push --skip-generate', {
    cwd: join(__dirname, '../..'),
    env: { ...process.env, DATABASE_URL: `file:${testDbPath}` },
    stdio: 'pipe',
  });

  prisma = new PrismaClient();
  await prisma.$connect();
});

afterAll(async () => {
  await prisma.$disconnect();
  // Clean up test database
  if (existsSync(testDbPath)) {
    unlinkSync(testDbPath);
  }
});

beforeEach(() => {
  vi.clearAllMocks();
});

export { prisma };
