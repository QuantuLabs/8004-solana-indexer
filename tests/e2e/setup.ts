import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { execSync } from 'child_process';
import { mkdtempSync, rmSync } from 'fs';
import { tmpdir } from 'os';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const testDbDir = mkdtempSync(join(tmpdir(), 'indexer-e2e-'));
const testDbPath = join(testDbDir, 'test.db');

// Set environment BEFORE any other imports
process.env.DATABASE_URL = `file:${testDbPath}`;
process.env.RPC_URL =
  process.env.HELIUS_DEVNET_URL ||
  process.env.DEVNET_RPC_URL ||
  process.env.RPC_URL ||
  "https://api.devnet.solana.com";
const isLocalnetRpc = /https?:\/\/(localhost|127\.0\.0\.1):8899\b/.test(process.env.RPC_URL);
process.env.WS_URL = process.env.WS_URL || (isLocalnetRpc ? "ws://localhost:8900" : "wss://api.devnet.solana.com");
process.env.PROGRAM_ID =
  process.env.PROGRAM_ID ||
  (isLocalnetRpc
    ? "8oo4dC4JvBLwy5tGgiH3WwK4B9PWxL9Z4XjA2jzkQMbQ"
    : "8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C");
process.env.ATOM_ENGINE_PROGRAM_ID =
  process.env.ATOM_ENGINE_PROGRAM_ID || "AToMufS4QD6hEXvcvBDg9m1AHeCLpmZQsyfYa5h9MwAF";
process.env.LOG_LEVEL = "silent";
process.env.INDEXER_MODE = process.env.INDEXER_MODE || "polling";
process.env.DB_MODE = "local";
process.env.ENABLE_PROOFPASS = "false";

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
  if (prisma) {
    await prisma.$disconnect();
  }
  rmSync(testDbDir, { recursive: true, force: true });
});

beforeEach(() => {
  vi.clearAllMocks();
});

export { prisma };
