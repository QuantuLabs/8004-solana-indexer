import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { PrismaClient } from "@prisma/client";
import { describe, expect, it } from "vitest";
import { trySaveLocalIndexerStateWithSql } from "../../../src/db/indexer-state-local.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const repoRoot = resolve(__dirname, "../../..");

function migrateFreshSqlite(dbPath: string): void {
  execFileSync("zsh", ["-lc", "bunx prisma migrate deploy"], {
    cwd: repoRoot,
    env: { ...process.env, DATABASE_URL: `file:${dbPath}` },
    stdio: "pipe",
  });
}

describe("local sqlite indexer state writes", () => {
  it("uses SQL monotonic upsert to avoid trigger-induced Prisma upsert failures", async () => {
    const dir = mkdtempSync(resolve(tmpdir(), "idx-indexer-state-"));
    const dbPath = resolve(dir, "fresh.db");
    const previousDatabaseUrl = process.env.DATABASE_URL;

    try {
      migrateFreshSqlite(dbPath);
      process.env.DATABASE_URL = `file:${dbPath}`;

      const prisma = new PrismaClient({
        datasources: {
          db: {
            url: `file:${dbPath}`,
          },
        },
      });

      try {
        await trySaveLocalIndexerStateWithSql(prisma, {
          signature: "sig-z",
          slot: 10n,
          txIndex: 1,
          source: "poller",
        });

        await expect(
          trySaveLocalIndexerStateWithSql(prisma, {
            signature: "sig-a",
            slot: 10n,
            txIndex: 1,
            source: "poller",
          })
        ).resolves.toBe(true);

        const stateAfterStaleWrite = await prisma.indexerState.findUnique({
          where: { id: "main" },
        });

        expect(stateAfterStaleWrite?.lastSignature).toBe("sig-z");
        expect(stateAfterStaleWrite?.lastSlot).toBe(10n);
        expect(stateAfterStaleWrite?.lastTxIndex).toBe(1);

        await trySaveLocalIndexerStateWithSql(prisma, {
          signature: "sig-y",
          slot: 10n,
          txIndex: 2,
          source: "websocket",
        });

        const advancedState = await prisma.indexerState.findUnique({
          where: { id: "main" },
        });

        expect(advancedState?.lastSignature).toBe("sig-y");
        expect(advancedState?.lastSlot).toBe(10n);
        expect(advancedState?.lastTxIndex).toBe(2);
        expect(advancedState?.source).toBe("websocket");
      } finally {
        await prisma.$disconnect();
      }
    } finally {
      process.env.DATABASE_URL = previousDatabaseUrl;
      if (existsSync(dir)) {
        rmSync(dir, { recursive: true, force: true });
      }
    }
  });
});
