import { execFileSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

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

describe("sqlite runtime schema migration", () => {
  it("adds all Agent runtime columns required by the local prisma runtime", () => {
    const dir = mkdtempSync(resolve(tmpdir(), "idx-agent-runtime-cols-"));
    const dbPath = resolve(dir, "fresh.db");

    try {
      migrateFreshSqlite(dbPath);

      const db = new DatabaseSync(dbPath);
      const rows = db.prepare(`PRAGMA table_info("Agent")`).all() as Array<{ name: string }>;
      db.close();

      const columns = new Set(rows.map((row) => row.name));
      expect(columns.has("trustTier")).toBe(true);
      expect(columns.has("qualityScore")).toBe(true);
      expect(columns.has("confidence")).toBe(true);
      expect(columns.has("riskScore")).toBe(true);
      expect(columns.has("diversityRatio")).toBe(true);
      expect(columns.has("feedbackCount")).toBe(true);
      expect(columns.has("rawAvgScore")).toBe(true);
      expect(columns.has("txIndex")).toBe(true);
      for (const column of [
        "owner_order_key", "wallet_order_key", "uri_order_key", "atom_enabled_order_key",
        "collection_order_key", "parent_order_key", "atom_stats_order_key",
      ]) {
        expect(columns.has(column)).toBe(true);
      }
    } finally {
      if (existsSync(dir)) {
        rmSync(dir, { recursive: true, force: true });
      }
    }
  });

  it("seeds NULL group clocks at the fixed cutover floor and preserves clocks on repeat", () => {
    const db = new DatabaseSync(":memory:");
    db.exec(`
      CREATE TABLE "Agent" (
        "id" TEXT PRIMARY KEY, "owner" TEXT, "createdSlot" BIGINT, "txIndex" INTEGER,
        "eventOrdinal" INTEGER, "createdTxSignature" TEXT, "mutation_order_key" TEXT
      );
      CREATE TABLE "IndexerState" (
        "id" TEXT PRIMARY KEY, "lastSlot" BIGINT, "lastTxIndex" INTEGER, "lastSignature" TEXT
      );
      INSERT INTO "Agent" VALUES ('a', 'unchanged', 10, 1, 2, 'create', '00000000000000000020:0000000001:0000000002:mutation');
      INSERT INTO "Agent" VALUES ('no-anchor', 'still-unchanged', NULL, NULL, NULL, NULL, NULL);
      INSERT INTO "IndexerState" VALUES ('main', 30, NULL, 'cursor');
    `);
    const migration = readFileSync(resolve(repoRoot, "prisma/migrations/20260908160000_agent_field_order_keys/migration.sql"), "utf8");
    db.exec(migration);

    const floor = "00000000000000000030:2147483647:2147483647:cursor";
    const row = db.prepare(`SELECT * FROM "Agent" WHERE "id" = 'a'`).get() as Record<string, unknown>;
    expect(row.owner).toBe("unchanged");
    for (const column of [
      "owner_order_key", "wallet_order_key", "uri_order_key", "atom_enabled_order_key",
      "collection_order_key", "parent_order_key", "atom_stats_order_key",
    ]) {
      expect(row[column]).toBe(floor);
    }
    expect((db.prepare(`SELECT "owner_order_key" FROM "Agent" WHERE "id" = 'no-anchor'`).get() as any).owner_order_key).toBe(floor);

    db.exec(`
      UPDATE "Agent" SET "uri_order_key" = 'preserved' WHERE "id" = 'a';
      DELETE FROM "IndexerState";
      INSERT INTO "Agent" ("id", "owner") VALUES ('truly-no-anchor', 'unchanged');
    `);
    db.exec(migration.slice(migration.indexOf("WITH anchors AS")));
    expect((db.prepare(`SELECT "uri_order_key" FROM "Agent" WHERE "id" = 'a'`).get() as any).uri_order_key).toBe("preserved");
    expect((db.prepare(`SELECT "owner_order_key" FROM "Agent" WHERE "id" = 'truly-no-anchor'`).get() as any).owner_order_key).toBeNull();
    db.close();
  });

  it("adds OrphanResponse.runningDigest required by local orphan replay", () => {
    const dir = mkdtempSync(resolve(tmpdir(), "idx-agent-runtime-cols-"));
    const dbPath = resolve(dir, "fresh.db");

    try {
      migrateFreshSqlite(dbPath);

      const db = new DatabaseSync(dbPath);
      const rows = db.prepare(`PRAGMA table_info("OrphanResponse")`).all() as Array<{ name: string }>;
      db.close();

      const columns = new Set(rows.map((row) => row.name));
      expect(columns.has("runningDigest")).toBe(true);
    } finally {
      if (existsSync(dir)) {
        rmSync(dir, { recursive: true, force: true });
      }
    }
  });

  it("stores Feedback.value as TEXT in SQLite to match the runtime contract", () => {
    const dir = mkdtempSync(resolve(tmpdir(), "idx-agent-runtime-cols-"));
    const dbPath = resolve(dir, "fresh.db");

    try {
      migrateFreshSqlite(dbPath);

      const db = new DatabaseSync(dbPath);
      const rows = db
        .prepare(`PRAGMA table_info("Feedback")`)
        .all() as Array<{ name: string; type: string }>;
      db.close();

      const valueColumn = rows.find((row) => row.name === "value");
      expect(valueColumn?.type).toBe("TEXT");
    } finally {
      if (existsSync(dir)) {
        rmSync(dir, { recursive: true, force: true });
      }
    }
  });
});
