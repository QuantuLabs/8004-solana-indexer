import { execFileSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
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
    } finally {
      if (existsSync(dir)) {
        rmSync(dir, { recursive: true, force: true });
      }
    }
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
