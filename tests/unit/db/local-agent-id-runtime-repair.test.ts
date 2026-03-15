import { execFileSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { assertLocalCollectionIdSchema, repairLocalCollectionIdSchema } from "../../../src/db/local-collection-id-schema.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const repoRoot = resolve(__dirname, "../../..");

function dbPushFreshSqlite(dbPath: string): void {
  execFileSync("zsh", ["-lc", "bunx prisma db push --skip-generate"], {
    cwd: repoRoot,
    env: { ...process.env, DATABASE_URL: `file:${dbPath}` },
    stdio: "pipe",
  });
}

function createExecutor(db: DatabaseSync) {
  return {
    async $executeRawUnsafe(sql: string, ...params: unknown[]): Promise<number> {
      if (params.length > 0) {
        db.prepare(sql).run(...params);
      } else {
        db.exec(sql);
      }
      return 0;
    },
    async $queryRawUnsafe(sql: string): Promise<Array<Record<string, unknown>>> {
      return db.prepare(sql).all() as Array<Record<string, unknown>>;
    },
  };
}

describe("local SQLite agent_id runtime repair", () => {
  it("rejects an incomplete SQLite schema instead of false-passing quoted missing identifiers", async () => {
    const dir = mkdtempSync(resolve(tmpdir(), "idx-agent-repair-"));
    const dbPath = resolve(dir, "incomplete.db");

    try {
      const db = new DatabaseSync(dbPath);
      db.exec(`
        CREATE TABLE "CollectionPointer" (
          "col" TEXT NOT NULL,
          "creator" TEXT NOT NULL
        );
        CREATE TABLE "Agent" (
          "id" TEXT NOT NULL PRIMARY KEY,
          "status" TEXT
        );
        CREATE TABLE "IdCounter" (
          "scope" TEXT NOT NULL PRIMARY KEY,
          "nextValue" BIGINT NOT NULL,
          "updatedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO "IdCounter" ("scope", "nextValue") VALUES
          ('collection:global', 1),
          ('agent:global', 1);
        CREATE TRIGGER "CollectionPointer_assign_collection_id_after_insert"
        AFTER INSERT ON "CollectionPointer"
        BEGIN
          SELECT 1;
        END;
        CREATE TRIGGER "CollectionPointer_assign_collection_id_after_update"
        AFTER UPDATE ON "CollectionPointer"
        BEGIN
          SELECT 1;
        END;
        CREATE TRIGGER "Agent_assign_agent_id_after_insert"
        AFTER INSERT ON "Agent"
        BEGIN
          SELECT 1;
        END;
        CREATE TRIGGER "Agent_assign_agent_id_after_update"
        AFTER UPDATE ON "Agent"
        BEGIN
          SELECT 1;
        END;
      `);

      const executor = createExecutor(db);
      await expect(assertLocalCollectionIdSchema(executor as never)).rejects.toThrow();

      db.close();
    } finally {
      if (existsSync(dir)) {
        rmSync(dir, { recursive: true, force: true });
      }
    }
  });

  it("backfills legacy null agent_id rows deterministically and assigns new ids at ingest time", async () => {
    const dir = mkdtempSync(resolve(tmpdir(), "idx-agent-repair-"));
    const dbPath = resolve(dir, "fresh.db");

    try {
      dbPushFreshSqlite(dbPath);

      const db = new DatabaseSync(dbPath);
      db.exec(`
        INSERT INTO "Agent" (
          "id", "owner", "uri", "nftName", "collection", "registry",
          "createdAt", "updatedAt", "createdSlot", "txIndex", "eventOrdinal", "createdTxSignature", "status"
        ) VALUES
          ('asset-b', 'owner-b', 'uri-b', '', 'collection-1', 'registry-1',
           '2026-03-15T10:00:00.000Z', '2026-03-15T10:00:00.000Z', 11, 0, 0, 'sig-b', 'PENDING'),
          ('asset-a', 'owner-a', 'uri-a', '', 'collection-1', 'registry-1',
           '2026-03-15T09:00:00.000Z', '2026-03-15T09:00:00.000Z', 10, 5, 0, 'sig-a', 'PENDING'),
          ('asset-orphan', 'owner-o', 'uri-o', '', 'collection-1', 'registry-1',
           '2026-03-15T08:00:00.000Z', '2026-03-15T08:00:00.000Z', 9, 0, 0, 'sig-o', 'ORPHANED');
      `);

      const executor = createExecutor(db);
      await repairLocalCollectionIdSchema(executor as never);
      await expect(assertLocalCollectionIdSchema(executor as never)).resolves.toBeUndefined();

      const repaired = db
        .prepare(`SELECT "id", "agent_id" AS agentId FROM "Agent" ORDER BY "id" ASC`)
        .all() as Array<{ id: string; agentId: number | null }>;

      expect(repaired).toEqual([
        { id: "asset-a", agentId: 1 },
        { id: "asset-b", agentId: 2 },
        { id: "asset-orphan", agentId: null },
      ]);

      db.exec(`
        INSERT INTO "Agent" (
          "id", "owner", "uri", "nftName", "collection", "registry",
          "createdAt", "updatedAt", "createdSlot", "txIndex", "eventOrdinal", "createdTxSignature", "status"
        ) VALUES (
          'asset-c', 'owner-c', 'uri-c', '', 'collection-1', 'registry-1',
          '2026-03-15T11:00:00.000Z', '2026-03-15T11:00:00.000Z', 12, 0, 0, 'sig-c', 'PENDING'
        );
      `);

      const inserted = db
        .prepare(`SELECT "agent_id" AS agentId FROM "Agent" WHERE "id" = 'asset-c'`)
        .get() as { agentId: number | null };
      expect(inserted.agentId).toBe(3);

      db.exec(`
        UPDATE "Agent"
        SET "status" = 'PENDING', "updatedAt" = '2026-03-15T12:00:00.000Z'
        WHERE "id" = 'asset-orphan';
      `);

      const updated = db
        .prepare(`SELECT "agent_id" AS agentId FROM "Agent" WHERE "id" = 'asset-orphan'`)
        .get() as { agentId: number | null };
      expect(updated.agentId).toBe(4);

      const counter = db
        .prepare(`SELECT "nextValue" AS nextValue FROM "IdCounter" WHERE "scope" = 'agent:global'`)
        .get() as { nextValue: number };
      expect(counter.nextValue).toBe(5);

      db.close();
    } finally {
      if (existsSync(dir)) {
        rmSync(dir, { recursive: true, force: true });
      }
    }
  });

  it("re-writes drifted non-orphan agent_id values into canonical order and preserves orphan ids", async () => {
    const dir = mkdtempSync(resolve(tmpdir(), "idx-agent-repair-"));
    const dbPath = resolve(dir, "drifted.db");

    try {
      dbPushFreshSqlite(dbPath);

      const db = new DatabaseSync(dbPath);
      db.exec(`
        INSERT INTO "Agent" (
          "id", "owner", "uri", "nftName", "collection", "registry",
          "createdAt", "updatedAt", "createdSlot", "txIndex", "eventOrdinal", "createdTxSignature", "agent_id", "status"
        ) VALUES
          ('asset-b', 'owner-b', 'uri-b', '', 'collection-1', 'registry-1',
           '2026-03-15T10:00:00.000Z', '2026-03-15T10:00:00.000Z', 11, 0, 0, 'sig-b', 1, 'PENDING'),
          ('asset-a', 'owner-a', 'uri-a', '', 'collection-1', 'registry-1',
           '2026-03-15T09:00:00.000Z', '2026-03-15T09:00:00.000Z', 10, 5, 0, 'sig-a', 2, 'PENDING'),
          ('asset-orphan', 'owner-o', 'uri-o', '', 'collection-1', 'registry-1',
           '2026-03-15T08:00:00.000Z', '2026-03-15T08:00:00.000Z', 9, 0, 0, 'sig-o', 500, 'ORPHANED');
      `);

      const executor = createExecutor(db);
      await repairLocalCollectionIdSchema(executor as never);

      const repaired = db
        .prepare(`SELECT "id", "agent_id" AS agentId FROM "Agent" ORDER BY "id" ASC`)
        .all() as Array<{ id: string; agentId: number | null }>;

      expect(repaired).toEqual([
        { id: "asset-a", agentId: 1 },
        { id: "asset-b", agentId: 2 },
        { id: "asset-orphan", agentId: 500 },
      ]);

      db.exec(`
        INSERT INTO "Agent" (
          "id", "owner", "uri", "nftName", "collection", "registry",
          "createdAt", "updatedAt", "createdSlot", "txIndex", "eventOrdinal", "createdTxSignature", "status"
        ) VALUES (
          'asset-c', 'owner-c', 'uri-c', '', 'collection-1', 'registry-1',
          '2026-03-15T11:00:00.000Z', '2026-03-15T11:00:00.000Z', 12, 0, 0, 'sig-c', 'PENDING'
        );
      `);

      const inserted = db
        .prepare(`SELECT "agent_id" AS agentId FROM "Agent" WHERE "id" = 'asset-c'`)
        .get() as { agentId: number | null };
      expect(inserted.agentId).toBe(501);

      db.close();
    } finally {
      if (existsSync(dir)) {
        rmSync(dir, { recursive: true, force: true });
      }
    }
  });
});
