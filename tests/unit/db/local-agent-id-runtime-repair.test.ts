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

  it("preserves assigned IDs, counters, and cursor across repeated startup repair", async () => {
    const dir = mkdtempSync(resolve(tmpdir(), "idx-agent-repair-"));
    const dbPath = resolve(dir, "fresh.db");

    try {
      dbPushFreshSqlite(dbPath);

      const db = new DatabaseSync(dbPath);
      const executor = createExecutor(db);
      await repairLocalCollectionIdSchema(executor as never);
      db.exec(`
        INSERT INTO "Agent" (
          "id", "owner", "uri", "nftName", "collection", "registry",
          "createdAt", "updatedAt", "createdSlot", "txIndex", "eventOrdinal", "createdTxSignature", "agent_id", "status"
        ) VALUES
          ('asset-b', 'owner-b', 'uri-b', '', 'collection-1', 'registry-1',
           '2026-03-15T10:00:00.000Z', '2026-03-15T10:00:00.000Z', 11, 0, 0, 'sig-b', 2, 'PENDING'),
          ('asset-a', 'owner-a', 'uri-a', '', 'collection-1', 'registry-1',
           '2026-03-15T09:00:00.000Z', '2026-03-15T09:00:00.000Z', 10, 5, 0, 'sig-a', 1, 'ORPHANED');
        UPDATE "IdCounter"
        SET "nextValue" = 3, "updatedAt" = '2026-03-15T12:00:00.000Z'
        WHERE "scope" = 'agent:global';
        INSERT INTO "IndexerState" (
          "id", "lastSignature", "lastSlot", "lastTxIndex", "source", "updatedAt"
        ) VALUES (
          'main', 'cursor-signature', 123, 4, 'poller', '2026-03-15T12:30:00.000Z'
        );
      `);

      await repairLocalCollectionIdSchema(executor as never);
      await expect(assertLocalCollectionIdSchema(executor as never)).resolves.toBeUndefined();
      await repairLocalCollectionIdSchema(executor as never);

      const repaired = db
        .prepare(`SELECT "id", "agent_id" AS agentId FROM "Agent" ORDER BY "id" ASC`)
        .all() as Array<{ id: string; agentId: number | null }>;

      expect(repaired).toEqual([
        { id: "asset-a", agentId: 1 },
        { id: "asset-b", agentId: 2 },
      ]);
      expect(
        db.prepare(`SELECT "nextValue", "updatedAt" FROM "IdCounter" WHERE "scope" = 'agent:global'`).get()
      ).toEqual({ nextValue: 3, updatedAt: "2026-03-15T12:00:00.000Z" });
      expect(
        db.prepare(`SELECT * FROM "IndexerState" WHERE "id" = 'main'`).get()
      ).toEqual({
        id: "main",
        lastSignature: "cursor-signature",
        lastSlot: 123,
        lastTxIndex: 4,
        source: "poller",
        updatedAt: "2026-03-15T12:30:00.000Z",
      });

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

      db.close();
    } finally {
      if (existsSync(dir)) {
        rmSync(dir, { recursive: true, force: true });
      }
    }
  });

  it("preserves an existing high ID and assigns the next unused ID", async () => {
    const dir = mkdtempSync(resolve(tmpdir(), "idx-agent-repair-"));
    const dbPath = resolve(dir, "drifted.db");

    try {
      dbPushFreshSqlite(dbPath);

      const db = new DatabaseSync(dbPath);
      const executor = createExecutor(db);
      await repairLocalCollectionIdSchema(executor as never);
      db.exec(`
        INSERT INTO "Agent" (
          "id", "owner", "uri", "nftName", "collection", "registry",
          "createdAt", "updatedAt", "createdSlot", "txIndex", "eventOrdinal", "createdTxSignature", "agent_id", "status"
        ) VALUES
          ('asset-77', 'owner-77', 'uri-77', '', 'collection-1', 'registry-1',
           '2026-03-15T10:00:00.000Z', '2026-03-15T10:00:00.000Z', 11, 0, 0, 'sig-77', 77, 'PENDING');
      `);

      await repairLocalCollectionIdSchema(executor as never);

      expect(db.prepare(`SELECT "agent_id" AS agentId FROM "Agent" WHERE "id" = 'asset-77'`).get())
        .toEqual({ agentId: 77 });

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
      expect(inserted.agentId).toBe(78);

      db.close();
    } finally {
      if (existsSync(dir)) {
        rmSync(dir, { recursive: true, force: true });
      }
    }
  });

  it("backfills missing IDs from an already higher monotonic counter", async () => {
    const dir = mkdtempSync(resolve(tmpdir(), "idx-agent-repair-"));
    const dbPath = resolve(dir, "counter-floor.db");

    try {
      dbPushFreshSqlite(dbPath);

      const db = new DatabaseSync(dbPath);
      const executor = createExecutor(db);
      await repairLocalCollectionIdSchema(executor as never);
      db.exec(`
        DROP TRIGGER "Agent_assign_agent_id_after_insert";
        DROP TRIGGER "Agent_assign_agent_id_after_update";
        INSERT INTO "Agent" (
          "id", "owner", "uri", "nftName", "collection", "registry",
          "createdAt", "updatedAt", "createdSlot", "txIndex", "eventOrdinal", "createdTxSignature", "agent_id", "status"
        ) VALUES
          ('asset-77', 'owner-77', 'uri-77', '', 'collection-1', 'registry-1',
           '2026-03-15T10:00:00.000Z', '2026-03-15T10:00:00.000Z', 10, 0, 0, 'sig-77', 77, 'PENDING'),
          ('asset-missing', 'owner-m', 'uri-m', '', 'collection-1', 'registry-1',
           '2026-03-15T11:00:00.000Z', '2026-03-15T11:00:00.000Z', 11, 0, 0, 'sig-m', NULL, 'PENDING');
        UPDATE "IdCounter"
        SET "nextValue" = 100
        WHERE "scope" = 'agent:global';
      `);

      await repairLocalCollectionIdSchema(executor as never);

      expect(
        db.prepare(`SELECT "id", "agent_id" AS agentId FROM "Agent" ORDER BY "id"`).all()
      ).toEqual([
        { id: "asset-77", agentId: 77 },
        { id: "asset-missing", agentId: 100 },
      ]);
      expect(
        db.prepare(`SELECT "nextValue" AS nextValue FROM "IdCounter" WHERE "scope" = 'agent:global'`).get()
      ).toEqual({ nextValue: 101 });

      db.close();
    } finally {
      if (existsSync(dir)) {
        rmSync(dir, { recursive: true, force: true });
      }
    }
  });
});
