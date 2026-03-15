import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const repoRoot = resolve(__dirname, "../../..");

function hasSqlite3(): boolean {
  try {
    execFileSync("sqlite3", ["-version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

function runSql(dbPath: string, sql: string): string {
  return execFileSync("sqlite3", [dbPath], { input: sql, encoding: "utf8" }).trim();
}

function applyMigration(dbPath: string, relPath: string): void {
  const migrationPath = resolve(repoRoot, relPath);
  const sql = readFileSync(migrationPath, "utf8");
  runSql(dbPath, sql);
}

describe("collection_id migration execution", () => {
  it("keeps collection_id assignment conflict-free on sqlite upgrades", () => {
    if (!hasSqlite3()) {
      return;
    }

    const dir = mkdtempSync(resolve(tmpdir(), "idx-cid-mig-"));
    const dbPath = resolve(dir, "upgrade.db");

    try {
      const bootstrapMigrations = [
        "prisma/migrations/20260127131718_init/migration.sql",
        "prisma/migrations/20260128210625_remove_base_index/migration.sql",
        "prisma/migrations/20260129160641_reorg_resilience/migration.sql",
        "prisma/migrations/20260130_hash_chain_verification/migration.sql",
        "prisma/migrations/20260209150246_add_response_count_and_tx_index/migration.sql",
        "prisma/migrations/20260209150514_add_hash_chain_checkpoints/migration.sql",
        "prisma/migrations/20260225120000_v060_identity_extensions/migration.sql",
        "prisma/migrations/20260225183000_collection_pointer_registry/migration.sql",
        "prisma/migrations/20260225191500_agent_parent_tree_index/migration.sql",
        "prisma/migrations/20260225194000_collection_listing_indexes/migration.sql",
        "prisma/migrations/20260225220000_collection_metadata_fields/migration.sql",
        "prisma/migrations/20260226170000_add_event_ordinal/migration.sql",
        "prisma/migrations/20260227193000_add_feedback_revocation_ids/migration.sql",
        "prisma/migrations/20260227195500_add_orphan_response_count/migration.sql",
      ];

      for (const migration of bootstrapMigrations) {
        applyMigration(dbPath, migration);
      }

      runSql(
        dbPath,
        `
        INSERT INTO "CollectionPointer" (
          "col", "creator", "firstSeenAsset", "firstSeenAt", "firstSeenSlot", "firstSeenTxSignature",
          "lastSeenAt", "lastSeenSlot", "lastSeenTxSignature", "assetCount"
        ) VALUES
          ('c1:alpha', 'creator-a', 'asset-1', CURRENT_TIMESTAMP, 100, 'sig-100', CURRENT_TIMESTAMP, 100, 'sig-100', 1),
          ('c1:alpha', 'creator-b', 'asset-2', CURRENT_TIMESTAMP, 101, 'sig-101', CURRENT_TIMESTAMP, 101, 'sig-101', 1),
          ('c1:beta',  'creator-c', 'asset-3', CURRENT_TIMESTAMP, 102, 'sig-102', CURRENT_TIMESTAMP, 102, 'sig-102', 1),
          ('c1:gamma', 'creator-d', 'asset-4', CURRENT_TIMESTAMP, 103, 'sig-103', CURRENT_TIMESTAMP, 103, 'sig-103', 1);
        `
      );

      applyMigration(dbPath, "prisma/migrations/20260303161000_add_collection_sequential_id/migration.sql");
      applyMigration(dbPath, "prisma/migrations/20260304120000_add_agent_sequential_id/migration.sql");
      applyMigration(dbPath, "prisma/migrations/20260305113000_backfill_collection_sequential_id/migration.sql");
      applyMigration(dbPath, "prisma/migrations/20260305143000_add_indexer_state_monotonic_guard/migration.sql");
      applyMigration(dbPath, "prisma/migrations/20260311173000_add_collection_pointer_last_seen_tx_index/migration.sql");
      runSql(
        dbPath,
        `UPDATE "CollectionPointer" SET "collection_id" = NULL WHERE "col" = 'c1:gamma' AND "creator" = 'creator-d';`
      );
      applyMigration(dbPath, "prisma/migrations/20260312110000_add_local_collection_id_counter/migration.sql");

      runSql(
        dbPath,
        `
        INSERT INTO "CollectionPointer" (
          "col", "creator", "firstSeenAsset", "firstSeenAt", "firstSeenSlot", "firstSeenTxSignature",
          "lastSeenAt", "lastSeenSlot", "lastSeenTxSignature", "assetCount"
        ) VALUES (
          'c1:delta', 'creator-e', 'asset-5', CURRENT_TIMESTAMP, 104, 'sig-104',
          CURRENT_TIMESTAMP, 104, 'sig-104', 1
        );
        `
      );
      runSql(
        dbPath,
        `
        INSERT INTO "CollectionPointer" (
          "col", "creator", "firstSeenAsset", "firstSeenAt", "firstSeenSlot", "firstSeenTxSignature",
          "lastSeenAt", "lastSeenSlot", "lastSeenTxSignature", "assetCount"
        ) VALUES (
          'c1:delta', 'creator-e', 'asset-5', CURRENT_TIMESTAMP, 105, 'sig-105',
          CURRENT_TIMESTAMP, 105, 'sig-105', 2
        )
        ON CONFLICT("col", "creator") DO UPDATE SET
          "assetCount" = excluded."assetCount";
        `
      );

      const nullCount = Number(
        runSql(dbPath, `SELECT COUNT(*) FROM "CollectionPointer" WHERE "collection_id" IS NULL;`)
      );
      const dupCount = Number(
        runSql(
          dbPath,
          `
          SELECT COUNT(*)
          FROM (
            SELECT "collection_id", COUNT(*) AS c
            FROM "CollectionPointer"
            WHERE "collection_id" IS NOT NULL
            GROUP BY "collection_id"
            HAVING c > 1
          );
          `
        )
      );
      const triggerPresent = runSql(
        dbPath,
        `SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='IndexerState_monotonic_guard';`
      );
      const collectionIdTriggerCount = Number(
        runSql(
          dbPath,
          `
          SELECT COUNT(*)
          FROM sqlite_master
          WHERE type='trigger'
            AND name IN (
              'CollectionPointer_assign_collection_id_after_insert',
              'CollectionPointer_assign_collection_id_after_update'
            );
          `
        )
      );
      const counterSeed = runSql(
        dbPath,
        `SELECT "nextValue" FROM "IdCounter" WHERE "scope" = 'collection:global';`
      );
      const newCollectionId = runSql(
        dbPath,
        `SELECT "collection_id" FROM "CollectionPointer" WHERE "col" = 'c1:delta' AND "creator" = 'creator-e';`
      );
      const repairedCollectionId = runSql(
        dbPath,
        `SELECT "collection_id" FROM "CollectionPointer" WHERE "col" = 'c1:gamma' AND "creator" = 'creator-d';`
      );

      expect(nullCount).toBe(0);
      expect(dupCount).toBe(0);
      expect(Number(triggerPresent)).toBe(1);
      expect(collectionIdTriggerCount).toBe(2);
      expect(Number(counterSeed)).toBeGreaterThan(0);
      expect(Number(newCollectionId)).toBeGreaterThan(0);
      expect(Number(repairedCollectionId)).toBeGreaterThan(0);
    } finally {
      if (existsSync(dir)) {
        rmSync(dir, { recursive: true, force: true });
      }
    }
  });
});
