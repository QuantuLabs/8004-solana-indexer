import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const bundleSql = readFileSync(
  resolve(__dirname, "../../../prisma/legacy-upgrades/v1.7.7-dbpush-to-current.sql"),
  "utf8"
);

describe("legacy SQLite v1.7.7 db-push upgrade bundle", () => {
  it("includes only the proven subset needed by the legacy shape", () => {
    expect(bundleSql).toContain('ALTER TABLE "CollectionPointer"\nADD COLUMN "collection_id" BIGINT;');
    expect(bundleSql).toContain('ALTER TABLE "CollectionPointer"\nADD COLUMN "lastSeenTxIndex" INTEGER;');
    expect(bundleSql).toContain('CREATE TABLE IF NOT EXISTS "IdCounter"');
    expect(bundleSql).toContain('CREATE TRIGGER "CollectionPointer_assign_collection_id_after_insert"');
    expect(bundleSql).toContain('CREATE TRIGGER "CollectionPointer_assign_collection_id_after_update"');
    expect(bundleSql).toContain('CREATE TABLE "OrphanFeedback"');
    expect(bundleSql).toContain('ALTER TABLE "IndexerState"\nADD COLUMN "lastTxIndex" INTEGER;');
    expect(bundleSql).toContain('ALTER TABLE "OrphanResponse" ADD COLUMN "sealHash" BLOB;');
    expect(bundleSql).toContain('ALTER TABLE "FeedbackResponse"\nADD COLUMN "sealHash" BLOB;');
    expect(bundleSql).toContain('CREATE TABLE "new_Feedback"');
    expect(bundleSql).toContain('CREATE TRIGGER "IndexerState_monotonic_guard"');
  });

  it("excludes duplicate-column migrations already present in the legacy db-push schema", () => {
    expect(bundleSql).not.toContain('ALTER TABLE "Agent"\nADD COLUMN "agent_id" BIGINT;');
    expect(bundleSql).not.toContain('ALTER TABLE "Agent" ADD COLUMN "trustTier" INTEGER NOT NULL DEFAULT 0;');
    expect(bundleSql).not.toContain('ALTER TABLE "OrphanResponse" ADD COLUMN "runningDigest" BLOB;');
  });
});
