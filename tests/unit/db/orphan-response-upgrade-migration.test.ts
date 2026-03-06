import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const migrationSql = readFileSync(
  resolve(
    __dirname,
    "../../../supabase/migrations/20260307123000_backfill_legacy_orphan_responses.sql"
  ),
  "utf8"
);

describe("legacy orphan response PG upgrade migration", () => {
  it("moves only missing-parent legacy ORPHANED rows into orphan_responses", () => {
    expect(migrationSql).toContain("INSERT INTO orphan_responses");
    expect(migrationSql).toContain("FROM feedback_responses fr");
    expect(migrationSql).toContain("fr.status = 'ORPHANED'");
    expect(migrationSql).toContain("fr.response_id IS NULL");
    expect(migrationSql).toContain("NOT EXISTS (");
    expect(migrationSql).toContain("FROM feedbacks f");
    expect(migrationSql).toContain("f.asset = fr.asset");
    expect(migrationSql).toContain("f.client_address = fr.client_address");
    expect(migrationSql).toContain("f.feedback_index = fr.feedback_index");
    expect(migrationSql).toContain("ON CONFLICT (id) DO NOTHING");
    expect(migrationSql).toContain("DELETE FROM feedback_responses fr");
    expect(migrationSql).toContain("FROM orphan_responses o");
    expect(migrationSql).toContain("o.id = fr.id");
    expect(migrationSql).toContain("NULL,");
  });

  it("does not touch legitimate mismatch rows with an existing parent feedback", () => {
    expect(migrationSql).not.toContain("DELETE FROM feedback_responses WHERE status = 'ORPHANED'");
    expect(migrationSql).not.toContain("UPDATE feedback_responses");
    expect(migrationSql).not.toContain("SET response_id =");
    expect(migrationSql).not.toContain("SET status =");
  });
});
