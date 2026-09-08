import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { collectPendingSql } from "../../../scripts/upgrade-postgres.mjs";

const tempDirs: string[] = [];
const wrapped = [
  "20260301123000_gapless_ids_for_all_scopes.sql",
  "20260303160000_add_collection_sequential_id.sql",
];

afterEach(() => {
  for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function fixture(files: Record<string, string>) {
  const dir = mkdtempSync(resolve(tmpdir(), "postgres-upgrade-entrypoint-"));
  tempDirs.push(dir);
  for (const [name, sql] of Object.entries(files)) writeFileSync(resolve(dir, name), sql);
  return dir;
}

describe("PostgreSQL upgrade entrypoint", () => {
  it("selects only later files and removes only the two known outer wrappers", () => {
    const dir = fixture({
      "20260301000000_before.sql": "SELECT 0;\n",
      [wrapped[0]]: "-- wrapper\nBEGIN;\nDO $$ BEGIN PERFORM 1; END $$;\nCOMMIT;\n",
      [wrapped[1]]: "BEGIN;\nSELECT 'BEGIN;';\nCOMMIT;\n",
      "20260304000000_after.sql": "SELECT 4;\n",
    });
    const result = collectPendingSql("20260301000000_before.sql", dir);
    expect(result.filenames).toEqual([...wrapped, "20260304000000_after.sql"]);
    expect(result.sql).not.toMatch(/^BEGIN;|^COMMIT;/m);
    expect(result.sql).toContain("DO $$ BEGIN PERFORM 1; END $$;");
    expect(result.sql).toContain("SELECT 'BEGIN;';");
  });

  it("rejects an unexpected wrapper while collecting, before any database execution", () => {
    const dir = fixture({
      "20260301000000_before.sql": "SELECT 0;\n",
      "20260302000000_bad.sql": "BEGIN;\nSELECT 1;\nCOMMIT;\n",
    });
    expect(() => collectPendingSql("20260301000000_before.sql", dir))
      .toThrow(/unexpected standalone transaction control/);

    const changedKnown = fixture({
      "20260301000000_before.sql": "SELECT 0;\n",
      [wrapped[0]]: "BEGIN;\nCOMMIT;\nCOMMIT;\n",
    });
    expect(() => collectPendingSql("20260301000000_before.sql", changedKnown))
      .toThrow(/known transaction wrapper shape changed/);
  });
});
