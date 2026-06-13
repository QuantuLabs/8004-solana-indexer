import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const migrationSql = readFileSync(
  resolve(process.cwd(), "supabase/migrations/20260318210000_rpc_limit_offset_caps.sql"),
  "utf8",
);

describe("RPC limit caps migration", () => {
  it("clamps public read RPC limit and offset inside SQL", () => {
    expect(migrationSql).toContain("CREATE OR REPLACE FUNCTION get_leaderboard(");
    expect(migrationSql).toContain("LIMIT LEAST(GREATEST(COALESCE(p_limit, 50), 1), 1000);");
    expect(migrationSql).toContain("CREATE OR REPLACE FUNCTION get_collection_agents(");
    expect(migrationSql).toContain("LIMIT LEAST(GREATEST(COALESCE($2, 20), 1), 1000)");
    expect(migrationSql).toContain("OFFSET LEAST(GREATEST(COALESCE($3, 0), 0), 10000);");
    expect(migrationSql).toContain("GRANT EXECUTE ON FUNCTION get_leaderboard(TEXT, INT, INT, BIGINT) TO anon;");
    expect(migrationSql).toContain("GRANT EXECUTE ON FUNCTION get_collection_agents(BIGINT, INT, INT) TO anon;");
  });
});
