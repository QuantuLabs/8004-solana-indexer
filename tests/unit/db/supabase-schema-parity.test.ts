import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const schemaSql = readFileSync(resolve(__dirname, "../../../supabase/schema.sql"), "utf8");

describe("supabase schema revocations bootstrap parity", () => {
  it("drops and recreates revocations on clean bootstrap", () => {
    expect(schemaSql).toContain("DROP TABLE IF EXISTS revocations CASCADE;");
    expect(schemaSql).toMatch(/CREATE TABLE revocations\s*\(/);
  });

  it("defines revocations columns used by runtime handlers", () => {
    expect(schemaSql).toContain("revocation_id BIGINT");
    expect(schemaSql).toContain("running_digest BYTEA");
    expect(schemaSql).toContain("revoke_count BIGINT NOT NULL");
    expect(schemaSql).toContain("tx_index INTEGER");
    expect(schemaSql).toContain("event_ordinal INTEGER");
    expect(schemaSql).toContain("status TEXT DEFAULT 'PENDING'");
    expect(schemaSql).toContain("UNIQUE(asset, client_address, feedback_index)");
  });

  it("defines revocation indexes for id assignment and canonical ordering", () => {
    expect(schemaSql).toContain("CREATE INDEX idx_revocations_asset ON revocations(asset);");
    expect(schemaSql).toContain("CREATE INDEX idx_revocations_status ON revocations(status) WHERE status = 'PENDING';");
    expect(schemaSql).toContain("CREATE UNIQUE INDEX idx_revocations_asset_revocation_id");
    expect(schemaSql).toContain("WHERE revocation_id IS NOT NULL;");
    expect(schemaSql).toContain("CREATE INDEX idx_revocations_asset_revocation_id_lookup");
    expect(schemaSql).toContain("CREATE INDEX idx_revocations_canonical_order");
    expect(schemaSql).toContain("ON revocations(asset, slot, tx_signature, tx_index NULLS LAST, event_ordinal NULLS LAST, id);");
  });

  it("hardens scoped sequential IDs at DB layer with trigger assignment", () => {
    expect(schemaSql).toContain("CHECK (status = 'ORPHANED' OR agent_id IS NOT NULL)");
    expect(schemaSql).toContain("CREATE OR REPLACE FUNCTION assign_feedback_id()");
    expect(schemaSql).toContain("CREATE OR REPLACE FUNCTION assign_response_id()");
    expect(schemaSql).toContain("CREATE OR REPLACE FUNCTION assign_revocation_id()");
    expect(schemaSql).toContain("pg_advisory_xact_lock(hashtextextended('feedback:' || NEW.asset, 0));");
    expect(schemaSql).toContain("'response:' || NEW.asset || ':' || NEW.client_address || ':' || NEW.feedback_index::text");
    expect(schemaSql).toContain("pg_advisory_xact_lock(hashtextextended('revocation:' || NEW.asset, 0));");
    expect(schemaSql).toContain("CREATE TRIGGER trg_assign_agent_id");
    expect(schemaSql).toContain("BEFORE INSERT OR UPDATE ON agents");
    expect(schemaSql).toContain("CREATE TRIGGER trg_assign_feedback_id");
    expect(schemaSql).toContain("CREATE TRIGGER trg_assign_response_id");
    expect(schemaSql).toContain("CREATE TRIGGER trg_assign_revocation_id");
  });

  it("includes revocations in verification stats and RLS policy", () => {
    expect(schemaSql).toContain("'revocations' AS model");
    expect(schemaSql).toContain("FROM revocations");
    expect(schemaSql).toContain("ALTER TABLE revocations ENABLE ROW LEVEL SECURITY;");
    expect(schemaSql).toContain('CREATE POLICY "Public read revocations" ON revocations FOR SELECT USING (true);');
    expect(schemaSql).toContain("GRANT SELECT ON global_stats TO anon;");
    expect(schemaSql).toContain("GRANT SELECT ON global_stats TO authenticated;");
  });

  it("enforces non-orphan scoped IDs as non-null invariants", () => {
    expect(schemaSql).toContain("CHECK (status = 'ORPHANED' OR feedback_id IS NOT NULL)");
    expect(schemaSql).toContain("CHECK (status = 'ORPHANED' OR response_id IS NOT NULL)");
    expect(schemaSql).toContain("CHECK (status = 'ORPHANED' OR revocation_id IS NOT NULL)");
  });

  it("excludes BASE registry rows from global_stats total_collections", () => {
    expect(schemaSql).toContain("CREATE OR REPLACE VIEW global_stats");
    expect(schemaSql).toContain(
      "(SELECT COUNT(*) FROM collections WHERE status != 'ORPHANED' AND registry_type != 'BASE') AS total_collections"
    );
  });
});
