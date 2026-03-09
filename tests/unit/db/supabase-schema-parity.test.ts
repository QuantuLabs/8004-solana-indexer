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

  it("defines orphan_responses staging table used for delayed response replay", () => {
    expect(schemaSql).toContain("DROP TABLE IF EXISTS orphan_responses CASCADE;");
    expect(schemaSql).toContain("CREATE TABLE orphan_responses (");
    expect(schemaSql).toContain("seal_hash TEXT");
    expect(schemaSql).toContain("running_digest BYTEA");
    expect(schemaSql).toContain("response_count BIGINT NOT NULL DEFAULT 0");
    expect(schemaSql).toContain("UNIQUE(asset, client_address, feedback_index, responder, tx_signature)");
    expect(schemaSql).toContain("CREATE INDEX idx_orphan_responses_asset ON orphan_responses(asset);");
    expect(schemaSql).toContain("CREATE INDEX idx_orphan_responses_lookup");
  });

  it("defines revocation indexes for id assignment and canonical ordering", () => {
    expect(schemaSql).toContain("CREATE INDEX idx_revocations_asset ON revocations(asset);");
    expect(schemaSql).toContain("CREATE INDEX idx_revocations_status ON revocations(status) WHERE status = 'PENDING';");
    expect(schemaSql).toContain("CREATE UNIQUE INDEX idx_revocations_asset_revocation_id");
    expect(schemaSql).toContain("WHERE revocation_id IS NOT NULL;");
    expect(schemaSql).toContain("CREATE INDEX idx_revocations_asset_revocation_id_lookup");
    expect(schemaSql).toContain("CREATE INDEX idx_revocations_canonical_order");
    expect(schemaSql).toContain("ON revocations(asset, slot, tx_index NULLS LAST, event_ordinal NULLS LAST, tx_signature, id);");
  });

  it("hardens scoped sequential IDs at DB layer with trigger assignment", () => {
    expect(schemaSql).toContain("CHECK (status = 'ORPHANED' OR agent_id IS NOT NULL)");
    expect(schemaSql).toContain("CREATE TABLE IF NOT EXISTS id_counters (");
    expect(schemaSql).toContain(
      "CREATE OR REPLACE FUNCTION alloc_gapless_id(p_scope TEXT, p_updated_at TIMESTAMPTZ DEFAULT NULL)"
    );
    expect(schemaSql).toContain("CREATE OR REPLACE FUNCTION assign_feedback_id()");
    expect(schemaSql).toContain("CREATE OR REPLACE FUNCTION assign_response_id()");
    expect(schemaSql).toContain("CREATE OR REPLACE FUNCTION assign_revocation_id()");
    expect(schemaSql).toContain("'feedback:id:' || NEW.asset || ':' || NEW.client_address || ':' || NEW.feedback_index::text");
    expect(schemaSql).toContain(
      "'response:id:' || NEW.asset || ':' || NEW.client_address || ':' || NEW.feedback_index::text || ':' || NEW.responder || ':' || COALESCE(NEW.tx_signature, '')"
    );
    expect(schemaSql).toContain("'revocation:id:' || NEW.asset || ':' || NEW.client_address || ':' || NEW.feedback_index::text");
    expect(schemaSql).toContain(
      "NEW.agent_id := alloc_gapless_id('agent:global', COALESCE(NEW.created_at, NEW.updated_at, NOW()));"
    );
    expect(schemaSql).toContain(
      "IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN"
    );
    expect(schemaSql).toContain(
      "NEW.feedback_id := alloc_gapless_id('feedback:' || NEW.asset, COALESCE(NEW.created_at, NOW()));"
    );
    expect(schemaSql).toContain(
      "'collection:global',\n    COALESCE(NEW.first_seen_at, NEW.last_seen_at, NOW())"
    );
    expect(schemaSql).toContain("'response:' || NEW.asset || ':' || NEW.client_address || ':' || NEW.feedback_index::text");
    expect(schemaSql).toContain(
      "COALESCE(NEW.created_at, NOW())"
    );
    expect(schemaSql).toContain(
      "NEW.revocation_id := alloc_gapless_id('revocation:' || NEW.asset, COALESCE(NEW.created_at, NOW()));"
    );
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
      "(SELECT COUNT(*) FROM agents WHERE status != 'ORPHANED') AS total_agents"
    );
    expect(schemaSql).toContain(
      "(SELECT COUNT(*) FROM collection_pointers) AS total_collections"
    );
    expect(schemaSql).toContain(
      "(SELECT COUNT(*) FROM feedbacks WHERE status != 'ORPHANED') AS total_feedbacks"
    );
  });

  it("defines proxy schema objects used by agent0 REST clients", () => {
    expect(schemaSql).toContain("DROP VIEW IF EXISTS agent_reputation CASCADE;");
    expect(schemaSql).toContain("CREATE OR REPLACE VIEW agent_reputation");
    expect(schemaSql).toContain("LEFT JOIN feedback_stats fs");
    expect(schemaSql).toContain("THEN COALESCE(fs.avg_score, a.raw_avg_score::numeric)");
    expect(schemaSql).toContain("LEFT JOIN validation_stats vs");
    expect(schemaSql).toContain("COALESCE(vs.validation_count, 0) AS validation_count");
    expect(schemaSql).toContain("GRANT SELECT ON agent_reputation TO anon;");
    expect(schemaSql).toContain("GRANT SELECT ON agent_reputation TO authenticated;");

    expect(schemaSql).toContain("DROP FUNCTION IF EXISTS get_collection_agents CASCADE;");
    expect(schemaSql).toContain("CREATE OR REPLACE FUNCTION get_collection_agents(");
    expect(schemaSql).toContain("RETURNS TABLE (");
    expect(schemaSql).toContain("feedback_count INTEGER");
    expect(schemaSql).toContain("FROM collection_pointers cp");
    expect(schemaSql).toContain("JOIN agents a");
    expect(schemaSql).toContain("a.canonical_col = cp.col");
    expect(schemaSql).toContain("a.creator = cp.creator");
    expect(schemaSql).toContain("WHERE cp.collection_id = $1");
    expect(schemaSql).toContain("GRANT EXECUTE ON FUNCTION get_collection_agents(BIGINT, INT, INT) TO anon;");
    expect(schemaSql).toContain("GRANT EXECUTE ON FUNCTION get_collection_agents(BIGINT, INT, INT) TO authenticated;");
  });
});
