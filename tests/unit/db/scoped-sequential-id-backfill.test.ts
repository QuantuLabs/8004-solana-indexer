import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const supabaseMigrationSql = readFileSync(
  resolve(__dirname, "../../../supabase/migrations/20260227203000_scoped_sequential_ids_backfill.sql"),
  "utf8"
);

const prismaMigrationSql = readFileSync(
  resolve(__dirname, "../../../prisma/migrations/20260227193000_add_feedback_revocation_ids/migration.sql"),
  "utf8"
);

describe("scoped sequential ID backfill migrations", () => {
  it("defines deterministic supabase backfill for feedbacks, responses, and revocations", () => {
    expect(supabaseMigrationSql).toContain("feedback_pending AS");
    expect(supabaseMigrationSql).toContain("PARTITION BY f.asset");
    expect(supabaseMigrationSql).toContain("ORDER BY");
    expect(supabaseMigrationSql).toContain("f.block_slot ASC NULLS LAST");
    expect(supabaseMigrationSql).toContain("f.event_ordinal ASC NULLS LAST");

    expect(supabaseMigrationSql).toContain("response_pending AS");
    expect(supabaseMigrationSql).toContain("PARTITION BY fr.asset, fr.client_address, fr.feedback_index");
    expect(supabaseMigrationSql).toContain("fr.response_count ASC NULLS LAST");
    expect(supabaseMigrationSql).toContain("fr.event_ordinal ASC NULLS LAST");

    expect(supabaseMigrationSql).toContain("revocation_pending AS");
    expect(supabaseMigrationSql).toContain("PARTITION BY r.asset");
    expect(supabaseMigrationSql).toContain("r.revoke_count ASC NULLS LAST");
    expect(supabaseMigrationSql).toContain("r.event_ordinal ASC NULLS LAST");
  });

  it("enforces non-orphan scoped ID constraints in supabase migration", () => {
    expect(supabaseMigrationSql).toContain("feedbacks_non_orphan_feedback_id_required");
    expect(supabaseMigrationSql).toContain("feedback_responses_non_orphan_response_id_required");
    expect(supabaseMigrationSql).toContain("revocations_non_orphan_revocation_id_required");
    expect(supabaseMigrationSql).toContain("CHECK (status = 'ORPHANED' OR feedback_id IS NOT NULL)");
    expect(supabaseMigrationSql).toContain("CHECK (status = 'ORPHANED' OR response_id IS NOT NULL)");
    expect(supabaseMigrationSql).toContain("CHECK (status = 'ORPHANED' OR revocation_id IS NOT NULL)");
  });

  it("defines deterministic local Prisma backfill for feedbacks, responses, and revocations", () => {
    expect(prismaMigrationSql).toContain('"feedback_ranked"');
    expect(prismaMigrationSql).toContain('PARTITION BY f."agentId"');
    expect(prismaMigrationSql).toContain('COALESCE(f."createdSlot"');

    expect(prismaMigrationSql).toContain('"response_ranked"');
    expect(prismaMigrationSql).toContain('PARTITION BY fr."feedbackId"');
    expect(prismaMigrationSql).toContain('COALESCE(fr."responseCount"');

    expect(prismaMigrationSql).toContain('"revocation_ranked"');
    expect(prismaMigrationSql).toContain('PARTITION BY r."agentId"');
    expect(prismaMigrationSql).toContain('COALESCE(r."revokeCount"');
  });
});
