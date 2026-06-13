import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const supabaseMigration = readFileSync(
  resolve(process.cwd(), "supabase/migrations/20260318204500_response_event_ordinal_identity.sql"),
  "utf8",
);
const prismaMigration = readFileSync(
  resolve(process.cwd(), "prisma/migrations/20260318204500_response_event_ordinal_identity/migration.sql"),
  "utf8",
);

describe("response event ordinal identity migration", () => {
  it("adds ordinal-aware response identity on the Supabase upgrade path", () => {
    expect(supabaseMigration).toContain("ALTER TABLE feedback_responses ADD COLUMN IF NOT EXISTS event_ordinal INTEGER;");
    expect(supabaseMigration).toContain("ALTER TABLE orphan_responses ADD COLUMN IF NOT EXISTS event_ordinal INTEGER;");
    expect(supabaseMigration).toContain("idx_feedback_responses_response_event_identity");
    expect(supabaseMigration).toContain("idx_orphan_responses_response_event_identity");
    expect(supabaseMigration).toContain("(COALESCE(event_ordinal, -1))");
    expect(supabaseMigration).toContain("AND COALESCE(event_ordinal, -1) = COALESCE(NEW.event_ordinal, -1)");
  });

  it("replaces Prisma response uniqueness with event ordinal identity", () => {
    expect(prismaMigration).toContain('DROP INDEX IF EXISTS "FeedbackResponse_feedbackId_responder_txSignature_key";');
    expect(prismaMigration).toContain('DROP INDEX IF EXISTS "OrphanResponse_agentId_client_feedbackIndex_responder_txSignature_key";');
    expect(prismaMigration).toContain('"FeedbackResponse"("feedbackId", "responder", "txSignature", "eventOrdinal")');
    expect(prismaMigration).toContain('"OrphanResponse"("agentId", "client", "feedbackIndex", "responder", "txSignature", "eventOrdinal")');
  });
});
