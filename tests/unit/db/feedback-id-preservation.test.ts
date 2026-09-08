import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const sql = readFileSync(
  resolve(process.cwd(), "supabase/migrations/20260318193000_fix_feedback_id_recovery_determinism.sql"),
  "utf8",
);

describe("feedback ID preservation migration", () => {
  it("keeps an assigned update ID before orphan handling and never recanonicalizes rows", () => {
    expect(sql).toContain(
      "IF TG_OP = 'UPDATE' AND OLD.feedback_id IS NOT NULL THEN\n    NEW.feedback_id := OLD.feedback_id;\n    RETURN NEW;\n  END IF;\n\n  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN",
    );
    expect(sql).not.toMatch(/recanonicalize|ROW_NUMBER|UPDATE\s+feedbacks|INSERT\s+INTO\s+id_counters/i);
  });
});
