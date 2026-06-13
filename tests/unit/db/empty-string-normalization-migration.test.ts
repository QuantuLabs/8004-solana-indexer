import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const supabaseMigration = readFileSync(
  resolve(process.cwd(), "supabase/migrations/20260318211500_normalize_empty_optional_fields.sql"),
  "utf8",
);
const prismaMigration = readFileSync(
  resolve(process.cwd(), "prisma/migrations/20260318211500_normalize_empty_optional_fields/migration.sql"),
  "utf8",
);

describe("empty string normalization migration", () => {
  it("normalizes historical Supabase optional-field drift to NULL", () => {
    for (const statement of [
      "UPDATE agents\nSET agent_uri = NULL\nWHERE agent_uri = '';",
      "UPDATE feedback_responses\nSET response_uri = NULL\nWHERE response_uri = '';",
      "UPDATE validations\nSET request_uri = NULL\nWHERE request_uri = '';",
      "UPDATE validations\nSET response_uri = NULL\nWHERE response_uri = '';",
      "UPDATE validations\nSET tag = NULL\nWHERE tag = '';",
    ]) {
      expect(supabaseMigration).toContain(statement);
    }
    expect(supabaseMigration).not.toContain("created_at");
  });

  it("normalizes nullable Prisma validation fields only", () => {
    expect(prismaMigration).toContain('UPDATE "Validation"\nSET "requestUri" = NULL\nWHERE "requestUri" = \'\';');
    expect(prismaMigration).toContain('UPDATE "Validation"\nSET "responseUri" = NULL\nWHERE "responseUri" = \'\';');
    expect(prismaMigration).toContain('UPDATE "Validation"\nSET "tag" = NULL\nWHERE "tag" = \'\';');
    expect(prismaMigration).not.toContain('"Agent"');
    expect(prismaMigration).not.toContain('"FeedbackResponse"');
  });
});
