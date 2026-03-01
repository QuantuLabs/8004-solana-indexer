ALTER TABLE "Feedback"
ADD COLUMN "feedback_id" BIGINT;

ALTER TABLE "FeedbackResponse"
ADD COLUMN "response_id" BIGINT;

ALTER TABLE "Revocation"
ADD COLUMN "revocation_id" BIGINT;

CREATE UNIQUE INDEX IF NOT EXISTS "Feedback_agentId_feedback_id_key"
ON "Feedback"("agentId", "feedback_id");

CREATE UNIQUE INDEX IF NOT EXISTS "FeedbackResponse_feedbackId_response_id_key"
ON "FeedbackResponse"("feedbackId", "response_id");

CREATE UNIQUE INDEX IF NOT EXISTS "Revocation_agentId_revocation_id_key"
ON "Revocation"("agentId", "revocation_id");

CREATE INDEX IF NOT EXISTS "Feedback_agentId_feedback_id_idx"
ON "Feedback"("agentId", "feedback_id");

CREATE INDEX IF NOT EXISTS "FeedbackResponse_feedbackId_response_id_idx"
ON "FeedbackResponse"("feedbackId", "response_id");

CREATE INDEX IF NOT EXISTS "Revocation_agentId_revocation_id_idx"
ON "Revocation"("agentId", "revocation_id");

-- Backfill missing non-orphan feedback_id values deterministically per agent.
WITH "feedback_seed" AS (
  SELECT "agentId", COALESCE(MAX("feedback_id"), 0) AS "max_id"
  FROM "Feedback"
  WHERE "feedback_id" IS NOT NULL
  GROUP BY "agentId"
),
"feedback_ranked" AS (
  SELECT
    f."id",
    f."agentId",
    ROW_NUMBER() OVER (
      PARTITION BY f."agentId"
      ORDER BY
        COALESCE(f."createdSlot", 9223372036854775807) ASC,
        COALESCE(f."createdTxSignature", '') ASC,
        COALESCE(f."txIndex", 2147483647) ASC,
        COALESCE(f."eventOrdinal", 2147483647) ASC,
        f."client" ASC,
        f."feedbackIndex" ASC,
        f."id" ASC
    ) AS "rn"
  FROM "Feedback" f
  WHERE f."status" != 'ORPHANED'
    AND f."feedback_id" IS NULL
)
UPDATE "Feedback"
SET "feedback_id" = (
  SELECT CAST(COALESCE(s."max_id", 0) + r."rn" AS INTEGER)
  FROM "feedback_ranked" r
  LEFT JOIN "feedback_seed" s ON s."agentId" = r."agentId"
  WHERE r."id" = "Feedback"."id"
)
WHERE "id" IN (SELECT "id" FROM "feedback_ranked");

-- Backfill missing non-orphan response_id values deterministically per feedback scope.
WITH "response_seed" AS (
  SELECT "feedbackId", COALESCE(MAX("response_id"), 0) AS "max_id"
  FROM "FeedbackResponse"
  WHERE "response_id" IS NOT NULL
  GROUP BY "feedbackId"
),
"response_ranked" AS (
  SELECT
    fr."id",
    fr."feedbackId",
    ROW_NUMBER() OVER (
      PARTITION BY fr."feedbackId"
      ORDER BY
        COALESCE(fr."responseCount", 9223372036854775807) ASC,
        COALESCE(fr."slot", 9223372036854775807) ASC,
        COALESCE(fr."txSignature", '') ASC,
        COALESCE(fr."txIndex", 2147483647) ASC,
        COALESCE(fr."eventOrdinal", 2147483647) ASC,
        fr."responder" ASC,
        fr."id" ASC
    ) AS "rn"
  FROM "FeedbackResponse" fr
  WHERE fr."status" != 'ORPHANED'
    AND fr."response_id" IS NULL
)
UPDATE "FeedbackResponse"
SET "response_id" = (
  SELECT CAST(COALESCE(s."max_id", 0) + r."rn" AS INTEGER)
  FROM "response_ranked" r
  LEFT JOIN "response_seed" s ON s."feedbackId" = r."feedbackId"
  WHERE r."id" = "FeedbackResponse"."id"
)
WHERE "id" IN (SELECT "id" FROM "response_ranked");

-- Backfill missing non-orphan revocation_id values deterministically per agent.
WITH "revocation_seed" AS (
  SELECT "agentId", COALESCE(MAX("revocation_id"), 0) AS "max_id"
  FROM "Revocation"
  WHERE "revocation_id" IS NOT NULL
  GROUP BY "agentId"
),
"revocation_ranked" AS (
  SELECT
    r."id",
    r."agentId",
    ROW_NUMBER() OVER (
      PARTITION BY r."agentId"
      ORDER BY
        COALESCE(r."revokeCount", 9223372036854775807) ASC,
        COALESCE(r."slot", 9223372036854775807) ASC,
        COALESCE(r."txSignature", '') ASC,
        COALESCE(r."txIndex", 2147483647) ASC,
        COALESCE(r."eventOrdinal", 2147483647) ASC,
        r."client" ASC,
        r."feedbackIndex" ASC,
        r."id" ASC
    ) AS "rn"
  FROM "Revocation" r
  WHERE r."status" != 'ORPHANED'
    AND r."revocation_id" IS NULL
)
UPDATE "Revocation"
SET "revocation_id" = (
  SELECT CAST(COALESCE(s."max_id", 0) + r."rn" AS INTEGER)
  FROM "revocation_ranked" r
  LEFT JOIN "revocation_seed" s ON s."agentId" = r."agentId"
  WHERE r."id" = "Revocation"."id"
)
WHERE "id" IN (SELECT "id" FROM "revocation_ranked");
