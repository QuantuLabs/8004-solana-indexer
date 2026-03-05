ALTER TABLE "Agent"
ADD COLUMN "agent_id" BIGINT;

CREATE UNIQUE INDEX IF NOT EXISTS "Agent_agent_id_key"
ON "Agent"("agent_id");

CREATE INDEX IF NOT EXISTS "Agent_agent_id_idx"
ON "Agent"("agent_id");

WITH "agent_seed" AS (
  SELECT COALESCE(MAX("agent_id"), 0) AS "max_id"
  FROM "Agent"
  WHERE "agent_id" IS NOT NULL
),
"agent_ranked" AS (
  SELECT
    a."id",
    ROW_NUMBER() OVER (
      ORDER BY
        COALESCE(a."createdSlot", 9223372036854775807) ASC,
        CASE WHEN a."createdTxSignature" IS NULL THEN 1 ELSE 0 END ASC,
        COALESCE(a."createdTxSignature", '') ASC,
        COALESCE(a."eventOrdinal", 2147483647) ASC,
        a."id" ASC
    ) AS "rn"
  FROM "Agent" a
  WHERE (a."status" IS NULL OR a."status" != 'ORPHANED')
    AND a."agent_id" IS NULL
)
UPDATE "Agent"
SET "agent_id" = (
  SELECT CAST((SELECT "max_id" FROM "agent_seed") + r."rn" AS BIGINT)
  FROM "agent_ranked" r
  WHERE r."id" = "Agent"."id"
)
WHERE "id" IN (SELECT "id" FROM "agent_ranked");
