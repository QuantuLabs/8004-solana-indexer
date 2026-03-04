ALTER TABLE "CollectionPointer"
ADD COLUMN "collection_id" BIGINT;

CREATE UNIQUE INDEX IF NOT EXISTS "CollectionPointer_collection_id_key"
ON "CollectionPointer"("collection_id");

CREATE INDEX IF NOT EXISTS "CollectionPointer_collection_id_idx"
ON "CollectionPointer"("collection_id");

WITH seed AS (
  SELECT COALESCE(MAX("collection_id"), 0) AS max_id
  FROM "CollectionPointer"
  WHERE "collection_id" IS NOT NULL
),
ranked AS (
  SELECT
    cp."col",
    cp."creator",
    ROW_NUMBER() OVER (
      ORDER BY
        cp."firstSeenAt" ASC,
        COALESCE(cp."firstSeenSlot", 9223372036854775807) ASC,
        CASE WHEN cp."firstSeenTxSignature" IS NULL THEN 1 ELSE 0 END ASC,
        cp."firstSeenTxSignature" ASC,
        cp."col" ASC,
        cp."creator" ASC
    ) AS rn
  FROM "CollectionPointer" cp
  WHERE cp."collection_id" IS NULL
)
UPDATE "CollectionPointer"
SET "collection_id" = (
  SELECT CAST((SELECT max_id FROM seed) + r.rn AS BIGINT)
  FROM ranked r
  WHERE r."col" = "CollectionPointer"."col"
    AND r."creator" = "CollectionPointer"."creator"
)
WHERE "collection_id" IS NULL;
