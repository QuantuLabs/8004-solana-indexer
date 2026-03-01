-- v0.6.x canonical collection pointer registry
CREATE TABLE "CollectionPointer" (
  "col" TEXT NOT NULL,
  "creator" TEXT NOT NULL,
  "firstSeenAsset" TEXT NOT NULL,
  "firstSeenAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "firstSeenSlot" BIGINT NOT NULL,
  "firstSeenTxSignature" TEXT,
  "lastSeenAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "lastSeenSlot" BIGINT NOT NULL,
  "lastSeenTxSignature" TEXT,
  "assetCount" BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY ("col", "creator")
);

CREATE INDEX IF NOT EXISTS "CollectionPointer_creator_idx" ON "CollectionPointer"("creator");
CREATE INDEX IF NOT EXISTS "CollectionPointer_firstSeenAt_idx" ON "CollectionPointer"("firstSeenAt");
CREATE INDEX IF NOT EXISTS "CollectionPointer_lastSeenAt_idx" ON "CollectionPointer"("lastSeenAt");
CREATE INDEX IF NOT EXISTS "Agent_collectionPointer_idx" ON "Agent"("collectionPointer");

-- Best-effort backfill from existing agent snapshots.
INSERT INTO "CollectionPointer" (
  "col",
  "creator",
  "firstSeenAsset",
  "firstSeenAt",
  "firstSeenSlot",
  "firstSeenTxSignature",
  "lastSeenAt",
  "lastSeenSlot",
  "lastSeenTxSignature",
  "assetCount"
)
SELECT
  a."collectionPointer" AS "col",
  COALESCE(a."creator", a."owner") AS "creator",
  a."id" AS "firstSeenAsset",
  a."updatedAt" AS "firstSeenAt",
  COALESCE(a."createdSlot", 0) AS "firstSeenSlot",
  a."createdTxSignature" AS "firstSeenTxSignature",
  a."updatedAt" AS "lastSeenAt",
  COALESCE(a."createdSlot", 0) AS "lastSeenSlot",
  a."createdTxSignature" AS "lastSeenTxSignature",
  0 AS "assetCount"
FROM "Agent" a
WHERE a."collectionPointer" != ''
ON CONFLICT("col", "creator") DO NOTHING;

UPDATE "CollectionPointer"
SET "assetCount" = (
  SELECT COUNT(*)
  FROM "Agent" a
  WHERE a."collectionPointer" = "CollectionPointer"."col"
    AND COALESCE(a."creator", a."owner") = "CollectionPointer"."creator"
);
