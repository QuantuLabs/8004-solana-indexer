-- Legacy SQLite upgrade bundle for databases created by the known v1.7.7
-- local/Docker `prisma db push` flow without a valid `_prisma_migrations`
-- baseline.
--
-- Proven target:
-- - preserves existing indexed rows/public IDs on the tested legacy shape
-- - adds only the schema/runtime pieces missing from that shape
-- - intentionally excludes migrations that duplicate columns already present
--
-- Excluded on purpose:
-- - 20260304120000_add_agent_sequential_id
-- - 20260306233000_add_missing_agent_runtime_columns_sqlite

-- 20260303161000_add_collection_sequential_id
ALTER TABLE "CollectionPointer"
ADD COLUMN "collection_id" BIGINT;

-- 20260311173000_add_collection_pointer_last_seen_tx_index
ALTER TABLE "CollectionPointer"
ADD COLUMN "lastSeenTxIndex" INTEGER;

ALTER TABLE "FeedbackResponse"
ADD COLUMN "sealHash" BLOB;

CREATE UNIQUE INDEX IF NOT EXISTS "CollectionPointer_collection_id_key"
ON "CollectionPointer"("collection_id");

CREATE INDEX IF NOT EXISTS "CollectionPointer_collection_id_idx"
ON "CollectionPointer"("collection_id");

DROP TABLE IF EXISTS "__collection_pointer_ranked";
CREATE TEMP TABLE "__collection_pointer_ranked" (
  "col" TEXT NOT NULL,
  "creator" TEXT NOT NULL,
  "new_id" BIGINT NOT NULL,
  PRIMARY KEY ("col", "creator")
);

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
        COALESCE(cp."firstSeenSlot", 9223372036854775807) ASC,
        CASE WHEN cp."firstSeenTxSignature" IS NULL THEN 1 ELSE 0 END ASC,
        cp."firstSeenTxSignature" ASC,
        cp."col" ASC,
        cp."creator" ASC
    ) AS rn
  FROM "CollectionPointer" cp
  WHERE cp."collection_id" IS NULL
)
INSERT INTO "__collection_pointer_ranked" ("col", "creator", "new_id")
SELECT
  r."col",
  r."creator",
  CAST((SELECT max_id FROM seed) + r.rn AS BIGINT)
FROM ranked r;

UPDATE OR IGNORE "CollectionPointer"
SET "collection_id" = (
  SELECT rr."new_id"
  FROM "__collection_pointer_ranked" rr
  WHERE rr."col" = "CollectionPointer"."col"
    AND rr."creator" = "CollectionPointer"."creator"
)
WHERE "collection_id" IS NULL;

DROP TABLE "__collection_pointer_ranked";

DROP TABLE IF EXISTS "__collection_pointer_ranked";
CREATE TEMP TABLE "__collection_pointer_ranked" (
  "col" TEXT NOT NULL,
  "creator" TEXT NOT NULL,
  "new_id" BIGINT NOT NULL,
  PRIMARY KEY ("col", "creator")
);

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
        COALESCE(cp."firstSeenSlot", 9223372036854775807) ASC,
        CASE WHEN cp."firstSeenTxSignature" IS NULL THEN 1 ELSE 0 END ASC,
        cp."firstSeenTxSignature" ASC,
        cp."col" ASC,
        cp."creator" ASC
    ) AS rn
  FROM "CollectionPointer" cp
  WHERE cp."collection_id" IS NULL
)
INSERT INTO "__collection_pointer_ranked" ("col", "creator", "new_id")
SELECT
  r."col",
  r."creator",
  CAST((SELECT max_id FROM seed) + r.rn AS BIGINT)
FROM ranked r;

UPDATE OR IGNORE "CollectionPointer"
SET "collection_id" = (
  SELECT rr."new_id"
  FROM "__collection_pointer_ranked" rr
  WHERE rr."col" = "CollectionPointer"."col"
    AND rr."creator" = "CollectionPointer"."creator"
)
WHERE "collection_id" IS NULL;

DROP TABLE "__collection_pointer_ranked";

DROP TABLE IF EXISTS "__collection_pointer_ranked";
CREATE TEMP TABLE "__collection_pointer_ranked" (
  "col" TEXT NOT NULL,
  "creator" TEXT NOT NULL,
  "new_id" BIGINT NOT NULL,
  PRIMARY KEY ("col", "creator")
);

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
        COALESCE(cp."firstSeenSlot", 9223372036854775807) ASC,
        CASE WHEN cp."firstSeenTxSignature" IS NULL THEN 1 ELSE 0 END ASC,
        cp."firstSeenTxSignature" ASC,
        cp."col" ASC,
        cp."creator" ASC
    ) AS rn
  FROM "CollectionPointer" cp
  WHERE cp."collection_id" IS NULL
)
INSERT INTO "__collection_pointer_ranked" ("col", "creator", "new_id")
SELECT
  r."col",
  r."creator",
  CAST((SELECT max_id FROM seed) + r.rn AS BIGINT)
FROM ranked r;

UPDATE OR IGNORE "CollectionPointer"
SET "collection_id" = (
  SELECT rr."new_id"
  FROM "__collection_pointer_ranked" rr
  WHERE rr."col" = "CollectionPointer"."col"
    AND rr."creator" = "CollectionPointer"."creator"
)
WHERE "collection_id" IS NULL;

DROP TABLE "__collection_pointer_ranked";

DROP TABLE IF EXISTS "__collection_pointer_ranked";
CREATE TEMP TABLE "__collection_pointer_ranked" (
  "col" TEXT NOT NULL,
  "creator" TEXT NOT NULL,
  "new_id" BIGINT NOT NULL,
  PRIMARY KEY ("col", "creator")
);

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
        COALESCE(cp."firstSeenSlot", 9223372036854775807) ASC,
        CASE WHEN cp."firstSeenTxSignature" IS NULL THEN 1 ELSE 0 END ASC,
        cp."firstSeenTxSignature" ASC,
        cp."col" ASC,
        cp."creator" ASC
    ) AS rn
  FROM "CollectionPointer" cp
  WHERE cp."collection_id" IS NULL
)
INSERT INTO "__collection_pointer_ranked" ("col", "creator", "new_id")
SELECT
  r."col",
  r."creator",
  CAST((SELECT max_id FROM seed) + r.rn AS BIGINT)
FROM ranked r;

UPDATE "CollectionPointer"
SET "collection_id" = (
  SELECT rr."new_id"
  FROM "__collection_pointer_ranked" rr
  WHERE rr."col" = "CollectionPointer"."col"
    AND rr."creator" = "CollectionPointer"."creator"
)
WHERE "collection_id" IS NULL;

DROP TABLE "__collection_pointer_ranked";

CREATE TEMP TABLE "__collection_pointer_unresolved_guard" (
  "remaining" INTEGER NOT NULL CHECK ("remaining" = 0)
);

INSERT INTO "__collection_pointer_unresolved_guard" ("remaining")
SELECT COUNT(*)
FROM "CollectionPointer"
WHERE "collection_id" IS NULL;

DROP TABLE "__collection_pointer_unresolved_guard";

-- 20260305113000_backfill_collection_sequential_id
DROP TABLE IF EXISTS "__collection_pointer_ranked";
CREATE TEMP TABLE "__collection_pointer_ranked" (
  "col" TEXT NOT NULL,
  "creator" TEXT NOT NULL,
  "new_id" BIGINT NOT NULL,
  PRIMARY KEY ("col", "creator")
);

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
        COALESCE(cp."firstSeenSlot", 9223372036854775807) ASC,
        CASE WHEN cp."firstSeenTxSignature" IS NULL THEN 1 ELSE 0 END ASC,
        cp."firstSeenTxSignature" ASC,
        cp."col" ASC,
        cp."creator" ASC
    ) AS rn
  FROM "CollectionPointer" cp
  WHERE cp."collection_id" IS NULL
)
INSERT INTO "__collection_pointer_ranked" ("col", "creator", "new_id")
SELECT
  r."col",
  r."creator",
  CAST((SELECT max_id FROM seed) + r.rn AS BIGINT)
FROM ranked r;

UPDATE OR IGNORE "CollectionPointer"
SET "collection_id" = (
  SELECT rr."new_id"
  FROM "__collection_pointer_ranked" rr
  WHERE rr."col" = "CollectionPointer"."col"
    AND rr."creator" = "CollectionPointer"."creator"
)
WHERE "collection_id" IS NULL;

DROP TABLE "__collection_pointer_ranked";

DROP TABLE IF EXISTS "__collection_pointer_ranked";
CREATE TEMP TABLE "__collection_pointer_ranked" (
  "col" TEXT NOT NULL,
  "creator" TEXT NOT NULL,
  "new_id" BIGINT NOT NULL,
  PRIMARY KEY ("col", "creator")
);

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
        COALESCE(cp."firstSeenSlot", 9223372036854775807) ASC,
        CASE WHEN cp."firstSeenTxSignature" IS NULL THEN 1 ELSE 0 END ASC,
        cp."firstSeenTxSignature" ASC,
        cp."col" ASC,
        cp."creator" ASC
    ) AS rn
  FROM "CollectionPointer" cp
  WHERE cp."collection_id" IS NULL
)
INSERT INTO "__collection_pointer_ranked" ("col", "creator", "new_id")
SELECT
  r."col",
  r."creator",
  CAST((SELECT max_id FROM seed) + r.rn AS BIGINT)
FROM ranked r;

UPDATE OR IGNORE "CollectionPointer"
SET "collection_id" = (
  SELECT rr."new_id"
  FROM "__collection_pointer_ranked" rr
  WHERE rr."col" = "CollectionPointer"."col"
    AND rr."creator" = "CollectionPointer"."creator"
)
WHERE "collection_id" IS NULL;

DROP TABLE "__collection_pointer_ranked";

DROP TABLE IF EXISTS "__collection_pointer_ranked";
CREATE TEMP TABLE "__collection_pointer_ranked" (
  "col" TEXT NOT NULL,
  "creator" TEXT NOT NULL,
  "new_id" BIGINT NOT NULL,
  PRIMARY KEY ("col", "creator")
);

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
        COALESCE(cp."firstSeenSlot", 9223372036854775807) ASC,
        CASE WHEN cp."firstSeenTxSignature" IS NULL THEN 1 ELSE 0 END ASC,
        cp."firstSeenTxSignature" ASC,
        cp."col" ASC,
        cp."creator" ASC
    ) AS rn
  FROM "CollectionPointer" cp
  WHERE cp."collection_id" IS NULL
)
INSERT INTO "__collection_pointer_ranked" ("col", "creator", "new_id")
SELECT
  r."col",
  r."creator",
  CAST((SELECT max_id FROM seed) + r.rn AS BIGINT)
FROM ranked r;

UPDATE OR IGNORE "CollectionPointer"
SET "collection_id" = (
  SELECT rr."new_id"
  FROM "__collection_pointer_ranked" rr
  WHERE rr."col" = "CollectionPointer"."col"
    AND rr."creator" = "CollectionPointer"."creator"
)
WHERE "collection_id" IS NULL;

DROP TABLE "__collection_pointer_ranked";

DROP TABLE IF EXISTS "__collection_pointer_ranked";
CREATE TEMP TABLE "__collection_pointer_ranked" (
  "col" TEXT NOT NULL,
  "creator" TEXT NOT NULL,
  "new_id" BIGINT NOT NULL,
  PRIMARY KEY ("col", "creator")
);

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
        COALESCE(cp."firstSeenSlot", 9223372036854775807) ASC,
        CASE WHEN cp."firstSeenTxSignature" IS NULL THEN 1 ELSE 0 END ASC,
        cp."firstSeenTxSignature" ASC,
        cp."col" ASC,
        cp."creator" ASC
    ) AS rn
  FROM "CollectionPointer" cp
  WHERE cp."collection_id" IS NULL
)
INSERT INTO "__collection_pointer_ranked" ("col", "creator", "new_id")
SELECT
  r."col",
  r."creator",
  CAST((SELECT max_id FROM seed) + r.rn AS BIGINT)
FROM ranked r;

UPDATE "CollectionPointer"
SET "collection_id" = (
  SELECT rr."new_id"
  FROM "__collection_pointer_ranked" rr
  WHERE rr."col" = "CollectionPointer"."col"
    AND rr."creator" = "CollectionPointer"."creator"
)
WHERE "collection_id" IS NULL;

DROP TABLE "__collection_pointer_ranked";

CREATE TEMP TABLE "__collection_pointer_unresolved_guard" (
  "remaining" INTEGER NOT NULL CHECK ("remaining" = 0)
);

INSERT INTO "__collection_pointer_unresolved_guard" ("remaining")
SELECT COUNT(*)
FROM "CollectionPointer"
WHERE "collection_id" IS NULL;

DROP TABLE "__collection_pointer_unresolved_guard";

-- 20260305143000_add_indexer_state_monotonic_guard
DROP TRIGGER IF EXISTS "IndexerState_monotonic_guard";

CREATE TRIGGER "IndexerState_monotonic_guard"
BEFORE UPDATE ON "IndexerState"
FOR EACH ROW
WHEN
  OLD."lastSlot" IS NOT NULL
  AND (
    NEW."lastSlot" IS NULL
    OR NEW."lastSlot" < OLD."lastSlot"
    OR (
      NEW."lastSlot" = OLD."lastSlot"
      AND OLD."lastSignature" IS NOT NULL
      AND (
        NEW."lastSignature" IS NULL
        OR NEW."lastSignature" < OLD."lastSignature"
      )
    )
  )
BEGIN
  SELECT RAISE(IGNORE);
END;

-- 20260312110000_add_local_collection_id_counter
CREATE TABLE IF NOT EXISTS "IdCounter" (
  "scope" TEXT NOT NULL PRIMARY KEY,
  "nextValue" BIGINT NOT NULL,
  "updatedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO "IdCounter" ("scope", "nextValue", "updatedAt")
VALUES (
  'collection:global',
  COALESCE((SELECT MAX("collection_id") FROM "CollectionPointer"), 0) + 1,
  CURRENT_TIMESTAMP
)
ON CONFLICT("scope") DO UPDATE SET
  "nextValue" = CASE
    WHEN excluded."nextValue" > "IdCounter"."nextValue" THEN excluded."nextValue"
    ELSE "IdCounter"."nextValue"
  END,
  "updatedAt" = CURRENT_TIMESTAMP;

DROP TRIGGER IF EXISTS "CollectionPointer_assign_collection_id_after_insert";
DROP TRIGGER IF EXISTS "CollectionPointer_assign_collection_id_after_update";

CREATE TRIGGER "CollectionPointer_assign_collection_id_after_insert"
AFTER INSERT ON "CollectionPointer"
FOR EACH ROW
WHEN NEW."collection_id" IS NULL
BEGIN
  INSERT INTO "IdCounter" ("scope", "nextValue", "updatedAt")
  VALUES ('collection:global', 2, CURRENT_TIMESTAMP)
  ON CONFLICT("scope") DO UPDATE SET
    "nextValue" = "IdCounter"."nextValue" + 1,
    "updatedAt" = CURRENT_TIMESTAMP;

  UPDATE "CollectionPointer"
  SET "collection_id" = (
    SELECT "nextValue" - 1
    FROM "IdCounter"
    WHERE "scope" = 'collection:global'
  )
  WHERE "col" = NEW."col"
    AND "creator" = NEW."creator"
    AND "collection_id" IS NULL;
END;

CREATE TRIGGER "CollectionPointer_assign_collection_id_after_update"
AFTER UPDATE ON "CollectionPointer"
FOR EACH ROW
WHEN NEW."collection_id" IS NULL
BEGIN
  INSERT INTO "IdCounter" ("scope", "nextValue", "updatedAt")
  VALUES ('collection:global', 2, CURRENT_TIMESTAMP)
  ON CONFLICT("scope") DO UPDATE SET
    "nextValue" = "IdCounter"."nextValue" + 1,
    "updatedAt" = CURRENT_TIMESTAMP;

  UPDATE "CollectionPointer"
  SET "collection_id" = (
    SELECT "nextValue" - 1
    FROM "IdCounter"
    WHERE "scope" = 'collection:global'
  )
  WHERE "col" = NEW."col"
    AND "creator" = NEW."creator"
    AND "collection_id" IS NULL;
END;

UPDATE "CollectionPointer"
SET "collection_id" = "collection_id"
WHERE "collection_id" IS NULL;

-- 20260305235500_add_orphan_feedback
CREATE TABLE "OrphanFeedback" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "agentId" TEXT NOT NULL,
    "client" TEXT NOT NULL,
    "feedbackIndex" BIGINT NOT NULL,
    "value" TEXT NOT NULL DEFAULT '0',
    "valueDecimals" INTEGER NOT NULL DEFAULT 0,
    "score" INTEGER,
    "tag1" TEXT NOT NULL,
    "tag2" TEXT NOT NULL,
    "endpoint" TEXT NOT NULL,
    "feedbackUri" TEXT NOT NULL,
    "feedbackHash" BLOB,
    "runningDigest" BLOB,
    "atomEnabled" BOOLEAN NOT NULL DEFAULT false,
    "newTrustTier" INTEGER NOT NULL DEFAULT 0,
    "newQualityScore" INTEGER NOT NULL DEFAULT 0,
    "newConfidence" INTEGER NOT NULL DEFAULT 0,
    "newRiskScore" INTEGER NOT NULL DEFAULT 0,
    "newDiversityRatio" INTEGER NOT NULL DEFAULT 0,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "txSignature" TEXT,
    "slot" BIGINT,
    "txIndex" INTEGER,
    "eventOrdinal" INTEGER
);

CREATE UNIQUE INDEX "OrphanFeedback_agentId_client_feedbackIndex_key"
ON "OrphanFeedback"("agentId", "client", "feedbackIndex");

CREATE INDEX "OrphanFeedback_agentId_idx"
ON "OrphanFeedback"("agentId");

CREATE INDEX "OrphanFeedback_agentId_client_feedbackIndex_idx"
ON "OrphanFeedback"("agentId", "client", "feedbackIndex");

-- 20260306164000_add_indexer_state_last_tx_index
ALTER TABLE "IndexerState"
ADD COLUMN "lastTxIndex" INTEGER;

-- 20260306210000_extend_orphan_response_proof
ALTER TABLE "OrphanResponse" ADD COLUMN "sealHash" BLOB;
ALTER TABLE "OrphanResponse" ADD COLUMN "txIndex" INTEGER;
ALTER TABLE "OrphanResponse" ADD COLUMN "eventOrdinal" INTEGER;

-- 20260306234500_sqlite_fix_feedback_value_text
PRAGMA defer_foreign_keys=ON;
PRAGMA foreign_keys=OFF;

CREATE TABLE "new_Feedback" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "feedback_id" BIGINT,
    "agentId" TEXT NOT NULL,
    "client" TEXT NOT NULL,
    "feedbackIndex" BIGINT NOT NULL,
    "value" TEXT NOT NULL DEFAULT '0',
    "valueDecimals" INTEGER NOT NULL DEFAULT 0,
    "score" INTEGER,
    "tag1" TEXT NOT NULL,
    "tag2" TEXT NOT NULL,
    "endpoint" TEXT NOT NULL,
    "feedbackUri" TEXT NOT NULL,
    "feedbackHash" BLOB,
    "revoked" BOOLEAN NOT NULL DEFAULT false,
    "runningDigest" BLOB,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdTxSignature" TEXT,
    "createdSlot" BIGINT,
    "txIndex" INTEGER,
    "eventOrdinal" INTEGER,
    "revokedTxSignature" TEXT,
    "revokedSlot" BIGINT,
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "verifiedAt" DATETIME,
    CONSTRAINT "Feedback_agentId_fkey" FOREIGN KEY ("agentId") REFERENCES "Agent" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO "new_Feedback" (
    "id",
    "feedback_id",
    "agentId",
    "client",
    "feedbackIndex",
    "value",
    "valueDecimals",
    "score",
    "tag1",
    "tag2",
    "endpoint",
    "feedbackUri",
    "feedbackHash",
    "revoked",
    "runningDigest",
    "createdAt",
    "createdTxSignature",
    "createdSlot",
    "txIndex",
    "eventOrdinal",
    "revokedTxSignature",
    "revokedSlot",
    "status",
    "verifiedAt"
)
SELECT
    "id",
    "feedback_id",
    "agentId",
    "client",
    "feedbackIndex",
    CAST("value" AS TEXT),
    "valueDecimals",
    "score",
    "tag1",
    "tag2",
    "endpoint",
    "feedbackUri",
    "feedbackHash",
    "revoked",
    "runningDigest",
    "createdAt",
    "createdTxSignature",
    "createdSlot",
    "txIndex",
    "eventOrdinal",
    "revokedTxSignature",
    "revokedSlot",
    "status",
    "verifiedAt"
FROM "Feedback";

DROP TABLE "Feedback";
ALTER TABLE "new_Feedback" RENAME TO "Feedback";

CREATE INDEX "Feedback_agentId_idx" ON "Feedback"("agentId");
CREATE INDEX "Feedback_agentId_feedback_id_idx" ON "Feedback"("agentId", "feedback_id");
CREATE INDEX "Feedback_client_idx" ON "Feedback"("client");
CREATE INDEX "Feedback_score_idx" ON "Feedback"("score");
CREATE INDEX "Feedback_tag1_idx" ON "Feedback"("tag1");
CREATE INDEX "Feedback_tag2_idx" ON "Feedback"("tag2");
CREATE INDEX "Feedback_status_idx" ON "Feedback"("status");
CREATE UNIQUE INDEX "Feedback_agentId_client_feedbackIndex_key" ON "Feedback"("agentId", "client", "feedbackIndex");
CREATE UNIQUE INDEX "Feedback_agentId_feedback_id_key" ON "Feedback"("agentId", "feedback_id");

CREATE INDEX IF NOT EXISTS "CollectionPointer_col_idx" ON "CollectionPointer"("col");

PRAGMA foreign_keys=ON;
PRAGMA defer_foreign_keys=OFF;

-- 20260307000500_fix_indexer_state_monotonic_guard_tx_index
DROP TRIGGER IF EXISTS "IndexerState_monotonic_guard";

CREATE TRIGGER "IndexerState_monotonic_guard"
BEFORE UPDATE ON "IndexerState"
FOR EACH ROW
WHEN
  OLD."lastSlot" IS NOT NULL
  AND (
    NEW."lastSlot" IS NULL
    OR NEW."lastSlot" < OLD."lastSlot"
    OR (
      NEW."lastSlot" = OLD."lastSlot"
      AND (
        COALESCE(OLD."lastTxIndex", -1) > COALESCE(NEW."lastTxIndex", -1)
        OR (
          COALESCE(OLD."lastTxIndex", -1) = COALESCE(NEW."lastTxIndex", -1)
          AND OLD."lastSignature" IS NOT NULL
          AND (
            NEW."lastSignature" IS NULL
            OR NEW."lastSignature" < OLD."lastSignature"
          )
        )
      )
    )
  )
BEGIN
  SELECT RAISE(IGNORE);
END;
