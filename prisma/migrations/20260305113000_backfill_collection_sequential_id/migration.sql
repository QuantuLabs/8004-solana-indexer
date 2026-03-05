-- Non-blocking optimistic backfill for missing collection_id values:
-- - deterministic ordering for assignment
-- - 3 optimistic passes (OR IGNORE) to reduce transient conflicts
-- - 1 strict convergence pass
-- - final guard that aborts if unresolved NULL remains

-- Pass 1: non-blocking optimistic assignment using materialized ranking.
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

-- Pass 2: retry optimistic assignment with refreshed seed/rank.
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

-- Pass 3: retry optimistic assignment with refreshed seed/rank.
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

-- Final strict convergence pass.
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

-- Guardrail: abort migration if unresolved NULL IDs remain.
CREATE TEMP TABLE "__collection_pointer_unresolved_guard" (
  "remaining" INTEGER NOT NULL CHECK ("remaining" = 0)
);

INSERT INTO "__collection_pointer_unresolved_guard" ("remaining")
SELECT COUNT(*)
FROM "CollectionPointer"
WHERE "collection_id" IS NULL;

DROP TABLE "__collection_pointer_unresolved_guard";
