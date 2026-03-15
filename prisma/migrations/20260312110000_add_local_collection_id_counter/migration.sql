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
