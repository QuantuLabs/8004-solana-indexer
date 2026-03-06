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
