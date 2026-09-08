ALTER TABLE "Agent" ADD COLUMN "atom_aux_order_key" TEXT;

WITH anchors AS (
  SELECT "id",
         printf('%020lld', "createdSlot")
         || ':' || printf('%010d', COALESCE("txIndex", 2147483647))
         || ':' || printf('%010d', COALESCE("eventOrdinal", 2147483647))
         || ':' || COALESCE("createdTxSignature", '') AS value
  FROM "Agent"
  WHERE "createdSlot" IS NOT NULL
  UNION ALL
  SELECT "id", "mutation_order_key" FROM "Agent" WHERE "mutation_order_key" IS NOT NULL
  UNION ALL
  SELECT a."id",
         printf('%020lld', s."lastSlot")
         || ':' || printf('%010d', COALESCE(s."lastTxIndex", 2147483647))
         || ':2147483647:' || s."lastSignature"
  FROM "Agent" a
  JOIN "IndexerState" s ON s."id" = 'main'
  WHERE s."lastSlot" IS NOT NULL AND s."lastSignature" IS NOT NULL
), floors AS (
  SELECT "id", MAX(value) AS value FROM anchors GROUP BY "id"
)
UPDATE "Agent"
SET "atom_aux_order_key" = COALESCE("atom_aux_order_key", (SELECT value FROM floors WHERE floors."id" = "Agent"."id"));
