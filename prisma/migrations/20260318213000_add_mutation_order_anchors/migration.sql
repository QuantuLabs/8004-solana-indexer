ALTER TABLE "Agent" ADD COLUMN "mutation_order_key" TEXT;
ALTER TABLE "AgentMetadata" ADD COLUMN "mutation_order_key" TEXT;

UPDATE "Agent"
SET "mutation_order_key" =
  printf('%020lld', "createdSlot")
  || ':' || printf('%010d', COALESCE("txIndex", 2147483647))
  || ':' || printf('%010d', COALESCE("eventOrdinal", 2147483647))
  || ':' || COALESCE("createdTxSignature", '')
WHERE "mutation_order_key" IS NULL
  AND "createdSlot" IS NOT NULL;

UPDATE "AgentMetadata"
SET "mutation_order_key" =
  printf('%020lld', "slot")
  || ':' || printf('%010d', COALESCE("txIndex", 2147483647))
  || ':' || printf('%010d', COALESCE("eventOrdinal", 2147483647))
  || ':' || COALESCE("txSignature", '')
WHERE "mutation_order_key" IS NULL
  AND "slot" IS NOT NULL;
