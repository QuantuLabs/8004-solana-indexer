ALTER TABLE "OrphanResponse" ADD COLUMN "sealHash" BLOB;
ALTER TABLE "OrphanResponse" ADD COLUMN "txIndex" INTEGER;
ALTER TABLE "OrphanResponse" ADD COLUMN "eventOrdinal" INTEGER;
