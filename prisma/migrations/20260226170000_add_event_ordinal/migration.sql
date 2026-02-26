-- Add deterministic intra-transaction ordering fields
ALTER TABLE "Agent" ADD COLUMN "eventOrdinal" INTEGER;

ALTER TABLE "AgentMetadata" ADD COLUMN "txIndex" INTEGER;
ALTER TABLE "AgentMetadata" ADD COLUMN "eventOrdinal" INTEGER;

ALTER TABLE "Feedback" ADD COLUMN "eventOrdinal" INTEGER;

ALTER TABLE "FeedbackResponse" ADD COLUMN "eventOrdinal" INTEGER;

ALTER TABLE "Revocation" ADD COLUMN "eventOrdinal" INTEGER;

ALTER TABLE "Validation" ADD COLUMN "requestTxIndex" INTEGER;
ALTER TABLE "Validation" ADD COLUMN "requestEventOrdinal" INTEGER;
ALTER TABLE "Validation" ADD COLUMN "responseTxIndex" INTEGER;
ALTER TABLE "Validation" ADD COLUMN "responseEventOrdinal" INTEGER;
