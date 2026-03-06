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
