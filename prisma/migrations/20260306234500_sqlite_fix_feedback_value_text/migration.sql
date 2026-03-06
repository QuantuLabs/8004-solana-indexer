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
