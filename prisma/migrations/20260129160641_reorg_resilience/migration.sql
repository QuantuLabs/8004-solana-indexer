-- CreateTable
CREATE TABLE "AgentDigestCache" (
    "agentId" TEXT NOT NULL PRIMARY KEY,
    "feedbackDigest" BLOB,
    "feedbackCount" BIGINT NOT NULL DEFAULT 0,
    "responseDigest" BLOB,
    "responseCount" BIGINT NOT NULL DEFAULT 0,
    "revokeDigest" BLOB,
    "revokeCount" BIGINT NOT NULL DEFAULT 0,
    "lastVerifiedAt" DATETIME,
    "lastVerifiedSlot" BIGINT,
    "needsGapFill" BOOLEAN NOT NULL DEFAULT false,
    "gapFillFromSlot" BIGINT
);

-- RedefineTables
PRAGMA defer_foreign_keys=ON;
PRAGMA foreign_keys=OFF;
CREATE TABLE "new_Agent" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "owner" TEXT NOT NULL,
    "wallet" TEXT,
    "uri" TEXT NOT NULL,
    "nftName" TEXT NOT NULL,
    "collection" TEXT NOT NULL,
    "registry" TEXT NOT NULL,
    "atomEnabled" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" DATETIME NOT NULL,
    "createdTxSignature" TEXT,
    "createdSlot" BIGINT,
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "verifiedAt" DATETIME,
    "verifiedSlot" BIGINT
);
INSERT INTO "new_Agent" ("atomEnabled", "collection", "createdAt", "createdSlot", "createdTxSignature", "id", "nftName", "owner", "registry", "updatedAt", "uri", "wallet") SELECT "atomEnabled", "collection", "createdAt", "createdSlot", "createdTxSignature", "id", "nftName", "owner", "registry", "updatedAt", "uri", "wallet" FROM "Agent";
DROP TABLE "Agent";
ALTER TABLE "new_Agent" RENAME TO "Agent";
CREATE INDEX "Agent_owner_idx" ON "Agent"("owner");
CREATE INDEX "Agent_collection_idx" ON "Agent"("collection");
CREATE INDEX "Agent_registry_idx" ON "Agent"("registry");
CREATE INDEX "Agent_status_idx" ON "Agent"("status");
CREATE TABLE "new_AgentMetadata" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "agentId" TEXT NOT NULL,
    "key" TEXT NOT NULL,
    "value" BLOB NOT NULL,
    "immutable" BOOLEAN NOT NULL DEFAULT false,
    "txSignature" TEXT,
    "slot" BIGINT,
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "verifiedAt" DATETIME,
    CONSTRAINT "AgentMetadata_agentId_fkey" FOREIGN KEY ("agentId") REFERENCES "Agent" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);
INSERT INTO "new_AgentMetadata" ("agentId", "id", "immutable", "key", "slot", "txSignature", "value") SELECT "agentId", "id", "immutable", "key", "slot", "txSignature", "value" FROM "AgentMetadata";
DROP TABLE "AgentMetadata";
ALTER TABLE "new_AgentMetadata" RENAME TO "AgentMetadata";
CREATE INDEX "AgentMetadata_agentId_idx" ON "AgentMetadata"("agentId");
CREATE INDEX "AgentMetadata_status_idx" ON "AgentMetadata"("status");
CREATE UNIQUE INDEX "AgentMetadata_agentId_key_key" ON "AgentMetadata"("agentId", "key");
CREATE TABLE "new_Feedback" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "agentId" TEXT NOT NULL,
    "client" TEXT NOT NULL,
    "feedbackIndex" BIGINT NOT NULL,
    "value" BIGINT NOT NULL DEFAULT 0,
    "valueDecimals" INTEGER NOT NULL DEFAULT 0,
    "score" INTEGER,
    "tag1" TEXT NOT NULL,
    "tag2" TEXT NOT NULL,
    "endpoint" TEXT NOT NULL,
    "feedbackUri" TEXT NOT NULL,
    "feedbackHash" BLOB,
    "revoked" BOOLEAN NOT NULL DEFAULT false,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdTxSignature" TEXT,
    "createdSlot" BIGINT,
    "revokedTxSignature" TEXT,
    "revokedSlot" BIGINT,
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "verifiedAt" DATETIME,
    CONSTRAINT "Feedback_agentId_fkey" FOREIGN KEY ("agentId") REFERENCES "Agent" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);
INSERT INTO "new_Feedback" ("agentId", "client", "createdAt", "createdSlot", "createdTxSignature", "endpoint", "feedbackHash", "feedbackIndex", "feedbackUri", "id", "revoked", "revokedSlot", "revokedTxSignature", "score", "tag1", "tag2", "value", "valueDecimals") SELECT "agentId", "client", "createdAt", "createdSlot", "createdTxSignature", "endpoint", "feedbackHash", "feedbackIndex", "feedbackUri", "id", "revoked", "revokedSlot", "revokedTxSignature", "score", "tag1", "tag2", "value", "valueDecimals" FROM "Feedback";
DROP TABLE "Feedback";
ALTER TABLE "new_Feedback" RENAME TO "Feedback";
CREATE INDEX "Feedback_agentId_idx" ON "Feedback"("agentId");
CREATE INDEX "Feedback_client_idx" ON "Feedback"("client");
CREATE INDEX "Feedback_score_idx" ON "Feedback"("score");
CREATE INDEX "Feedback_tag1_idx" ON "Feedback"("tag1");
CREATE INDEX "Feedback_tag2_idx" ON "Feedback"("tag2");
CREATE INDEX "Feedback_status_idx" ON "Feedback"("status");
CREATE UNIQUE INDEX "Feedback_agentId_client_feedbackIndex_key" ON "Feedback"("agentId", "client", "feedbackIndex");
CREATE TABLE "new_FeedbackResponse" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "feedbackId" TEXT NOT NULL,
    "responder" TEXT NOT NULL,
    "responseUri" TEXT NOT NULL,
    "responseHash" BLOB,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "txSignature" TEXT,
    "slot" BIGINT,
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "verifiedAt" DATETIME,
    CONSTRAINT "FeedbackResponse_feedbackId_fkey" FOREIGN KEY ("feedbackId") REFERENCES "Feedback" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);
INSERT INTO "new_FeedbackResponse" ("createdAt", "feedbackId", "id", "responder", "responseHash", "responseUri", "slot", "txSignature") SELECT "createdAt", "feedbackId", "id", "responder", "responseHash", "responseUri", "slot", "txSignature" FROM "FeedbackResponse";
DROP TABLE "FeedbackResponse";
ALTER TABLE "new_FeedbackResponse" RENAME TO "FeedbackResponse";
CREATE INDEX "FeedbackResponse_feedbackId_idx" ON "FeedbackResponse"("feedbackId");
CREATE INDEX "FeedbackResponse_feedbackId_responder_idx" ON "FeedbackResponse"("feedbackId", "responder");
CREATE INDEX "FeedbackResponse_status_idx" ON "FeedbackResponse"("status");
CREATE UNIQUE INDEX "FeedbackResponse_feedbackId_responder_txSignature_key" ON "FeedbackResponse"("feedbackId", "responder", "txSignature");
CREATE TABLE "new_IndexerState" (
    "id" TEXT NOT NULL PRIMARY KEY DEFAULT 'main',
    "lastSignature" TEXT,
    "lastSlot" BIGINT,
    "source" TEXT NOT NULL DEFAULT 'poller',
    "updatedAt" DATETIME NOT NULL
);
INSERT INTO "new_IndexerState" ("id", "lastSignature", "lastSlot", "updatedAt") SELECT "id", "lastSignature", "lastSlot", "updatedAt" FROM "IndexerState";
DROP TABLE "IndexerState";
ALTER TABLE "new_IndexerState" RENAME TO "IndexerState";
CREATE TABLE "new_Registry" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "collection" TEXT NOT NULL,
    "registryType" TEXT NOT NULL,
    "authority" TEXT NOT NULL,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "txSignature" TEXT,
    "slot" BIGINT,
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "verifiedAt" DATETIME
);
INSERT INTO "new_Registry" ("authority", "collection", "createdAt", "id", "registryType", "slot", "txSignature") SELECT "authority", "collection", "createdAt", "id", "registryType", "slot", "txSignature" FROM "Registry";
DROP TABLE "Registry";
ALTER TABLE "new_Registry" RENAME TO "Registry";
CREATE UNIQUE INDEX "Registry_collection_key" ON "Registry"("collection");
CREATE INDEX "Registry_registryType_idx" ON "Registry"("registryType");
CREATE INDEX "Registry_authority_idx" ON "Registry"("authority");
CREATE INDEX "Registry_status_idx" ON "Registry"("status");
CREATE TABLE "new_Validation" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "agentId" TEXT NOT NULL,
    "validator" TEXT NOT NULL,
    "requester" TEXT NOT NULL,
    "nonce" BIGINT NOT NULL,
    "requestUri" TEXT,
    "requestHash" BLOB,
    "response" INTEGER,
    "responseUri" TEXT,
    "responseHash" BLOB,
    "tag" TEXT,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "respondedAt" DATETIME,
    "requestTxSignature" TEXT,
    "requestSlot" BIGINT,
    "responseTxSignature" TEXT,
    "responseSlot" BIGINT,
    "chainStatus" TEXT NOT NULL DEFAULT 'PENDING',
    "chainVerifiedAt" DATETIME,
    CONSTRAINT "Validation_agentId_fkey" FOREIGN KEY ("agentId") REFERENCES "Agent" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);
INSERT INTO "new_Validation" ("agentId", "createdAt", "id", "nonce", "requestHash", "requestSlot", "requestTxSignature", "requestUri", "requester", "respondedAt", "response", "responseHash", "responseSlot", "responseTxSignature", "responseUri", "tag", "validator") SELECT "agentId", "createdAt", "id", "nonce", "requestHash", "requestSlot", "requestTxSignature", "requestUri", "requester", "respondedAt", "response", "responseHash", "responseSlot", "responseTxSignature", "responseUri", "tag", "validator" FROM "Validation";
DROP TABLE "Validation";
ALTER TABLE "new_Validation" RENAME TO "Validation";
CREATE INDEX "Validation_agentId_idx" ON "Validation"("agentId");
CREATE INDEX "Validation_validator_idx" ON "Validation"("validator");
CREATE INDEX "Validation_requester_idx" ON "Validation"("requester");
CREATE INDEX "Validation_chainStatus_idx" ON "Validation"("chainStatus");
CREATE UNIQUE INDEX "Validation_agentId_validator_nonce_key" ON "Validation"("agentId", "validator", "nonce");
PRAGMA foreign_keys=ON;
PRAGMA defer_foreign_keys=OFF;
