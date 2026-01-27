-- CreateTable
CREATE TABLE "Agent" (
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
    "createdSlot" BIGINT
);

-- CreateTable
CREATE TABLE "AgentMetadata" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "agentId" TEXT NOT NULL,
    "key" TEXT NOT NULL,
    "value" BLOB NOT NULL,
    "immutable" BOOLEAN NOT NULL DEFAULT false,
    "txSignature" TEXT,
    "slot" BIGINT,
    CONSTRAINT "AgentMetadata_agentId_fkey" FOREIGN KEY ("agentId") REFERENCES "Agent" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "Feedback" (
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
    CONSTRAINT "Feedback_agentId_fkey" FOREIGN KEY ("agentId") REFERENCES "Agent" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "FeedbackResponse" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "feedbackId" TEXT NOT NULL,
    "responder" TEXT NOT NULL,
    "responseUri" TEXT NOT NULL,
    "responseHash" BLOB,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "txSignature" TEXT,
    "slot" BIGINT,
    CONSTRAINT "FeedbackResponse_feedbackId_fkey" FOREIGN KEY ("feedbackId") REFERENCES "Feedback" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "OrphanResponse" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "agentId" TEXT NOT NULL,
    "client" TEXT NOT NULL,
    "feedbackIndex" BIGINT NOT NULL,
    "responder" TEXT NOT NULL,
    "responseUri" TEXT NOT NULL,
    "responseHash" BLOB,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "txSignature" TEXT,
    "slot" BIGINT
);

-- CreateTable
CREATE TABLE "Validation" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "agentId" TEXT NOT NULL,
    "validator" TEXT NOT NULL,
    "requester" TEXT NOT NULL,
    "nonce" INTEGER NOT NULL,
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
    CONSTRAINT "Validation_agentId_fkey" FOREIGN KEY ("agentId") REFERENCES "Agent" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "Registry" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "collection" TEXT NOT NULL,
    "registryType" TEXT NOT NULL,
    "authority" TEXT NOT NULL,
    "baseIndex" INTEGER,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "txSignature" TEXT,
    "slot" BIGINT
);

-- CreateTable
CREATE TABLE "IndexerState" (
    "id" TEXT NOT NULL PRIMARY KEY DEFAULT 'main',
    "lastSignature" TEXT,
    "lastSlot" BIGINT,
    "updatedAt" DATETIME NOT NULL
);

-- CreateTable
CREATE TABLE "EventLog" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "eventType" TEXT NOT NULL,
    "signature" TEXT NOT NULL,
    "slot" BIGINT NOT NULL,
    "blockTime" DATETIME NOT NULL,
    "data" JSONB NOT NULL,
    "processed" BOOLEAN NOT NULL DEFAULT false,
    "error" TEXT,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateIndex
CREATE INDEX "Agent_owner_idx" ON "Agent"("owner");

-- CreateIndex
CREATE INDEX "Agent_collection_idx" ON "Agent"("collection");

-- CreateIndex
CREATE INDEX "Agent_registry_idx" ON "Agent"("registry");

-- CreateIndex
CREATE INDEX "AgentMetadata_agentId_idx" ON "AgentMetadata"("agentId");

-- CreateIndex
CREATE UNIQUE INDEX "AgentMetadata_agentId_key_key" ON "AgentMetadata"("agentId", "key");

-- CreateIndex
CREATE INDEX "Feedback_agentId_idx" ON "Feedback"("agentId");

-- CreateIndex
CREATE INDEX "Feedback_client_idx" ON "Feedback"("client");

-- CreateIndex
CREATE INDEX "Feedback_score_idx" ON "Feedback"("score");

-- CreateIndex
CREATE INDEX "Feedback_tag1_idx" ON "Feedback"("tag1");

-- CreateIndex
CREATE INDEX "Feedback_tag2_idx" ON "Feedback"("tag2");

-- CreateIndex
CREATE UNIQUE INDEX "Feedback_agentId_client_feedbackIndex_key" ON "Feedback"("agentId", "client", "feedbackIndex");

-- CreateIndex
CREATE INDEX "FeedbackResponse_feedbackId_idx" ON "FeedbackResponse"("feedbackId");

-- CreateIndex
CREATE INDEX "FeedbackResponse_feedbackId_responder_idx" ON "FeedbackResponse"("feedbackId", "responder");

-- CreateIndex
CREATE UNIQUE INDEX "FeedbackResponse_feedbackId_responder_txSignature_key" ON "FeedbackResponse"("feedbackId", "responder", "txSignature");

-- CreateIndex
CREATE INDEX "OrphanResponse_agentId_idx" ON "OrphanResponse"("agentId");

-- CreateIndex
CREATE INDEX "OrphanResponse_agentId_client_feedbackIndex_idx" ON "OrphanResponse"("agentId", "client", "feedbackIndex");

-- CreateIndex
CREATE UNIQUE INDEX "OrphanResponse_agentId_client_feedbackIndex_responder_txSignature_key" ON "OrphanResponse"("agentId", "client", "feedbackIndex", "responder", "txSignature");

-- CreateIndex
CREATE INDEX "Validation_agentId_idx" ON "Validation"("agentId");

-- CreateIndex
CREATE INDEX "Validation_validator_idx" ON "Validation"("validator");

-- CreateIndex
CREATE INDEX "Validation_requester_idx" ON "Validation"("requester");

-- CreateIndex
CREATE UNIQUE INDEX "Validation_agentId_validator_nonce_key" ON "Validation"("agentId", "validator", "nonce");

-- CreateIndex
CREATE UNIQUE INDEX "Registry_collection_key" ON "Registry"("collection");

-- CreateIndex
CREATE INDEX "Registry_registryType_idx" ON "Registry"("registryType");

-- CreateIndex
CREATE INDEX "Registry_authority_idx" ON "Registry"("authority");

-- CreateIndex
CREATE INDEX "EventLog_eventType_idx" ON "EventLog"("eventType");

-- CreateIndex
CREATE INDEX "EventLog_signature_idx" ON "EventLog"("signature");

-- CreateIndex
CREATE INDEX "EventLog_slot_idx" ON "EventLog"("slot");

-- CreateIndex
CREATE INDEX "EventLog_processed_idx" ON "EventLog"("processed");
