#!/usr/bin/env node
/**
 * Initialize Supabase database with fresh schema
 * Usage: node scripts/init-supabase.js
 */

import pg from 'pg';
const { Client } = pg;

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error('❌ DATABASE_URL environment variable is required');
  console.error('   Set it to your Supabase connection string');
  process.exit(1);
}

const initSQL = `
-- Drop all tables if they exist
DROP TABLE IF EXISTS "EventLog" CASCADE;
DROP TABLE IF EXISTS "IndexerState" CASCADE;
DROP TABLE IF EXISTS "Registry" CASCADE;
DROP TABLE IF EXISTS "FeedbackResponse" CASCADE;
DROP TABLE IF EXISTS "Validation" CASCADE;
DROP TABLE IF EXISTS "Feedback" CASCADE;
DROP TABLE IF EXISTS "AgentMetadata" CASCADE;
DROP TABLE IF EXISTS "Agent" CASCADE;

-- CreateTable: Agent
CREATE TABLE "Agent" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "owner" TEXT NOT NULL,
    "wallet" TEXT,
    "uri" TEXT NOT NULL,
    "nftName" TEXT NOT NULL,
    "collection" TEXT NOT NULL,
    "registry" TEXT NOT NULL,
    "createdAt" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP NOT NULL,
    "createdTxSignature" TEXT,
    "createdSlot" BIGINT
);

-- CreateTable: AgentMetadata
CREATE TABLE "AgentMetadata" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "agentId" TEXT NOT NULL,
    "key" TEXT NOT NULL,
    "value" BYTEA NOT NULL,
    "immutable" BOOLEAN NOT NULL DEFAULT false,
    "txSignature" TEXT,
    "slot" BIGINT,
    CONSTRAINT "AgentMetadata_agentId_fkey" FOREIGN KEY ("agentId") REFERENCES "Agent" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable: Feedback (with client in unique constraint)
CREATE TABLE "Feedback" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "agentId" TEXT NOT NULL,
    "client" TEXT NOT NULL,
    "feedbackIndex" BIGINT NOT NULL,
    "score" INTEGER NOT NULL,
    "tag1" TEXT NOT NULL,
    "tag2" TEXT NOT NULL,
    "endpoint" TEXT NOT NULL,
    "feedbackUri" TEXT NOT NULL,
    "feedbackHash" BYTEA NOT NULL,
    "revoked" BOOLEAN NOT NULL DEFAULT false,
    "createdAt" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdTxSignature" TEXT,
    "createdSlot" BIGINT,
    "revokedTxSignature" TEXT,
    "revokedSlot" BIGINT,
    CONSTRAINT "Feedback_agentId_fkey" FOREIGN KEY ("agentId") REFERENCES "Agent" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable: FeedbackResponse
CREATE TABLE "FeedbackResponse" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "feedbackId" TEXT NOT NULL,
    "responder" TEXT NOT NULL,
    "responseUri" TEXT NOT NULL,
    "responseHash" BYTEA NOT NULL,
    "createdAt" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "txSignature" TEXT,
    "slot" BIGINT,
    CONSTRAINT "FeedbackResponse_feedbackId_fkey" FOREIGN KEY ("feedbackId") REFERENCES "Feedback" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable: Validation
CREATE TABLE "Validation" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "agentId" TEXT NOT NULL,
    "validator" TEXT NOT NULL,
    "requester" TEXT NOT NULL,
    "nonce" INTEGER NOT NULL,
    "requestUri" TEXT NOT NULL,
    "requestHash" BYTEA NOT NULL,
    "response" INTEGER,
    "responseUri" TEXT,
    "responseHash" BYTEA,
    "tag" TEXT,
    "createdAt" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "respondedAt" TIMESTAMP,
    "requestTxSignature" TEXT,
    "requestSlot" BIGINT,
    "responseTxSignature" TEXT,
    "responseSlot" BIGINT,
    CONSTRAINT "Validation_agentId_fkey" FOREIGN KEY ("agentId") REFERENCES "Agent" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable: Registry
CREATE TABLE "Registry" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "collection" TEXT NOT NULL,
    "registryType" TEXT NOT NULL,
    "authority" TEXT NOT NULL,
    "baseIndex" INTEGER,
    "createdAt" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "txSignature" TEXT,
    "slot" BIGINT
);

-- CreateTable: IndexerState
CREATE TABLE "IndexerState" (
    "id" TEXT NOT NULL PRIMARY KEY DEFAULT 'main',
    "lastSignature" TEXT,
    "lastSlot" BIGINT,
    "updatedAt" TIMESTAMP NOT NULL
);

-- CreateTable: EventLog
CREATE TABLE "EventLog" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "eventType" TEXT NOT NULL,
    "signature" TEXT NOT NULL,
    "slot" BIGINT NOT NULL,
    "blockTime" TIMESTAMP NOT NULL,
    "data" JSONB NOT NULL,
    "processed" BOOLEAN NOT NULL DEFAULT false,
    "error" TEXT,
    "createdAt" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX "Agent_owner_idx" ON "Agent"("owner");
CREATE INDEX "Agent_collection_idx" ON "Agent"("collection");
CREATE INDEX "Agent_registry_idx" ON "Agent"("registry");
CREATE INDEX "AgentMetadata_agentId_idx" ON "AgentMetadata"("agentId");
CREATE UNIQUE INDEX "AgentMetadata_agentId_key_key" ON "AgentMetadata"("agentId", "key");
CREATE INDEX "Feedback_agentId_idx" ON "Feedback"("agentId");
CREATE INDEX "Feedback_client_idx" ON "Feedback"("client");
CREATE INDEX "Feedback_score_idx" ON "Feedback"("score");
CREATE INDEX "Feedback_tag1_idx" ON "Feedback"("tag1");
CREATE INDEX "Feedback_tag2_idx" ON "Feedback"("tag2");
CREATE UNIQUE INDEX "Feedback_agentId_client_feedbackIndex_key" ON "Feedback"("agentId", "client", "feedbackIndex");
CREATE INDEX "FeedbackResponse_feedbackId_idx" ON "FeedbackResponse"("feedbackId");
CREATE INDEX "Validation_agentId_idx" ON "Validation"("agentId");
CREATE INDEX "Validation_validator_idx" ON "Validation"("validator");
CREATE INDEX "Validation_requester_idx" ON "Validation"("requester");
CREATE UNIQUE INDEX "Validation_agentId_validator_nonce_key" ON "Validation"("agentId", "validator", "nonce");
CREATE UNIQUE INDEX "Registry_collection_key" ON "Registry"("collection");
CREATE INDEX "Registry_registryType_idx" ON "Registry"("registryType");
CREATE INDEX "Registry_authority_idx" ON "Registry"("authority");
CREATE INDEX "EventLog_eventType_idx" ON "EventLog"("eventType");
CREATE INDEX "EventLog_signature_idx" ON "EventLog"("signature");
CREATE INDEX "EventLog_slot_idx" ON "EventLog"("slot");
CREATE INDEX "EventLog_processed_idx" ON "EventLog"("processed");
`;

async function main() {
  console.log('🔗 Connecting to Supabase...');
  const client = new Client({ connectionString: DATABASE_URL });

  try {
    await client.connect();
    console.log('✅ Connected to Supabase\n');

    console.log('🗑️  Dropping existing tables...');
    console.log('📋 Creating fresh schema with updated constraints...\n');

    await client.query(initSQL);

    console.log('✅ Database initialized successfully!');
    console.log('\n📊 Verifying Feedback table constraint:');

    const result = await client.query(`
      SELECT
          indexname,
          indexdef
      FROM pg_indexes
      WHERE tablename = 'Feedback'
        AND indexname LIKE '%feedbackIndex%';
    `);

    console.log(result.rows);

  } catch (error) {
    console.error('❌ Initialization failed:', error.message);
    console.error(error.stack);
    process.exit(1);
  } finally {
    await client.end();
  }
}

main();
