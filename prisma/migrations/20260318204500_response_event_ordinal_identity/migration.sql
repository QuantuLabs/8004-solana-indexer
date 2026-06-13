DROP INDEX IF EXISTS "FeedbackResponse_feedbackId_responder_txSignature_key";
DROP INDEX IF EXISTS "OrphanResponse_agentId_client_feedbackIndex_responder_txSignature_key";

CREATE UNIQUE INDEX IF NOT EXISTS "FeedbackResponse_feedbackId_responder_txSignature_eventOrdinal_key"
ON "FeedbackResponse"("feedbackId", "responder", "txSignature", "eventOrdinal");

CREATE UNIQUE INDEX IF NOT EXISTS "OrphanResponse_agentId_client_feedbackIndex_responder_txSignature_eventOrdinal_key"
ON "OrphanResponse"("agentId", "client", "feedbackIndex", "responder", "txSignature", "eventOrdinal");
