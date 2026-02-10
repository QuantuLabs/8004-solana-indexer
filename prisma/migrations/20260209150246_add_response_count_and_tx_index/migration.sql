/*
  Warnings:

  - You are about to alter the column `atomEnabled` on the `Revocation` table. The data in that column could be lost. The data in that column will be cast from `Int` to `Boolean`.
  - You are about to alter the column `feedbackIndex` on the `Revocation` table. The data in that column could be lost. The data in that column will be cast from `Int` to `BigInt`.
  - You are about to alter the column `hadImpact` on the `Revocation` table. The data in that column could be lost. The data in that column will be cast from `Int` to `Boolean`.
  - You are about to alter the column `revokeCount` on the `Revocation` table. The data in that column could be lost. The data in that column will be cast from `Int` to `BigInt`.
  - You are about to alter the column `slot` on the `Revocation` table. The data in that column could be lost. The data in that column will be cast from `Int` to `BigInt`.
  - Made the column `id` on table `Revocation` required. This step will fail if there are existing NULL values in that column.

*/
-- AlterTable
ALTER TABLE "Feedback" ADD COLUMN "txIndex" INTEGER;

-- AlterTable
ALTER TABLE "FeedbackResponse" ADD COLUMN "responseCount" BIGINT;
ALTER TABLE "FeedbackResponse" ADD COLUMN "txIndex" INTEGER;

-- RedefineTables
PRAGMA defer_foreign_keys=ON;
PRAGMA foreign_keys=OFF;
CREATE TABLE "new_Revocation" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "agentId" TEXT NOT NULL,
    "client" TEXT NOT NULL,
    "feedbackIndex" BIGINT NOT NULL,
    "feedbackHash" BLOB,
    "slot" BIGINT NOT NULL,
    "originalScore" INTEGER,
    "atomEnabled" BOOLEAN NOT NULL DEFAULT false,
    "hadImpact" BOOLEAN NOT NULL DEFAULT false,
    "runningDigest" BLOB,
    "revokeCount" BIGINT NOT NULL,
    "txSignature" TEXT,
    "txIndex" INTEGER,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "verifiedAt" DATETIME
);
INSERT INTO "new_Revocation" ("agentId", "atomEnabled", "client", "createdAt", "feedbackHash", "feedbackIndex", "hadImpact", "id", "originalScore", "revokeCount", "runningDigest", "slot", "status", "txSignature", "verifiedAt") SELECT "agentId", coalesce("atomEnabled", false) AS "atomEnabled", "client", coalesce("createdAt", CURRENT_TIMESTAMP) AS "createdAt", "feedbackHash", "feedbackIndex", coalesce("hadImpact", false) AS "hadImpact", "id", "originalScore", "revokeCount", "runningDigest", "slot", coalesce("status", 'PENDING') AS "status", "txSignature", "verifiedAt" FROM "Revocation";
DROP TABLE "Revocation";
ALTER TABLE "new_Revocation" RENAME TO "Revocation";
CREATE INDEX "Revocation_agentId_idx" ON "Revocation"("agentId");
CREATE INDEX "Revocation_status_idx" ON "Revocation"("status");
CREATE UNIQUE INDEX "Revocation_agentId_client_feedbackIndex_key" ON "Revocation"("agentId", "client", "feedbackIndex");
PRAGMA foreign_keys=ON;
PRAGMA defer_foreign_keys=OFF;

-- CreateIndex
CREATE INDEX "FeedbackResponse_feedbackId_responseCount_idx" ON "FeedbackResponse"("feedbackId", "responseCount");
