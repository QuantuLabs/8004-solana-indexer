-- CreateTable
CREATE TABLE "HashChainCheckpoint" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "agentId" TEXT NOT NULL,
    "chainType" TEXT NOT NULL,
    "eventCount" INTEGER NOT NULL,
    "digest" TEXT NOT NULL,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateIndex
CREATE INDEX "HashChainCheckpoint_agentId_chainType_idx" ON "HashChainCheckpoint"("agentId", "chainType");

-- CreateIndex
CREATE UNIQUE INDEX "HashChainCheckpoint_agentId_chainType_eventCount_key" ON "HashChainCheckpoint"("agentId", "chainType", "eventCount");
