ALTER TABLE Feedback ADD COLUMN runningDigest BLOB;
ALTER TABLE FeedbackResponse ADD COLUMN runningDigest BLOB;

CREATE TABLE Revocation (
  id TEXT PRIMARY KEY,
  agentId TEXT NOT NULL,
  client TEXT NOT NULL,
  feedbackIndex INTEGER NOT NULL,
  feedbackHash BLOB,
  slot INTEGER NOT NULL,
  originalScore INTEGER,
  atomEnabled INTEGER DEFAULT 0,
  hadImpact INTEGER DEFAULT 0,
  runningDigest BLOB,
  revokeCount INTEGER NOT NULL,
  txSignature TEXT,
  createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
  status TEXT DEFAULT 'PENDING',
  verifiedAt DATETIME,
  FOREIGN KEY (agentId) REFERENCES Agent(id) ON DELETE CASCADE,
  UNIQUE(agentId, client, feedbackIndex)
);

CREATE INDEX idx_revocation_agent ON Revocation(agentId);
CREATE INDEX idx_revocation_status ON Revocation(status);
