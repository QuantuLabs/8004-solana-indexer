import { execFileSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

const repoRoot = resolve(import.meta.dirname, "../../..");
const bundleSql = readFileSync(
  resolve(repoRoot, "prisma/legacy-upgrades/v1.7.7-dbpush-to-current.sql"),
  "utf8",
);
const tempDirs: string[] = [];

afterEach(() => {
  for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

describe("legacy SQLite v1.7.7 db-push upgrade bundle", () => {
  it("executes once on the real v1.7.7 schema while preserving rows, IDs, and cursor", () => {
    const dir = mkdtempSync(resolve(tmpdir(), "indexer-v177-upgrade-"));
    tempDirs.push(dir);
    const schemaPath = resolve(dir, "schema.prisma");
    const dbPath = resolve(dir, "legacy.db");
    writeFileSync(
      schemaPath,
      execFileSync("git", ["show", "v1.7.7:prisma/schema.prisma"], {
        cwd: repoRoot,
        encoding: "utf8",
      }),
    );
    execFileSync(resolve(repoRoot, "node_modules/.bin/prisma"), [
      "db", "push", "--schema", schemaPath, "--skip-generate",
    ], {
      cwd: repoRoot,
      env: { ...process.env, DATABASE_URL: `file:${dbPath}` },
      stdio: "pipe",
    });

    const db = new DatabaseSync(dbPath);
    db.exec(`
      INSERT INTO "Agent" (
        id, owner, uri, nftName, collection, registry, createdTxSignature,
        createdSlot, txIndex, eventOrdinal, agent_id, createdAt, updatedAt
      ) VALUES ('asset-1', 'owner-1', '', 'Agent', 'collection-1', 'registry-1',
        'sig-agent', 12, 3, 4, 77, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
      INSERT INTO "AgentMetadata" (id, agentId, key, value, txSignature, slot, txIndex, eventOrdinal)
      VALUES ('metadata-1', 'asset-1', 'name', X'01', 'sig-metadata', 13, 5, 6);
      INSERT INTO "Feedback" (id, feedback_id, agentId, client, feedbackIndex, tag1, tag2, endpoint, feedbackUri)
      VALUES ('feedback-1', 88, 'asset-1', 'client-1', 1, '', '', '', '');
      INSERT INTO "FeedbackResponse" (id, response_id, feedbackId, responder, responseUri, txSignature, eventOrdinal)
      VALUES ('response-1', 99, 'feedback-1', 'responder-1', '', 'sig-response', 7);
      INSERT INTO "IndexerState" (id, lastSignature, lastSlot, source, updatedAt)
      VALUES ('main', 'cursor-signature', 123, 'poller', CURRENT_TIMESTAMP);
    `);
    const before = db.prepare(`
      SELECT a.agent_id, a.uri, f.feedback_id, f."feedbackUri", r.response_id,
             r."responseUri", s."lastSignature", s."lastSlot", s.source
      FROM "Agent" a, "Feedback" f, "FeedbackResponse" r, "IndexerState" s
      WHERE a.id = 'asset-1' AND f.id = 'feedback-1' AND r.id = 'response-1' AND s.id = 'main'
    `).get();

    db.exec(bundleSql);

    expect(db.prepare(`
      SELECT a.agent_id, a.uri, f.feedback_id, f."feedbackUri", r.response_id,
             r."responseUri", s."lastSignature", s."lastSlot", s.source
      FROM "Agent" a, "Feedback" f, "FeedbackResponse" r, "IndexerState" s
      WHERE a.id = 'asset-1' AND f.id = 'feedback-1' AND r.id = 'response-1' AND s.id = 'main'
    `).get()).toEqual(before);
    expect(db.prepare(`SELECT mutation_order_key FROM "Agent" WHERE id = 'asset-1'`).get())
      .toEqual({ mutation_order_key: "00000000000000000012:0000000003:0000000004:sig-agent" });
    expect(db.prepare(`SELECT mutation_order_key FROM "AgentMetadata" WHERE id = 'metadata-1'`).get())
      .toEqual({ mutation_order_key: "00000000000000000013:0000000005:0000000006:sig-metadata" });
    expect(db.prepare(`SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE '%eventOrdinal_key' ORDER BY name`).all())
      .toEqual([
        { name: "FeedbackResponse_feedbackId_responder_txSignature_eventOrdinal_key" },
        { name: "OrphanResponse_agentId_client_feedbackIndex_responder_txSignature_eventOrdinal_key" },
      ]);
    expect(() => db.exec(bundleSql)).toThrow(/duplicate column name: collection_id/);
    db.close();
  });

  it("does not contain the deferred historical payload rewrite", () => {
    expect(bundleSql).not.toContain('SET "requestUri" = NULL');
    expect(bundleSql).not.toContain('SET "responseUri" = NULL');
    expect(bundleSql).not.toContain('SET "tag" = NULL');
  });
});
