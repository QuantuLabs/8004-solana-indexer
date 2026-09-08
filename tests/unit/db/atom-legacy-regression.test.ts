import { execFileSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { PrismaClient } from "@prisma/client";
import { Keypair, PublicKey } from "@solana/web3.js";
import { afterEach, describe, expect, it } from "vitest";
import { handleEventAtomic, type EventContext } from "../../../src/db/handlers.js";
import type { ProgramEvent } from "../../../src/parser/types.js";

const repoRoot = resolve(import.meta.dirname, "../../..");
const node = process.execPath;
const bundleSql = readFileSync(resolve(repoRoot, "prisma/legacy-upgrades/v1.7.7-dbpush-to-current.sql"), "utf8");
const tempDirs: string[] = [];

afterEach(() => {
  for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function ctx(slot: bigint, signature: string): EventContext {
  return { slot, signature, txIndex: 0, eventOrdinal: 0, blockTime: new Date(Number(slot) * 1000) };
}

function feedbackEvent(asset: string, client: string, index: bigint, risk: number, diversity: number): ProgramEvent {
  return {
    type: "NewFeedback",
    data: {
      asset: new PublicKey(asset),
      clientAddress: new PublicKey(client),
      feedbackIndex: index,
      value: 1n,
      valueDecimals: 0,
      score: 80,
      tag1: "",
      tag2: "",
      endpoint: "",
      feedbackUri: "",
      feedbackFileHash: null,
      sealHash: new Uint8Array(32),
      atomEnabled: true,
      newTrustTier: 4,
      newQualityScore: 4,
      newConfidence: 4,
      newRiskScore: risk,
      newDiversityRatio: diversity,
      isUniqueClient: true,
      newFeedbackDigest: new Uint8Array(32),
      newFeedbackCount: 2n,
      slot: 110n,
    },
  };
}

function revokeEvent(asset: string, client: string): ProgramEvent {
  return {
    type: "FeedbackRevoked",
    data: {
      asset: new PublicKey(asset),
      clientAddress: new PublicKey(client),
      feedbackIndex: 1n,
      sealHash: new Uint8Array(32),
      slot: 120n,
      originalScore: 10,
      atomEnabled: true,
      hadImpact: true,
      newTrustTier: 3,
      newQualityScore: 9,
      newConfidence: 9,
      newRevokeDigest: new Uint8Array(32),
      newRevokeCount: 1n,
    },
  };
}

describe("real migrated SQLite ATOM and legacy response regressions", () => {
  it("converges partial ATOM snapshots and promotes a NULL-ordinal v1.7.7 orphan once", async () => {
    const dir = mkdtempSync(resolve(tmpdir(), "atom-legacy-regression-"));
    tempDirs.push(dir);
    const dbPath = resolve(dir, "legacy.db");
    const schemaPath = resolve(dir, "schema.prisma");
    writeFileSync(schemaPath, execFileSync("git", ["show", "v1.7.7:prisma/schema.prisma"], { cwd: repoRoot, encoding: "utf8" }));
    execFileSync(node, [resolve(repoRoot, "node_modules/prisma/build/index.js"), "db", "push", "--schema", schemaPath, "--skip-generate"], {
      cwd: repoRoot,
      env: { ...process.env, DATABASE_URL: `file:${dbPath}` },
      stdio: "pipe",
    });

    const orphanAsset = Keypair.generate().publicKey.toBase58();
    const orderedAsset = Keypair.generate().publicKey.toBase58();
    const delayedAsset = Keypair.generate().publicKey.toBase58();
    const client = Keypair.generate().publicKey.toBase58();
    const responder = Keypair.generate().publicKey.toBase58();
    const db = new DatabaseSync(dbPath);
    const insertAgent = db.prepare(`INSERT INTO "Agent" (id, owner, uri, nftName, collection, registry, atomEnabled, trustTier, qualityScore, confidence, riskScore, diversityRatio, createdTxSignature, createdSlot, txIndex, eventOrdinal, agent_id, createdAt, updatedAt) VALUES (?, 'owner', '', '', 'collection', 'registry', 1, 1, 1, 1, 10, 100, 'create', 50, 0, 0, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`);
    insertAgent.run(orphanAsset, 77);
    insertAgent.run(orderedAsset, 78);
    insertAgent.run(delayedAsset, 79);
    db.prepare(`INSERT INTO "OrphanResponse" (id, agentId, client, feedbackIndex, responder, responseUri, responseCount, txSignature, slot) VALUES ('legacy-orphan-id', ?, ?, 9, ?, 'legacy://response', 1, 'legacy-response-signature', 90)`).run(orphanAsset, client, responder);
    const insertA = db.prepare(`INSERT INTO "Feedback" (id, feedback_id, agentId, client, feedbackIndex, value, valueDecimals, score, tag1, tag2, endpoint, feedbackUri, createdTxSignature, createdSlot, txIndex, eventOrdinal) VALUES (?, 1, ?, ?, 1, '1', 0, 10, '', '', '', '', 'feedback-a', 105, 0, 0)`);
    insertA.run("ordered-a", orderedAsset, client);
    insertA.run("delayed-a", delayedAsset, client);
    db.exec(`INSERT INTO "IndexerState" (id, lastSignature, lastSlot, source, updatedAt) VALUES ('main', 'cutover', 100, 'poller', CURRENT_TIMESTAMP)`);
    db.exec(bundleSql);
    db.close();

    const prisma = new PrismaClient({ datasources: { db: { url: `file:${dbPath}` } } });
    try {
      const orphanFeedback = feedbackEvent(orphanAsset, client, 9n, 40, 200);
      await handleEventAtomic(prisma, orphanFeedback, ctx(110n, "orphan-parent"));
      const promoted = await prisma.feedbackResponse.findFirst({ where: { responder, eventOrdinal: null } });
      expect(promoted).toMatchObject({ id: "legacy-orphan-id", responseId: 1n, eventOrdinal: null });
      const cursorSelect = { lastSignature: true, lastSlot: true, lastTxIndex: true } as const;
      const cursorAfterPromotion = await prisma.indexerState.findUnique({ where: { id: "main" }, select: cursorSelect });
      await handleEventAtomic(prisma, orphanFeedback, ctx(110n, "orphan-parent"));
      expect(await prisma.feedbackResponse.findMany({ where: { responder } })).toEqual([promoted]);
      expect(await prisma.indexerState.findUnique({ where: { id: "main" }, select: cursorSelect })).toEqual(cursorAfterPromotion);

      await handleEventAtomic(prisma, feedbackEvent(orderedAsset, client, 2n, 40, 200), ctx(110n, "ordered-b"));
      await handleEventAtomic(prisma, revokeEvent(orderedAsset, client), ctx(120n, "ordered-revoke"));
      await handleEventAtomic(prisma, revokeEvent(delayedAsset, client), ctx(120n, "delayed-revoke"));
      await handleEventAtomic(prisma, feedbackEvent(delayedAsset, client, 2n, 40, 200), ctx(110n, "delayed-b"));

      const select = { trustTier: true, qualityScore: true, confidence: true, riskScore: true, diversityRatio: true, feedbackCount: true, rawAvgScore: true } as const;
      const ordered = await prisma.agent.findUniqueOrThrow({ where: { id: orderedAsset }, select });
      const delayed = await prisma.agent.findUniqueOrThrow({ where: { id: delayedAsset }, select });
      expect(delayed).toEqual(ordered);
      expect(delayed).toEqual({ trustTier: 3, qualityScore: 9, confidence: 9, riskScore: 40, diversityRatio: 200, feedbackCount: 1, rawAvgScore: 80 });
    } finally {
      await prisma.$disconnect();
    }
  }, 180_000);
});
