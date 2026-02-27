import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AddressInfo } from "net";
import type { Server } from "http";
import type { Express } from "express";

const originalEnv = process.env;

const ACTIVE_AGENT = {
  asset: "AssetActive111111111111111111111111111111111",
  owner: "OwnerActive111111111111111111111111111111111",
  creator: "CreatorActive111111111111111111111111111111",
  agentUri: "https://example.com/active.json",
  agentWallet: "WalletActive11111111111111111111111111111",
  collectionPointer: "c1:active-pointer",
  colLocked: true,
  parentAsset: "ParentAsset1111111111111111111111111111111",
  parentCreator: "ParentCreator11111111111111111111111111111",
  parentLocked: false,
  status: "PENDING",
};

const ACTIVE_FEEDBACK = {
  id: "fb-row-1",
  feedbackId: 7n,
  asset: ACTIVE_AGENT.asset,
  clientAddress: "ClientActive11111111111111111111111111111",
  feedbackIndex: 7n,
  value: "1234",
  valueDecimals: 0,
  score: 88,
  tag1: "quality",
  tag2: "speed",
  endpoint: "https://api.example.com/feedback",
  feedbackUri: "https://example.com/feedback/1.json",
  feedbackHashHex: "aabbccdd",
  runningDigestHex: "0011223344",
  isRevoked: false,
  status: "FINALIZED",
  createdSlot: 222n,
  createdTxSignature: "sig-feedback-1",
  createdAt: new Date("2024-01-03T00:00:00.000Z"),
};

const ACTIVE_RESPONSE = {
  id: "resp-row-1",
  responseId: 1n,
  responder: "ResponderActive111111111111111111111111111",
  responseUri: "https://example.com/response/1.json",
  responseHashHex: "ddeeff00",
  runningDigestHex: "55667788",
  responseCount: 1n,
  status: "FINALIZED",
  slot: 333n,
  txSignature: "sig-response-1",
  createdAt: new Date("2024-01-04T00:00:00.000Z"),
};

const ACTIVE_STATS = {
  totalAgents: 1,
  totalFeedback: 1,
  totalValidations: 2,
};

function toUnixSeconds(value: string | Date): string {
  const ms = value instanceof Date ? value.getTime() : new Date(value).getTime();
  return String(Math.floor(ms / 1000));
}

function makePrismaStub() {
  return {
    agent: {
      findMany: vi.fn().mockResolvedValue([
        {
          id: ACTIVE_AGENT.asset,
          owner: ACTIVE_AGENT.owner,
          creator: ACTIVE_AGENT.creator,
          uri: ACTIVE_AGENT.agentUri,
          wallet: ACTIVE_AGENT.agentWallet,
          collection: "Collection111111111111111111111111111111",
          collectionPointer: ACTIVE_AGENT.collectionPointer,
          colLocked: ACTIVE_AGENT.colLocked,
          parentAsset: ACTIVE_AGENT.parentAsset,
          parentCreator: ACTIVE_AGENT.parentCreator,
          parentLocked: ACTIVE_AGENT.parentLocked,
          nftName: "Active",
          atomEnabled: true,
          trustTier: 0,
          qualityScore: 0,
          confidence: 0,
          riskScore: 0,
          diversityRatio: 0,
          feedbackCount: 0,
          rawAvgScore: 0,
          agentId: 1n,
          status: ACTIVE_AGENT.status,
          verifiedAt: null,
          createdAt: new Date("2024-01-01T00:00:00.000Z"),
          updatedAt: new Date("2024-01-02T00:00:00.000Z"),
        },
      ]),
      count: vi.fn().mockResolvedValue(1),
    },
    feedback: {
      findMany: vi.fn().mockResolvedValue([
        {
          id: ACTIVE_FEEDBACK.id,
          feedbackId: ACTIVE_FEEDBACK.feedbackId,
          agentId: ACTIVE_FEEDBACK.asset,
          client: ACTIVE_FEEDBACK.clientAddress,
          feedbackIndex: ACTIVE_FEEDBACK.feedbackIndex,
          value: ACTIVE_FEEDBACK.value,
          valueDecimals: ACTIVE_FEEDBACK.valueDecimals,
          score: ACTIVE_FEEDBACK.score,
          tag1: ACTIVE_FEEDBACK.tag1,
          tag2: ACTIVE_FEEDBACK.tag2,
          endpoint: ACTIVE_FEEDBACK.endpoint,
          feedbackUri: ACTIVE_FEEDBACK.feedbackUri,
          feedbackHash: Buffer.from(ACTIVE_FEEDBACK.feedbackHashHex, "hex"),
          runningDigest: Buffer.from(ACTIVE_FEEDBACK.runningDigestHex, "hex"),
          revoked: ACTIVE_FEEDBACK.isRevoked,
          status: ACTIVE_FEEDBACK.status,
          verifiedAt: null,
          createdSlot: ACTIVE_FEEDBACK.createdSlot,
          createdTxSignature: ACTIVE_FEEDBACK.createdTxSignature,
          createdAt: ACTIVE_FEEDBACK.createdAt,
        },
      ]),
      count: vi.fn().mockResolvedValue(ACTIVE_STATS.totalFeedback),
    },
    feedbackResponse: {
      findMany: vi.fn().mockResolvedValue([
        {
          id: ACTIVE_RESPONSE.id,
          responseId: ACTIVE_RESPONSE.responseId,
          feedbackId: ACTIVE_FEEDBACK.id,
          responder: ACTIVE_RESPONSE.responder,
          responseUri: ACTIVE_RESPONSE.responseUri,
          responseHash: Buffer.from(ACTIVE_RESPONSE.responseHashHex, "hex"),
          runningDigest: Buffer.from(ACTIVE_RESPONSE.runningDigestHex, "hex"),
          responseCount: ACTIVE_RESPONSE.responseCount,
          status: ACTIVE_RESPONSE.status,
          verifiedAt: null,
          slot: ACTIVE_RESPONSE.slot,
          txSignature: ACTIVE_RESPONSE.txSignature,
          createdAt: ACTIVE_RESPONSE.createdAt,
          feedback: {
            feedbackId: ACTIVE_FEEDBACK.feedbackId,
            agentId: ACTIVE_FEEDBACK.asset,
            client: ACTIVE_FEEDBACK.clientAddress,
            feedbackIndex: ACTIVE_FEEDBACK.feedbackIndex,
            createdSlot: ACTIVE_FEEDBACK.createdSlot,
            createdTxSignature: ACTIVE_FEEDBACK.createdTxSignature,
          },
        },
      ]),
      count: vi.fn().mockResolvedValue(1),
    },
    registry: {
      count: vi.fn().mockResolvedValue(1),
    },
    validation: {
      count: vi.fn().mockResolvedValue(ACTIVE_STATS.totalValidations),
    },
  };
}

function makePoolStub() {
  return {
    query: vi.fn().mockImplementation((sql: string) => {
      if (sql.includes("FROM agents a")) {
        return {
          rows: [
            {
              asset: ACTIVE_AGENT.asset,
              owner: ACTIVE_AGENT.owner,
              creator: ACTIVE_AGENT.creator,
              agent_uri: ACTIVE_AGENT.agentUri,
              agent_wallet: ACTIVE_AGENT.agentWallet,
              collection_pointer: ACTIVE_AGENT.collectionPointer,
              col_locked: ACTIVE_AGENT.colLocked,
              parent_asset: ACTIVE_AGENT.parentAsset,
              parent_creator: ACTIVE_AGENT.parentCreator,
              parent_locked: ACTIVE_AGENT.parentLocked,
              status: ACTIVE_AGENT.status,
            },
          ],
        };
      }
      if (
        sql.includes("FROM feedbacks") &&
        sql.includes("SELECT id, feedback_id::text AS feedback_id, asset, client_address, feedback_index")
      ) {
        return {
          rows: [
            {
              id: ACTIVE_FEEDBACK.id,
              feedback_id: ACTIVE_FEEDBACK.feedbackId.toString(),
              asset: ACTIVE_FEEDBACK.asset,
              client_address: ACTIVE_FEEDBACK.clientAddress,
              feedback_index: ACTIVE_FEEDBACK.feedbackIndex.toString(),
              value: ACTIVE_FEEDBACK.value,
              value_decimals: ACTIVE_FEEDBACK.valueDecimals,
              score: ACTIVE_FEEDBACK.score,
              tag1: ACTIVE_FEEDBACK.tag1,
              tag2: ACTIVE_FEEDBACK.tag2,
              endpoint: ACTIVE_FEEDBACK.endpoint,
              feedback_uri: ACTIVE_FEEDBACK.feedbackUri,
              feedback_hash: ACTIVE_FEEDBACK.feedbackHashHex,
              running_digest: ACTIVE_FEEDBACK.runningDigestHex,
              is_revoked: ACTIVE_FEEDBACK.isRevoked,
              status: ACTIVE_FEEDBACK.status,
              verified_at: null,
              tx_signature: ACTIVE_FEEDBACK.createdTxSignature,
              block_slot: ACTIVE_FEEDBACK.createdSlot.toString(),
              created_at: ACTIVE_FEEDBACK.createdAt.toISOString(),
              revoked_at: null,
            },
          ],
        };
      }
      if (
        sql.includes("FROM feedback_responses") &&
        sql.includes("SELECT id, response_id::text AS response_id, asset, client_address, feedback_index, responder")
      ) {
        return {
          rows: [
            {
              id: ACTIVE_RESPONSE.id,
              response_id: ACTIVE_RESPONSE.responseId.toString(),
              asset: ACTIVE_FEEDBACK.asset,
              client_address: ACTIVE_FEEDBACK.clientAddress,
              feedback_index: ACTIVE_FEEDBACK.feedbackIndex.toString(),
              responder: ACTIVE_RESPONSE.responder,
              response_uri: ACTIVE_RESPONSE.responseUri,
              response_hash: ACTIVE_RESPONSE.responseHashHex,
              running_digest: ACTIVE_RESPONSE.runningDigestHex,
              response_count: ACTIVE_RESPONSE.responseCount.toString(),
              status: ACTIVE_RESPONSE.status,
              verified_at: null,
              tx_signature: ACTIVE_RESPONSE.txSignature,
              block_slot: ACTIVE_RESPONSE.slot.toString(),
              created_at: ACTIVE_RESPONSE.createdAt.toISOString(),
            },
          ],
        };
      }
      if (sql.includes("total_agents") && sql.includes("total_feedback") && sql.includes("total_validations")) {
        return {
          rows: [
            {
              total_agents: String(ACTIVE_STATS.totalAgents),
              total_feedback: String(ACTIVE_STATS.totalFeedback),
              total_validations: String(ACTIVE_STATS.totalValidations),
              tags: [ACTIVE_FEEDBACK.tag1, ACTIVE_FEEDBACK.tag2],
            },
          ],
        };
      }
      return { rows: [] };
    }),
  };
}

async function startServer(options: { prisma: any; pool: any }) {
  const { createApiServer } = await import("../../src/api/server.js");
  const app: Express = createApiServer(options);

  const server = await new Promise<Server>((resolve, reject) => {
    const started = app.listen(0, "127.0.0.1", () => resolve(started));
    started.on("error", reject);
  });

  const addr = server.address() as AddressInfo;
  return {
    server,
    baseUrl: `http://127.0.0.1:${addr.port}`,
  };
}

async function stopServer(server: Server) {
  await new Promise<void>((resolve, reject) => {
    server.close((err) => (err ? reject(err) : resolve()));
  });
}

async function waitForGraphqlMount(baseUrl: string): Promise<void> {
  for (let i = 0; i < 20; i += 1) {
    const res = await fetch(`${baseUrl}/v2/graphql`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ query: "{ __typename }" }),
    });
    if (res.status === 200) return;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error("GraphQL endpoint was not mounted in time");
}

describe("REST/GraphQL parity in API_MODE=both", () => {
  beforeEach(() => {
    vi.resetModules();
    process.env = {
      ...originalEnv,
      API_MODE: "both",
      ENABLE_GRAPHQL: "true",
    };
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  it("returns parity for agents, feedbacks, responses, and stats with default orphan exclusion", async () => {
    const prisma = makePrismaStub();
    const pool = makePoolStub();
    const { server, baseUrl } = await startServer({
      prisma: prisma as any,
      pool: pool as any,
    });

    try {
      const restAgentsRes = await fetch(`${baseUrl}/rest/v1/agents`);
      expect(restAgentsRes.status).toBe(200);
      const restAgentsBody = await restAgentsRes.json();
      expect((restAgentsBody as Array<Record<string, unknown>>)[0]?.agent_id).toBe("1");

      const restAgentsByIdRes = await fetch(`${baseUrl}/rest/v1/agents?agent_id=1`);
      expect(restAgentsByIdRes.status).toBe(200);

      const restAgentsByIdInvalidRes = await fetch(`${baseUrl}/rest/v1/agents?agent_id=not-a-number`);
      expect(restAgentsByIdInvalidRes.status).toBe(400);

      const restFeedbacksRes = await fetch(`${baseUrl}/rest/v1/feedbacks?limit=10`);
      expect(restFeedbacksRes.status).toBe(200);
      const restFeedbacksBody = await restFeedbacksRes.json();
      const restFeedbackRow = (restFeedbacksBody as Array<Record<string, unknown>>)[0] ?? {};
      expect(restFeedbackRow.id).toBe(ACTIVE_FEEDBACK.feedbackId.toString());
      expect("row_id" in restFeedbackRow).toBe(false);

      const restFeedbackByIdRes = await fetch(
        `${baseUrl}/rest/v1/feedbacks?asset=${encodeURIComponent(ACTIVE_FEEDBACK.asset)}&feedback_id=${encodeURIComponent(ACTIVE_FEEDBACK.feedbackId.toString())}`
      );
      expect(restFeedbackByIdRes.status).toBe(200);
      const restFeedbackByIdBody = await restFeedbackByIdRes.json();
      expect((restFeedbackByIdBody as Array<Record<string, unknown>>)[0]?.feedback_id).toBe(
        ACTIVE_FEEDBACK.feedbackId.toString()
      );

      const restFeedbackByIdInvalidRes = await fetch(
        `${baseUrl}/rest/v1/feedbacks?feedback_id=${encodeURIComponent("not-a-number")}`
      );
      expect(restFeedbackByIdInvalidRes.status).toBe(400);

      const restResponsesRes = await fetch(`${baseUrl}/rest/v1/feedback_responses?limit=10`);
      expect(restResponsesRes.status).toBe(200);
      const restResponsesBody = await restResponsesRes.json();
      const restResponseRow = (restResponsesBody as Array<Record<string, unknown>>)[0] ?? {};
      expect(restResponseRow.id).toBe(ACTIVE_RESPONSE.responseId.toString());
      expect(restResponseRow.feedback_id).toBe(ACTIVE_FEEDBACK.feedbackId.toString());
      expect("row_id" in restResponseRow).toBe(false);
      expect("feedback_row_id" in restResponseRow).toBe(false);

      const restResponsesByCanonicalFeedbackIdRes = await fetch(
        `${baseUrl}/rest/v1/feedback_responses?feedback_id=${encodeURIComponent(`${ACTIVE_FEEDBACK.asset}:${ACTIVE_FEEDBACK.clientAddress}:${ACTIVE_FEEDBACK.feedbackIndex.toString()}`)}`
      );
      expect(restResponsesByCanonicalFeedbackIdRes.status).toBe(400);

      const restResponsesByScopedFeedbackIdWithoutAssetRes = await fetch(
        `${baseUrl}/rest/v1/feedback_responses?feedback_id=${encodeURIComponent(ACTIVE_FEEDBACK.feedbackId.toString())}`
      );
      expect(restResponsesByScopedFeedbackIdWithoutAssetRes.status).toBe(400);

      const restResponsesByFeedbackIdRes = await fetch(
        `${baseUrl}/rest/v1/feedback_responses?asset=${encodeURIComponent(ACTIVE_FEEDBACK.asset)}&feedback_id=${encodeURIComponent(ACTIVE_FEEDBACK.feedbackId.toString())}`
      );
      expect(restResponsesByFeedbackIdRes.status).toBe(200);
      const restResponsesByFeedbackIdBody = await restResponsesByFeedbackIdRes.json();
      expect((restResponsesByFeedbackIdBody as Array<Record<string, unknown>>)[0]?.feedback_id).toBe(
        ACTIVE_FEEDBACK.feedbackId.toString()
      );

      const restResponsesByFeedbackIdInvalidRes = await fetch(
        `${baseUrl}/rest/v1/feedback_responses?feedback_id=${encodeURIComponent(`${ACTIVE_FEEDBACK.asset}:${ACTIVE_FEEDBACK.clientAddress}:not-a-number`)}`
      );
      expect(restResponsesByFeedbackIdInvalidRes.status).toBe(400);

      const restStatsRes = await fetch(`${baseUrl}/rest/v1/global_stats`);
      expect(restStatsRes.status).toBe(200);
      const restStatsBody = await restStatsRes.json();

      await waitForGraphqlMount(baseUrl);
      const gqlRes = await fetch(`${baseUrl}/v2/graphql`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          query: `
            query {
              agents(first: 10) {
                owner
                creator
                agentURI
                agentWallet
                collectionPointer
                colLocked
                parentAsset
                parentCreator
                parentLocked
                solana {
                  assetPubkey
                  verificationStatus
                }
              }
              feedbacks(first: 10) {
                id
                clientAddress
                feedbackIndex
                value
                tag1
                tag2
                endpoint
                feedbackURI
                feedbackHash
                isRevoked
                createdAt
                solana {
                  verificationStatus
                  txSignature
                  blockSlot
                }
              }
              feedbackResponses(first: 10) {
                id
                responder
                responseUri
                responseHash
                createdAt
                solana {
                  verificationStatus
                  txSignature
                  blockSlot
                  responseCount
                }
              }
              globalStats(id: "stats") {
                totalAgents
                totalFeedback
                totalValidations
              }
            }
          `,
        }),
      });
      expect(gqlRes.status).toBe(200);
      const gqlBody = await gqlRes.json();
      expect(gqlBody.errors).toBeUndefined();

      const restAgentsCanonical = (restAgentsBody as Array<Record<string, unknown>>).map((row) => ({
        asset: row.asset,
        owner: row.owner,
        creator: row.creator,
        agentURI: row.agent_uri,
        agentWallet: row.agent_wallet,
        collectionPointer: row.collection_pointer,
        colLocked: row.col_locked,
        parentAsset: row.parent_asset,
        parentCreator: row.parent_creator,
        parentLocked: row.parent_locked,
        status: row.status,
      }));

      const gqlAgentsCanonical = ((gqlBody?.data?.agents ?? []) as Array<Record<string, any>>).map((row) => ({
        asset: row.solana?.assetPubkey ?? null,
        owner: row.owner,
        creator: row.creator,
        agentURI: row.agentURI,
        agentWallet: row.agentWallet,
        collectionPointer: row.collectionPointer,
        colLocked: row.colLocked,
        parentAsset: row.parentAsset,
        parentCreator: row.parentCreator,
        parentLocked: row.parentLocked,
        status: row.solana?.verificationStatus ?? null,
      }));

      const restFeedbacksCanonical = (restFeedbacksBody as Array<Record<string, any>>).map((row) => ({
        id: row.id,
        clientAddress: row.client_address,
        feedbackIndex: row.feedback_index,
        value: row.value,
        tag1: row.tag1,
        tag2: row.tag2,
        endpoint: row.endpoint,
        feedbackURI: row.feedback_uri,
        feedbackHash: row.feedback_hash,
        isRevoked: row.is_revoked,
        createdAt: toUnixSeconds(row.created_at),
        status: row.status,
        txSignature: row.tx_signature,
        blockSlot: String(row.block_slot),
      }));

      const gqlFeedbacksCanonical = ((gqlBody?.data?.feedbacks ?? []) as Array<Record<string, any>>).map((row) => {
        return {
          id: row.id,
          clientAddress: row.clientAddress,
          feedbackIndex: row.feedbackIndex,
          value: row.value,
          tag1: row.tag1,
          tag2: row.tag2,
          endpoint: row.endpoint,
          feedbackURI: row.feedbackURI,
          feedbackHash: row.feedbackHash,
          isRevoked: row.isRevoked,
          createdAt: row.createdAt,
          status: row.solana?.verificationStatus ?? null,
          txSignature: row.solana?.txSignature ?? null,
          blockSlot: row.solana?.blockSlot ? String(row.solana.blockSlot) : null,
        };
      });

      const restResponsesCanonical = (restResponsesBody as Array<Record<string, any>>).map((row) => ({
        id: row.id,
        responder: row.responder,
        responseUri: row.response_uri,
        responseHash: row.response_hash,
        createdAt: toUnixSeconds(row.created_at),
        status: row.status,
        txSignature: row.tx_signature,
        blockSlot: String(row.block_slot),
        responseCount: row.response_count ?? null,
      }));

      const gqlResponsesCanonical = ((gqlBody?.data?.feedbackResponses ?? []) as Array<Record<string, any>>).map((row) => {
        return {
          id: row.id,
          responder: row.responder,
          responseUri: row.responseUri,
          responseHash: row.responseHash,
          createdAt: row.createdAt,
          status: row.solana?.verificationStatus ?? null,
          txSignature: row.solana?.txSignature ?? null,
          blockSlot: row.solana?.blockSlot ? String(row.solana.blockSlot) : null,
          responseCount: row.solana?.responseCount ?? null,
        };
      });

      const restStatsCanonical = {
        totalAgents: String(restStatsBody?.[0]?.total_agents ?? 0),
        totalFeedback: String(restStatsBody?.[0]?.total_feedbacks ?? 0),
        totalValidations: String(restStatsBody?.[0]?.total_validations ?? 0),
      };

      const gqlStatsCanonical = {
        totalAgents: String(gqlBody?.data?.globalStats?.totalAgents ?? 0),
        totalFeedback: String(gqlBody?.data?.globalStats?.totalFeedback ?? 0),
        totalValidations: String(gqlBody?.data?.globalStats?.totalValidations ?? 0),
      };

      expect(gqlAgentsCanonical).toEqual(restAgentsCanonical);
      expect(gqlFeedbacksCanonical).toEqual(restFeedbacksCanonical);
      expect(gqlResponsesCanonical).toEqual(restResponsesCanonical);
      expect(gqlStatsCanonical).toEqual(restStatsCanonical);

      expect(prisma.agent.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            status: { not: "ORPHANED" },
          }),
        })
      );
      expect(prisma.feedback.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            status: { not: "ORPHANED" },
          }),
          orderBy: [
            { createdAt: "desc" },
            { agentId: "asc" },
            { client: "asc" },
            { feedbackIndex: "asc" },
            { id: "asc" },
          ],
        })
      );
      expect(prisma.feedbackResponse.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            status: { not: "ORPHANED" },
          }),
        })
      );

      const gqlAgentsCall = (pool.query as any).mock.calls.find(
        (call: unknown[]) => typeof call[0] === "string" && (call[0] as string).includes("FROM agents a")
      );
      expect(gqlAgentsCall).toBeDefined();
      expect(gqlAgentsCall[0]).toContain("status != 'ORPHANED'");

      const gqlFeedbacksCall = (pool.query as any).mock.calls.find(
        (call: unknown[]) =>
          typeof call[0] === "string" &&
          (call[0] as string).includes("FROM feedbacks") &&
          (call[0] as string).includes("SELECT id, feedback_id::text AS feedback_id, asset, client_address, feedback_index")
      );
      expect(gqlFeedbacksCall).toBeDefined();
      expect(gqlFeedbacksCall[0]).toContain("status != 'ORPHANED'");
      expect(gqlFeedbacksCall[0]).toContain(
        "ORDER BY created_at DESC, asset ASC, client_address ASC, feedback_index ASC, feedback_id ASC NULLS LAST, id ASC"
      );

      const gqlResponsesCall = (pool.query as any).mock.calls.find(
        (call: unknown[]) =>
          typeof call[0] === "string" &&
          (call[0] as string).includes("FROM feedback_responses") &&
          (call[0] as string).includes("SELECT id, response_id::text AS response_id, asset, client_address, feedback_index, responder")
      );
      expect(gqlResponsesCall).toBeDefined();
      expect(gqlResponsesCall[0]).toContain("status != 'ORPHANED'");
    } finally {
      await stopServer(server);
    }
  });
});
