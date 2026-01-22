/**
 * REST API Server for local mode
 * Exposes endpoints compatible with Supabase PostgREST API format
 */

import express, { Express, Request, Response } from 'express';
import { PrismaClient } from '@prisma/client';
import { logger } from '../logger.js';

export interface ApiServerOptions {
  prisma: PrismaClient;
  port?: number;
}

/**
 * Parse PostgREST-style query parameter value
 * Examples: "eq.value" -> value, "neq.value" -> value, "false" -> "false"
 */
function parsePostgRESTValue(value: string | undefined): string | undefined {
  if (!value) return undefined;
  // Handle PostgREST format: eq.value, neq.value, etc.
  if (value.startsWith('eq.')) return value.slice(3);
  if (value.startsWith('neq.')) return value.slice(4);
  // Return as-is for non-PostgREST format
  return value;
}

export function createApiServer(options: ApiServerOptions): Express {
  const { prisma } = options;
  const app = express();

  app.use(express.json());

  // Health check
  app.get('/health', (_req: Request, res: Response) => {
    res.json({ status: 'ok' });
  });

  // GET /rest/v1/agents - List agents with filters (PostgREST format)
  app.get('/rest/v1/agents', async (req: Request, res: Response) => {
    try {
      const id = parsePostgRESTValue(req.query.id as string);
      const owner = parsePostgRESTValue(req.query.owner as string);
      const collection = parsePostgRESTValue(req.query.collection as string);
      const agent_wallet = parsePostgRESTValue(req.query.agent_wallet as string);
      const limit = req.query.limit ? parseInt(req.query.limit as string) : 100;
      const offset = req.query.offset ? parseInt(req.query.offset as string) : 0;

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const where: any = {};
      if (id) where.id = id;
      if (owner) where.owner = owner;
      if (collection) where.collection = collection;
      if (agent_wallet) where.wallet = agent_wallet;

      const agents = await prisma.agent.findMany({
        where,
        orderBy: { createdAt: 'desc' },
        take: limit,
        skip: offset,
      });

      // Map to SDK expected format
      const mapped = agents.map(a => ({
        asset: a.id,
        owner: a.owner,
        agent_uri: a.uri,
        agent_wallet: a.wallet,
        collection: a.collection,
        nft_name: a.nftName,
        atom_enabled: a.atomEnabled,
        created_at: a.createdAt.toISOString(),
        updated_at: a.updatedAt.toISOString(),
      }));

      res.json(mapped);
    } catch (error) {
      logger.error({ error }, 'Error fetching agents');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/feedbacks - List feedbacks with filters (PostgREST format)
  app.get('/rest/v1/feedbacks', async (req: Request, res: Response) => {
    try {
      const asset = parsePostgRESTValue(req.query.asset as string);
      const client_address = parsePostgRESTValue(req.query.client_address as string);
      const feedback_index = parsePostgRESTValue(req.query.feedback_index as string);
      const is_revoked = parsePostgRESTValue(req.query.is_revoked as string);
      const tag1 = parsePostgRESTValue(req.query.tag1 as string);
      const tag2 = parsePostgRESTValue(req.query.tag2 as string);
      const endpoint = parsePostgRESTValue(req.query.endpoint as string);
      const orFilter = req.query.or as string; // Handle OR filter for tag search
      const limit = req.query.limit ? parseInt(req.query.limit as string) : 100;
      const offset = req.query.offset ? parseInt(req.query.offset as string) : 0;

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const where: any = {};
      if (asset) where.agentId = asset;
      if (client_address) where.client = client_address;
      if (feedback_index !== undefined) where.feedbackIndex = BigInt(feedback_index);
      if (is_revoked !== undefined) where.revoked = is_revoked === 'true';
      if (tag1) where.tag1 = tag1;
      if (tag2) where.tag2 = tag2;
      if (endpoint) where.endpoint = endpoint;
      // Handle OR filter: (tag1.eq.value,tag2.eq.value)
      if (orFilter) {
        const matches = orFilter.match(/tag1\.eq\.([^,)]+)/);
        const tagValue = matches ? decodeURIComponent(matches[1]) : null;
        if (tagValue) {
          where.OR = [{ tag1: tagValue }, { tag2: tagValue }];
        }
      }

      const feedbacks = await prisma.feedback.findMany({
        where,
        orderBy: { createdAt: 'desc' },
        take: limit,
        skip: offset,
      });

      // Map to SDK expected format
      const mapped = feedbacks.map(f => ({
        id: f.id,
        asset: f.agentId,
        client_address: f.client,
        feedback_index: Number(f.feedbackIndex),
        score: f.score,
        tag1: f.tag1,
        tag2: f.tag2,
        endpoint: f.endpoint,
        feedback_uri: f.feedbackUri,
        feedback_hash: f.feedbackHash ? Buffer.from(f.feedbackHash).toString('hex') : null,
        is_revoked: f.revoked,
        revoked_at: null, // TODO: add revokedAt tracking
        block_slot: Number(f.createdSlot || 0),
        tx_signature: f.createdTxSignature || '',
        created_at: f.createdAt.toISOString(),
      }));

      res.json(mapped);
    } catch (error) {
      logger.error({ error }, 'Error fetching feedbacks');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/responses and /rest/v1/feedback_responses - List responses with filters (PostgREST format)
  // Both routes supported for SDK compatibility
  const responsesHandler = async (req: Request, res: Response) => {
    try {
      const feedback_id = parsePostgRESTValue(req.query.feedback_id as string);
      const asset = parsePostgRESTValue(req.query.asset as string);
      const client_address = parsePostgRESTValue(req.query.client_address as string);
      const feedback_index = parsePostgRESTValue(req.query.feedback_index as string);
      const limit = req.query.limit ? parseInt(req.query.limit as string) : 100;
      const offset = req.query.offset ? parseInt(req.query.offset as string) : 0;

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const where: any = {};

      if (feedback_id) {
        where.feedbackId = feedback_id;
      } else if (asset && client_address && feedback_index !== undefined) {
        // Find feedback first, then get responses
        const feedback = await prisma.feedback.findFirst({
          where: {
            agentId: asset,
            client: client_address,
            feedbackIndex: BigInt(feedback_index),
          },
        });
        if (feedback) {
          where.feedbackId = feedback.id;
        } else {
          res.json([]);
          return;
        }
      }

      const responses = await prisma.feedbackResponse.findMany({
        where,
        orderBy: { createdAt: 'desc' },
        take: limit,
        skip: offset,
        include: { feedback: true },
      });

      // Map to SDK expected format (IndexedFeedbackResponse)
      const mapped = responses.map(r => ({
        id: r.id,
        feedback_id: r.feedbackId,
        asset: r.feedback.agentId,
        client_address: r.feedback.client,
        feedback_index: Number(r.feedback.feedbackIndex),
        responder: r.responder,
        response_uri: r.responseUri,
        response_hash: r.responseHash ? Buffer.from(r.responseHash).toString('hex') : null,
        block_slot: r.slot ? Number(r.slot) : Number(r.feedback.createdSlot || 0),
        tx_signature: r.txSignature || r.feedback.createdTxSignature || '',
        created_at: r.createdAt.toISOString(),
      }));

      res.json(mapped);
    } catch (error) {
      logger.error({ error }, 'Error fetching responses');
      res.status(500).json({ error: 'Internal server error' });
    }
  };
  app.get('/rest/v1/responses', responsesHandler);
  app.get('/rest/v1/feedback_responses', responsesHandler);

  // GET /rest/v1/validations - List validations with filters (PostgREST format)
  app.get('/rest/v1/validations', async (req: Request, res: Response) => {
    try {
      const asset = parsePostgRESTValue(req.query.asset as string);
      const validator = parsePostgRESTValue(req.query.validator as string);
      const nonce = parsePostgRESTValue(req.query.nonce as string);
      const responded = parsePostgRESTValue(req.query.responded as string);
      const limit = req.query.limit ? parseInt(req.query.limit as string) : 100;
      const offset = req.query.offset ? parseInt(req.query.offset as string) : 0;

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const where: any = {};
      if (asset) where.agentId = asset;
      if (validator) where.validator = validator;
      if (nonce !== undefined) where.nonce = parseInt(nonce);
      if (responded !== undefined) {
        where.response = responded === 'true' ? { not: null } : null;
      }

      const validations = await prisma.validation.findMany({
        where,
        orderBy: { createdAt: 'desc' },
        take: limit,
        skip: offset,
      });

      // Map to SDK expected format (IndexedValidation interface)
      const mapped = validations.map(v => ({
        id: v.id,
        asset: v.agentId,
        validator_address: v.validator,
        nonce: v.nonce,
        requester: v.requester,
        request_uri: v.requestUri,
        request_hash: null, // Not stored in this version
        response: v.response,
        response_uri: v.responseUri,
        response_hash: null, // Not stored in this version
        tag: v.tag,
        status: v.response !== null ? 'RESPONDED' as const : 'PENDING' as const,
        block_slot: 0, // Not stored in this version
        tx_signature: '',
        created_at: v.createdAt.toISOString(),
        updated_at: v.respondedAt?.toISOString() || v.createdAt.toISOString(),
      }));

      res.json(mapped);
    } catch (error) {
      logger.error({ error }, 'Error fetching validations');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/registries - List registries/collections (PostgREST format)
  app.get('/rest/v1/registries', async (req: Request, res: Response) => {
    try {
      const collection = parsePostgRESTValue(req.query.collection as string);
      const limit = req.query.limit ? parseInt(req.query.limit as string) : 100;
      const offset = req.query.offset ? parseInt(req.query.offset as string) : 0;

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const where: any = {};
      if (collection) where.collection = collection;

      const registries = await prisma.registry.findMany({
        where,
        orderBy: { createdAt: 'desc' },
        take: limit,
        skip: offset,
      });

      res.json(registries);
    } catch (error) {
      logger.error({ error }, 'Error fetching registries');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/collection_stats - Collection statistics (PostgREST format)
  app.get('/rest/v1/collection_stats', async (req: Request, res: Response) => {
    try {
      const collection = parsePostgRESTValue(req.query.collection as string);
      const orderBy = req.query.order as string;

      // If collection specified, get stats for that collection only
      if (collection) {
        const agentCount = await prisma.agent.count({
          where: { collection: collection },
        });

        const feedbackAgg = await prisma.feedback.aggregate({
          where: {
            agent: { collection: collection },
          },
          _count: true,
          _avg: { score: true },
        });

        const registry = await prisma.registry.findFirst({
          where: { collection: collection },
        });

        res.json([{
          collection: collection,
          registry_type: registry?.registryType || 'USER',
          authority: registry?.authority || null,
          agent_count: agentCount,
          total_feedbacks: feedbackAgg._count || 0,
          avg_score: feedbackAgg._avg?.score || null,
        }]);
      } else {
        // Get stats for all collections
        const registries = await prisma.registry.findMany();
        const stats = await Promise.all(registries.map(async (reg) => {
          const agentCount = await prisma.agent.count({
            where: { collection: reg.collection },
          });
          const feedbackAgg = await prisma.feedback.aggregate({
            where: { agent: { collection: reg.collection } },
            _count: true,
            _avg: { score: true },
          });
          return {
            collection: reg.collection,
            registry_type: reg.registryType,
            authority: reg.authority,
            agent_count: agentCount,
            total_feedbacks: feedbackAgg._count || 0,
            avg_score: feedbackAgg._avg?.score || null,
          };
        }));

        // Sort by agent_count if requested
        if (orderBy === 'agent_count.desc') {
          stats.sort((a, b) => b.agent_count - a.agent_count);
        }

        res.json(stats);
      }
    } catch (error) {
      logger.error({ error }, 'Error fetching collection stats');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/stats and /rest/v1/global_stats - Global stats
  const globalStatsHandler = async (_req: Request, res: Response) => {
    try {
      const [totalAgents, totalFeedbacks, totalRegistries, totalValidations] = await Promise.all([
        prisma.agent.count(),
        prisma.feedback.count(),
        prisma.registry.count(),
        prisma.validation.count(),
      ]);

      // Return as array for SDK compatibility (PostgREST format)
      res.json([{
        total_agents: totalAgents,
        total_feedbacks: totalFeedbacks,
        total_collections: totalRegistries,
        total_validations: totalValidations,
      }]);
    } catch (error) {
      logger.error({ error }, 'Error fetching stats');
      res.status(500).json({ error: 'Internal server error' });
    }
  };
  app.get('/rest/v1/stats', globalStatsHandler);
  app.get('/rest/v1/global_stats', globalStatsHandler);

  // GET /rest/v1/leaderboard - Top agents (PostgREST format)
  app.get('/rest/v1/leaderboard', async (req: Request, res: Response) => {
    try {
      const limit = req.query.limit ? parseInt(req.query.limit as string) : 100;
      const collection = parsePostgRESTValue(req.query.collection as string);

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const where: any = {};
      if (collection) where.collection = collection;

      // Get agents with feedback count
      const agents = await prisma.agent.findMany({
        where,
        take: limit,
        include: {
          feedbacks: {
            where: { revoked: false },
          },
        },
      });

      // Calculate average score and sort
      const withScores = agents.map(a => {
        const activeFeedbacks = a.feedbacks.filter(f => !f.revoked);
        const avgScore = activeFeedbacks.length > 0
          ? activeFeedbacks.reduce((sum, f) => sum + f.score, 0) / activeFeedbacks.length
          : 0;
        return {
          asset: a.id,
          owner: a.owner,
          collection: a.collection,
          trust_score: Math.round(avgScore),
          feedback_count: activeFeedbacks.length,
        };
      });

      // Sort by score descending
      withScores.sort((a, b) => b.trust_score - a.trust_score);

      res.json(withScores.slice(0, limit));
    } catch (error) {
      logger.error({ error }, 'Error fetching leaderboard');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  return app;
}

export async function startApiServer(options: ApiServerOptions): Promise<void> {
  const { port = 3001 } = options;
  const app = createApiServer(options);

  return new Promise((resolve) => {
    app.listen(port, () => {
      logger.info({ port }, 'REST API server started');
      resolve();
    });
  });
}
