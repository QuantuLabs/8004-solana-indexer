/**
 * REST API Server for local mode
 * Exposes endpoints compatible with Supabase PostgREST API format
 */

import express, { Express, Request, Response } from 'express';
import rateLimit from 'express-rate-limit';
import { LRUCache } from 'lru-cache';
import { Server } from 'http';
import { PrismaClient, Prisma } from '@prisma/client';
import { logger } from '../logger.js';
import { decompressFromStorage } from '../utils/compression.js';
import { ReplayVerifier } from '../services/replay-verifier.js';
import cors from 'cors';

// Security constants
const MAX_LIMIT = 1000; // Maximum items per page
const MAX_OFFSET = 10000; // Maximum pagination offset (prevents O(N) deep scans)
const MAX_METADATA_LIMIT = 100; // Metadata limit lower due to large values (100 * 100KB = 10MB max)
const MAX_COLLECTION_STATS = 50; // Maximum collections in stats
const LEADERBOARD_POOL_SIZE = 1000; // Pool size for leaderboard sorting (DB aggregation)
const MAX_METADATA_AGGREGATE_BYTES = 10 * 1024 * 1024; // 10MB max aggregate decompressed size
const LEADERBOARD_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minute cache for leaderboard
const LEADERBOARD_CACHE_MAX_SIZE = 100; // Max collections to cache (LRU eviction)
const RATE_LIMIT_WINDOW_MS = 60 * 1000; // 1 minute
const RATE_LIMIT_MAX_REQUESTS = 100; // 100 requests per minute per IP

export interface ApiServerOptions {
  prisma: PrismaClient;
  port?: number;
}

// LRU cache for leaderboard (prevents unbounded memory growth + repeated queries)
type LeaderboardEntry = { asset: string; owner: string; collection: string; trust_score: number; feedback_count: number };
const leaderboardCache = new LRUCache<string, LeaderboardEntry[]>({
  max: LEADERBOARD_CACHE_MAX_SIZE,
  ttl: LEADERBOARD_CACHE_TTL_MS,
});

// Cache for collection stats (prevents repeated heavy aggregations)
type CollectionStatsEntry = { collection: string; registry_type: string; authority: string | null; agent_count: number; total_feedbacks: number; avg_score: number | null };
const collectionStatsCache = new LRUCache<string, CollectionStatsEntry[]>({
  max: 10, // Small cache - only need to cache "all collections" and a few individual ones
  ttl: LEADERBOARD_CACHE_TTL_MS, // Same 5 minute TTL
});

/**
 * Safely extract string from query parameter
 * Express can pass arrays or objects - we only want strings
 */
function safeQueryString(value: unknown): string | undefined {
  if (typeof value === 'string') return value;
  if (Array.isArray(value) && typeof value[0] === 'string') return value[0];
  return undefined;
}

/**
 * Safely parse pagination params (limit/offset)
 */
function safePaginationLimit(value: unknown, defaultVal = 100): number {
  const str = safeQueryString(value);
  if (!str) return defaultVal;
  const num = parseInt(str, 10);
  if (isNaN(num) || num <= 0) return defaultVal;
  return Math.min(num, MAX_LIMIT);
}

function safePaginationOffset(value: unknown): number {
  const str = safeQueryString(value);
  if (!str) return 0;
  const num = parseInt(str, 10);
  return isNaN(num) ? 0 : Math.min(Math.max(0, num), MAX_OFFSET);
}

/**
 * Safely parse BigInt from query parameter
 * Returns undefined for invalid input instead of throwing
 */
function safeBigInt(value: unknown): bigint | undefined {
  const str = safeQueryString(value);
  if (!str) return undefined;
  // Validate: only digits, optional leading minus
  if (!/^-?\d+$/.test(str)) return undefined;
  try {
    return BigInt(str);
  } catch {
    return undefined;
  }
}

/**
 * Safely parse BigInt array from query parameter (comma-separated)
 */
function safeBigIntArray(value: unknown): bigint[] | undefined {
  const str = safeQueryString(value);
  if (!str) return undefined;
  const parts = str.split(',').map(s => s.trim());
  const result: bigint[] = [];
  for (const part of parts) {
    if (!/^-?\d+$/.test(part)) return undefined;
    try {
      result.push(BigInt(part));
    } catch {
      return undefined;
    }
  }
  return result.length > 0 ? result : undefined;
}

/**
 * Check if request wants count in response (PostgREST Prefer: count=exact)
 */
function wantsCount(req: Request): boolean {
  const prefer = req.headers['prefer'];
  if (!prefer) return false;
  const preferStr = Array.isArray(prefer) ? prefer[0] : prefer;
  return preferStr.includes('count=exact');
}

/**
 * Set Content-Range header for PostgREST compatibility
 * Format: "offset-end/total" e.g., "0-99/1234"
 */
function setContentRange(res: Response, offset: number, items: number, total: number): void {
  if (items === 0) {
    res.setHeader('Content-Range', `items */${total}`);
    return;
  }
  const end = offset + items - 1;
  res.setHeader('Content-Range', `items ${offset}-${end}/${total}`);
}

/**
 * Parse PostgREST-style query parameter value
 * Examples: "eq.value" -> value, "neq.value" -> value, "false" -> "false"
 */
function parsePostgRESTValue(value: unknown): string | undefined {
  const str = safeQueryString(value);
  if (!str) return undefined;
  // Handle PostgREST format: eq.value, neq.value, etc.
  if (str.startsWith('eq.')) return str.slice(3);
  if (str.startsWith('neq.')) return str.slice(4);
  // Return as-is for non-PostgREST format
  return str;
}

/**
 * Parse PostgREST IN query: "in.(val1,val2,val3)" -> ["val1", "val2", "val3"]
 * Returns undefined if not in.() format
 */
function parsePostgRESTIn(value: unknown): string[] | undefined {
  const str = safeQueryString(value);
  if (!str || !str.startsWith('in.(') || !str.endsWith(')')) return undefined;
  const inner = str.slice(4, -1);
  if (!inner) return [];
  return inner.split(',').map(v => v.trim());
}

/**
 * Build status filter for verification status
 * Default: exclude ORPHANED (return PENDING + FINALIZED)
 * ?status=FINALIZED: only finalized
 * ?status=PENDING: only pending
 * ?includeOrphaned=true: include all statuses
 */
const VALID_STATUSES = new Set(['PENDING', 'FINALIZED', 'ORPHANED']);

function buildStatusFilter(req: Request, fieldName = 'status'): Record<string, unknown> | undefined | { _invalid: true } {
  const status = parsePostgRESTValue(req.query.status);
  const includeOrphaned = safeQueryString(req.query.includeOrphaned) === 'true';

  if (status) {
    if (!VALID_STATUSES.has(status)) {
      return { _invalid: true };
    }
    return { [fieldName]: status };
  }

  if (includeOrphaned) {
    return undefined; // No filter, return all
  }

  // Default: exclude orphaned data
  return { [fieldName]: { not: 'ORPHANED' } };
}

function isInvalidStatus(filter: ReturnType<typeof buildStatusFilter>): filter is { _invalid: true } {
  return filter !== undefined && '_invalid' in filter;
}

export function createApiServer(options: ApiServerOptions): Express {
  const { prisma } = options;
  const app = express();

  const trustProxyRaw = process.env.TRUST_PROXY;
  let trustProxy: string | number | boolean = 1;
  if (trustProxyRaw !== undefined) {
    if (trustProxyRaw === 'true') trustProxy = true;
    else if (trustProxyRaw === 'false') trustProxy = false;
    else if (/^\d+$/.test(trustProxyRaw)) trustProxy = Number(trustProxyRaw);
    else trustProxy = trustProxyRaw;
  }
  app.set('trust proxy', trustProxy);

  app.use(express.json({ limit: '100kb' }));

  // CORS - allow configurable origins
  const allowedOrigins = process.env.CORS_ORIGINS?.split(',').map(s => s.trim()) || ['*'];
  app.use(cors({
    origin: allowedOrigins.includes('*') ? '*' : allowedOrigins,
    methods: ['GET', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'Prefer'],
    exposedHeaders: ['Content-Range'],
    maxAge: 86400,
  }));

  // Security headers
  app.use((_req: Request, res: Response, next: Function) => {
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('X-Frame-Options', 'DENY');
    res.setHeader('X-XSS-Protection', '0');
    res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
    res.setHeader('Content-Security-Policy', "default-src 'none'");
    next();
  });

  // Health check (before rate limiter - cheap endpoint)
  app.get('/health', (_req: Request, res: Response) => {
    res.json({ status: 'ok' });
  });

  // Global rate limiting
  const limiter = rateLimit({
    windowMs: RATE_LIMIT_WINDOW_MS,
    max: RATE_LIMIT_MAX_REQUESTS,
    message: { error: 'Too many requests, please try again later' },
    standardHeaders: true,
    legacyHeaders: false,
  });
  app.use(limiter);

  // GET /rest/v1/agents - List agents with filters (PostgREST format)
  app.get('/rest/v1/agents', async (req: Request, res: Response) => {
    try {
      const id = parsePostgRESTValue(req.query.id);
      const owner = parsePostgRESTValue(req.query.owner);
      const collection = parsePostgRESTValue(req.query.collection);
      const agent_wallet = parsePostgRESTValue(req.query.agent_wallet);
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);

      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }
      const where: Prisma.AgentWhereInput = { ...statusFilter };
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
        status: a.status,
        verified_at: a.verifiedAt?.toISOString() || null,
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
      const asset = parsePostgRESTValue(req.query.asset);
      const client_address = parsePostgRESTValue(req.query.client_address);
      const feedback_index = parsePostgRESTValue(req.query.feedback_index);
      const feedback_index_in = parsePostgRESTIn(req.query.feedback_index);
      const is_revoked = parsePostgRESTValue(req.query.is_revoked);
      const tag1 = parsePostgRESTValue(req.query.tag1);
      const tag2 = parsePostgRESTValue(req.query.tag2);
      const endpoint = parsePostgRESTValue(req.query.endpoint);
      const orFilterRaw = safeQueryString(req.query.or); // Handle OR filter for tag search
      const orFilter = orFilterRaw && orFilterRaw.length <= 200 ? orFilterRaw : undefined; // Limit filter length
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);

      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }
      const where: Prisma.FeedbackWhereInput = { ...statusFilter };
      if (asset) where.agentId = asset;
      if (client_address) where.client = client_address;
      if (feedback_index_in) {
        const indices = safeBigIntArray(feedback_index_in.join(','));
        if (indices) where.feedbackIndex = { in: indices };
      } else if (feedback_index !== undefined) {
        const idx = safeBigInt(feedback_index);
        if (idx !== undefined) where.feedbackIndex = idx;
      }
      if (is_revoked !== undefined) where.revoked = is_revoked === 'true';
      if (tag1) where.tag1 = tag1;
      if (tag2) where.tag2 = tag2;
      if (endpoint) where.endpoint = endpoint;
      // Handle OR filter: (tag1.eq.value,tag2.eq.value)
      if (orFilter) {
        const tag1Match = orFilter.match(/tag1\.eq\.([^,)]+)/);
        const tag2Match = orFilter.match(/tag2\.eq\.([^,)]+)/);
        const orConditions: Prisma.FeedbackWhereInput[] = [];
        if (tag1Match) orConditions.push({ tag1: decodeURIComponent(tag1Match[1]) });
        if (tag2Match) orConditions.push({ tag2: decodeURIComponent(tag2Match[1]) });
        if (orConditions.length > 0) where.OR = orConditions;
      }

      // If Prefer: count=exact, also get total count
      const needsCount = wantsCount(req);
      const [feedbacks, totalCount] = await Promise.all([
        prisma.feedback.findMany({
          where,
          orderBy: { createdAt: 'desc' },
          take: limit,
          skip: offset,
        }),
        needsCount ? prisma.feedback.count({ where }) : Promise.resolve(0),
      ]);

      // Set Content-Range header if count was requested
      if (needsCount) {
        setContentRange(res, offset, feedbacks.length, totalCount);
      }

      // Map to SDK expected format
      // Note: feedback_index as String to preserve BigInt precision (> 2^53)
      const mapped = feedbacks.map(f => ({
        id: f.id,
        asset: f.agentId,
        client_address: f.client,
        feedback_index: f.feedbackIndex.toString(),
        value: f.value.toString(),           // v0.5.0: i64 raw metric value
        value_decimals: f.valueDecimals,     // v0.5.0: decimal precision 0-6
        score: f.score,                      // v0.5.0: Option<u8>, null if ATOM skipped
        tag1: f.tag1,
        tag2: f.tag2,
        endpoint: f.endpoint,
        feedback_uri: f.feedbackUri,
        feedback_hash: f.feedbackHash ? Buffer.from(f.feedbackHash).toString('hex') : null,
        running_digest: f.runningDigest ? Buffer.from(f.runningDigest).toString('hex') : null,
        is_revoked: f.revoked,
        revoked_at: null, // TODO: add revokedAt tracking
        status: f.status,
        verified_at: f.verifiedAt?.toISOString() || null,
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
      const feedback_id = parsePostgRESTValue(req.query.feedback_id);
      const asset = parsePostgRESTValue(req.query.asset);
      const client_address = parsePostgRESTValue(req.query.client_address);
      const feedback_index = parsePostgRESTValue(req.query.feedback_index);
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);
      const order = safeQueryString(req.query.order);

      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }
      const where: Prisma.FeedbackResponseWhereInput = { ...statusFilter };

      if (feedback_id) {
        where.feedbackId = feedback_id;
      } else if (asset && client_address && feedback_index !== undefined) {
        const idx = safeBigInt(feedback_index);
        if (idx === undefined) {
          res.status(400).json({ error: 'Invalid feedback_index: must be a valid integer' });
          return;
        }
        // Find feedback first, then get responses
        const feedback = await prisma.feedback.findFirst({
          where: {
            agentId: asset,
            client: client_address,
            feedbackIndex: idx,
          },
        });
        if (feedback) {
          where.feedbackId = feedback.id;
        } else {
          // Check orphan responses (feedback not yet indexed)
          const orphans = await prisma.orphanResponse.findMany({
            where: { agentId: asset, client: client_address, feedbackIndex: idx },
            orderBy: { createdAt: 'desc' },
            take: limit,
            skip: offset,
          });
          const mapped = orphans.map(o => ({
            id: o.id,
            feedback_id: null,
            asset: o.agentId,
            client_address: o.client,
            feedback_index: o.feedbackIndex.toString(),
            responder: o.responder,
            response_uri: o.responseUri,
            response_hash: o.responseHash ? Buffer.from(o.responseHash).toString('hex') : null,
            status: 'PENDING',
            block_slot: o.slot ? Number(o.slot) : 0,
            tx_signature: o.txSignature || '',
            created_at: o.createdAt.toISOString(),
          }));
          res.json(mapped);
          return;
        }
      }

      const orderBy: Prisma.FeedbackResponseOrderByWithRelationInput =
        order === 'response_count.asc' ? { responseCount: 'asc' } :
        order === 'response_count.desc' ? { responseCount: 'desc' } :
        { createdAt: 'desc' };

      const needsCount = wantsCount(req);
      const [responses, totalCount] = await Promise.all([
        prisma.feedbackResponse.findMany({
          where,
          orderBy,
          take: limit,
          skip: offset,
          include: { feedback: true },
        }),
        needsCount ? prisma.feedbackResponse.count({ where }) : Promise.resolve(0),
      ]);

      if (needsCount) {
        setContentRange(res, offset, responses.length, totalCount);
      }

      // Map to SDK expected format (IndexedFeedbackResponse)
      // Note: feedback_index as String to preserve BigInt precision (> 2^53)
      const mapped = responses.map(r => ({
        id: r.id,
        feedback_id: r.feedbackId,
        asset: r.feedback.agentId,
        client_address: r.feedback.client,
        feedback_index: r.feedback.feedbackIndex.toString(),
        responder: r.responder,
        response_uri: r.responseUri,
        response_hash: r.responseHash ? Buffer.from(r.responseHash).toString('hex') : null,
        running_digest: r.runningDigest ? Buffer.from(r.runningDigest).toString('hex') : null,
        response_count: r.responseCount ? Number(r.responseCount) : null,
        status: r.status,
        verified_at: r.verifiedAt?.toISOString() || null,
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

  // GET /rest/v1/revocations - List revocations with filters (PostgREST format)
  app.get('/rest/v1/revocations', async (req: Request, res: Response) => {
    try {
      const asset = parsePostgRESTValue(req.query.asset);
      const client = parsePostgRESTValue(req.query.client);
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);
      const order = safeQueryString(req.query.order);

      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }
      const where: Prisma.RevocationWhereInput = { ...statusFilter };
      if (asset) where.agentId = asset;
      if (client) where.client = client;

      const orderBy: { revokeCount?: 'asc' | 'desc'; createdAt?: 'asc' | 'desc' } =
        order === 'revoke_count.desc' ? { revokeCount: 'desc' } : { createdAt: 'desc' };

      const revocations = await prisma.revocation.findMany({
        where,
        orderBy,
        take: limit,
        skip: offset,
      });

      const mapped = revocations.map(r => ({
        id: r.id,
        asset: r.agentId,
        client_address: r.client,
        feedback_index: r.feedbackIndex.toString(),
        feedback_hash: r.feedbackHash ? Buffer.from(r.feedbackHash).toString('hex') : null,
        slot: Number(r.slot),
        original_score: r.originalScore,
        atom_enabled: r.atomEnabled,
        had_impact: r.hadImpact,
        running_digest: r.runningDigest ? Buffer.from(r.runningDigest).toString('hex') : null,
        revoke_count: Number(r.revokeCount),
        tx_signature: r.txSignature,
        status: r.status,
        verified_at: r.verifiedAt?.toISOString() || null,
        created_at: r.createdAt.toISOString(),
      }));

      res.json(mapped);
    } catch (error) {
      logger.error({ error }, 'Error fetching revocations');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/validations - List validations with filters (PostgREST format)
  app.get('/rest/v1/validations', async (req: Request, res: Response) => {
    try {
      const asset = parsePostgRESTValue(req.query.asset);
      const validator = parsePostgRESTValue(req.query.validator) || parsePostgRESTValue(req.query.validator_address);
      const nonce = parsePostgRESTValue(req.query.nonce);
      const responded = parsePostgRESTValue(req.query.responded);
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);

      const statusFilter = buildStatusFilter(req, 'chainStatus');
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }
      const where: Prisma.ValidationWhereInput = { ...statusFilter };
      if (asset) where.agentId = asset;
      if (validator) where.validator = validator;
      if (nonce !== undefined) {
        const nonceInt = safeBigInt(nonce);
        if (nonceInt !== undefined) where.nonce = nonceInt;
      }
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
      // Note: nonce is BigInt but small values - safe to convert to Number for JSON
      const mapped = validations.map(v => ({
        id: v.id,
        asset: v.agentId,
        validator_address: v.validator,
        nonce: v.nonce > BigInt(Number.MAX_SAFE_INTEGER) ? v.nonce.toString() : Number(v.nonce),
        requester: v.requester,
        request_uri: v.requestUri,
        request_hash: v.requestHash ? Buffer.from(v.requestHash).toString('hex') : null,
        response: v.response,
        response_uri: v.responseUri,
        response_hash: v.responseHash ? Buffer.from(v.responseHash).toString('hex') : null,
        tag: v.tag,
        status: v.response !== null ? 'RESPONDED' as const : 'PENDING' as const,
        chain_status: v.chainStatus,
        chain_verified_at: v.chainVerifiedAt?.toISOString() || null,
        block_slot: v.requestSlot ? Number(v.requestSlot) : (v.responseSlot ? Number(v.responseSlot) : 0),
        tx_signature: v.requestTxSignature || v.responseTxSignature || '',
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
      const collection = parsePostgRESTValue(req.query.collection);
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);

      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }
      const where: Prisma.RegistryWhereInput = { ...statusFilter };
      if (collection) where.collection = collection;

      const registries = await prisma.registry.findMany({
        where,
        orderBy: { createdAt: 'desc' },
        take: limit,
        skip: offset,
      });

      // Convert BigInt fields to strings for JSON serialization
      const mapped = registries.map(r => ({
        ...r,
        slot: r.slot !== null ? r.slot.toString() : null,
        verified_at: r.verifiedAt?.toISOString() || null,
      }));

      res.json(mapped);
    } catch (error) {
      logger.error({ error }, 'Error fetching registries');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/collection_stats - Collection statistics (PostgREST format)
  // GET /rest/v1/collection_stats - Collection statistics (cached to prevent DB DoS)
  app.get('/rest/v1/collection_stats', async (req: Request, res: Response) => {
    try {
      const collection = parsePostgRESTValue(req.query.collection);
      const orderBy = safeQueryString(req.query.order);
      const cacheKey = collection ? `c:${collection}` : '__global__';

      // Check cache first (prevents repeated heavy aggregations)
      const cached = collectionStatsCache.get(cacheKey);
      if (cached) {
        const result = orderBy === 'agent_count.desc'
          ? [...cached].sort((a, b) => b.agent_count - a.agent_count)
          : cached;
        return res.json(result);
      }

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

        const stats = [{
          collection: collection,
          registry_type: registry?.registryType || 'USER',
          authority: registry?.authority || null,
          agent_count: agentCount,
          total_feedbacks: feedbackAgg._count || 0,
          avg_score: feedbackAgg._avg?.score || null,
        }];

        collectionStatsCache.set(cacheKey, stats);
        res.json(stats);
      } else {
        // Get stats for all collections using single SQL query (prevents N+1 DoS)
        // Instead of 50 registries × 2 queries = 100 queries, this does 1 query
        // Note: Table/column names match Prisma schema (Registry, Agent, Feedback)
        const stats = await prisma.$queryRaw<Array<{
          collection: string;
          registry_type: string;
          authority: string | null;
          agent_count: bigint;
          total_feedbacks: bigint;
          avg_score: number | null;
        }>>`
          SELECT
            r.collection,
            r."registryType" as registry_type,
            r.authority,
            COALESCE(agent_stats.agent_count, 0) as agent_count,
            COALESCE(feedback_stats.total_feedbacks, 0) as total_feedbacks,
            feedback_stats.avg_score
          FROM "Registry" r
          LEFT JOIN (
            SELECT collection, COUNT(*) as agent_count
            FROM "Agent"
            GROUP BY collection
          ) agent_stats ON agent_stats.collection = r.collection
          LEFT JOIN (
            SELECT a.collection, COUNT(f.id) as total_feedbacks, AVG(f.score) as avg_score
            FROM "Feedback" f
            JOIN "Agent" a ON a.id = f."agentId"
            GROUP BY a.collection
          ) feedback_stats ON feedback_stats.collection = r.collection
          ORDER BY r."createdAt" DESC
          LIMIT ${MAX_COLLECTION_STATS}
        `;

        // Convert BigInt to number for JSON serialization
        const formattedStats = stats.map(s => ({
          collection: s.collection,
          registry_type: s.registry_type,
          authority: s.authority,
          agent_count: Number(s.agent_count),
          total_feedbacks: Number(s.total_feedbacks),
          avg_score: s.avg_score,
        }));

        // Cache before sorting (sort is cheap, aggregation is expensive)
        collectionStatsCache.set(cacheKey, formattedStats);

        // Sort by agent_count if requested
        const result = orderBy === 'agent_count.desc'
          ? [...formattedStats].sort((a, b) => b.agent_count - a.agent_count)
          : formattedStats;

        res.json(result);
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

  // GET /rest/v1/stats/verification - Verification status stats
  app.get('/rest/v1/stats/verification', async (_req: Request, res: Response) => {
    try {
      const [agents, feedbacks, validations, registries, metadata, responses] = await Promise.all([
        prisma.agent.groupBy({ by: ['status'], _count: true }),
        prisma.feedback.groupBy({ by: ['status'], _count: true }),
        prisma.validation.groupBy({ by: ['chainStatus'], _count: true }),
        prisma.registry.groupBy({ by: ['status'], _count: true }),
        prisma.agentMetadata.groupBy({ by: ['status'], _count: true }),
        prisma.feedbackResponse.groupBy({ by: ['status'], _count: true }),
      ]);

      const toStatusMap = (groups: { _count: number; status?: string; chainStatus?: string }[]) => {
        const result: Record<string, number> = { PENDING: 0, FINALIZED: 0, ORPHANED: 0 };
        for (const g of groups) {
          const status = g.status || g.chainStatus || 'PENDING';
          result[status] = g._count;
        }
        return result;
      };

      res.json({
        agents: toStatusMap(agents),
        feedbacks: toStatusMap(feedbacks),
        validations: toStatusMap(validations),
        registries: toStatusMap(registries),
        metadata: toStatusMap(metadata),
        feedback_responses: toStatusMap(responses),
      });
    } catch (error) {
      logger.error({ error }, 'Error fetching verification stats');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/metadata - Metadata entries (PostgREST format)
  app.get('/rest/v1/metadata', async (req: Request, res: Response) => {
    try {
      const asset = parsePostgRESTValue(req.query.asset);
      const key = parsePostgRESTValue(req.query.key);
      // Use stricter limit for metadata (each value can be up to 100KB compressed)
      const requestedLimit = safePaginationLimit(req.query.limit);
      const limit = Math.min(requestedLimit, MAX_METADATA_LIMIT);

      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }
      const where: Prisma.AgentMetadataWhereInput = { ...statusFilter };
      if (asset) where.agentId = asset;
      if (key) where.key = key;

      const metadata = await prisma.agentMetadata.findMany({
        where,
        take: limit,
      });

      // Decompress sequentially with aggregate size limit (prevent OOM)
      const results: Array<{
        id: string;
        asset: string;
        key: string;
        value: string;
        immutable: boolean;
        status: string;
        verified_at: string | null;
      }> = [];
      let totalBytes = 0;

      for (const m of metadata) {
        const decompressed = await decompressFromStorage(Buffer.from(m.value));
        totalBytes += decompressed.length;

        // Stop if aggregate exceeds limit (prevent 1000 * 1MB = 1GB OOM)
        if (totalBytes > MAX_METADATA_AGGREGATE_BYTES) {
          logger.warn({
            totalBytes,
            limit: MAX_METADATA_AGGREGATE_BYTES,
            itemsProcessed: results.length,
            totalItems: metadata.length
          }, 'Metadata aggregate size limit exceeded, truncating response');
          break;
        }

        results.push({
          id: `${m.agentId}:${m.key}`,
          asset: m.agentId,
          key: m.key,
          value: decompressed.toString('base64'),
          immutable: m.immutable,
          status: m.status,
          verified_at: m.verifiedAt?.toISOString() || null,
        });
      }

      res.json(results);
    } catch (error) {
      logger.error({ error }, 'Error fetching metadata');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/leaderboard - Top agents (PostgREST format)
  // Uses DB-level aggregation to prevent loading 100k+ rows into memory
  // LRU cached with TTL to prevent repeated heavy queries
  app.get('/rest/v1/leaderboard', async (req: Request, res: Response) => {
    try {
      const limit = safePaginationLimit(req.query.limit);
      const collection = parsePostgRESTValue(req.query.collection);
      const cacheKey = collection ? `c:${collection}` : '__global__';

      // Check LRU cache first (TTL handled by cache)
      const cached = leaderboardCache.get(cacheKey);
      if (cached) {
        return res.json(cached.slice(0, limit));
      }

      // Use Prisma.sql for safe parameterized queries (prevents SQL injection regression)
      // This avoids the 1000 agents × 100 feedbacks = 100k objects memory issue
      type LeaderboardRow = {
        asset: string;
        owner: string;
        collection: string;
        trust_score: number;
        feedback_count: bigint;
      };

      // Use separate queries to avoid dynamic SQL construction (safer pattern)
      // Note: CAST instead of ::int for SQLite/PostgreSQL compatibility
      // Note: Table names match Prisma model names (Agent, Feedback)
      const result = collection
        ? await prisma.$queryRaw<LeaderboardRow[]>`
            SELECT
              a.id as asset,
              a.owner,
              a.collection,
              CAST(COALESCE(ROUND(AVG(f.score)), 0) AS INTEGER) as trust_score,
              COUNT(f.id) as feedback_count
            FROM "Agent" a
            LEFT JOIN "Feedback" f ON f."agentId" = a.id
              AND f.revoked = false
              AND f.score IS NOT NULL
            WHERE a.collection = ${collection}
            GROUP BY a.id, a.owner, a.collection
            HAVING COUNT(f.id) > 0
            ORDER BY trust_score DESC, feedback_count DESC
            LIMIT ${LEADERBOARD_POOL_SIZE}
          `
        : await prisma.$queryRaw<LeaderboardRow[]>`
            SELECT
              a.id as asset,
              a.owner,
              a.collection,
              CAST(COALESCE(ROUND(AVG(f.score)), 0) AS INTEGER) as trust_score,
              COUNT(f.id) as feedback_count
            FROM "Agent" a
            LEFT JOIN "Feedback" f ON f."agentId" = a.id
              AND f.revoked = false
              AND f.score IS NOT NULL
            GROUP BY a.id, a.owner, a.collection
            HAVING COUNT(f.id) > 0
            ORDER BY trust_score DESC, feedback_count DESC
            LIMIT ${LEADERBOARD_POOL_SIZE}
          `;

      // Convert BigInt to number for JSON serialization
      const withScores = result.map(r => ({
        asset: r.asset,
        owner: r.owner,
        collection: r.collection,
        trust_score: r.trust_score,
        feedback_count: Number(r.feedback_count),
      }));

      // Update LRU cache (TTL + max size handled automatically)
      leaderboardCache.set(cacheKey, withScores);

      res.json(withScores.slice(0, limit));
    } catch (error) {
      logger.error({ error }, 'Error fetching leaderboard');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/checkpoints/:asset - All checkpoints for an agent
  app.get('/rest/v1/checkpoints/:asset', async (req: Request, res: Response) => {
    try {
      const asset = safeQueryString(req.params.asset);
      if (!asset) { res.status(400).json({ error: 'asset parameter required' }); return; }
      const chainType = safeQueryString(req.query.chainType);
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);

      const where: Prisma.HashChainCheckpointWhereInput = { agentId: asset };
      if (chainType) where.chainType = chainType;

      const checkpoints = await prisma.hashChainCheckpoint.findMany({
        where,
        orderBy: { eventCount: 'asc' },
        take: limit,
        skip: offset,
      });

      res.json(checkpoints.map(cp => ({
        agent_id: cp.agentId,
        chain_type: cp.chainType,
        event_count: cp.eventCount,
        digest: cp.digest,
        created_at: cp.createdAt.toISOString(),
      })));
    } catch (error) {
      logger.error({ error }, 'Error fetching checkpoints');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/checkpoints/:asset/latest - Latest checkpoint per chain type
  app.get('/rest/v1/checkpoints/:asset/latest', async (req: Request, res: Response) => {
    try {
      const asset = safeQueryString(req.params.asset);
      if (!asset) { res.status(400).json({ error: 'asset parameter required' }); return; }

      const [feedback, response, revoke] = await Promise.all(
        ['feedback', 'response', 'revoke'].map(ct =>
          prisma.hashChainCheckpoint.findFirst({
            where: { agentId: asset, chainType: ct },
            orderBy: { eventCount: 'desc' },
          })
        )
      );

      const fmt = (cp: typeof feedback) => cp ? {
        event_count: cp.eventCount,
        digest: cp.digest,
        created_at: cp.createdAt.toISOString(),
      } : null;

      res.json({ feedback: fmt(feedback), response: fmt(response), revoke: fmt(revoke) });
    } catch (error) {
      logger.error({ error }, 'Error fetching latest checkpoints');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/verify/replay/:asset - Trigger full replay verification
  app.get('/rest/v1/verify/replay/:asset', async (req: Request, res: Response) => {
    try {
      const asset = safeQueryString(req.params.asset);
      if (!asset) { res.status(400).json({ error: 'asset parameter required' }); return; }
      const verifier = new ReplayVerifier(prisma);
      const result = await verifier.fullReplay(asset);
      res.json(result);
    } catch (error) {
      logger.error({ error }, 'Error during replay verification');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/events/:asset/replay-data - Events ordered for client-side replay
  app.get('/rest/v1/events/:asset/replay-data', async (req: Request, res: Response) => {
    try {
      const asset = safeQueryString(req.params.asset);
      if (!asset) { res.status(400).json({ error: 'asset parameter required' }); return; }
      const chainType = safeQueryString(req.query.chainType) || 'feedback';
      const fromCount = parseInt(safeQueryString(req.query.fromCount) || '0', 10);
      const toCountStr = safeQueryString(req.query.toCount);
      const toCount = toCountStr ? parseInt(toCountStr, 10) : undefined;
      const limit = safePaginationLimit(req.query.limit);

      if (!['feedback', 'response', 'revoke'].includes(chainType)) {
        res.status(400).json({ error: 'Invalid chainType. Must be feedback, response, or revoke.' });
        return;
      }

      if (chainType === 'feedback') {
        const where: Prisma.FeedbackWhereInput = {
          agentId: asset,
          feedbackIndex: { gte: BigInt(fromCount) },
        };
        if (toCount !== undefined) {
          where.feedbackIndex = { gte: BigInt(fromCount), lt: BigInt(toCount) };
        }

        const events = await prisma.feedback.findMany({
          where,
          orderBy: { feedbackIndex: 'asc' },
          take: limit,
        });

        res.json({
          events: events.map(f => ({
            asset: f.agentId,
            client: f.client,
            feedback_index: f.feedbackIndex.toString(),
            feedback_hash: f.feedbackHash ? Buffer.from(f.feedbackHash).toString('hex') : null,
            slot: f.createdSlot ? Number(f.createdSlot) : 0,
            running_digest: f.runningDigest ? Buffer.from(f.runningDigest).toString('hex') : null,
          })),
          hasMore: events.length === limit,
          nextFromCount: events.length > 0
            ? Number(events[events.length - 1].feedbackIndex) + 1
            : fromCount,
        });
      } else if (chainType === 'response') {
        const where: Prisma.FeedbackResponseWhereInput = {
          feedback: { agentId: asset },
        };
        const rcFilter: { gte: bigint; lt?: bigint } = { gte: BigInt(fromCount) };
        if (toCount !== undefined) rcFilter.lt = BigInt(toCount);
        where.responseCount = rcFilter;

        const events = await prisma.feedbackResponse.findMany({
          where,
          orderBy: { responseCount: 'asc' },
          take: limit,
          include: {
            feedback: {
              select: { agentId: true, client: true, feedbackIndex: true, feedbackHash: true },
            },
          },
        });

        res.json({
          events: events.map(r => ({
            asset: r.feedback.agentId,
            client: r.feedback.client,
            feedback_index: r.feedback.feedbackIndex.toString(),
            responder: r.responder,
            response_hash: r.responseHash ? Buffer.from(r.responseHash).toString('hex') : null,
            feedback_hash: r.feedback.feedbackHash ? Buffer.from(r.feedback.feedbackHash).toString('hex') : null,
            slot: r.slot ? Number(r.slot) : 0,
            running_digest: r.runningDigest ? Buffer.from(r.runningDigest).toString('hex') : null,
            response_count: r.responseCount != null ? Number(r.responseCount) : null,
          })),
          hasMore: events.length === limit,
          nextFromCount: events.length > 0
            ? Number(events[events.length - 1].responseCount ?? 0) + 1
            : fromCount,
        });
      } else {
        const where: Prisma.RevocationWhereInput = {
          agentId: asset,
          revokeCount: { gte: BigInt(fromCount) },
        };
        if (toCount !== undefined) {
          where.revokeCount = { gte: BigInt(fromCount), lt: BigInt(toCount) };
        }

        const events = await prisma.revocation.findMany({
          where,
          orderBy: { revokeCount: 'asc' },
          take: limit,
        });

        res.json({
          events: events.map(r => ({
            asset: r.agentId,
            client: r.client,
            feedback_index: r.feedbackIndex.toString(),
            feedback_hash: r.feedbackHash ? Buffer.from(r.feedbackHash).toString('hex') : null,
            slot: Number(r.slot),
            running_digest: r.runningDigest ? Buffer.from(r.runningDigest).toString('hex') : null,
            revoke_count: Number(r.revokeCount),
          })),
          hasMore: events.length === limit,
          nextFromCount: events.length > 0
            ? Number(events[events.length - 1].revokeCount) + 1
            : fromCount,
        });
      }
    } catch (error) {
      logger.error({ error }, 'Error fetching replay data');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  return app;
}

export async function startApiServer(options: ApiServerOptions): Promise<Server> {
  const { port = 3001 } = options;
  const app = createApiServer(options);

  return new Promise((resolve) => {
    const server = app.listen(port, () => {
      logger.info({ port }, 'REST API server started');
      resolve(server);
    });
  });
}
