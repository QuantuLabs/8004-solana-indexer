/**
 * REST API Server for local mode
 * Exposes endpoints compatible with Supabase PostgREST API format
 */

import express, { Express, Request, Response } from 'express';
import rateLimit from 'express-rate-limit';
import { LRUCache } from 'lru-cache';
import { Server } from 'http';
import { PrismaClient, Prisma, Agent as PrismaAgent, Collection as PrismaCollection } from '@prisma/client';
import { logger } from '../logger.js';
import { decompressFromStorage } from '../utils/compression.js';
import { ReplayVerifier } from '../services/replay-verifier.js';
import cors from 'cors';
import type { Pool } from 'pg';
import { config } from '../config.js';
import { renderIntegrityMetrics } from '../observability/integrity-metrics.js';

// GraphQL rate limiting constants
const GRAPHQL_RATE_LIMIT_WINDOW_MS = 60 * 1000;
const GRAPHQL_RATE_LIMIT_MAX_REQUESTS = parseInt(
  process.env.GRAPHQL_RATE_LIMIT_MAX_REQUESTS || '30',
  10
);

// Security constants
const MAX_LIMIT = 1000; // Maximum items per page
const MAX_OFFSET = 10000; // Maximum pagination offset (prevents O(N) deep scans)
const MAX_METADATA_LIMIT = 100; // Metadata limit lower due to large values (100 * 100KB = 10MB max)
const MAX_COLLECTION_STATS = 50; // Maximum collections in stats
const LEADERBOARD_POOL_SIZE = 1000; // Pool size for leaderboard sorting (DB aggregation)
const MAX_METADATA_AGGREGATE_BYTES = 10 * 1024 * 1024; // 10MB max aggregate decompressed size
const MAX_TREE_DEPTH = 8; // Max recursive depth for parent/children traversal
const LEADERBOARD_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minute cache for leaderboard
const LEADERBOARD_CACHE_MAX_SIZE = 100; // Max collections to cache (LRU eviction)
const RATE_LIMIT_WINDOW_MS = 60 * 1000; // 1 minute
const RATE_LIMIT_MAX_REQUESTS = parseInt(
  process.env.RATE_LIMIT_MAX_REQUESTS || '100',
  10
); // 100 requests per minute per IP (default)
const REPLAY_RATE_LIMIT_WINDOW_MS = 30 * 1000; // 30 seconds
const REPLAY_RATE_LIMIT_MAX_REQUESTS = 1; // 1 request per 30s per IP
const REPLAY_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minute cache for replay results
const REPLAY_CACHE_MAX_SIZE = 50;

export interface ApiServerOptions {
  prisma?: PrismaClient | null;
  pool?: Pool | null;
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

// Cache for replay verification results (prevents repeated expensive replays)
const replayCache = new LRUCache<string, import('../services/replay-verifier.js').VerificationResult>({
  max: REPLAY_CACHE_MAX_SIZE,
  ttl: REPLAY_CACHE_TTL_MS,
});

// Base58 alphabet for input validation
const BASE58_REGEX = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;

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

function parsePostgRESTBoolean(value: unknown): boolean | undefined {
  const parsed = parsePostgRESTValue(value);
  if (parsed === undefined) return undefined;
  if (parsed === 'true') return true;
  if (parsed === 'false') return false;
  return undefined;
}

function parseTimestampValue(value: string): Date | undefined {
  const trimmed = value.trim();
  if (!trimmed) return undefined;

  if (/^-?\d+$/.test(trimmed)) {
    const raw = Number(trimmed);
    if (!Number.isFinite(raw)) return undefined;
    const ms = Math.abs(raw) >= 1_000_000_000_000 ? raw : raw * 1000;
    const date = new Date(ms);
    if (!Number.isNaN(date.getTime())) return date;
  }

  const date = new Date(trimmed);
  return Number.isNaN(date.getTime()) ? undefined : date;
}

function parsePostgRESTDateFilter(value: unknown): Prisma.DateTimeFilter | undefined {
  const raw = safeQueryString(value);
  if (!raw) return undefined;

  const parseBound = (
    prefix: string,
    op: 'gt' | 'gte' | 'lt' | 'lte' | 'equals'
  ): Prisma.DateTimeFilter | undefined => {
    if (!raw.startsWith(prefix)) return undefined;
    const date = parseTimestampValue(raw.slice(prefix.length));
    if (!date) return undefined;
    return { [op]: date } as Prisma.DateTimeFilter;
  };

  return (
    parseBound('gt.', 'gt') ||
    parseBound('gte.', 'gte') ||
    parseBound('lt.', 'lt') ||
    parseBound('lte.', 'lte') ||
    parseBound('eq.', 'equals') ||
    (() => {
      const date = parseTimestampValue(raw);
      return date ? ({ equals: date } as Prisma.DateTimeFilter) : undefined;
    })()
  );
}

type AgentApiRow = Pick<
  PrismaAgent,
  | 'id'
  | 'owner'
  | 'creator'
  | 'uri'
  | 'wallet'
  | 'collection'
  | 'collectionPointer'
  | 'colLocked'
  | 'parentAsset'
  | 'parentCreator'
  | 'parentLocked'
  | 'nftName'
  | 'atomEnabled'
  | 'trustTier'
  | 'qualityScore'
  | 'confidence'
  | 'riskScore'
  | 'diversityRatio'
  | 'feedbackCount'
  | 'rawAvgScore'
  | 'agentId'
  | 'status'
  | 'verifiedAt'
  | 'createdAt'
  | 'updatedAt'
>;

function mapAgentToApi(a: AgentApiRow): Record<string, unknown> {
  return {
    asset: a.id,
    owner: a.owner,
    creator: a.creator,
    agent_uri: a.uri,
    agent_wallet: a.wallet,
    collection: a.collection,
    collection_pointer: a.collectionPointer,
    col_locked: a.colLocked,
    parent_asset: a.parentAsset,
    parent_creator: a.parentCreator,
    parent_locked: a.parentLocked,
    nft_name: a.nftName,
    atom_enabled: a.atomEnabled,
    trust_tier: a.trustTier,
    quality_score: a.qualityScore,
    confidence: a.confidence,
    risk_score: a.riskScore,
    diversity_ratio: a.diversityRatio,
    feedback_count: a.feedbackCount,
    raw_avg_score: a.rawAvgScore,
    status: a.status,
    verified_at: a.verifiedAt?.toISOString() || null,
    created_at: a.createdAt.toISOString(),
    updated_at: a.updatedAt.toISOString(),
  };
}

function mapCollectionToApi(collection: PrismaCollection): Record<string, unknown> {
  return {
    collection: collection.col,
    creator: collection.creator,
    first_seen_asset: collection.firstSeenAsset,
    first_seen_at: collection.firstSeenAt.toISOString(),
    first_seen_slot: collection.firstSeenSlot.toString(),
    first_seen_tx_signature: collection.firstSeenTxSignature,
    last_seen_at: collection.lastSeenAt.toISOString(),
    last_seen_slot: collection.lastSeenSlot.toString(),
    last_seen_tx_signature: collection.lastSeenTxSignature,
    asset_count: collection.assetCount.toString(),
    version: collection.version,
    name: collection.name,
    symbol: collection.symbol,
    description: collection.description,
    image: collection.image,
    banner_image: collection.bannerImage,
    social_website: collection.socialWebsite,
    social_x: collection.socialX,
    social_discord: collection.socialDiscord,
    metadata_status: collection.metadataStatus,
    metadata_hash: collection.metadataHash,
    metadata_bytes: collection.metadataBytes,
    metadata_updated_at: collection.metadataUpdatedAt?.toISOString() || null,
  };
}

/**
 * Build status filter for verification status
 * Default: exclude ORPHANED (return PENDING + FINALIZED)
 * ?status=FINALIZED: only finalized
 * ?status=PENDING: only pending
 * ?includeOrphaned=true: include all statuses
 */
const VALID_STATUSES = new Set(['PENDING', 'FINALIZED', 'ORPHANED']);

function buildStatusFilter(
  req: Request,
  fieldName = 'status',
  queryKeys: string | string[] = 'status'
): Record<string, unknown> | undefined | { _invalid: true } {
  const keys = Array.isArray(queryKeys) ? queryKeys : [queryKeys];
  let status: string | undefined;
  const queryRecord = req.query as Record<string, unknown>;
  for (const key of keys) {
    status = parsePostgRESTValue(queryRecord[key]);
    if (status) break;
  }
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
  const wantsRest = config.apiMode !== 'graphql';
  const wantsGraphql = config.apiMode !== 'rest' && config.enableGraphql;
  const restEnabled = wantsRest && !!options.prisma;
  const graphqlEnabled = wantsGraphql && !!options.pool;

  if (config.apiMode === 'rest' && !options.prisma) {
    throw new Error('REST mode requires local Prisma client');
  }
  if (config.apiMode === 'graphql' && !options.pool) {
    throw new Error('GraphQL mode requires Supabase PostgreSQL pool (DB_MODE=supabase)');
  }
  if (!restEnabled && !graphqlEnabled) {
    throw new Error(
      'No API backend available for API_MODE. Provide Prisma (REST), Supabase pool (GraphQL), or set API_MODE explicitly.'
    );
  }

  const prisma = options.prisma as PrismaClient;
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
  if (allowedOrigins.includes('*')) {
    logger.warn('CORS_ORIGINS not set, defaulting to wildcard (*). Set CORS_ORIGINS env var for production.');
  }
  app.use(cors({
    origin: allowedOrigins.includes('*') ? '*' : allowedOrigins,
    methods: ['GET', 'POST', 'OPTIONS'],
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

  if (config.metricsEndpointEnabled) {
    app.get('/metrics', (_req: Request, res: Response) => {
      res.setHeader('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
      res.send(renderIntegrityMetrics());
    });
  }

  // Global rate limiting
  const limiter = rateLimit({
    windowMs: RATE_LIMIT_WINDOW_MS,
    max: RATE_LIMIT_MAX_REQUESTS,
    message: { error: 'Too many requests, please try again later' },
    standardHeaders: true,
    legacyHeaders: false,
  });
  app.use(limiter);

  // Dedicated rate limiter for replay verification (expensive endpoint)
  const replayLimiter = rateLimit({
    windowMs: REPLAY_RATE_LIMIT_WINDOW_MS,
    max: REPLAY_RATE_LIMIT_MAX_REQUESTS,
    message: { error: 'Replay verification rate limited. Try again in 30 seconds.' },
    standardHeaders: true,
    legacyHeaders: false,
  });

  if (!restEnabled) {
    app.use('/rest/v1', (_req: Request, res: Response) => {
      res.status(410).json({ error: 'REST API disabled. Use GraphQL endpoint at /v2/graphql.' });
    });
  }

  // GET /rest/v1/agents - List agents with filters (PostgREST format)
  app.get('/rest/v1/agents', async (req: Request, res: Response) => {
    try {
      const id = parsePostgRESTValue(req.query.id) ?? parsePostgRESTValue(req.query.asset);
      const owner = parsePostgRESTValue(req.query.owner);
      const creator = parsePostgRESTValue(req.query.creator);
      const collection = parsePostgRESTValue(req.query.collection);
      const collectionPointer =
        parsePostgRESTValue(req.query.canonical_col) ??
        parsePostgRESTValue(req.query.collection_pointer);
      const agent_wallet = parsePostgRESTValue(req.query.agent_wallet);
      const parentAsset = parsePostgRESTValue(req.query.parent_asset);
      const parentCreator = parsePostgRESTValue(req.query.parent_creator);
      const colLocked = parsePostgRESTBoolean(req.query.col_locked);
      const parentLocked = parsePostgRESTBoolean(req.query.parent_locked);
      const updatedAtFilter = parsePostgRESTDateFilter(req.query.updated_at);
      const updatedAtGtRaw = parsePostgRESTValue(req.query.updated_at_gt);
      const updatedAtLtRaw = parsePostgRESTValue(req.query.updated_at_lt);
      const updatedAtGt = updatedAtGtRaw ? parseTimestampValue(updatedAtGtRaw) : undefined;
      const updatedAtLt = updatedAtLtRaw ? parseTimestampValue(updatedAtLtRaw) : undefined;
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);

      if ((updatedAtGtRaw && !updatedAtGt) || (updatedAtLtRaw && !updatedAtLt)) {
        res.status(400).json({ error: 'Invalid updated_at filter value' });
        return;
      }

      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }
      const where: Prisma.AgentWhereInput = { ...statusFilter };
      if (id) where.id = id;
      if (owner) where.owner = owner;
      if (creator) where.creator = creator;
      if (collection) where.collection = collection;
      if (collectionPointer) where.collectionPointer = collectionPointer;
      if (agent_wallet) where.wallet = agent_wallet;
      if (parentAsset) where.parentAsset = parentAsset;
      if (parentCreator) where.parentCreator = parentCreator;
      if (colLocked !== undefined) where.colLocked = colLocked;
      if (parentLocked !== undefined) where.parentLocked = parentLocked;
      if (updatedAtFilter || updatedAtGt || updatedAtLt) {
        where.updatedAt = {
          ...(updatedAtFilter ?? {}),
          ...(updatedAtGt ? { gt: updatedAtGt } : {}),
          ...(updatedAtLt ? { lt: updatedAtLt } : {}),
        };
      }

      const agents = await prisma.agent.findMany({
        where,
        orderBy: [{ createdAt: 'desc' }, { id: 'desc' }],
        take: limit,
        skip: offset,
      });

      const mapped = agents.map(mapAgentToApi);

      res.json(mapped);
    } catch (error) {
      logger.error({ error }, 'Error fetching agents');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/agents/children - Direct children for a given parent asset
  app.get('/rest/v1/agents/children', async (req: Request, res: Response) => {
    try {
      const parentAsset = parsePostgRESTValue(req.query.parent_asset) ?? parsePostgRESTValue(req.query.parent);
      if (!parentAsset) {
        res.status(400).json({ error: 'Missing required query param: parent_asset' });
        return;
      }

      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);

      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }

      const where: Prisma.AgentWhereInput = { ...statusFilter, parentAsset };
      const children = await prisma.agent.findMany({
        where,
        orderBy: [
          { createdAt: 'desc' },
          { id: 'desc' },
        ],
        take: limit,
        skip: offset,
      });

      res.json(children.map(mapAgentToApi));
    } catch (error) {
      logger.error({ error }, 'Error fetching agent children');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/agents/tree - Reconstruct parent/children tree (bounded depth)
  app.get('/rest/v1/agents/tree', async (req: Request, res: Response) => {
    try {
      const rootAsset = parsePostgRESTValue(req.query.root_asset)
        ?? parsePostgRESTValue(req.query.root)
        ?? parsePostgRESTValue(req.query.parent_asset);
      if (!rootAsset) {
        res.status(400).json({ error: 'Missing required query param: root_asset' });
        return;
      }

      const maxDepthRaw = safeQueryString(req.query.max_depth);
      const parsedMaxDepth = maxDepthRaw ? parseInt(maxDepthRaw, 10) : 5;
      const maxDepth = Number.isFinite(parsedMaxDepth)
        ? Math.min(Math.max(parsedMaxDepth, 0), MAX_TREE_DEPTH)
        : 5;
      const includeRoot = parsePostgRESTBoolean(req.query.include_root) !== false;
      const limit = safePaginationLimit(req.query.limit, 1000);
      const offset = safePaginationOffset(req.query.offset);

      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }

      const root = await prisma.agent.findFirst({
        where: { ...statusFilter, id: rootAsset },
      });
      if (!root) {
        res.json([]);
        return;
      }

      const visited = new Set<string>([root.id]);
      const depthByAsset = new Map<string, number>([[root.id, 0]]);
      const pathByAsset = new Map<string, string[]>([[root.id, [root.id]]]);
      const orderedNodes: AgentApiRow[] = includeRoot ? [root] : [];

      let frontier: string[] = [root.id];
      for (let depth = 1; depth <= maxDepth && frontier.length > 0; depth++) {
        const children = await prisma.agent.findMany({
          where: {
            ...statusFilter,
            parentAsset: { in: frontier },
          },
          orderBy: [
            { createdAt: 'asc' },
            { id: 'asc' },
          ],
        });

        const nextFrontier: string[] = [];
        for (const child of children) {
          if (visited.has(child.id)) continue;
          const parent = child.parentAsset;
          if (!parent) continue;
          const parentPath = pathByAsset.get(parent);
          if (!parentPath) continue;

          visited.add(child.id);
          depthByAsset.set(child.id, depth);
          pathByAsset.set(child.id, [...parentPath, child.id]);
          orderedNodes.push(child);
          nextFrontier.push(child.id);
        }
        frontier = nextFrontier;
      }

      const paged = orderedNodes.slice(offset, offset + limit);
      res.json(
        paged.map((node) => ({
          ...mapAgentToApi(node),
          depth: depthByAsset.get(node.id) ?? 0,
          path: pathByAsset.get(node.id) ?? [node.id],
        }))
      );
    } catch (error) {
      logger.error({ error }, 'Error building agent tree');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/agents/lineage - Parent chain for a given asset
  app.get('/rest/v1/agents/lineage', async (req: Request, res: Response) => {
    try {
      const asset = parsePostgRESTValue(req.query.asset);
      if (!asset) {
        res.status(400).json({ error: 'Missing required query param: asset' });
        return;
      }

      const includeSelf = parsePostgRESTBoolean(req.query.include_self) !== false;
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);
      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }

      const chain: AgentApiRow[] = [];
      let cursorAsset: string | null = asset;
      let depth = 0;
      const seen = new Set<string>();

      while (cursorAsset && depth <= MAX_TREE_DEPTH * 4) {
        if (seen.has(cursorAsset)) break;
        seen.add(cursorAsset);

        const lineageNode: AgentApiRow | null = await prisma.agent.findFirst({
          where: { ...statusFilter, id: cursorAsset },
        });
        if (!lineageNode) break;

        chain.push(lineageNode);
        cursorAsset = lineageNode.parentAsset;
        depth++;
      }

      const ordered = chain.reverse();
      const out = includeSelf ? ordered : ordered.slice(0, -1);
      res.json(out.slice(offset, offset + limit).map(mapAgentToApi));
    } catch (error) {
      logger.error({ error }, 'Error fetching agent lineage');
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
      const createdAtFilter = parsePostgRESTDateFilter(req.query.created_at);
      const createdAtGtRaw = parsePostgRESTValue(req.query.created_at_gt);
      const createdAtLtRaw = parsePostgRESTValue(req.query.created_at_lt);
      const createdAtGt = createdAtGtRaw ? parseTimestampValue(createdAtGtRaw) : undefined;
      const createdAtLt = createdAtLtRaw ? parseTimestampValue(createdAtLtRaw) : undefined;
      const orFilterRaw = safeQueryString(req.query.or); // Handle OR filter for tag search
      const orFilter = orFilterRaw && orFilterRaw.length <= 200 ? orFilterRaw : undefined; // Limit filter length
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);

      if ((createdAtGtRaw && !createdAtGt) || (createdAtLtRaw && !createdAtLt)) {
        res.status(400).json({ error: 'Invalid created_at filter value' });
        return;
      }

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
      if (createdAtFilter || createdAtGt || createdAtLt) {
        where.createdAt = {
          ...(createdAtFilter ?? {}),
          ...(createdAtGt ? { gt: createdAtGt } : {}),
          ...(createdAtLt ? { lt: createdAtLt } : {}),
        };
      }
      // Handle OR filter: (tag1.eq.value,tag2.eq.value)
      if (orFilter) {
        const tag1Match = orFilter.match(/tag1\.eq\.([^,)]+)/);
        const tag2Match = orFilter.match(/tag2\.eq\.([^,)]+)/);
        const orConditions: Prisma.FeedbackWhereInput[] = [];
        try {
          if (tag1Match) orConditions.push({ tag1: decodeURIComponent(tag1Match[1]) });
          if (tag2Match) orConditions.push({ tag2: decodeURIComponent(tag2Match[1]) });
        } catch {
          res.status(400).json({ error: 'Invalid percent-encoding in filter' });
          return;
        }
        if (orConditions.length > 0) where.OR = orConditions;
      }

      // If Prefer: count=exact, also get total count
      const needsCount = wantsCount(req);
      const [feedbacks, totalCount] = await Promise.all([
        prisma.feedback.findMany({
          where,
          orderBy: [
            { createdAt: 'desc' },
            { agentId: 'asc' },
            { client: 'asc' },
            { feedbackIndex: 'asc' },
            { id: 'asc' },
          ],
          take: limit,
          skip: offset,
        }),
        needsCount ? prisma.feedback.count({ where }) : Promise.resolve(0),
      ]);

      // Set Content-Range header if count was requested
      if (needsCount) {
        setContentRange(res, offset, feedbacks.length, totalCount);
      }

      const revokedFeedbacks = feedbacks.filter((feedback) => feedback.revoked);
      const revokedAtByFeedbackKey = new Map<string, Date>();
      const revocationModel = (prisma as unknown as {
        revocation?: {
          findMany?: (args: unknown) => Promise<Array<{
            agentId: string;
            client: string;
            feedbackIndex: bigint;
            createdAt: Date;
          }>>;
        };
      }).revocation;

      if (
        revokedFeedbacks.length > 0 &&
        revocationModel &&
        typeof revocationModel.findMany === 'function'
      ) {
        const revocations = await revocationModel.findMany({
          where: {
            OR: revokedFeedbacks.map((feedback) => ({
              agentId: feedback.agentId,
              client: feedback.client,
              feedbackIndex: feedback.feedbackIndex,
            })),
          },
          select: {
            agentId: true,
            client: true,
            feedbackIndex: true,
            createdAt: true,
          },
        });

        for (const revocation of revocations) {
          revokedAtByFeedbackKey.set(
            `${revocation.agentId}:${revocation.client}:${revocation.feedbackIndex.toString()}`,
            revocation.createdAt
          );
        }
      }

      // Map to SDK expected format
      // Note: feedback_index as String to preserve BigInt precision (> 2^53)
      const mapped = feedbacks.map(f => ({
        id: f.id,
        asset: f.agentId,
        client_address: f.client,
        feedback_index: f.feedbackIndex.toString(),
        value: f.value.toString(),           // v0.6.0: i128 raw metric value (stringified)
        value_decimals: f.valueDecimals,     // v0.6.0: decimal precision 0-18
        score: f.score,                      // v0.5.0: Option<u8>, null if ATOM skipped
        tag1: f.tag1,
        tag2: f.tag2,
        endpoint: f.endpoint,
        feedback_uri: f.feedbackUri,
        feedback_hash: f.feedbackHash ? Buffer.from(f.feedbackHash).toString('hex') : null,
        running_digest: f.runningDigest ? Buffer.from(f.runningDigest).toString('hex') : null,
        is_revoked: f.revoked,
        revoked_at: (
          revokedAtByFeedbackKey.get(`${f.agentId}:${f.client}:${f.feedbackIndex.toString()}`) ?? null
        )?.toISOString() ?? null,
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

      let parsedFeedbackIndex: bigint | undefined;
      if (feedback_index !== undefined) {
        const idx = safeBigInt(feedback_index);
        if (idx === undefined) {
          res.status(400).json({ error: 'Invalid feedback_index: must be a valid integer' });
          return;
        }
        parsedFeedbackIndex = idx;
      }

      if (feedback_id) {
        where.feedbackId = feedback_id;
      } else if (asset && client_address && parsedFeedbackIndex !== undefined) {
        // Find feedback first, then get responses
        const feedback = await prisma.feedback.findFirst({
          where: {
            agentId: asset,
            client: client_address,
            feedbackIndex: parsedFeedbackIndex,
          },
        });
        if (feedback) {
          where.feedbackId = feedback.id;
        } else {
          // Check orphan responses (feedback not yet indexed)
          const orphans = await prisma.orphanResponse.findMany({
            where: { agentId: asset, client: client_address, feedbackIndex: parsedFeedbackIndex },
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
      } else {
        const feedbackWhere: Prisma.FeedbackWhereInput = {};
        if (asset) feedbackWhere.agentId = asset;
        if (client_address) feedbackWhere.client = client_address;
        if (parsedFeedbackIndex !== undefined) feedbackWhere.feedbackIndex = parsedFeedbackIndex;
        if (Object.keys(feedbackWhere).length > 0) {
          where.feedback = feedbackWhere;
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
        response_count: r.responseCount ? r.responseCount.toString() : null,
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
      const client = parsePostgRESTValue(req.query.client_address) ?? parsePostgRESTValue(req.query.client);
      const feedback_index = parsePostgRESTValue(req.query.feedback_index);
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
      if (feedback_index !== undefined) {
        const idx = safeBigInt(feedback_index);
        if (idx === undefined) {
          res.status(400).json({ error: 'Invalid feedback_index: must be a valid integer' });
          return;
        }
        where.feedbackIndex = idx;
      }

      const orderBy: { revokeCount?: 'asc' | 'desc'; createdAt?: 'asc' | 'desc' } =
        order === 'revoke_count.asc'
          ? { revokeCount: 'asc' }
          : order === 'revoke_count.desc'
            ? { revokeCount: 'desc' }
            : { createdAt: 'desc' };

      const needsCount = wantsCount(req);
      const [revocations, totalCount] = await Promise.all([
        prisma.revocation.findMany({
          where,
          orderBy,
          take: limit,
          skip: offset,
        }),
        needsCount ? prisma.revocation.count({ where }) : Promise.resolve(0),
      ]);

      if (needsCount) {
        setContentRange(res, offset, revocations.length, totalCount);
      }

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

  // GET /rest/v1/validations - disabled (validation module archived on-chain in v0.5.0+)
  app.get('/rest/v1/validations', async (_req: Request, res: Response) => {
    res.status(410).json({
      error: 'Validation module is archived in agent-registry-8004 (v0.5.0+). Endpoint disabled.',
    });
  });

  // GET /rest/v1/collections - Canonical collections (creator + collection pointer key)
  app.get('/rest/v1/collections', async (req: Request, res: Response) => {
    try {
      const col = parsePostgRESTValue(req.query.collection);
      const creator = parsePostgRESTValue(req.query.creator);
      const firstSeenAsset = parsePostgRESTValue(req.query.first_seen_asset);
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);

      const where: Prisma.CollectionWhereInput = {};
      if (col) where.col = col;
      if (creator) where.creator = creator;
      if (firstSeenAsset) where.firstSeenAsset = firstSeenAsset;

      const needsCount = wantsCount(req);
      const [rows, totalCount] = await Promise.all([
        prisma.collection.findMany({
          where,
          orderBy: [
            { firstSeenAt: 'desc' },
            { col: 'asc' },
            { creator: 'asc' },
          ],
          take: limit,
          skip: offset,
        }),
        needsCount ? prisma.collection.count({ where }) : Promise.resolve(0),
      ]);

      if (needsCount) {
        setContentRange(res, offset, rows.length, totalCount);
      }

      res.json(rows.map(mapCollectionToApi));
    } catch (error) {
      logger.error({ error }, 'Error fetching collections');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/collection_asset_count - Count assets for creator+col scope
  app.get('/rest/v1/collection_asset_count', async (req: Request, res: Response) => {
    try {
      const col = parsePostgRESTValue(req.query.collection);
      if (!col) {
        res.status(400).json({ error: 'Missing required query param: collection' });
        return;
      }
      const creator = parsePostgRESTValue(req.query.creator);

      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }

      const where: Prisma.AgentWhereInput = { ...statusFilter, collectionPointer: col };
      if (creator) where.creator = creator;

      const count = await prisma.agent.count({ where });
      res.json({
        collection: col,
        creator: creator || null,
        asset_count: count,
      });
    } catch (error) {
      logger.error({ error }, 'Error counting collection assets');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/collection_assets - Paginated assets for a canonical collection pointer
  app.get('/rest/v1/collection_assets', async (req: Request, res: Response) => {
    try {
      const col = parsePostgRESTValue(req.query.collection);
      if (!col) {
        res.status(400).json({ error: 'Missing required query param: collection' });
        return;
      }
      const creator = parsePostgRESTValue(req.query.creator);
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);

      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }

      const where: Prisma.AgentWhereInput = { ...statusFilter, collectionPointer: col };
      if (creator) where.creator = creator;

      const needsCount = wantsCount(req);
      const [agents, totalCount] = await Promise.all([
        prisma.agent.findMany({
          where,
          orderBy: [
            { createdAt: 'desc' },
            { id: 'desc' },
          ],
          take: limit,
          skip: offset,
        }),
        needsCount ? prisma.agent.count({ where }) : Promise.resolve(0),
      ]);

      if (needsCount) {
        setContentRange(res, offset, agents.length, totalCount);
      }

      res.json(agents.map(mapAgentToApi));
    } catch (error) {
      logger.error({ error }, 'Error fetching collection assets');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/collection_stats - Collection statistics (PostgREST format)
  // GET /rest/v1/collection_stats - Collection statistics (cached to prevent DB DoS)
  app.get('/rest/v1/collection_stats', async (req: Request, res: Response) => {
    try {
      const collection = parsePostgRESTValue(req.query.collection);
      const orderBy = safeQueryString(req.query.order);
      const includeOrphaned = safeQueryString(req.query.includeOrphaned) === 'true';
      const cacheScope = includeOrphaned ? 'all' : 'active';
      const cacheKey = collection ? `${cacheScope}:c:${collection}` : `${cacheScope}:__global__`;

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
        const agentWhere: Prisma.AgentWhereInput = includeOrphaned
          ? { collection }
          : { collection, status: { not: 'ORPHANED' } };
        const feedbackWhere: Prisma.FeedbackWhereInput = includeOrphaned
          ? { agent: { collection } }
          : {
              status: { not: 'ORPHANED' },
              agent: { collection, status: { not: 'ORPHANED' } },
            };
        const registryWhere: Prisma.RegistryWhereInput = includeOrphaned
          ? { collection }
          : { collection, status: { not: 'ORPHANED' } };

        const agentCount = await prisma.agent.count({
          where: agentWhere,
        });

        const feedbackAgg = await prisma.feedback.aggregate({
          where: feedbackWhere,
          _count: true,
          _avg: { score: true },
        });

        const registry = await prisma.registry.findFirst({
          where: registryWhere,
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
        const stats = includeOrphaned
          ? await prisma.$queryRaw<Array<{
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
            `
          : await prisma.$queryRaw<Array<{
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
                WHERE status != 'ORPHANED'
                GROUP BY collection
              ) agent_stats ON agent_stats.collection = r.collection
              LEFT JOIN (
                SELECT a.collection, COUNT(f.id) as total_feedbacks, AVG(f.score) as avg_score
                FROM "Feedback" f
                JOIN "Agent" a ON a.id = f."agentId"
                WHERE f.status != 'ORPHANED'
                  AND a.status != 'ORPHANED'
                GROUP BY a.collection
              ) feedback_stats ON feedback_stats.collection = r.collection
              WHERE r.status != 'ORPHANED'
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
  const globalStatsHandler = async (req: Request, res: Response) => {
    try {
      const includeOrphaned = safeQueryString(req.query.includeOrphaned) === 'true';
      const agentWhere: Prisma.AgentWhereInput = includeOrphaned ? {} : { status: { not: 'ORPHANED' } };
      const feedbackWhere: Prisma.FeedbackWhereInput = includeOrphaned ? {} : { status: { not: 'ORPHANED' } };
      const registryWhere: Prisma.RegistryWhereInput = includeOrphaned ? {} : { status: { not: 'ORPHANED' } };
      const validationWhere: Prisma.ValidationWhereInput = includeOrphaned
        ? {}
        : { chainStatus: { not: 'ORPHANED' } };

      const [totalAgents, totalFeedbacks, totalRegistries, totalValidations] = await Promise.all([
        prisma.agent.count({ where: agentWhere }),
        prisma.feedback.count({ where: feedbackWhere }),
        prisma.registry.count({ where: registryWhere }),
        prisma.validation.count({ where: validationWhere }),
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
      const includeOrphaned = safeQueryString(req.query.includeOrphaned) === 'true';
      const cacheScope = includeOrphaned ? 'all' : 'active';
      const cacheKey = collection ? `${cacheScope}:c:${collection}` : `${cacheScope}:__global__`;

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
        trust_score: number | bigint;
        feedback_count: bigint;
      };

      // Use separate queries to avoid dynamic SQL construction (safer pattern)
      // Note: CAST instead of ::int for SQLite/PostgreSQL compatibility
      // Note: Table names match Prisma model names (Agent, Feedback)
      let result: LeaderboardRow[];
      if (collection && includeOrphaned) {
        result = await prisma.$queryRaw<LeaderboardRow[]>`
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
        `;
      } else if (collection) {
        result = await prisma.$queryRaw<LeaderboardRow[]>`
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
            AND f.status != 'ORPHANED'
          WHERE a.collection = ${collection}
            AND a.status != 'ORPHANED'
          GROUP BY a.id, a.owner, a.collection
          HAVING COUNT(f.id) > 0
          ORDER BY trust_score DESC, feedback_count DESC
          LIMIT ${LEADERBOARD_POOL_SIZE}
        `;
      } else if (includeOrphaned) {
        result = await prisma.$queryRaw<LeaderboardRow[]>`
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
      } else {
        result = await prisma.$queryRaw<LeaderboardRow[]>`
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
            AND f.status != 'ORPHANED'
          WHERE a.status != 'ORPHANED'
          GROUP BY a.id, a.owner, a.collection
          HAVING COUNT(f.id) > 0
          ORDER BY trust_score DESC, feedback_count DESC
          LIMIT ${LEADERBOARD_POOL_SIZE}
        `;
      }

      // Convert BigInt to number for JSON serialization
      const withScores = result.map(r => ({
        asset: r.asset,
        owner: r.owner,
        collection: r.collection,
        trust_score: Number(r.trust_score),
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
      if (!BASE58_REGEX.test(asset)) { res.status(400).json({ error: 'Invalid asset: must be a base58-encoded public key (32-44 chars)' }); return; }
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
      if (!BASE58_REGEX.test(asset)) { res.status(400).json({ error: 'Invalid asset: must be a base58-encoded public key (32-44 chars)' }); return; }

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

  // GET /rest/v1/verify/replay/:asset - Trigger incremental replay verification
  app.get('/rest/v1/verify/replay/:asset', replayLimiter, async (req: Request, res: Response) => {
    try {
      const asset = safeQueryString(req.params.asset);
      if (!asset) { res.status(400).json({ error: 'asset parameter required' }); return; }
      if (!BASE58_REGEX.test(asset)) { res.status(400).json({ error: 'Invalid asset: must be a base58-encoded public key (32-44 chars)' }); return; }

      const cached = replayCache.get(asset);
      if (cached) { res.json(cached); return; }

      const verifier = new ReplayVerifier(prisma);
      const result = await verifier.incrementalVerify(asset);
      replayCache.set(asset, result);
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
      if (!BASE58_REGEX.test(asset)) { res.status(400).json({ error: 'Invalid asset: must be a base58-encoded public key (32-44 chars)' }); return; }
      const chainType = safeQueryString(req.query.chainType) || 'feedback';
      const fromCountStr = safeQueryString(req.query.fromCount) || '0';
      const toCountStr = safeQueryString(req.query.toCount);
      if (!/^\d+$/.test(fromCountStr) || (toCountStr && !/^\d+$/.test(toCountStr))) {
        res.status(400).json({ error: 'fromCount and toCount must be non-negative integers' });
        return;
      }
      const fromCount = parseInt(fromCountStr, 10);
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

  // GraphQL endpoint (behind feature flag)
  if (graphqlEnabled && options.pool) {
    import('./graphql/index.js').then(({ createGraphQLHandler }) => {
      const graphqlLimiter = rateLimit({
        windowMs: GRAPHQL_RATE_LIMIT_WINDOW_MS,
        max: GRAPHQL_RATE_LIMIT_MAX_REQUESTS,
        message: { error: 'GraphQL rate limited. Max 30 requests per minute.' },
        standardHeaders: true,
        legacyHeaders: false,
      });
      const yoga = createGraphQLHandler({ pool: options.pool!, prisma: options.prisma ?? null });
      app.use('/v2/graphql', graphqlLimiter, yoga.handle as any);
      app.use('/graphql', graphqlLimiter, (req: Request, res: Response, next: Function) => {
        res.setHeader('Deprecation', 'true');
        res.setHeader('Link', '</v2/graphql>; rel="successor-version"');
        (yoga.handle as any)(req, res, next);
      });
      logger.info('GraphQL endpoint mounted at /v2/graphql (legacy alias /graphql)');
    }).catch((err: unknown) => {
      logger.error({ error: err }, 'Failed to mount GraphQL endpoint');
    });
  }

  return app;
}

export async function startApiServer(options: ApiServerOptions): Promise<Server> {
  const { port = 3001 } = options;
  const app = createApiServer(options);

  return new Promise((resolve) => {
    const server = app.listen(port, () => {
      logger.info({ port, apiMode: config.apiMode }, 'API server started');
      resolve(server);
    });
  });
}
