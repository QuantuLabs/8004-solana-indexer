/**
 * API server:
 * - Local mode: native REST handlers backed by Prisma
 * - Supabase mode: optional REST proxy passthrough + GraphQL endpoint
 */

import express, { Express, NextFunction, Request, Response } from 'express';
import rateLimit from 'express-rate-limit';
import { LRUCache } from 'lru-cache';
import { Server } from 'http';
import { PrismaClient, Prisma, Agent as PrismaAgent, Collection as PrismaCollection } from '@prisma/client';
import { logger } from '../logger.js';
import { computeKeyHash } from '../utils/pda.js';
import { createGraphQLHandler } from './graphql/index.js';
import {
  fetchReplayDataFromPool,
  getLatestCheckpointsFromPool,
  listCheckpointsFromPool,
  PoolReplayVerifier,
  ReplayChainType,
  ReplayVerifier,
} from '../services/replay-verifier.js';
import cors from 'cors';
import type { Pool } from 'pg';
import { config } from '../config.js';
import { renderIntegrityMetrics } from '../observability/integrity-metrics.js';
import { VERIFICATION_STATS_SQL } from './verification-stats-sql.js';

// GraphQL rate limiting constants
const GRAPHQL_RATE_LIMIT_WINDOW_MS = 60 * 1000;
const GRAPHQL_RATE_LIMIT_MAX_REQUESTS = parseInt(
  process.env.GRAPHQL_RATE_LIMIT_MAX_REQUESTS || '100',
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
const REPLAY_RATE_LIMIT_WINDOW_MS = parseInt(
  process.env.REPLAY_RATE_LIMIT_WINDOW_MS || '30000',
  10
); // 30 seconds
const REPLAY_RATE_LIMIT_MAX_REQUESTS = parseInt(
  process.env.REPLAY_RATE_LIMIT_MAX_REQUESTS || '1',
  10
); // 1 request per 30s per IP
const REPLAY_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minute cache for replay results
const REPLAY_CACHE_MAX_SIZE = 50;
const REST_PROXY_READY_CACHE_TTL_MS = 5 * 1000;
const POOL_READY_CACHE_TTL_MS = 5 * 1000;
const READY_DETAILS_CACHE_TTL_MS = 5 * 1000;
const CIDV1_BASE32_REGEX = /^b[a-z2-7]{20,}$/;

export interface ApiServerOptions {
  prisma?: PrismaClient | null;
  pool?: Pool | null;
  port?: number;
  isReady?: () => boolean;
  isCaughtUp?: () => boolean;
  checkRestProxyReady?: () => Promise<boolean>;
  checkPoolReady?: () => Promise<boolean>;
}

type ReadyStateSnapshot = {
  mainSlot: number | null;
  mainTxIndex: number | null;
  counts: {
    agents: number;
    feedbacks: number;
    responses: number;
    revocations: number;
    collections: number;
  };
  pending: {
    agents: number;
    feedbacks: number;
    responses: number;
    revocations: number;
    metadata: number;
    registries: number;
    validations: number;
  };
  orphans: {
    agents: number;
    feedbacks: number;
    responses: number;
    revocations: number;
    metadata: number;
    registries: number;
    validations: number;
  };
};

type ReplayEventOrder = {
  count: bigint;
  slot: bigint | null;
  txIndex: number | null;
  eventOrdinal: number | null;
  signature: string | null;
  rowId: string;
};

function compareReplayEventOrder(left: ReplayEventOrder, right: ReplayEventOrder): number {
  if (left.count !== right.count) return left.count < right.count ? -1 : 1;
  const leftSlot = left.slot ?? -1n;
  const rightSlot = right.slot ?? -1n;
  if (leftSlot !== rightSlot) return leftSlot < rightSlot ? -1 : 1;
  const leftTxIndex = left.txIndex ?? Number.MAX_SAFE_INTEGER;
  const rightTxIndex = right.txIndex ?? Number.MAX_SAFE_INTEGER;
  if (leftTxIndex !== rightTxIndex) return leftTxIndex - rightTxIndex;
  const leftOrdinal = left.eventOrdinal ?? Number.MAX_SAFE_INTEGER;
  const rightOrdinal = right.eventOrdinal ?? Number.MAX_SAFE_INTEGER;
  if (leftOrdinal !== rightOrdinal) return leftOrdinal - rightOrdinal;
  const leftSignature = left.signature ?? '';
  const rightSignature = right.signature ?? '';
  if (leftSignature !== rightSignature) return leftSignature.localeCompare(rightSignature);
  return left.rowId.localeCompare(right.rowId);
}

type DerivedPrismaCheckpoint = {
  chainType: ReplayChainType;
  eventCount: bigint;
  digest: string;
  createdAt: string;
};

function digestBytesToHex(value: Uint8Array | null | undefined): string | null {
  if (!value) return null;
  return Buffer.from(value).toString('hex');
}

async function queryCheckpointsForPrisma(
  prisma: PrismaClient,
  agentId: string,
  chainType: ReplayChainType,
): Promise<DerivedPrismaCheckpoint[]> {
  if (chainType === 'feedback') {
    const [feedbacks, orphanFeedbacks] = await Promise.all([
      prisma.feedback.findMany({
        where: { agentId, runningDigest: { not: null } },
        select: {
          id: true,
          feedbackIndex: true,
          runningDigest: true,
          createdAt: true,
          createdSlot: true,
          txIndex: true,
          eventOrdinal: true,
          createdTxSignature: true,
        },
      }),
      prisma.orphanFeedback.findMany({
        where: { agentId, runningDigest: { not: null } },
        select: {
          id: true,
          feedbackIndex: true,
          runningDigest: true,
          createdAt: true,
          slot: true,
          txIndex: true,
          eventOrdinal: true,
          txSignature: true,
        },
      }),
    ]);

    return [
      ...feedbacks.map((row) => ({
        chainType,
        eventCount: row.feedbackIndex + 1n,
        digest: digestBytesToHex(row.runningDigest)!,
        createdAt: row.createdAt.toISOString(),
        order: {
          count: row.feedbackIndex + 1n,
          slot: row.createdSlot,
          txIndex: row.txIndex,
          eventOrdinal: row.eventOrdinal,
          signature: row.createdTxSignature,
          rowId: row.id,
        },
      })),
      ...orphanFeedbacks.map((row) => ({
        chainType,
        eventCount: row.feedbackIndex + 1n,
        digest: digestBytesToHex(row.runningDigest)!,
        createdAt: row.createdAt.toISOString(),
        order: {
          count: row.feedbackIndex + 1n,
          slot: row.slot,
          txIndex: row.txIndex,
          eventOrdinal: row.eventOrdinal,
          signature: row.txSignature,
          rowId: row.id,
        },
      })),
    ]
      .filter((row) => row.eventCount % 1000n === 0n)
      .sort((left, right) => compareReplayEventOrder(left.order, right.order))
      .map(({ chainType: checkpointChainType, eventCount, digest, createdAt }) => ({
        chainType: checkpointChainType,
        eventCount,
        digest,
        createdAt,
      }));
  }

  if (chainType === 'response') {
    const [responses, orphanResponses] = await Promise.all([
      prisma.feedbackResponse.findMany({
        where: { feedback: { agentId }, runningDigest: { not: null } },
        select: {
          id: true,
          responseCount: true,
          runningDigest: true,
          createdAt: true,
          slot: true,
          txIndex: true,
          eventOrdinal: true,
          txSignature: true,
        },
      }),
      prisma.orphanResponse.findMany({
        where: { agentId, runningDigest: { not: null } },
        select: {
          id: true,
          responseCount: true,
          runningDigest: true,
          createdAt: true,
          slot: true,
          txIndex: true,
          eventOrdinal: true,
          txSignature: true,
        },
      }),
    ]);

    return [
      ...responses.map((row) => ({
        chainType,
        eventCount: row.responseCount ?? 0n,
        digest: digestBytesToHex(row.runningDigest)!,
        createdAt: row.createdAt.toISOString(),
        order: {
          count: row.responseCount ?? 0n,
          slot: row.slot,
          txIndex: row.txIndex,
          eventOrdinal: row.eventOrdinal,
          signature: row.txSignature,
          rowId: row.id,
        },
      })),
      ...orphanResponses.map((row) => ({
        chainType,
        eventCount: row.responseCount ?? 0n,
        digest: digestBytesToHex(row.runningDigest)!,
        createdAt: row.createdAt.toISOString(),
        order: {
          count: row.responseCount ?? 0n,
          slot: row.slot,
          txIndex: row.txIndex,
          eventOrdinal: row.eventOrdinal,
          signature: row.txSignature,
          rowId: row.id,
        },
      })),
    ]
      .filter((row) => row.eventCount % 1000n === 0n)
      .sort((left, right) => compareReplayEventOrder(left.order, right.order))
      .map(({ chainType: checkpointChainType, eventCount, digest, createdAt }) => ({
        chainType: checkpointChainType,
        eventCount,
        digest,
        createdAt,
      }));
  }

  const revocations = await prisma.revocation.findMany({
    where: { agentId, runningDigest: { not: null } },
    select: {
      id: true,
      revokeCount: true,
      runningDigest: true,
      createdAt: true,
      slot: true,
      txIndex: true,
      eventOrdinal: true,
      txSignature: true,
    },
  });

  return revocations
    .map((row) => ({
      chainType,
      eventCount: row.revokeCount,
      digest: digestBytesToHex(row.runningDigest)!,
      createdAt: row.createdAt.toISOString(),
      order: {
        count: row.revokeCount,
        slot: row.slot,
        txIndex: row.txIndex,
        eventOrdinal: row.eventOrdinal,
        signature: row.txSignature,
        rowId: row.id,
      },
    }))
    .filter((row) => row.eventCount % 1000n === 0n)
    .sort((left, right) => compareReplayEventOrder(left.order, right.order))
    .map(({ chainType: checkpointChainType, eventCount, digest, createdAt }) => ({
      chainType: checkpointChainType,
      eventCount,
      digest,
      createdAt,
    }));
}

async function listCheckpointsFromPrisma(
  prisma: PrismaClient,
  agentId: string,
  chainType?: ReplayChainType,
): Promise<DerivedPrismaCheckpoint[]> {
  if (chainType) {
    return queryCheckpointsForPrisma(prisma, agentId, chainType);
  }

  const [feedback, response, revoke] = await Promise.all([
    queryCheckpointsForPrisma(prisma, agentId, 'feedback'),
    queryCheckpointsForPrisma(prisma, agentId, 'response'),
    queryCheckpointsForPrisma(prisma, agentId, 'revoke'),
  ]);

  return [...feedback, ...response, ...revoke].sort((left, right) => {
    if (left.eventCount !== right.eventCount) return left.eventCount < right.eventCount ? -1 : 1;
    return left.chainType.localeCompare(right.chainType);
  });
}

async function getLatestCheckpointsFromPrisma(
  prisma: PrismaClient,
  agentId: string,
): Promise<{
  feedback: Omit<DerivedPrismaCheckpoint, 'chainType'> | null;
  response: Omit<DerivedPrismaCheckpoint, 'chainType'> | null;
  revoke: Omit<DerivedPrismaCheckpoint, 'chainType'> | null;
}> {
  const [feedback, response, revoke] = await Promise.all([
    queryCheckpointsForPrisma(prisma, agentId, 'feedback'),
    queryCheckpointsForPrisma(prisma, agentId, 'response'),
    queryCheckpointsForPrisma(prisma, agentId, 'revoke'),
  ]);

  const stripChain = (
    entry: DerivedPrismaCheckpoint | undefined,
  ): Omit<DerivedPrismaCheckpoint, 'chainType'> | null => (
    entry ? { eventCount: entry.eventCount, digest: entry.digest, createdAt: entry.createdAt } : null
  );

  return {
    feedback: stripChain(feedback[feedback.length - 1]),
    response: stripChain(response[response.length - 1]),
    revoke: stripChain(revoke[revoke.length - 1]),
  };
}

// LRU cache for leaderboard (prevents unbounded memory growth + repeated queries)
type LeaderboardEntry = {
  asset: string;
  owner: string;
  collection: string | null;
  nft_name: string | null;
  agent_uri: string | null;
  trust_tier: number | null;
  quality_score: number | null;
  confidence: number | null;
  risk_score: number | null;
  diversity_ratio: number | null;
  feedback_count: number | null;
  sort_key: string;
};
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
 * Examples: "eq.value" -> value, "value" -> value
 * Note: neq/in/not.in require dedicated comparison/list parsers.
 */
function parsePostgRESTValue(value: unknown): string | undefined {
  const str = safeQueryString(value);
  if (!str) return undefined;
  // Handle PostgREST format: eq.value, neq.value, etc.
  if (str.startsWith('eq.')) return str.slice(3);
  // Return as-is for non-PostgREST format
  return str;
}

function hasUnsupportedEqOnlyOperator(value: unknown): boolean {
  const str = safeQueryString(value);
  if (!str) return false;
  if (str.startsWith('eq.')) return false;
  const lowered = str.toLowerCase();
  return (
    lowered.startsWith('neq.') ||
    lowered.startsWith('in.(') ||
    lowered.startsWith('not.in.(')
  );
}

type PostgRESTComparison = {
  op: 'eq' | 'neq';
  value: string;
};

function parsePostgRESTComparison(value: unknown): PostgRESTComparison | undefined {
  const str = safeQueryString(value);
  if (!str) return undefined;
  if (str.startsWith('eq.')) return { op: 'eq', value: str.slice(3) };
  if (str.startsWith('neq.')) return { op: 'neq', value: str.slice(4) };
  return { op: 'eq', value: str };
}

type PostgRESTListOperator = 'in' | 'not.in';

type PostgRESTListState = {
  matched: boolean;
  malformed: boolean;
  values: string[];
};

function parsePostgRESTListOperator(value: unknown, operator: PostgRESTListOperator): PostgRESTListState {
  const str = safeQueryString(value);
  const prefix = operator === 'in' ? 'in.(' : 'not.in.(';
  if (!str || !str.startsWith(prefix)) {
    return { matched: false, malformed: false, values: [] };
  }
  if (!str.endsWith(')')) {
    return { matched: true, malformed: true, values: [] };
  }
  const inner = str.slice(prefix.length, -1);
  const values = parsePostgRESTList(inner);
  if (values === undefined) {
    return { matched: true, malformed: true, values: [] };
  }
  return { matched: true, malformed: false, values };
}

function parsePostgRESTList(inner: string): string[] | undefined {
  if (!inner) return [];
  const values: string[] = [];
  let current = '';
  let inQuotes = false;
  let escaped = false;

  for (let i = 0; i < inner.length; i++) {
    const ch = inner[i];

    if (inQuotes) {
      if (escaped) {
        current += ch;
        escaped = false;
        continue;
      }
      if (ch === '\\') {
        escaped = true;
        continue;
      }
      if (ch === '"') {
        inQuotes = false;
        continue;
      }
      current += ch;
      continue;
    }

    if (ch === ',') {
      values.push(current.trim());
      current = '';
      continue;
    }
    if (ch === '"') {
      inQuotes = true;
      continue;
    }
    current += ch;
  }

  if (inQuotes || escaped) {
    return undefined;
  }

  values.push(current.trim());
  return values;
}

function formatPostgRESTList(values: string[]): string {
  return values.map((value) => {
    if (!/[,\s()"\\]/.test(value)) return value;
    const escaped = value.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
    return `"${escaped}"`;
  }).join(',');
}

function collectionPointerVariants(raw: string): string[] {
  const trimmed = raw.trim();
  if (!trimmed) return [];
  if (trimmed.startsWith('c1:')) {
    const bare = trimmed.slice(3);
    if (!CIDV1_BASE32_REGEX.test(bare)) return [trimmed];
    return [trimmed, bare];
  }
  if (!CIDV1_BASE32_REGEX.test(trimmed)) return [trimmed];
  return [`c1:${trimmed}`, trimmed];
}

function formatCanonicalCollectionFilter(raw: string): string {
  const variants = [...new Set(collectionPointerVariants(raw))];
  if (variants.length === 0) return 'in.()';
  if (variants.length === 1) return `eq.${variants[0]}`;
  return `in.(${formatPostgRESTList(variants)})`;
}

function normalizeCollectionPointerFilterValue(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed) return trimmed;
  if (trimmed.startsWith('eq.')) return formatCanonicalCollectionFilter(trimmed.slice(3));
  if (trimmed.startsWith('neq.')) {
    const expanded = [...new Set(collectionPointerVariants(trimmed.slice(4)))];
    if (expanded.length === 0) {
      return "in.()";
    }
    if (expanded.length === 1) {
      return `neq.${expanded[0]}`;
    }
    return `not.in.(${formatPostgRESTList(expanded)})`;
  }
  const inState = parsePostgRESTListOperator(trimmed, 'in');
  if (inState.matched) {
    if (inState.malformed) {
      return 'in.()';
    }
    const inValues = inState.values;
    const expanded = [...new Set(inValues.flatMap((value) => collectionPointerVariants(value)))];
    if (expanded.length === 0) {
      return "in.()";
    }
    if (expanded.length <= 1) {
      return `eq.${expanded[0]}`;
    }
    return `in.(${formatPostgRESTList(expanded)})`;
  }
  const notInState = parsePostgRESTListOperator(trimmed, 'not.in');
  if (notInState.matched) {
    if (notInState.malformed) {
      return 'in.()';
    }
    const notInValues = notInState.values;
    const expanded = [...new Set(notInValues.flatMap((value) => collectionPointerVariants(value)))];
    if (expanded.length === 0) {
      return "in.()";
    }
    return `not.in.(${formatPostgRESTList(expanded)})`;
  }
  return formatCanonicalCollectionFilter(trimmed);
}

type CollectionPointerFilter =
  | { kind: 'none' }
  | { kind: 'no_match' }
  | { kind: 'eq'; values: string[] }
  | { kind: 'neq'; values: string[] };

function resolveCollectionPointerFilter(query: Request['query']): CollectionPointerFilter {
  const queryRecord = query as Record<string, unknown>;
  const hasCanonicalCol = Object.prototype.hasOwnProperty.call(queryRecord, 'canonical_col');
  const hasCollectionPointer = Object.prototype.hasOwnProperty.call(queryRecord, 'collection_pointer');
  if (!hasCanonicalCol && !hasCollectionPointer) {
    return { kind: 'none' };
  }

  const canonicalColRaw = safeQueryString(queryRecord.canonical_col);
  const collectionPointerRaw = safeQueryString(queryRecord.collection_pointer);
  const resolvedRaw = canonicalColRaw && canonicalColRaw.trim().length > 0
    ? canonicalColRaw
    : collectionPointerRaw;

  if (!resolvedRaw || resolvedRaw.trim().length === 0) {
    return { kind: 'no_match' };
  }

  const normalized = normalizeCollectionPointerFilterValue(resolvedRaw);
  const inState = parsePostgRESTListOperator(normalized, 'in');
  if (inState.matched) {
    if (inState.malformed) {
      return { kind: 'no_match' };
    }
    const values = inState.values
      .map((value) => value.trim())
      .filter((value) => value.length > 0);
    return values.length > 0
      ? { kind: 'eq', values }
      : { kind: 'no_match' };
  }

  const notInState = parsePostgRESTListOperator(normalized, 'not.in');
  if (notInState.matched) {
    if (notInState.malformed) {
      return { kind: 'no_match' };
    }
    const values = notInState.values
      .map((value) => value.trim())
      .filter((value) => value.length > 0);
    return values.length > 0
      ? { kind: 'neq', values }
      : { kind: 'no_match' };
  }

  const comparison = parsePostgRESTComparison(normalized);
  if (!comparison) {
    return { kind: 'none' };
  }
  if (comparison.op === 'neq') {
    if (comparison.value.trim().length === 0) {
      return { kind: 'no_match' };
    }
    return { kind: 'neq', values: [comparison.value] };
  }
  if (comparison.value.trim().length === 0) {
    return { kind: 'no_match' };
  }
  return { kind: 'eq', values: [comparison.value] };
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

function parsePostgRESTBigIntFilter(value: unknown): bigint | Prisma.BigIntFilter | undefined | null {
  const raw = safeQueryString(value);
  if (!raw) return undefined;

  const parseOperand = (operand: string): bigint | undefined => safeBigInt(operand);

  if (raw.startsWith('eq.')) {
    return parseOperand(raw.slice(3)) ?? null;
  }

  const parseBound = (
    prefix: string,
    op: 'gt' | 'gte' | 'lt' | 'lte'
  ): Prisma.BigIntFilter | undefined => {
    if (!raw.startsWith(prefix)) return undefined;
    const value = parseOperand(raw.slice(prefix.length));
    if (value === undefined) return undefined;
    return { [op]: value } as Prisma.BigIntFilter;
  };

  const bounded =
    parseBound('gt.', 'gt') ||
    parseBound('gte.', 'gte') ||
    parseBound('lt.', 'lt') ||
    parseBound('lte.', 'lte');
  if (bounded) return bounded;

  const equals = parseOperand(raw);
  return equals ?? null;
}

function parsePostgRESTTextFilter(value: unknown): string | Prisma.StringNullableFilter | undefined | null {
  const raw = safeQueryString(value);
  if (raw === undefined) return undefined;
  if (raw.trim().length === 0) return null;

  const inState = parsePostgRESTListOperator(value, 'in');
  if (inState.matched) {
    if (inState.malformed || inState.values.length === 0 || inState.values.some((entry) => entry.length === 0)) return null;
    return { in: inState.values };
  }

  const notInState = parsePostgRESTListOperator(value, 'not.in');
  if (notInState.matched) {
    if (notInState.malformed || notInState.values.length === 0 || notInState.values.some((entry) => entry.length === 0)) return null;
    return { notIn: notInState.values };
  }

  if (raw.includes('.') && !raw.startsWith('eq.') && !raw.startsWith('neq.')) return null;

  const comparison = parsePostgRESTComparison(raw);
  if (!comparison || comparison.value.trim().length === 0) return null;
  return comparison.op === 'neq' ? { not: comparison.value } : comparison.value;
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
  | 'verifiedSlot'
  | 'createdAt'
  | 'updatedAt'
  | 'createdTxSignature'
  | 'createdSlot'
  | 'txIndex'
  | 'eventOrdinal'
>;

type AgentFeedbackCountOverride = {
  feedbackCount?: number | string | bigint | null;
};

function rot32(value: number, bits: number): number {
  return ((value << bits) | (value >>> (32 - bits))) >>> 0;
}

function mix32(a: number, b: number, c: number): [number, number, number] {
  a = (a - c) >>> 0; a = (a ^ rot32(c, 4)) >>> 0; c = (c + b) >>> 0;
  b = (b - a) >>> 0; b = (b ^ rot32(a, 6)) >>> 0; a = (a + c) >>> 0;
  c = (c - b) >>> 0; c = (c ^ rot32(b, 8)) >>> 0; b = (b + a) >>> 0;
  a = (a - c) >>> 0; a = (a ^ rot32(c, 16)) >>> 0; c = (c + b) >>> 0;
  b = (b - a) >>> 0; b = (b ^ rot32(a, 19)) >>> 0; a = (a + c) >>> 0;
  c = (c - b) >>> 0; c = (c ^ rot32(b, 4)) >>> 0; b = (b + a) >>> 0;
  return [a, b, c];
}

function final32(a: number, b: number, c: number): [number, number, number] {
  c = (c ^ b) >>> 0; c = (c - rot32(b, 14)) >>> 0;
  a = (a ^ c) >>> 0; a = (a - rot32(c, 11)) >>> 0;
  b = (b ^ a) >>> 0; b = (b - rot32(a, 25)) >>> 0;
  c = (c ^ b) >>> 0; c = (c - rot32(b, 16)) >>> 0;
  a = (a ^ c) >>> 0; a = (a - rot32(c, 4)) >>> 0;
  b = (b ^ a) >>> 0; b = (b - rot32(a, 14)) >>> 0;
  c = (c ^ b) >>> 0; c = (c - rot32(b, 24)) >>> 0;
  return [a, b, c];
}

function pgHashBytes(input: Buffer): number {
  let len = input.length >>> 0;
  let a = (0x9e3779b9 + len + 3923095) >>> 0;
  let b = a;
  let c = a;
  let offset = 0;

  while (len >= 12) {
    a = (a + input[offset + 0] + (input[offset + 1] << 8) + (input[offset + 2] << 16) + (input[offset + 3] << 24)) >>> 0;
    b = (b + input[offset + 4] + (input[offset + 5] << 8) + (input[offset + 6] << 16) + (input[offset + 7] << 24)) >>> 0;
    c = (c + input[offset + 8] + (input[offset + 9] << 8) + (input[offset + 10] << 16) + (input[offset + 11] << 24)) >>> 0;
    [a, b, c] = mix32(a, b, c);
    offset += 12;
    len -= 12;
  }

  switch (len) {
    case 11:
      c = (c + (input[offset + 10] << 24)) >>> 0;
      // falls through
    case 10:
      c = (c + (input[offset + 9] << 16)) >>> 0;
      // falls through
    case 9:
      c = (c + (input[offset + 8] << 8)) >>> 0;
      // falls through
    case 8:
      b = (b + (input[offset + 7] << 24)) >>> 0;
      // falls through
    case 7:
      b = (b + (input[offset + 6] << 16)) >>> 0;
      // falls through
    case 6:
      b = (b + (input[offset + 5] << 8)) >>> 0;
      // falls through
    case 5:
      b = (b + input[offset + 4]) >>> 0;
      // falls through
    case 4:
      a = (a + (input[offset + 3] << 24)) >>> 0;
      // falls through
    case 3:
      a = (a + (input[offset + 2] << 16)) >>> 0;
      // falls through
    case 2:
      a = (a + (input[offset + 1] << 8)) >>> 0;
      // falls through
    case 1:
      a = (a + input[offset + 0]) >>> 0;
      // falls through
    default:
      break;
  }

  [, , c] = final32(a, b, c);
  return c >>> 0;
}

function pgHashtextTieBreaker(input: string): number {
  const signed = pgHashBytes(Buffer.from(input, 'utf8')) | 0;
  const abs = signed === -2147483648 ? 2147483648 : Math.abs(signed);
  return abs % 10_000_000;
}

function computeLocalSortKey(agent: Pick<PrismaAgent, 'id' | 'trustTier' | 'qualityScore' | 'confidence'>): string {
  const trustTier = BigInt(agent.trustTier ?? 0);
  const qualityScore = BigInt(agent.qualityScore ?? 0);
  const confidence = BigInt(agent.confidence ?? 0);
  const tieBreaker = BigInt(pgHashtextTieBreaker(agent.id));
  return (
    trustTier * 1000200010000000n
    + qualityScore * 100010000000n
    + confidence * 10000000n
    + tieBreaker
  ).toString();
}

function normalizeNullableText(value: string | null | undefined): string | null {
  if (typeof value !== 'string') return value ?? null;
  return value.length > 0 ? value : null;
}

function resolveAgentFeedbackCount(
  baseCount: number | string | bigint | null | undefined,
  overrideCount?: number | string | bigint | null,
): number | null {
  return numericValue(overrideCount) ?? numericValue(baseCount);
}

function mapAgentToApi(a: AgentApiRow, overrides?: AgentFeedbackCountOverride): Record<string, unknown> {
  return {
    asset: a.id,
    agent_id: a.agentId !== null ? a.agentId.toString() : null,
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
    nft_name: normalizeNullableText(a.nftName),
    atom_enabled: a.atomEnabled,
    trust_tier: a.trustTier,
    quality_score: a.qualityScore,
    confidence: a.confidence,
    risk_score: a.riskScore,
    diversity_ratio: a.diversityRatio,
    feedback_count: resolveAgentFeedbackCount(a.feedbackCount, overrides?.feedbackCount),
    raw_avg_score: a.rawAvgScore,
    sort_key: computeLocalSortKey(a),
    block_slot: a.createdSlot !== null ? Number(a.createdSlot) : 0,
    tx_index: a.txIndex,
    event_ordinal: a.eventOrdinal,
    tx_signature: a.createdTxSignature,
    status: a.status,
    verified_at: a.verifiedAt?.toISOString() || null,
    verified_slot: a.verifiedSlot !== null ? Number(a.verifiedSlot) : null,
    created_at: a.createdAt.toISOString(),
    updated_at: a.updatedAt.toISOString(),
  };
}

function mapCollectionToApi(collection: PrismaCollection): Record<string, unknown> {
  return {
    collection_id: collection.collectionId != null ? collection.collectionId.toString() : null,
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

function checkpointEventCountToJson(value: bigint | number | null | undefined): number | null {
  if (value === null || value === undefined) return null;
  const numeric = typeof value === 'bigint' ? Number(value) : value;
  return Number.isFinite(numeric) ? numeric : null;
}

function extractAggregateCount(value: unknown): number {
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  if (typeof value === 'bigint') return Number(value);
  if (value && typeof value === 'object' && '_all' in value) {
    const nested = (value as { _all?: unknown })._all;
    if (typeof nested === 'number') return Number.isFinite(nested) ? nested : 0;
    if (typeof nested === 'bigint') return Number(nested);
  }
  return 0;
}

function roundNullableScore(value: number | null | undefined): number | null {
  if (value === null || value === undefined || !Number.isFinite(value)) return null;
  return Math.round(value);
}

function resolveFeedbackCount(aggregateCount: unknown, fallbackCount: number | null | undefined): number {
  const count = extractAggregateCount(aggregateCount);
  if (count > 0) return count;
  return Number.isFinite(fallbackCount ?? NaN) ? Number(fallbackCount) : 0;
}

function resolveAverageScore(
  feedbackCount: number,
  aggregateAvg: number | null | undefined,
  fallbackRawAvg: number | null | undefined,
): number | null {
  if (feedbackCount <= 0) return null;
  const roundedAggregate = roundNullableScore(aggregateAvg);
  if (roundedAggregate !== null) return roundedAggregate;
  if (fallbackRawAvg === null || fallbackRawAvg === undefined || !Number.isFinite(fallbackRawAvg)) return null;
  return Math.round(fallbackRawAvg);
}

function numericValue(value: string | number | bigint | null | undefined): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  if (typeof value === 'bigint') return Number(value);
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function isoStringOrNull(value: string | Date | null | undefined): string | null {
  if (value === null || value === undefined) return null;
  if (value instanceof Date) return value.toISOString();
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

type AgentReputationRow = {
  asset: string;
  owner: string;
  collection: string;
  nft_name: string | null;
  agent_uri: string | null;
  feedback_count: number;
  avg_score: number | null;
  positive_count: number;
  negative_count: number;
  validation_count: number;
};

type LocalLeaderboardRow = ReturnType<typeof mapAgentToApi> & {
  asset: string;
  sort_key: string;
};

type PoolAgentApiRow = {
  asset: string;
  owner: string;
  creator: string | null;
  agent_uri: string | null;
  agent_wallet: string | null;
  collection: string | null;
  canonical_col: string | null;
  col_locked: boolean | null;
  parent_asset: string | null;
  parent_creator: string | null;
  parent_locked: boolean | null;
  nft_name: string | null;
  atom_enabled: boolean | null;
  trust_tier: number | null;
  quality_score: number | null;
  confidence: number | null;
  risk_score: number | null;
  diversity_ratio: number | null;
  feedback_count: string | number | null;
  raw_avg_score: string | number | null;
  agent_id: string | number | null;
  status: string | null;
  verified_at: string | Date | null;
  verified_slot: string | number | null;
  created_at: string | Date | null;
  updated_at: string | Date | null;
  tx_signature: string | null;
  block_slot: string | number | null;
  tx_index: number | null;
  event_ordinal: number | null;
  sort_key: string | number | null;
};

async function loadLocalAgentDigestFeedbackCounts(
  prisma: PrismaClient,
  assetIds: string[],
): Promise<Map<string, number>> {
  const uniqueAssetIds = [...new Set(assetIds.filter((asset): asset is string => typeof asset === 'string' && asset.length > 0))];
  if (uniqueAssetIds.length === 0) return new Map();

  const model = (prisma as PrismaClient & {
    agentDigestCache?: {
      findMany?: (args: {
        where: { agentId: { in: string[] } };
        select: { agentId: true; feedbackCount: true };
      }) => Promise<Array<{ agentId: string; feedbackCount: bigint | number | null }>>;
    };
  }).agentDigestCache;

  if (!model?.findMany) return new Map();

  const rows = await model.findMany({
    where: { agentId: { in: uniqueAssetIds } },
    select: { agentId: true, feedbackCount: true },
  });

  return new Map(
    rows
      .map((row) => [row.agentId, resolveAgentFeedbackCount(null, row.feedbackCount)])
      .filter((entry): entry is [string, number] => entry[1] !== null),
  );
}

async function loadPoolAgentDigestFeedbackCounts(
  pool: Pool,
  assetIds: string[],
): Promise<Map<string, number>> {
  const uniqueAssetIds = [...new Set(assetIds.filter((asset): asset is string => typeof asset === 'string' && asset.length > 0))];
  if (uniqueAssetIds.length === 0) return new Map();

  const { rows } = await pool.query<{ agent_id: string; feedback_count: string | number | null }>(
    `SELECT agent_id, feedback_count::text AS feedback_count
     FROM agent_digest_cache
     WHERE agent_id = ANY($1::text[])`,
    [uniqueAssetIds]
  );

  return new Map(
    rows
      .map((row) => [row.agent_id, resolveAgentFeedbackCount(null, row.feedback_count)])
      .filter((entry): entry is [string, number] => entry[1] !== null),
  );
}

type PoolAgentTreeNodeRow = {
  asset: string;
  parent_asset: string | null;
  path: string[];
  depth: number;
};

function buildPoolAgentSelect(alias = 'a'): string {
  return `${alias}.asset,
          ${alias}.owner,
          ${alias}.creator,
          ${alias}.agent_uri,
          ${alias}.agent_wallet,
          ${alias}.collection,
          ${alias}.canonical_col,
          ${alias}.col_locked,
          ${alias}.parent_asset,
          ${alias}.parent_creator,
          ${alias}.parent_locked,
          ${alias}.nft_name,
          ${alias}.atom_enabled,
          ${alias}.trust_tier,
          ${alias}.quality_score,
          ${alias}.confidence,
          ${alias}.risk_score,
          ${alias}.diversity_ratio,
          ${alias}.feedback_count,
          ${alias}.raw_avg_score,
          ${alias}.agent_id,
          ${alias}.status,
          ${alias}.verified_at,
          ${alias}.verified_slot,
          ${alias}.created_at,
          ${alias}.updated_at,
          ${alias}.tx_signature,
          ${alias}.block_slot,
          ${alias}.tx_index,
          ${alias}.event_ordinal,
          ${alias}.sort_key`;
}

function buildPoolStatusPredicate(
  statusFilter: ReturnType<typeof buildStatusFilter>,
  params: unknown[],
  field: string,
): string {
  if (!statusFilter || isInvalidStatus(statusFilter)) return '';
  const statusValue = (statusFilter as Record<string, unknown>).status;
  if (typeof statusValue === 'string') {
    params.push(statusValue);
    return ` AND ${field} = $${params.length}::text`;
  }
  if (statusValue && typeof statusValue === 'object' && 'not' in statusValue) {
    const notValue = (statusValue as { not?: unknown }).not;
    if (typeof notValue === 'string') {
      params.push(notValue);
      return ` AND ${field} != $${params.length}::text`;
    }
  }
  return '';
}

function mapPoolAgentToApi(row: PoolAgentApiRow, overrides?: AgentFeedbackCountOverride): Record<string, unknown> {
  return {
    asset: row.asset,
    agent_id: row.agent_id !== null && row.agent_id !== undefined ? String(row.agent_id) : null,
    owner: row.owner,
    creator: row.creator,
    agent_uri: row.agent_uri,
    agent_wallet: row.agent_wallet,
    collection: row.collection,
    collection_pointer: row.canonical_col,
    col_locked: row.col_locked,
    parent_asset: row.parent_asset,
    parent_creator: row.parent_creator,
    parent_locked: row.parent_locked,
    nft_name: normalizeNullableText(row.nft_name),
    atom_enabled: row.atom_enabled,
    trust_tier: numericValue(row.trust_tier),
    quality_score: numericValue(row.quality_score),
    confidence: numericValue(row.confidence),
    risk_score: numericValue(row.risk_score),
    diversity_ratio: numericValue(row.diversity_ratio),
    feedback_count: resolveAgentFeedbackCount(row.feedback_count, overrides?.feedbackCount),
    raw_avg_score: numericValue(row.raw_avg_score),
    sort_key: row.sort_key !== null && row.sort_key !== undefined ? String(row.sort_key) : null,
    block_slot: numericValue(row.block_slot) ?? 0,
    tx_index: row.tx_index,
    event_ordinal: row.event_ordinal,
    tx_signature: row.tx_signature,
    status: row.status,
    verified_at: isoStringOrNull(row.verified_at),
    verified_slot: numericValue(row.verified_slot),
    created_at: isoStringOrNull(row.created_at),
    updated_at: isoStringOrNull(row.updated_at),
  };
}

function mapAgentToLeaderboardEntry(
  agent: Pick<PrismaAgent,
    'id'
    | 'owner'
    | 'collection'
    | 'nftName'
    | 'uri'
    | 'trustTier'
    | 'qualityScore'
    | 'confidence'
    | 'riskScore'
    | 'diversityRatio'
    | 'feedbackCount'>
): LeaderboardEntry {
  return {
    asset: agent.id,
    owner: agent.owner,
    collection: agent.collection,
    nft_name: normalizeNullableText(agent.nftName),
    agent_uri: agent.uri,
    trust_tier: agent.trustTier,
    quality_score: agent.qualityScore,
    confidence: agent.confidence,
    risk_score: agent.riskScore,
    diversity_ratio: agent.diversityRatio,
    feedback_count: agent.feedbackCount,
    sort_key: computeLocalSortKey(agent),
  };
}

function parsePostgRESTInt(value: unknown): number | undefined | null {
  const comparison = parsePostgRESTComparison(value);
  if (!comparison) return undefined;
  if (comparison.op !== 'eq') return null;
  const parsed = Number.parseInt(comparison.value, 10);
  return Number.isFinite(parsed) ? parsed : null;
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
  let statusComparison: PostgRESTComparison | undefined;
  const queryRecord = req.query as Record<string, unknown>;
  for (const key of keys) {
    statusComparison = parsePostgRESTComparison(queryRecord[key]);
    if (statusComparison) break;
  }
  const includeOrphaned = safeQueryString(req.query.includeOrphaned) === 'true';

  if (statusComparison?.value) {
    if (!VALID_STATUSES.has(statusComparison.value)) {
      return { _invalid: true };
    }
    if (statusComparison.op === 'neq') {
      return { [fieldName]: { not: statusComparison.value } };
    }
    return { [fieldName]: statusComparison.value };
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

const PROXY_RESPONSE_HEADER_ALLOWLIST = [
  'content-type',
  'content-range',
  'content-location',
  'location',
  'etag',
  'cache-control',
  'preference-applied',
] as const;

const PROXY_REQUEST_HEADER_BLOCKLIST = new Set([
  'host',
  'content-length',
  'connection',
  'transfer-encoding',
  'upgrade',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
]);

function resolveSupabaseRestBaseUrl(): string | null {
  const postgrestRaw = typeof process.env.POSTGREST_URL === 'string' ? process.env.POSTGREST_URL.trim() : '';
  if (postgrestRaw) {
    return postgrestRaw.replace(/\/+$/, '');
  }

  const supabaseRaw = typeof process.env.SUPABASE_URL === 'string' ? process.env.SUPABASE_URL.trim() : '';
  if (supabaseRaw) {
    const normalized = supabaseRaw.replace(/\/+$/, '');
    return normalized.endsWith('/rest/v1') ? normalized : `${normalized}/rest/v1`;
  }

  if (!config.supabaseUrl) return null;
  const trimmed = config.supabaseUrl.trim();
  if (!trimmed) return null;
  const normalized = trimmed.replace(/\/+$/, '');
  return normalized.endsWith('/rest/v1') ? normalized : `${normalized}/rest/v1`;
}

const ARCHIVED_VALIDATIONS_ERROR =
  'Validation endpoints are archived and no longer exposed. /rest/v1/validations has been retired.';

const REST_PROXY_PATH_ALLOWLIST = [
  '/agents',
  '/agent_reputation',
  '/feedbacks',
  '/responses',
  '/feedback_responses',
  '/revocations',
  '/collections',
  '/collection_stats',
  '/stats',
  '/global_stats',
  '/metadata',
  '/leaderboard',
  '/rpc/get_collection_agents',
  '/rpc/get_leaderboard',
] as const;

const REST_PROXY_LOCAL_COMPAT_PATHS = [
  '/agent_reputation',
  '/leaderboard',
  '/collections',
  '/collection_pointers',
  '/collection_stats',
  '/collection_asset_count',
  '/collection_assets',
  '/checkpoints',
  '/events',
  '/global_stats',
  '/rpc/get_collection_agents',
  '/rpc/get_leaderboard',
  '/stats',
  '/verify/replay',
  '/agents/children',
  '/agents/tree',
  '/agents/lineage',
] as const;

const REST_PROXY_STATUS_DEFAULT_PATHS = new Set([
  '/feedbacks',
  '/responses',
  '/feedback_responses',
  '/revocations',
]);

const REST_PROXY_READ_POST_PATHS = new Set([
  '/rpc/get_leaderboard',
]);

function isAllowedRestProxyPath(pathname: string): boolean {
  return REST_PROXY_PATH_ALLOWLIST.some((allowed) => pathname === allowed || pathname.startsWith(`${allowed}/`));
}

function isLocalRestCompatPath(pathname: string): boolean {
  return REST_PROXY_LOCAL_COMPAT_PATHS.some((allowed) => pathname === allowed || pathname.startsWith(`${allowed}/`));
}

function isReplayChainType(value: string | undefined): value is ReplayChainType {
  return value === 'feedback' || value === 'response' || value === 'revoke';
}

function hasUnsafeRestProxyPath(pathname: string): boolean {
  const lowered = pathname.toLowerCase();
  return (
    pathname.includes("..") ||
    lowered.includes("%2e") ||
    lowered.includes("%2f") ||
    lowered.includes("%5c")
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

async function normalizeAgentsProxyPayload(
  payload: Uint8Array,
  contentType: string | null,
  pool?: Pool | null,
): Promise<Uint8Array> {
  if (!contentType || !contentType.toLowerCase().includes('application/json')) {
    return payload;
  }

  try {
    const parsed: unknown = JSON.parse(Buffer.from(payload).toString('utf8'));
    let changed = false;
    const rows = Array.isArray(parsed)
      ? parsed.filter(isRecord)
      : isRecord(parsed)
        ? [parsed]
        : [];
    const assets = rows
      .map((row) => (typeof row.asset === 'string' ? row.asset : null))
      .filter((asset): asset is string => asset !== null);
    const digestFeedbackCounts = pool
      ? await loadPoolAgentDigestFeedbackCounts(pool, assets)
      : new Map<string, number>();

    const normalizeRow = (row: Record<string, unknown>): void => {
      if (Object.prototype.hasOwnProperty.call(row, 'canonical_col') && !Object.prototype.hasOwnProperty.call(row, 'collection_pointer')) {
        row.collection_pointer = row.canonical_col ?? null;
        changed = true;
      }
      if (typeof row.asset === 'string') {
        const feedbackCount = digestFeedbackCounts.get(row.asset);
        const resolvedFeedbackCount = resolveAgentFeedbackCount(
          row.feedback_count as number | string | bigint | null | undefined,
          feedbackCount,
        );
        if (resolvedFeedbackCount !== null && row.feedback_count !== resolvedFeedbackCount) {
          row.feedback_count = resolvedFeedbackCount;
          changed = true;
        }
      }
      const normalizedName = normalizeNullableText(typeof row.nft_name === 'string' ? row.nft_name : null);
      if (row.nft_name !== normalizedName) {
        row.nft_name = normalizedName;
        changed = true;
      }
    };

    if (Array.isArray(parsed)) {
      for (const item of rows) {
        normalizeRow(item);
      }
      return changed ? Buffer.from(JSON.stringify(parsed)) : payload;
    }

    if (isRecord(parsed)) {
      normalizeRow(parsed);
      return changed ? Buffer.from(JSON.stringify(parsed)) : payload;
    }

    return payload;
  } catch {
    return payload;
  }
}

function buildSupabaseProxyHeaders(req: Request): Headers {
  const upstreamHeaders = new Headers();
  for (const [name, value] of Object.entries(req.headers)) {
    const lowerName = name.toLowerCase();
    if (!value || PROXY_REQUEST_HEADER_BLOCKLIST.has(lowerName)) continue;
    if (Array.isArray(value)) {
      upstreamHeaders.set(name, value.join(','));
    } else {
      upstreamHeaders.set(name, value);
    }
  }

  if (!upstreamHeaders.has('apikey') && config.supabaseKey) {
    upstreamHeaders.set('apikey', config.supabaseKey);
  }
  if (!upstreamHeaders.has('authorization') && config.supabaseKey) {
    upstreamHeaders.set('authorization', `Bearer ${config.supabaseKey}`);
  }

  return upstreamHeaders;
}

function buildSupabaseServiceHeaders(): Headers {
  const upstreamHeaders = new Headers();
  upstreamHeaders.set('accept', 'application/json');
  if (config.supabaseKey) {
    upstreamHeaders.set('apikey', config.supabaseKey);
    upstreamHeaders.set('authorization', `Bearer ${config.supabaseKey}`);
  }
  return upstreamHeaders;
}

function parseContentRangeTotal(contentRange: string | null): number | null {
  if (!contentRange) return null;
  const match = contentRange.match(/\/(\d+)$/);
  if (!match) return null;
  const parsed = Number(match[1]);
  if (!Number.isFinite(parsed) || parsed < 0) return null;
  return parsed;
}

async function fetchProxyTableCount(
  supabaseRestBaseUrl: string,
  upstreamHeaders: Headers,
  endpoint: string,
  params: URLSearchParams,
): Promise<number> {
  const query = new URLSearchParams(params.toString());
  if (!query.has('limit')) query.set('limit', '1');

  const headers = new Headers(upstreamHeaders);
  headers.set('Prefer', 'count=exact');
  headers.set('Range', '0-0');

  const upstreamUrl = `${supabaseRestBaseUrl}${endpoint}${query.toString() ? `?${query.toString()}` : ''}`;
  const response = await fetch(upstreamUrl, { method: 'GET', headers });
  const payload = Buffer.from(await response.arrayBuffer());

  if (!response.ok) {
    throw new Error(`Count query failed for ${endpoint}: HTTP ${response.status} ${payload.toString('utf8')}`);
  }

  const contentRange = response.headers.get('content-range');
  const total = parseContentRangeTotal(contentRange);
  if (total !== null) return total;

  try {
    const parsed: unknown = JSON.parse(payload.toString('utf8'));
    if (Array.isArray(parsed)) return parsed.length;
  } catch {
    // fall through
  }

  return 0;
}

async function fetchFallbackGlobalStats(
  supabaseRestBaseUrl: string,
  upstreamHeaders: Headers,
  includeOrphaned: boolean,
): Promise<Array<Record<string, number | null>>> {
  const agentParams = new URLSearchParams({ select: 'asset' });
  if (!includeOrphaned) agentParams.append('status', 'neq.ORPHANED');

  const feedbackParams = new URLSearchParams({ select: 'id' });
  if (!includeOrphaned) feedbackParams.append('status', 'neq.ORPHANED');

  const collectionParams = new URLSearchParams({
    select: 'col',
  });

  const [totalAgents, totalFeedbacks, totalCollections] = await Promise.all([
    fetchProxyTableCount(supabaseRestBaseUrl, upstreamHeaders, '/agents', agentParams),
    fetchProxyTableCount(supabaseRestBaseUrl, upstreamHeaders, '/feedbacks', feedbackParams),
    fetchProxyTableCount(supabaseRestBaseUrl, upstreamHeaders, '/collection_pointers', collectionParams),
  ]);

  return [{
    total_agents: totalAgents,
    total_feedbacks: totalFeedbacks,
    total_collections: totalCollections,
    platinum_agents: 0,
    gold_agents: 0,
    avg_quality: null,
  }];
}

function isMissingGlobalStatsRelationError(status: number, payload: Uint8Array, contentType: string | null): boolean {
  if (status !== 404 || !contentType || !contentType.toLowerCase().includes('application/json')) {
    return false;
  }

  try {
    const parsed = JSON.parse(Buffer.from(payload).toString('utf8')) as { message?: string; code?: string };
    const message = String(parsed?.message ?? '');
    if (message.includes('global_stats')) return true;
    return parsed?.code === '42P01' || parsed?.code === 'PGRST205';
  } catch {
    return false;
  }
}

export function createApiServer(options: ApiServerOptions): Express {
  const wantsRest = config.apiMode !== 'graphql';
  const wantsGraphql = config.apiMode !== 'rest' && config.enableGraphql;
  const supabaseRestBaseUrl = resolveSupabaseRestBaseUrl();
  const hasSupabaseKey = typeof config.supabaseKey === 'string' && config.supabaseKey.trim().length > 0;
  const restProxyMissingKey = wantsRest && !options.prisma && !!options.pool && !!supabaseRestBaseUrl && !hasSupabaseKey;
  const restProxyEnabled = wantsRest && !options.prisma && !!options.pool && !!supabaseRestBaseUrl && hasSupabaseKey;
  const restEnabled = wantsRest && (!!options.prisma || restProxyEnabled);
  const graphqlEnabled = wantsGraphql && !!options.pool;

  if (config.apiMode === 'rest' && !restEnabled) {
    throw new Error(
      'REST mode requires local Prisma client or Supabase PostgREST proxy auth (SUPABASE_URL + SUPABASE_KEY, or POSTGREST_URL + POSTGREST_TOKEN)'
    );
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
  const isReady = options.isReady ?? (() => true);
  const isCaughtUp = options.isCaughtUp ?? (() => true);
  const graphqlState = {
    mounted: !graphqlEnabled || !options.pool,
  };
  const restProxyReadyState = {
    lastCheckedAt: 0,
    lastHealthy: !restProxyEnabled,
    inFlight: null as Promise<boolean> | null,
  };
  const hasReadyStateBackend = !!prisma || !!(options.pool && typeof (options.pool as any).query === 'function');
  const poolReadyState = {
    lastCheckedAt: 0,
    lastHealthy: true,
    inFlight: null as Promise<boolean> | null,
  };
  const readyDetailsState = {
    lastCheckedAt: 0,
    lastValue: null as ReadyStateSnapshot | null,
    inFlight: null as Promise<ReadyStateSnapshot | null> | null,
  };
  const checkRestProxyReady = options.checkRestProxyReady ?? (async (): Promise<boolean> => {
    if (!restProxyEnabled || !supabaseRestBaseUrl) {
      return true;
    }
    const now = Date.now();
    if (now - restProxyReadyState.lastCheckedAt < REST_PROXY_READY_CACHE_TTL_MS) {
      return restProxyReadyState.lastHealthy;
    }
    if (restProxyReadyState.inFlight) {
      return await restProxyReadyState.inFlight;
    }
    restProxyReadyState.inFlight = (async () => {
      try {
        const headers = buildSupabaseServiceHeaders();
        const [agentsResponse, leaderboardResponse] = await Promise.all([
          fetch(
            `${supabaseRestBaseUrl}/agents?select=asset&limit=1`,
            { method: 'GET', headers },
          ),
          fetch(
            `${supabaseRestBaseUrl}/leaderboard?select=asset&limit=1`,
            { method: 'GET', headers },
          ),
        ]);
        restProxyReadyState.lastHealthy = agentsResponse.ok && leaderboardResponse.ok;
      } catch {
        restProxyReadyState.lastHealthy = false;
      } finally {
        restProxyReadyState.lastCheckedAt = Date.now();
        restProxyReadyState.inFlight = null;
      }
      return restProxyReadyState.lastHealthy;
    })();
    return await restProxyReadyState.inFlight;
  });
  const checkPoolReady = options.checkPoolReady ?? (async (): Promise<boolean> => {
    const pool = options.pool as Pool | null | undefined;
    if (!pool || typeof (pool as any).query !== 'function') {
      return true;
    }
    const now = Date.now();
    if (now - poolReadyState.lastCheckedAt < POOL_READY_CACHE_TTL_MS) {
      return poolReadyState.lastHealthy;
    }
    if (poolReadyState.inFlight) {
      return await poolReadyState.inFlight;
    }
    poolReadyState.inFlight = (async () => {
      try {
        await pool.query('SELECT 1');
        poolReadyState.lastHealthy = true;
      } catch (error) {
        logger.debug({ error }, 'Failed to probe pool readiness');
        poolReadyState.lastHealthy = false;
      } finally {
        poolReadyState.lastCheckedAt = Date.now();
        poolReadyState.inFlight = null;
      }
      return poolReadyState.lastHealthy;
    })();
    return await poolReadyState.inFlight;
  });

  const loadReadyStateDetails = async (): Promise<ReadyStateSnapshot | null> => {
    const now = Date.now();
    if (now - readyDetailsState.lastCheckedAt < READY_DETAILS_CACHE_TTL_MS) {
      return readyDetailsState.lastValue;
    }
    if (readyDetailsState.inFlight) {
      return await readyDetailsState.inFlight;
    }
    readyDetailsState.inFlight = (async () => {
      try {
        if (prisma && typeof prisma.agent?.count === 'function') {
          const [
            mainState,
            agents,
            feedbacks,
            responses,
            revocations,
            collections,
            pendingAgents,
            pendingFeedbacks,
            pendingResponses,
            pendingRevocations,
            pendingMetadata,
            pendingRegistries,
            pendingValidations,
            orphanAgents,
            orphanFeedbacks,
            orphanResponses,
            orphanRevocations,
            orphanMetadata,
            orphanRegistries,
            orphanValidations,
            orphanFeedbackStaging,
            orphanResponseStaging,
          ] = await Promise.all([
            prisma.indexerState.findUnique({
              where: { id: 'main' },
              select: { lastSlot: true, lastTxIndex: true },
            }),
            prisma.agent.count(),
            prisma.feedback.count(),
            prisma.feedbackResponse.count(),
            prisma.revocation.count(),
            prisma.collection.count(),
            prisma.agent.count({ where: { status: 'PENDING' } }),
            prisma.feedback.count({ where: { status: 'PENDING' } }),
            prisma.feedbackResponse.count({ where: { status: 'PENDING' } }),
            prisma.revocation.count({ where: { status: 'PENDING' } }),
            prisma.agentMetadata.count({ where: { status: 'PENDING' } }),
            prisma.registry.count({ where: { status: 'PENDING' } }),
            typeof (prisma as any).validation?.count === 'function'
              ? (prisma as any).validation.count({ where: { chainStatus: 'PENDING' } })
              : Promise.resolve(0),
            prisma.agent.count({ where: { status: 'ORPHANED' } }),
            prisma.feedback.count({ where: { status: 'ORPHANED' } }),
            prisma.feedbackResponse.count({ where: { status: 'ORPHANED' } }),
            prisma.revocation.count({ where: { status: 'ORPHANED' } }),
            prisma.agentMetadata.count({ where: { status: 'ORPHANED' } }),
            prisma.registry.count({ where: { status: 'ORPHANED' } }),
            typeof (prisma as any).validation?.count === 'function'
              ? (prisma as any).validation.count({ where: { chainStatus: 'ORPHANED' } })
              : Promise.resolve(0),
            typeof (prisma as any).orphanFeedback?.count === 'function'
              ? (prisma as any).orphanFeedback.count()
              : Promise.resolve(0),
            typeof (prisma as any).orphanResponse?.count === 'function'
              ? (prisma as any).orphanResponse.count()
              : Promise.resolve(0),
          ]);

          readyDetailsState.lastValue = {
            mainSlot: mainState?.lastSlot != null ? Number(mainState.lastSlot) : null,
            mainTxIndex: mainState?.lastTxIndex ?? null,
            counts: { agents, feedbacks, responses, revocations, collections },
            pending: {
              agents: pendingAgents,
              feedbacks: pendingFeedbacks,
              responses: pendingResponses,
              revocations: pendingRevocations,
              metadata: pendingMetadata,
              registries: pendingRegistries,
              validations: pendingValidations,
            },
            orphans: {
              agents: orphanAgents,
              feedbacks: orphanFeedbacks + orphanFeedbackStaging,
              responses: orphanResponses + orphanResponseStaging,
              revocations: orphanRevocations,
              metadata: orphanMetadata,
              registries: orphanRegistries,
              validations: orphanValidations,
            },
          };
          return readyDetailsState.lastValue;
        }

        const pool = options.pool as Pool | null | undefined;
        if (pool && typeof (pool as any).query === 'function') {
          const { rows } = await pool.query<{
            main_slot: string | null;
            main_tx_index: number | null;
            agents: string | number;
            feedbacks: string | number;
            responses: string | number;
            revocations: string | number;
            collections: string | number;
            pending_feedbacks: string | number;
            pending_responses: string | number;
            pending_revocations: string | number;
            pending_agents: string | number;
            pending_metadata: string | number;
            pending_registries: string | number;
            pending_validations: string | number;
            orphan_feedbacks: string | number;
            orphan_responses: string | number;
            orphan_revocations: string | number;
            orphan_agents: string | number;
            orphan_metadata: string | number;
            orphan_registries: string | number;
            orphan_validations: string | number;
            staging_orphan_feedbacks: string | number;
            staging_orphan_responses: string | number;
          }>(
            `SELECT
               (SELECT last_slot::text FROM indexer_state WHERE id = 'main') AS main_slot,
               (SELECT last_tx_index FROM indexer_state WHERE id = 'main') AS main_tx_index,
               (SELECT COUNT(*) FROM agents) AS agents,
               (SELECT COUNT(*) FROM feedbacks) AS feedbacks,
               (SELECT COUNT(*) FROM feedback_responses) AS responses,
               (SELECT COUNT(*) FROM revocations) AS revocations,
               (SELECT COUNT(*) FROM collection_pointers) AS collections,
               (SELECT COUNT(*) FROM agents WHERE status = 'PENDING') AS pending_agents,
               (SELECT COUNT(*) FROM feedbacks WHERE status = 'PENDING') AS pending_feedbacks,
               (SELECT COUNT(*) FROM feedback_responses WHERE status = 'PENDING') AS pending_responses,
               (SELECT COUNT(*) FROM revocations WHERE status = 'PENDING') AS pending_revocations,
               (SELECT COUNT(*) FROM metadata WHERE status = 'PENDING') AS pending_metadata,
               (SELECT COUNT(*) FROM collections WHERE status = 'PENDING') AS pending_registries,
               (SELECT COUNT(*) FROM validations WHERE chain_status = 'PENDING') AS pending_validations,
               (SELECT COUNT(*) FROM agents WHERE status = 'ORPHANED') AS orphan_agents,
               (SELECT COUNT(*) FROM feedbacks WHERE status = 'ORPHANED') AS orphan_feedbacks,
               (SELECT COUNT(*) FROM feedback_responses WHERE status = 'ORPHANED') AS orphan_responses,
               (SELECT COUNT(*) FROM revocations WHERE status = 'ORPHANED') AS orphan_revocations,
               (SELECT COUNT(*) FROM metadata WHERE status = 'ORPHANED') AS orphan_metadata,
               (SELECT COUNT(*) FROM collections WHERE status = 'ORPHANED') AS orphan_registries,
               (SELECT COUNT(*) FROM validations WHERE chain_status = 'ORPHANED') AS orphan_validations,
               (SELECT COUNT(*) FROM orphan_feedbacks) AS staging_orphan_feedbacks,
               (SELECT COUNT(*) FROM orphan_responses) AS staging_orphan_responses`
          );

          const row = rows[0];
          readyDetailsState.lastValue = {
            mainSlot: row?.main_slot != null ? Number(row.main_slot) : null,
            mainTxIndex: row?.main_tx_index ?? null,
            counts: {
              agents: Number(row?.agents ?? 0),
              feedbacks: Number(row?.feedbacks ?? 0),
              responses: Number(row?.responses ?? 0),
              revocations: Number(row?.revocations ?? 0),
              collections: Number(row?.collections ?? 0),
            },
            pending: {
              agents: Number(row?.pending_agents ?? 0),
              feedbacks: Number(row?.pending_feedbacks ?? 0),
              responses: Number(row?.pending_responses ?? 0),
              revocations: Number(row?.pending_revocations ?? 0),
              metadata: Number(row?.pending_metadata ?? 0),
              registries: Number(row?.pending_registries ?? 0),
              validations: Number(row?.pending_validations ?? 0),
            },
            orphans: {
              agents: Number(row?.orphan_agents ?? 0),
              feedbacks: Number(row?.orphan_feedbacks ?? 0) + Number(row?.staging_orphan_feedbacks ?? 0),
              responses: Number(row?.orphan_responses ?? 0) + Number(row?.staging_orphan_responses ?? 0),
              revocations: Number(row?.orphan_revocations ?? 0),
              metadata: Number(row?.orphan_metadata ?? 0),
              registries: Number(row?.orphan_registries ?? 0),
              validations: Number(row?.orphan_validations ?? 0),
            },
          };
          return readyDetailsState.lastValue;
        }
      } catch (error) {
        logger.debug({ error }, 'Failed to load /ready state details');
      } finally {
        readyDetailsState.lastCheckedAt = Date.now();
        readyDetailsState.inFlight = null;
      }
      readyDetailsState.lastValue = null;
      return null;
    })();
    return await readyDetailsState.inFlight;
  };

  const trustProxyRaw = process.env.TRUST_PROXY;
  let trustProxy: string | number | boolean = false;
  if (trustProxyRaw !== undefined) {
    if (trustProxyRaw === 'true') trustProxy = true;
    else if (trustProxyRaw === 'false') trustProxy = false;
    else if (/^\d+$/.test(trustProxyRaw)) trustProxy = Number(trustProxyRaw);
    else trustProxy = trustProxyRaw;
  }
  app.set('trust proxy', trustProxy);

  app.use(express.json({ limit: '100kb' }));

  // CORS - allow configurable origins
  const parsedAllowedOrigins = process.env.CORS_ORIGINS
    ?.split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  const allowedOrigins = parsedAllowedOrigins && parsedAllowedOrigins.length > 0
    ? parsedAllowedOrigins
    : ['*'];
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

  app.get('/ready', async (_req: Request, res: Response) => {
    const details = await loadReadyStateDetails();
    const base = {
      phase: 'bootstrapping' as 'bootstrapping' | 'catching_up' | 'live',
      state: details,
    };
    if (!isReady() || !graphqlState.mounted) {
      res.status(503).json({ status: 'starting', ...base });
      return;
    }
    if (!(await checkRestProxyReady())) {
      res.status(503).json({ status: 'starting', ...base });
      return;
    }
    if (!(await checkPoolReady())) {
      res.status(503).json({ status: 'starting', ...base });
      return;
    }
    if (hasReadyStateBackend && !details) {
      res.status(503).json({ status: 'starting', ...base });
      return;
    }
    const hasBacklog = details
      ? Object.values(details.pending).some((count) => count > 0)
        || Object.values(details.orphans).some((count) => count > 0)
      : true;
    const phase = !details
      ? 'catching_up'
      : hasBacklog
        ? 'catching_up'
        : isCaughtUp()
          ? 'live'
          : 'catching_up';
    res.json({ status: 'ready', phase, state: details });
  });

  if (config.metricsEndpointEnabled) {
    app.get('/metrics', (_req: Request, res: Response) => {
      res.setHeader('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
      res.send(renderIntegrityMetrics());
    });
  }

  app.use(async (req: Request, res: Response, next: NextFunction) => {
    if (req.path === '/health' || req.path === '/ready' || req.path === '/metrics') {
      next();
      return;
    }

    if (!isReady() || !graphqlState.mounted) {
      res.setHeader('Retry-After', '5');
      res.status(503).json({ error: 'Indexer bootstrap in progress. Retry shortly.' });
      return;
    }

    const isGraphqlRequest = req.path === '/graphql' || req.path === '/v2/graphql';
    if (isGraphqlRequest && !(await checkPoolReady())) {
      res.setHeader('Retry-After', '5');
      res.status(503).json({ error: 'Indexer bootstrap in progress. Retry shortly.' });
      return;
    }

    next();
  });

  // Global rate limiting
  const limiter = rateLimit({
    windowMs: RATE_LIMIT_WINDOW_MS,
    max: RATE_LIMIT_MAX_REQUESTS,
    message: { error: 'Too many requests, please try again later' },
    standardHeaders: true,
    legacyHeaders: false,
    skip: (req) => req.path === '/graphql' || req.path === '/v2/graphql',
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

  // Validation indexing API was archived. Keep this hard-block before any proxy/REST handlers.
  app.all(/^\/rest\/v1\/validations(?:\/.*)?$/, (_req: Request, res: Response) => {
    res.status(410).json({ error: ARCHIVED_VALIDATIONS_ERROR });
  });

  if (restProxyEnabled && supabaseRestBaseUrl) {
    app.use('/rest/v1', async (req: Request, res: Response, next: NextFunction) => {
      try {
        const upstreamRawPath = req.url.startsWith('/') ? req.url : `/${req.url}`;
        const [upstreamRawPathname, upstreamRawQuery = ''] = upstreamRawPath.split('?', 2);
        const method = req.method.toUpperCase();

        const upstreamPathname = upstreamRawPathname === '/stats'
          ? '/global_stats'
          : upstreamRawPathname === '/responses'
            ? '/feedback_responses'
            : upstreamRawPathname === '/agents/'
              ? '/agents'
            : upstreamRawPathname;
        let upstreamQuery = upstreamRawQuery;
        const isGlobalStatsProxyPath =
          upstreamRawPathname === '/stats'
          || upstreamRawPathname === '/global_stats'
          || upstreamPathname === '/global_stats';
        if (upstreamPathname === '/agents') {
          const params = new URLSearchParams(upstreamRawQuery);
          const hasCanonicalCol = params.has('canonical_col');
          const hasCollectionPointer = params.has('collection_pointer');
          const collectionPointer = params.get('collection_pointer');
          const canonicalCol = params.get('canonical_col');
          const resolvedCollectionPointer = (canonicalCol && canonicalCol.trim().length > 0)
            ? canonicalCol
            : collectionPointer;
          if ((hasCanonicalCol || hasCollectionPointer) && (!resolvedCollectionPointer || resolvedCollectionPointer.trim().length === 0)) {
            if (method === 'GET' || method === 'HEAD') {
              res.json([]);
              return;
            }
          }
          if (resolvedCollectionPointer && resolvedCollectionPointer.trim().length > 0) {
            // Backward-compatible input alias: canonical_col -> collection_pointer.
            const normalized = normalizeCollectionPointerFilterValue(resolvedCollectionPointer);
            if (normalized === 'in.()') {
              if (method === 'GET' || method === 'HEAD') {
                res.json([]);
                return;
              }
            }
            params.set('canonical_col', normalized);
          } else {
            params.delete('canonical_col');
          }
          params.delete('collection_pointer');
          const includeOrphaned = params.get('includeOrphaned') === 'true';
          params.delete('includeOrphaned');
          if (!params.has('status') && !includeOrphaned) {
            params.append('status', 'neq.ORPHANED');
            upstreamQuery = params.toString();
          } else {
            upstreamQuery = params.toString();
          }
        } else if (REST_PROXY_STATUS_DEFAULT_PATHS.has(upstreamPathname)) {
          const params = new URLSearchParams(upstreamRawQuery);
          const includeOrphaned = params.get('includeOrphaned') === 'true';
          params.delete('includeOrphaned');
          if (!includeOrphaned && !params.has('status')) {
            params.append('status', 'neq.ORPHANED');
          }
          upstreamQuery = params.toString();
        } else if (upstreamPathname === '/global_stats') {
          const params = new URLSearchParams(upstreamRawQuery);
          // PostgREST global_stats view has fixed non-orphaned semantics.
          // Drop compatibility toggle to avoid forwarding unknown column filters.
          params.delete('includeOrphaned');
          upstreamQuery = params.toString();
        }

        const upstreamPath = upstreamQuery ? `${upstreamPathname}?${upstreamQuery}` : upstreamPathname;
        const isVerificationStatsPath =
          upstreamPathname === '/stats/verification' || upstreamPathname === '/stats/verification/';
        const isLocalLeaderboardIncludeOrphaned =
          upstreamPathname === '/leaderboard' && safeQueryString(req.query.includeOrphaned) === 'true';
        if (isVerificationStatsPath) {
          if (method === 'OPTIONS') {
            res.status(204).end();
            return;
          }
          if (method !== 'GET' && method !== 'HEAD') {
            res.status(405).json({ error: 'REST proxy is read-only. Mutating methods are disabled.' });
            return;
          }
          next();
          return;
        }
        if (isLocalLeaderboardIncludeOrphaned) {
          next();
          return;
        }
        if (isLocalRestCompatPath(upstreamPathname)) {
          next();
          return;
        }

        if (method === 'OPTIONS') {
          res.status(204).end();
          return;
        }
        const allowReadPost = REST_PROXY_READ_POST_PATHS.has(upstreamPathname);
        const isAllowedReadMethod = method === 'GET' || method === 'HEAD' || (allowReadPost && method === 'POST');
        if (!isAllowedReadMethod) {
          res.status(405).json({ error: 'REST proxy is read-only. Mutating methods are disabled.' });
          return;
        }

        const upstreamHeaders = buildSupabaseProxyHeaders(req);

        let body: string | Buffer | Uint8Array | undefined;
        if (method !== 'GET' && method !== 'HEAD') {
          if (typeof req.body === 'string' || Buffer.isBuffer(req.body) || req.body instanceof Uint8Array) {
            body = req.body;
          } else if (req.body && typeof req.body === 'object') {
            body = JSON.stringify(req.body);
          } else if (req.readable) {
            const chunks: Buffer[] = [];
            for await (const chunk of req) {
              chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
            }
            if (chunks.length > 0) {
              body = Buffer.concat(chunks);
            }
          }
        }

        if (hasUnsafeRestProxyPath(upstreamPathname)) {
          res.status(403).json({ error: 'REST proxy path not allowed' });
          return;
        }
        if (!isAllowedRestProxyPath(upstreamPathname)) {
          res.status(403).json({ error: 'REST proxy path not allowed' });
          return;
        }
        const upstreamUrl = `${supabaseRestBaseUrl}${upstreamPath}`;
        const upstreamRes = await fetch(upstreamUrl, {
          method,
          headers: upstreamHeaders,
          body: body as BodyInit | undefined,
        });

        let payload: Uint8Array = Buffer.from(await upstreamRes.arrayBuffer());
        if (isGlobalStatsProxyPath && isMissingGlobalStatsRelationError(
          upstreamRes.status,
          payload,
          upstreamRes.headers.get('content-type'),
        )) {
          const params = new URLSearchParams(upstreamRawQuery);
          const includeOrphaned = params.get('includeOrphaned') === 'true';
          const fallbackStats = await fetchFallbackGlobalStats(
            supabaseRestBaseUrl,
            upstreamHeaders,
            includeOrphaned,
          );
          res.status(200).json(fallbackStats);
          return;
        }

        for (const header of PROXY_RESPONSE_HEADER_ALLOWLIST) {
          const value = upstreamRes.headers.get(header);
          if (value) {
            res.setHeader(header, value);
          }
        }

        if (upstreamPathname === '/agents') {
          payload = await normalizeAgentsProxyPayload(payload, upstreamRes.headers.get('content-type'), options.pool);
        }
        res.status(upstreamRes.status).send(payload);
      } catch (error) {
        logger.error({ error }, 'Supabase REST proxy request failed');
        res.status(502).json({ error: 'Supabase REST backend unavailable' });
      }
    });
  } else if (restProxyMissingKey) {
    app.use('/rest/v1', (_req: Request, res: Response) => {
      res.status(503).json({
        error: 'REST proxy unavailable: SUPABASE_KEY (or POSTGREST_TOKEN) is not configured. Use /v2/graphql or set SUPABASE_KEY/POSTGREST_TOKEN.',
      });
    });
  } else if (!restEnabled) {
    app.use('/rest/v1', (_req: Request, res: Response) => {
      res.status(410).json({ error: 'REST API disabled. Use GraphQL endpoint at /v2/graphql.' });
    });
  }

  // GET /rest/v1/agents - List agents with filters (PostgREST format)
  app.get('/rest/v1/agents', async (req: Request, res: Response) => {
    try {
      const id = parsePostgRESTValue(req.query.id) ?? parsePostgRESTValue(req.query.asset);
      const agentIdRaw = parsePostgRESTValue(req.query.agent_id) ?? parsePostgRESTValue(req.query.agentId);
      const agentId = safeBigInt(agentIdRaw);
      const blockSlotFilter = parsePostgRESTBigIntFilter(req.query.block_slot);
      const owner = parsePostgRESTValue(req.query.owner);
      const creator = parsePostgRESTValue(req.query.creator);
      const collection = parsePostgRESTValue(req.query.collection);
      const collectionPointerFilter = resolveCollectionPointerFilter(req.query);
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
      const order = safeQueryString(req.query.order);

      if (agentIdRaw !== undefined && agentId === undefined) {
        res.status(400).json({ error: 'Invalid agent_id filter value' });
        return;
      }
      if (blockSlotFilter === null) {
        res.status(400).json({ error: 'Invalid block_slot filter value' });
        return;
      }

      if ((updatedAtGtRaw && !updatedAtGt) || (updatedAtLtRaw && !updatedAtLt)) {
        res.status(400).json({ error: 'Invalid updated_at filter value' });
        return;
      }

      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }
      if (collectionPointerFilter.kind === 'no_match') {
        res.json([]);
        return;
      }
      const where: Prisma.AgentWhereInput = { ...statusFilter };
      if (id) where.id = id;
      if (agentId !== undefined) where.agentId = agentId;
      if (blockSlotFilter !== undefined) where.createdSlot = blockSlotFilter;
      if (owner) where.owner = owner;
      if (creator) where.creator = creator;
      if (collection) where.collection = collection;
      if (collectionPointerFilter.kind === 'eq') {
        if (collectionPointerFilter.values.length > 1) {
          where.OR = collectionPointerFilter.values.map((value) => ({ collectionPointer: value }));
        } else if (collectionPointerFilter.values.length === 1) {
          where.collectionPointer = collectionPointerFilter.values[0];
        }
      } else if (collectionPointerFilter.kind === 'neq') {
        if (collectionPointerFilter.values.length > 1) {
          where.NOT = { collectionPointer: { in: collectionPointerFilter.values } };
        } else if (collectionPointerFilter.values.length === 1) {
          where.NOT = { collectionPointer: collectionPointerFilter.values[0] };
        }
      }
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
        orderBy:
          order === 'block_slot.asc'
            ? [{ createdSlot: 'asc' }, { txIndex: 'asc' }, { eventOrdinal: 'asc' }, { id: 'asc' }]
            : order === 'block_slot.desc'
              ? [{ createdSlot: 'desc' }, { txIndex: 'desc' }, { eventOrdinal: 'desc' }, { id: 'desc' }]
              : [{ createdAt: 'desc' }, { id: 'desc' }],
        take: limit,
        skip: offset,
      });

      const digestFeedbackCounts = await loadLocalAgentDigestFeedbackCounts(prisma, agents.map((agent) => agent.id));
      const mapped = agents.map((agent) => mapAgentToApi(agent, { feedbackCount: digestFeedbackCounts.get(agent.id) }));

      res.json(mapped);
    } catch (error) {
      logger.error({ error }, 'Error fetching agents');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/agents/children - Direct children for a given parent asset
  app.get('/rest/v1/agents/children', async (req: Request, res: Response) => {
    try {
      if (!prisma) {
        if (!options.pool) {
          res.status(503).json({ error: 'Agent hierarchy endpoints require local Prisma backend' });
          return;
        }

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

        const params: unknown[] = [parentAsset];
        const statusSql = buildPoolStatusPredicate(statusFilter, params, 'a.status');
        params.push(limit, offset);

        const { rows } = await options.pool.query<PoolAgentApiRow>(
          `SELECT ${buildPoolAgentSelect('a')}
           FROM agents a
           WHERE a.parent_asset = $1::text${statusSql}
           ORDER BY a.created_at DESC, a.asset DESC
           LIMIT $${params.length - 1}::int OFFSET $${params.length}::int`,
          params
        );
        const digestFeedbackCounts = await loadPoolAgentDigestFeedbackCounts(options.pool, rows.map((row) => row.asset));
        res.json(rows.map((row) => mapPoolAgentToApi(row, { feedbackCount: digestFeedbackCounts.get(row.asset) })));
        return;
      }

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
      const digestFeedbackCounts = await loadLocalAgentDigestFeedbackCounts(prisma, children.map((child) => child.id));
      res.json(children.map((child) => mapAgentToApi(child, { feedbackCount: digestFeedbackCounts.get(child.id) })));
    } catch (error) {
      logger.error({ error }, 'Error fetching agent children');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/agents/tree - Reconstruct parent/children tree (bounded depth)
  app.get('/rest/v1/agents/tree', async (req: Request, res: Response) => {
    try {
      if (!prisma) {
        if (!options.pool) {
          res.status(503).json({ error: 'Agent hierarchy endpoints require local Prisma backend' });
          return;
        }

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

        const treeParams: unknown[] = [rootAsset, maxDepth, includeRoot];
        const baseStatusSql = buildPoolStatusPredicate(statusFilter, treeParams, 'a.status');
        const recursiveStatusSql = buildPoolStatusPredicate(statusFilter, treeParams, 'c.status');
        treeParams.push(limit, offset);

        const { rows } = await options.pool.query<PoolAgentTreeNodeRow>(
          `WITH RECURSIVE tree AS (
             SELECT
               a.asset,
               a.parent_asset,
               ARRAY[a.asset]::text[] AS path,
               0 AS depth
             FROM agents a
             WHERE a.asset = $1::text${baseStatusSql}
             UNION ALL
             SELECT
               c.asset,
               c.parent_asset,
               t.path || c.asset,
               t.depth + 1
             FROM agents c
             INNER JOIN tree t ON c.parent_asset = t.asset
             WHERE t.depth < $2::int${recursiveStatusSql}
               AND NOT (c.asset = ANY(t.path))
           )
           SELECT asset, parent_asset, path, depth
           FROM tree
           WHERE $3::boolean OR depth > 0
           ORDER BY depth ASC, path ASC
           LIMIT $${treeParams.length - 1}::int OFFSET $${treeParams.length}::int`,
          treeParams
        );

        if (rows.length === 0) {
          res.json([]);
          return;
        }

        const assets = rows.map((row) => row.asset);
        const { rows: agentRows } = await options.pool.query<PoolAgentApiRow>(
          `SELECT ${buildPoolAgentSelect('a')}
           FROM agents a
           WHERE a.asset = ANY($1::text[])`,
          [assets]
        );
        const byAsset = new Map(agentRows.map((row) => [row.asset, row]));
        const digestFeedbackCounts = await loadPoolAgentDigestFeedbackCounts(options.pool, assets);
        res.json(
          rows.flatMap((row) => {
            const agent = byAsset.get(row.asset);
            if (!agent) return [];
            return [{
              ...mapPoolAgentToApi(agent, { feedbackCount: digestFeedbackCounts.get(agent.asset) }),
              depth: row.depth,
              path: row.path,
            }];
          })
        );
        return;
      }

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
      const digestFeedbackCounts = await loadLocalAgentDigestFeedbackCounts(prisma, paged.map((node) => node.id));
      res.json(
        paged.map((node) => ({
          ...mapAgentToApi(node, { feedbackCount: digestFeedbackCounts.get(node.id) }),
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
      if (!prisma) {
        if (!options.pool) {
          res.status(503).json({ error: 'Agent hierarchy endpoints require local Prisma backend' });
          return;
        }

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

        const lineageParams: unknown[] = [asset, MAX_TREE_DEPTH * 4, includeSelf];
        const baseStatusSql = buildPoolStatusPredicate(statusFilter, lineageParams, 'a.status');
        const recursiveStatusSql = buildPoolStatusPredicate(statusFilter, lineageParams, 'p.status');
        const { rows } = await options.pool.query<PoolAgentTreeNodeRow>(
          `WITH RECURSIVE lineage AS (
             SELECT
               a.asset,
               a.parent_asset,
               ARRAY[a.asset]::text[] AS path,
               0 AS depth
             FROM agents a
             WHERE a.asset = $1::text${baseStatusSql}
             UNION ALL
             SELECT
               p.asset,
               p.parent_asset,
               l.path || p.asset,
               l.depth + 1
             FROM agents p
             INNER JOIN lineage l ON l.parent_asset = p.asset
             WHERE l.depth < $2::int${recursiveStatusSql}
               AND NOT (p.asset = ANY(l.path))
           )
           SELECT asset, parent_asset, path, depth
           FROM lineage
           WHERE $3::boolean OR depth > 0
           ORDER BY depth DESC, asset ASC`,
          lineageParams
        );

        if (rows.length === 0) {
          res.json([]);
          return;
        }

        const assets = rows.map((row) => row.asset);
        const { rows: agentRows } = await options.pool.query<PoolAgentApiRow>(
          `SELECT ${buildPoolAgentSelect('a')}
           FROM agents a
           WHERE a.asset = ANY($1::text[])`,
          [assets]
        );
        const byAsset = new Map(agentRows.map((row) => [row.asset, row]));
        const digestFeedbackCounts = await loadPoolAgentDigestFeedbackCounts(options.pool, assets);
        const ordered = rows.flatMap((row) => {
          const agent = byAsset.get(row.asset);
          return agent ? [mapPoolAgentToApi(agent, { feedbackCount: digestFeedbackCounts.get(agent.asset) })] : [];
        });
        res.json(ordered.slice(offset, offset + limit));
        return;
      }

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
      const paged = out.slice(offset, offset + limit);
      const digestFeedbackCounts = await loadLocalAgentDigestFeedbackCounts(prisma, paged.map((node) => node.id));
      res.json(paged.map((node) => mapAgentToApi(node, { feedbackCount: digestFeedbackCounts.get(node.id) })));
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
      const blockSlotFilter = parsePostgRESTBigIntFilter(req.query.block_slot);
      const feedbackIndexRaw = safeQueryString(req.query.feedback_index);
      const feedbackIndexFilter = parsePostgRESTBigIntFilter(req.query.feedback_index);
      const feedbackIndexInState = parsePostgRESTListOperator(req.query.feedback_index, 'in');
      const feedbackIndexNotInState = parsePostgRESTListOperator(req.query.feedback_index, 'not.in');
      const feedback_id = parsePostgRESTValue(req.query.feedback_id);
      const isRevokedComparison = parsePostgRESTComparison(req.query.is_revoked);
      const tag1 = parsePostgRESTValue(req.query.tag1);
      const tag2 = parsePostgRESTValue(req.query.tag2);
      const endpoint = parsePostgRESTValue(req.query.endpoint);
      const createdAtFilter = parsePostgRESTDateFilter(req.query.created_at);
      const createdAtGtRaw = parsePostgRESTValue(req.query.created_at_gt);
      const createdAtLtRaw = parsePostgRESTValue(req.query.created_at_lt);
      const createdAtGt = createdAtGtRaw ? parseTimestampValue(createdAtGtRaw) : undefined;
      const createdAtLt = createdAtLtRaw ? parseTimestampValue(createdAtLtRaw) : undefined;
      const txSignatureFilter = parsePostgRESTTextFilter(req.query.tx_signature);
      const orFilterRaw = safeQueryString(req.query.or); // Handle OR filter for tag search
      const orFilter = orFilterRaw && orFilterRaw.length <= 200 ? orFilterRaw : undefined; // Limit filter length
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);
      const order = safeQueryString(req.query.order);

      if (blockSlotFilter === null) {
        res.status(400).json({ error: 'Invalid block_slot filter value' });
        return;
      }
      if ((createdAtGtRaw && !createdAtGt) || (createdAtLtRaw && !createdAtLt)) {
        res.status(400).json({ error: 'Invalid created_at filter value' });
        return;
      }

      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }
      if (
        hasUnsupportedEqOnlyOperator(req.query.asset) ||
        hasUnsupportedEqOnlyOperator(req.query.client_address) ||
        hasUnsupportedEqOnlyOperator(req.query.tag1) ||
        hasUnsupportedEqOnlyOperator(req.query.tag2) ||
        hasUnsupportedEqOnlyOperator(req.query.endpoint)
      ) {
        res.status(400).json({
          error: 'Invalid eq-only filter: asset, client_address, tag1, tag2, and endpoint accept plain values or eq.<value> only',
        });
        return;
      }
      const where: Prisma.FeedbackWhereInput = { ...statusFilter };
      if (asset) where.agentId = asset;
      if (blockSlotFilter !== undefined) where.createdSlot = blockSlotFilter;
      if (client_address) where.client = client_address;
      if (feedback_id !== undefined) {
        const parsedFeedbackId = safeBigInt(feedback_id);
        if (parsedFeedbackId === undefined) {
          res.status(400).json({ error: 'Invalid feedback_id: must be a valid integer' });
          return;
        }
        where.feedbackId = parsedFeedbackId;
      }
      if (feedbackIndexInState.matched) {
        if (feedbackIndexInState.malformed || feedbackIndexInState.values.length === 0) {
          res.status(400).json({ error: 'Invalid feedback_index IN filter: must be in.(int,int,...)' });
          return;
        }
        const indices = safeBigIntArray(feedbackIndexInState.values.join(','));
        if (indices === undefined) {
          res.status(400).json({ error: 'Invalid feedback_index IN filter: must be in.(int,int,...)' });
          return;
        }
        where.feedbackIndex = { in: indices };
      } else if (feedbackIndexNotInState.matched) {
        if (feedbackIndexNotInState.malformed || feedbackIndexNotInState.values.length === 0) {
          res.status(400).json({ error: 'Invalid feedback_index NOT IN filter: must be not.in.(int,int,...)' });
          return;
        }
        const indices = safeBigIntArray(feedbackIndexNotInState.values.join(','));
        if (indices === undefined) {
          res.status(400).json({ error: 'Invalid feedback_index NOT IN filter: must be not.in.(int,int,...)' });
          return;
        }
        where.feedbackIndex = { notIn: indices };
      } else if (feedbackIndexRaw?.startsWith('neq.')) {
        const idx = safeBigInt(feedbackIndexRaw.slice(4));
        if (idx === undefined) {
          res.status(400).json({ error: 'Invalid feedback_index: must be a valid integer' });
          return;
        }
        where.feedbackIndex = { not: idx };
      } else if (feedbackIndexFilter === null) {
        res.status(400).json({ error: 'Invalid feedback_index: must be a valid integer' });
        return;
      } else if (feedbackIndexFilter !== undefined) {
        where.feedbackIndex = feedbackIndexFilter;
      }
      if (isRevokedComparison) {
        const revokedValue = isRevokedComparison.value.toLowerCase();
        if (revokedValue !== 'true' && revokedValue !== 'false') {
          res.status(400).json({ error: 'Invalid is_revoked: allowed values are true or false' });
          return;
        }
        const revokedBool = revokedValue === 'true';
        where.revoked = isRevokedComparison.op === 'neq'
          ? { not: revokedBool }
          : revokedBool;
      }
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
      if (txSignatureFilter === null) {
        res.status(400).json({ error: 'Invalid tx_signature filter: use eq/neq/in/not.in with non-empty values' });
        return;
      }
      if (txSignatureFilter !== undefined) {
        where.createdTxSignature = txSignatureFilter;
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

      const feedbackOrderBy: Prisma.FeedbackOrderByWithRelationInput[] =
        order === 'block_slot.asc'
          ? [{ createdSlot: 'asc' }, { createdAt: 'asc' }, { agentId: 'asc' }, { client: 'asc' }, { feedbackIndex: 'asc' }, { id: 'asc' }]
          : order === 'block_slot.desc'
            ? [{ createdSlot: 'desc' }, { createdAt: 'desc' }, { agentId: 'asc' }, { client: 'asc' }, { feedbackIndex: 'asc' }, { id: 'asc' }]
            : [
                { createdAt: 'desc' },
                { agentId: 'asc' },
                { client: 'asc' },
                { feedbackIndex: 'asc' },
                { id: 'asc' },
              ];

      // If Prefer: count=exact, also get total count.
      // Also count when feedback_id is queried without asset because feedback_id is only agent-scoped.
      const needsCount = wantsCount(req);
      const needsAmbiguityCount = feedback_id !== undefined && !asset;
      const [feedbacks, totalCount] = await Promise.all([
        prisma.feedback.findMany({
          where,
          orderBy: feedbackOrderBy,
          take: limit,
          skip: offset,
        }),
        needsCount || needsAmbiguityCount ? prisma.feedback.count({ where }) : Promise.resolve(0),
      ]);

      if (feedback_id !== undefined && !asset && totalCount > 1) {
        res.status(400).json({ error: 'feedback_id is scoped by agent. Provide asset filter when ambiguous.' });
        return;
      }

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
        id: f.feedbackId != null ? f.feedbackId.toString() : null,
        feedback_id: f.feedbackId != null ? f.feedbackId.toString() : null,
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
        tx_index: f.txIndex,
        event_ordinal: f.eventOrdinal,
        tx_signature: f.createdTxSignature || '',
        created_at: f.createdAt.toISOString(),
      }));

      res.json(mapped);
    } catch (error) {
      logger.error({ error }, 'Error fetching feedbacks');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/responses and /rest/v1/feedback_responses - List canonical feedback responses
  // Both routes are aliases for SDK compatibility and intentionally exclude orphan-response staging rows.
  const responsesHandler = async (req: Request, res: Response) => {
    try {
      const feedback_id = parsePostgRESTValue(req.query.feedback_id);
      const responseIdComparison = parsePostgRESTComparison(req.query.response_id);
      const asset = parsePostgRESTValue(req.query.asset);
      const client_address = parsePostgRESTValue(req.query.client_address);
      const blockSlotFilter = parsePostgRESTBigIntFilter(req.query.block_slot);
      const feedbackIndexRaw = safeQueryString(req.query.feedback_index);
      const feedbackIndexFilter = parsePostgRESTBigIntFilter(req.query.feedback_index);
      const feedbackIndexInState = parsePostgRESTListOperator(req.query.feedback_index, 'in');
      const feedbackIndexNotInState = parsePostgRESTListOperator(req.query.feedback_index, 'not.in');
      const txSignatureFilter = parsePostgRESTTextFilter(req.query.tx_signature);
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);
      const order = safeQueryString(req.query.order);

      if (blockSlotFilter === null) {
        res.status(400).json({ error: 'Invalid block_slot filter value' });
        return;
      }
      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }
      if (txSignatureFilter === null) {
        res.status(400).json({ error: 'Invalid tx_signature filter: use eq/neq/in/not.in with non-empty values' });
        return;
      }
      if (
        hasUnsupportedEqOnlyOperator(req.query.asset) ||
        hasUnsupportedEqOnlyOperator(req.query.client_address)
      ) {
        res.status(400).json({
          error: 'Invalid eq-only filter: asset and client_address accept plain values or eq.<value> only',
        });
        return;
      }
      const where: Prisma.FeedbackResponseWhereInput = { ...statusFilter };
      if (blockSlotFilter !== undefined) {
        where.OR = [
          { slot: blockSlotFilter },
          {
            AND: [
              { slot: null },
              { feedback: { createdSlot: blockSlotFilter } },
            ],
          },
        ];
      }
      if (txSignatureFilter !== undefined) {
        where.txSignature = txSignatureFilter;
      }
      let parsedResponseId: bigint | undefined;
      if (responseIdComparison) {
        parsedResponseId = safeBigInt(responseIdComparison.value);
        if (parsedResponseId === undefined) {
          res.status(400).json({ error: 'Invalid response_id: must be a valid integer' });
          return;
        }
        where.responseId = responseIdComparison.op === 'neq'
          ? { not: parsedResponseId }
          : parsedResponseId;
      }

      let parsedFeedbackIndex: bigint | undefined;
      let feedbackIndexClause: bigint | Prisma.BigIntFilter | undefined;
      if (feedbackIndexInState.matched) {
        if (feedbackIndexInState.malformed || feedbackIndexInState.values.length === 0) {
          res.status(400).json({ error: 'Invalid feedback_index IN filter: must be in.(int,int,...)' });
          return;
        }
        const indices = safeBigIntArray(feedbackIndexInState.values.join(','));
        if (indices === undefined) {
          res.status(400).json({ error: 'Invalid feedback_index IN filter: must be in.(int,int,...)' });
          return;
        }
        feedbackIndexClause = { in: indices };
      } else if (feedbackIndexNotInState.matched) {
        if (feedbackIndexNotInState.malformed || feedbackIndexNotInState.values.length === 0) {
          res.status(400).json({ error: 'Invalid feedback_index NOT IN filter: must be not.in.(int,int,...)' });
          return;
        }
        const indices = safeBigIntArray(feedbackIndexNotInState.values.join(','));
        if (indices === undefined) {
          res.status(400).json({ error: 'Invalid feedback_index NOT IN filter: must be not.in.(int,int,...)' });
          return;
        }
        feedbackIndexClause = { notIn: indices };
      } else if (feedbackIndexRaw?.startsWith('neq.')) {
        const idx = safeBigInt(feedbackIndexRaw.slice(4));
        if (idx === undefined) {
          res.status(400).json({ error: 'Invalid feedback_index: must be a valid integer' });
          return;
        }
        feedbackIndexClause = { not: idx };
      } else if (feedbackIndexFilter === null) {
        res.status(400).json({ error: 'Invalid feedback_index: must be a valid integer' });
        return;
      } else if (feedbackIndexFilter !== undefined) {
        feedbackIndexClause = feedbackIndexFilter;
        if (typeof feedbackIndexFilter === 'bigint') {
          parsedFeedbackIndex = feedbackIndexFilter;
        }
      }

      let parsedFeedbackSequentialId: bigint | undefined;
      if (feedback_id !== undefined) {
        parsedFeedbackSequentialId = safeBigInt(feedback_id);
        if (parsedFeedbackSequentialId === undefined) {
          res.status(400).json({ error: 'Invalid feedback_id: must be a valid integer' });
          return;
        }
        if (!asset) {
          res.status(400).json({ error: 'feedback_id is scoped by agent. Provide asset filter when using sequential feedback_id.' });
          return;
        }
      }

      const hasCanonicalFeedbackScope = (
        (feedback_id !== undefined && !!asset) ||
        (!!asset && !!client_address && parsedFeedbackIndex !== undefined)
      );

      if (responseIdComparison && !hasCanonicalFeedbackScope) {
        res.status(400).json({
          error: 'response_id requires canonical feedback scope (asset + client_address + feedback_index, or asset + feedback_id).',
        });
        return;
      }

      if (feedback_id !== undefined) {
        where.feedback = {
          agentId: asset!,
          feedbackId: parsedFeedbackSequentialId!,
        };
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
          res.json([]);
          return;
        }
      } else {
        const feedbackWhere: Prisma.FeedbackWhereInput = {};
        if (asset) feedbackWhere.agentId = asset;
        if (client_address) feedbackWhere.client = client_address;
        if (feedbackIndexClause !== undefined) feedbackWhere.feedbackIndex = feedbackIndexClause;
        if (Object.keys(feedbackWhere).length > 0) {
          where.feedback = feedbackWhere;
        }
      }

      const orderBy: Prisma.FeedbackResponseOrderByWithRelationInput[] =
        order === 'response_count.asc'
          ? [{ responseCount: 'asc' }, { feedback: { agentId: 'asc' } }, { feedback: { client: 'asc' } }, { feedback: { feedbackIndex: 'asc' } }, { responder: 'asc' }, { txIndex: 'asc' }, { eventOrdinal: 'asc' }, { txSignature: { sort: 'asc', nulls: 'last' } }]
          : order === 'response_count.desc'
            ? [{ responseCount: 'desc' }, { feedback: { agentId: 'asc' } }, { feedback: { client: 'asc' } }, { feedback: { feedbackIndex: 'asc' } }, { responder: 'asc' }, { txIndex: 'asc' }, { eventOrdinal: 'asc' }, { txSignature: { sort: 'asc', nulls: 'last' } }]
            : order === 'response_id.asc'
              ? [{ responseId: 'asc' }, { feedback: { agentId: 'asc' } }, { feedback: { client: 'asc' } }, { feedback: { feedbackIndex: 'asc' } }, { responder: 'asc' }, { txIndex: 'asc' }, { eventOrdinal: 'asc' }, { txSignature: { sort: 'asc', nulls: 'last' } }]
              : order === 'response_id.desc'
                ? [{ responseId: 'desc' }, { feedback: { agentId: 'asc' } }, { feedback: { client: 'asc' } }, { feedback: { feedbackIndex: 'asc' } }, { responder: 'asc' }, { txIndex: 'asc' }, { eventOrdinal: 'asc' }, { txSignature: { sort: 'asc', nulls: 'last' } }]
                : order === 'block_slot.asc'
                  ? [{ slot: 'asc' }, { feedback: { agentId: 'asc' } }, { feedback: { client: 'asc' } }, { feedback: { feedbackIndex: 'asc' } }, { responder: 'asc' }, { txIndex: 'asc' }, { eventOrdinal: 'asc' }, { txSignature: { sort: 'asc', nulls: 'last' } }]
                  : order === 'block_slot.desc'
                    ? [{ slot: 'desc' }, { feedback: { agentId: 'asc' } }, { feedback: { client: 'asc' } }, { feedback: { feedbackIndex: 'asc' } }, { responder: 'asc' }, { txIndex: 'asc' }, { eventOrdinal: 'asc' }, { txSignature: { sort: 'asc', nulls: 'last' } }]
                    : [{ createdAt: 'desc' }, { feedback: { agentId: 'asc' } }, { feedback: { client: 'asc' } }, { feedback: { feedbackIndex: 'asc' } }, { responder: 'asc' }, { txIndex: 'asc' }, { eventOrdinal: 'asc' }, { txSignature: { sort: 'asc', nulls: 'last' } }];

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
        id: r.responseId != null ? r.responseId.toString() : null,
        response_id: r.responseId != null ? r.responseId.toString() : null,
        feedback_id: r.feedback.feedbackId != null ? r.feedback.feedbackId.toString() : null,
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
        tx_index: r.txIndex,
        event_ordinal: r.eventOrdinal,
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
      const slotFilter = parsePostgRESTBigIntFilter(req.query.slot ?? req.query.block_slot);
      const feedbackIndexRaw = safeQueryString(req.query.feedback_index);
      const feedbackIndexFilter = parsePostgRESTBigIntFilter(req.query.feedback_index);
      const feedbackIndexInState = parsePostgRESTListOperator(req.query.feedback_index, 'in');
      const feedbackIndexNotInState = parsePostgRESTListOperator(req.query.feedback_index, 'not.in');
      const revocationIdRaw = safeQueryString(req.query.revocation_id);
      const revocationIdFilter = parsePostgRESTBigIntFilter(req.query.revocation_id);
      const revokeCountRaw = safeQueryString(req.query.revoke_count);
      const revokeCountFilter = parsePostgRESTBigIntFilter(req.query.revoke_count);
      const revokeCountInState = parsePostgRESTListOperator(req.query.revoke_count, 'in');
      const revokeCountNotInState = parsePostgRESTListOperator(req.query.revoke_count, 'not.in');
      const txSignatureFilter = parsePostgRESTTextFilter(req.query.tx_signature);
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);
      const order = safeQueryString(req.query.order);

      if (slotFilter === null) {
        res.status(400).json({ error: 'Invalid slot filter value' });
        return;
      }
      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }
      if (txSignatureFilter === null) {
        res.status(400).json({ error: 'Invalid tx_signature filter: use eq/neq/in/not.in with non-empty values' });
        return;
      }
      if (
        hasUnsupportedEqOnlyOperator(req.query.asset) ||
        hasUnsupportedEqOnlyOperator(req.query.client_address ?? req.query.client)
      ) {
        res.status(400).json({
          error: 'Invalid eq-only filter: asset, client_address, and client accept plain values or eq.<value> only',
        });
        return;
      }
      const where: Prisma.RevocationWhereInput = { ...statusFilter };
      if (asset) where.agentId = asset;
      if (client) where.client = client;
      if (slotFilter !== undefined) where.slot = slotFilter;
      if (txSignatureFilter !== undefined) where.txSignature = txSignatureFilter;
      if (revocationIdRaw !== undefined && !asset) {
        res.status(400).json({ error: 'revocation_id is scoped by agent. Provide asset filter when using revocation_id.' });
        return;
      }
      if (revocationIdFilter === null) {
        res.status(400).json({ error: 'Invalid revocation_id filter: use eq/gt/gte/lt/lte with a valid integer' });
        return;
      }
      if (revocationIdFilter !== undefined) {
        where.revocationId = revocationIdFilter;
      }
      if (feedbackIndexInState.matched) {
        if (feedbackIndexInState.malformed || feedbackIndexInState.values.length === 0) {
          res.status(400).json({ error: 'Invalid feedback_index IN filter: must be in.(int,int,...)' });
          return;
        }
        const indices = safeBigIntArray(feedbackIndexInState.values.join(','));
        if (indices === undefined) {
          res.status(400).json({ error: 'Invalid feedback_index IN filter: must be in.(int,int,...)' });
          return;
        }
        where.feedbackIndex = { in: indices };
      } else if (feedbackIndexNotInState.matched) {
        if (feedbackIndexNotInState.malformed || feedbackIndexNotInState.values.length === 0) {
          res.status(400).json({ error: 'Invalid feedback_index NOT IN filter: must be not.in.(int,int,...)' });
          return;
        }
        const indices = safeBigIntArray(feedbackIndexNotInState.values.join(','));
        if (indices === undefined) {
          res.status(400).json({ error: 'Invalid feedback_index NOT IN filter: must be not.in.(int,int,...)' });
          return;
        }
        where.feedbackIndex = { notIn: indices };
      } else if (feedbackIndexRaw?.startsWith('neq.')) {
        const idx = safeBigInt(feedbackIndexRaw.slice(4));
        if (idx === undefined) {
          res.status(400).json({ error: 'Invalid feedback_index: must be a valid integer' });
          return;
        }
        where.feedbackIndex = { not: idx };
      } else if (feedbackIndexFilter === null) {
        res.status(400).json({ error: 'Invalid feedback_index: must be a valid integer' });
        return;
      } else if (feedbackIndexFilter !== undefined) {
        where.feedbackIndex = feedbackIndexFilter;
      }
      if (revokeCountInState.matched) {
        if (revokeCountInState.malformed || revokeCountInState.values.length === 0) {
          res.status(400).json({ error: 'Invalid revoke_count IN filter: in.() must include at least one integer' });
          return;
        }

        const counts = safeBigIntArray(revokeCountInState.values.join(','));
        if (counts === undefined) {
          res.status(400).json({ error: 'Invalid revoke_count IN filter: must be in.(int,int,...)' });
          return;
        }
        where.revokeCount = { in: counts };
      } else if (revokeCountNotInState.matched) {
        if (revokeCountNotInState.malformed || revokeCountNotInState.values.length === 0) {
          res.status(400).json({ error: 'Invalid revoke_count NOT IN filter: must be not.in.(int,int,...)' });
          return;
        }

        const counts = safeBigIntArray(revokeCountNotInState.values.join(','));
        if (counts === undefined) {
          res.status(400).json({ error: 'Invalid revoke_count NOT IN filter: must be not.in.(int,int,...)' });
          return;
        }
        where.revokeCount = { notIn: counts };
      } else if (revokeCountRaw?.startsWith('neq.')) {
        const count = safeBigInt(revokeCountRaw.slice(4));
        if (count === undefined) {
          res.status(400).json({ error: 'Invalid revoke_count: must be a valid integer' });
          return;
        }
        where.revokeCount = { not: count };
      } else if (revokeCountFilter === null) {
        res.status(400).json({ error: 'Invalid revoke_count: must be a valid integer' });
        return;
      } else if (revokeCountFilter !== undefined) {
        where.revokeCount = revokeCountFilter;
      }

      const orderBy: Prisma.RevocationOrderByWithRelationInput[] =
        order === 'revoke_count.asc'
          ? [{ revokeCount: 'asc' }, { agentId: 'asc' }, { client: 'asc' }, { feedbackIndex: 'asc' }, { txIndex: 'asc' }, { eventOrdinal: 'asc' }, { txSignature: { sort: 'asc', nulls: 'last' } }]
          : order === 'revoke_count.desc'
            ? [{ revokeCount: 'desc' }, { agentId: 'asc' }, { client: 'asc' }, { feedbackIndex: 'asc' }, { txIndex: 'asc' }, { eventOrdinal: 'asc' }, { txSignature: { sort: 'asc', nulls: 'last' } }]
            : order === 'revocation_id.asc'
              ? [{ revocationId: 'asc' }, { agentId: 'asc' }, { client: 'asc' }, { feedbackIndex: 'asc' }, { txIndex: 'asc' }, { eventOrdinal: 'asc' }, { txSignature: { sort: 'asc', nulls: 'last' } }]
              : order === 'revocation_id.desc'
                ? [{ revocationId: 'desc' }, { agentId: 'asc' }, { client: 'asc' }, { feedbackIndex: 'asc' }, { txIndex: 'asc' }, { eventOrdinal: 'asc' }, { txSignature: { sort: 'asc', nulls: 'last' } }]
                : order === 'slot.asc' || order === 'block_slot.asc'
                  ? [{ slot: 'asc' }, { agentId: 'asc' }, { client: 'asc' }, { feedbackIndex: 'asc' }, { txIndex: 'asc' }, { eventOrdinal: 'asc' }, { txSignature: { sort: 'asc', nulls: 'last' } }]
                  : order === 'slot.desc' || order === 'block_slot.desc'
                    ? [{ slot: 'desc' }, { agentId: 'asc' }, { client: 'asc' }, { feedbackIndex: 'asc' }, { txIndex: 'asc' }, { eventOrdinal: 'asc' }, { txSignature: { sort: 'asc', nulls: 'last' } }]
            : [{ createdAt: 'desc' }, { agentId: 'asc' }, { client: 'asc' }, { feedbackIndex: 'asc' }, { txIndex: 'asc' }, { eventOrdinal: 'asc' }, { txSignature: { sort: 'asc', nulls: 'last' } }];

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
        id: r.revocationId != null ? r.revocationId.toString() : null,
        revocation_id: r.revocationId != null ? r.revocationId.toString() : null,
        asset: r.agentId,
        client_address: r.client,
        feedback_index: r.feedbackIndex.toString(),
        feedback_hash: r.feedbackHash ? Buffer.from(r.feedbackHash).toString('hex') : null,
        slot: Number(r.slot),
        original_score: r.originalScore,
        atom_enabled: r.atomEnabled,
        had_impact: r.hadImpact,
        running_digest: r.runningDigest ? Buffer.from(r.runningDigest).toString('hex') : null,
        revoke_count: r.revokeCount.toString(),
        tx_index: r.txIndex,
        event_ordinal: r.eventOrdinal,
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

  // GET /rest/v1/collections - Canonical collections (creator + collection pointer key)
  const collectionsHandler = async (req: Request, res: Response) => {
    try {
      const collectionIdFilter = parsePostgRESTBigIntFilter(req.query.collection_id);
      const colRaw = parsePostgRESTValue(req.query.collection);
      const colVariants = colRaw ? collectionPointerVariants(colRaw) : [];
      const creator = parsePostgRESTValue(req.query.creator);
      const firstSeenAsset = parsePostgRESTValue(req.query.first_seen_asset);
      const firstSeenSlotFilter = parsePostgRESTBigIntFilter(req.query.first_seen_slot);
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);
      const order = safeQueryString(req.query.order);

      if (collectionIdFilter === null) {
        res.status(400).json({ error: 'Invalid collection_id filter: use eq/gt/gte/lt/lte with a valid integer' });
        return;
      }
      if (firstSeenSlotFilter === null) {
        res.status(400).json({ error: 'Invalid first_seen_slot filter: use eq/gt/gte/lt/lte with a valid integer' });
        return;
      }

      if (!prisma) {
        if (!options.pool) {
          res.status(503).json({ error: 'Collections endpoint unavailable without database backend' });
          return;
        }

        type CollectionSqlRow = {
          collection_id: string | null;
          col: string;
          creator: string;
          first_seen_asset: string;
          first_seen_at: Date | string;
          first_seen_slot: string;
          first_seen_tx_signature: string | null;
          last_seen_at: Date | string;
          last_seen_slot: string;
          last_seen_tx_signature: string | null;
          asset_count: string;
          version: string | null;
          name: string | null;
          symbol: string | null;
          description: string | null;
          image: string | null;
          banner_image: string | null;
          social_website: string | null;
          social_x: string | null;
          social_discord: string | null;
          metadata_status: string | null;
          metadata_hash: string | null;
          metadata_bytes: number | null;
          metadata_updated_at: Date | string | null;
        };

        const filterSql: string[] = [];
        const filterParams: unknown[] = [];
        let paramIdx = 1;

        if (collectionIdFilter !== undefined) {
          if (typeof collectionIdFilter === 'bigint') {
            filterSql.push(`collection_id = $${paramIdx}::bigint`);
            filterParams.push(collectionIdFilter.toString());
            paramIdx++;
          } else {
            if (collectionIdFilter.gt !== undefined) {
              filterSql.push(`collection_id > $${paramIdx}::bigint`);
              filterParams.push(collectionIdFilter.gt.toString());
              paramIdx++;
            }
            if (collectionIdFilter.gte !== undefined) {
              filterSql.push(`collection_id >= $${paramIdx}::bigint`);
              filterParams.push(collectionIdFilter.gte.toString());
              paramIdx++;
            }
            if (collectionIdFilter.lt !== undefined) {
              filterSql.push(`collection_id < $${paramIdx}::bigint`);
              filterParams.push(collectionIdFilter.lt.toString());
              paramIdx++;
            }
            if (collectionIdFilter.lte !== undefined) {
              filterSql.push(`collection_id <= $${paramIdx}::bigint`);
              filterParams.push(collectionIdFilter.lte.toString());
              paramIdx++;
            }
          }
        }
        if (colVariants.length > 1) {
          filterSql.push(`col = ANY($${paramIdx}::text[])`);
          filterParams.push(colVariants);
          paramIdx++;
        } else if (colVariants.length === 1) {
          filterSql.push(`col = $${paramIdx}::text`);
          filterParams.push(colVariants[0]);
          paramIdx++;
        }
        if (creator) {
          filterSql.push(`creator = $${paramIdx}::text`);
          filterParams.push(creator);
          paramIdx++;
        }
        if (firstSeenAsset) {
          filterSql.push(`first_seen_asset = $${paramIdx}::text`);
          filterParams.push(firstSeenAsset);
          paramIdx++;
        }
        if (firstSeenSlotFilter !== undefined) {
          if (typeof firstSeenSlotFilter === 'bigint') {
            filterSql.push(`first_seen_slot = $${paramIdx}::bigint`);
            filterParams.push(firstSeenSlotFilter.toString());
            paramIdx++;
          } else {
            if (firstSeenSlotFilter.gt !== undefined) {
              filterSql.push(`first_seen_slot > $${paramIdx}::bigint`);
              filterParams.push(firstSeenSlotFilter.gt.toString());
              paramIdx++;
            }
            if (firstSeenSlotFilter.gte !== undefined) {
              filterSql.push(`first_seen_slot >= $${paramIdx}::bigint`);
              filterParams.push(firstSeenSlotFilter.gte.toString());
              paramIdx++;
            }
            if (firstSeenSlotFilter.lt !== undefined) {
              filterSql.push(`first_seen_slot < $${paramIdx}::bigint`);
              filterParams.push(firstSeenSlotFilter.lt.toString());
              paramIdx++;
            }
            if (firstSeenSlotFilter.lte !== undefined) {
              filterSql.push(`first_seen_slot <= $${paramIdx}::bigint`);
              filterParams.push(firstSeenSlotFilter.lte.toString());
              paramIdx++;
            }
          }
        }

        const whereSql = filterSql.length > 0 ? `WHERE ${filterSql.join(' AND ')}` : '';
        const needsCount = wantsCount(req);
        const rowsOrderSql =
          order === 'first_seen_slot.asc'
            ? 'ORDER BY first_seen_slot ASC, col ASC, creator ASC'
            : order === 'first_seen_slot.desc'
              ? 'ORDER BY first_seen_slot DESC, col ASC, creator ASC'
              : 'ORDER BY first_seen_at DESC, col ASC, creator ASC';
        const rowsSql = `SELECT
                            collection_id::text,
                            col,
                            creator,
                            first_seen_asset,
                            first_seen_at,
                            first_seen_slot::text,
                            first_seen_tx_signature,
                            last_seen_at,
                            last_seen_slot::text,
                            last_seen_tx_signature,
                            asset_count::text,
                            version,
                            name,
                            symbol,
                            description,
                            image,
                            banner_image,
                            social_website,
                            social_x,
                            social_discord,
                            metadata_status,
                            metadata_hash,
                            metadata_bytes,
                            metadata_updated_at
                         FROM collection_pointers
                         ${whereSql}
                         ${rowsOrderSql}
                         LIMIT $${paramIdx}::int OFFSET $${paramIdx + 1}::int`;

        const [rowsResult, countResult] = await Promise.all([
          options.pool.query<CollectionSqlRow>(rowsSql, [...filterParams, limit, offset]),
          needsCount
            ? options.pool.query<{ total: string }>(
              `SELECT COUNT(*)::text AS total FROM collection_pointers ${whereSql}`,
              filterParams
            )
            : Promise.resolve({ rows: [{ total: '0' }] } as { rows: { total: string }[] }),
        ]);

        if (needsCount) {
          const totalCount = Number(countResult.rows[0]?.total ?? '0');
          setContentRange(res, offset, rowsResult.rows.length, Number.isFinite(totalCount) ? totalCount : 0);
        }

        res.json(rowsResult.rows.map((row) => ({
          collection_id: row.collection_id,
          collection: row.col,
          creator: row.creator,
          first_seen_asset: row.first_seen_asset,
          first_seen_at: new Date(row.first_seen_at).toISOString(),
          first_seen_slot: row.first_seen_slot,
          first_seen_tx_signature: row.first_seen_tx_signature,
          last_seen_at: new Date(row.last_seen_at).toISOString(),
          last_seen_slot: row.last_seen_slot,
          last_seen_tx_signature: row.last_seen_tx_signature,
          asset_count: row.asset_count,
          version: row.version,
          name: row.name,
          symbol: row.symbol,
          description: row.description,
          image: row.image,
          banner_image: row.banner_image,
          social_website: row.social_website,
          social_x: row.social_x,
          social_discord: row.social_discord,
          metadata_status: row.metadata_status,
          metadata_hash: row.metadata_hash,
          metadata_bytes: row.metadata_bytes,
          metadata_updated_at: row.metadata_updated_at ? new Date(row.metadata_updated_at).toISOString() : null,
        })));
        return;
      }

      const where: Prisma.CollectionWhereInput = {};
      if (collectionIdFilter !== undefined) where.collectionId = collectionIdFilter;
      if (firstSeenSlotFilter !== undefined) where.firstSeenSlot = firstSeenSlotFilter;
      if (colVariants.length > 1) {
        where.OR = colVariants.map((value) => ({ col: value }));
      } else if (colVariants.length === 1) {
        where.col = colVariants[0];
      }
      if (creator) where.creator = creator;
      if (firstSeenAsset) where.firstSeenAsset = firstSeenAsset;

      const needsCount = wantsCount(req);
      const [rows, totalCount] = await Promise.all([
        prisma.collection.findMany({
          where,
          orderBy:
            order === 'first_seen_slot.asc'
              ? [{ firstSeenSlot: 'asc' }, { col: 'asc' }, { creator: 'asc' }]
              : order === 'first_seen_slot.desc'
                ? [{ firstSeenSlot: 'desc' }, { col: 'asc' }, { creator: 'asc' }]
                : [
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
  };
  app.get('/rest/v1/collections', collectionsHandler);
  app.get('/rest/v1/collection_pointers', collectionsHandler);

  // GET /rest/v1/collection_asset_count - Count assets for creator+col scope
  app.get('/rest/v1/collection_asset_count', async (req: Request, res: Response) => {
    try {
      const col = parsePostgRESTValue(req.query.collection);
      if (!col) {
        res.status(400).json({ error: 'Missing required query param: collection' });
        return;
      }
      const creator = parsePostgRESTValue(req.query.creator)?.trim();
      if (!creator) {
        res.status(400).json({ error: 'Missing required query param: creator (scope is creator+collection)' });
        return;
      }

      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }

      if (!prisma) {
        if (!restProxyEnabled || !supabaseRestBaseUrl) {
          res.status(503).json({ error: 'Collection asset count unavailable without REST backend' });
          return;
        }

        const params = new URLSearchParams();
        params.set('select', 'asset');
        params.set('limit', '1');
        params.set('canonical_col', formatCanonicalCollectionFilter(col));
        params.set('creator', `eq.${creator}`);

        const status = parsePostgRESTComparison(req.query.status);
        const includeOrphaned = safeQueryString(req.query.includeOrphaned) === 'true';
        if (status?.value) params.set('status', `${status.op}.${status.value}`);
        else if (!includeOrphaned) params.set('status', 'neq.ORPHANED');

        const upstreamHeaders = buildSupabaseProxyHeaders(req);
        upstreamHeaders.set('prefer', 'count=exact');

        const upstreamRes = await fetch(`${supabaseRestBaseUrl}/agents?${params.toString()}`, {
          method: 'GET',
          headers: upstreamHeaders,
        });

        for (const header of PROXY_RESPONSE_HEADER_ALLOWLIST) {
          const value = upstreamRes.headers.get(header);
          if (value) {
            res.setHeader(header, value);
          }
        }

        const payload = Buffer.from(await upstreamRes.arrayBuffer());
        if (!upstreamRes.ok) {
          res.status(upstreamRes.status).send(payload);
          return;
        }

        let count = parseContentRangeTotal(upstreamRes.headers.get('content-range'));
        if (count === null) {
          try {
            const parsed = JSON.parse(payload.toString('utf8'));
            count = Array.isArray(parsed) ? parsed.length : 0;
          } catch {
            count = 0;
          }
        }

        res.json({
          collection: col,
          creator,
          asset_count: count,
        });
        return;
      }

      const where: Prisma.AgentWhereInput = { ...statusFilter };
      const collectionPointerVariantsList = collectionPointerVariants(col);
      if (collectionPointerVariantsList.length > 1) {
        where.OR = collectionPointerVariantsList.map((value) => ({ collectionPointer: value }));
      } else {
        where.collectionPointer = collectionPointerVariantsList[0] ?? col;
      }
      if (creator) where.creator = creator;

      const count = await prisma.agent.count({ where });
      res.json({
        collection: col,
        creator,
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
      const creator = parsePostgRESTValue(req.query.creator)?.trim();
      if (!creator) {
        res.status(400).json({ error: 'Missing required query param: creator (scope is creator+collection)' });
        return;
      }
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);

      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }

      if (!prisma) {
        if (!restProxyEnabled || !supabaseRestBaseUrl) {
          res.status(503).json({ error: 'Collection assets unavailable without REST backend' });
          return;
        }

        const params = new URLSearchParams();
        params.set('canonical_col', formatCanonicalCollectionFilter(col));
        params.set('creator', `eq.${creator}`);
        params.set('limit', String(limit));
        params.set('offset', String(offset));
        params.set('order', safeQueryString(req.query.order) || 'created_at.desc,asset.desc');

        const status = parsePostgRESTComparison(req.query.status);
        const includeOrphaned = safeQueryString(req.query.includeOrphaned) === 'true';
        if (status?.value) params.set('status', `${status.op}.${status.value}`);
        else if (!includeOrphaned) params.set('status', 'neq.ORPHANED');

        const upstreamRes = await fetch(`${supabaseRestBaseUrl}/agents?${params.toString()}`, {
          method: 'GET',
          headers: buildSupabaseProxyHeaders(req),
        });

        for (const header of PROXY_RESPONSE_HEADER_ALLOWLIST) {
          const value = upstreamRes.headers.get(header);
          if (value) {
            res.setHeader(header, value);
          }
        }

        const payload = Buffer.from(await upstreamRes.arrayBuffer());
        res.status(upstreamRes.status).send(payload);
        return;
      }

      const where: Prisma.AgentWhereInput = { ...statusFilter };
      const collectionPointerVariantsList = collectionPointerVariants(col);
      if (collectionPointerVariantsList.length > 1) {
        where.OR = collectionPointerVariantsList.map((value) => ({ collectionPointer: value }));
      } else {
        where.collectionPointer = collectionPointerVariantsList[0] ?? col;
      }
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

      const digestFeedbackCounts = await loadLocalAgentDigestFeedbackCounts(prisma, agents.map((agent) => agent.id));
      res.json(agents.map((agent) => mapAgentToApi(agent, { feedbackCount: digestFeedbackCounts.get(agent.id) })));
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

      type CollectionStatsRow = {
        collection: string;
        registry_type: string;
        authority: string | null;
        agent_count: bigint | number;
        total_feedbacks: bigint | number;
        avg_score: number | null;
      };

      let formattedStats: Array<{
        collection: string;
        registry_type: string;
        authority: string | null;
        agent_count: number;
        total_feedbacks: number;
        avg_score: number | null;
      }>;

      if (prisma) {
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

          const agentCount = await prisma.agent.count({ where: agentWhere });
          const feedbackAgg = await prisma.feedback.aggregate({
            where: feedbackWhere,
            _count: true,
            _avg: { score: true },
          });
          const registry = await prisma.registry.findFirst({ where: registryWhere });

          formattedStats = [{
            collection,
            registry_type: registry?.registryType || 'USER',
            authority: registry?.authority || null,
            agent_count: agentCount,
            total_feedbacks: extractAggregateCount(feedbackAgg._count),
            avg_score: feedbackAgg._avg?.score || null,
          }];
        } else {
          const stats = includeOrphaned
            ? await prisma.$queryRaw<CollectionStatsRow[]>`
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
            : await prisma.$queryRaw<CollectionStatsRow[]>`
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

          formattedStats = stats.map((s) => ({
            collection: s.collection,
            registry_type: s.registry_type,
            authority: s.authority,
            agent_count: Number(s.agent_count),
            total_feedbacks: Number(s.total_feedbacks),
            avg_score: s.avg_score,
          }));
        }
      } else if (options.pool) {
        type PoolCollectionStatsRow = {
          collection: string;
          registry_type: string | null;
          authority: string | null;
          agent_count: string | number | null;
          total_feedbacks: string | number | null;
          avg_score: number | null;
        };

        const activeCollectionsClause = includeOrphaned ? '' : `WHERE c.status != 'ORPHANED'`;
        const activeAgentFilter = includeOrphaned ? '' : `WHERE status != 'ORPHANED'`;
        const activeFeedbackFilter = includeOrphaned
          ? ''
          : `WHERE f.status != 'ORPHANED' AND a.status != 'ORPHANED'`;

        if (collection) {
          const rows = await options.pool.query<PoolCollectionStatsRow>(
            `SELECT
               c.collection,
               c.registry_type,
               c.authority,
               COALESCE(agent_stats.agent_count, 0) AS agent_count,
               COALESCE(feedback_stats.total_feedbacks, 0) AS total_feedbacks,
               feedback_stats.avg_score
             FROM collections c
             LEFT JOIN (
               SELECT collection, COUNT(*)::text AS agent_count
               FROM agents
               ${activeAgentFilter}
               GROUP BY collection
             ) agent_stats ON agent_stats.collection = c.collection
             LEFT JOIN (
               SELECT a.collection, COUNT(f.id)::text AS total_feedbacks, AVG(f.score) AS avg_score
               FROM feedbacks f
               JOIN agents a ON a.asset = f.asset
               ${activeFeedbackFilter}
               GROUP BY a.collection
             ) feedback_stats ON feedback_stats.collection = c.collection
             WHERE c.collection = $1
             ${includeOrphaned ? '' : `AND c.status != 'ORPHANED'`}
             LIMIT 1`,
            [collection]
          );

          formattedStats = rows.rows.map((row) => ({
            collection: row.collection,
            registry_type: row.registry_type || 'USER',
            authority: row.authority,
            agent_count: Number(row.agent_count ?? 0),
            total_feedbacks: Number(row.total_feedbacks ?? 0),
            avg_score: row.avg_score,
          }));
        } else {
          const rows = await options.pool.query<PoolCollectionStatsRow>(
            `SELECT
               c.collection,
               c.registry_type,
               c.authority,
               COALESCE(agent_stats.agent_count, 0) AS agent_count,
               COALESCE(feedback_stats.total_feedbacks, 0) AS total_feedbacks,
               feedback_stats.avg_score
             FROM collections c
             LEFT JOIN (
               SELECT collection, COUNT(*)::text AS agent_count
               FROM agents
               ${activeAgentFilter}
               GROUP BY collection
             ) agent_stats ON agent_stats.collection = c.collection
             LEFT JOIN (
               SELECT a.collection, COUNT(f.id)::text AS total_feedbacks, AVG(f.score) AS avg_score
               FROM feedbacks f
               JOIN agents a ON a.asset = f.asset
               ${activeFeedbackFilter}
               GROUP BY a.collection
             ) feedback_stats ON feedback_stats.collection = c.collection
             ${activeCollectionsClause}
             ORDER BY c.created_at DESC
             LIMIT $1`,
            [MAX_COLLECTION_STATS]
          );

          formattedStats = rows.rows.map((row) => ({
            collection: row.collection,
            registry_type: row.registry_type || 'USER',
            authority: row.authority,
            agent_count: Number(row.agent_count ?? 0),
            total_feedbacks: Number(row.total_feedbacks ?? 0),
            avg_score: row.avg_score,
          }));
        }
      } else {
        res.status(503).json({ error: 'Collection stats unavailable without supported database backend' });
        return;
      }

      collectionStatsCache.set(cacheKey, formattedStats);
      const result = orderBy === 'agent_count.desc'
        ? [...formattedStats].sort((a, b) => b.agent_count - a.agent_count)
        : formattedStats;
      res.json(result);
    } catch (error) {
      logger.error({ error }, 'Error fetching collection stats');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/stats and /rest/v1/global_stats - Global stats
  const globalStatsHandler = async (req: Request, res: Response) => {
    try {
      const includeOrphaned = safeQueryString(req.query.includeOrphaned) === 'true';
      if (!prisma) {
        if (!options.pool) {
          res.status(503).json({ error: 'Global stats unavailable without database backend' });
          return;
        }

        type GlobalStatsRow = {
          total_agents: string | number | null;
          total_collections: string | number | null;
          total_feedbacks: string | number | null;
          platinum_agents: string | number | null;
          gold_agents: string | number | null;
          avg_quality: string | number | null;
        };
        const parseCount = (value: string | number | null | undefined): number => {
          if (value === null || value === undefined) return 0;
          if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
          const parsed = Number(value);
          return Number.isFinite(parsed) ? parsed : 0;
        };
        const parseNullableNumber = (value: string | number | null | undefined): number | null => {
          if (value === null || value === undefined) return null;
          if (typeof value === 'number') return Number.isFinite(value) ? value : null;
          const parsed = Number(value);
          return Number.isFinite(parsed) ? parsed : null;
        };

        try {
          const query = includeOrphaned
            ? `SELECT
                 (SELECT COUNT(*) FROM agents) AS total_agents,
                 (SELECT COUNT(*) FROM collection_pointers) AS total_collections,
                 (SELECT COUNT(*) FROM feedbacks) AS total_feedbacks,
                 (SELECT COUNT(*) FROM agents WHERE trust_tier = 4) AS platinum_agents,
                 (SELECT COUNT(*) FROM agents WHERE trust_tier = 3) AS gold_agents,
                 (SELECT ROUND(AVG(quality_score), 0) FROM agents WHERE feedback_count > 0) AS avg_quality`
            : `SELECT total_agents, total_collections, total_feedbacks, platinum_agents, gold_agents, avg_quality
               FROM global_stats
               LIMIT 1`;
          const result = await options.pool.query<GlobalStatsRow>(query);
          const row = result.rows[0] ?? null;
          res.json([{
            total_agents: parseCount(row?.total_agents),
            total_feedbacks: parseCount(row?.total_feedbacks),
            total_collections: parseCount(row?.total_collections),
            platinum_agents: parseCount(row?.platinum_agents),
            gold_agents: parseCount(row?.gold_agents),
            avg_quality: parseNullableNumber(row?.avg_quality),
          }]);
          return;
        } catch {
          const [fallbackRow] = await fetchFallbackGlobalStats(
            supabaseRestBaseUrl ?? '',
            buildSupabaseProxyHeaders(req),
            includeOrphaned
          );
          res.json([fallbackRow]);
          return;
        }
      }

      const agentWhere: Prisma.AgentWhereInput = includeOrphaned ? {} : { status: { not: 'ORPHANED' } };
      const feedbackWhere: Prisma.FeedbackWhereInput = includeOrphaned ? {} : { status: { not: 'ORPHANED' } };
      const [totalAgents, totalFeedbacks, totalCollections, platinumAgents, goldAgents, avgQuality] = await Promise.all([
        prisma.agent.count({ where: agentWhere }),
        prisma.feedback.count({ where: feedbackWhere }),
        prisma.collection.count({ where: {} }),
        prisma.agent.count({ where: { ...agentWhere, trustTier: 4 } }),
        prisma.agent.count({ where: { ...agentWhere, trustTier: 3 } }),
        prisma.agent.aggregate({
          where: { ...agentWhere, feedbackCount: { gt: 0 } },
          _avg: { qualityScore: true },
        }).then((result) => {
          const value = result._avg.qualityScore;
          return value === null || value === undefined ? null : Math.round(value);
        }),
      ]);

      res.json([{
        total_agents: totalAgents,
        total_feedbacks: totalFeedbacks,
        total_collections: totalCollections,
        platinum_agents: platinumAgents,
        gold_agents: goldAgents,
        avg_quality: avgQuality,
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
      const extractGroupCount = (value: unknown): number => {
        if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
        if (!value || typeof value !== 'object') return 0;
        const record = value as Record<string, unknown>;
        if (typeof record._all === 'number' && Number.isFinite(record._all)) {
          return record._all;
        }
        for (const candidate of Object.values(record)) {
          if (typeof candidate === 'number' && Number.isFinite(candidate)) {
            return candidate;
          }
        }
        return 0;
      };

      const toStatusMap = (groups: { _count: unknown; status?: string; chainStatus?: string }[]) => {
        const result: Record<string, number> = { PENDING: 0, FINALIZED: 0, ORPHANED: 0 };
        for (const g of groups) {
          const status = g.status || g.chainStatus || 'PENDING';
          result[status] = extractGroupCount(g._count);
        }
        return result;
      };

      if (!prisma) {
        if (!options.pool) {
          res.status(503).json({ error: 'Verification stats unavailable without database backend' });
          return;
        }

        type VerificationStatRow = {
          model: string;
          pending_count: string | number | null;
          finalized_count: string | number | null;
          orphaned_count: string | number | null;
        };
        const grouped = await options.pool.query<VerificationStatRow>(VERIFICATION_STATS_SQL);

        const parseCount = (value: string | number | null | undefined): number => {
          if (value === null || value === undefined) return 0;
          if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
          const parsed = Number(value);
          return Number.isFinite(parsed) ? parsed : 0;
        };

        const statusMaps: Record<string, Record<string, number>> = {
          agents: { PENDING: 0, FINALIZED: 0, ORPHANED: 0 },
          feedbacks: { PENDING: 0, FINALIZED: 0, ORPHANED: 0 },
          registries: { PENDING: 0, FINALIZED: 0, ORPHANED: 0 },
          metadata: { PENDING: 0, FINALIZED: 0, ORPHANED: 0 },
          validations: { PENDING: 0, FINALIZED: 0, ORPHANED: 0 },
          feedback_responses: { PENDING: 0, FINALIZED: 0, ORPHANED: 0 },
          revocations: { PENDING: 0, FINALIZED: 0, ORPHANED: 0 },
        };

        for (const row of grouped.rows) {
          const key = row.model === 'collections' ? 'registries' : row.model;
          if (!statusMaps[key]) continue;
          statusMaps[key] = {
            PENDING: parseCount(row.pending_count),
            FINALIZED: parseCount(row.finalized_count),
            ORPHANED: parseCount(row.orphaned_count),
          };
        }

        res.json({
          agents: statusMaps.agents,
          feedbacks: statusMaps.feedbacks,
          registries: statusMaps.registries,
          metadata: statusMaps.metadata,
          validations: statusMaps.validations,
          feedback_responses: statusMaps.feedback_responses,
          revocations: statusMaps.revocations,
        });
        return;
      }

      const [agents, feedbacks, registries, metadata, validations, responses, revocations, orphanFeedbackCount, orphanResponseCount] = await Promise.all([
        prisma.agent.groupBy({ by: ['status'], _count: true }),
        prisma.feedback.groupBy({ by: ['status'], _count: true }),
        prisma.registry.groupBy({ by: ['status'], _count: true }),
        prisma.agentMetadata.groupBy({ by: ['status'], _count: true }),
        typeof (prisma as any).validation?.groupBy === 'function'
          ? (prisma as any).validation.groupBy({ by: ['chainStatus'], _count: true })
          : Promise.resolve([]),
        prisma.feedbackResponse.groupBy({ by: ['status'], _count: true }),
        prisma.revocation.groupBy({ by: ['status'], _count: true }),
        typeof (prisma as any).orphanFeedback?.count === 'function'
          ? (prisma as any).orphanFeedback.count()
          : Promise.resolve(0),
        typeof (prisma as any).orphanResponse?.count === 'function'
          ? (prisma as any).orphanResponse.count()
          : Promise.resolve(0),
      ]);

      const feedbackStatuses = toStatusMap(feedbacks);
      feedbackStatuses.ORPHANED += orphanFeedbackCount;
      const responseStatuses = toStatusMap(responses);
      responseStatuses.ORPHANED += orphanResponseCount;

      res.json({
        agents: toStatusMap(agents),
        feedbacks: feedbackStatuses,
        registries: toStatusMap(registries),
        metadata: toStatusMap(metadata),
        validations: toStatusMap(validations),
        feedback_responses: responseStatuses,
        revocations: toStatusMap(revocations),
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
      const blockSlotFilter = parsePostgRESTBigIntFilter(req.query.block_slot);
      // Use stricter limit for metadata (each value can be up to 100KB compressed)
      const requestedLimit = safePaginationLimit(req.query.limit);
      const limit = Math.min(requestedLimit, MAX_METADATA_LIMIT);
      const offset = safePaginationOffset(req.query.offset);
      const order = safeQueryString(req.query.order);

      if (blockSlotFilter === null) {
        res.status(400).json({ error: 'Invalid block_slot filter value' });
        return;
      }

      const statusFilter = buildStatusFilter(req);
      if (isInvalidStatus(statusFilter)) {
        res.status(400).json({ error: 'Invalid status value. Allowed: PENDING, FINALIZED, ORPHANED' });
        return;
      }
      const where: Prisma.AgentMetadataWhereInput = { ...statusFilter };
      if (asset) where.agentId = asset;
      if (key) where.key = key;
      if (blockSlotFilter !== undefined) where.slot = blockSlotFilter;
      const metadataOrderBy: Prisma.AgentMetadataOrderByWithRelationInput[] = [
        ...(order === 'block_slot.asc'
          ? [{ slot: Prisma.SortOrder.asc }, { txIndex: Prisma.SortOrder.asc }, { eventOrdinal: Prisma.SortOrder.asc }]
          : [{ slot: Prisma.SortOrder.desc }, { txIndex: Prisma.SortOrder.desc }, { eventOrdinal: Prisma.SortOrder.desc }]),
        { agentId: Prisma.SortOrder.asc },
        { key: Prisma.SortOrder.asc },
        { id: Prisma.SortOrder.asc },
      ];

      const needsCount = wantsCount(req);
      const [metadata, totalCount] = await Promise.all([
        prisma.agentMetadata.findMany({
          where,
          orderBy: metadataOrderBy,
          take: limit,
          skip: offset,
        }),
        needsCount ? prisma.agentMetadata.count({ where }) : Promise.resolve(0),
      ]);

      // Decompress sequentially with aggregate size limit (prevent OOM)
      const totalStoredBytes = metadata.reduce((sum, m) => sum + Buffer.from(m.value).length, 0);
      if (totalStoredBytes > MAX_METADATA_AGGREGATE_BYTES) {
        logger.warn({
          totalStoredBytes,
          limit: MAX_METADATA_AGGREGATE_BYTES,
          totalItems: metadata.length,
        }, 'Metadata aggregate size limit exceeded');
        res.status(413).json({ error: 'Metadata aggregate size limit exceeded' });
        return;
      }

      const results = metadata.map((m) => {
        const keyHash = Buffer.from(computeKeyHash(m.key)).toString('hex');
        return {
          id: `${m.agentId}:${keyHash}`,
          asset: m.agentId,
          key: m.key,
          key_hash: keyHash,
          value: Buffer.from(m.value).toString('base64'),
          immutable: m.immutable,
          block_slot: Number(m.slot ?? 0n),
          tx_index: m.txIndex ?? null,
          event_ordinal: m.eventOrdinal ?? null,
          tx_signature: m.txSignature ?? '',
          status: m.status,
          verified_at: m.verifiedAt?.toISOString() || null,
        };
      });

      if (needsCount) {
        setContentRange(res, offset, results.length, totalCount);
      }

      res.json(results);
    } catch (error) {
      logger.error({ error }, 'Error fetching metadata');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  app.get('/rest/v1/agent_reputation', async (req: Request, res: Response) => {
    try {
      const asset = parsePostgRESTValue(req.query.asset);
      if (!asset) {
        res.status(400).json({ error: 'Missing required query param: asset' });
        return;
      }

      if (!prisma) {
        if (!options.pool) {
          res.status(503).json({ error: 'agent_reputation requires local Prisma backend or REST proxy mode' });
          return;
        }

        type PoolAgentReputationRow = {
          asset: string;
          owner: string;
          collection: string;
          nft_name: string | null;
          agent_uri: string | null;
          feedback_count: string | number | null;
          avg_score: number | null;
          positive_count: string | number | null;
          negative_count: string | number | null;
          validation_count: string | number | null;
        };

        const rows = await options.pool.query<PoolAgentReputationRow>(
          `WITH feedback_stats AS (
             SELECT
               f.asset,
               COUNT(*) FILTER (
                 WHERE f.status != 'ORPHANED'
                   AND f.is_revoked = false
                   AND f.score IS NOT NULL
               )::integer AS feedback_count,
               ROUND(AVG(f.score) FILTER (
                 WHERE f.status != 'ORPHANED'
                   AND f.is_revoked = false
                   AND f.score IS NOT NULL
               ), 0) AS avg_score,
               COUNT(*) FILTER (
                 WHERE f.status != 'ORPHANED'
                   AND f.is_revoked = false
                   AND f.score IS NOT NULL
                   AND f.score >= 50
               )::integer AS positive_count,
               COUNT(*) FILTER (
                 WHERE f.status != 'ORPHANED'
                   AND f.is_revoked = false
                   AND f.score IS NOT NULL
                   AND f.score < 50
               )::integer AS negative_count
             FROM feedbacks f
             GROUP BY f.asset
           ),
           validation_stats AS (
             SELECT
               v.asset,
               COUNT(*) FILTER (
                 WHERE v.chain_status != 'ORPHANED'
               )::integer AS validation_count
             FROM validations v
             GROUP BY v.asset
           )
           SELECT
             a.asset,
             a.owner,
             a.collection,
             a.nft_name,
             a.agent_uri,
             COALESCE(fs.feedback_count, a.feedback_count, 0) AS feedback_count,
             CASE
               WHEN COALESCE(fs.feedback_count, a.feedback_count, 0) > 0
                 THEN COALESCE(fs.avg_score, a.raw_avg_score::numeric)
               ELSE NULL
             END AS avg_score,
             COALESCE(fs.positive_count, 0) AS positive_count,
             COALESCE(fs.negative_count, 0) AS negative_count,
             COALESCE(vs.validation_count, 0) AS validation_count
           FROM agents a
           LEFT JOIN feedback_stats fs ON fs.asset = a.asset
           LEFT JOIN validation_stats vs ON vs.asset = a.asset
           WHERE a.asset = $1
             AND a.status != 'ORPHANED'
           LIMIT 1`,
          [asset]
        );

        if (rows.rows.length === 0) {
          res.json([]);
          return;
        }

        const row = rows.rows[0];
        res.json([{
          asset: row.asset,
          owner: row.owner,
          collection: row.collection,
          nft_name: normalizeNullableText(row.nft_name),
          agent_uri: row.agent_uri,
          feedback_count: numericValue(row.feedback_count) ?? 0,
          avg_score: roundNullableScore(row.avg_score),
          positive_count: numericValue(row.positive_count) ?? 0,
          negative_count: numericValue(row.negative_count) ?? 0,
          validation_count: numericValue(row.validation_count) ?? 0,
        } satisfies AgentReputationRow]);
        return;
      }

      const agent = await prisma.agent.findFirst({
        where: {
          id: asset,
          status: { not: 'ORPHANED' },
        },
        select: {
          id: true,
          owner: true,
          collection: true,
          nftName: true,
          uri: true,
          feedbackCount: true,
          rawAvgScore: true,
        },
      });

      if (!agent) {
        res.json([]);
        return;
      }

      const [feedbackAgg, positiveCount, negativeCount, validationCount] = await Promise.all([
        prisma.feedback.aggregate({
          where: {
            agentId: asset,
            status: { not: 'ORPHANED' },
            revoked: false,
            score: { not: null },
          },
          _count: true,
          _avg: { score: true },
        }),
        prisma.feedback.count({
          where: {
            agentId: asset,
            status: { not: 'ORPHANED' },
            revoked: false,
            score: { gte: 50 },
          },
        }),
        prisma.feedback.count({
          where: {
            agentId: asset,
            status: { not: 'ORPHANED' },
            revoked: false,
            score: { lt: 50 },
          },
        }),
        prisma.validation.count({
          where: {
            agentId: asset,
            chainStatus: { not: 'ORPHANED' },
          },
        }),
      ]);

      const feedbackCount = resolveFeedbackCount(feedbackAgg._count, agent.feedbackCount);

      const row: AgentReputationRow = {
        asset: agent.id,
        owner: agent.owner,
        collection: agent.collection,
        nft_name: normalizeNullableText(agent.nftName),
        agent_uri: agent.uri,
        feedback_count: feedbackCount,
        avg_score: resolveAverageScore(feedbackCount, feedbackAgg._avg.score, agent.rawAvgScore),
        positive_count: positiveCount,
        negative_count: negativeCount,
        validation_count: validationCount,
      };

      res.json([row]);
    } catch (error) {
      logger.error({ error }, 'Error fetching agent reputation');
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

      let withScores: LeaderboardEntry[];
      if (prisma) {
        const agents = await prisma.agent.findMany({
          where: {
            trustTier: { gte: 2 },
            ...(collection ? { collection } : {}),
            ...(includeOrphaned ? {} : { status: { not: 'ORPHANED' } }),
          },
          select: {
            id: true,
            owner: true,
            collection: true,
            nftName: true,
            uri: true,
            trustTier: true,
            qualityScore: true,
            confidence: true,
            riskScore: true,
            diversityRatio: true,
            feedbackCount: true,
          },
        });

        const digestFeedbackCounts = await loadLocalAgentDigestFeedbackCounts(prisma, agents.map((agent) => agent.id));
        withScores = agents
          .map((agent) => mapAgentToLeaderboardEntry({
            ...agent,
            feedbackCount: resolveAgentFeedbackCount(agent.feedbackCount, digestFeedbackCounts.get(agent.id)) ?? agent.feedbackCount,
          }))
          .sort((a, b) => {
            const aKey = BigInt(a.sort_key);
            const bKey = BigInt(b.sort_key);
            if (aKey === bKey) {
              return a.asset.localeCompare(b.asset);
            }
            return aKey > bKey ? -1 : 1;
          })
          .slice(0, LEADERBOARD_POOL_SIZE);
      } else if (options.pool) {
        const collectionParam = collection ? collectionPointerVariants(collection) : null;
        const { rows } = await options.pool.query<{
          asset: string;
          owner: string;
          collection: string | null;
          nft_name: string | null;
          agent_uri: string | null;
          trust_tier: string | number | null;
          quality_score: string | number | null;
          confidence: string | number | null;
          risk_score: string | number | null;
          diversity_ratio: string | number | null;
          feedback_count: string | number | null;
          sort_key: string | number;
        }>(
          `SELECT
             asset,
             owner,
             collection,
             nft_name,
             agent_uri,
             trust_tier,
             quality_score,
             confidence,
             risk_score,
             diversity_ratio,
             feedback_count,
             sort_key
           FROM agents
           WHERE trust_tier >= 2
             AND ($1::boolean OR status != 'ORPHANED')
             AND ($2::text[] IS NULL OR collection = ANY($2::text[]))
           ORDER BY sort_key DESC, asset ASC
           LIMIT $3::int`,
          [includeOrphaned, collectionParam, LEADERBOARD_POOL_SIZE],
        );
        const digestFeedbackCounts = await loadPoolAgentDigestFeedbackCounts(options.pool, rows.map((row) => row.asset));
        withScores = rows.map((row) => ({
          asset: row.asset,
          owner: row.owner,
          collection: row.collection,
          nft_name: normalizeNullableText(row.nft_name),
          agent_uri: row.agent_uri,
          trust_tier: numericValue(row.trust_tier) ?? 0,
          quality_score: numericValue(row.quality_score) ?? 0,
          confidence: numericValue(row.confidence) ?? 0,
          risk_score: numericValue(row.risk_score) ?? 0,
          diversity_ratio: numericValue(row.diversity_ratio) ?? 0,
          feedback_count: resolveAgentFeedbackCount(row.feedback_count, digestFeedbackCounts.get(row.asset)) ?? 0,
          sort_key: String(row.sort_key),
        }));
      } else {
        res.status(503).json({ error: 'leaderboard requires local Prisma backend or PostgreSQL pool' });
        return;
      }

      // Update LRU cache (TTL + max size handled automatically)
      leaderboardCache.set(cacheKey, withScores);

      res.json(withScores.slice(0, limit));
    } catch (error) {
      logger.error({ error }, 'Error fetching leaderboard');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  app.post('/rest/v1/rpc/get_leaderboard', async (req: Request, res: Response) => {
    try {
      const body = isRecord(req.body) ? req.body : {};
      const limit = Math.min(
        Math.max(Number.parseInt(String(body.p_limit ?? '50'), 10) || 50, 1),
        MAX_LIMIT
      );
      const minTier = Math.max(Number.parseInt(String(body.p_min_tier ?? '0'), 10) || 0, 0);
      const collection = typeof body.p_collection === 'string' && body.p_collection.trim().length > 0
        ? body.p_collection.trim()
        : null;
      const includeOrphaned = body.p_include_orphaned === true || body.p_include_orphaned === 'true';
      const cursorSortKey = typeof body.p_cursor_sort_key === 'string' && body.p_cursor_sort_key.trim().length > 0
        ? BigInt(body.p_cursor_sort_key)
        : body.p_cursor_sort_key === null || body.p_cursor_sort_key === undefined
          ? null
          : BigInt(String(body.p_cursor_sort_key));

      if (!prisma) {
        if (!options.pool) {
          res.status(503).json({ error: 'get_leaderboard requires local Prisma backend or REST proxy mode' });
          return;
        }

        const rows = await options.pool.query<PoolAgentApiRow>(
          `SELECT
             a.asset,
             a.owner,
             a.creator,
             a.agent_uri,
             a.agent_wallet,
             a.collection,
             a.canonical_col,
             a.col_locked,
             a.parent_asset,
             a.parent_creator,
             a.parent_locked,
             a.nft_name,
             a.atom_enabled,
             a.trust_tier,
             a.quality_score,
             a.confidence,
             a.risk_score,
             a.diversity_ratio,
             a.feedback_count,
             a.raw_avg_score,
             a.agent_id,
             a.status,
             a.verified_at,
             a.verified_slot,
             a.created_at,
             a.updated_at,
             a.tx_signature,
             a.block_slot,
             a.tx_index,
             a.event_ordinal,
             a.sort_key
           FROM agents a
           WHERE a.trust_tier >= $1
             AND ($2::text IS NULL OR a.collection = $2)
             AND ($3::boolean OR a.status != 'ORPHANED')
             AND ($4::bigint IS NULL OR a.sort_key < $4)
           ORDER BY a.sort_key DESC, a.asset ASC
           LIMIT $5`,
          [minTier, collection, includeOrphaned, cursorSortKey?.toString() ?? null, limit]
        );

        const digestFeedbackCounts = await loadPoolAgentDigestFeedbackCounts(options.pool, rows.rows.map((row) => row.asset));
        res.json(rows.rows.map((row) => mapPoolAgentToApi(row, { feedbackCount: digestFeedbackCounts.get(row.asset) })));
        return;
      }

      const agents = await prisma.agent.findMany({
        where: {
          trustTier: { gte: minTier },
          ...(collection ? { collection } : {}),
          ...(includeOrphaned ? {} : { status: { not: 'ORPHANED' } }),
        },
        select: {
          id: true,
          owner: true,
          creator: true,
          uri: true,
          wallet: true,
          collection: true,
          collectionPointer: true,
          colLocked: true,
          parentAsset: true,
          parentCreator: true,
          parentLocked: true,
          nftName: true,
          atomEnabled: true,
          trustTier: true,
          qualityScore: true,
          confidence: true,
          riskScore: true,
          diversityRatio: true,
          feedbackCount: true,
          rawAvgScore: true,
          agentId: true,
          status: true,
          verifiedAt: true,
          verifiedSlot: true,
          createdAt: true,
          updatedAt: true,
          createdTxSignature: true,
          createdSlot: true,
          txIndex: true,
          eventOrdinal: true,
        },
      });

      const digestFeedbackCounts = await loadLocalAgentDigestFeedbackCounts(prisma, agents.map((agent) => agent.id));
      const rows: LocalLeaderboardRow[] = agents
        .map((agent) => ({
          ...mapAgentToApi(agent, { feedbackCount: digestFeedbackCounts.get(agent.id) }),
          sort_key: computeLocalSortKey(agent),
        }) as LocalLeaderboardRow)
        .filter((agent) => cursorSortKey === null || BigInt(String(agent.sort_key)) < cursorSortKey)
        .sort((a, b) => {
          const aKey = BigInt(String(a.sort_key));
          const bKey = BigInt(String(b.sort_key));
          if (aKey === bKey) {
            return String(a.asset).localeCompare(String(b.asset));
          }
          return aKey > bKey ? -1 : 1;
        })
        .slice(0, limit);

      res.json(rows);
    } catch (error) {
      logger.error({ error }, 'Error fetching RPC leaderboard');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  app.get('/rest/v1/rpc/get_collection_agents', async (req: Request, res: Response) => {
    try {
      const collectionId = parsePostgRESTInt(req.query.collection_id);
      const pageLimit = parsePostgRESTInt(req.query.page_limit);
      const pageOffset = parsePostgRESTInt(req.query.page_offset);

      if (collectionId === null || pageLimit === null || pageOffset === null) {
        res.status(400).json({ error: 'Invalid collection_id/page_limit/page_offset filter value' });
        return;
      }
      if (collectionId === undefined) {
        res.status(400).json({ error: 'Missing required query param: collection_id' });
        return;
      }

      const limit = Math.min(Math.max(pageLimit ?? 20, 1), MAX_LIMIT);
      const offset = Math.max(pageOffset ?? 0, 0);

      if (!prisma) {
        if (!options.pool) {
          res.status(503).json({ error: 'get_collection_agents requires local Prisma backend or REST proxy mode' });
          return;
        }

        type PoolCollectionAgentRow = {
          asset: string;
          owner: string;
          collection: string;
          nft_name: string | null;
          agent_uri: string | null;
          feedback_count: string | number | null;
          avg_score: number | null;
          positive_count: string | number | null;
          negative_count: string | number | null;
          validation_count: string | number | null;
        };

        const rows = await options.pool.query<PoolCollectionAgentRow>(
          `WITH feedback_stats AS (
             SELECT
               f.asset,
               COUNT(*) FILTER (
                 WHERE f.status != 'ORPHANED'
                   AND f.is_revoked = false
                   AND f.score IS NOT NULL
               )::integer AS feedback_count,
               ROUND(AVG(f.score) FILTER (
                 WHERE f.status != 'ORPHANED'
                   AND f.is_revoked = false
                   AND f.score IS NOT NULL
               ), 0) AS avg_score,
               COUNT(*) FILTER (
                 WHERE f.status != 'ORPHANED'
                   AND f.is_revoked = false
                   AND f.score IS NOT NULL
                   AND f.score >= 50
               )::integer AS positive_count,
               COUNT(*) FILTER (
                 WHERE f.status != 'ORPHANED'
                   AND f.is_revoked = false
                   AND f.score IS NOT NULL
                   AND f.score < 50
               )::integer AS negative_count
             FROM feedbacks f
             GROUP BY f.asset
           ),
           validation_stats AS (
             SELECT
               v.asset,
               COUNT(*) FILTER (
                 WHERE v.chain_status != 'ORPHANED'
               )::integer AS validation_count
             FROM validations v
             GROUP BY v.asset
           )
           SELECT
             a.asset,
             a.owner,
             a.collection,
             a.nft_name,
             a.agent_uri,
             COALESCE(fs.feedback_count, a.feedback_count, 0) AS feedback_count,
             CASE
               WHEN COALESCE(fs.feedback_count, a.feedback_count, 0) > 0
                 THEN COALESCE(fs.avg_score, a.raw_avg_score::numeric)
               ELSE NULL
             END AS avg_score,
             COALESCE(fs.positive_count, 0) AS positive_count,
             COALESCE(fs.negative_count, 0) AS negative_count,
             COALESCE(vs.validation_count, 0) AS validation_count
           FROM collection_pointers cp
           JOIN agents a
             ON a.canonical_col = cp.col
            AND a.creator = cp.creator
           LEFT JOIN feedback_stats fs ON fs.asset = a.asset
           LEFT JOIN validation_stats vs ON vs.asset = a.asset
           WHERE cp.collection_id = $1
             AND a.status != 'ORPHANED'
           ORDER BY a.created_at DESC, a.asset DESC
           LIMIT $2
           OFFSET $3`,
          [collectionId, limit, offset]
        );

        res.json(rows.rows.map((row) => ({
          asset: row.asset,
          owner: row.owner,
          collection: row.collection,
          nft_name: normalizeNullableText(row.nft_name),
          agent_uri: row.agent_uri,
          feedback_count: numericValue(row.feedback_count) ?? 0,
          avg_score: roundNullableScore(row.avg_score),
          positive_count: numericValue(row.positive_count) ?? 0,
          negative_count: numericValue(row.negative_count) ?? 0,
          validation_count: numericValue(row.validation_count) ?? 0,
        })));
        return;
      }

      const collection = await prisma.collection.findUnique({
        where: { collectionId: BigInt(collectionId) },
        select: { col: true, creator: true },
      });

      if (!collection) {
        res.json([]);
        return;
      }

      const agents = await prisma.agent.findMany({
        where: {
          collectionPointer: collection.col,
          creator: collection.creator,
          status: { not: 'ORPHANED' },
        },
        orderBy: [{ createdAt: 'desc' }, { id: 'desc' }],
        take: limit,
        skip: offset,
        select: {
          id: true,
          owner: true,
          collection: true,
          nftName: true,
          uri: true,
          feedbackCount: true,
          rawAvgScore: true,
        },
      });

      const assetIds = agents.map((agent) => agent.id);
      const [feedbackStats, validationStats] = await Promise.all([
        assetIds.length === 0
          ? Promise.resolve([])
          : prisma.feedback.groupBy({
              by: ['agentId'],
              where: {
                agentId: { in: assetIds },
                status: { not: 'ORPHANED' },
                revoked: false,
                score: { not: null },
              },
              _count: true,
              _avg: { score: true },
            }),
        assetIds.length === 0
          ? Promise.resolve([])
          : prisma.validation.groupBy({
              by: ['agentId'],
              where: {
                agentId: { in: assetIds },
                chainStatus: { not: 'ORPHANED' },
              },
              _count: true,
            }),
      ]);

      const positiveByAgent = new Map<string, number>();
      const negativeByAgent = new Map<string, number>();
      if (assetIds.length > 0) {
        const [positiveRows, negativeRows] = await Promise.all([
          prisma.feedback.groupBy({
            by: ['agentId'],
            where: {
              agentId: { in: assetIds },
              status: { not: 'ORPHANED' },
              revoked: false,
              score: { gte: 50 },
            },
            _count: true,
          }),
          prisma.feedback.groupBy({
            by: ['agentId'],
            where: {
              agentId: { in: assetIds },
              status: { not: 'ORPHANED' },
              revoked: false,
              score: { lt: 50 },
            },
            _count: true,
          }),
        ]);
        for (const row of positiveRows) positiveByAgent.set(row.agentId, row._count);
        for (const row of negativeRows) negativeByAgent.set(row.agentId, row._count);
      }

      const feedbackByAgent = new Map(feedbackStats.map((row) => [
        row.agentId,
        {
          feedback_count: resolveFeedbackCount(row._count, null),
          avg_score: roundNullableScore(row._avg.score),
        },
      ]));
      const validationByAgent = new Map(validationStats.map((row) => [row.agentId, Number(row._count ?? 0)]));

      res.json(agents.map((agent) => {
        const feedback = feedbackByAgent.get(agent.id);
        const feedbackCount = feedback?.feedback_count ?? resolveFeedbackCount(null, agent.feedbackCount);
        return {
          asset: agent.id,
          owner: agent.owner,
          collection: agent.collection,
          nft_name: normalizeNullableText(agent.nftName),
          agent_uri: agent.uri,
          feedback_count: feedbackCount,
          avg_score: feedback?.avg_score ?? resolveAverageScore(feedbackCount, null, agent.rawAvgScore),
          positive_count: positiveByAgent.get(agent.id) ?? 0,
          negative_count: negativeByAgent.get(agent.id) ?? 0,
          validation_count: validationByAgent.get(agent.id) ?? 0,
        };
      }));
    } catch (error) {
      logger.error({ error }, 'Error fetching collection agents');
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // GET /rest/v1/checkpoints/:asset - All checkpoints for an agent
  app.get('/rest/v1/checkpoints/:asset', async (req: Request, res: Response) => {
    try {
      const asset = safeQueryString(req.params.asset);
      if (!asset) { res.status(400).json({ error: 'asset parameter required' }); return; }
      if (!BASE58_REGEX.test(asset)) { res.status(400).json({ error: 'Invalid asset: must be a base58-encoded public key (32-44 chars)' }); return; }
      const limit = safePaginationLimit(req.query.limit);
      const offset = safePaginationOffset(req.query.offset);
      const chainTypeRaw = safeQueryString(req.query.chainType);
      const chainType = isReplayChainType(chainTypeRaw) ? chainTypeRaw : undefined;
      if (chainTypeRaw && !chainType) {
        res.json([]);
        return;
      }

      if (!prisma) {
        if (!options.pool) {
          res.status(503).json({ error: 'Checkpoint endpoints unavailable without supported database backend' });
          return;
        }

        const checkpoints = await listCheckpointsFromPool(options.pool, asset, chainType);
        res.json(
          checkpoints.slice(offset, offset + limit).map((cp) => ({
            agent_id: asset,
            chain_type: cp.chainType,
            event_count: checkpointEventCountToJson(cp.eventCount),
            digest: cp.digest,
            created_at: cp.createdAt,
          }))
        );
        return;
      }

      const checkpoints = await listCheckpointsFromPrisma(prisma, asset, chainType);

      res.json(checkpoints.slice(offset, offset + limit).map(cp => ({
        agent_id: asset,
        chain_type: cp.chainType,
        event_count: checkpointEventCountToJson(cp.eventCount),
        digest: cp.digest,
        created_at: cp.createdAt,
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

      if (!prisma) {
        if (!options.pool) {
          res.status(503).json({ error: 'Checkpoint endpoints unavailable without supported database backend' });
          return;
        }

        const checkpoints = await getLatestCheckpointsFromPool(options.pool, asset);
        const fmt = (cp: typeof checkpoints.feedback) => cp ? {
          event_count: checkpointEventCountToJson(cp.eventCount),
          digest: cp.digest,
          created_at: cp.createdAt,
        } : null;

        res.json({
          feedback: fmt(checkpoints.feedback),
          response: fmt(checkpoints.response),
          revoke: fmt(checkpoints.revoke),
        });
        return;
      }

      const { feedback, response, revoke } = await getLatestCheckpointsFromPrisma(prisma, asset);

      const fmt = (cp: typeof feedback) => cp ? {
        event_count: checkpointEventCountToJson(cp.eventCount),
        digest: cp.digest,
        created_at: cp.createdAt,
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

      if (!prisma && !options.pool) {
        res.status(503).json({ error: 'Replay verification unavailable without supported database backend' });
        return;
      }

      const verifier = prisma ? new ReplayVerifier(prisma) : new PoolReplayVerifier(options.pool!);
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

      if (!prisma) {
        if (!options.pool) {
          res.status(503).json({ error: 'Replay data unavailable without supported database backend' });
          return;
        }

        const replayData = await fetchReplayDataFromPool(
          options.pool,
          asset,
          chainType as ReplayChainType,
          fromCount,
          toCount,
          limit,
        );
        res.json(replayData);
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

        const fetchTake = limit + 1;
        const [events, orphanEvents] = await Promise.all([
          prisma.feedback.findMany({
            where,
            orderBy: { feedbackIndex: 'asc' },
            take: fetchTake,
          }),
          typeof (prisma as any).orphanFeedback?.findMany === 'function'
            ? (prisma as any).orphanFeedback.findMany({
                where: {
                  agentId: asset,
                  feedbackIndex: toCount !== undefined
                    ? { gte: BigInt(fromCount), lt: BigInt(toCount) }
                    : { gte: BigInt(fromCount) },
                },
                orderBy: { feedbackIndex: 'asc' },
                take: fetchTake,
              })
            : Promise.resolve([]),
        ]);

        const combined = [
          ...events.map((f) => ({
            order: {
              count: BigInt(f.feedbackIndex),
              slot: f.createdSlot ?? null,
              txIndex: f.txIndex ?? null,
              eventOrdinal: f.eventOrdinal ?? null,
              signature: f.createdTxSignature ?? null,
              rowId: f.id,
            },
            payload: {
              asset: f.agentId,
              client: f.client,
              feedback_index: f.feedbackIndex.toString(),
              feedback_hash: f.feedbackHash ? Buffer.from(f.feedbackHash).toString('hex') : null,
              slot: f.createdSlot ? Number(f.createdSlot) : 0,
              running_digest: f.runningDigest ? Buffer.from(f.runningDigest).toString('hex') : null,
            },
          })),
          ...orphanEvents.map((f: any) => ({
            order: {
              count: BigInt(f.feedbackIndex),
              slot: f.slot ?? null,
              txIndex: f.txIndex ?? null,
              eventOrdinal: f.eventOrdinal ?? null,
              signature: f.txSignature ?? null,
              rowId: f.id,
            },
            payload: {
              asset: f.agentId,
              client: f.client,
              feedback_index: f.feedbackIndex.toString(),
              feedback_hash: f.feedbackHash ? Buffer.from(f.feedbackHash).toString('hex') : null,
              slot: f.slot ? Number(f.slot) : 0,
              running_digest: f.runningDigest ? Buffer.from(f.runningDigest).toString('hex') : null,
            },
          })),
        ].sort((left, right) => compareReplayEventOrder(left.order, right.order));
        const limited = combined.slice(0, limit);

        res.json({
          events: limited.map((entry) => entry.payload),
          hasMore: combined.length > limit,
          nextFromCount: limited.length > 0
            ? Number(limited[limited.length - 1].order.count) + 1
            : fromCount,
        });
      } else if (chainType === 'response') {
        const where: Prisma.FeedbackResponseWhereInput = {
          feedback: {
            agentId: asset,
          },
        };
        const rcFilter: { gte: bigint; lt?: bigint } = { gte: BigInt(fromCount) };
        if (toCount !== undefined) rcFilter.lt = BigInt(toCount);
        where.responseCount = rcFilter;

        const fetchTake = limit + 1;
        const [events, orphanEvents] = await Promise.all([
          prisma.feedbackResponse.findMany({
            where,
            orderBy: { responseCount: 'asc' },
            take: fetchTake,
            include: {
              feedback: {
                select: { agentId: true, client: true, feedbackIndex: true, feedbackHash: true },
              },
            },
          }),
          typeof (prisma as any).orphanResponse?.findMany === 'function'
            ? (prisma as any).orphanResponse.findMany({
                where: {
                  agentId: asset,
                  responseCount: rcFilter,
                },
                orderBy: { responseCount: 'asc' },
                take: fetchTake,
              })
            : Promise.resolve([]),
        ]);

        const combined = [
          ...events.map((r) => ({
            order: {
              count: BigInt(r.responseCount ?? 0),
              slot: r.slot ?? null,
              txIndex: r.txIndex ?? null,
              eventOrdinal: r.eventOrdinal ?? null,
              signature: r.txSignature ?? null,
              rowId: r.id,
            },
            payload: {
              asset: r.feedback.agentId,
              client: r.feedback.client,
              feedback_index: r.feedback.feedbackIndex.toString(),
              responder: r.responder,
              response_hash: r.responseHash ? Buffer.from(r.responseHash).toString('hex') : null,
              feedback_hash: r.sealHash ? Buffer.from(r.sealHash).toString('hex') : null,
              slot: r.slot ? Number(r.slot) : 0,
              running_digest: r.runningDigest ? Buffer.from(r.runningDigest).toString('hex') : null,
              response_count: r.responseCount != null ? Number(r.responseCount) : null,
            },
          })),
          ...orphanEvents.map((r: any) => ({
            order: {
              count: BigInt(r.responseCount ?? 0),
              slot: r.slot ?? null,
              txIndex: r.txIndex ?? null,
              eventOrdinal: r.eventOrdinal ?? null,
              signature: r.txSignature ?? null,
              rowId: r.id,
            },
            payload: {
              asset: r.agentId,
              client: r.client,
              feedback_index: r.feedbackIndex.toString(),
              responder: r.responder,
              response_hash: r.responseHash ? Buffer.from(r.responseHash).toString('hex') : null,
              feedback_hash: r.sealHash ? Buffer.from(r.sealHash).toString('hex') : null,
              slot: r.slot ? Number(r.slot) : 0,
              running_digest: r.runningDigest ? Buffer.from(r.runningDigest).toString('hex') : null,
              response_count: r.responseCount != null ? Number(r.responseCount) : null,
            },
          })),
        ].sort((left, right) => compareReplayEventOrder(left.order, right.order));
        const limited = combined.slice(0, limit);

        res.json({
          events: limited.map((entry) => entry.payload),
          hasMore: combined.length > limit,
          nextFromCount: limited.length > 0
            ? Number(limited[limited.length - 1].order.count) + 1
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
          take: limit + 1,
        });

        res.json({
          events: events.slice(0, limit).map(r => ({
            asset: r.agentId,
            client: r.client,
            feedback_index: r.feedbackIndex.toString(),
            feedback_hash: r.feedbackHash ? Buffer.from(r.feedbackHash).toString('hex') : null,
            slot: Number(r.slot),
            running_digest: r.runningDigest ? Buffer.from(r.runningDigest).toString('hex') : null,
            revoke_count: r.revokeCount.toString(),
          })),
          hasMore: events.length > limit,
          nextFromCount: events.length > 0
            ? Number(events[Math.min(events.length, limit) - 1].revokeCount) + 1
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
    const graphqlLimiter = rateLimit({
      windowMs: GRAPHQL_RATE_LIMIT_WINDOW_MS,
      max: GRAPHQL_RATE_LIMIT_MAX_REQUESTS,
      message: {
        error: `GraphQL rate limited. Max ${GRAPHQL_RATE_LIMIT_MAX_REQUESTS} requests per minute.`,
      },
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
    graphqlState.mounted = true;
    logger.info('GraphQL endpoint mounted at /v2/graphql (legacy alias /graphql)');
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
