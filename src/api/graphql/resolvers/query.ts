import type { GraphQLContext } from '../context.js';
import { decodeFeedbackId, decodeResponseId, decodeValidationId, decodeAgentId } from '../utils/ids.js';
import { clampFirst, clampSkip, MAX_FIRST } from '../utils/pagination.js';
import { buildWhereClause } from '../utils/filters.js';
import { createBadUserInputError } from '../utils/errors.js';
import type { AgentRow, FeedbackRow, ResponseRow, ValidationRow } from '../dataloaders.js';
import { config } from '../../../config.js';

const ORDER_MAP_AGENT: Record<string, string> = {
  createdAt: 'created_at',
  updatedAt: 'updated_at',
  totalFeedback: 'feedback_count',
  qualityScore: 'quality_score',
  trustTier: 'trust_tier',
};

const ORDER_MAP_FEEDBACK: Record<string, string> = {
  createdAt: 'created_at',
  value: 'value',
  feedbackIndex: 'feedback_index',
};

const ORDER_MAP_RESPONSE: Record<string, string> = {
  createdAt: 'created_at',
};

const MAX_TREE_DEPTH = 8;
const MAX_LINEAGE_DEPTH = 32;
const MAX_AGENT_PAGE_SIZE = 1000;

const AGENT_SELECT_WITH_DIGESTS = `a.asset,
                          a.owner,
                          a.creator,
                          a.agent_uri,
                          a.agent_wallet,
                          a.collection,
                          a.canonical_col AS collection_pointer,
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
                          a.sort_key,
                          a.status,
                          a.verified_at,
                          a.created_at,
                          a.updated_at,
                          a.tx_signature AS created_tx_signature,
                          a.block_slot::text AS created_slot,
                          adc.feedback_digest::text AS feedback_digest,
                          adc.response_digest::text AS response_digest,
                          adc.revoke_digest::text AS revoke_digest,
                          COALESCE(adc.digest_feedback_count::text, a.feedback_count::text, '0') AS feedback_count,
                          COALESCE(adc.digest_response_count::text, '0') AS response_count,
                          COALESCE(adc.digest_revoke_count::text, '0') AS revoke_count`;

interface DecodedCursor {
  created_at: string;
  asset?: string;
  id?: string;
}

interface CollectionPointerRow {
  col: string;
  creator: string;
  first_seen_asset: string;
  first_seen_at: string;
  first_seen_slot: string;
  first_seen_tx_signature: string | null;
  last_seen_at: string;
  last_seen_slot: string;
  last_seen_tx_signature: string | null;
  asset_count: string;
}

interface AgentTreeNodeRow {
  asset: string;
  parent_asset: string | null;
  path: string[];
  depth: number;
}

interface AggregatedStats {
  totalAgents: string;
  totalFeedback: string;
  totalValidations: string;
  tags: string[];
}

interface CachedAggregatedStats {
  value: AggregatedStats;
  expiresAt: number;
}

const STATS_CACHE_TTL_MS = config.graphqlStatsCacheTtlMs;
const statsCacheByNetwork = new Map<string, CachedAggregatedStats>();
const statsRefreshByNetwork = new Map<string, Promise<AggregatedStats>>();

function resolveDirection(dir: string | undefined | null): 'ASC' | 'DESC' {
  return dir === 'asc' ? 'ASC' : 'DESC';
}

function decodeFlexibleCursor(cursor: string): DecodedCursor | null {
  try {
    const json = Buffer.from(cursor, 'base64').toString('utf-8');
    const parsed: unknown = JSON.parse(json);
    if (!parsed || typeof parsed !== 'object') return null;
    const obj = parsed as Record<string, unknown>;
    if (typeof obj.created_at !== 'string') return null;
    const asset = typeof obj.asset === 'string' ? obj.asset : undefined;
    const id = typeof obj.id === 'string' ? obj.id : undefined;
    return { created_at: obj.created_at, asset, id };
  } catch {
    return null;
  }
}

function assertNoMixedCursorOffset(after: string | undefined, skip: number): void {
  if (after && skip > 0) {
    throw createBadUserInputError('Cannot combine cursor pagination (after) with offset pagination (skip).');
  }
}

function assertCursorOrderCompatibility(after: string | undefined, orderCol: string): void {
  if (after && orderCol !== 'created_at') {
    throw createBadUserInputError('The after cursor is only supported when orderBy is createdAt.');
  }
}

function resolveAssetId(input: string): string {
  return decodeAgentId(input) ?? input;
}

function clampTreeDepth(depth: number | undefined): number {
  if (depth === undefined || depth === null || Number.isNaN(depth)) return 5;
  if (depth < 0) return 0;
  return Math.min(depth, MAX_TREE_DEPTH);
}

function clampAgentPageSize(first: number | undefined): number {
  if (first === undefined || first === null || Number.isNaN(first) || first <= 0) return 100;
  return Math.min(first, MAX_AGENT_PAGE_SIZE);
}

function resolveAgentOrderColumn(orderBy: string | undefined): string {
  return ORDER_MAP_AGENT[orderBy ?? 'createdAt'] ?? 'created_at';
}

function toUnixTimestamp(dateStr: string): string {
  return String(Math.floor(new Date(dateStr).getTime() / 1000));
}

async function fetchAggregatedStats(ctx: GraphQLContext): Promise<AggregatedStats> {
  const { rows } = await ctx.pool.query<{
    total_agents: string;
    total_feedback: string;
    total_validations: string;
    tags: string[] | null;
  }>(
    `SELECT
       (SELECT COUNT(*)::text FROM agents WHERE status != 'ORPHANED') AS total_agents,
       (SELECT COUNT(*)::text FROM feedbacks WHERE status != 'ORPHANED') AS total_feedback,
       (SELECT COUNT(*)::text FROM validations WHERE chain_status != 'ORPHANED') AS total_validations,
       COALESCE((
         SELECT ARRAY(
           SELECT tag FROM (
             SELECT tag1 AS tag FROM feedbacks WHERE tag1 != '' AND status != 'ORPHANED'
             UNION ALL
             SELECT tag2 AS tag FROM feedbacks WHERE tag2 != '' AND status != 'ORPHANED'
           ) t
           GROUP BY tag
           LIMIT 100
         )
       ), ARRAY[]::text[]) AS tags`
  );

  return {
    totalAgents: rows[0]?.total_agents ?? '0',
    totalFeedback: rows[0]?.total_feedback ?? '0',
    totalValidations: rows[0]?.total_validations ?? '0',
    tags: rows[0]?.tags ?? [],
  };
}

function refreshAggregatedStats(
  cacheKey: string,
  ctx: GraphQLContext
): Promise<AggregatedStats> {
  const existing = statsRefreshByNetwork.get(cacheKey);
  if (existing) return existing;

  const pending = fetchAggregatedStats(ctx)
    .then((value) => {
      statsCacheByNetwork.set(cacheKey, {
        value,
        expiresAt: Date.now() + STATS_CACHE_TTL_MS,
      });
      return value;
    })
    .finally(() => {
      statsRefreshByNetwork.delete(cacheKey);
    });

  statsRefreshByNetwork.set(cacheKey, pending);
  return pending;
}

async function loadAggregatedStats(ctx: GraphQLContext): Promise<AggregatedStats> {
  const cacheKey = ctx.networkMode;
  const now = Date.now();
  const cached = statsCacheByNetwork.get(cacheKey);

  if (cached && cached.expiresAt > now) {
    return cached.value;
  }

  if (cached) {
    // Serve stale result immediately and refresh asynchronously.
    void refreshAggregatedStats(cacheKey, ctx).catch(() => undefined);
    return cached.value;
  }

  return refreshAggregatedStats(cacheKey, ctx);
}

export function resetAggregatedStatsCacheForTests(): void {
  statsCacheByNetwork.clear();
  statsRefreshByNetwork.clear();
}

export const queryResolvers = {
  Query: {
    async agent(_: unknown, args: { id: string }, ctx: GraphQLContext) {
      const asset = decodeAgentId(args.id);
      if (!asset) return null;
      return ctx.loaders.agentById.load(asset);
    },

    async agents(
      _: unknown,
      args: {
        first?: number; skip?: number; after?: string;
        where?: Record<string, unknown>;
        orderBy?: string; orderDirection?: string;
      },
      ctx: GraphQLContext
    ) {
      const first = clampFirst(args.first);
      const skip = clampSkip(args.skip);
      const dir = resolveDirection(args.orderDirection);
      const orderCol = resolveAgentOrderColumn(args.orderBy);

      assertNoMixedCursorOffset(args.after, skip);
      assertCursorOrderCompatibility(args.after, orderCol);

      const where = buildWhereClause('agent', args.where);
      const params = [...where.params];
      let paramIdx = where.paramIndex;

      let cursorSql = '';
      if (args.after) {
        const cursor = decodeFlexibleCursor(args.after);
        if (!cursor || !cursor.asset) {
          throw createBadUserInputError('Invalid agents cursor. Expected base64 JSON with created_at and asset.');
        }
        const op = dir === 'DESC' ? '<' : '>';
        cursorSql = ` AND (created_at, asset) ${op} ($${paramIdx}::timestamptz, $${paramIdx + 1}::text)`;
        params.push(cursor.created_at, cursor.asset);
        paramIdx += 2;
      }

      const sql = `SELECT
                          ${AGENT_SELECT_WITH_DIGESTS}
                   FROM agents a
                   LEFT JOIN (
                     SELECT
                       agent_id,
                       feedback_digest,
                       response_digest,
                       revoke_digest,
                       feedback_count AS digest_feedback_count,
                       response_count AS digest_response_count,
                       revoke_count AS digest_revoke_count
                     FROM agent_digest_cache
                   ) adc ON adc.agent_id = a.asset
                   ${where.sql}${cursorSql}
                   ORDER BY ${orderCol} ${dir}, a.asset ${dir}
                   LIMIT $${paramIdx}::int OFFSET $${paramIdx + 1}::int`;
      params.push(first, skip);

      const { rows } = await ctx.pool.query<AgentRow>(sql, params);

      for (const row of rows) {
        ctx.loaders.agentById.prime(row.asset, row);
      }

      return rows;
    },

    async feedback(_: unknown, args: { id: string }, ctx: GraphQLContext) {
      const decoded = decodeFeedbackId(args.id);
      if (!decoded) return null;

      const { rows } = await ctx.pool.query<FeedbackRow>(
        `SELECT id, asset, client_address, feedback_index, value, value_decimals,
                score, tag1, tag2, endpoint, feedback_uri, feedback_hash,
                running_digest, is_revoked, status, verified_at,
                tx_signature, block_slot, created_at, revoked_at
         FROM feedbacks
         WHERE asset = $1 AND client_address = $2 AND feedback_index = $3
         AND status != 'ORPHANED'`,
        [decoded.asset, decoded.client, decoded.index]
      );
      return rows[0] ?? null;
    },

    async feedbacks(
      _: unknown,
      args: {
        first?: number; skip?: number; after?: string;
        where?: Record<string, unknown>;
        orderBy?: string; orderDirection?: string;
      },
      ctx: GraphQLContext
    ) {
      const first = clampFirst(args.first);
      const skip = clampSkip(args.skip);
      const dir = resolveDirection(args.orderDirection);
      const orderCol = ORDER_MAP_FEEDBACK[args.orderBy ?? 'createdAt'] ?? 'created_at';

      assertNoMixedCursorOffset(args.after, skip);
      assertCursorOrderCompatibility(args.after, orderCol);

      const where = buildWhereClause('feedback', args.where);
      const params = [...where.params];
      let paramIdx = where.paramIndex;

      let cursorSql = '';
      if (args.after) {
        const cursor = decodeFlexibleCursor(args.after);
        if (!cursor || !cursor.asset) {
          throw createBadUserInputError('Invalid feedbacks cursor. Expected base64 JSON with created_at and asset.');
        }
        const op = dir === 'DESC' ? '<' : '>';
        const cursorId = cursor.id ?? '';
        cursorSql = ` AND (created_at, asset, id) ${op} ($${paramIdx}::timestamptz, $${paramIdx + 1}::text, $${paramIdx + 2}::text)`;
        params.push(cursor.created_at, cursor.asset, cursorId);
        paramIdx += 3;
      }

      const sql = `SELECT id, asset, client_address, feedback_index, value, value_decimals,
                          score, tag1, tag2, endpoint, feedback_uri, feedback_hash,
                          running_digest, is_revoked, status, verified_at,
                          tx_signature, block_slot, created_at, revoked_at
                   FROM feedbacks ${where.sql}${cursorSql}
                   ORDER BY ${orderCol} ${dir}, asset ${dir}, id ${dir}
                   LIMIT $${paramIdx}::int OFFSET $${paramIdx + 1}::int`;
      params.push(first, skip);

      const { rows } = await ctx.pool.query<FeedbackRow>(sql, params);
      return rows;
    },

    async feedbackResponse(_: unknown, args: { id: string }, ctx: GraphQLContext) {
      const decoded = decodeResponseId(args.id);
      if (!decoded) return null;

      const useSig = decoded.sig.length > 0;
      const sql = `SELECT id, asset, client_address, feedback_index, responder,
                          response_uri, response_hash, running_digest, response_count,
                          status, verified_at, tx_signature, block_slot, created_at
                   FROM feedback_responses
                   WHERE asset = $1 AND client_address = $2 AND feedback_index = $3
                     AND responder = $4 AND status != 'ORPHANED'
                     ${useSig ? 'AND tx_signature = $5' : ''}
                   LIMIT 1`;
      const params = useSig
        ? [decoded.asset, decoded.client, decoded.index, decoded.responder, decoded.sig]
        : [decoded.asset, decoded.client, decoded.index, decoded.responder];

      const { rows } = await ctx.pool.query<ResponseRow>(sql, params);
      return rows[0] ?? null;
    },

    async feedbackResponses(
      _: unknown,
      args: {
        first?: number; skip?: number; after?: string;
        where?: Record<string, unknown>;
        orderBy?: string; orderDirection?: string;
      },
      ctx: GraphQLContext
    ) {
      const first = clampFirst(args.first);
      const skip = clampSkip(args.skip);
      const dir = resolveDirection(args.orderDirection);
      const orderCol = ORDER_MAP_RESPONSE[args.orderBy ?? 'createdAt'] ?? 'created_at';

      assertNoMixedCursorOffset(args.after, skip);
      assertCursorOrderCompatibility(args.after, orderCol);

      const where = buildWhereClause('response', args.where);
      const params = [...where.params];
      let paramIdx = where.paramIndex;

      let cursorSql = '';
      if (args.after) {
        const cursor = decodeFlexibleCursor(args.after);
        if (!cursor) {
          throw createBadUserInputError('Invalid feedbackResponses cursor. Expected base64 JSON with created_at and optional id.');
        }
        const op = dir === 'DESC' ? '<' : '>';
        const cursorId = cursor.id ?? '';
        cursorSql = ` AND (created_at, id) ${op} ($${paramIdx}::timestamptz, $${paramIdx + 1}::text)`;
        params.push(cursor.created_at, cursorId);
        paramIdx += 2;
      }

      const sql = `SELECT id, asset, client_address, feedback_index, responder,
                          response_uri, response_hash, running_digest, response_count,
                          status, verified_at, tx_signature, block_slot, created_at
                   FROM feedback_responses ${where.sql}${cursorSql}
                   ORDER BY ${orderCol} ${dir}, id ${dir}
                   LIMIT $${paramIdx}::int OFFSET $${paramIdx + 1}::int`;
      params.push(first, skip);

      const { rows } = await ctx.pool.query<ResponseRow>(sql, params);
      return rows;
    },

    async validation(_: unknown, args: { id: string }, ctx: GraphQLContext) {
      const decoded = decodeValidationId(args.id);
      if (!decoded) return null;

      const { rows } = await ctx.pool.query<ValidationRow>(
        `SELECT id, asset, validator_address AS validator, requester, nonce, request_uri, request_hash,
                response, response_uri, response_hash, tag, chain_status,
                created_at, updated_at AS responded_at,
                tx_signature AS request_tx_signature,
                tx_signature AS response_tx_signature
         FROM validations
         WHERE asset = $1 AND validator_address = $2 AND nonce = $3
         AND chain_status != 'ORPHANED'`,
        [decoded.asset, decoded.validator, decoded.nonce]
      );
      return rows[0] ?? null;
    },

    async validations(
      _: unknown,
      args: {
        first?: number; skip?: number; after?: string;
        where?: Record<string, unknown>;
      },
      ctx: GraphQLContext
    ) {
      const first = clampFirst(args.first);
      const skip = clampSkip(args.skip);

      assertNoMixedCursorOffset(args.after, skip);

      const where = buildWhereClause('validation', args.where);
      const params = [...where.params];
      let paramIdx = where.paramIndex;

      let cursorSql = '';
      if (args.after) {
        const cursor = decodeFlexibleCursor(args.after);
        if (!cursor) {
          throw createBadUserInputError('Invalid validations cursor. Expected base64 JSON with created_at and optional id.');
        }
        const cursorId = cursor.id ?? '';
        cursorSql = ` AND (created_at, id) < ($${paramIdx}::timestamptz, $${paramIdx + 1}::text)`;
        params.push(cursor.created_at, cursorId);
        paramIdx += 2;
      }

      const sql = `SELECT id, asset, validator_address AS validator, requester, nonce, request_uri, request_hash,
                          response, response_uri, response_hash, tag, chain_status,
                          created_at, updated_at AS responded_at,
                          tx_signature AS request_tx_signature,
                          tx_signature AS response_tx_signature
                   FROM validations ${where.sql}${cursorSql}
                   ORDER BY created_at DESC, id DESC
                   LIMIT $${paramIdx}::int OFFSET $${paramIdx + 1}::int`;
      params.push(first, skip);

      const { rows } = await ctx.pool.query<ValidationRow>(sql, params);
      return rows;
    },

    async agentMetadatas(
      _: unknown,
      args: {
        first?: number; skip?: number;
        where?: Record<string, unknown>;
      },
      ctx: GraphQLContext
    ) {
      const first = clampFirst(args.first);
      const skip = clampSkip(args.skip);

      const where = buildWhereClause('metadata', args.where);
      const params = [...where.params];
      const paramIdx = where.paramIndex;

      const sql = `SELECT id, asset, key, value, immutable, status, updated_at, tx_signature
                   FROM metadata ${where.sql} AND key NOT LIKE '\\_uri:%' ESCAPE '\\'
                   ORDER BY asset, key
                   LIMIT $${paramIdx}::int OFFSET $${paramIdx + 1}::int`;
      params.push(first, skip);

      const { rows } = await ctx.pool.query(sql, params);
      return rows;
    },

    async agentStats(_: unknown, args: { id: string }, ctx: GraphQLContext) {
      const asset = decodeAgentId(args.id);
      if (!asset) return null;
      return ctx.loaders.agentStatsByAgent.load(asset);
    },

    async protocol(_: unknown, args: { id: string }, ctx: GraphQLContext) {
      const stats = await loadAggregatedStats(ctx);
      return {
        id: args.id,
        totalAgents: stats.totalAgents,
        totalFeedback: stats.totalFeedback,
        totalValidations: stats.totalValidations,
        tags: stats.tags,
      };
    },

    async protocols(
      _: unknown,
      _args: { first?: number; skip?: number },
      ctx: GraphQLContext
    ) {
      const proto = await queryResolvers.Query.protocol(_, { id: `solana-${ctx.networkMode}` }, ctx);
      return proto ? [proto] : [];
    },

    async globalStats(_: unknown, args: { id: string }, ctx: GraphQLContext) {
      const stats = await loadAggregatedStats(ctx);
      return {
        id: args.id,
        totalAgents: stats.totalAgents,
        totalFeedback: stats.totalFeedback,
        totalValidations: stats.totalValidations,
        totalProtocols: '1',
        tags: stats.tags,
      };
    },

    async agentSearch(
      _: unknown,
      args: { query: string; first?: number },
      ctx: GraphQLContext
    ) {
      const first = Math.min(args.first ?? 20, MAX_FIRST);
      const searchTerm = `%${args.query.replace(/[%_]/g, '\\$&')}%`;

      const { rows } = await ctx.pool.query<AgentRow>(
        `SELECT
                ${AGENT_SELECT_WITH_DIGESTS}
         FROM agents a
         LEFT JOIN (
           SELECT
             agent_id,
             feedback_digest,
             response_digest,
             revoke_digest,
             feedback_count AS digest_feedback_count,
             response_count AS digest_response_count,
             revoke_count AS digest_revoke_count
           FROM agent_digest_cache
         ) adc ON adc.agent_id = a.asset
         WHERE a.status != 'ORPHANED'
         AND (a.nft_name ILIKE $1 ESCAPE '\\' OR a.asset ILIKE $1 ESCAPE '\\' OR a.owner ILIKE $1 ESCAPE '\\')
         ORDER BY a.created_at DESC
         LIMIT $2::int`,
        [searchTerm, first]
      );

      for (const row of rows) {
        ctx.loaders.agentById.prime(row.asset, row);
      }

      return rows;
    },

    async collectionPointers(
      _: unknown,
      args: {
        first?: number;
        skip?: number;
        col?: string;
        creator?: string;
      },
      ctx: GraphQLContext
    ) {
      const first = clampFirst(args.first);
      const skip = clampSkip(args.skip);
      const params: unknown[] = [];
      const filters: string[] = [];
      let paramIdx = 1;

      if (args.col) {
        filters.push(`col = $${paramIdx}::text`);
        params.push(args.col);
        paramIdx++;
      }
      if (args.creator) {
        filters.push(`creator = $${paramIdx}::text`);
        params.push(args.creator);
        paramIdx++;
      }

      const whereSql = filters.length > 0 ? `WHERE ${filters.join(' AND ')}` : '';
      const sql = `SELECT
                          col,
                          creator,
                          first_seen_asset,
                          first_seen_at,
                          first_seen_slot::text,
                          first_seen_tx_signature,
                          last_seen_at,
                          last_seen_slot::text,
                          last_seen_tx_signature,
                          asset_count::text
                   FROM collection_pointers
                   ${whereSql}
                   ORDER BY first_seen_at DESC, col ASC, creator ASC
                   LIMIT $${paramIdx}::int OFFSET $${paramIdx + 1}::int`;
      params.push(first, skip);

      const { rows } = await ctx.pool.query<CollectionPointerRow>(sql, params);
      return rows.map((row) => ({
        col: row.col,
        creator: row.creator,
        firstSeenAsset: row.first_seen_asset,
        firstSeenAt: toUnixTimestamp(row.first_seen_at),
        firstSeenSlot: row.first_seen_slot,
        firstSeenTxSignature: row.first_seen_tx_signature,
        lastSeenAt: toUnixTimestamp(row.last_seen_at),
        lastSeenSlot: row.last_seen_slot,
        lastSeenTxSignature: row.last_seen_tx_signature,
        assetCount: row.asset_count,
      }));
    },

    async collectionAssetCount(
      _: unknown,
      args: {
        col: string;
        creator?: string;
      },
      ctx: GraphQLContext
    ) {
      const params: unknown[] = [args.col];
      let sql = `SELECT COUNT(*)::text AS count
                 FROM agents
                 WHERE status != 'ORPHANED'
                   AND canonical_col = $1::text`;

      if (args.creator) {
        params.push(args.creator);
        sql += ` AND creator = $2::text`;
      }

      const { rows } = await ctx.pool.query<{ count: string }>(sql, params);
      return rows[0]?.count ?? '0';
    },

    async collectionAssets(
      _: unknown,
      args: {
        col: string;
        creator?: string;
        first?: number;
        skip?: number;
        orderBy?: string;
        orderDirection?: string;
      },
      ctx: GraphQLContext
    ) {
      const first = clampAgentPageSize(args.first);
      const skip = clampSkip(args.skip);
      const dir = resolveDirection(args.orderDirection);
      const orderCol = resolveAgentOrderColumn(args.orderBy);

      const params: unknown[] = [args.col];
      let paramIdx = 2;
      let creatorSql = '';
      if (args.creator) {
        creatorSql = ` AND a.creator = $${paramIdx}::text`;
        params.push(args.creator);
        paramIdx++;
      }

      const sql = `SELECT
                          ${AGENT_SELECT_WITH_DIGESTS}
                   FROM agents a
                   LEFT JOIN (
                     SELECT
                       agent_id,
                       feedback_digest,
                       response_digest,
                       revoke_digest,
                       feedback_count AS digest_feedback_count,
                       response_count AS digest_response_count,
                       revoke_count AS digest_revoke_count
                     FROM agent_digest_cache
                   ) adc ON adc.agent_id = a.asset
                   WHERE a.status != 'ORPHANED'
                     AND a.canonical_col = $1::text
                     ${creatorSql}
                   ORDER BY ${orderCol} ${dir}, a.asset ${dir}
                   LIMIT $${paramIdx}::int OFFSET $${paramIdx + 1}::int`;
      params.push(first, skip);

      const { rows } = await ctx.pool.query<AgentRow>(sql, params);
      for (const row of rows) {
        ctx.loaders.agentById.prime(row.asset, row);
      }
      return rows;
    },

    async agentChildren(
      _: unknown,
      args: {
        parent: string;
        first?: number;
        skip?: number;
      },
      ctx: GraphQLContext
    ) {
      const parentAsset = resolveAssetId(args.parent);
      const first = clampFirst(args.first);
      const skip = clampSkip(args.skip);

      const { rows } = await ctx.pool.query<AgentRow>(
        `SELECT
                ${AGENT_SELECT_WITH_DIGESTS}
         FROM agents a
         LEFT JOIN (
           SELECT
             agent_id,
             feedback_digest,
             response_digest,
             revoke_digest,
             feedback_count AS digest_feedback_count,
             response_count AS digest_response_count,
             revoke_count AS digest_revoke_count
           FROM agent_digest_cache
         ) adc ON adc.agent_id = a.asset
         WHERE a.parent_asset = $1::text
           AND a.status != 'ORPHANED'
         ORDER BY a.created_at DESC, a.asset DESC
         LIMIT $2::int OFFSET $3::int`,
        [parentAsset, first, skip]
      );

      for (const row of rows) {
        ctx.loaders.agentById.prime(row.asset, row);
      }

      return rows;
    },

    async agentLineage(
      _: unknown,
      args: {
        asset: string;
        includeSelf?: boolean;
        first?: number;
        skip?: number;
      },
      ctx: GraphQLContext
    ) {
      const asset = resolveAssetId(args.asset);
      const includeSelf = args.includeSelf !== false;
      const first = clampAgentPageSize(args.first);
      const skip = clampSkip(args.skip);

      const { rows } = await ctx.pool.query<AgentTreeNodeRow>(
        `WITH RECURSIVE lineage AS (
           SELECT
             a.asset,
             a.parent_asset,
             ARRAY[a.asset]::text[] AS path,
             0 AS depth
           FROM agents a
           WHERE a.asset = $1::text
             AND a.status != 'ORPHANED'
           UNION ALL
           SELECT
             p.asset,
             p.parent_asset,
             l.path || p.asset,
             l.depth + 1
           FROM agents p
           INNER JOIN lineage l ON l.parent_asset = p.asset
           WHERE p.status != 'ORPHANED'
             AND l.depth < $2::int
             AND NOT (p.asset = ANY(l.path))
         )
         SELECT asset, parent_asset, path, depth
         FROM lineage
         WHERE $3::boolean OR depth > 0
         ORDER BY depth DESC, asset ASC`,
        [asset, MAX_LINEAGE_DEPTH, includeSelf]
      );

      if (rows.length === 0) return [];

      const assets = rows.map((row) => row.asset);
      const { rows: agentRows } = await ctx.pool.query<AgentRow>(
        `SELECT
                ${AGENT_SELECT_WITH_DIGESTS}
         FROM agents a
         LEFT JOIN (
           SELECT
             agent_id,
             feedback_digest,
             response_digest,
             revoke_digest,
             feedback_count AS digest_feedback_count,
             response_count AS digest_response_count,
             revoke_count AS digest_revoke_count
           FROM agent_digest_cache
         ) adc ON adc.agent_id = a.asset
         WHERE a.asset = ANY($1::text[])
           AND a.status != 'ORPHANED'`,
        [assets]
      );
      const byAsset = new Map(agentRows.map((row) => [row.asset, row]));

      const ordered = rows
        .map((row) => byAsset.get(row.asset) ?? null)
        .filter((row): row is AgentRow => row !== null)
        .map((row) => {
          ctx.loaders.agentById.prime(row.asset, row);
          return row;
        });
      return ordered.slice(skip, skip + first);
    },

    async agentTree(
      _: unknown,
      args: {
        root: string;
        maxDepth?: number;
        includeRoot?: boolean;
        first?: number;
        skip?: number;
      },
      ctx: GraphQLContext
    ) {
      const rootAsset = resolveAssetId(args.root);
      const maxDepth = clampTreeDepth(args.maxDepth);
      const includeRoot = args.includeRoot !== false;
      const first = clampAgentPageSize(args.first);
      const skip = clampSkip(args.skip);

      const { rows } = await ctx.pool.query<AgentTreeNodeRow>(
        `WITH RECURSIVE tree AS (
           SELECT
             a.asset,
             a.parent_asset,
             ARRAY[a.asset]::text[] AS path,
             0 AS depth
           FROM agents a
           WHERE a.asset = $1::text
             AND a.status != 'ORPHANED'
           UNION ALL
           SELECT
             c.asset,
             c.parent_asset,
             t.path || c.asset,
             t.depth + 1
           FROM agents c
           INNER JOIN tree t ON c.parent_asset = t.asset
           WHERE c.status != 'ORPHANED'
             AND t.depth < $2::int
             AND NOT (c.asset = ANY(t.path))
         )
         SELECT asset, parent_asset, path, depth
         FROM tree
         WHERE $3::boolean OR depth > 0
         ORDER BY depth ASC, path ASC
         LIMIT $4::int OFFSET $5::int`,
        [rootAsset, maxDepth, includeRoot, first, skip]
      );

      if (rows.length === 0) return [];

      const assets = rows.map((row) => row.asset);
      const agentSql = `SELECT
                              ${AGENT_SELECT_WITH_DIGESTS}
                        FROM agents a
                        LEFT JOIN (
                          SELECT
                            agent_id,
                            feedback_digest,
                            response_digest,
                            revoke_digest,
                            feedback_count AS digest_feedback_count,
                            response_count AS digest_response_count,
                            revoke_count AS digest_revoke_count
                          FROM agent_digest_cache
                        ) adc ON adc.agent_id = a.asset
                        WHERE a.asset = ANY($1::text[])
                          AND a.status != 'ORPHANED'`;
      const { rows: agentRows } = await ctx.pool.query<AgentRow>(agentSql, [assets]);
      const byAsset = new Map(agentRows.map((row) => [row.asset, row]));

      return rows
        .map((row) => {
          const agent = byAsset.get(row.asset);
          if (!agent) return null;
          ctx.loaders.agentById.prime(agent.asset, agent);
          return {
            depth: row.depth,
            path: row.path,
            parentAsset: row.parent_asset,
            agent,
          };
        })
        .filter((row): row is { depth: number; path: string[]; parentAsset: string | null; agent: AgentRow } => row !== null);
    },

    async agentRegistrationFiles(
      _: unknown,
      args: { first?: number; skip?: number; where?: { agent?: string } },
      ctx: GraphQLContext
    ) {
      const first = clampFirst(args.first);
      const skip = clampSkip(args.skip);

      const requestedAsset = args.where?.agent ? decodeAgentId(args.where.agent) : null;
      if (args.where?.agent && !requestedAsset) return [];

      const params: unknown[] = [];
      let paramIdx = 1;
      let assetSql = '';
      if (requestedAsset) {
        assetSql = ` AND m.asset = $${paramIdx}::text`;
        params.push(requestedAsset);
        paramIdx++;
      }

      const sql = `SELECT m.asset, MAX(m.updated_at) AS latest_updated_at
                   FROM metadata m
                   INNER JOIN agents a ON a.asset = m.asset
                   WHERE m.key LIKE '\\_uri:%' ESCAPE '\\'
                     AND m.status != 'ORPHANED'
                     AND a.status != 'ORPHANED'
                     ${assetSql}
                   GROUP BY m.asset
                   ORDER BY latest_updated_at DESC NULLS LAST, m.asset DESC
                   LIMIT $${paramIdx}::int OFFSET $${paramIdx + 1}::int`;
      params.push(first, skip);

      const { rows } = await ctx.pool.query<{ asset: string }>(sql, params);
      return rows.map(row => ({ _asset: row.asset }));
    },
  },
};
