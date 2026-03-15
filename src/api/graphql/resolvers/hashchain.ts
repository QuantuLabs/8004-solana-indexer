import type { GraphQLContext } from '../context.js';
import { decodeAgentId } from '../utils/ids.js';
import { createBadUserInputError } from '../utils/errors.js';

const MAX_REPLAY_FIRST = 1000;

type HashChainType = 'FEEDBACK' | 'RESPONSE' | 'REVOKE';

type HashChainHead = { digest: string | null; count: string };

type HashChainCheckpoint = { eventCount: string; digest: string; createdAt: string };

type HashChainReplayEvent = {
  asset: string;
  client: string;
  feedbackIndex: string;
  slot: string;
  runningDigest: string | null;
  feedbackHash: string | null;
  responder: string | null;
  responseHash: string | null;
  responseCount: string | null;
  revokeCount: string | null;
};

function toBigIntSafe(value: unknown, fallback = 0n): bigint {
  if (typeof value === 'bigint') return value;
  if (typeof value === 'number' && Number.isFinite(value)) return BigInt(Math.trunc(value));
  if (typeof value === 'string' && value.trim() !== '') {
    try {
      return BigInt(value);
    } catch {
      return fallback;
    }
  }
  return fallback;
}

function clampReplayFirst(value: unknown): number {
  const n = typeof value === 'number' ? value : (typeof value === 'string' ? Number.parseInt(value, 10) : NaN);
  if (!Number.isFinite(n) || n < 1) return MAX_REPLAY_FIRST;
  return Math.min(MAX_REPLAY_FIRST, Math.trunc(n));
}

function assertNonNegative(name: string, value: bigint): void {
  if (value < 0n) {
    throw createBadUserInputError(`${name} must be non-negative`);
  }
}

function epochSecondsExpr(column: string): string {
  // seconds since epoch as text (to feed BigInt scalar)
  return `FLOOR(EXTRACT(EPOCH FROM ${column}))::bigint::text`;
}

function resolveChainType(value: unknown): HashChainType {
  if (value === 'FEEDBACK' || value === 'RESPONSE' || value === 'REVOKE') return value;
  throw createBadUserInputError('Invalid chainType. Expected FEEDBACK, RESPONSE, or REVOKE.');
}

async function fetchHead(ctx: GraphQLContext, asset: string, chain: HashChainType): Promise<HashChainHead> {
  if (chain === 'FEEDBACK') {
    const [countRes, digestRes] = await Promise.all([
      ctx.pool.query<{ count: string }>(
        `SELECT COALESCE(MAX(feedback_index) + 1, 0)::text AS count
         FROM (
           SELECT feedback_index FROM feedbacks WHERE asset = $1
           UNION ALL
           SELECT feedback_index FROM orphan_feedbacks WHERE asset = $1
         ) combined`,
        [asset]
      ),
      ctx.pool.query<{ digest: string | null }>(
        `SELECT encode(running_digest, 'hex') AS digest
         FROM (
           SELECT running_digest, block_slot, tx_index, event_ordinal, tx_signature, id::text AS row_id
           FROM feedbacks
           WHERE asset = $1 AND running_digest IS NOT NULL
           UNION ALL
           SELECT running_digest, block_slot, tx_index, event_ordinal, tx_signature, id::text AS row_id
           FROM orphan_feedbacks
           WHERE asset = $1 AND running_digest IS NOT NULL
         ) combined
         ORDER BY block_slot DESC NULLS LAST, tx_index DESC NULLS LAST, event_ordinal DESC NULLS LAST, tx_signature DESC NULLS LAST, row_id DESC
         LIMIT 1`,
        [asset]
      ),
    ]);

    return {
      digest: digestRes.rows[0]?.digest ?? null,
      count: countRes.rows[0]?.count ?? '0',
    };
  }

  if (chain === 'RESPONSE') {
    const [countRes, digestRes] = await Promise.all([
      ctx.pool.query<{ count: string }>(
        `SELECT COALESCE(MAX(response_count), 0)::text AS count
         FROM (
           SELECT response_count FROM feedback_responses WHERE asset = $1
           UNION ALL
           SELECT response_count FROM orphan_responses WHERE asset = $1
         ) combined`,
        [asset]
      ),
      ctx.pool.query<{ digest: string | null }>(
        `SELECT encode(running_digest, 'hex') AS digest
         FROM (
           SELECT running_digest, block_slot, tx_index, event_ordinal, tx_signature, id::text AS row_id
           FROM feedback_responses
           WHERE asset = $1 AND running_digest IS NOT NULL
           UNION ALL
           SELECT running_digest, block_slot, tx_index, event_ordinal, tx_signature, id::text AS row_id
           FROM orphan_responses
           WHERE asset = $1 AND running_digest IS NOT NULL
         ) combined
         ORDER BY block_slot DESC NULLS LAST, tx_index DESC NULLS LAST, event_ordinal DESC NULLS LAST, tx_signature DESC NULLS LAST, row_id DESC
         LIMIT 1`,
        [asset]
      ),
    ]);

    return {
      digest: digestRes.rows[0]?.digest ?? null,
      count: countRes.rows[0]?.count ?? '0',
    };
  }

  const [countRes, digestRes] = await Promise.all([
    ctx.pool.query<{ count: string }>(
      `SELECT COALESCE(MAX(revoke_count), 0)::text AS count
       FROM revocations
       WHERE asset = $1`,
      [asset]
    ),
    ctx.pool.query<{ digest: string | null }>(
      `SELECT encode(running_digest, 'hex') AS digest
       FROM revocations
       WHERE asset = $1 AND running_digest IS NOT NULL
       ORDER BY revoke_count DESC
       LIMIT 1`,
      [asset]
    ),
  ]);

  return {
    digest: digestRes.rows[0]?.digest ?? null,
    count: countRes.rows[0]?.count ?? '0',
  };
}

async function fetchLatestCheckpoint(
  ctx: GraphQLContext,
  asset: string,
  chain: HashChainType,
): Promise<HashChainCheckpoint | null> {
  if (chain === 'FEEDBACK') {
    const { rows } = await ctx.pool.query<{ feedback_index: string; digest: string; created_at: string }>(
      `SELECT
         feedback_index::text AS feedback_index,
         encode(running_digest, 'hex') AS digest,
         ${epochSecondsExpr('created_at')} AS created_at
       FROM (
         SELECT feedback_index, running_digest, created_at
         FROM feedbacks
         WHERE asset = $1 AND running_digest IS NOT NULL
         UNION ALL
         SELECT feedback_index, running_digest, created_at
         FROM orphan_feedbacks
         WHERE asset = $1 AND running_digest IS NOT NULL
       ) combined
       WHERE ((feedback_index + 1) % 1000) = 0
       ORDER BY feedback_index DESC
       LIMIT 1`,
      [asset]
    );

    const row = rows[0];
    if (!row) return null;
    const eventCount = (BigInt(row.feedback_index) + 1n).toString();

    return { eventCount, digest: row.digest, createdAt: row.created_at };
  }

  if (chain === 'RESPONSE') {
    const { rows } = await ctx.pool.query<{ response_count: string; digest: string; created_at: string }>(
      `SELECT
         response_count::text AS response_count,
         encode(running_digest, 'hex') AS digest,
         ${epochSecondsExpr('created_at')} AS created_at
       FROM (
         SELECT response_count, running_digest, created_at
         FROM feedback_responses
         WHERE asset = $1 AND running_digest IS NOT NULL
         UNION ALL
         SELECT response_count, running_digest, created_at
         FROM orphan_responses
         WHERE asset = $1 AND running_digest IS NOT NULL
       ) combined
       WHERE (response_count % 1000) = 0
       ORDER BY response_count DESC
       LIMIT 1`,
      [asset]
    );

    const row = rows[0];
    if (!row) return null;
    const eventCount = BigInt(row.response_count).toString();

    return { eventCount, digest: row.digest, createdAt: row.created_at };
  }

  const { rows } = await ctx.pool.query<{ revoke_count: string; digest: string; created_at: string }>(
    `SELECT
       revoke_count::text AS revoke_count,
       encode(running_digest, 'hex') AS digest,
       ${epochSecondsExpr('created_at')} AS created_at
     FROM revocations
     WHERE asset = $1
       AND running_digest IS NOT NULL
       AND (revoke_count % 1000) = 0
     ORDER BY revoke_count DESC
     LIMIT 1`,
    [asset]
  );

  const row = rows[0];
  if (!row) return null;
  const eventCount = BigInt(row.revoke_count).toString();

  return { eventCount, digest: row.digest, createdAt: row.created_at };
}

function emptyReplayPage(fromCount: bigint) {
  return { events: [] as HashChainReplayEvent[], hasMore: false, nextFromCount: fromCount.toString() };
}

function normalizeReplayFromCount(_chainType: HashChainType, fromCount: bigint): bigint {
  return fromCount;
}

export const hashChainResolvers = {
  Query: {
    async hashChainHeads(_: unknown, args: { agent: string }, ctx: GraphQLContext) {
      const asset = decodeAgentId(args.agent);
      if (!asset) {
        throw createBadUserInputError('Invalid agent id. Expected asset pubkey.');
      }

      const [feedback, response, revoke] = await Promise.all([
        fetchHead(ctx, asset, 'FEEDBACK'),
        fetchHead(ctx, asset, 'RESPONSE'),
        fetchHead(ctx, asset, 'REVOKE'),
      ]);

      return { feedback, response, revoke };
    },

    async hashChainLatestCheckpoints(_: unknown, args: { agent: string }, ctx: GraphQLContext) {
      const asset = decodeAgentId(args.agent);
      if (!asset) {
        throw createBadUserInputError('Invalid agent id. Expected asset pubkey.');
      }

      const [feedback, response, revoke] = await Promise.all([
        fetchLatestCheckpoint(ctx, asset, 'FEEDBACK'),
        fetchLatestCheckpoint(ctx, asset, 'RESPONSE'),
        fetchLatestCheckpoint(ctx, asset, 'REVOKE'),
      ]);

      return { feedback, response, revoke };
    },

    async hashChainReplayData(
      _: unknown,
      args: {
        agent: string;
        chainType: unknown;
        fromCount?: unknown;
        toCount?: unknown;
        first?: unknown;
      },
      ctx: GraphQLContext,
    ) {
      const asset = decodeAgentId(args.agent);
      if (!asset) {
        throw createBadUserInputError('Invalid agent id. Expected asset pubkey.');
      }

      const chainType = resolveChainType(args.chainType);

      const fromCount = toBigIntSafe(args.fromCount, 0n);
      const toCount = args.toCount != null ? toBigIntSafe(args.toCount, 0n) : null;
      assertNonNegative('fromCount', fromCount);
      if (toCount != null) assertNonNegative('toCount', toCount);

      const first = clampReplayFirst(args.first);
      const replayFromCount = normalizeReplayFromCount(chainType, fromCount);
      const queryTake = first + 1;
      const effectiveTo = toCount ?? (replayFromCount + BigInt(queryTake));

      if (effectiveTo <= replayFromCount) {
        return emptyReplayPage(fromCount);
      }

      let take = first;
      const diff = effectiveTo - replayFromCount;
      if (diff < BigInt(take)) {
        take = Number(diff);
      }
      if (take <= 0) {
        return emptyReplayPage(fromCount);
      }

      if (chainType === 'FEEDBACK') {
        const { rows } = await ctx.pool.query<{
          client: string;
          feedback_index: string;
          feedback_hash: string | null;
          slot: string;
          running_digest: string | null;
        }>(
          `WITH combined AS (
             SELECT
               client_address AS client,
               feedback_index::text AS feedback_index,
               feedback_hash,
               block_slot::text AS slot,
               encode(running_digest, 'hex') AS running_digest,
               block_slot,
               tx_index,
               event_ordinal,
               tx_signature,
               id::text AS row_id
             FROM feedbacks
             WHERE asset = $1
             UNION ALL
             SELECT
               client_address AS client,
               feedback_index::text AS feedback_index,
               feedback_hash,
               block_slot::text AS slot,
               encode(running_digest, 'hex') AS running_digest,
               block_slot,
               tx_index,
               event_ordinal,
               tx_signature,
               id::text AS row_id
             FROM orphan_feedbacks
             WHERE asset = $1
           )
           SELECT
             client,
             feedback_index,
             feedback_hash,
             slot,
             running_digest
           FROM combined
           WHERE (feedback_index)::bigint >= $2::bigint
             AND (feedback_index)::bigint < $3::bigint
           ORDER BY (feedback_index)::bigint ASC, block_slot ASC, tx_index ASC NULLS LAST, event_ordinal ASC NULLS LAST, tx_signature ASC NULLS LAST, row_id ASC
           LIMIT $4::int`,
          [asset, replayFromCount.toString(), effectiveTo.toString(), queryTake]
        );

        const events: HashChainReplayEvent[] = rows.slice(0, take).map((r) => ({
          asset,
          client: r.client,
          feedbackIndex: r.feedback_index,
          slot: r.slot,
          runningDigest: r.running_digest,
          feedbackHash: r.feedback_hash,
          responder: null,
          responseHash: null,
          responseCount: null,
          revokeCount: null,
        }));

        const last = events.length > 0 ? BigInt(events[events.length - 1]!.feedbackIndex) : fromCount;
        const nextFromCount = events.length > 0 ? (last + 1n).toString() : fromCount.toString();

        return {
          events,
          hasMore: rows.length > take,
          nextFromCount,
        };
      }

      if (chainType === 'RESPONSE') {
        const { rows } = await ctx.pool.query<{
          client: string;
          feedback_index: string;
          responder: string;
          response_hash: string | null;
          feedback_hash: string | null;
          slot: string;
          running_digest: string | null;
          response_count: string;
        }>(
          `WITH combined AS (
             SELECT
               fr.client_address AS client,
               fr.feedback_index::text AS feedback_index,
               fr.responder,
               fr.response_hash,
               fr.seal_hash AS feedback_hash,
               encode(fr.running_digest, 'hex') AS running_digest,
               fr.block_slot::text AS slot,
               fr.response_count::text AS response_count,
               fr.block_slot,
               fr.tx_index,
               fr.event_ordinal,
               fr.tx_signature,
               fr.id::text AS row_id
             FROM feedback_responses fr
             WHERE fr.asset = $1
             UNION ALL
             SELECT
               o.client_address AS client,
               o.feedback_index::text AS feedback_index,
               o.responder,
               o.response_hash,
               o.seal_hash AS feedback_hash,
               encode(o.running_digest, 'hex') AS running_digest,
               o.block_slot::text AS slot,
               o.response_count::text AS response_count,
               o.block_slot,
               o.tx_index,
               o.event_ordinal,
               o.tx_signature,
               o.id::text AS row_id
             FROM orphan_responses o
             WHERE o.asset = $1
           )
           SELECT
             client,
             feedback_index,
             responder,
             response_hash,
             feedback_hash,
             slot,
             running_digest,
             response_count
           FROM combined
           WHERE (response_count)::bigint >= $2::bigint
             AND (response_count)::bigint < $3::bigint
           ORDER BY (response_count)::bigint ASC, block_slot ASC, tx_index ASC NULLS LAST, event_ordinal ASC NULLS LAST, tx_signature ASC NULLS LAST, row_id ASC
           LIMIT $4::int`,
          [asset, replayFromCount.toString(), effectiveTo.toString(), queryTake]
        );

        const events: HashChainReplayEvent[] = rows.slice(0, take).map((r) => ({
          asset,
          client: r.client,
          feedbackIndex: r.feedback_index,
          slot: r.slot,
          runningDigest: r.running_digest,
          feedbackHash: r.feedback_hash,
          responder: r.responder,
          responseHash: r.response_hash,
          responseCount: r.response_count,
          revokeCount: null,
        }));

        const last = events.length > 0 ? BigInt(events[events.length - 1]!.responseCount ?? fromCount.toString()) : fromCount;
        const nextFromCount = events.length > 0 ? (last + 1n).toString() : fromCount.toString();

        return {
          events,
          hasMore: rows.length > take,
          nextFromCount,
        };
      }

      const { rows } = await ctx.pool.query<{
        client: string;
        feedback_index: string;
        feedback_hash: string | null;
        slot: string;
        running_digest: string | null;
        revoke_count: string;
      }>(
        `SELECT
           client_address AS client,
           feedback_index::text AS feedback_index,
           feedback_hash,
           slot::text AS slot,
           encode(running_digest, 'hex') AS running_digest,
           revoke_count::text AS revoke_count
         FROM revocations
         WHERE asset = $1
           AND revoke_count >= $2::bigint
           AND revoke_count < $3::bigint
         ORDER BY revoke_count ASC, slot ASC, tx_index ASC NULLS LAST, event_ordinal ASC NULLS LAST, tx_signature ASC NULLS LAST, id ASC
         LIMIT $4::int`,
        [asset, replayFromCount.toString(), effectiveTo.toString(), queryTake]
      );

      const events: HashChainReplayEvent[] = rows.slice(0, take).map((r) => ({
        asset,
        client: r.client,
        feedbackIndex: r.feedback_index,
        slot: r.slot,
        runningDigest: r.running_digest,
        feedbackHash: r.feedback_hash,
        responder: null,
        responseHash: null,
        responseCount: null,
        revokeCount: r.revoke_count,
      }));

      const last = events.length > 0 ? BigInt(events[events.length - 1]!.revokeCount ?? fromCount.toString()) : fromCount;
      const nextFromCount = events.length > 0 ? (last + 1n).toString() : fromCount.toString();

      return {
        events,
        hasMore: rows.length > take,
        nextFromCount,
      };
    },
  },
};
