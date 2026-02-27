import DataLoader from 'dataloader';
import type { Pool } from 'pg';

export interface AgentRow {
  asset: string;
  owner: string;
  creator: string | null;
  agent_uri: string | null;
  agent_wallet: string | null;
  collection: string | null;
  collection_pointer: string | null;
  col_locked: boolean;
  parent_asset: string | null;
  parent_creator: string | null;
  parent_locked: boolean;
  nft_name: string | null;
  atom_enabled: boolean;
  trust_tier: number | null;
  quality_score: number | null;
  confidence: number | null;
  risk_score: number | null;
  diversity_ratio: number | null;
  sort_key: string | null;
  status: string;
  verified_at: string | null;
  created_at: string;
  updated_at: string;
  agent_id: string | null;
  created_tx_signature: string | null;
  created_slot: string | null;
  feedback_digest: string | null;
  response_digest: string | null;
  revoke_digest: string | null;
  feedback_count: string | null;
  response_count: string | null;
  revoke_count: string | null;
}

export interface FeedbackRow {
  id: string;
  feedback_id: string | null;
  asset: string;
  client_address: string;
  feedback_index: string;
  value: string;
  value_decimals: number;
  score: number | null;
  tag1: string | null;
  tag2: string | null;
  endpoint: string | null;
  feedback_uri: string | null;
  feedback_hash: string | null;
  running_digest: string | null;
  is_revoked: boolean;
  status: string;
  verified_at: string | null;
  tx_signature: string | null;
  block_slot: string | null;
  created_at: string;
  revoked_at: string | null;
}

export interface ResponseRow {
  id: string;
  response_id: string | null;
  asset: string;
  client_address: string;
  feedback_index: string;
  responder: string;
  response_uri: string | null;
  response_hash: string | null;
  running_digest: string | null;
  response_count: string | null;
  status: string;
  verified_at: string | null;
  tx_signature: string | null;
  block_slot: string | null;
  created_at: string;
}

export interface RevocationRow {
  id: string;
  revocation_id: string | null;
  asset: string;
  client_address: string;
  feedback_index: string;
  feedback_hash: string | null;
  slot: string;
  original_score: number | null;
  atom_enabled: boolean;
  had_impact: boolean;
  running_digest: string | null;
  revoke_count: string;
  status: string;
  verified_at: string | null;
  tx_signature: string | null;
  created_at: string;
}

export interface ValidationRow {
  id: string;
  asset: string;
  validator: string;
  requester: string;
  nonce: string;
  request_uri: string | null;
  request_hash: string | null;
  response: number | null;
  response_uri: string | null;
  response_hash: string | null;
  tag: string | null;
  chain_status: string;
  created_at: string;
  responded_at: string | null;
  request_tx_signature: string | null;
  response_tx_signature: string | null;
}

export interface MetadataRow {
  id: string;
  asset: string;
  key: string;
  value: Buffer;
  immutable: boolean;
  status: string;
  updated_at: string | null;
  tx_signature: string | null;
}

export interface RegistrationRow {
  asset: string;
  key: string;
  value: Buffer;
}

export interface AgentStatsRow {
  asset: string;
  feedback_count: string;
  avg_value: string | null;
  validation_count: string;
  completed_validations: string;
  avg_validation_score: string | null;
  last_activity: string | null;
}

export interface FeedbackCountRow {
  asset: string;
  count: string;
}

export interface LastActivityRow {
  asset: string;
  last_activity: string | null;
}

export interface FeedbackPageKey {
  asset: string;
  first: number;
  skip: number;
  orderBy: 'created_at' | 'feedback_id' | 'value' | 'feedback_index';
  orderDirection: 'ASC' | 'DESC';
}

export interface ValidationPageKey {
  asset: string;
  first: number;
  skip: number;
}

export interface ResponsePageKey {
  asset: string;
  clientAddress: string;
  feedbackIndex: string;
  first: number;
  skip: number;
}

function feedbackLookupKey(asset: string, clientAddress: string, feedbackIndex: string): string {
  return `${asset}:${clientAddress}:${feedbackIndex}`;
}

function feedbackPageCacheKey(key: FeedbackPageKey): string {
  return `${key.asset}:${key.first}:${key.skip}:${key.orderBy}:${key.orderDirection}`;
}

function validationPageCacheKey(key: ValidationPageKey): string {
  return `${key.asset}:${key.first}:${key.skip}`;
}

function responsePageCacheKey(key: ResponsePageKey): string {
  return `${key.asset}:${key.clientAddress}:${key.feedbackIndex}:${key.first}:${key.skip}`;
}

function feedbackLookupCacheKeyFromComposite(key: string): string {
  const parts = key.split(':');
  if (parts.length !== 3) return key;
  return feedbackLookupKey(parts[0], parts[1], parts[2]);
}

function createAgentByIdLoader(pool: Pool) {
  return new DataLoader<string, AgentRow | null>(async (keys) => {
    const { rows } = await pool.query<AgentRow>(
      `SELECT
              a.asset,
              a.agent_id::text AS agent_id,
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
              COALESCE(adc.digest_revoke_count::text, '0') AS revoke_count
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
       WHERE a.asset = ANY($1::text[]) AND a.status != 'ORPHANED'`,
      [keys as string[]]
    );
    const map = new Map(rows.map(r => [r.asset, r]));
    return keys.map(k => map.get(k) ?? null);
  });
}

function createFeedbackByLookupLoader(pool: Pool) {
  return new DataLoader<string, FeedbackRow | null>(async (keys) => {
    const assets: string[] = [];
    const clients: string[] = [];
    const indices: string[] = [];

    for (const key of keys) {
      const parts = key.split(':');
      if (parts.length !== 3) continue;
      assets.push(parts[0]);
      clients.push(parts[1]);
      indices.push(parts[2]);
    }

    if (assets.length === 0) {
      return keys.map(() => null);
    }

    const { rows } = await pool.query<FeedbackRow>(
      `SELECT id, feedback_id::text AS feedback_id, asset, client_address, feedback_index, value, value_decimals,
              score, tag1, tag2, endpoint, feedback_uri, feedback_hash,
              running_digest, is_revoked, status, verified_at,
              tx_signature, block_slot, created_at, revoked_at
       FROM feedbacks
       WHERE (asset, client_address, feedback_index) IN (
         SELECT unnest($1::text[]), unnest($2::text[]), unnest($3::bigint[])
       )
       AND status != 'ORPHANED'`,
      [assets, clients, indices]
    );

    const map = new Map<string, FeedbackRow>();
    for (const row of rows) {
      map.set(feedbackLookupKey(row.asset, row.client_address, row.feedback_index), row);
    }

    return keys.map(k => map.get(feedbackLookupCacheKeyFromComposite(k)) ?? null);
  });
}

function createFeedbackPageByAgentLoader(pool: Pool) {
  return new DataLoader<FeedbackPageKey, FeedbackRow[], string>(
    async (keys) => {
      const grouped = new Map<string, FeedbackPageKey[]>();
      for (const key of keys) {
        const groupKey = `${key.first}:${key.skip}:${key.orderBy}:${key.orderDirection}`;
        const group = grouped.get(groupKey) ?? [];
        group.push(key);
        grouped.set(groupKey, group);
      }

      const results = new Map<string, FeedbackRow[]>();

      for (const group of grouped.values()) {
        const sample = group[0];
        const assets = group.map(k => k.asset);
        const upperBound = sample.skip + sample.first;

        const { rows } = await pool.query<FeedbackRow>(
          `WITH requested AS (
             SELECT unnest($1::text[]) AS asset
           ), ranked AS (
             SELECT
               f.id,
               f.feedback_id::text AS feedback_id,
               f.asset,
               f.client_address,
               f.feedback_index,
               f.value,
               f.value_decimals,
               f.score,
               f.tag1,
               f.tag2,
               f.endpoint,
               f.feedback_uri,
               f.feedback_hash,
               f.running_digest,
               f.is_revoked,
               f.status,
               f.verified_at,
               f.tx_signature,
               f.block_slot,
               f.created_at,
               f.revoked_at,
               ROW_NUMBER() OVER (
                 PARTITION BY f.asset
                 ORDER BY ${sample.orderBy} ${sample.orderDirection}, f.client_address ASC, f.feedback_index ASC, f.feedback_id ASC NULLS LAST, f.id ASC
               ) AS rn
             FROM feedbacks f
             INNER JOIN requested r ON r.asset = f.asset
             WHERE f.status != 'ORPHANED'
           )
           SELECT
             id,
             feedback_id,
             asset,
             client_address,
             feedback_index,
             value,
             value_decimals,
             score,
             tag1,
             tag2,
             endpoint,
             feedback_uri,
             feedback_hash,
             running_digest,
             is_revoked,
             status,
             verified_at,
             tx_signature,
             block_slot,
             created_at,
             revoked_at
           FROM ranked
           WHERE rn > $2::int AND rn <= $3::int
           ORDER BY asset, rn`,
          [assets, sample.skip, upperBound]
        );

        const rowsByAsset = new Map<string, FeedbackRow[]>();
        for (const row of rows) {
          const list = rowsByAsset.get(row.asset) ?? [];
          list.push(row);
          rowsByAsset.set(row.asset, list);
        }

        for (const key of group) {
          results.set(feedbackPageCacheKey(key), rowsByAsset.get(key.asset) ?? []);
        }
      }

      return keys.map(k => results.get(feedbackPageCacheKey(k)) ?? []);
    },
    { cacheKeyFn: feedbackPageCacheKey }
  );
}

function createResponsesPageByFeedbackLoader(pool: Pool) {
  return new DataLoader<ResponsePageKey, ResponseRow[], string>(
    async (keys) => {
      const grouped = new Map<string, ResponsePageKey[]>();
      for (const key of keys) {
        const groupKey = `${key.first}:${key.skip}`;
        const group = grouped.get(groupKey) ?? [];
        group.push(key);
        grouped.set(groupKey, group);
      }

      const results = new Map<string, ResponseRow[]>();

      for (const group of grouped.values()) {
        const sample = group[0];
        const assets = group.map(k => k.asset);
        const clients = group.map(k => k.clientAddress);
        const indices = group.map(k => k.feedbackIndex);
        const upperBound = sample.skip + sample.first;

        const { rows } = await pool.query<ResponseRow>(
          `WITH requested AS (
             SELECT
               unnest($1::text[]) AS asset,
               unnest($2::text[]) AS client_address,
               unnest($3::bigint[]) AS feedback_index
           ), ranked AS (
             SELECT
               fr.id,
               fr.response_id::text AS response_id,
               fr.asset,
               fr.client_address,
               fr.feedback_index,
               fr.responder,
               fr.response_uri,
               fr.response_hash,
               fr.running_digest,
               fr.response_count,
               fr.status,
               fr.verified_at,
               fr.tx_signature,
               fr.block_slot,
               fr.created_at,
               ROW_NUMBER() OVER (
                 PARTITION BY fr.asset, fr.client_address, fr.feedback_index
                 ORDER BY fr.response_id ASC NULLS LAST, fr.created_at ASC, fr.id ASC
               ) AS rn
             FROM feedback_responses fr
             INNER JOIN requested r
               ON r.asset = fr.asset
              AND r.client_address = fr.client_address
              AND r.feedback_index = fr.feedback_index
             WHERE fr.status != 'ORPHANED'
           )
           SELECT
             id,
             response_id,
             asset,
             client_address,
             feedback_index,
             responder,
             response_uri,
             response_hash,
             running_digest,
             response_count,
             status,
             verified_at,
             tx_signature,
             block_slot,
             created_at
           FROM ranked
           WHERE rn > $4::int AND rn <= $5::int
           ORDER BY asset, client_address, feedback_index, rn`,
          [assets, clients, indices, sample.skip, upperBound]
        );

        const rowsByLookup = new Map<string, ResponseRow[]>();
        for (const row of rows) {
          const lookup = feedbackLookupKey(row.asset, row.client_address, row.feedback_index);
          const list = rowsByLookup.get(lookup) ?? [];
          list.push(row);
          rowsByLookup.set(lookup, list);
        }

        for (const key of group) {
          const lookup = feedbackLookupKey(key.asset, key.clientAddress, key.feedbackIndex);
          results.set(responsePageCacheKey(key), rowsByLookup.get(lookup) ?? []);
        }
      }

      return keys.map(k => results.get(responsePageCacheKey(k)) ?? []);
    },
    { cacheKeyFn: responsePageCacheKey }
  );
}

function createValidationsPageByAgentLoader(pool: Pool) {
  return new DataLoader<ValidationPageKey, ValidationRow[], string>(
    async (keys) => {
      const grouped = new Map<string, ValidationPageKey[]>();
      for (const key of keys) {
        const groupKey = `${key.first}:${key.skip}`;
        const group = grouped.get(groupKey) ?? [];
        group.push(key);
        grouped.set(groupKey, group);
      }

      const results = new Map<string, ValidationRow[]>();

      for (const group of grouped.values()) {
        const sample = group[0];
        const assets = group.map(k => k.asset);
        const upperBound = sample.skip + sample.first;

        const { rows } = await pool.query<ValidationRow>(
          `WITH requested AS (
             SELECT unnest($1::text[]) AS asset
           ), ranked AS (
             SELECT
               v.id,
               v.asset,
               v.validator_address AS validator,
               v.requester,
               v.nonce,
               v.request_uri,
               v.request_hash,
               v.response,
               v.response_uri,
               v.response_hash,
               v.tag,
               v.chain_status,
               v.created_at,
               v.updated_at AS responded_at,
               v.tx_signature AS request_tx_signature,
               v.tx_signature AS response_tx_signature,
               ROW_NUMBER() OVER (
                 PARTITION BY v.asset
                 ORDER BY v.created_at DESC, v.id DESC
               ) AS rn
             FROM validations v
             INNER JOIN requested r ON r.asset = v.asset
             WHERE v.chain_status != 'ORPHANED'
           )
           SELECT
             id,
             asset,
             validator,
             requester,
             nonce,
             request_uri,
             request_hash,
             response,
             response_uri,
             response_hash,
             tag,
             chain_status,
             created_at,
             responded_at,
             request_tx_signature,
             response_tx_signature
           FROM ranked
           WHERE rn > $2::int AND rn <= $3::int
           ORDER BY asset, rn`,
          [assets, sample.skip, upperBound]
        );

        const rowsByAsset = new Map<string, ValidationRow[]>();
        for (const row of rows) {
          const list = rowsByAsset.get(row.asset) ?? [];
          list.push(row);
          rowsByAsset.set(row.asset, list);
        }

        for (const key of group) {
          results.set(validationPageCacheKey(key), rowsByAsset.get(key.asset) ?? []);
        }
      }

      return keys.map(k => results.get(validationPageCacheKey(k)) ?? []);
    },
    { cacheKeyFn: validationPageCacheKey }
  );
}

function createMetadataByAgentLoader(pool: Pool) {
  return new DataLoader<string, MetadataRow[]>(async (keys) => {
    const { rows } = await pool.query<MetadataRow>(
      `SELECT id, asset, key, value, immutable, status, updated_at, tx_signature
       FROM metadata WHERE asset = ANY($1::text[])
       AND key NOT LIKE '\\_uri:%' ESCAPE '\\' AND status != 'ORPHANED'`,
      [keys as string[]]
    );
    const map = new Map<string, MetadataRow[]>();
    for (const row of rows) {
      const arr = map.get(row.asset) ?? [];
      arr.push(row);
      map.set(row.asset, arr);
    }
    return keys.map(k => map.get(k) ?? []);
  });
}

function createFeedbackCountByAgentLoader(pool: Pool) {
  return new DataLoader<string, number>(async (keys) => {
    const { rows } = await pool.query<FeedbackCountRow>(
      `SELECT asset, COUNT(*)::text as count FROM feedbacks
       WHERE asset = ANY($1::text[]) AND NOT is_revoked AND status != 'ORPHANED'
       GROUP BY asset`,
      [keys as string[]]
    );
    const map = new Map(rows.map(r => [r.asset, parseInt(r.count, 10)]));
    return keys.map(k => map.get(k) ?? 0);
  });
}

function createLastActivityByAgentLoader(pool: Pool) {
  return new DataLoader<string, string | null>(async (keys) => {
    const { rows } = await pool.query<LastActivityRow>(
      `SELECT asset, MAX(created_at)::text as last_activity FROM feedbacks
       WHERE asset = ANY($1::text[]) AND status != 'ORPHANED'
       GROUP BY asset`,
      [keys as string[]]
    );
    const map = new Map(rows.map(r => [r.asset, r.last_activity]));
    return keys.map(k => map.get(k) ?? null);
  });
}

function createRegistrationByAgentLoader(pool: Pool) {
  return new DataLoader<string, RegistrationRow[]>(async (keys) => {
    const { rows } = await pool.query<RegistrationRow>(
      `SELECT asset, key, value FROM metadata
       WHERE asset = ANY($1::text[]) AND key LIKE '\\_uri:%' ESCAPE '\\' AND status != 'ORPHANED'`,
      [keys as string[]]
    );
    const map = new Map<string, RegistrationRow[]>();
    for (const row of rows) {
      const arr = map.get(row.asset) ?? [];
      arr.push(row);
      map.set(row.asset, arr);
    }
    return keys.map(k => map.get(k) ?? []);
  });
}

function createAgentStatsByAgentLoader(pool: Pool) {
  return new DataLoader<string, AgentStatsRow | null>(async (keys) => {
    const { rows } = await pool.query<AgentStatsRow>(
      `SELECT
         a.asset,
         COALESCE(f.cnt, 0)::text as feedback_count,
         f.avg_val::text as avg_value,
         COALESCE(v.cnt, 0)::text as validation_count,
         COALESCE(v.completed, 0)::text as completed_validations,
         v.avg_score::text as avg_validation_score,
         GREATEST(f.last_fb, v.last_val)::text as last_activity
       FROM (SELECT unnest($1::text[]) as asset) a
       LEFT JOIN (
         SELECT asset, COUNT(*) as cnt, AVG(value::numeric / POWER(10, COALESCE(value_decimals, 0))) as avg_val, MAX(created_at) as last_fb
         FROM feedbacks WHERE asset = ANY($1::text[]) AND NOT is_revoked AND status != 'ORPHANED'
         GROUP BY asset
       ) f ON f.asset = a.asset
       LEFT JOIN (
         SELECT asset, COUNT(*) as cnt,
                COUNT(*) FILTER (WHERE response IS NOT NULL) as completed,
                AVG(response) FILTER (WHERE response IS NOT NULL) as avg_score,
                MAX(created_at) as last_val
         FROM validations WHERE asset = ANY($1::text[]) AND chain_status != 'ORPHANED'
         GROUP BY asset
       ) v ON v.asset = a.asset`,
      [keys as string[]]
    );
    const map = new Map(rows.map(r => [r.asset, r]));
    return keys.map(k => map.get(k) ?? null);
  });
}

export interface DataLoaders {
  agentById: DataLoader<string, AgentRow | null>;
  feedbackByLookup: DataLoader<string, FeedbackRow | null>;
  feedbackPageByAgent: DataLoader<FeedbackPageKey, FeedbackRow[]>;
  responsesPageByFeedback: DataLoader<ResponsePageKey, ResponseRow[]>;
  validationsPageByAgent: DataLoader<ValidationPageKey, ValidationRow[]>;
  metadataByAgent: DataLoader<string, MetadataRow[]>;
  feedbackCountByAgent: DataLoader<string, number>;
  lastActivityByAgent: DataLoader<string, string | null>;
  registrationByAgent: DataLoader<string, RegistrationRow[]>;
  agentStatsByAgent: DataLoader<string, AgentStatsRow | null>;
}

export function createDataLoaders(pool: Pool): DataLoaders {
  return {
    agentById: createAgentByIdLoader(pool),
    feedbackByLookup: createFeedbackByLookupLoader(pool),
    feedbackPageByAgent: createFeedbackPageByAgentLoader(pool),
    responsesPageByFeedback: createResponsesPageByFeedbackLoader(pool),
    validationsPageByAgent: createValidationsPageByAgentLoader(pool),
    metadataByAgent: createMetadataByAgentLoader(pool),
    feedbackCountByAgent: createFeedbackCountByAgentLoader(pool),
    lastActivityByAgent: createLastActivityByAgentLoader(pool),
    registrationByAgent: createRegistrationByAgentLoader(pool),
    agentStatsByAgent: createAgentStatsByAgentLoader(pool),
  };
}
