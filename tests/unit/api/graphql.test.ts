import { describe, it, expect, vi, beforeEach } from 'vitest';
import { parse } from 'graphql';

vi.mock('../../../src/logger.js', () => {
  const mockLogger = {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    fatal: vi.fn(),
    child: vi.fn(() => mockLogger),
  };
  return {
    createChildLogger: vi.fn(() => mockLogger),
    logger: mockLogger,
  };
});

import { analyzeQuery, calculateComplexity, countAliases, MAX_COMPLEXITY, MAX_FIRST_CAP, MAX_ALIASES } from '../../../src/api/graphql/plugins/complexity.js';
import { analyzeDepth, calculateDepth, MAX_DEPTH } from '../../../src/api/graphql/plugins/depth-limit.js';
import { buildWhereClause } from '../../../src/api/graphql/utils/filters.js';
import { clampFirst, clampSkip, encodeCursor, decodeCursor, MAX_FIRST, MAX_SKIP } from '../../../src/api/graphql/utils/pagination.js';
import { scalarResolvers } from '../../../src/api/graphql/resolvers/scalars.js';
import { queryResolvers, resetAggregatedStatsCacheForTests } from '../../../src/api/graphql/resolvers/query.js';
import { agentResolvers } from '../../../src/api/graphql/resolvers/agent.js';
import { feedbackResolvers } from '../../../src/api/graphql/resolvers/feedback.js';
import { responseResolvers } from '../../../src/api/graphql/resolvers/response.js';
import { solanaResolvers } from '../../../src/api/graphql/resolvers/solana.js';
import { validationResolvers } from '../../../src/api/graphql/resolvers/validation.js';

describe('GraphQL Complexity Analysis', () => {
  it('allows simple queries', () => {
    const doc = parse('{ agents(first: 10) { id agentURI } }');
    const result = analyzeQuery(doc);
    expect(result.allowed).toBe(true);
    expect(result.cost).toBeLessThan(MAX_COMPLEXITY);
  });

  it('rejects overly complex queries', () => {
    const doc = parse(`{
      a1: agents(first: 250) { id feedback(first: 250) { id responses { id } } }
      a2: agents(first: 250) { id feedback(first: 250) { id responses { id } } }
      a3: agents(first: 250) { id feedback(first: 250) { id responses { id } } }
    }`);
    const result = analyzeQuery(doc);
    expect(result.allowed).toBe(false);
  });

  it('rejects queries with too many aliases', () => {
    const fields = Array.from({ length: 12 }, (_, i) => `a${i}: agents(first: 1) { id }`).join('\n');
    const doc = parse(`{ ${fields} }`);
    const result = analyzeQuery(doc);
    expect(result.allowed).toBe(false);
    expect(result.reason).toContain('aliases');
  });

  it('allows query with few aliases', () => {
    const doc = parse('{ a1: agents(first: 5) { id } a2: agents(first: 5) { id } }');
    const result = analyzeQuery(doc);
    expect(result.allowed).toBe(true);
  });

  it('calculates complexity based on first argument', () => {
    const doc1 = parse('{ agents(first: 1) { id } }');
    const doc250 = parse('{ agents(first: 250) { id } }');
    const cost1 = calculateComplexity(doc1);
    const cost250 = calculateComplexity(doc250);
    expect(cost250).toBeGreaterThan(cost1);
  });

  it('caps first argument by MAX_FIRST_CAP', () => {
    const capped = calculateComplexity(parse('{ agents(first: 999999) { id } }'));
    const atCap = calculateComplexity(parse(`{ agents(first: ${MAX_FIRST_CAP}) { id } }`));
    expect(capped).toBe(atCap);
  });

  it('applies nested list multipliers to complexity', () => {
    const nested = parse('{ agents(first: 10) { feedback(first: 10) { id } } }');
    const flat = parse('{ agents(first: 10) { id } }');
    expect(calculateComplexity(nested)).toBeGreaterThan(calculateComplexity(flat));
  });
});

describe('GraphQL Depth Limit', () => {
  it('allows shallow queries', () => {
    const doc = parse('{ agents { id owner } }');
    const result = analyzeDepth(doc);
    expect(result.allowed).toBe(true);
    expect(result.depth).toBeLessThanOrEqual(MAX_DEPTH);
  });

  it('rejects deeply nested queries', () => {
    const doc = parse(`{
      agents {
        feedback {
          agent {
            feedback {
              agent {
                feedback { id }
              }
            }
          }
        }
      }
    }`);
    const result = analyzeDepth(doc);
    expect(result.allowed).toBe(false);
    expect(result.depth).toBeGreaterThan(MAX_DEPTH);
  });

  it('calculates correct depth', () => {
    const doc = parse('{ agents { id } }');
    expect(calculateDepth(doc)).toBe(2);
  });
});

describe('Filter Builder', () => {
  it('builds empty filter with orphaned exclusion', () => {
    const result = buildWhereClause('agent', null);
    expect(result.sql).toContain("status != 'ORPHANED'");
    expect(result.params).toHaveLength(0);
  });

  it('builds agent filter with owner', () => {
    const result = buildWhereClause('agent', { owner: 'testOwner123' });
    expect(result.sql).toContain('owner = $1');
    expect(result.params).toEqual(['testOwner123']);
    expect(result.sql).toContain("status != 'ORPHANED'");
  });

  it('builds filter with array values', () => {
    const result = buildWhereClause('agent', { id_in: ['abc', 'def'] });
    expect(result.sql).toContain('ANY($1::text[])');
    expect(result.params[0]).toEqual(['abc', 'def']);
  });

  it('accepts raw agent ID in filter', () => {
    const result = buildWhereClause('agent', { id: 'myAssetPubkey' });
    expect(result.sql).toContain('asset = $1');
    expect(result.params).toEqual(['myAssetPubkey']);
  });

  it('accepts raw agent ID in feedback filter', () => {
    const result = buildWhereClause('feedback', { agent: 'myAssetPubkey' });
    expect(result.sql).toContain('asset = $1');
    expect(result.params).toEqual(['myAssetPubkey']);
  });

  it('handles boolean filters', () => {
    const result = buildWhereClause('agent', { atomEnabled: true });
    expect(result.sql).toContain('atom_enabled = $1');
    expect(result.params).toEqual([true]);
  });

  it('handles timestamp comparison filters', () => {
    const result = buildWhereClause('agent', { createdAt_gt: 1700000000 });
    expect(result.sql).toContain('created_at > to_timestamp($1)');
    expect(result.params).toEqual([1700000000]);
  });

  it('handles updatedAt range filters', () => {
    const result = buildWhereClause('agent', { updatedAt_gt: 1700000000, updatedAt_lt: 1700000500 });
    expect(result.sql).toContain('updated_at > to_timestamp($1)');
    expect(result.sql).toContain('updated_at < to_timestamp($2)');
    expect(result.params).toEqual([1700000000, 1700000500]);
  });

  it('ignores unknown filter fields', () => {
    const result = buildWhereClause('agent', { unknownField: 'value', owner: 'test' });
    expect(result.sql).toContain('owner = $1');
    expect(result.params).toHaveLength(1);
  });

  it('uses chain_status for validation entities', () => {
    const result = buildWhereClause('validation', null);
    expect(result.sql).toContain("chain_status != 'ORPHANED'");
  });

  it('maps collection and parent identity filters for agents', () => {
    const result = buildWhereClause('agent', {
      creator: 'Creator111',
      collectionPointer: 'c1:bafy-test',
      parentAsset: 'Parent111',
      colLocked: true,
    });
    expect(result.sql).toContain('creator = $1');
    expect(result.sql).toContain('canonical_col = $2');
    expect(result.sql).toContain('parent_asset = $3');
    expect(result.sql).toContain('col_locked = $4');
    expect(result.params).toEqual(['Creator111', 'c1:bafy-test', 'Parent111', true]);
  });

  it('handles multiple filters', () => {
    const result = buildWhereClause('feedback', {
      agent: 'asset1',
      tag1: 'uptime',
      isRevoked: false,
    });
    expect(result.sql).toContain('asset = $1');
    expect(result.sql).toContain('tag1 = $2');
    expect(result.sql).toContain('is_revoked = $3');
    expect(result.params).toEqual(['asset1', 'uptime', false]);
  });

  it('builds response feedback filter from feedback ID', () => {
    const result = buildWhereClause('response', { feedback: 'sol:asset1:clientA:9' });
    expect(result.sql).toContain('asset = $1');
    expect(result.sql).toContain('client_address = $2');
    expect(result.sql).toContain('feedback_index = $3::bigint');
    expect(result.params).toEqual(['asset1', 'clientA', '9']);
  });

  it('maps validation status filter to response nullability', () => {
    const pending = buildWhereClause('validation', { status: 'PENDING' });
    const completed = buildWhereClause('validation', { status: 'COMPLETED' });
    expect(pending.sql).toContain('response IS NULL');
    expect(completed.sql).toContain('response IS NOT NULL');
  });

  it('caps oversized _in filter arrays', () => {
    const ids = Array.from({ length: 300 }, (_, i) => `id${i}`);
    const result = buildWhereClause('agent', { id_in: ids });
    expect((result.params[0] as string[]).length).toBe(250);
  });
});

describe('Pagination Utilities', () => {
  function decodeBase64Json(cursor: string): Record<string, unknown> {
    return JSON.parse(Buffer.from(cursor, 'base64').toString('utf-8')) as Record<string, unknown>;
  }

  it('clamps first to MAX_FIRST', () => {
    expect(clampFirst(1000)).toBe(MAX_FIRST);
    expect(clampFirst(50)).toBe(50);
    expect(clampFirst(undefined)).toBe(100);
    expect(clampFirst(null)).toBe(100);
    expect(clampFirst(-1)).toBe(100);
    expect(clampFirst(0)).toBe(100);
  });

  it('clamps skip to MAX_SKIP', () => {
    expect(clampSkip(10000)).toBe(MAX_SKIP);
    expect(clampSkip(50)).toBe(50);
    expect(clampSkip(undefined)).toBe(0);
    expect(clampSkip(null)).toBe(0);
    expect(clampSkip(-5)).toBe(0);
  });

  it('encodes and decodes cursor', () => {
    const data = { created_at: '2025-01-01T00:00:00Z', asset: 'testAsset' };
    const cursor = encodeCursor(data);
    expect(typeof cursor).toBe('string');

    const decoded = decodeCursor(cursor);
    expect(decoded).toEqual(data);
  });

  it('returns null for invalid cursor', () => {
    expect(decodeCursor('')).toBeNull();
    expect(decodeCursor('not-base64-json')).toBeNull();
    expect(decodeCursor(Buffer.from('{}').toString('base64'))).toBeNull();
    expect(decodeCursor(Buffer.from('{"created_at": 123}').toString('base64'))).toBeNull();
  });

  it('exposes Agent.cursor compatible with Query.agents(after: ...)', () => {
    const parent = { asset: 'agent1', created_at: '2025-01-01T00:00:00Z' } as any;
    const cursor = agentResolvers.Agent.cursor(parent);
    expect(decodeCursor(cursor)).toEqual({ created_at: parent.created_at, asset: parent.asset });
  });

  it('exposes Feedback.cursor compatible with Query.feedbacks(after: ...)', () => {
    const parent = {
      id: 'fb1',
      asset: 'agent1',
      client_address: 'client1',
      feedback_index: '42',
      created_at: '2025-01-01T00:00:00Z',
    } as any;
    const cursor = feedbackResolvers.Feedback.cursor(parent);
    expect(decodeBase64Json(cursor)).toEqual({
      created_at: parent.created_at,
      asset: parent.asset,
      client_address: parent.client_address,
      feedback_index: parent.feedback_index,
      id: parent.id,
    });
  });

  it('exposes FeedbackResponse.cursor compatible with Query.feedbackResponses(after: ...)', () => {
    const parent = { id: 'resp1', created_at: '2025-01-01T00:00:00Z' } as any;
    const cursor = responseResolvers.FeedbackResponse.cursor(parent);
    expect(decodeBase64Json(cursor)).toEqual({ created_at: parent.created_at, id: parent.id });
  });

  it('exposes Validation.cursor compatible with Query.validations(after: ...)', () => {
    const parent = { id: 'val1', created_at: '2025-01-01T00:00:00Z' } as any;
    const cursor = validationResolvers.Validation.cursor(parent);
    expect(decodeBase64Json(cursor)).toEqual({ created_at: parent.created_at, id: parent.id });
  });
});

describe('Scalar Resolvers', () => {
  it('serializes BigInt as string', () => {
    expect(scalarResolvers.BigInt.serialize(42n)).toBe('42');
    expect(scalarResolvers.BigInt.serialize(123)).toBe('123');
    expect(scalarResolvers.BigInt.serialize('999')).toBe('999');
  });

  it('parses BigInt from string', () => {
    expect(scalarResolvers.BigInt.parseValue('42')).toBe(42n);
    expect(scalarResolvers.BigInt.parseValue(42)).toBe(42n);
  });

  it('serializes BigDecimal as string', () => {
    expect(scalarResolvers.BigDecimal.serialize('99.77')).toBe('99.77');
    expect(scalarResolvers.BigDecimal.serialize(3.14)).toBe('3.14');
  });

  it('serializes Bytes from Buffer', () => {
    const buf = Buffer.from([0xab, 0xcd]);
    expect(scalarResolvers.Bytes.serialize(buf)).toBe('abcd');
  });

  it('passes through hex string for Bytes', () => {
    expect(scalarResolvers.Bytes.serialize('deadbeef')).toBe('deadbeef');
  });
});

describe('Agent Field Resolvers', () => {
  it('maps Agent.agentId from DB agent_id', () => {
    const parent = { asset: 'asset1', agent_id: '42' } as any;
    expect(agentResolvers.Agent.agentId(parent)).toBe('42');
  });

  it('throws when DB agent_id is null', () => {
    const parent = { asset: 'asset2', agent_id: null } as any;
    expect(() => agentResolvers.Agent.agentId(parent)).toThrow('Missing agent_id');
  });
});

describe('Feedback Field Resolvers', () => {
  it('normalizes feedback.value to decimal string and preserves string output', () => {
    const normalized = feedbackResolvers.Feedback.value({
      value: '1234',
      value_decimals: 2,
    } as any);
    expect(normalized).toBe('12.34');
    expect(typeof normalized).toBe('string');

    const negative = feedbackResolvers.Feedback.value({
      value: '-1200',
      value_decimals: 3,
    } as any);
    expect(negative).toBe('-1.2');

    const padded = feedbackResolvers.Feedback.value({
      value: '5',
      value_decimals: 3,
    } as any);
    expect(padded).toBe('0.005');
  });

  it('passes through value_decimals on Solana feedback extension', () => {
    const valueDecimals = solanaResolvers.SolanaFeedbackExtension.valueDecimals({
      value_decimals: 18,
    } as any);
    expect(valueDecimals).toBe(18);
  });
});

describe('Query Resolver User Input Errors', () => {
  it('returns BAD_USER_INPUT when combining after and skip', async () => {
    const poolQuery = vi.fn();
    const ctx = {
      pool: { query: poolQuery },
      prisma: null,
      loaders: {},
      networkMode: 'devnet',
    } as any;
    const after = Buffer.from(JSON.stringify({
      created_at: '2025-01-01T00:00:00Z',
      asset: 'agent1',
    })).toString('base64');

    await expect(
      queryResolvers.Query.agents({}, { first: 10, skip: 1, after }, ctx)
    ).rejects.toMatchObject({
      message: 'Cannot combine cursor pagination (after) with offset pagination (skip).',
      extensions: { code: 'BAD_USER_INPUT' },
    });

    expect(poolQuery).not.toHaveBeenCalled();
  });

  it('returns BAD_USER_INPUT when using after with non-createdAt orderBy', async () => {
    const poolQuery = vi.fn();
    const ctx = {
      pool: { query: poolQuery },
      prisma: null,
      loaders: {},
      networkMode: 'devnet',
    } as any;
    const after = Buffer.from(JSON.stringify({
      created_at: '2025-01-01T00:00:00Z',
      asset: 'agent1',
    })).toString('base64');

    await expect(
      queryResolvers.Query.agents({}, { first: 10, after, orderBy: 'updatedAt' }, ctx)
    ).rejects.toMatchObject({
      message: 'The after cursor is only supported when orderBy is createdAt.',
      extensions: { code: 'BAD_USER_INPUT' },
    });

    expect(poolQuery).not.toHaveBeenCalled();
  });

  it('returns BAD_USER_INPUT for invalid feedbacks cursor format', async () => {
    const poolQuery = vi.fn();
    const ctx = {
      pool: { query: poolQuery },
      prisma: null,
      loaders: {},
      networkMode: 'devnet',
    } as any;

    await expect(
      queryResolvers.Query.feedbacks({}, { first: 10, after: 'invalid-cursor' }, ctx)
    ).rejects.toMatchObject({
      message: 'Invalid feedbacks cursor. Expected base64 JSON with created_at, asset, client_address, feedback_index, and id.',
      extensions: { code: 'BAD_USER_INPUT' },
    });

    expect(poolQuery).not.toHaveBeenCalled();
  });

  it('returns BAD_USER_INPUT for invalid feedbackResponses cursor format', async () => {
    const poolQuery = vi.fn();
    const ctx = {
      pool: { query: poolQuery },
      prisma: null,
      loaders: {},
      networkMode: 'devnet',
    } as any;

    await expect(
      queryResolvers.Query.feedbackResponses({}, { first: 10, after: 'invalid-cursor' }, ctx)
    ).rejects.toMatchObject({
      message: 'Invalid feedbackResponses cursor. Expected base64 JSON with created_at and optional id.',
      extensions: { code: 'BAD_USER_INPUT' },
    });

    expect(poolQuery).not.toHaveBeenCalled();
  });
});

describe('Query Aggregated Stats Cache', () => {
  beforeEach(() => {
    resetAggregatedStatsCacheForTests();
  });

  it('caches globalStats results within TTL', async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [{
        total_agents: '10',
        total_feedback: '20',
        total_validations: '30',
        tags: ['ai'],
      }],
    });

    const ctx = {
      pool: { query },
      prisma: null,
      loaders: {},
      networkMode: 'devnet',
    } as any;

    const first = await queryResolvers.Query.globalStats({}, { id: 'stats' }, ctx);
    const second = await queryResolvers.Query.globalStats({}, { id: 'stats' }, ctx);

    expect(query).toHaveBeenCalledTimes(1);
    expect(first.totalAgents).toBe('10');
    expect(second.totalAgents).toBe('10');
    expect(second.tags).toEqual(['ai']);
  });

  it('deduplicates concurrent globalStats refreshes', async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [{
        total_agents: '1',
        total_feedback: '2',
        total_validations: '3',
        tags: ['x'],
      }],
    });

    const ctx = {
      pool: { query },
      prisma: null,
      loaders: {},
      networkMode: 'mainnet',
    } as any;

    await Promise.all([
      queryResolvers.Query.globalStats({}, { id: 'a' }, ctx),
      queryResolvers.Query.protocol({}, { id: 'b' }, ctx),
    ]);

    expect(query).toHaveBeenCalledTimes(1);
  });
});

describe('Collection And Tree Queries', () => {
  it('maps collections rows to GraphQL shape', async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [{
        col: 'c1:bafy-test',
        creator: 'Creator111',
        first_seen_asset: 'Asset111',
        first_seen_at: '2026-02-25T12:00:00.000Z',
        first_seen_slot: '123',
        first_seen_tx_signature: 'sig1',
        last_seen_at: '2026-02-25T12:10:00.000Z',
        last_seen_slot: '130',
        last_seen_tx_signature: 'sig2',
        asset_count: '2',
        version: '1.0.0',
        name: 'My Collection',
        symbol: 'COLL',
        description: 'Optional description',
        image: 'ipfs://bafy-img',
        banner_image: 'ipfs://bafy-banner',
        social_website: 'https://example.com',
        social_x: '@mycollection',
        social_discord: 'https://discord.gg/mycollection',
        metadata_status: 'ok',
        metadata_hash: 'abcd',
        metadata_bytes: '256',
        metadata_updated_at: '2026-02-25T12:11:00.000Z',
      }],
    });

    const ctx = {
      pool: { query },
      prisma: null,
      loaders: {},
      networkMode: 'devnet',
    } as any;

    const rows = await queryResolvers.Query.collections(
      {},
      { first: 10, skip: 0, collection: 'c1:bafy-test' },
      ctx
    );

    expect(query).toHaveBeenCalledTimes(1);
    expect(rows).toEqual([
      expect.objectContaining({
        collection: 'c1:bafy-test',
        creator: 'Creator111',
        firstSeenAsset: 'Asset111',
        firstSeenSlot: '123',
        lastSeenSlot: '130',
        assetCount: '2',
        version: '1.0.0',
        name: 'My Collection',
        symbol: 'COLL',
        metadataStatus: 'ok',
        metadataBytes: '256',
      }),
    ]);
    expect(typeof rows[0].firstSeenAt).toBe('string');
    expect(typeof rows[0].lastSeenAt).toBe('string');
  });

  it('counts assets by creator+collection scope', async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [{ count: '42' }],
    });
    const ctx = {
      pool: { query },
      prisma: null,
      loaders: {},
      networkMode: 'devnet',
    } as any;

    const count = await queryResolvers.Query.collectionAssetCount(
      {},
      { collection: 'c1:bafy-test', creator: 'Creator111' },
      ctx
    );

    expect(count).toBe('42');
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0][1]).toEqual(['c1:bafy-test', 'Creator111']);
  });

  it('lists collection assets with pagination and primes dataloader', async () => {
    const row = {
      asset: 'Asset111',
      owner: 'Owner111',
      creator: 'Creator111',
      agent_uri: 'ipfs://asset',
      agent_wallet: null,
      collection: 'Collection111',
      collection_pointer: 'c1:bafy-test',
      col_locked: true,
      parent_asset: null,
      parent_creator: null,
      parent_locked: false,
      nft_name: 'asset',
      atom_enabled: true,
      trust_tier: null,
      quality_score: null,
      confidence: null,
      risk_score: null,
      diversity_ratio: null,
      sort_key: null,
      status: 'FINALIZED',
      verified_at: null,
      created_at: '2026-02-25T12:00:00.000Z',
      updated_at: '2026-02-25T12:00:00.000Z',
      created_tx_signature: null,
      created_slot: null,
      feedback_digest: null,
      response_digest: null,
      revoke_digest: null,
      feedback_count: '0',
      response_count: '0',
      revoke_count: '0',
    };
    const query = vi.fn().mockResolvedValue({ rows: [row] });
    const prime = vi.fn();
    const ctx = {
      pool: { query },
      prisma: null,
      loaders: { agentById: { prime } },
      networkMode: 'devnet',
    } as any;

    const rows = await queryResolvers.Query.collectionAssets(
      {},
      { collection: 'c1:bafy-test', creator: 'Creator111', first: 25, skip: 5, orderBy: 'createdAt', orderDirection: 'desc' },
      ctx
    );

    expect(rows).toHaveLength(1);
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0][1]).toEqual(['c1:bafy-test', 'Creator111', 25, 5]);
    expect(prime).toHaveBeenCalledWith('Asset111', expect.any(Object));
  });

  it('loads direct children and primes dataloader', async () => {
    const childRow = {
      asset: 'Child111',
      owner: 'Owner111',
      creator: 'Creator111',
      agent_uri: 'ipfs://child',
      agent_wallet: null,
      collection: 'Collection111',
      collection_pointer: 'c1:bafy-test',
      col_locked: true,
      parent_asset: 'Parent111',
      parent_creator: 'ParentCreator111',
      parent_locked: true,
      nft_name: 'child',
      atom_enabled: true,
      trust_tier: null,
      quality_score: null,
      confidence: null,
      risk_score: null,
      diversity_ratio: null,
      sort_key: null,
      status: 'FINALIZED',
      verified_at: null,
      created_at: '2026-02-25T12:00:00.000Z',
      updated_at: '2026-02-25T12:00:00.000Z',
      created_tx_signature: null,
      created_slot: null,
      feedback_digest: null,
      response_digest: null,
      revoke_digest: null,
      feedback_count: '0',
      response_count: '0',
      revoke_count: '0',
    };
    const query = vi.fn().mockResolvedValue({ rows: [childRow] });
    const prime = vi.fn();
    const ctx = {
      pool: { query },
      prisma: null,
      loaders: { agentById: { prime } },
      networkMode: 'devnet',
    } as any;

    const rows = await queryResolvers.Query.agentChildren(
      {},
      { parent: 'Parent111', first: 10, skip: 0 },
      ctx
    );

    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0][1][0]).toBe('Parent111');
    expect(rows).toHaveLength(1);
    expect(prime).toHaveBeenCalledWith('Child111', expect.objectContaining({ parent_asset: 'Parent111' }));
  });

  it('builds agent lineage from root to asset', async () => {
    const lineageRows = [
      { asset: 'Root111', parent_asset: null, path: ['Child111', 'Root111'], depth: 1 },
      { asset: 'Child111', parent_asset: 'Root111', path: ['Child111'], depth: 0 },
    ];
    const agentRows = [
      {
        asset: 'Root111',
        owner: 'OwnerRoot',
        creator: 'CreatorRoot',
        agent_uri: 'ipfs://root',
        agent_wallet: null,
        collection: 'Collection111',
        collection_pointer: 'c1:bafy-test',
        col_locked: true,
        parent_asset: null,
        parent_creator: null,
        parent_locked: false,
        nft_name: 'root',
        atom_enabled: true,
        trust_tier: null,
        quality_score: null,
        confidence: null,
        risk_score: null,
        diversity_ratio: null,
        sort_key: null,
        status: 'FINALIZED',
        verified_at: null,
        created_at: '2026-02-25T12:00:00.000Z',
        updated_at: '2026-02-25T12:00:00.000Z',
        created_tx_signature: null,
        created_slot: null,
        feedback_digest: null,
        response_digest: null,
        revoke_digest: null,
        feedback_count: '0',
        response_count: '0',
        revoke_count: '0',
      },
      {
        asset: 'Child111',
        owner: 'OwnerChild',
        creator: 'CreatorRoot',
        agent_uri: 'ipfs://child',
        agent_wallet: null,
        collection: 'Collection111',
        collection_pointer: 'c1:bafy-test',
        col_locked: true,
        parent_asset: 'Root111',
        parent_creator: 'CreatorRoot',
        parent_locked: true,
        nft_name: 'child',
        atom_enabled: true,
        trust_tier: null,
        quality_score: null,
        confidence: null,
        risk_score: null,
        diversity_ratio: null,
        sort_key: null,
        status: 'FINALIZED',
        verified_at: null,
        created_at: '2026-02-25T12:01:00.000Z',
        updated_at: '2026-02-25T12:01:00.000Z',
        created_tx_signature: null,
        created_slot: null,
        feedback_digest: null,
        response_digest: null,
        revoke_digest: null,
        feedback_count: '0',
        response_count: '0',
        revoke_count: '0',
      },
    ];
    const query = vi.fn()
      .mockResolvedValueOnce({ rows: lineageRows })
      .mockResolvedValueOnce({ rows: agentRows });
    const prime = vi.fn();
    const ctx = {
      pool: { query },
      prisma: null,
      loaders: { agentById: { prime } },
      networkMode: 'devnet',
    } as any;

    const rows = await queryResolvers.Query.agentLineage(
      {},
      { asset: 'Child111', includeSelf: true },
      ctx
    );

    expect(rows).toHaveLength(2);
    expect(rows[0].asset).toBe('Root111');
    expect(rows[1].asset).toBe('Child111');
    expect(prime).toHaveBeenCalledWith('Root111', expect.any(Object));
    expect(prime).toHaveBeenCalledWith('Child111', expect.any(Object));
  });

  it('builds agentTree with depth/path and embedded agent rows', async () => {
    const treeRows = [
      { asset: 'Root111', parent_asset: null, path: ['Root111'], depth: 0 },
      { asset: 'Child111', parent_asset: 'Root111', path: ['Root111', 'Child111'], depth: 1 },
    ];
    const agentRows = [
      {
        asset: 'Root111',
        owner: 'OwnerRoot',
        creator: 'CreatorRoot',
        agent_uri: 'ipfs://root',
        agent_wallet: null,
        collection: 'Collection111',
        collection_pointer: 'c1:bafy-test',
        col_locked: true,
        parent_asset: null,
        parent_creator: null,
        parent_locked: false,
        nft_name: 'root',
        atom_enabled: true,
        trust_tier: null,
        quality_score: null,
        confidence: null,
        risk_score: null,
        diversity_ratio: null,
        sort_key: null,
        status: 'FINALIZED',
        verified_at: null,
        created_at: '2026-02-25T12:00:00.000Z',
        updated_at: '2026-02-25T12:00:00.000Z',
        created_tx_signature: null,
        created_slot: null,
        feedback_digest: null,
        response_digest: null,
        revoke_digest: null,
        feedback_count: '0',
        response_count: '0',
        revoke_count: '0',
      },
      {
        asset: 'Child111',
        owner: 'OwnerChild',
        creator: 'CreatorRoot',
        agent_uri: 'ipfs://child',
        agent_wallet: null,
        collection: 'Collection111',
        collection_pointer: 'c1:bafy-test',
        col_locked: true,
        parent_asset: 'Root111',
        parent_creator: 'CreatorRoot',
        parent_locked: true,
        nft_name: 'child',
        atom_enabled: true,
        trust_tier: null,
        quality_score: null,
        confidence: null,
        risk_score: null,
        diversity_ratio: null,
        sort_key: null,
        status: 'FINALIZED',
        verified_at: null,
        created_at: '2026-02-25T12:01:00.000Z',
        updated_at: '2026-02-25T12:01:00.000Z',
        created_tx_signature: null,
        created_slot: null,
        feedback_digest: null,
        response_digest: null,
        revoke_digest: null,
        feedback_count: '0',
        response_count: '0',
        revoke_count: '0',
      },
    ];

    const query = vi.fn()
      .mockResolvedValueOnce({ rows: treeRows })
      .mockResolvedValueOnce({ rows: agentRows });
    const prime = vi.fn();
    const ctx = {
      pool: { query },
      prisma: null,
      loaders: { agentById: { prime } },
      networkMode: 'devnet',
    } as any;

    const rows = await queryResolvers.Query.agentTree(
      {},
      { root: 'Root111', maxDepth: 2, includeRoot: true },
      ctx
    );

    expect(query).toHaveBeenCalledTimes(2);
    expect(query.mock.calls[0][1][0]).toBe('Root111');
    expect(rows).toHaveLength(2);
    expect(rows[1]).toEqual(
      expect.objectContaining({
        depth: 1,
        path: ['Root111', 'Child111'],
        parentAsset: 'Root111',
      })
    );
    expect(rows[1].agent).toEqual(expect.objectContaining({ asset: 'Child111' }));
    expect(prime).toHaveBeenCalledWith('Root111', expect.any(Object));
    expect(prime).toHaveBeenCalledWith('Child111', expect.any(Object));
  });
});
