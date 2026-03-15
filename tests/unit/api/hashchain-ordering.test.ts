import { describe, expect, it, vi } from 'vitest';
import { hashChainResolvers } from '../../../src/api/graphql/resolvers/hashchain.js';

const AGENT = '11111111111111111111111111111111';

describe('HashChain resolver ordering', () => {
  it('uses canonical tx ordering for feedback and response heads', async () => {
    const sqls: string[] = [];
    const query = vi.fn(async (sql: string) => {
      sqls.push(sql);

      if (sql.includes('MAX(feedback_index) + 1')) {
        return { rows: [{ count: '2' }] };
      }
      if (sql.includes('MAX(response_count)')) {
        return { rows: [{ count: '2' }] };
      }
      if (sql.includes('MAX(revoke_count)')) {
        return { rows: [{ count: '1' }] };
      }
      if (sql.includes('FROM feedbacks') && sql.includes('FROM orphan_feedbacks') && sql.includes('running_digest')) {
        return { rows: [{ digest: 'feed-head' }] };
      }
      if (sql.includes('FROM feedback_responses') && sql.includes('FROM orphan_responses') && sql.includes('running_digest')) {
        return { rows: [{ digest: 'resp-head' }] };
      }
      if (sql.includes('FROM revocations') && sql.includes('running_digest')) {
        return { rows: [{ digest: 'revoke-head' }] };
      }
      return { rows: [] };
    });

    const result = await hashChainResolvers.Query.hashChainHeads({}, { agent: AGENT }, { pool: { query } } as any);

    const feedbackSql = sqls.find((sql) => sql.includes('FROM feedbacks') && sql.includes('FROM orphan_feedbacks') && sql.includes('running_digest'));
    const responseSql = sqls.find((sql) => sql.includes('FROM feedback_responses') && sql.includes('FROM orphan_responses') && sql.includes('running_digest'));
    const revokeSql = sqls.find((sql) => sql.includes('FROM revocations') && sql.includes('running_digest'));

    expect(feedbackSql).toContain('ORDER BY block_slot DESC NULLS LAST, tx_index DESC NULLS LAST, event_ordinal DESC NULLS LAST, tx_signature DESC NULLS LAST, row_id DESC');
    expect(feedbackSql).toContain('FROM orphan_feedbacks');
    expect(responseSql).toContain('ORDER BY block_slot DESC NULLS LAST, tx_index DESC NULLS LAST, event_ordinal DESC NULLS LAST, tx_signature DESC NULLS LAST, row_id DESC');
    expect(responseSql).toContain('FROM orphan_responses');
    expect(revokeSql).not.toContain("status != 'ORPHANED'");
    expect(result).toEqual({
      feedback: { digest: 'feed-head', count: '2' },
      response: { digest: 'resp-head', count: '2' },
      revoke: { digest: 'revoke-head', count: '1' },
    });
  });

  it('uses canonical tx ordering when computing response checkpoints', async () => {
    const sqls: string[] = [];
    const query = vi.fn(async (sql: string) => {
      sqls.push(sql);

      if (sql.includes('FROM feedbacks') && sql.includes('feedback_index::text AS feedback_index')) {
        return { rows: [] };
      }
      if (sql.includes('FROM feedback_responses') && sql.includes('FROM orphan_responses')) {
        return { rows: [{ response_count: '1000', digest: 'checkpoint-digest', created_at: '1700000000' }] };
      }
      if (sql.includes('FROM revocations')) {
        return { rows: [] };
      }
      return { rows: [] };
    });

    const result = await hashChainResolvers.Query.hashChainLatestCheckpoints({}, { agent: AGENT }, { pool: { query } } as any);

    const responseSql = sqls.find((sql) => sql.includes('FROM feedback_responses') && sql.includes('FROM orphan_responses'));
    expect(responseSql).toContain('FROM orphan_responses');
    expect(responseSql).toContain('ORDER BY response_count DESC');
    expect(result.response).toEqual({
      eventCount: '1000',
      digest: 'checkpoint-digest',
      createdAt: '1700000000',
    });
  });

  it('uses canonical tx ordering when replaying response hash-chain data', async () => {
    const sqls: string[] = [];
    const query = vi.fn(async (sql: string) => {
      sqls.push(sql);
      return {
        rows: [{
          client: 'client-1',
          feedback_index: '0',
          responder: 'responder-1',
          response_hash: 'resp-hash',
          feedback_hash: 'feedback-hash',
          slot: '42',
          running_digest: 'running-digest',
          response_count: '0',
        }],
      };
    });

    const result = await hashChainResolvers.Query.hashChainReplayData(
      {},
      { agent: AGENT, chainType: 'RESPONSE', fromCount: '0', first: 2 },
      { pool: { query } } as any,
    );

    const responseSql = sqls[0];
    expect(responseSql).toContain('FROM orphan_responses');
    expect(responseSql).toContain('ORDER BY (response_count)::bigint ASC, block_slot ASC, tx_index ASC NULLS LAST, event_ordinal ASC NULLS LAST, tx_signature ASC NULLS LAST, row_id ASC');
    expect(result.events).toEqual([
      expect.objectContaining({
        asset: AGENT,
        client: 'client-1',
        feedbackIndex: '0',
        responder: 'responder-1',
        responseHash: 'resp-hash',
        responseCount: '0',
      }),
    ]);
    expect(result.nextFromCount).toBe('1');
    expect(result.hasMore).toBe(false);
  });

  it('does not overreport hasMore at an exact replay page boundary', async () => {
    const query = vi.fn(async () => ({
      rows: [
        {
          client: 'client-1',
          feedback_index: '0',
          responder: 'responder-1',
          response_hash: 'resp-hash-1',
          feedback_hash: 'feedback-hash',
          slot: '42',
          running_digest: 'running-digest-1',
          response_count: '0',
        },
        {
          client: 'client-1',
          feedback_index: '0',
          responder: 'responder-2',
          response_hash: 'resp-hash-2',
          feedback_hash: 'feedback-hash',
          slot: '43',
          running_digest: 'running-digest-2',
          response_count: '1',
        },
      ],
    }));

    const result = await hashChainResolvers.Query.hashChainReplayData(
      {},
      { agent: AGENT, chainType: 'RESPONSE', fromCount: '0', first: 2 },
      { pool: { query } } as any,
    );

    expect(result.events).toHaveLength(2);
    expect(result.nextFromCount).toBe('2');
    expect(result.hasMore).toBe(false);
  });

  it('starts revoke replay at count 0 when fromCount is 0', async () => {
    const sqls: string[] = [];
    const query = vi.fn(async (sql: string, params?: unknown[]) => {
      sqls.push(sql);
      expect(params?.[1]).toBe('0');
      expect(params?.[2]).toBe('4');
      return {
        rows: [
          {
            client: 'client-1',
            feedback_index: '0',
            feedback_hash: 'feedback-hash-1',
            slot: '42',
            running_digest: 'running-digest-1',
            revoke_count: '1',
          },
          {
            client: 'client-2',
            feedback_index: '1',
            feedback_hash: 'feedback-hash-2',
            slot: '43',
            running_digest: 'running-digest-2',
            revoke_count: '2',
          },
          {
            client: 'client-3',
            feedback_index: '2',
            feedback_hash: 'feedback-hash-3',
            slot: '44',
            running_digest: 'running-digest-3',
            revoke_count: '3',
          },
          {
            client: 'client-4',
            feedback_index: '3',
            feedback_hash: 'feedback-hash-4',
            slot: '45',
            running_digest: 'running-digest-4',
            revoke_count: '4',
          },
        ],
      };
    });

    const result = await hashChainResolvers.Query.hashChainReplayData(
      {},
      { agent: AGENT, chainType: 'REVOKE', fromCount: '0', first: 3 },
      { pool: { query } } as any,
    );

    expect(sqls[0]).toContain('FROM revocations');
    expect(result.events).toHaveLength(3);
    expect(result.events[0]).toEqual(expect.objectContaining({ revokeCount: '1' }));
    expect(result.events[2]).toEqual(expect.objectContaining({ revokeCount: '3' }));
    expect(result.nextFromCount).toBe('4');
    expect(result.hasMore).toBe(true);
  });
});
