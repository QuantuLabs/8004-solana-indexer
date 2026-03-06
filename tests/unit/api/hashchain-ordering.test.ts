import { describe, expect, it, vi } from 'vitest';
import { hashChainResolvers } from '../../../src/api/graphql/resolvers/hashchain.js';

const AGENT = '11111111111111111111111111111111';

describe('HashChain resolver ordering', () => {
  it('uses canonical tx ordering for feedback and response heads', async () => {
    const sqls: string[] = [];
    const query = vi.fn(async (sql: string) => {
      sqls.push(sql);

      if (sql.includes('SELECT COUNT(*)::text AS count FROM feedbacks')) {
        return { rows: [{ count: '2' }] };
      }
      if (sql.includes('SELECT COUNT(*)::text AS count FROM feedback_responses')) {
        return { rows: [{ count: '3' }] };
      }
      if (sql.includes('SELECT COUNT(*)::text AS count FROM revocations')) {
        return { rows: [{ count: '1' }] };
      }
      if (sql.includes('FROM feedbacks') && sql.includes('running_digest')) {
        return { rows: [{ digest: 'feed-head' }] };
      }
      if (sql.includes('FROM feedback_responses') && sql.includes('running_digest')) {
        return { rows: [{ digest: 'resp-head' }] };
      }
      if (sql.includes('FROM revocations') && sql.includes('running_digest')) {
        return { rows: [{ digest: 'revoke-head' }] };
      }
      return { rows: [] };
    });

    const result = await hashChainResolvers.Query.hashChainHeads({}, { agent: AGENT }, { pool: { query } } as any);

    const feedbackSql = sqls.find((sql) => sql.includes('FROM feedbacks') && sql.includes('running_digest'));
    const responseSql = sqls.find((sql) => sql.includes('FROM feedback_responses') && sql.includes('running_digest'));

    expect(feedbackSql).toContain('ORDER BY block_slot DESC NULLS LAST, tx_index DESC NULLS LAST, event_ordinal DESC NULLS LAST, tx_signature DESC NULLS LAST, id DESC');
    expect(feedbackSql).not.toContain('tx_signature DESC NULLS LAST, tx_index DESC NULLS LAST');
    expect(responseSql).toContain('ORDER BY block_slot DESC NULLS LAST, tx_index DESC NULLS LAST, event_ordinal DESC NULLS LAST, tx_signature DESC NULLS LAST, id DESC');
    expect(responseSql).not.toContain('tx_signature DESC NULLS LAST, tx_index DESC NULLS LAST');
    expect(result).toEqual({
      feedback: { digest: 'feed-head', count: '2' },
      response: { digest: 'resp-head', count: '3' },
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
      if (sql.includes('WITH ordered AS (') && sql.includes('FROM feedback_responses')) {
        return { rows: [{ response_count: '999', digest: 'checkpoint-digest', created_at: '1700000000' }] };
      }
      if (sql.includes('FROM revocations')) {
        return { rows: [] };
      }
      return { rows: [] };
    });

    const result = await hashChainResolvers.Query.hashChainLatestCheckpoints({}, { agent: AGENT }, { pool: { query } } as any);

    const responseSql = sqls.find((sql) => sql.includes('WITH ordered AS (') && sql.includes('FROM feedback_responses'));
    expect(responseSql).toContain('ORDER BY block_slot ASC, tx_index ASC NULLS LAST, event_ordinal ASC NULLS LAST, tx_signature ASC NULLS LAST, id ASC');
    expect(responseSql).not.toContain('ORDER BY block_slot ASC, tx_signature ASC, tx_index ASC NULLS LAST, event_ordinal ASC NULLS LAST, id ASC');
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
    expect(responseSql).toContain('ORDER BY fr.block_slot ASC, fr.tx_index ASC NULLS LAST, fr.event_ordinal ASC NULLS LAST, fr.tx_signature ASC NULLS LAST, fr.id ASC');
    expect(responseSql).not.toContain('ORDER BY fr.block_slot ASC, fr.tx_signature ASC, fr.tx_index ASC NULLS LAST, fr.event_ordinal ASC NULLS LAST, fr.id ASC');
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
});
