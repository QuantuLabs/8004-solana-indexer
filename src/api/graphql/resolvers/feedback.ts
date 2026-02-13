import type { GraphQLContext } from '../context.js';
import type { FeedbackRow } from '../dataloaders.js';
import { encodeFeedbackId } from '../utils/ids.js';
import { clampFirst, clampSkip } from '../utils/pagination.js';

function detectUriType(uri: string | null): string | null {
  if (!uri) return null;
  if (uri.startsWith('ipfs://') || uri.startsWith('Qm') || uri.startsWith('bafy')) return 'IPFS';
  if (uri.startsWith('ar://')) return 'ARWEAVE';
  return 'HTTP';
}

function toUnixTimestamp(dateStr: string | null): string | null {
  if (!dateStr) return null;
  const ms = new Date(dateStr).getTime();
  return isNaN(ms) ? null : String(Math.floor(ms / 1000));
}

function normalizeValue(raw: string, decimals: number): string {
  if (decimals === 0) return raw;
  const str = raw.replace('-', '');
  const isNeg = raw.startsWith('-');
  const padded = str.padStart(decimals + 1, '0');
  const intPart = padded.slice(0, -decimals);
  const fracPart = padded.slice(-decimals).replace(/0+$/, '');
  const result = fracPart ? `${intPart}.${fracPart}` : intPart;
  return isNeg ? `-${result}` : result;
}

export const feedbackResolvers = {
  Feedback: {
    id(parent: FeedbackRow) {
      return encodeFeedbackId(parent.asset, parent.client_address, parent.feedback_index);
    },
    cursor(parent: FeedbackRow) {
      // Opaque cursor used by Query.feedbacks(after: ...)
      return Buffer.from(
        JSON.stringify({ created_at: parent.created_at, asset: parent.asset, id: parent.id }),
        'utf-8'
      ).toString('base64');
    },
    async agent(parent: FeedbackRow, _args: unknown, ctx: GraphQLContext) {
      return ctx.loaders.agentById.load(parent.asset);
    },
    clientAddress(parent: FeedbackRow) {
      return parent.client_address;
    },
    feedbackIndex(parent: FeedbackRow) {
      return parent.feedback_index;
    },
    value(parent: FeedbackRow) {
      return normalizeValue(parent.value, parent.value_decimals);
    },
    tag1(parent: FeedbackRow) {
      return parent.tag1;
    },
    tag2(parent: FeedbackRow) {
      return parent.tag2;
    },
    endpoint(parent: FeedbackRow) {
      return parent.endpoint || null;
    },
    feedbackURI(parent: FeedbackRow) {
      return parent.feedback_uri || null;
    },
    feedbackURIType(parent: FeedbackRow) {
      return detectUriType(parent.feedback_uri);
    },
    feedbackHash(parent: FeedbackRow) {
      return parent.feedback_hash;
    },
    isRevoked(parent: FeedbackRow) {
      return parent.is_revoked;
    },
    createdAt(parent: FeedbackRow) {
      return toUnixTimestamp(parent.created_at);
    },
    revokedAt(parent: FeedbackRow) {
      return toUnixTimestamp(parent.revoked_at);
    },
    async responses(
      parent: FeedbackRow,
      args: { first?: number; skip?: number },
      ctx: GraphQLContext
    ) {
      const first = clampFirst(args.first);
      const skip = clampSkip(args.skip);
      return ctx.loaders.responsesPageByFeedback.load({
        asset: parent.asset,
        clientAddress: parent.client_address,
        feedbackIndex: parent.feedback_index,
        first,
        skip,
      });
    },
    solana(parent: FeedbackRow) {
      return parent;
    },
  },
};
