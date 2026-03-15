import type { GraphQLContext } from '../context.js';
import type { ResponseRow } from '../dataloaders.js';

function toUnixTimestamp(dateStr: string | null): string | null {
  if (!dateStr) return null;
  const ms = new Date(dateStr).getTime();
  return isNaN(ms) ? null : String(Math.floor(ms / 1000));
}

function requireCanonicalResponseId(parent: ResponseRow): string {
  if (
    !parent.asset
    || !parent.client_address
    || parent.feedback_index === null
    || parent.feedback_index === undefined
    || !parent.responder
  ) {
    throw new Error(`Missing canonical response id fields for response row ${parent.id}`);
  }
  const discriminator = parent.tx_signature ?? (parent.response_count != null ? String(parent.response_count) : null);
  if (!discriminator) {
    throw new Error(`Missing canonical response discriminator for response row ${parent.id}`);
  }
  return `${parent.asset}:${parent.client_address}:${String(parent.feedback_index)}:${parent.responder}:${discriminator}`;
}

function requireResponseCursorId(parent: ResponseRow): string {
  if (!parent.response_id) {
    throw new Error(`Missing response_id for response row ${parent.id}`);
  }
  return parent.response_id;
}

export const responseResolvers = {
  FeedbackResponse: {
    id(parent: ResponseRow) {
      return requireCanonicalResponseId(parent);
    },
    cursor(parent: ResponseRow) {
      // Opaque cursor used by Query.feedbackResponses(after: ...)
      return Buffer.from(
        JSON.stringify({ created_at: parent.created_at, id: requireResponseCursorId(parent), row_id: parent.id }),
        'utf-8'
      ).toString('base64');
    },
    async feedback(parent: ResponseRow, _args: unknown, ctx: GraphQLContext) {
      return ctx.loaders.feedbackByLookup.load(`${parent.asset}:${parent.client_address}:${parent.feedback_index}`);
    },
    responder(parent: ResponseRow) {
      return parent.responder;
    },
    responseUri(parent: ResponseRow) {
      return parent.response_uri;
    },
    responseHash(parent: ResponseRow) {
      return parent.response_hash;
    },
    createdAt(parent: ResponseRow) {
      return toUnixTimestamp(parent.created_at);
    },
    solana(parent: ResponseRow) {
      return parent;
    },
  },
};
