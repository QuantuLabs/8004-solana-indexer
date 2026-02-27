import type { GraphQLContext } from '../context.js';
import type { ResponseRow } from '../dataloaders.js';

function toUnixTimestamp(dateStr: string | null): string | null {
  if (!dateStr) return null;
  const ms = new Date(dateStr).getTime();
  return isNaN(ms) ? null : String(Math.floor(ms / 1000));
}

function requireResponseId(parent: ResponseRow): string {
  if (!parent.response_id) {
    throw new Error(`Missing response_id for response row ${parent.id}`);
  }
  return parent.response_id;
}

export const responseResolvers = {
  FeedbackResponse: {
    id(parent: ResponseRow) {
      return requireResponseId(parent);
    },
    cursor(parent: ResponseRow) {
      // Opaque cursor used by Query.feedbackResponses(after: ...)
      return Buffer.from(
        JSON.stringify({ created_at: parent.created_at, id: requireResponseId(parent), row_id: parent.id }),
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
