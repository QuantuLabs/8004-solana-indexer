import type { GraphQLContext } from '../context.js';
import type { ValidationRow } from '../dataloaders.js';
import { encodeValidationId } from '../utils/ids.js';

function toUnixTimestamp(dateStr: string | null): string | null {
  if (!dateStr) return null;
  const ms = new Date(dateStr).getTime();
  return isNaN(ms) ? null : String(Math.floor(ms / 1000));
}

function deriveStatus(row: ValidationRow): string {
  if (row.response !== null) return 'COMPLETED';
  return 'PENDING';
}

export const validationResolvers = {
  Validation: {
    id(parent: ValidationRow) {
      return encodeValidationId(parent.asset, parent.validator, parent.nonce);
    },
    cursor(parent: ValidationRow) {
      // Opaque pagination cursor.
      return Buffer.from(
        JSON.stringify({ created_at: parent.created_at, id: parent.id }),
        'utf-8'
      ).toString('base64');
    },
    async agent(parent: ValidationRow, _args: unknown, ctx: GraphQLContext) {
      return ctx.loaders.agentById.load(parent.asset);
    },
    validatorAddress(parent: ValidationRow) {
      return parent.validator;
    },
    requestUri(parent: ValidationRow) {
      return parent.request_uri;
    },
    requestHash(parent: ValidationRow) {
      return parent.request_hash;
    },
    response(parent: ValidationRow) {
      return parent.response;
    },
    responseUri(parent: ValidationRow) {
      return parent.response_uri;
    },
    responseHash(parent: ValidationRow) {
      return parent.response_hash;
    },
    tag(parent: ValidationRow) {
      return parent.tag;
    },
    status(parent: ValidationRow) {
      return deriveStatus(parent);
    },
    createdAt(parent: ValidationRow) {
      return toUnixTimestamp(parent.created_at);
    },
    updatedAt(parent: ValidationRow) {
      return toUnixTimestamp(parent.responded_at);
    },
  },
};
