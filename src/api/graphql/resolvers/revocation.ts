import type { GraphQLContext } from '../context.js';
import type { RevocationRow } from '../dataloaders.js';

function toUnixTimestamp(dateStr: string | null): string | null {
  if (!dateStr) return null;
  const ms = new Date(dateStr).getTime();
  return Number.isNaN(ms) ? null : String(Math.floor(ms / 1000));
}

function requireCanonicalRevocationId(parent: RevocationRow): string {
  if (!parent.asset || !parent.client_address || parent.feedback_index === null || parent.feedback_index === undefined) {
    throw new Error(`Missing canonical revocation id fields for revocation row ${parent.id}`);
  }
  return `${parent.asset}:${parent.client_address}:${String(parent.feedback_index)}`;
}

function requireRevocationCursorId(parent: RevocationRow): string {
  if (!parent.revocation_id) {
    throw new Error(`Missing revocation_id for revocation row ${parent.id}`);
  }
  return parent.revocation_id;
}

export const revocationResolvers = {
  Revocation: {
    id(parent: RevocationRow) {
      return requireCanonicalRevocationId(parent);
    },
    cursor(parent: RevocationRow) {
      return Buffer.from(
        JSON.stringify({ created_at: parent.created_at, id: requireRevocationCursorId(parent), row_id: parent.id }),
        'utf-8'
      ).toString('base64');
    },
    async agent(parent: RevocationRow, _args: unknown, ctx: GraphQLContext) {
      return ctx.loaders.agentById.load(parent.asset);
    },
    clientAddress(parent: RevocationRow) {
      return parent.client_address;
    },
    feedbackIndex(parent: RevocationRow) {
      return parent.feedback_index;
    },
    feedbackHash(parent: RevocationRow) {
      return parent.feedback_hash;
    },
    originalScore(parent: RevocationRow) {
      return parent.original_score;
    },
    atomEnabled(parent: RevocationRow) {
      return parent.atom_enabled;
    },
    hadImpact(parent: RevocationRow) {
      return parent.had_impact;
    },
    createdAt(parent: RevocationRow) {
      return toUnixTimestamp(parent.created_at);
    },
    solana(parent: RevocationRow) {
      return parent;
    },
  },
};
