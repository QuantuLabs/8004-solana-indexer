import type { GraphQLContext } from '../context.js';
import type { AgentRow } from '../dataloaders.js';
import { encodeAgentId, numericAgentId } from '../utils/ids.js';
import { clampFirst, clampSkip, encodeCursor } from '../utils/pagination.js';

const FEEDBACK_ORDER_MAP: Record<string, 'created_at' | 'value' | 'feedback_index'> = {
  createdAt: 'created_at',
  value: 'value',
  feedbackIndex: 'feedback_index',
};

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

function resolveChainId(ctx: GraphQLContext): number {
  return ctx.networkMode === 'mainnet' ? 101 : 103;
}

export const agentResolvers = {
  Agent: {
    id(parent: AgentRow) {
      return encodeAgentId(parent.asset);
    },
    cursor(parent: AgentRow) {
      // Opaque cursor used by Query.agents(after: ...)
      return encodeCursor({ created_at: parent.created_at, asset: parent.asset });
    },
    chainId(_parent: AgentRow, _args: unknown, ctx: GraphQLContext) {
      return resolveChainId(ctx);
    },
    agentId(parent: AgentRow) {
      return numericAgentId(parent.asset).toString();
    },
    agentid(parent: AgentRow) {
      return parent.agent_id !== null ? String(parent.agent_id) : null;
    },
    agentidFormatted(parent: AgentRow) {
      if (!parent.agent_id) return null;
      const id = parseInt(parent.agent_id, 10);
      if (isNaN(id)) return null;
      const pad = id < 1000 ? 3 : id < 10000 ? 4 : id < 100000 ? 5 : 6;
      return '#' + String(id).padStart(pad, '0');
    },
    agentURI(parent: AgentRow) {
      return parent.agent_uri;
    },
    agentURIType(parent: AgentRow) {
      return detectUriType(parent.agent_uri);
    },
    owner(parent: AgentRow) {
      return parent.owner;
    },
    creator(parent: AgentRow) {
      return parent.creator;
    },
    agentWallet(parent: AgentRow) {
      return parent.agent_wallet;
    },
    collectionPointer(parent: AgentRow) {
      return parent.collection_pointer;
    },
    colLocked(parent: AgentRow) {
      return parent.col_locked;
    },
    parentAsset(parent: AgentRow) {
      return parent.parent_asset;
    },
    parentCreator(parent: AgentRow) {
      return parent.parent_creator;
    },
    parentLocked(parent: AgentRow) {
      return parent.parent_locked;
    },
    operators() {
      return [];
    },
    async totalFeedback(parent: AgentRow, _args: unknown, ctx: GraphQLContext) {
      const count = await ctx.loaders.feedbackCountByAgent.load(parent.asset);
      return String(count);
    },
    async lastActivity(parent: AgentRow, _args: unknown, ctx: GraphQLContext) {
      const lastAct = await ctx.loaders.lastActivityByAgent.load(parent.asset);
      return toUnixTimestamp(lastAct);
    },
    createdAt(parent: AgentRow) {
      return toUnixTimestamp(parent.created_at);
    },
    updatedAt(parent: AgentRow) {
      return toUnixTimestamp(parent.updated_at);
    },
    async registrationFile(parent: AgentRow, _args: unknown, _ctx: GraphQLContext) {
      return { _asset: parent.asset };
    },
    async feedback(
      parent: AgentRow,
      args: { first?: number; skip?: number; orderBy?: string; orderDirection?: string },
      ctx: GraphQLContext
    ) {
      const first = clampFirst(args.first);
      const skip = clampSkip(args.skip);
      const orderBy = FEEDBACK_ORDER_MAP[args.orderBy ?? 'createdAt'] ?? 'created_at';
      const orderDirection = args.orderDirection === 'asc' ? 'ASC' : 'DESC';
      return ctx.loaders.feedbackPageByAgent.load({
        asset: parent.asset,
        first,
        skip,
        orderBy,
        orderDirection,
      });
    },
    async validations(
      parent: AgentRow,
      args: { first?: number; skip?: number },
      ctx: GraphQLContext
    ) {
      const first = clampFirst(args.first);
      const skip = clampSkip(args.skip);
      return ctx.loaders.validationsPageByAgent.load({
        asset: parent.asset,
        first,
        skip,
      });
    },
    async metadata(parent: AgentRow, _args: unknown, ctx: GraphQLContext) {
      return ctx.loaders.metadataByAgent.load(parent.asset);
    },
    async stats(parent: AgentRow, _args: unknown, ctx: GraphQLContext) {
      const stats = await ctx.loaders.agentStatsByAgent.load(parent.asset);
      if (!stats) return null;
      return { ...stats, _asset: parent.asset };
    },
    solana(parent: AgentRow) {
      return parent;
    },
  },
};
