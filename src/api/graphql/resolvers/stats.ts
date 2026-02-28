import type { AgentStatsRow } from '../dataloaders.js';
import { encodeAgentId } from '../utils/ids.js';

function toUnixTimestamp(dateStr: string | null): string | null {
  if (!dateStr) return null;
  const ms = new Date(dateStr).getTime();
  return isNaN(ms) ? null : String(Math.floor(ms / 1000));
}

export const statsResolvers = {
  AgentStats: {
    id(parent: AgentStatsRow & { _asset?: string }) {
      return parent._asset ? encodeAgentId(parent._asset) : parent.asset;
    },
    totalFeedback(parent: AgentStatsRow) {
      return parent.feedback_count;
    },
    averageFeedbackValue(parent: AgentStatsRow) {
      return parent.avg_value;
    },
    lastActivity(parent: AgentStatsRow) {
      return toUnixTimestamp(parent.last_activity);
    },
  },

  Protocol: {
    id(parent: { id: string }) { return parent.id; },
    totalAgents(parent: { totalAgents: string }) { return parent.totalAgents; },
    totalFeedback(parent: { totalFeedback: string }) { return parent.totalFeedback; },
    tags(parent: { tags: string[] }) { return parent.tags; },
  },

  GlobalStats: {
    id(parent: { id: string }) { return parent.id; },
    totalAgents(parent: { totalAgents: string }) { return parent.totalAgents; },
    totalFeedback(parent: { totalFeedback: string }) { return parent.totalFeedback; },
    totalCollections(parent: { totalCollections: string }) { return parent.totalCollections; },
    tags(parent: { tags: string[] }) { return parent.tags; },
  },
};
