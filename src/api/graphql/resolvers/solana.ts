import type { AgentRow, FeedbackRow, ResponseRow, RevocationRow } from '../dataloaders.js';

export const solanaResolvers = {
  SolanaAgentExtension: {
    assetPubkey(parent: AgentRow) { return parent.asset; },
    collection(parent: AgentRow) { return parent.collection; },
    atomEnabled(parent: AgentRow) { return parent.atom_enabled; },
    trustTier(parent: AgentRow) { return parent.trust_tier; },
    qualityScore(parent: AgentRow) { return parent.quality_score; },
    confidence(parent: AgentRow) { return parent.confidence; },
    riskScore(parent: AgentRow) { return parent.risk_score; },
    diversityRatio(parent: AgentRow) { return parent.diversity_ratio; },
    verificationStatus(parent: AgentRow) { return parent.status; },
    feedbackDigest(parent: AgentRow) { return parent.feedback_digest; },
    responseDigest(parent: AgentRow) { return parent.response_digest; },
    revokeDigest(parent: AgentRow) { return parent.revoke_digest; },
  },

  SolanaFeedbackExtension: {
    valueRaw(parent: FeedbackRow) { return parent.value; },
    valueDecimals(parent: FeedbackRow) { return parent.value_decimals; },
    score(parent: FeedbackRow) { return parent.score; },
    runningDigest(parent: FeedbackRow) { return parent.running_digest; },
    verificationStatus(parent: FeedbackRow) { return parent.status; },
    txSignature(parent: FeedbackRow) { return parent.tx_signature; },
    blockSlot(parent: FeedbackRow) { return parent.block_slot; },
  },

  SolanaResponseExtension: {
    runningDigest(parent: ResponseRow) { return parent.running_digest; },
    responseCount(parent: ResponseRow) { return parent.response_count; },
    verificationStatus(parent: ResponseRow) { return parent.status; },
    txSignature(parent: ResponseRow) { return parent.tx_signature; },
    blockSlot(parent: ResponseRow) { return parent.block_slot; },
  },

  SolanaRevocationExtension: {
    runningDigest(parent: RevocationRow) { return parent.running_digest; },
    revokeCount(parent: RevocationRow) { return parent.revoke_count; },
    verificationStatus(parent: RevocationRow) { return parent.status; },
    txSignature(parent: RevocationRow) { return parent.tx_signature; },
    blockSlot(parent: RevocationRow) { return parent.slot; },
  },
};
