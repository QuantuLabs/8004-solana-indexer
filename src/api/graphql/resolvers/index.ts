import { scalarResolvers } from './scalars.js';
import { queryResolvers } from './query.js';
import { agentResolvers } from './agent.js';
import { feedbackResolvers } from './feedback.js';
import { responseResolvers } from './response.js';
import { revocationResolvers } from './revocation.js';
import { validationResolvers } from './validation.js';
import { metadataResolvers } from './metadata.js';
import { statsResolvers } from './stats.js';
import { registrationResolvers } from './registration.js';
import { solanaResolvers } from './solana.js';
import { hashChainResolvers } from './hashchain.js';

export const resolvers = {
  ...scalarResolvers,

  // Multiple modules contribute to Query; merge them instead of overwriting.
  Query: {
    ...queryResolvers.Query,
    ...hashChainResolvers.Query,
  },

  ...agentResolvers,
  ...feedbackResolvers,
  ...responseResolvers,
  ...revocationResolvers,
  ...validationResolvers,
  ...metadataResolvers,
  ...statsResolvers,
  ...registrationResolvers,
  ...solanaResolvers,
};
