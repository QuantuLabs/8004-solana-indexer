import { scalarResolvers } from './scalars.js';
import { queryResolvers } from './query.js';
import { agentResolvers } from './agent.js';
import { feedbackResolvers } from './feedback.js';
import { responseResolvers } from './response.js';
import { validationResolvers } from './validation.js';
import { metadataResolvers } from './metadata.js';
import { statsResolvers } from './stats.js';
import { registrationResolvers } from './registration.js';
import { solanaResolvers } from './solana.js';
import { hashChainResolvers } from './hashchain.js';

export const resolvers = {
  ...scalarResolvers,
  ...queryResolvers,
  ...agentResolvers,
  ...feedbackResolvers,
  ...responseResolvers,
  ...validationResolvers,
  ...metadataResolvers,
  ...statsResolvers,
  ...registrationResolvers,
  ...solanaResolvers,
  ...hashChainResolvers,
};
