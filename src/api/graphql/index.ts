import { createYoga } from 'graphql-yoga';
import { makeExecutableSchema } from '@graphql-tools/schema';
import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import type { Pool } from 'pg';
import type { PrismaClient } from '@prisma/client';

import { resolvers } from './resolvers/index.js';
import { createContext } from './context.js';
import { analyzeQuery } from './plugins/complexity.js';
import { analyzeDepth } from './plugins/depth-limit.js';
import { createBadUserInputError } from './utils/errors.js';
import { createChildLogger } from '../../logger.js';
import { config } from '../../config.js';
import type { DocumentNode } from 'graphql';

const logger = createChildLogger('graphql');

const __dirname = dirname(fileURLToPath(import.meta.url));

function loadTypeDefs(): string {
  return readFileSync(join(__dirname, 'schema.graphql'), 'utf-8');
}

export interface GraphQLHandlerOptions {
  pool: Pool;
  prisma?: PrismaClient | null;
}

export function createGraphQLHandler(options: GraphQLHandlerOptions) {
  const typeDefs = loadTypeDefs();
  const schema = makeExecutableSchema({ typeDefs, resolvers });

  const contextFactory = createContext({
    pool: options.pool,
    prisma: options.prisma,
  });

  const graphqlDebugEnabled = config.graphqlDebugEnabled;

  const yoga = createYoga({
    schema,
    context: contextFactory,
    graphiql: graphqlDebugEnabled,
    landingPage: false,
    maskedErrors: !graphqlDebugEnabled,
    logging: {
      debug: (...args: unknown[]) => logger.debug(args[0], 'GraphQL debug'),
      info: (...args: unknown[]) => logger.info(args[0], 'GraphQL info'),
      warn: (...args: unknown[]) => logger.warn(args[0], 'GraphQL warn'),
      error: (...args: unknown[]) => logger.error(args[0], 'GraphQL error'),
    },
    plugins: [
      {
        onParse() {
          return ({ result }: { result: DocumentNode | Error | null }) => {
            if (!result || result instanceof Error) return;

            const complexityResult = analyzeQuery(result);
            if (!complexityResult.allowed) {
              const reason = complexityResult.reason ?? 'Query complexity limit exceeded.';
              logger.warn({
                cost: complexityResult.cost,
                reason,
              }, 'Query rejected: complexity');
              throw createBadUserInputError(reason);
            }

            const depthResult = analyzeDepth(result);
            if (!depthResult.allowed) {
              const reason = depthResult.reason ?? 'Query depth limit exceeded.';
              logger.warn({
                depth: depthResult.depth,
                reason,
              }, 'Query rejected: depth');
              throw createBadUserInputError(reason);
            }

            logger.debug({
              cost: complexityResult.cost,
              depth: depthResult.depth,
            }, 'Query analyzed');
          };
        },
      },
    ],
  });

  logger.info({ graphiql: graphqlDebugEnabled, maskedErrors: !graphqlDebugEnabled }, 'GraphQL handler created');
  return yoga;
}
