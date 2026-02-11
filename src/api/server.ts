/**
 * GraphQL-only API server.
 * Exposes:
 * - GET /health
 * - POST/GET /graphql
 */

import express, { Express, Request, Response } from 'express';
import cors from 'cors';
import rateLimit from 'express-rate-limit';
import { Server } from 'http';
import type { Pool } from 'pg';
import type { PrismaClient } from '@prisma/client';

import { logger } from '../logger.js';
import { createGraphQLHandler } from './graphql/index.js';

const RATE_LIMIT_WINDOW_MS = 60 * 1000;
const RATE_LIMIT_MAX_REQUESTS = parseInt(
  process.env.RATE_LIMIT_MAX_REQUESTS || '100',
  10
);
const GRAPHQL_RATE_LIMIT_WINDOW_MS = 60 * 1000;
const GRAPHQL_RATE_LIMIT_MAX_REQUESTS = parseInt(
  process.env.GRAPHQL_RATE_LIMIT_MAX_REQUESTS || '30',
  10
);

export interface ApiServerOptions {
  pool: Pool;
  prisma?: PrismaClient | null;
  port?: number;
}

export function createApiServer(options: ApiServerOptions): Express {
  if (!options.pool) {
    throw new Error('GraphQL API requires Supabase PostgreSQL pool (DB_MODE=supabase)');
  }

  const app = express();

  const trustProxyRaw = process.env.TRUST_PROXY;
  let trustProxy: string | number | boolean = 1;
  if (trustProxyRaw !== undefined) {
    if (trustProxyRaw === 'true') trustProxy = true;
    else if (trustProxyRaw === 'false') trustProxy = false;
    else if (/^\d+$/.test(trustProxyRaw)) trustProxy = Number(trustProxyRaw);
    else trustProxy = trustProxyRaw;
  }
  app.set('trust proxy', trustProxy);

  app.use(express.json({ limit: '100kb' }));

  const allowedOrigins = process.env.CORS_ORIGINS?.split(',').map((s) => s.trim()) || ['*'];
  if (allowedOrigins.includes('*')) {
    logger.warn('CORS_ORIGINS not set, defaulting to wildcard (*). Set CORS_ORIGINS env var for production.');
  }
  app.use(cors({
    origin: allowedOrigins.includes('*') ? '*' : allowedOrigins,
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'Prefer'],
    maxAge: 86400,
  }));

  app.use((_req: Request, res: Response, next: Function) => {
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('X-Frame-Options', 'DENY');
    res.setHeader('X-XSS-Protection', '0');
    res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
    next();
  });

  app.get('/health', (_req: Request, res: Response) => {
    res.json({ status: 'ok' });
  });

  const globalLimiter = rateLimit({
    windowMs: RATE_LIMIT_WINDOW_MS,
    max: RATE_LIMIT_MAX_REQUESTS,
    message: { error: 'Too many requests, please try again later' },
    standardHeaders: true,
    legacyHeaders: false,
  });
  app.use(globalLimiter);

  const graphqlLimiter = rateLimit({
    windowMs: GRAPHQL_RATE_LIMIT_WINDOW_MS,
    max: GRAPHQL_RATE_LIMIT_MAX_REQUESTS,
    message: { error: 'GraphQL rate limited. Please try again later.' },
    standardHeaders: true,
    legacyHeaders: false,
  });

  const yoga = createGraphQLHandler({
    pool: options.pool,
    prisma: options.prisma ?? null,
  });
  app.use('/graphql', graphqlLimiter, yoga.handle as any);

  logger.info('GraphQL endpoint mounted at /graphql');
  return app;
}

export async function startApiServer(options: ApiServerOptions): Promise<Server> {
  const { port = 3001 } = options;
  const app = createApiServer(options);

  return new Promise((resolve) => {
    const server = app.listen(port, () => {
      logger.info({ port }, 'GraphQL API server started');
      resolve(server);
    });
  });
}
