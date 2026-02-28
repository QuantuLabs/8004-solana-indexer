# syntax=docker/dockerfile:1.7

FROM node:20-bookworm-slim AS base
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends openssl ca-certificates \
  && rm -rf /var/lib/apt/lists/*
ENV NPM_CONFIG_UPDATE_NOTIFIER=false \
    NPM_CONFIG_FUND=false \
    PRISMA_SKIP_POSTINSTALL_GENERATE=true

FROM base AS deps
COPY package*.json ./
RUN npm ci

FROM deps AS builder
COPY prisma ./prisma
RUN npx prisma generate
COPY . .
RUN npm run build
RUN mkdir -p data && DATABASE_URL=file:./data/indexer.db npx prisma db push --skip-generate
RUN npm prune --omit=dev

FROM deps AS test
COPY prisma ./prisma
RUN npx prisma generate
COPY . .
CMD ["npx", "vitest", "run"]

FROM base AS runner
ARG INDEXER_VERSION=dev
ARG VCS_REF=unknown
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.title="8004 Solana Indexer" \
      org.opencontainers.image.description="Solana 8004 indexer with GraphQL/REST API" \
      org.opencontainers.image.version="${INDEXER_VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.created="${BUILD_DATE}"

WORKDIR /app
ENV NODE_ENV=production \
    API_PORT=3001

COPY --from=builder --chown=node:node /app/node_modules ./node_modules
COPY --from=builder --chown=node:node /app/dist ./dist
COPY --from=builder --chown=node:node /app/prisma ./prisma
COPY --from=builder --chown=node:node /app/idl ./idl
COPY --from=builder --chown=node:node /app/data ./data
COPY --from=builder --chown=node:node /app/package*.json ./

RUN mkdir -p /app/data && chown -R node:node /app/data
USER node

EXPOSE 3001
HEALTHCHECK --interval=30s --timeout=5s --start-period=45s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:' + (process.env.API_PORT || '3001') + '/health').then((r)=>{if(!r.ok)process.exit(1)}).catch(()=>process.exit(1))"

CMD ["node", "dist/index.js"]
