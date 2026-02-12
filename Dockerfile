# Stage 1: Build
FROM node:20-alpine AS builder

# Build tools for @mongodb-js/zstd native module + git for GitHub deps
RUN apk add --no-cache python3 make g++ git

WORKDIR /app

COPY package.json package-lock.json ./
COPY prisma ./prisma/

RUN npm ci
RUN npx prisma generate

COPY tsconfig.json ./
COPY src ./src/

RUN npm run build

# Remove devDependencies (prisma CLI goes, but @prisma/client + .prisma/client stay)
RUN npm prune --production

# Stage 2: Runtime
FROM node:20-alpine

RUN apk add --no-cache tini

WORKDIR /app

COPY --from=builder /app/dist ./dist/
COPY --from=builder /app/node_modules ./node_modules/
COPY --from=builder /app/package.json ./
COPY --from=builder /app/prisma ./prisma/
COPY idl ./idl/

# Use tini for proper signal handling (SIGTERM for graceful shutdown)
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/index.js"]
