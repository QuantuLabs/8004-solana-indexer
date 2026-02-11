# 8004 Solana Indexer

GraphQL-only Solana indexer for the 8004 Agent Registry.

## Features

- WebSocket + polling ingestion modes
- Reorg resilience with verification worker
- GraphQL API (`/graphql`) with query depth/complexity guards
- Supabase/PostgreSQL data backend support

## Quick Start

```bash
npm install
cp .env.example .env
npm run db:generate
npm run db:push
npm run dev
```

GraphQL endpoint:

```text
http://localhost:3001/graphql
```

## Required Environment

```bash
DB_MODE=supabase
SUPABASE_DSN=postgresql://...
SUPABASE_URL=https://<project>.supabase.co
SUPABASE_KEY=<service_role_key>
RPC_URL=https://api.devnet.solana.com
WS_URL=wss://api.devnet.solana.com
```

Notes:

- API is GraphQL-only.
- GraphQL API requires `DB_MODE=supabase`.

## Commands

```bash
npm run dev
npm run build
npm test
npm run test:e2e
npm run check:graphql:coherence
npm run bench:graphql:sql
```

## GraphQL Example

```graphql
query Dashboard {
  stats {
    totalAgents
    totalFeedback
    totalValidations
  }
  agents(first: 10, orderBy: createdAt, orderDirection: desc) {
    id
    asset
    owner
    feedbackCount
    trustScore
  }
}
```

## E2E Scope

`npm run test:e2e` runs the maintained major end-to-end suites:

- `tests/e2e/reorg-resilience.test.ts`
- `tests/e2e/devnet-verification.test.ts`

## Project Structure

```text
src/
├── api/        # GraphQL server + resolvers
├── db/         # Database handlers
├── indexer/    # Poller, websocket, processor, verifier
├── parser/     # Program event decoder
├── config.ts   # Runtime config
└── index.ts    # Entry point
```
