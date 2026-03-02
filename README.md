# 8004 Solana Indexer

Solana indexer for the 8004 Agent Registry with GraphQL v2 and transitional REST v1.

## Features

- WebSocket + polling ingestion modes
- Reorg resilience with verification worker
- GraphQL API (`/v2/graphql`) with query depth/complexity guards
- Legacy REST v1 (`/rest/v1/*`) available when `API_MODE=rest|both`
- Supabase/PostgreSQL data backend support
- Strict events-only integrity policy:
  - revoke/response without parent feedback => `ORPHANED`
  - revocation `seal_hash` mismatch with parent feedback => logged/flagged (not `ORPHANED`)
  - response `seal_hash` mismatch => `ORPHANED`
  - GraphQL filters exclude `ORPHANED` records by default

## Quick Start

```bash
npm install
cp .env.devnet.example .env
npm run db:generate
npm run db:push
npm run dev
```

Mainnet bootstrap:

```bash
cp .env.mainnet.example .env
```

GraphQL v2 endpoint:

```text
http://localhost:3001/v2/graphql
```

## WARNING: Destructive Schema Init Only

`supabase/schema.sql` is destructive and intended for fresh initialization only.

- `scripts/init-supabase.js` drops and recreates indexer tables when applied to an existing database.
- Upgrades must use `supabase/migrations/*.sql` (in order), not `schema.sql`.
- When existing indexer tables are detected, init requires explicit `RESET` confirmation in an interactive TTY, or `FORCE_SCHEMA_RESET=1` for non-interactive forced resets.

## WARNING: Historical Sync vs WebSocket

WebSocket subscriptions are live notifications, not historical replay.

- For complete bootstrap/sync, use HTTP RPC history first (`getSignaturesForAddress` + transaction fetch).
- Then run live tail with `INDEXER_MODE=websocket` or `INDEXER_MODE=auto`.
- If your provider/network keeps limited history (often devnet/testnet), use archival RPC for bootstrap, or set `INDEXER_START_SIGNATURE` + `INDEXER_START_SLOT`.

## Remote API Docs

Canonical API docs are maintained in:

- https://github.com/QuantuLabs/8004-solana-api
- https://github.com/QuantuLabs/8004-solana-api/blob/main/docs/rest-v1.md
- https://github.com/QuantuLabs/8004-solana-api/blob/main/docs/collections.md
- https://github.com/QuantuLabs/8004-solana-api/tree/main/docs/examples

## Agent ID Model

Canonical identity is asset-based:

- GraphQL `id`: raw agent asset pubkey (base58).
- REST `asset`: same value as GraphQL `id`.
- GraphQL `agentId`: sequential registration ID from DB `agent_id` (auto-assigned), serialized as GraphQL `BigInt` string for precision safety.

Public API note:

- Public field name is `agentId`; internal DB column names are implementation details and not part of the API contract.

## Required Environment

```bash
DB_MODE=supabase
SUPABASE_DSN=POSTGRES_DSN_REDACTED
SUPABASE_URL=https://<project>.supabase.co
SUPABASE_KEY=<service_role_key>
SOLANA_NETWORK=devnet
RPC_URL=https://api.devnet.solana.com
WS_URL=wss://api.devnet.solana.com
PROGRAM_ID=8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C
ATOM_ENGINE_PROGRAM_ID=AToMufS4QD6hEXvcvBDg9m1AHeCLpmZQsyfYa5h9MwAF
```

```bash
# Mainnet config example
# SOLANA_NETWORK=mainnet-beta
# RPC_URL=https://api.mainnet-beta.solana.com
# WS_URL=wss://api.mainnet-beta.solana.com
# PROGRAM_ID=8oo4dC4JvBLwy5tGgiH3WwK4B9PWxL9Z4XjA2jzkQMbQ
# ATOM_ENGINE_PROGRAM_ID=AToMw53aiPQ8j7iHVb4fGt6nzUNxUhcPc3tbPBZuzVVb
```

Notes:

- GraphQL requires `DB_MODE=supabase` (recommended `API_MODE=graphql`).
- REST v1 works with:
  - `DB_MODE=local` (Prisma), or
  - `DB_MODE=supabase` with PostgREST proxy auth (`SUPABASE_URL` + `SUPABASE_KEY`, or `POSTGREST_URL` + `POSTGREST_TOKEN`).
- Runtime default is `API_MODE=both` when unset; `.env.example` pins `API_MODE=graphql` as the recommended production baseline.
- `.env.devnet.example` includes current devnet bootstrap cursor (`INDEXER_START_SIGNATURE` + `INDEXER_START_SLOT`).
- `.env.mainnet.example` is prefilled with current mainnet `PROGRAM_ID` and `ATOM_ENGINE_PROGRAM_ID`; set startup signature/slot for first sync.
- IDLs are stored side-by-side in `idl/`: `agent_registry_8004.json` (devnet/default runtime) and `agent_registry_8004.mainnet.json` (mainnet reference copy).
- `API_MODE=both` is best-effort dual mode and disables whichever side has no matching DB backend.
- `SOLANA_NETWORK` drives default RPC/WS endpoints when `RPC_URL`/`WS_URL` are unset.
- `MAX_SUPPORTED_TRANSACTION_VERSION` controls parsed transaction version support for RPC fetches (default `0`).
- Optional startup cursor bootstrap: `INDEXER_START_SIGNATURE` (and optional `INDEXER_START_SLOT`) is applied only when no persisted `indexer_state` exists.
- `INDEXER_START_SLOT` requires `INDEXER_START_SIGNATURE`.
- If `SOLANA_NETWORK=mainnet-beta`, ensure `PROGRAM_ID` and `ATOM_ENGINE_PROGRAM_ID` match your deployed mainnet programs. Startup validation warns if mainnet is selected but `PROGRAM_ID` is still the default devnet ID.
- `.env.localnet` is preconfigured for local REST mode.
- `GRAPHQL_STATS_CACHE_TTL_MS` controls `globalStats`/`protocol` aggregate cache TTL (default `60000` ms).
- `IPFS_GATEWAY_BASE` sets the gateway base used for `ipfs://` URI digest fetches and canonical collection pointer (`c1:<cid>`) fetches (default `https://ipfs.io`).
- `URI_DIGEST_TRUSTED_HOSTS` is optional and only accepts `localhost`/`127.0.0.1` to allow local IPFS gateway tests without disabling SSRF protections globally.
- Validation module is archived on-chain in v0.5.0+; validation indexing is opt-in via `INDEX_VALIDATIONS=true` (default `false` outside tests).
- Validation entity endpoints are removed from the public API surface: REST exposes no `/rest/v1/validations` route and GraphQL exposes no `validation`/`validations` query fields.

## Commands

```bash
npm run dev
npm run build
npm test
npm run test:e2e
npm run localnet:start
npm run localnet:init
npm run test:localnet:only
npm run test:localnet
npm run test:metadata:digestion
npm run localnet:stop
npm run test:docker:ci
npm run check:graphql:coherence
npm run bench:graphql:sql
npm run bench:hashchain
```

## Docker

Build local image:

```bash
docker build \
  --build-arg INDEXER_VERSION=$(node -p "require('./package.json').version") \
  -t 8004-indexer:local .
```

Run local smoke container:

```bash
docker run --rm -p 3001:3001 --env-file .env 8004-indexer:local
```

CI test target in Docker:

```bash
npm run test:docker:ci
```

Runtime stack (digest-pin friendly):

```bash
docker compose -f docker/stack/indexer-stack.yml up -d
```

Optional PostgREST sidecar (local PostgreSQL REST, default stack unchanged):

```bash
cp docker/stack/postgrest.env.example .env.postgrest
# edit .env.postgrest with your local PostgreSQL DSN/role/secret

# generate a local POSTGREST_TOKEN (HS256 JWT with role=web_anon by default)
node -e 'const c=require("crypto");const enc=(o)=>Buffer.from(JSON.stringify(o)).toString("base64url");const secret=process.env.SECRET||"replace-with-random-secret";const payload={role:"web_anon",iat:Math.floor(Date.now()/1000),exp:Math.floor(Date.now()/1000)+315360000};const token=enc({alg:"HS256",typ:"JWT"})+"."+enc(payload);const sig=c.createHmac("sha256",secret).update(token).digest("base64url");console.log(token+"."+sig)'

docker compose \
  --env-file .env \
  --env-file .env.postgrest \
  -f docker/stack/indexer-stack.yml \
  -f docker/stack/indexer-stack.postgrest.yml \
  --profile postgrest up -d
```

PostgREST endpoint:

```text
http://localhost:3002
```

Indexer endpoint remains unchanged:

```text
http://localhost:3001/rest/v1/*
```

Minimum PostgreSQL grants for `PGRST_DB_ANON_ROLE` (example role `web_anon`):

```sql
create role web_anon nologin;
grant usage on schema public to web_anon;
grant select on all tables in schema public to web_anon;
alter default privileges in schema public grant select on tables to web_anon;
```

Integrity helpers:

```bash
# Official GHCR namespace: ghcr.io/quantulabs/*
scripts/docker/record-digest.sh ghcr.io/quantulabs/8004-indexer v1.7.3 docker/digests.yml
scripts/docker/verify-image-integrity.sh ghcr.io/quantulabs/8004-indexer v1.7.3
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
    agentId
    owner
    totalFeedback
    solana {
      assetPubkey
      qualityScore
      trustTier
    }
  }
}
```

## E2E Scope

`npm run test:e2e` runs the maintained major end-to-end suites:

- `tests/e2e/reorg-resilience.test.ts`
- `tests/e2e/devnet-verification.test.ts`

`npm run test:localnet` handles the full localnet flow:

- start validator + deploy program
- initialize on-chain localnet state
- run `Localnet`-tagged E2E suite + metadata digestion checks (`uriDigest` + `collectionDigest` unit suites)
- stop validator (unless `KEEP_LOCALNET=1`)

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
