# 8004 Solana Indexer

Solana indexer for the 8004 Agent Registry with GraphQL v2 and transitional REST v1.

## Features

- WebSocket + polling ingestion modes
- Reorg resilience with verification worker
- GraphQL API (`/v2/graphql`) with query depth/complexity guards
- Legacy REST v1 (`/rest/v1/*`) available when `API_MODE=rest|both`
- Supabase/PostgreSQL data backend support
- Strict parent-aware entity policy on top of event integrity:
  - revoke without parent feedback => `ORPHANED`
  - response without parent feedback => staged in `orphan_responses` until a matching parent exists
  - revocation `seal_hash` mismatch with parent feedback => logged/flagged (not `ORPHANED`)
  - response `seal_hash` mismatch => logged/flagged while the parent exists (not `ORPHANED`)
  - GraphQL filters exclude `ORPHANED` records by default

## Transition Notes

If you are upgrading from `v1.7.7`, read:

- [docs/transition-from-v1.7.7.md](docs/transition-from-v1.7.7.md)
- Canonical collection identity is `creator + collection_pointer` (collection endpoints enforce this scope).

## Upgrade

### Existing PostgreSQL / Supabase

- Back up the database.
- Apply the SQL files in `supabase/migrations/` in lexicographic order after the last migration you already applied.
- If you are upgrading specifically from `v1.7.7`, apply these files in order:
  - `20260303160000_add_collection_sequential_id.sql`
  - `20260305220000_align_global_stats_with_graphql_status.sql`
  - `20260305235500_add_orphan_feedbacks.sql`
  - `20260306164000_add_indexer_state_last_tx_index.sql`
  - `20260306210000_add_orphan_responses.sql`
  - `20260306223000_reorder_canonical_indexes.sql`
  - `20260306230000_align_global_stats_collection_pointers.sql`
  - `20260307111500_deterministic_id_counter_timestamps.sql`
  - `20260307123000_backfill_legacy_orphan_responses.sql`
  - `20260309190000_add_agent_reputation_proxy_objects.sql`
  - `20260311173000_add_collection_pointer_last_seen_tx_index.sql`
  - `20260312123000_align_proxy_views_and_leaderboard_grant.sql`
  - `20260313193000_add_feedback_response_seal_hash.sql`
  - `20260314150000_include_staging_orphans_in_verification_stats.sql`
  - `20260315193000_add_validations_to_verification_stats.sql`
  - `20260315195500_fix_recovered_orphan_agent_id_assignment.sql`
- Restart the indexer on the same database.
- Do not run `supabase/schema.sql` on an existing database.
- Keep an operator record of the last migration filename you applied; the repo does not maintain a PG migration-tracking table for you.

### Existing local SQLite

- Back up the SQLite file.
- If the file already has a valid Prisma migration baseline (`_prisma_migrations`), run `prisma migrate deploy` against it.
- If you are upgrading specifically from `v1.7.7`, `prisma migrate deploy` should apply:
  - `20260303161000_add_collection_sequential_id`
  - `20260304120000_add_agent_sequential_id`
  - `20260305113000_backfill_collection_sequential_id`
  - `20260305143000_add_indexer_state_monotonic_guard`
  - `20260305235500_add_orphan_feedback`
  - `20260306164000_add_indexer_state_last_tx_index`
  - `20260306210000_extend_orphan_response_proof`
  - `20260306233000_add_missing_agent_runtime_columns_sqlite`
  - `20260306234500_sqlite_fix_feedback_value_text`
  - `20260307000500_fix_indexer_state_monotonic_guard_tx_index`
  - `20260311173000_add_collection_pointer_last_seen_tx_index`
  - `20260312110000_add_local_collection_id_counter`
  - `20260313193000_add_feedback_response_seal_hash`
- If the file comes from the known `v1.7.7` local/Docker `prisma db push` flow without a valid `_prisma_migrations` baseline, do not run a blind `prisma migrate deploy`. Apply the shipped legacy bundle against that same file:
  - `sqlite3 /path/to/indexer.db < prisma/legacy-upgrades/v1.7.7-dbpush-to-current.sql`
- That bundle is intentionally scoped to the proven `v1.7.7` db-push shape and already excludes the two duplicate-column migrations that fail on that legacy schema.
- For older/unknown local SQLite files outside that proven `v1.7.7` db-push shape, the simple supported path is still reindex into a fresh SQLite database unless you deliberately baseline and verify the schema yourself.
- Restart the indexer with the same database file.
- Do not use `prisma db push` on an existing indexed SQLite database.

### Docker / Compose

- Upgrading the image alone is not enough for persisted databases.
- Container startup does not run upgrade migrations automatically.
- If the container uses PostgreSQL / Supabase, apply the pending `supabase/migrations/*.sql` to that persisted database, then restart the container.
- If the container uses local SQLite, run `prisma migrate deploy` against the same persisted SQLite file, then restart the container.
- The runtime image defaults local SQLite to `file:/app/data/indexer.db`; if you mount a different path, point both the runtime `DATABASE_URL` and your manual `prisma migrate deploy` command at that exact same file.
- If that SQLite file came from the known `v1.7.7` Docker `db push` flow without a valid `_prisma_migrations` baseline, apply `prisma/legacy-upgrades/v1.7.7-dbpush-to-current.sql` with `sqlite3` against the mounted file, then restart the container.
- If the file is older/unknown and does not match that proven shape, do not force a blind migrate; reindex into a fresh SQLite file unless you are deliberately doing a manual baseline.
- Restart the container with the same volume/env after the backend-specific upgrade step completes.

### When Migration + Restart Is Enough

- This is the supported path when you keep the same network/program/history and upgrade an already indexed database through the shipped migrations only.
- Shipped upgrade migrations are intended to fill missing runtime data needed by the newer runtime without a full reindex.
- For affected local SQLite drift, startup repair may rewrite previously wrong `agents.agent_id` values to their canonical order without requiring a reindex.
- Do not treat an in-place upgrade on a historically drifted database as proof that every public sequential ID will match a fresh rebuild row-for-row.
- Do not treat an in-place upgrade on a historically drifted PostgreSQL database as proof that historical hash-chain statuses or orphan staging layout will match a fresh rebuild exactly.

### When Reindex Is Required

- Reindex is required after a destructive reset, when switching to a different network/program/bootstrap window, or when rebuilding a database already known to contain historical holes/inconsistent rows from an older run.
- For the known `v1.7.7` local/Docker SQLite `db push` shape without `_prisma_migrations`, do not use `prisma migrate deploy`; use the exact manual `sqlite3` subset listed above.
- For older/unknown SQLite files without `_prisma_migrations`, do not assume `prisma migrate deploy` is safe blindly; reindex into a fresh database unless you deliberately baseline and verify the schema yourself.

### How To Verify After Upgrade

- Check `/health` for process liveness.
- Check `/ready` for readiness plus current indexer state. The response keeps `status` (`starting|ready`) and now also includes `phase` plus a `state` block with the current `mainSlot`, object counts, and pending/orphan counts when the backend can provide them.
- Confirm the persisted cursor resumes from the existing state and advances.
- Verify representative `agents`, `feedbacks`, `responses`, `revocations`, and `collections` still resolve through the API.
- If you use collection-scoped reads, verify them with `creator + collection`.
- If you need strict PostgreSQL-vs-SQLite parity validation, treat that as a separate verification exercise, not as the default in-place upgrade contract.

## Quick Start

```bash
npm install
cp .env.devnet.example .env
npm run db:generate
# Optional (fresh local DB only) when DB_MODE=local (Prisma local DB)
# npm run db:push
# Upgrades must use Prisma migrations:
# npx prisma migrate deploy
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
- `scripts/migrate-supabase.js` and `scripts/run-supabase-migration.js` are legacy helpers and are intentionally disabled.
- Upgrades must use `supabase/migrations/*.sql` (in order), not `schema.sql`.
- When existing indexer tables are detected, init requires explicit `RESET` confirmation in an interactive TTY, or `FORCE_SCHEMA_RESET=1` for non-interactive forced resets.

## WARNING: Historical Sync vs WebSocket

WebSocket subscriptions are live notifications, not historical replay.

- For complete bootstrap/sync, use HTTP RPC history first (`getSignaturesForAddress` + transaction fetch).
- Then run live tail with `INDEXER_MODE=websocket` or `INDEXER_MODE=auto`.
- If your provider/network keeps limited history (often devnet/testnet), use archival RPC for bootstrap, or set `INDEXER_START_SIGNATURE` + `INDEXER_START_SLOT` to the same exact tx/slot pair.
- When both are set, startup validates that the signature resolves to that slot and fails fast on mismatch.

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
- Set `DB_MODE` and `API_MODE` explicitly in production env files. Docker still falls back to `API_MODE=both` if unset, and the PostgREST overlay forces `API_MODE=both`.
- `.env.devnet.example` includes the current devnet bootstrap cursor as an exact validated signature/slot pair.
- `.env.mainnet.example` is prefilled with current mainnet `PROGRAM_ID` and `ATOM_ENGINE_PROGRAM_ID`, plus a commented validated bootstrap pair for history-capable RPCs.
- IDLs are stored side-by-side in `idl/`: `agent_registry_8004.json` (devnet/testnet companion) and `agent_registry_8004.mainnet.json` (mainnet/localnet companion). Runtime selection follows the effective `PROGRAM_ID`.
- `API_MODE=both` is best-effort dual mode and disables whichever side has no matching DB backend.
- `TRUST_PROXY` defaults to `false`; set it explicitly (for example `1`) only when running behind a trusted reverse proxy.
- `SOLANA_NETWORK` drives default RPC/WS endpoints when `RPC_URL`/`WS_URL` are unset.
- `MAX_SUPPORTED_TRANSACTION_VERSION` controls parsed transaction version support for RPC fetches (default `0`).
- Optional startup cursor: `INDEXER_START_SIGNATURE` is applied when no persisted `indexer_state` exists.
- If `INDEXER_START_SIGNATURE` + `INDEXER_START_SLOT` are configured and persisted state exists but is older than that slot, the poller fast-forwards persisted state to the configured cursor.
- `INDEXER_START_SLOT` requires `INDEXER_START_SIGNATURE`.
- When both are set, startup validates that `INDEXER_START_SIGNATURE` resolves to `INDEXER_START_SLOT` and aborts on mismatch.
- If `SOLANA_NETWORK=mainnet-beta`, ensure `PROGRAM_ID` and `ATOM_ENGINE_PROGRAM_ID` match your deployed mainnet programs. Startup validation warns if mainnet is selected but `PROGRAM_ID` is still the default devnet ID.
- `.env.localnet` is preconfigured for local REST mode.
- `GRAPHQL_STATS_CACHE_TTL_MS` controls `globalStats`/`protocol` aggregate cache TTL (default `60000` ms).
- `IPFS_GATEWAY_BASE` sets the gateway base used for `ipfs://` URI digest fetches and canonical collection pointer (`c1:<cid>`) fetches (default `https://ipfs.io`).
- `URI_DIGEST_TRUSTED_HOSTS` is optional and only accepts `localhost`/`127.0.0.1` to allow local IPFS gateway tests without disabling SSRF protections globally.
- Validation module is archived on-chain in v0.5.0+; validation indexing is opt-in via `INDEX_VALIDATIONS=true` (default `false` outside tests).
- Validation entity endpoints are removed from the public API surface: REST `/rest/v1/validations` is retired and returns `410 Gone`, and GraphQL exposes no `validation`/`validations` query fields.

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

Local PostgreSQL bootstrap for `DB_MODE=supabase`:

```bash
scripts/start-local-postgres.sh
PGPASSWORD=indexer /opt/homebrew/opt/postgresql@17/bin/psql \
  -h 127.0.0.1 -p 55432 -U indexer -d indexer \
  -f supabase/schema.sql
```

Then point the indexer at:

```bash
SUPABASE_DSN=postgresql://indexer:indexer@127.0.0.1:55432/indexer?sslmode=disable
SUPABASE_SSL_VERIFY=false
```

Important:

- `docker/stack/indexer-stack.yml` does not start PostgreSQL for you.
- `docker/stack/indexer-stack.postgrest.yml` only adds PostgREST; it still expects an existing PostgreSQL server.
- `scripts/start-local-postgres.sh` defaults to Homebrew PostgreSQL 17 under `/opt/homebrew/opt/postgresql@17/bin`; override `PG_BIN` if your local binaries live elsewhere.
- For a fresh local PostgreSQL database, load `supabase/schema.sql` as the `indexer` owner user, not as `postgres`, otherwise the runtime service role will hit permission errors on tables like `indexer_state`.

Optional PostgREST sidecar (local PostgreSQL REST, default stack unchanged):

```bash
cp docker/stack/postgrest.env.example .env.postgrest
# edit .env.postgrest with your local PostgreSQL DSN/role/secret
# POSTGREST_* takes precedence over SUPABASE_* when both are set

# generate a local POSTGREST_TOKEN (HS256 JWT with role=anon by default)
node -e 'const c=require("crypto");const enc=(o)=>Buffer.from(JSON.stringify(o)).toString("base64url");const secret=process.env.SECRET||"replace-with-random-secret";const payload={role:"anon",iat:Math.floor(Date.now()/1000),exp:Math.floor(Date.now()/1000)+315360000};const token=enc({alg:"HS256",typ:"JWT"})+"."+enc(payload);const sig=c.createHmac("sha256",secret).update(token).digest("base64url");console.log(token+"."+sig)'

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

Indexer endpoint remains unchanged when the indexer REST surface is enabled:

```text
http://localhost:3001/rest/v1/*
```

Set `API_MODE=both` or `API_MODE=rest` if you want the indexer itself to expose `/rest/v1/*`; the default `.env.example` keeps `API_MODE=graphql`.

Minimum PostgreSQL grants for `PGRST_DB_ANON_ROLE` (example role `anon`):

```sql
create role anon nologin;
grant usage on schema public to anon;
grant select on all tables in schema public to anon;
alter default privileges in schema public grant select on tables to anon;
```

Integrity helpers:

```bash
# Official GHCR namespace: ghcr.io/quantulabs/*
scripts/docker/record-digest.sh ghcr.io/quantulabs/8004-indexer v1.8.2 docker/digests.yml
scripts/docker/verify-image-integrity.sh ghcr.io/quantulabs/8004-indexer v1.8.2
```

## GraphQL Example

```graphql
query Dashboard {
  globalStats {
    totalAgents
    totalFeedback
    totalCollections
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
