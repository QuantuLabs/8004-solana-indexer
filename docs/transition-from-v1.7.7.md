# Transition Guide: from v1.7.7

This guide covers integrator-facing changes introduced after `v1.7.7`.

## API compatibility status

### `/rest/v1/agents`
- Input compatibility:
  - `canonical_col` is still accepted (deprecated alias).
  - `collection_pointer` is accepted and mapped internally.
  - If both are provided, non-empty `canonical_col` is used first (v1.7.7-compatible precedence); empty `canonical_col` falls back to `collection_pointer`.
- Output compatibility (proxy mode):
  - `canonical_col` is preserved.
  - `collection_pointer` is added when available.

No client break is expected for existing consumers of `canonical_col` in proxy mode.

### `/rest/v1/collections`
- Added `collection_id` in response payload.
- Added `collection_id` filtering (`eq`, `gt`, `gte`, `lt`, `lte`).
- In proxy mode, this endpoint is now served by local compatibility logic (not raw PostgREST passthrough).
- Canonical uniqueness is `(creator, collection)`; the same collection pointer under different creators is represented as separate collection rows (and separate `collection_id` values).

If you relied on arbitrary PostgREST `select/order` on proxied `/collections`, validate behavior before rollout.

### `/rest/v1/collection_asset_count`
- `creator` is required (scope is `creator + collection`).
- Requests missing `creator` now return `400`.

### `/rest/v1/collection_assets`
- `creator` is required (scope is `creator + collection`).
- Requests missing `creator` now return `400`.
- Proxy fallback default order changed to:
  - `created_at.desc,asset.desc`

If your pagination logic depends on previous implicit ordering, send an explicit `order=` query param.

### `/rest/v1/responses` and `/rest/v1/feedback_responses`
- `response_id` filtering requires canonical feedback scope:
  - `asset + feedback_id`, or
  - `asset + client_address + feedback_index`.
- Requests using `response_id` outside these scopes return `400`.

### `tx_signature` filters (REST v1)
- Supported operators are `eq`, `neq`, `in`, `not.in`.
- Malformed or empty values return `400` (for example `eq.`, `in.()`, `in.(sig,)`, unmatched quotes).

## Supported upgrade paths

This transition supports in-place upgrade for existing indexed databases when you:

1. keep the same network/program/bootstrap window,
2. apply only the shipped upgrade migrations for your backend,
3. restart the indexer on the same database.

That is the supported `migration + restart` path. It is distinct from strict cross-engine parity validation.

## PostgreSQL / Supabase upgrade

1. Back up the database.
2. Apply these SQL files from `supabase/migrations/` in order:
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
3. Restart the indexer.
4. Verify the cursor resumes from the existing `indexer_state`.

Important:

- Do not run `supabase/schema.sql` on an existing database.
- `schema.sql` is for fresh initialization only.
- If `ENABLE_PROOFPASS=true`, startup now fails fast until `20260309192500_add_extra_proofpass_feedbacks.sql` is applied.
- The repo does not maintain a PostgreSQL migration-tracking table for you. Record the last filename you applied and continue lexicographically from there on future upgrades.

Optional PostgreSQL / Supabase enrichment extras:

| Feature | Env gate | Required SQL | Needed when feature is off? |
| --- | --- | --- | --- |
| ProofPass | `ENABLE_PROOFPASS=true` | `20260309192500_add_extra_proofpass_feedbacks.sql` | No |

When `ENABLE_PROOFPASS=true`, custom ProofPass deployments must also set `PROOFPASS_PROGRAM_ID` to the program that emits `PP_FINALIZE`.
On `DB_MODE=supabase`, GraphQL resolves the badge through `solana.proofPassAuth`; the extra only gates ProofPass storage/backfill, and the field remains in the schema and returns `false` when the extra is disabled.

## SQLite local upgrade

1. Back up the SQLite file.
2. If that file already has a valid Prisma migration baseline (`_prisma_migrations`), run `prisma migrate deploy` against it. From `v1.7.7`, Prisma should apply:
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
3. If it comes from the known `v1.7.7` local/Docker `prisma db push` flow without that baseline, do not run a blind `prisma migrate deploy`. Apply the shipped legacy bundle against that same file:
   - `sqlite3 /path/to/indexer.db < prisma/legacy-upgrades/v1.7.7-dbpush-to-current.sql`
4. That bundle is intentionally scoped to the proven `v1.7.7` db-push shape and already excludes the two duplicate-column migrations that fail on that legacy schema.
5. If the file is older/unknown and does not match that proven `v1.7.7` db-push shape, the simple supported path is still reindex into a fresh SQLite database unless you deliberately baseline and verify the schema yourself.
6. Restart the indexer with the same database file.
7. Verify the cursor resumes from the existing local state.

Important:

- Do not use `prisma db push` on an existing indexed SQLite database.
- `db push` is for fresh local schema initialization, not in-place upgrade.

## Docker / Compose upgrade

1. Pull/build the new image.
2. Stop the old container.
3. Apply the database migration outside the runtime container for the backend you actually use:
   - PostgreSQL / Supabase: apply the SQL files listed above, or continue lexicographically after the last migration filename you already applied.
   - Local SQLite: run `prisma migrate deploy` against the same persisted SQLite file.
4. Restart the container with the same volume/env.

Important:

- Container startup does not auto-run upgrade migrations for persisted databases.
- The image build may prepare a fresh local SQLite schema for smoke/fresh init, but that is not a substitute for upgrading an existing persisted database.
- The runtime image defaults local SQLite to `file:/app/data/indexer.db`; if you mount a different file, point both `DATABASE_URL` and your manual `prisma migrate deploy` command at that same file.
- If the persisted SQLite file matches the known `v1.7.7` Docker `db push` shape without `_prisma_migrations`, apply `prisma/legacy-upgrades/v1.7.7-dbpush-to-current.sql` with `sqlite3` against that same file, then restart.
- If it does not match that proven shape, do not force a blind migrate; reindex into a fresh database unless you are deliberately doing a manual baseline.

## When migration-only is enough

Migration-only is the supported path when:

- the database already contains the indexed history you want to keep,
- you are not changing network/program/bootstrap history,
- you apply the shipped migrations only,
- you are not trying to rebuild an already-corrupted or historically incomplete dataset.

Shipped upgrade migrations are intended to fill missing runtime data needed by the newer code without a full reindex.
With `ENABLE_PROOFPASS=true` on PostgreSQL / Supabase, startup + verifier can also backfill missing `extra_proofpass_feedbacks` rows from already indexed feedback transactions without a full reindex.
For local SQLite databases created by the affected drifted releases, the startup repair may deterministically rewrite `agents.agent_id` to the canonical order so the upgraded DB matches a fresh local/pool rebuild.
Do not treat a successful in-place upgrade on a historically drifted database as proof that every legacy runtime/status field will match a fresh rebuild row-for-row.
Do not treat a successful in-place upgrade on a historically drifted PostgreSQL database as proof that historical hash-chain statuses or orphan staging layout will match a fresh rebuild exactly.

## When reindex is required

Reindex is required when:

1. you intentionally reset the database,
2. you switch to a different network/program/bootstrap window,
3. the existing database is already known to contain historical holes/inconsistent rows from an older run,
4. you need a fresh canonical comparison baseline across different DB engines from the same chain window.

Additional SQLite note:

- If your local SQLite database was originally created by the known `v1.7.7` Docker/local `prisma db push` flow and the file does not contain a valid `_prisma_migrations` baseline, use the exact manual `sqlite3` subset above instead of `prisma migrate deploy`.
- If the file is older/unknown and does not match that proven shape, do not assume `prisma migrate deploy` is safe blindly on that file. The simple supported path is reindex into a fresh database.

## Post-upgrade verification

After upgrade:

1. check `/health` for process liveness,
2. check `/ready` for readiness and the current bootstrap/catch-up state (`status`, `phase`, `mainSlot`, pending/orphan counts when available),
2. confirm `indexer_state` resumes from the existing cursor and continues to advance,
3. validate representative reads on `agents`, `feedbacks`, `responses`, `revocations`, and `collections`,
4. validate collection-scoped endpoints with `creator + collection`,
5. if needed, compare high-level row counts before/after upgrade,
6. do not treat a successful in-place upgrade as proof of raw PostgreSQL-vs-SQLite row-for-row parity unless you run a separate parity check at the same cutoff.

## Sequential IDs and cross-engine comparisons

- Public sequential IDs are part of the API surface.
- Supported in-place upgrades preserve canonical public IDs. For affected local SQLite drift, startup repair may rewrite previously wrong `agents.agent_id` values to their canonical order without requiring a reindex.
- Strict PostgreSQL-vs-SQLite parity is a separate validation problem from in-place upgrade support.
- If you need strict cross-engine parity, bootstrap each backend from the same canonical chain window and verify them at the same cutoff.

## Rollout checklist

1. Update clients to read `collection_pointer` when present.
2. Keep `canonical_col` support until all clients migrate.
3. Send `creator` for `/collection_asset_count` and `/collection_assets` queries.
4. Add explicit `order` to `/collection_assets` queries for stable pagination.
5. Validate `/collections` queries in staging if you used advanced PostgREST query options.
6. Apply the backend-specific migration path above, then restart and verify cursor/API health.
