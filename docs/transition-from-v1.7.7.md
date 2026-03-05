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

## Migration path (no reindex)

1. Apply pending DB migrations (Supabase SQL migrations or Prisma migrations, depending on your backend).
2. For local Prisma upgrades, use `prisma migrate deploy` (do not use `db push` on existing data).
3. Restart the indexer.
4. No chain replay/reindex is required for already indexed data: existing collection-pointer rows are backfilled from current agent snapshots and `collection_id` is assigned in-place.

## Sequential IDs and backend parity

- Sequential IDs are deterministic per backend migration path.
- If you compare upgraded historical datasets across different DB engines (PostgreSQL vs SQLite), legacy IDs may differ.
- For strict cross-engine parity checks, bootstrap each backend from the same canonical chain window and migration baseline.

## Rollout checklist

1. Update clients to read `collection_pointer` when present.
2. Keep `canonical_col` support until all clients migrate.
3. Send `creator` for `/collection_asset_count` and `/collection_assets` queries.
4. Add explicit `order` to `/collection_assets` queries for stable pagination.
5. Validate `/collections` queries in staging if you used advanced PostgREST query options.
6. Apply DB migrations and restart; no reindex is needed for already indexed data.
