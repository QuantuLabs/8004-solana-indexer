# Transition Guide: from v1.7.7

This guide covers integrator-facing changes introduced after `v1.7.7`.

## API compatibility status

### `/rest/v1/agents`
- Input compatibility:
  - `canonical_col` is still accepted (deprecated alias).
  - `collection_pointer` is accepted and mapped internally.
  - If both are provided, `canonical_col` is used first (v1.7.7-compatible precedence).
- Output compatibility (proxy mode):
  - `canonical_col` is preserved.
  - `collection_pointer` is added when available.

No client break is expected for existing consumers of `canonical_col`.

### `/rest/v1/collections`
- Added `collection_id` in response payload.
- Added `collection_id` filtering (`eq`, `gt`, `gte`, `lt`, `lte`).
- In proxy mode, this endpoint is now served by local compatibility logic (not raw PostgREST passthrough).

If you relied on arbitrary PostgREST `select/order` on proxied `/collections`, validate behavior before rollout.

### `/rest/v1/collection_assets`
- Proxy fallback default order changed to:
  - `created_at.desc,asset.desc`

If your pagination logic depends on previous implicit ordering, send an explicit `order=` query param.

## Rollout checklist

1. Update clients to read `collection_pointer` when present.
2. Keep `canonical_col` support until all clients migrate.
3. Add explicit `order` to `/collection_assets` queries for stable pagination.
4. Validate `/collections` queries in staging if you used advanced PostgREST query options.
5. Apply DB migrations before enabling new `collection_id`-dependent features.
