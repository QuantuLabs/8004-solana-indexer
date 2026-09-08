# Upgrade to v1.8.4 without reindexing

This procedure preserves existing rows, public IDs, counters, and the full indexer cursor. It does not use `supabase/schema.sql`, `db push`, a reset, or a migration ledger.

This procedure is proven from the actual `v1.7.7` PostgreSQL schema and the current `v1.8.3` baseline. Earlier and unknown shapes are unsupported until a separate bridge is verified; stop before writing rather than forcing the chain.

1. Stop every indexer writer for the target database. Record the old image/commit, environment, and exact last applied migration basename; capture `indexer_state` and `id_counters`, and take and verify a restorable database backup.
2. From the new source directory, set libpq's standard `PG*` variables for the target and apply every later shipped migration as one transaction:

```sh
node scripts/upgrade-postgres.mjs --after <exact-last-applied-filename>
```

For `v1.7.7`, use `20260301123000_gapless_ids_for_all_scopes.sql`; for the verified `v1.8.3` baseline, use `20260315195500_fix_recovered_orphan_agent_id_assignment.sql`. The CLI requires an exact shipped basename, sorts all later files, validates the complete SQL before database execution, and automatically includes later release migrations added to the directory. It strips only the asserted outer `BEGIN;`/`COMMIT;` pair in the two known historical files and rejects unexpected top-level transaction control. It does not infer state, create a tracking table, log a DSN, reset data, or run at application startup.

3. Before restarting writers, verify row counts, all existing public-ID mappings, existing counter checkpoints, and every `indexer_state` field against the backup snapshot. Existing business-column values must be byte-for-byte unchanged. Newly introduced IDs and counter scopes may be initialized when the starting schema did not provide them. New metadata tombstones default to `false`; newly added response ordinals may be null. The seven internal agent field-order keys are initialized from the greatest available creation key, existing mutation key, and complete saved main cursor. This fixed cutover floor preserves the stored field values against historical replay; it does not reconstruct past field history. Existing non-null field keys remain unchanged.
4. Start the frozen v1.8.4 runtime only after those checks pass.

Before any new event is ingested, rollback is restoring the verified backup and the recorded previous image; there is no down migration. After v1.8.4 ingests events, binary-only or schema-only rollback is unsafe because v1 transaction data, ordinal-distinct responses, and metadata tombstones may not be representable by the previous runtime. Stop writers and restore the pre-upgrade backup (losing the post-backup tail), or use a separately reviewed forward repair.
