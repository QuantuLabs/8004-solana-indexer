# Upgrade to v1.8.4 without reindexing

This procedure preserves existing rows, public IDs, counters, and the full indexer cursor. It does not use `supabase/schema.sql`, `db push`, a reset, or a migration ledger.

1. Stop every indexer writer for the target database. Record the v1.8.3 image/commit and environment, capture `indexer_state` and `id_counters`, and take and verify a restorable database backup.
2. From the v1.8.4 source directory, apply the repaired files as one transaction:

```sh
psql -X -v ON_ERROR_STOP=1 --single-transaction --dbname "$TARGET_8004_DSN" \
  -f supabase/migrations/20260318193000_fix_feedback_id_recovery_determinism.sql \
  -f supabase/migrations/20260318201500_reassert_recovered_orphan_agent_id_assignment.sql \
  -f supabase/migrations/20260318203000_add_metadata_tombstones.sql \
  -f supabase/migrations/20260318204500_response_event_ordinal_identity.sql \
  -f supabase/migrations/20260318210000_rpc_limit_offset_caps.sql \
  -f supabase/migrations/20260318213000_add_mutation_order_anchors.sql
```

3. Before restarting writers, verify row counts, all existing public-ID mappings, `id_counters`, and every `indexer_state` field against the backup snapshot. Existing business-column values must be byte-for-byte unchanged. The only permitted stored changes are new `deleted = false` values and new internal `mutation_order_key` values derived from existing ordering fields; response `event_ordinal` columns may be newly added as null.
4. Start the frozen v1.8.4 runtime only after those checks pass.

Before any new event is ingested, rollback is restoring the verified backup and the v1.8.3 image; there is no down migration. After v1.8.4 ingests events, binary-only or schema-only rollback is unsafe because v1 transaction data, ordinal-distinct responses, and metadata tombstones may not be representable by v1.8.3. Stop writers and restore the pre-upgrade backup (losing the post-backup tail), or use a separately reviewed forward repair.
