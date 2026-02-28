# Agent ID Sequence Burn Validation

## What Is Covered

`tests/unit/db/agent-sequence-burn-on-conflict.test.ts` validates the current contract that drives sequence burn behavior:

1. `supabase/schema.sql` defines `assign_agent_id()` with `nextval('agent_id_seq')` in a `BEFORE INSERT` trigger.
2. Runtime agent writes (`src/db/supabase.ts` and `src/indexer/batch-processor.ts`) use:
   - `INSERT INTO agents (...)`
   - `ON CONFLICT (asset) DO UPDATE`
   - no `agent_id` mutation in the `DO UPDATE` clause.
3. A deterministic state-machine test demonstrates the implication:
   - first insert `asset-A` gets `agent_id=1`
   - second insert for existing `asset-A` consumes `2` but keeps stored `agent_id=1`
   - next new `asset-B` gets `agent_id=3`

## Limitation

This is a schema/query-contract test, not a live PostgreSQL execution test.

Current fast test infrastructure in this repository is:

- mocked `pg` unit tests, and
- SQLite-backed e2e setup (`tests/e2e/setup.ts`).

So this validation proves behavior under current SQL semantics, but does not execute the trigger against a running PostgreSQL instance inside unit test runtime.
