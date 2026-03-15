# Historical Catch-Up Chunking Plan

## Status

Not green for implementation yet.

Deep audit found that the current Solana historical scan primitive is fundamentally
`newest -> oldest`. A naive `scan bounded chunk -> replay bounded chunk` change
would replay the wrong side of the gap first and can violate global canonical
ordering.

This document therefore records the intended direction plus the blockers that
must be solved before any code change is safe.

## Goal

Reduce time-to-first-useful-progress during historical bootstrap by changing the
historical catch-up flow from:

- `scan all pages -> replay all pages`

to:

- `scan bounded chunk -> replay bounded chunk -> advance main cursor -> continue`

This plan is intentionally limited to the historical poller path. It does not
change the live polling path, websocket path, handlers, or verifier semantics.

## Why

The current historical bootstrap can accumulate hundreds of persisted
`historical-scan:*` pages while:

- `main` stays pinned to the bootstrap cursor
- `/ready` stays `starting`
- stats remain at `0`
- no business rows are written yet

That behavior is robust, but hard to observe and slow to produce useful
progress. Chunked replay would make progress visible and resumable earlier.

## Scope

Only touch:

- `src/config.ts`
- `src/indexer/poller.ts`
- `tests/unit/indexer/poller.test.ts`
- `tests/unit/indexer/poller-coverage.test.ts`

Do not change:

- `src/indexer/websocket.ts`
- `src/indexer/processor.ts`
- event handlers
- verifier
- API contracts

## Safety Constraints

These constraints must hold, otherwise the change is rejected.

1. Replay must consume persisted pages, not refetch pages during replay.
2. Historical replay order must remain canonical: `oldest -> newest`.
3. Chunk boundaries must remain slot-safe. Never cut a replay unit across an
   unresolved slot boundary.
4. `main` must advance only after the replay unit completes successfully.
5. Persisted pages must be deleted only after successful replay of those pages.
6. A restart must never require rescanning already-replayed pages.

## Confirmed Blockers From Audit

1. The current page scan direction is `newest -> oldest`, not `oldest -> newest`.
   A bounded scan does not guarantee that the scanned chunk is adjacent to the
   oldest unresolved edge of the gap. Replaying it early can skip older unseen
   history.

2. Persisted historical pages currently carry the durable `nextBeforeSignature`
   continuation. Deleting replayed pages without a separate scan-continuation
   cursor loses the ability to continue scanning safely.

3. The current replay path advances `main` during transaction processing, not at
   chunk commit boundaries. Any chunk design must account for that and either:
   - make replay idempotent across restart windows, or
   - add a staging cursor / atomic progress model.

4. Returning to the normal poll loop between chunks would be incorrect if any
   historical gap remains. Historical catch-up must remain inside the bootstrap
   loop until the gap is fully closed.

## Proposed Minimal Change

### 1. Add a real scan continuation cursor

Before any chunked replay is implemented, introduce an explicit persisted
historical scan continuation state:

- `stopSignature`
- `nextBeforeSignature`
- replay progress / phase

This state must be independent from the persisted page rows themselves.

### 2. Prove safe replay boundaries

Before replaying partial history, prove that the replay unit is the oldest
globally safe unit left in the unresolved gap. With the current API shape, that
is not yet guaranteed.

### 3. Only then consider a chunk cap

Add a config value:

- `HISTORICAL_SCAN_MAX_PAGES_PER_PASS`

Suggested default:

- `10`

This caps how many historical pages are scanned before replay starts.

### 4. Split historical catch-up into two explicit phases

Refactor `catchUpHistoricalGap()` into:

- `scanHistoricalPages(...)`
- `replayHistoricalPages(...)`

`scanHistoricalPages(...)`:

- scans at most `N` pages
- persists those pages as `historical-scan:*`
- preserves existing slot-boundary safety

`replayHistoricalPages(...)`:

- replays only those persisted pages
- never rescans them
- replays in canonical order
- advances `main` only after successful replay of the current unit
- deletes only the replayed pages

### 5. Continue incrementally

If more historical pages remain after a replayed chunk:

- return control to the poll loop
- next pass resumes scanning from the last persisted `nextBeforeSignature`

This keeps restarts useful because `main` advances after each chunk.

## Non-Goals

This change is not intended to:

- reduce provider `429`/`503` by itself
- alter live websocket behavior
- change business ordering semantics
- redesign verifier scheduling

## Tests Required

Add or update tests to prove:

1. `scan 2 pages -> replay 2 pages -> main advances`
2. restart after one replayed chunk resumes from the next page
3. only replayed pages are deleted
4. chunk boundaries remain slot-safe
5. replay still uses persisted page contents, not a fresh page fetch

## Acceptance Criteria

The plan is complete only if:

- unit tests prove chunked replay semantics
- `main` advances before the entire historical scan backlog is exhausted
- a restart after one replayed chunk resumes without rescanning replayed pages
- no new orphan/order regressions are introduced

Until the blockers above are resolved, no implementation should be merged.
