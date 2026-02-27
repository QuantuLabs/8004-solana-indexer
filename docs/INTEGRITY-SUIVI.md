# Integrity Suivi

Date: 2026-02-27

## Command Summary

- [x] `bunx vitest run tests/unit/indexer/cross-indexer-determinism.test.ts tests/unit/api-parity-both.test.ts tests/unit/db/handlers-coverage.test.ts tests/unit/indexer/processor.test.ts tests/unit/indexer/websocket-coverage.test.ts tests/unit/parser/decoder.test.ts`
  - Result: PASS (`6` test files, `269` tests, observed wall time ~`3.23s`)
- [x] `bun run test:localnet`
  - Result: PASS (`2` passed + `2` skipped test files, `19` passed + `44` skipped tests, observed wall time ~`19.98s`)
  - Localnet setup/deploy/init completed, then E2E suites executed successfully

## Integrity Checklist

- [x] deterministic ordering
- [x] agent_id consistency
- [x] lock flags handling
- [x] collection+parent ingestion
- [x] REST/GraphQL parity
- [x] localnet e2e status
- [ ] residual risks

## Residual Risks

- `tests/e2e/devnet-verification.test.ts` remained skipped in `test:localnet`, so devnet behavior is not validated in this run.
- `tests/e2e/reorg-resilience.test.ts` remained skipped in `test:localnet`, so reorg recovery behavior is not validated in this run.
