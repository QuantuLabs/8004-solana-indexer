# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Fixed
- REST `/rest/v1/revocations` now parses/applies `revoke_count` filters for both scalar (`eq.N`) and PostgREST `in.(...)` forms used by SDK spot-check calls.

## [1.7.5] - 2026-03-01

### Fixed
- Propagated `ParentAssetSet.lock` to PostgreSQL batch writes in `updateParentAssetSupabase`, so `parent_locked` is now updated consistently in batch mode.
- Corrected initial `collection_pointers.asset_count` on first insert from `0` to `1` across all PostgreSQL `CollectionPointerSet` code paths (batch, transactional, non-transactional).

### Impact
- First-time collection pointer assignments no longer undercount by one.
- Parent lock state remains consistent between batch and non-batch ingestion paths.

## [1.7.4] - 2026-03-01

### Changed
- Renamed aggregate metric naming from `totalProtocols` to `totalCollections` for collection count surfaces.
- Added PostgreSQL deployment notes for running REST via a PostgREST sidecar when Supabase is not used.
- GHCR release workflow now enforces canonical `8004-indexer` package visibility as `public`.

### Removed
- Retired the validation API surface and no longer expose public validation endpoints.

### Fixed
- Hardened sequential-id handling for agents, feedback, responses, and revocations to keep deterministic ordering stable during replay/reindex flows.
- Fixed runtime/default network behavior so devnet defaults apply consistently.

## [1.7.2] - 2026-02-27

### Fixed
- Restored GraphQL `Agent.agentId` to map the sequential DB `agents.agent_id` registration id (deterministic ordering pipeline), instead of asset-derived bytes.
- Tightened GraphQL contract by requiring non-null `agentId` and surfacing explicit errors when `agent_id` is unexpectedly missing.

### Changed
- Updated README GHCR integrity helper examples to use `v1.7.2`.

## [1.7.1] - 2026-02-27

### Changed
- Patch release for ID-model follow-up updates across API/GraphQL mappings and targeted tests.

## [1.7.0] - 2026-02-27

### Changed
- Renamed internal deterministic registration key from `global_id` to `agent_id` across schema, migrations, Prisma, and API query/resolver mappings; API output field remains `agentid`.
- Collection pointer naming and accounting were aligned around `collection_pointers`, with idempotent count updates and REST `/rest/v1/agents` aliases for `asset` (id) and `collection_pointer` (canonical_col).
- Added `SOLANA_NETWORK`-driven RPC/WS defaults (`devnet`, `mainnet-beta`, `testnet`, `localnet`) plus startup warning when mainnet uses the default devnet `PROGRAM_ID`.

### Added
- REST agent timestamp filtering for `updated_at`, `updated_at_gt`, and `updated_at_lt` (ISO or unix timestamps), with explicit `400` validation errors for invalid values.

### Fixed
- Integrity hardening: WebSocket queue overflow now fail-stops ingestion instead of silently dropping logs, with backup poller catch-up.
- URI/collection metadata queues now defer and replay tasks at capacity and run periodic recovery sweeps after restarts.
- Immutable metadata rows are no longer purged or overwritten by URI-derived writes.
- Identity lock hints are parsed and propagated into `col_locked` / `parent_locked`.
- Deterministic ordering was tightened for poller tx sorting and REST feedback tie-breakers.

### DevOps & Tests
- Expanded Docker CI unit test set to include API parity and indexer queue/processor coverage suites.
- Localnet E2E script now supports `LOCALNET_CLONE_ATOM_PROGRAM` and `VITEST_E2E_MAX_WORKERS`.
- Added coverage for `agent_id` assignment consistency, collection pointer/lock handling, queue recovery behavior, and decoder lock hints.

## [1.3.0] - 2026-02-09

### Breaking
- HTTP metadata URIs rejected by default (set `ALLOW_INSECURE_URI=true` to allow)
- Compressed metadata limit reduced from 100KB to 10KB
- Verification errors now fail-closed (events stay PENDING instead of auto-FINALIZED)

### Security
- HTTPS DNS re-validation to prevent DNS rebinding SSRF
- `@mongodb-js/zstd` moved to production dependencies (fixes deployment crash)
- Status query parameter validation (rejects invalid values with 400)
- BigInt nonce range check prevents truncation

### Changed
- Rate limiter applied globally (was only /rest/v1)
- Trust proxy configurable via `TRUST_PROXY` env var (default: 1)

### Added
- New env vars: `TRUST_PROXY`, `ALLOW_INSECURE_URI`

## 1.2.0 - 2026-02-06

### Added
- SEAL hash validation in verifier for feedback/response/revocation integrity
- Metadata queue: background URI fetching with concurrent processing
- Security tests for API input validation, SSRF, and rate limiting
- Revocation tracking with `running_digest` and `revoke_count` fields
- Supabase handler support for `running_digest`, revocations, and null checks

### Changed
- Single-collection architecture: `AgentRegisteredInRegistry` renamed to `AgentRegistered`, `BaseRegistryCreated`/`UserRegistryCreated` replaced by `RegistryInitialized`
- Event count reduced from 14 to 13 (merged registry events)
- IDL updated for v0.6.0 program changes
- E2e reorg tests updated for new event types

### Removed
- `global_id` column (unstable row numbering across reindexes)

## 1.1.0 - 2026-01-26

### Added
- **v0.5.0 Feedback Fields** - Support for `value` (i64), `valueDecimals` (0-6), nullable `score`
- GraphQL schema updated with new feedback fields

### Changed
- Prisma schema updated for new feedback columns
- Event parser handles v0.5.0 feedback signature

## 1.0.0 - 2026-01-10

### Added
- Initial release
- Dual-mode indexing (WebSocket + polling)
- 14 Anchor event types indexed
- GraphQL API with GraphiQL explorer
- SQLite/PostgreSQL support
