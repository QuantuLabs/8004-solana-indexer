# Changelog

All notable changes to this project will be documented in this file.

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
