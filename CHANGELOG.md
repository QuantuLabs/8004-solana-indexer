# Changelog

All notable changes to this project will be documented in this file.

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
