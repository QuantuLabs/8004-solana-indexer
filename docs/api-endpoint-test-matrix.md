# API Endpoint Test Matrix

This matrix covers the current public surface of the indexer.

## Lifecycle And Transport

| Surface | Smoke | Parity | Integrity |
| --- | --- | --- | --- |
| `/health` | `200` + body shape | local vs supabase | stays live during bootstrap |
| `/ready` | `503` before ready, `200` after | local vs supabase | `phase` and backlog reflect all verifier families |
| `/metrics` | gated by config + content type | n/a | payload renders cleanly |
| `/v2/graphql`, `/graphql` | basic query succeeds | alias parity | identical schema behavior |

## REST Families

| Family | Endpoints | Smoke | Parity | Integrity |
| --- | --- | --- | --- | --- |
| Agents | `/rest/v1/agents`, `/agents/children`, `/agents/tree`, `/agents/lineage` | list + lookup + bad params | local vs supabase | parent/child and lineage consistency |
| Feedback chain | `/feedbacks`, `/responses`, `/feedback_responses`, `/revocations` | list + key filters + bad params | local vs supabase | ordering, pagination, IDs, parent links |
| Collections | `/collections`, `/collection_pointers`, `/collection_asset_count`, `/collection_assets`, `/collection_stats` | list/count/assets | local vs supabase | scoped counts reconcile to source rows |
| Stats/meta | `/stats`, `/global_stats`, `/stats/verification`, `/metadata`, `/agent_reputation`, `/leaderboard` | body shape + basic filters | local vs supabase | totals reconcile to canonical tables plus staged-orphan folding where applicable |
| RPC/replay | `/rpc/get_leaderboard`, `/rpc/get_collection_agents`, `/checkpoints/:asset`, `/checkpoints/:asset/latest`, `/verify/replay/:asset`, `/events/:asset/replay-data` | `200/400` coverage | local vs supabase | digest continuity, checkpoint monotonicity, `hasMore` correctness |

## GraphQL Query Families

| Family | Queries | Smoke | Parity | Integrity |
| --- | --- | --- | --- | --- |
| Agents | `agent`, `agents`, `agentSearch`, `agentChildren`, `agentTree`, `agentLineage`, `agentRegistrationFiles` | basic query | GraphQL vs REST overlap | tree/lineage shape and pagination |
| Feedback chain | `feedback`, `feedbacks`, `feedbackResponse`, `feedbackResponses`, `revocation`, `revocations` | basic query | GraphQL vs REST overlap | counts, ordering, parent linkage |
| Stats/meta | `agentMetadatas`, `agentStats`, `globalStats`, `agentReputation`, `leaderboard`, `verificationStats` | basic query | GraphQL vs REST overlap | totals and status buckets reconcile |
| Hashchain/collections | `hashChainHeads`, `hashChainLatestCheckpoints`, `hashChainReplayData`, `collections`, `collectionAssetCount`, `collectionAssets` | basic query | GraphQL vs REST overlap | digest/count continuity and pagination correctness |

## Priority Order

1. Smoke every public route/query and every alias.
2. Run parity on overlapping surfaces: `local REST` vs `supabase REST`, then `REST` vs `GraphQL`.
3. Run integrity checks on readiness, stats totals, replay/checkpoint/hashchain semantics, and sequential IDs.
