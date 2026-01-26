# 8004-solana-indexer

A lightweight, self-hosted Solana indexer for the [8004 Agent Registry](https://github.com/QuantuLabs/8004-solana) program with GraphQL API.

## Features

- **Dual-mode indexing**: WebSocket (real-time) + polling (fallback)
- **14 Anchor event types** indexed (Identity, Reputation, Validation)
- **v0.5.0 feedback fields**: `value` (i64), `valueDecimals` (0-6), nullable `score`
- **GraphQL API** with built-in GraphiQL explorer
- **Works with any Solana RPC** (Helius, QuickNode, public devnet)
- **Zero external dependencies**: SQLite included, just Node.js required

## Quick Start

```bash
# Clone and install
git clone https://github.com/QuantuLabs/8004-solana-indexer.git
cd 8004-solana-indexer
npm install

# Setup
cp .env.example .env
npm run db:generate
npm run db:push

# Run
npm run dev
```

GraphQL API available at `http://localhost:4000/graphql`

## Configuration

Edit `.env` to customize:

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | Database path | `file:./data/indexer.db` (SQLite) |
| `RPC_URL` | Solana RPC HTTP endpoint | `https://api.devnet.solana.com` |
| `WS_URL` | Solana RPC WebSocket endpoint | `wss://api.devnet.solana.com` |
| `PROGRAM_ID` | 8004 Agent Registry program ID | `6MuHv4dY4p9E4hSCEPr9dgbCSpMhq8x1vrUexbMVjfw1` |
| `INDEXER_MODE` | `auto`, `polling`, or `websocket` | `auto` |
| `GRAPHQL_PORT` | GraphQL server port | `4000` |

### Using PostgreSQL (optional)

If you prefer PostgreSQL over SQLite:

1. Install PostgreSQL on your system
2. Create a database: `createdb indexer8004`
3. Update `.env`:
   ```
   DATABASE_URL="postgresql://user:pass@localhost:5432/indexer8004"
   ```
4. Change `prisma/schema.prisma`:
   ```prisma
   datasource db {
     provider = "postgresql"
     url      = env("DATABASE_URL")
   }
   ```
5. Run `npm run db:generate && npm run db:push`

## GraphQL API

### Example Queries

```graphql
# Get all agents
query {
  agents(limit: 10, orderBy: CREATED_AT_DESC) {
    id
    owner
    nftName
    feedbackCount
    averageScore
  }
}

# Get agent details with v0.5.0 feedback fields
query {
  agent(id: "AgentPubkeyHere") {
    id
    owner
    nftName
    metadata { key value }
    feedbacks(limit: 5) {
      score           # 0-100 or null (ATOM skipped)
      value           # i64 raw metric (e.g., profit in cents)
      valueDecimals   # 0-6 decimal precision
      tag1
      client
    }
  }
}

# Indexer status
query {
  stats {
    totalAgents
    totalFeedbacks
    totalValidations
  }
  indexerStatus {
    running
    mode
    pollerActive
    wsActive
  }
}
```

### Available Queries

| Query | Description |
|-------|-------------|
| `agent(id)` | Get single agent by ID |
| `agents(owner, collection, registry, limit, offset, orderBy)` | List agents with filters |
| `feedbacks(agentId, client, minScore, maxScore, tag, revoked)` | List feedbacks |
| `validations(agentId, validator, requester, pending)` | List validations |
| `registries(registryType, authority)` | List registries |
| `stats` | Indexer statistics |
| `indexerStatus` | Indexer health status |
| `searchAgents(query, limit)` | Search agents |

## Indexed Events

| Category | Events |
|----------|--------|
| **Identity** | AgentRegisteredInRegistry, AgentOwnerSynced, UriUpdated, WalletUpdated, MetadataSet, MetadataDeleted, BaseRegistryCreated, UserRegistryCreated, BaseRegistryRotated |
| **Reputation** | NewFeedback, FeedbackRevoked, ResponseAppended |
| **Validation** | ValidationRequested, ValidationResponded |

## Development

```bash
npm test              # Run unit tests
npm run test:coverage # Tests with coverage
npm run db:studio     # Open Prisma Studio GUI
```

### Project Structure

```
src/
├── api/        # GraphQL server and resolvers
├── db/         # Database handlers
├── indexer/    # Poller, WebSocket, Processor
├── parser/     # Anchor event decoder
├── config.ts   # Configuration
└── index.ts    # Entry point
```

## RPC Providers

| Provider | Polling | WebSocket | Notes |
|----------|---------|-----------|-------|
| Helius | ✅ | ✅ | Recommended (1M free credits) |
| QuickNode | ✅ | ✅ | Paid plans |
| Alchemy | ✅ | ✅ | Free tier available |
| Solana Public | ✅ | ✅ | Rate limited |

## License

MIT

## Related

- [8004-solana](https://github.com/QuantuLabs/8004-solana) - Solana program
- [ERC-8004](https://eips.ethereum.org/EIPS/eip-8004) - Specification
