# 8004-solana-indexer

A lightweight, self-hosted Solana indexer for the [8004 Agent Registry](https://github.com/QuantuLabs/8004-solana) program with GraphQL API.

## Features

- **Dual-mode indexing**: WebSocket (real-time) + polling (fallback)
- **14 Anchor event types** indexed (Identity, Reputation, Validation)
- **GraphQL API** with built-in GraphiQL explorer
- **Works with any Solana RPC** (Helius, QuickNode, public devnet)
- **Self-hosted**: PostgreSQL + Node.js, no external dependencies

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     8004-solana-indexer                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   Indexer    │───▶│  PostgreSQL  │◀───│  GraphQL API │  │
│  │  (Node.js)   │    │   (Prisma)   │    │    (Yoga)    │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│         │                                       │           │
│         ▼                                       ▼           │
│  ┌──────────────┐                      ┌──────────────┐    │
│  │  Any Solana  │                      │   Clients    │    │
│  │     RPC      │                      │  (Web/Apps)  │    │
│  └──────────────┘                      └──────────────┘    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Node.js 20+
- PostgreSQL 14+
- A Solana RPC endpoint (devnet/mainnet)

### Installation

```bash
# Clone the repository
git clone https://github.com/QuantuLabs/8004-solana-indexer.git
cd 8004-solana-indexer

# Install dependencies
npm install

# Generate Prisma client
npm run db:generate
```

### Configuration

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://postgres:postgres@localhost:5432/indexer8004` |
| `RPC_URL` | Solana RPC HTTP endpoint | `https://api.devnet.solana.com` |
| `WS_URL` | Solana RPC WebSocket endpoint | `wss://api.devnet.solana.com` |
| `PROGRAM_ID` | 8004 Agent Registry program ID | `3GGkAWC3mYYdud8GVBsKXK5QC9siXtFkWVZFYtbueVbC` |
| `INDEXER_MODE` | `auto`, `polling`, or `websocket` | `auto` |
| `GRAPHQL_PORT` | GraphQL server port | `4000` |

### Database Setup

```bash
# Run migrations
npm run db:migrate

# Or push schema directly (development)
npm run db:push
```

### Running

```bash
# Development (with hot reload)
npm run dev

# Production
npm run build
npm start
```

The GraphQL API will be available at `http://localhost:4000/graphql`

## GraphQL API

### Example Queries

```graphql
# Get all agents with their feedback stats
query GetAgents {
  agents(limit: 10, orderBy: CREATED_AT_DESC) {
    id
    owner
    nftName
    uri
    feedbackCount
    averageScore
  }
}

# Get agent with feedbacks and validations
query GetAgent($id: ID!) {
  agent(id: $id) {
    id
    owner
    nftName
    metadata {
      key
      value
    }
    feedbacks(limit: 5) {
      score
      tag1
      tag2
      client
      responses {
        responder
        responseUri
      }
    }
    validations(pending: true) {
      validator
      nonce
      requestUri
    }
  }
}

# Get indexer status
query GetStatus {
  stats {
    totalAgents
    totalFeedbacks
    totalValidations
    lastProcessedSignature
  }
  indexerStatus {
    running
    mode
    pollerActive
    wsActive
  }
}

# Search agents
query SearchAgents {
  searchAgents(query: "my-agent", limit: 5) {
    id
    nftName
    owner
  }
}
```

### Available Queries

| Query | Description |
|-------|-------------|
| `agent(id)` | Get single agent by ID |
| `agents(owner, collection, registry, limit, offset, orderBy)` | List agents with filters |
| `feedback(id)` | Get single feedback |
| `feedbacks(agentId, client, minScore, maxScore, tag, revoked)` | List feedbacks |
| `validation(id)` | Get single validation |
| `validations(agentId, validator, requester, pending)` | List validations |
| `registries(registryType, authority)` | List registries |
| `stats` | Indexer statistics |
| `indexerStatus` | Indexer health status |
| `searchAgents(query, limit)` | Search agents by ID, owner, or name |

## Indexed Events

### Identity Events
| Event | Description |
|-------|-------------|
| `AgentRegisteredInRegistry` | New agent created |
| `AgentOwnerSynced` | Agent ownership changed |
| `UriUpdated` | Agent URI updated |
| `WalletUpdated` | Agent wallet updated |
| `MetadataSet` | Metadata key/value set |
| `MetadataDeleted` | Metadata key deleted |
| `BaseRegistryCreated` | Base registry created |
| `UserRegistryCreated` | User registry created |
| `BaseRegistryRotated` | Base registry rotated |

### Reputation Events
| Event | Description |
|-------|-------------|
| `NewFeedback` | Client submitted feedback |
| `FeedbackRevoked` | Feedback was revoked |
| `ResponseAppended` | Agent responded to feedback |

### Validation Events
| Event | Description |
|-------|-------------|
| `ValidationRequested` | Validation request created |
| `ValidationResponded` | Validator submitted response |

## Development

### Project Structure

```
├── src/
│   ├── api/           # GraphQL server and resolvers
│   ├── db/            # Database handlers
│   ├── indexer/       # Poller, WebSocket, Processor
│   ├── parser/        # Anchor event decoder
│   ├── config.ts      # Configuration
│   ├── logger.ts      # Pino logger
│   └── index.ts       # Entry point
├── prisma/
│   └── schema.prisma  # Database schema
├── idl/
│   └── agent_registry_8004.json  # Anchor IDL
└── tests/
    ├── unit/          # Unit tests
    ├── mocks/         # Test mocks
    └── e2e/           # End-to-end tests
```

### Testing

```bash
# Run unit tests
npm test

# Run tests with coverage
npm run test:coverage

# Run e2e tests (requires database)
npm run test:e2e

# Run all tests
npm run test:all
```

### Database Management

```bash
# Open Prisma Studio (GUI)
npm run db:studio

# Create migration
npm run db:migrate

# Reset database
npx prisma migrate reset
```

## Docker

```bash
# Start PostgreSQL only
docker-compose up -d postgres

# Start full stack
docker-compose up -d
```

## RPC Provider Compatibility

| Provider | Polling | WebSocket | Notes |
|----------|---------|-----------|-------|
| Helius | ✅ | ✅ | Recommended (1M free credits) |
| QuickNode | ✅ | ✅ | Paid plans |
| Triton | ✅ | ✅ | High performance |
| Alchemy | ✅ | ✅ | Free tier available |
| Solana Public | ✅ | ✅ | Rate limited |

## Tech Stack

| Component | Technology |
|-----------|------------|
| Runtime | Node.js 20+ |
| Language | TypeScript |
| Database | PostgreSQL |
| ORM | Prisma |
| GraphQL | GraphQL Yoga |
| Parser | @coral-xyz/anchor |
| Logging | Pino |

## License

MIT

## Related

- [8004-solana](https://github.com/QuantuLabs/8004-solana) - Solana program implementation
- [ERC-8004](https://eips.ethereum.org/EIPS/eip-8004) - AI Agent Identity & Reputation Registry specification
