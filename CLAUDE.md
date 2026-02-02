# CLAUDE.md - 8004 Solana Indexer

## Overview

Solana indexer for the 8004 Agent Registry. Indexes agents, feedbacks, and metadata from on-chain events to Supabase PostgreSQL.

## Deployment

**Railway Project:** hopeful-love
**Service Name:** 8004-indexer

```bash
# Link Railway (interactive required)
railway link
# Select: MonteCrypto's Projects → hopeful-love → 8004-indexer

# View logs
railway logs

# Redeploy
railway redeploy
```

## Database

**Supabase PostgreSQL** (Railway reads from env vars):
- `SUPABASE_URL` - Supabase project URL
- `SUPABASE_KEY` - Service role key for writes
- `SUPABASE_DSN` - Direct PostgreSQL connection string

## Key Files

- `src/index.ts` - Entry point, fetches base collection from on-chain via SDK
- `src/config.ts` - Configuration including `runtimeConfig.baseCollection`
- `src/indexer/processor.ts` - Main indexing logic
- `src/db/supabase-handlers.ts` - Supabase write operations

## SDK Dependency

Uses `8004-solana` npm package for:
- `getBaseCollection(connection)` - Get correct collection from RootConfig → RegistryConfig

## Commands

```bash
npm run dev      # Local development
npm run build    # TypeScript compilation
npm start        # Production (Railway uses this)
```
