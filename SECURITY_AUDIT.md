# Security Audit Report - 8004-solana-indexer

**Audit Date:** 2026-01-10
**Auditor:** Claude Opus 4.5
**Version:** 1.0.0

---

## Executive Summary

This security audit evaluates the 8004-solana-indexer, a self-hosted Solana indexer for the 8004 Agent Registry program. The system indexes on-chain events and exposes data via a GraphQL API.

**Overall Risk Level:** LOW-MEDIUM

The codebase follows good security practices with Prisma ORM preventing SQL injection, proper input validation through TypeScript types, and separation of concerns. However, some improvements are recommended.

---

## Findings

### 1. CORS Configuration - Overly Permissive (MEDIUM)

**File:** `src/api/server.ts:88-91`

```typescript
cors: {
  origin: "*",
  methods: ["GET", "POST", "OPTIONS"],
},
```

**Risk:** The wildcard `*` allows any origin to make requests to the API. In production, this could enable:
- Cross-site request forgery attacks
- Data scraping from malicious sites

**Recommendation:** Restrict CORS to specific trusted origins:
```typescript
cors: {
  origin: process.env.ALLOWED_ORIGINS?.split(",") || ["http://localhost:3000"],
  methods: ["GET", "POST", "OPTIONS"],
},
```

---

### 2. No Rate Limiting (MEDIUM)

**Location:** GraphQL server

**Risk:** The API has no rate limiting, making it vulnerable to:
- Denial of Service (DoS) attacks
- Resource exhaustion via expensive queries
- Brute-force attacks on data scraping

**Recommendation:** Add rate limiting using a package like `graphql-rate-limit`:
```typescript
// Add to GraphQL Yoga configuration
import { createRateLimiter } from "graphql-rate-limit";

const rateLimiter = createRateLimiter({
  identifyContext: (ctx) => ctx.request.ip,
  windowMs: 60000, // 1 minute
  max: 100, // 100 requests per minute
});
```

---

### 3. Query Depth Not Limited (MEDIUM)

**Location:** GraphQL schema allows deep nesting

**Risk:** Malicious actors can craft deeply nested queries that consume excessive resources:
```graphql
{
  agents {
    feedbacks {
      agent {
        feedbacks {
          agent { ... }
        }
      }
    }
  }
}
```

**Recommendation:** Add query depth limiting:
```typescript
import { depthLimitRule } from "@escape.tech/graphql-armor";

const yoga = createYoga({
  plugins: [
    depthLimitRule({ maxDepth: 5 }),
  ],
});
```

---

### 4. Pagination Limits Not Enforced Server-Side (LOW)

**File:** `src/api/resolvers.ts`

```typescript
take: args.limit || 50,
```

**Risk:** Users can request arbitrarily large result sets by setting high limit values, potentially causing:
- Memory exhaustion
- Slow queries

**Recommendation:** Add maximum limit enforcement:
```typescript
const MAX_LIMIT = 100;
take: Math.min(args.limit || 50, MAX_LIMIT),
```

---

### 5. No Authentication/Authorization (LOW - INFORMATIONAL)

**Location:** Entire API

**Risk:** The API is publicly accessible without authentication.

**Context:** For a read-only blockchain indexer, this is often acceptable since all indexed data is public on-chain.

**Recommendation (if needed):** Add API key authentication for production use:
```typescript
// Check API key in context
context: ({ request }) => {
  const apiKey = request.headers.get("x-api-key");
  if (process.env.REQUIRE_API_KEY && apiKey !== process.env.API_KEY) {
    throw new Error("Unauthorized");
  }
  return { prisma, processor };
},
```

---

### 6. Development Credentials in .env.example (LOW)

**File:** `.env.example`

```
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/indexer8004?schema=public"
```

**Risk:** Default credentials could accidentally be used in production.

**Recommendation:** Use placeholder values:
```
DATABASE_URL="postgresql://user:password@host:5432/database?schema=public"
```

---

### 7. Dependency Vulnerabilities (LOW - DEV ONLY)

**Source:** `npm audit`

```
6 moderate severity vulnerabilities in esbuild (development dependency)
```

**Risk:** These are in development dependencies (vitest/vite) and don't affect production runtime.

**Recommendation:** Update vitest when a patched version is available:
```bash
npm update vitest
```

---

### 8. GraphiQL Enabled in Production (LOW)

**File:** `src/api/server.ts:32`

**Risk:** GraphiQL interface exposes schema introspection and could aid attackers.

**Recommendation:** Disable in production:
```typescript
graphiql: process.env.NODE_ENV !== "production",
```

---

### 9. Error Details in Logs (LOW)

**File:** `src/parser/decoder.ts:253`

```typescript
logger.error({ error, event }, "Failed to convert event to typed event");
```

**Risk:** Stack traces and raw event data could leak sensitive information in logs.

**Recommendation:** Sanitize log output in production:
```typescript
logger.error(
  {
    error: error instanceof Error ? error.message : "Unknown error",
    eventType: event?.type
  },
  "Failed to convert event"
);
```

---

## Positive Security Practices

### SQL Injection Prevention
- **Prisma ORM** is used throughout, providing parameterized queries
- No raw SQL queries found in codebase

### Input Validation
- TypeScript types enforce structure
- GraphQL schema provides type validation
- Enums used for order-by parameters

### Secure Data Handling
- Public keys validated via Solana SDK's `PublicKey` class
- Hashes stored as binary (Bytes) not strings
- BigInt used for large numbers (slot, feedbackIndex)

### Secrets Management
- Environment variables used for configuration
- `.env` file in `.gitignore`
- No hardcoded secrets in source code

### Error Handling
- Try-catch blocks around critical operations
- Graceful degradation on WebSocket failures
- Event processing errors logged but don't crash indexer

### Data Integrity
- Database constraints (unique, indexes)
- On-chain events are the source of truth
- Upsert operations prevent duplicates

---

## Recommendations Priority Matrix

| Priority | Finding | Effort |
|----------|---------|--------|
| HIGH | Add rate limiting | Medium |
| HIGH | Add query depth limiting | Low |
| MEDIUM | Restrict CORS origins | Low |
| MEDIUM | Enforce max pagination limit | Low |
| LOW | Disable GraphiQL in production | Low |
| LOW | Update dev dependencies | Low |

---

## Compliance Notes

### OWASP Top 10 Coverage

| Risk | Status | Notes |
|------|--------|-------|
| Injection | PASS | Prisma ORM prevents SQL injection |
| Broken Auth | N/A | Read-only public data |
| Sensitive Data | PASS | No PII stored, blockchain data only |
| XXE | N/A | No XML processing |
| Broken Access | PASS | All data is public |
| Security Misconfig | WARN | CORS too permissive |
| XSS | PASS | GraphQL returns JSON only |
| Insecure Deserialization | PASS | Standard JSON parsing |
| Vulnerable Components | WARN | Dev deps need update |
| Insufficient Logging | PASS | Pino logging configured |

---

## Conclusion

The 8004-solana-indexer demonstrates good security fundamentals for a blockchain indexer. The main areas for improvement are:

1. **DoS Protection**: Add rate limiting and query depth limits
2. **Production Hardening**: Restrict CORS, disable GraphiQL
3. **Resource Limits**: Enforce maximum pagination

These improvements can be implemented with minimal code changes and would bring the security posture to a production-ready level.

---

*This audit was performed through static code analysis. A full security assessment should include dynamic testing and penetration testing.*
