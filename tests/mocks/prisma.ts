import { vi } from "vitest";
import type { PrismaClient } from "@prisma/client";

export function createMockPrismaClient(): PrismaClient {
  const scopedCounters = new Map<string, bigint>();

  const mockClient = {
    $connect: vi.fn().mockResolvedValue(undefined),
    $disconnect: vi.fn().mockResolvedValue(undefined),
    // $transaction passes the same mock client to the callback
    // This allows testing atomic operations with the same mocks
    $transaction: vi.fn().mockImplementation(async (fn: (tx: any) => Promise<any>) => {
      return fn(mockClient);
    }),
    $executeRawUnsafe: vi.fn().mockResolvedValue(0),
    $queryRawUnsafe: vi.fn().mockImplementation(async (query: string, ...values: unknown[]) => {
      if (query.includes('INSERT INTO "IdCounter"')) {
        const scope = String(values[0] ?? "");
        const current = scopedCounters.get(scope) ?? 1n;
        scopedCounters.set(scope, current + 1n);
        return [{ allocated: current }];
      }
      if (query.includes('SELECT COALESCE(MAX("agent_id"), 0) AS max_id FROM "Agent"')) {
        return [{ max_id: 0n }];
      }
      if (query.includes('SELECT COALESCE(MAX("collection_id"), 0) AS max_id FROM "CollectionPointer"')) {
        return [{ max_id: 0n }];
      }
      return [];
    }),
    agent: {
      findUnique: vi.fn(),
      findMany: vi.fn().mockResolvedValue([]),
      create: vi.fn(),
      update: vi.fn(),
      updateMany: vi.fn().mockResolvedValue({ count: 1 }),
      upsert: vi.fn(),
      delete: vi.fn(),
      count: vi.fn().mockResolvedValue(0),
      aggregate: vi.fn().mockResolvedValue({ _avg: { score: null } }),
    },
    agentMetadata: {
      findUnique: vi.fn(),
      findMany: vi.fn().mockResolvedValue([]),
      create: vi.fn(),
      update: vi.fn(),
      upsert: vi.fn(),
      delete: vi.fn(),
      deleteMany: vi.fn(),
    },
    feedback: {
      findUnique: vi.fn(),
      findMany: vi.fn().mockResolvedValue([]),
      create: vi.fn(),
      update: vi.fn(),
      upsert: vi.fn(),
      updateMany: vi.fn(),
      count: vi.fn().mockResolvedValue(0),
      aggregate: vi.fn().mockResolvedValue({ _avg: { score: null } }),
    },
    feedbackResponse: {
      findUnique: vi.fn(),
      findMany: vi.fn().mockResolvedValue([]),
      create: vi.fn(),
      upsert: vi.fn(),
    },
    validation: {
      findUnique: vi.fn(),
      findMany: vi.fn().mockResolvedValue([]),
      create: vi.fn(),
      upsert: vi.fn(),
      updateMany: vi.fn(),
      count: vi.fn().mockResolvedValue(0),
    },
    registry: {
      findUnique: vi.fn(),
      findMany: vi.fn().mockResolvedValue([]),
      upsert: vi.fn(),
      count: vi.fn().mockResolvedValue(0),
    },
    collection: {
      findUnique: vi.fn(),
      findMany: vi.fn().mockResolvedValue([]),
      upsert: vi.fn(),
      update: vi.fn(),
      updateMany: vi.fn().mockResolvedValue({ count: 1 }),
      count: vi.fn().mockResolvedValue(0),
    },
    indexerState: {
      findUnique: vi.fn(),
      upsert: vi.fn(),
    },
    eventLog: {
      create: vi.fn(),
      findMany: vi.fn().mockResolvedValue([]),
    },
    revocation: {
      findUnique: vi.fn(),
      findMany: vi.fn().mockResolvedValue([]),
      create: vi.fn(),
      upsert: vi.fn(),
      count: vi.fn().mockResolvedValue(0),
    },
    orphanResponse: {
      findMany: vi.fn().mockResolvedValue([]),
      create: vi.fn(),
      upsert: vi.fn(),
      delete: vi.fn(),
      deleteMany: vi.fn().mockResolvedValue({ count: 0 }),
    },
    orphanFeedback: {
      findMany: vi.fn().mockResolvedValue([]),
      create: vi.fn(),
      upsert: vi.fn(),
      delete: vi.fn(),
      deleteMany: vi.fn().mockResolvedValue({ count: 0 }),
    },
    indexerCursor: {
      findUnique: vi.fn(),
      upsert: vi.fn(),
    },
    hashChainCheckpoint: {
      findFirst: vi.fn(),
      findMany: vi.fn().mockResolvedValue([]),
      create: vi.fn(),
      upsert: vi.fn(),
      count: vi.fn().mockResolvedValue(0),
    },
  };
  return mockClient as unknown as PrismaClient;
}

export function resetMockPrisma(prisma: PrismaClient): void {
  const mockPrisma = prisma as unknown as Record<string, Record<string, ReturnType<typeof vi.fn>>>;

  for (const model of Object.keys(mockPrisma)) {
    if (typeof mockPrisma[model] === "object" && mockPrisma[model] !== null) {
      for (const method of Object.keys(mockPrisma[model])) {
        if (typeof mockPrisma[model][method]?.mockClear === "function") {
          mockPrisma[model][method].mockClear();
        }
      }
    }
  }
}
