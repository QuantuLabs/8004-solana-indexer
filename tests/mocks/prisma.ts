import { vi } from "vitest";
import type { PrismaClient } from "@prisma/client";

export function createMockPrismaClient(): PrismaClient {
  return {
    $connect: vi.fn().mockResolvedValue(undefined),
    $disconnect: vi.fn().mockResolvedValue(undefined),
    agent: {
      findUnique: vi.fn(),
      findMany: vi.fn().mockResolvedValue([]),
      create: vi.fn(),
      update: vi.fn(),
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
    indexerState: {
      findUnique: vi.fn(),
      upsert: vi.fn(),
    },
    eventLog: {
      create: vi.fn(),
      findMany: vi.fn().mockResolvedValue([]),
    },
  } as unknown as PrismaClient;
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
