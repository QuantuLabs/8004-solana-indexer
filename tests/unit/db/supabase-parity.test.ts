/**
 * Supabase handler parity tests
 * Ensures Supabase handlers write running_digest, revocations table,
 * and status columns consistently with Prisma handlers.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import {
  TEST_ASSET,
  TEST_CLIENT,
  TEST_OWNER,
  TEST_HASH,
  TEST_SIGNATURE,
  TEST_SLOT,
  TEST_BLOCK_TIME,
} from "../../mocks/solana.js";

// Track queries executed against the pool
let executedQueries: Array<{ text: string; values: any[] }> = [];
let feedbackExistsOverride = true;

// vi.hoisted() runs before vi.mock() factories — required for mock references
const { mockPoolInstance } = vi.hoisted(() => {
  const mockPoolInstance = {
    query: vi.fn(),
    on: vi.fn(),
    end: vi.fn(),
  };
  return { mockPoolInstance };
});

vi.mock("pg", () => {
  class MockPool {
    constructor() {
      return mockPoolInstance as any;
    }
  }
  return { Pool: MockPool };
});

vi.mock("../../../src/config.js", async (importOriginal) => {
  const original = (await importOriginal()) as any;
  return {
    ...original,
    config: {
      ...original.config,
      dbMode: "supabase",
      supabaseDsn: "POSTGRES_DSN_REDACTED",
      supabaseSslVerify: false,
    },
  };
});

// Use handleEvent (standalone path) — uses pool.query() directly, no connect/release
const { handleEvent } = await import("../../../src/db/supabase.js");

describe("Supabase Handler Parity", () => {
  let ctx: { signature: string; slot: bigint; blockTime: Date; txIndex?: number };

  beforeEach(() => {
    executedQueries = [];
    feedbackExistsOverride = true;
    ctx = {
      signature: TEST_SIGNATURE,
      slot: TEST_SLOT,
      blockTime: TEST_BLOCK_TIME,
      txIndex: 0,
    };

    // Reset and set up pool.query mock for each test
    mockPoolInstance.query.mockReset();
    mockPoolInstance.query.mockImplementation((text: string, values?: any[]) => {
      executedQueries.push({ text, values: values || [] });
      if (text.includes("SELECT") && text.includes("feedbacks")) {
        if (feedbackExistsOverride) {
          return {
            rows: [{ id: "test-id", feedback_hash: "ab".repeat(32) }],
            rowCount: 1,
          };
        }
        return { rows: [], rowCount: 0 };
      }
      if (text.includes("INSERT") || text.includes("UPDATE")) {
        return { rows: [], rowCount: 1 };
      }
      return { rows: [], rowCount: 0 };
    });
  });

  describe("NewFeedback", () => {
    const makeFeedbackEvent = () => ({
      type: "NewFeedback" as const,
      data: {
        asset: TEST_ASSET,
        clientAddress: TEST_CLIENT,
        feedbackIndex: 0n,
        value: 9500n,
        valueDecimals: 2,
        score: 85,
        tag1: "quality",
        tag2: "speed",
        endpoint: "/api/chat",
        feedbackUri: "ipfs://QmXXX",
        feedbackFileHash: null,
        sealHash: TEST_HASH,
        slot: 123456n,
        atomEnabled: true,
        newFeedbackDigest: TEST_HASH,
        newFeedbackCount: 1n,
        newTrustTier: 1,
        newQualityScore: 8500,
        newConfidence: 100,
        newRiskScore: 0,
        newDiversityRatio: 10000,
        isUniqueClient: true,
      },
    });

    it("should include running_digest and status in feedback INSERT", async () => {
      await handleEvent(makeFeedbackEvent(), ctx);

      const insertQuery = executedQueries.find(
        (q) => q.text.includes("INSERT INTO feedbacks") && q.text.includes("running_digest")
      );
      expect(insertQuery).toBeDefined();

      const expectedDigest = Buffer.from(TEST_HASH).toString("hex");
      expect(insertQuery!.values).toContain(expectedDigest);
      expect(insertQuery!.values).toContain("PENDING");
      expect(insertQuery!.text).toContain("feedback_id");
      expect(insertQuery!.values[1]).toBe("1");
    });
  });

  describe("FeedbackRevoked", () => {
    const makeRevokeEvent = () => ({
      type: "FeedbackRevoked" as const,
      data: {
        asset: TEST_ASSET,
        clientAddress: TEST_CLIENT,
        feedbackIndex: 0n,
        sealHash: TEST_HASH,
        slot: 123456n,
        originalScore: 85,
        atomEnabled: true,
        hadImpact: true,
        newTrustTier: 0,
        newQualityScore: 7000,
        newConfidence: 90,
        newRevokeDigest: TEST_HASH,
        newRevokeCount: 1n,
      },
    });

    it("should INSERT into revocations with running_digest and PENDING when feedback exists", async () => {
      await handleEvent(makeRevokeEvent(), ctx);

      const revokeInsert = executedQueries.find(
        (q) => q.text.includes("INSERT INTO revocations") && q.text.includes("running_digest")
      );
      expect(revokeInsert).toBeDefined();

      const expectedDigest = Buffer.from(TEST_HASH).toString("hex");
      expect(revokeInsert!.values).toContain(expectedDigest);
      expect(revokeInsert!.values).toContain("PENDING");
      expect(revokeInsert!.text).toContain("revocation_id");
      expect(revokeInsert!.values[1]).toBe("1");
    });

    it("should set ORPHANED status when feedback not found", async () => {
      feedbackExistsOverride = false;
      await handleEvent(makeRevokeEvent(), ctx);

      const revokeInsert = executedQueries.find((q) => q.text.includes("INSERT INTO revocations"));
      expect(revokeInsert).toBeDefined();
      expect(revokeInsert!.values).toContain("ORPHANED");
      expect(revokeInsert!.values[1]).toBeNull();
    });
  });

  describe("ResponseAppended", () => {
    const makeResponseEvent = () => ({
      type: "ResponseAppended" as const,
      data: {
        asset: TEST_ASSET,
        client: TEST_CLIENT,
        feedbackIndex: 0n,
        responder: TEST_OWNER,
        responseUri: "ipfs://QmYYY",
        responseHash: TEST_HASH,
        sealHash: TEST_HASH,
        slot: 123456n,
        newResponseDigest: TEST_HASH,
        newResponseCount: 1n,
      },
    });

    it("should include running_digest in response INSERT when feedback exists", async () => {
      await handleEvent(makeResponseEvent(), ctx);

      const insertQuery = executedQueries.find(
        (q) =>
          q.text.includes("INSERT INTO feedback_responses") && q.text.includes("running_digest")
      );
      expect(insertQuery).toBeDefined();

      const expectedDigest = Buffer.from(TEST_HASH).toString("hex");
      expect(insertQuery!.values).toContain(expectedDigest);
      expect(insertQuery!.text).toContain("response_id");
      expect(insertQuery!.values[1]).toBe("1");
    });

    it("should include running_digest and ORPHANED in orphan response INSERT", async () => {
      feedbackExistsOverride = false;
      await handleEvent(makeResponseEvent(), ctx);

      const insertQuery = executedQueries.find(
        (q) =>
          q.text.includes("INSERT INTO feedback_responses") && q.text.includes("running_digest")
      );
      expect(insertQuery).toBeDefined();
      expect(insertQuery!.values).toContain("ORPHANED");
      expect(insertQuery!.values[1]).toBeNull();
    });
  });
});
