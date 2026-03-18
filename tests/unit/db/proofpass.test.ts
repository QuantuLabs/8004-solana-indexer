import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";

vi.mock("dotenv/config", () => ({}));

const mockQuery = vi.fn();

vi.mock("../../../src/db/supabase.js", () => ({
  getPool: vi.fn(() => ({
    connect: vi.fn().mockResolvedValue({
      query: mockQuery,
      release: vi.fn(),
    }),
    query: mockQuery,
  })),
}));

const originalEnv = process.env;

describe("db/proofpass", () => {
  beforeEach(() => {
    vi.resetModules();
    process.env = { ...originalEnv };
    mockQuery.mockReset();
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  it("throws at runtime when ENABLE_PROOFPASS=true with DB_MODE=local", async () => {
    process.env.DB_MODE = "local";
    process.env.ENABLE_PROOFPASS = "true";

    const { upsertProofPassMatches } = await import("../../../src/db/proofpass.js");

    await expect(
      upsertProofPassMatches([
        {
          id: "asset:client:1:sig",
          asset: "asset",
          clientAddress: "client",
          feedbackIndex: "1",
          txSignature: "sig",
          blockSlot: "42",
          feedbackHash: "ab".repeat(32),
          proofpassSession: "cd".repeat(32),
          contextType: 1,
          contextRefHash: "ef".repeat(32),
        },
      ])
    ).rejects.toThrow("ENABLE_PROOFPASS requires DB_MODE=supabase");
  });

  it("fails fast when ProofPass schema is missing in PostgreSQL", async () => {
    process.env.DB_MODE = "supabase";
    process.env.ENABLE_PROOFPASS = "true";
    mockQuery.mockRejectedValueOnce(new Error('relation "extra_proofpass_feedbacks" does not exist'));

    const {
      assertProofPassSchema,
      MISSING_PROOFPASS_SCHEMA_FATAL_MESSAGE,
    } = await import("../../../src/db/proofpass.js");

    await expect(assertProofPassSchema()).rejects.toThrow(
      MISSING_PROOFPASS_SCHEMA_FATAL_MESSAGE
    );
  });

  it("fails fast when ProofPass table is readable but not writable", async () => {
    process.env.DB_MODE = "supabase";
    process.env.ENABLE_PROOFPASS = "true";
    mockQuery
      .mockResolvedValueOnce({ rows: [], rowCount: 0 })
      .mockResolvedValueOnce({ rows: [{ canWrite: false }], rowCount: 1 });

    const {
      assertProofPassSchema,
      MISSING_PROOFPASS_SCHEMA_WRITE_PERMISSIONS_FATAL_MESSAGE,
    } = await import("../../../src/db/proofpass.js");

    await expect(assertProofPassSchema()).rejects.toThrow(
      MISSING_PROOFPASS_SCHEMA_WRITE_PERMISSIONS_FATAL_MESSAGE
    );
  });

  it("fails fast when ProofPass conflict target uniqueness is missing", async () => {
    process.env.DB_MODE = "supabase";
    process.env.ENABLE_PROOFPASS = "true";
    mockQuery
      .mockResolvedValueOnce({ rows: [], rowCount: 0 })
      .mockResolvedValueOnce({ rows: [{ canWrite: true }], rowCount: 1 })
      .mockResolvedValueOnce({ rows: [{ hasConflictTarget: false }], rowCount: 1 });

    const {
      assertProofPassSchema,
      MISSING_PROOFPASS_SCHEMA_CONFLICT_TARGET_FATAL_MESSAGE,
    } = await import("../../../src/db/proofpass.js");

    await expect(assertProofPassSchema()).rejects.toThrow(
      MISSING_PROOFPASS_SCHEMA_CONFLICT_TARGET_FATAL_MESSAGE
    );
  });

  it("orders ProofPass backfill retries by oldest updated_at first", async () => {
    process.env.DB_MODE = "supabase";
    process.env.ENABLE_PROOFPASS = "true";
    mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 });

    const { listProofPassBackfillRetryTxs } = await import("../../../src/db/proofpass.js");

    await listProofPassBackfillRetryTxs(10);

    const sql = String(mockQuery.mock.calls[0]?.[0] ?? "");
    expect(sql).toContain("ORDER BY updated_at ASC, last_slot ASC");
  });
});
