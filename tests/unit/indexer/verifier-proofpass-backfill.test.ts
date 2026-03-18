import { describe, expect, it, vi, beforeEach } from "vitest";

const {
  mockGetProofPassBackfillCursor,
  mockListNextProofPassBackfillTxs,
  mockListMissingProofPassFeedbackCandidatesBySignature,
  mockListProofPassBackfillRetryTxs,
  mockDeleteProofPassBackfillRetryTx,
  mockSaveProofPassBackfillRetryTx,
  mockSaveProofPassBackfillCursor,
  mockUpsertProofPassMatches,
  mockMatchProofPassFeedbackCandidates,
} = vi.hoisted(() => ({
  mockGetProofPassBackfillCursor: vi.fn(),
  mockListNextProofPassBackfillTxs: vi.fn(),
  mockListMissingProofPassFeedbackCandidatesBySignature: vi.fn(),
  mockListProofPassBackfillRetryTxs: vi.fn(),
  mockDeleteProofPassBackfillRetryTx: vi.fn(),
  mockSaveProofPassBackfillRetryTx: vi.fn(),
  mockSaveProofPassBackfillCursor: vi.fn(),
  mockUpsertProofPassMatches: vi.fn(),
  mockMatchProofPassFeedbackCandidates: vi.fn(),
}));

vi.mock("../../../src/config.js", () => ({
  config: {
    enableProofPass: true,
    programId: "8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C",
    verifyBatchSize: 100,
    maxSupportedTransactionVersion: 0,
  },
}));

vi.mock("../../../src/logger.js", () => {
  const mockLogger = {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    fatal: vi.fn(),
    child: vi.fn(() => mockLogger),
  };
  return {
    createChildLogger: vi.fn(() => mockLogger),
    logger: mockLogger,
  };
});

vi.mock("../../../src/db/proofpass.js", () => ({
  deleteProofPassBackfillRetryTx: mockDeleteProofPassBackfillRetryTx,
  getProofPassBackfillCursor: mockGetProofPassBackfillCursor,
  listNextProofPassBackfillTxs: mockListNextProofPassBackfillTxs,
  listMissingProofPassFeedbackCandidatesBySignature: mockListMissingProofPassFeedbackCandidatesBySignature,
  listProofPassBackfillRetryTxs: mockListProofPassBackfillRetryTxs,
  saveProofPassBackfillRetryTx: mockSaveProofPassBackfillRetryTx,
  saveProofPassBackfillCursor: mockSaveProofPassBackfillCursor,
  upsertProofPassMatches: mockUpsertProofPassMatches,
}));

vi.mock("../../../src/extras/proofpass.js", () => ({
  matchProofPassFeedbackCandidates: mockMatchProofPassFeedbackCandidates,
}));

import { DataVerifier } from "../../../src/indexer/verifier.js";

describe("DataVerifier ProofPass backfill", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockGetProofPassBackfillCursor.mockResolvedValue({
      lastSlot: null,
      lastTxIndex: null,
      lastSignature: null,
    });
    mockListProofPassBackfillRetryTxs.mockResolvedValue([]);
    mockListMissingProofPassFeedbackCandidatesBySignature.mockResolvedValue([]);
    mockDeleteProofPassBackfillRetryTx.mockResolvedValue(undefined);
    mockSaveProofPassBackfillRetryTx.mockResolvedValue(undefined);
    mockUpsertProofPassMatches.mockResolvedValue(undefined);
    mockSaveProofPassBackfillCursor.mockResolvedValue(undefined);
    mockMatchProofPassFeedbackCandidates.mockReturnValue([]);
  });

  it("advances the ProofPass backfill cursor past transactions whose parsed payload is unavailable", async () => {
    mockListNextProofPassBackfillTxs.mockResolvedValue([
      { blockSlot: "10", txIndex: 0, txSignature: "sig-a" },
      { blockSlot: "11", txIndex: 0, txSignature: "sig-b" },
    ]);

    const connection = {
      getParsedTransaction: vi
        .fn()
        .mockResolvedValueOnce(null)
        .mockResolvedValueOnce({ meta: { logMessages: [] } }),
    } as any;

    const verifier = new DataVerifier(connection, null, {} as any);
    await (verifier as any).backfillMissingProofPassMatches();

    expect(connection.getParsedTransaction).toHaveBeenCalledTimes(2);
    expect(mockSaveProofPassBackfillCursor).toHaveBeenCalledWith({
      blockSlot: "11",
      txIndex: 0,
      txSignature: "sig-b",
    });
    expect(mockSaveProofPassBackfillRetryTx).toHaveBeenCalledWith({
      blockSlot: "10",
      txIndex: 0,
      txSignature: "sig-a",
    });
    expect(mockMatchProofPassFeedbackCandidates).toHaveBeenCalledTimes(1);
  });

  it("retries deferred ProofPass backfill transactions before fresh cursor work", async () => {
    mockListProofPassBackfillRetryTxs.mockResolvedValue([
      { blockSlot: "9", txIndex: 0, txSignature: "sig-retry" },
    ]);
    mockListNextProofPassBackfillTxs.mockResolvedValue([]);
    mockListMissingProofPassFeedbackCandidatesBySignature.mockResolvedValue([
      {
        asset: "asset",
        clientAddress: "client",
        feedbackIndex: "0",
        txSignature: "sig-retry",
        blockSlot: "9",
        feedbackHash: "ab".repeat(32),
      },
    ]);
    mockMatchProofPassFeedbackCandidates.mockReturnValue([
      {
        id: "match-1",
        asset: "asset",
        clientAddress: "client",
        feedbackIndex: "0",
        txSignature: "sig-retry",
        feedbackHash: "ab".repeat(32),
        proofpassSession: "cd".repeat(32),
        contextType: "request",
        contextRefHash: "ef".repeat(32),
        blockSlot: "9",
      },
    ]);

    const connection = {
      getParsedTransaction: vi.fn().mockResolvedValue({ meta: { logMessages: [] } }),
    } as any;

    const verifier = new DataVerifier(connection, null, {} as any);
    await (verifier as any).backfillMissingProofPassMatches();

    expect(mockUpsertProofPassMatches).toHaveBeenCalledTimes(1);
    expect(mockDeleteProofPassBackfillRetryTx).toHaveBeenCalledWith({
      blockSlot: "9",
      txIndex: 0,
      txSignature: "sig-retry",
    });
    expect(mockSaveProofPassBackfillCursor).not.toHaveBeenCalled();
  });

  it("fails closed when ProofPass matching throws during backfill", async () => {
    mockListNextProofPassBackfillTxs.mockResolvedValue([
      { blockSlot: "10", txIndex: 0, txSignature: "sig-throw" },
    ]);
    mockListMissingProofPassFeedbackCandidatesBySignature.mockResolvedValue([
      {
        asset: "asset",
        clientAddress: "client",
        feedbackIndex: "0",
        txSignature: "sig-throw",
        blockSlot: "10",
        feedbackHash: "ab".repeat(32),
      },
    ]);
    mockMatchProofPassFeedbackCandidates.mockImplementation(() => {
      throw new Error("proofpass match failed");
    });

    const connection = {
      getParsedTransaction: vi.fn().mockResolvedValue({ meta: { logMessages: [] } }),
    } as any;

    const verifier = new DataVerifier(connection, null, {} as any);
    await expect((verifier as any).backfillMissingProofPassMatches()).rejects.toThrow(
      "proofpass match failed"
    );

    expect(mockSaveProofPassBackfillCursor).not.toHaveBeenCalled();
    expect(mockSaveProofPassBackfillRetryTx).not.toHaveBeenCalled();
    expect(mockDeleteProofPassBackfillRetryTx).not.toHaveBeenCalled();
    expect(mockUpsertProofPassMatches).not.toHaveBeenCalled();
  });

  it("fails closed when ProofPass backfill upsert fails", async () => {
    mockListNextProofPassBackfillTxs.mockResolvedValue([
      { blockSlot: "10", txIndex: 0, txSignature: "sig-upsert" },
    ]);
    mockListMissingProofPassFeedbackCandidatesBySignature.mockResolvedValue([
      {
        asset: "asset",
        clientAddress: "client",
        feedbackIndex: "0",
        txSignature: "sig-upsert",
        blockSlot: "10",
        feedbackHash: "ab".repeat(32),
      },
    ]);
    mockMatchProofPassFeedbackCandidates.mockReturnValue([
      {
        id: "match-upsert",
        asset: "asset",
        clientAddress: "client",
        feedbackIndex: "0",
        txSignature: "sig-upsert",
        feedbackHash: "ab".repeat(32),
        proofpassSession: "cd".repeat(32),
        contextType: "request",
        contextRefHash: "ef".repeat(32),
        blockSlot: "10",
      },
    ]);
    mockUpsertProofPassMatches.mockRejectedValueOnce(new Error("proofpass upsert failed"));

    const connection = {
      getParsedTransaction: vi.fn().mockResolvedValue({ meta: { logMessages: [] } }),
    } as any;

    const verifier = new DataVerifier(connection, null, {} as any);
    await expect((verifier as any).backfillMissingProofPassMatches()).rejects.toThrow(
      "proofpass upsert failed"
    );

    expect(mockUpsertProofPassMatches).toHaveBeenCalledTimes(1);
    expect(mockSaveProofPassBackfillCursor).not.toHaveBeenCalled();
    expect(mockSaveProofPassBackfillRetryTx).not.toHaveBeenCalled();
    expect(mockDeleteProofPassBackfillRetryTx).not.toHaveBeenCalled();
  });
});
