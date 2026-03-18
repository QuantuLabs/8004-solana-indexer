import { describe, expect, it } from "vitest";
import { PublicKey } from "@solana/web3.js";

import {
  matchProofPassFeedbackCandidates,
  matchProofPassFeedbacks,
  parseProofPassFinalizeLogs,
  proofPassLookupKey,
} from "../../../src/extras/proofpass.js";

describe("ProofPass extra", () => {
  const proofPassProgramId = "72WFGnAp9EjPok7JbadCEC1j83TZe3ti8k6ax25dQVzG";
  const otherProgramId = "ComputeBudget111111111111111111111111111111";
  const asset = new PublicKey(Buffer.alloc(32, 7));
  const client = new PublicKey(Buffer.alloc(32, 8));
  const sessionHex = Buffer.alloc(32, 0x44).toString("hex");
  const contextRefHash = Buffer.alloc(32, 0x55).toString("hex");
  const sealHash = Buffer.alloc(32, 0x66).toString("hex");
  const finalizeLog =
    `Program log: PP_FINALIZE|v=1|session=${sessionHex}` +
    `|client=${client.toBuffer().toString("hex")}` +
    `|asset=${asset.toBuffer().toString("hex")}` +
    `|feedback_index=7|context=4|context_ref=${contextRefHash}|seal=${sealHash}`;
  const proofPassScopedLogs = [
    `Program ${proofPassProgramId} invoke [1]`,
    finalizeLog,
    `Program ${proofPassProgramId} success`,
  ];

  it("parses the canonical PP_FINALIZE log", () => {
    const rows = parseProofPassFinalizeLogs(proofPassScopedLogs);

    expect(rows).toHaveLength(1);
    expect(rows[0]).toEqual({
      sessionHex,
      clientAddress: client.toBase58(),
      asset: asset.toBase58(),
      feedbackIndex: "7",
      contextType: 4,
      contextRefHash,
      sealHash,
    });
  });

  it("matches a PP_FINALIZE log to the exact 8004 feedback tuple", () => {
    const matches = matchProofPassFeedbacks({
      logs: proofPassScopedLogs,
      signature: "sig-123",
      slot: 999n,
      feedbackEvents: [
        {
          asset,
          clientAddress: client,
          feedbackIndex: 7n,
          sealHash: Buffer.from(sealHash, "hex"),
        } as any,
      ],
    });

    expect(matches).toHaveLength(1);
    expect(matches[0]).toEqual({
      id: `${asset.toBase58()}:${client.toBase58()}:7:sig-123`,
      asset: asset.toBase58(),
      clientAddress: client.toBase58(),
      feedbackIndex: "7",
      txSignature: "sig-123",
      blockSlot: "999",
      feedbackHash: sealHash,
      proofpassSession: sessionHex,
      contextType: 4,
      contextRefHash,
    });
  });

  it("matches already-indexed feedback rows for bounded historical backfill", () => {
    const matches = matchProofPassFeedbackCandidates({
      logs: proofPassScopedLogs,
      signature: "sig-rows",
      slot: 1234n,
      feedbacks: [
        {
          asset: asset.toBase58(),
          clientAddress: client.toBase58(),
          feedbackIndex: "7",
          txSignature: "sig-rows",
          blockSlot: "1234",
          feedbackHash: sealHash,
        },
      ],
    });

    expect(matches).toHaveLength(1);
    expect(matches[0]?.txSignature).toBe("sig-rows");
    expect(matches[0]?.proofpassSession).toBe(sessionHex);
  });

  it("throws when the seal hash differs inside a ProofPass-framed finalize", () => {
    expect(() =>
      matchProofPassFeedbacks({
        logs: proofPassScopedLogs,
        signature: "sig-123",
        slot: 999n,
        feedbackEvents: [
          {
            asset,
            clientAddress: client,
            feedbackIndex: 7n,
            sealHash: Buffer.alloc(32, 0x77),
          } as any,
        ],
      })
    ).toThrow("ProofPass transaction sig-123 has unmatched PP_FINALIZE logs; refusing silent downgrade");
  });

  it("throws when a ProofPass-framed tx emits feedback without a parseable PP_FINALIZE log", () => {
    expect(() =>
      matchProofPassFeedbacks({
        logs: [
          `Program ${proofPassProgramId} invoke [1]`,
          `Program log: PP_FINALIZE|v=1|session=oops|client=oops`,
          `Program ${proofPassProgramId} success`,
        ],
        signature: "sig-drift",
        slot: 999n,
        feedbackEvents: [
          {
            asset,
            clientAddress: client,
            feedbackIndex: 7n,
            sealHash: Buffer.from(sealHash, "hex"),
          } as any,
        ],
      })
    ).toThrow("ProofPass transaction sig-drift emitted feedback without a parseable PP_FINALIZE log");
  });

  it("ignores mixed txs that invoke ProofPass without emitting PP_FINALIZE", () => {
    const matches = matchProofPassFeedbacks({
      logs: [
        `Program ${proofPassProgramId} invoke [1]`,
        "Program log: PP_OPEN|session=deadbeef",
        `Program ${proofPassProgramId} success`,
      ],
      signature: "sig-mixed-nonfinalize",
      slot: 999n,
      feedbackEvents: [
        {
          asset,
          clientAddress: client,
          feedbackIndex: 7n,
          sealHash: Buffer.from(sealHash, "hex"),
        } as any,
      ],
    });

    expect(matches).toEqual([]);
  });

  it("throws when parseable PP_FINALIZE logs cannot be matched back to the 8004 feedback tuple", () => {
    expect(() =>
      matchProofPassFeedbacks({
        logs: proofPassScopedLogs,
        signature: "sig-mismatch",
        slot: 999n,
        feedbackEvents: [
          {
            asset,
            clientAddress: client,
            feedbackIndex: 999n,
            sealHash: Buffer.from(sealHash, "hex"),
          } as any,
        ],
      })
    ).toThrow("ProofPass transaction sig-mismatch has unmatched PP_FINALIZE logs; refusing silent downgrade");
  });

  it("ignores spoofed PP_FINALIZE logs emitted outside the ProofPass program frame", () => {
    const rows = parseProofPassFinalizeLogs([
      `Program ${otherProgramId} invoke [1]`,
      finalizeLog,
      `Program ${otherProgramId} success`,
    ]);

    expect(rows).toEqual([]);
  });

  it("accepts PP_FINALIZE after nested CPI returns to the ProofPass frame", () => {
    const rows = parseProofPassFinalizeLogs([
      `Program ${proofPassProgramId} invoke [1]`,
      `Program ${otherProgramId} invoke [2]`,
      `Program ${otherProgramId} success`,
      finalizeLog,
      `Program ${proofPassProgramId} success`,
    ]);

    expect(rows).toHaveLength(1);
    expect(rows[0]?.feedbackIndex).toBe("7");
  });

  it("builds a stable lookup key for GraphQL badge resolution", () => {
    expect(
      proofPassLookupKey(asset.toBase58(), client.toBase58(), "7", "sig-123", sealHash)
    ).toBe(`${asset.toBase58()}:${client.toBase58()}:7:sig-123:${sealHash}`);
  });

  it("deduplicates replayed PP_FINALIZE rows and ignores malformed or out-of-frame noise", () => {
    const malformedFinalizeLog =
      `Program log: PP_FINALIZE|v=1|session=${Buffer.alloc(32, 0x99).toString("hex")}` +
      `|client=${client.toBuffer().toString("hex")}` +
      `|asset=${asset.toBuffer().toString("hex")}` +
      `|feedback_index=oops|context=4|context_ref=${contextRefHash}|seal=${sealHash}`;
    const spoofedOutOfFrameLog =
      `Program log: PP_FINALIZE|v=1|session=${Buffer.alloc(32, 0xaa).toString("hex")}` +
      `|client=${client.toBuffer().toString("hex")}` +
      `|asset=${asset.toBuffer().toString("hex")}` +
      `|feedback_index=7|context=9|context_ref=${contextRefHash}|seal=${sealHash}`;

    const logs = [
      `Program ${proofPassProgramId} invoke [1]`,
      finalizeLog,
      malformedFinalizeLog,
      finalizeLog,
      `Program ${otherProgramId} invoke [2]`,
      spoofedOutOfFrameLog,
      `Program ${otherProgramId} success`,
      `Program ${proofPassProgramId} success`,
    ];

    expect(parseProofPassFinalizeLogs(logs)).toHaveLength(2);

    const matches = matchProofPassFeedbacks({
      logs,
      signature: "sig-replay",
      slot: 1000n,
      feedbackEvents: [
        {
          asset,
          clientAddress: client,
          feedbackIndex: 7n,
          sealHash: Buffer.from(sealHash, "hex"),
        } as any,
      ],
    });

    expect(matches).toHaveLength(1);
    expect(matches[0]).toEqual({
      id: `${asset.toBase58()}:${client.toBase58()}:7:sig-replay`,
      asset: asset.toBase58(),
      clientAddress: client.toBase58(),
      feedbackIndex: "7",
      txSignature: "sig-replay",
      blockSlot: "1000",
      feedbackHash: sealHash,
      proofpassSession: sessionHex,
      contextType: 4,
      contextRefHash,
    });
  });

  it("matches multiple ProofPass finalizations in one tx while ignoring nested noise", () => {
    const secondSessionHex = Buffer.alloc(32, 0x45).toString("hex");
    const secondSealHash = Buffer.alloc(32, 0x67).toString("hex");
    const secondFinalizeLog =
      `Program log: PP_FINALIZE|v=1|session=${secondSessionHex}` +
      `|client=${client.toBuffer().toString("hex")}` +
      `|asset=${asset.toBuffer().toString("hex")}` +
      `|feedback_index=8|context=4|context_ref=${contextRefHash}|seal=${secondSealHash}`;

    const matches = matchProofPassFeedbacks({
      logs: [
        `Program ${proofPassProgramId} invoke [1]`,
        finalizeLog,
        `Program ${otherProgramId} invoke [2]`,
        `Program log: PP_FINALIZE|v=1|session=${Buffer.alloc(32, 0xaa).toString("hex")}|client=${client.toBuffer().toString("hex")}|asset=${asset.toBuffer().toString("hex")}|feedback_index=999|context=1|context_ref=${contextRefHash}|seal=${Buffer.alloc(32, 0xbb).toString("hex")}`,
        `Program ${otherProgramId} success`,
        secondFinalizeLog,
        `Program ${proofPassProgramId} success`,
      ],
      signature: "sig-multi",
      slot: 1001n,
      feedbackEvents: [
        {
          asset,
          clientAddress: client,
          feedbackIndex: 7n,
          sealHash: Buffer.from(sealHash, "hex"),
        } as any,
        {
          asset,
          clientAddress: client,
          feedbackIndex: 8n,
          sealHash: Buffer.from(secondSealHash, "hex"),
        } as any,
      ],
    });

    expect(matches).toHaveLength(2);
    expect(matches.map((row) => row.feedbackIndex)).toEqual(["7", "8"]);
    expect(matches.map((row) => row.proofpassSession)).toEqual([sessionHex, secondSessionHex]);
  });

  it("preserves large feedback_index and slot values without precision loss", () => {
    const largeFeedbackIndex = "9007199254740997";
    const largeSlot = 9007199254741999n;
    const largeFinalizeLog =
      `Program log: PP_FINALIZE|v=1|session=${sessionHex}` +
      `|client=${client.toBuffer().toString("hex")}` +
      `|asset=${asset.toBuffer().toString("hex")}` +
      `|feedback_index=${largeFeedbackIndex}|context=4|context_ref=${contextRefHash}|seal=${sealHash}`;

    const matches = matchProofPassFeedbacks({
      logs: [
        `Program ${proofPassProgramId} invoke [1]`,
        largeFinalizeLog,
        `Program ${proofPassProgramId} success`,
      ],
      signature: "sig-large",
      slot: largeSlot,
      feedbackEvents: [
        {
          asset,
          clientAddress: client,
          feedbackIndex: 9007199254740997n,
          sealHash: Buffer.from(sealHash, "hex"),
        } as any,
      ],
    });

    expect(matches).toHaveLength(1);
    expect(matches[0]?.feedbackIndex).toBe(largeFeedbackIndex);
    expect(matches[0]?.blockSlot).toBe(largeSlot.toString());
    expect(matches[0]?.id).toBe(
      `${asset.toBase58()}:${client.toBase58()}:${largeFeedbackIndex}:sig-large`
    );
  });

  it("ignores malformed and duplicate finalize noise while keeping one exact match", () => {
    const duplicateFinalizeLog =
      `Program log: PP_FINALIZE|v=1|session=${Buffer.alloc(32, 0x99).toString("hex")}` +
      `|client=${client.toBuffer().toString("hex")}` +
      `|asset=${asset.toBuffer().toString("hex")}` +
      `|feedback_index=7|context=4|context_ref=${contextRefHash}|seal=${sealHash}`;

    const matches = matchProofPassFeedbacks({
      logs: [
        `Program ${proofPassProgramId} invoke [1]`,
        "Program log: PP_FINALIZE|v=1|session=badhex|client=nope",
        finalizeLog,
        duplicateFinalizeLog,
        `Program ${proofPassProgramId} success`,
      ],
      signature: "sig-dup",
      slot: 1002n,
      feedbackEvents: [
        {
          asset,
          clientAddress: client,
          feedbackIndex: 7n,
          sealHash: Buffer.from(sealHash, "hex"),
        } as any,
      ],
    });

    expect(matches).toHaveLength(1);
    expect(matches[0]?.feedbackIndex).toBe("7");
    expect(matches[0]?.feedbackHash).toBe(sealHash);
  });
});
