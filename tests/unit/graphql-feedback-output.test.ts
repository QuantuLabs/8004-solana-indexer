import { describe, it, expect, vi } from "vitest";

import { feedbackResolvers } from "../../src/api/graphql/resolvers/feedback.js";
import { solanaResolvers } from "../../src/api/graphql/resolvers/solana.js";
import { config } from "../../src/config.js";

describe("GraphQL feedback output parity", () => {
  it("returns normalized feedback value as string", () => {
    const row = {
      value: "1234",
      value_decimals: 2,
    } as any;

    const normalized = feedbackResolvers.Feedback.value(row);
    expect(normalized).toBe("12.34");
    expect(typeof normalized).toBe("string");
  });

  it("keeps Solana extension valueRaw/valueDecimals lossless", () => {
    const row = {
      value: "170141183460469231731687303715884105727",
      value_decimals: 18,
    } as any;

    expect(solanaResolvers.SolanaFeedbackExtension.valueRaw(row)).toBe(
      "170141183460469231731687303715884105727"
    );
    expect(solanaResolvers.SolanaFeedbackExtension.valueDecimals(row)).toBe(18);
  });

  it("preserves empty feedbackURI as empty string", () => {
    const row = {
      feedback_uri: "",
    } as any;

    expect(feedbackResolvers.Feedback.feedbackURI(row)).toBe("");
  });

  it("normalizes Solana runningDigest from postgres bytea string", () => {
    const row = {
      running_digest: "\\xdeadbeef",
    } as any;

    expect(solanaResolvers.SolanaFeedbackExtension.runningDigest(row)).toBe("deadbeef");
  });

  it("normalizes Solana runningDigest from Buffer", () => {
    const row = {
      running_digest: Buffer.from([0xde, 0xad, 0xbe, 0xef]),
    } as any;

    expect(solanaResolvers.SolanaFeedbackExtension.runningDigest(row)).toBe("deadbeef");
  });

  it("returns false for proofPassAuth when tx_signature or feedback_hash is missing", async () => {
    expect(
      solanaResolvers.SolanaFeedbackExtension.proofPassAuth({
        asset: "asset",
        client_address: "client",
        feedback_index: "1",
        tx_signature: null,
        feedback_hash: null,
      } as any, {}, { loaders: { proofPassAuthByFeedback: { load: vi.fn() } } } as any)
    ).toBe(false);
  });

  it("returns false for proofPassAuth when ProofPass is disabled", () => {
    const previous = config.enableProofPass;
    config.enableProofPass = false;
    try {
      expect(
        solanaResolvers.SolanaFeedbackExtension.proofPassAuth({
          asset: "asset",
          client_address: "client",
          feedback_index: "1",
          tx_signature: "sig-1",
          feedback_hash: "abcd",
        } as any, {}, { loaders: { proofPassAuthByFeedback: { load: vi.fn() } } } as any)
      ).toBe(false);
    } finally {
      config.enableProofPass = previous;
    }
  });

  it("delegates proofPassAuth lookup to the ProofPass loader when the feedback tuple is complete", async () => {
    const load = vi.fn().mockResolvedValue(true);
    const previous = config.enableProofPass;
    config.enableProofPass = true;

    try {
      await expect(
        solanaResolvers.SolanaFeedbackExtension.proofPassAuth({
          asset: "asset",
          client_address: "client",
          feedback_index: "7",
          tx_signature: "sig-123",
          feedback_hash: "abcd",
        } as any, {}, { loaders: { proofPassAuthByFeedback: { load } } } as any)
      ).resolves.toBe(true);
    } finally {
      config.enableProofPass = previous;
    }

    expect(load).toHaveBeenCalledWith("asset:client:7:sig-123:abcd");
  });
});
