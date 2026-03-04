import { describe, it, expect } from "vitest";

import { feedbackResolvers } from "../../src/api/graphql/resolvers/feedback.js";
import { solanaResolvers } from "../../src/api/graphql/resolvers/solana.js";

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
});
