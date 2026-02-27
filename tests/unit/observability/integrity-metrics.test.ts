import { beforeEach, describe, expect, it } from "vitest";
import {
  getIntegrityMetricsSnapshot,
  incrementVerifyCycles,
  renderIntegrityMetrics,
  resetIntegrityMetrics,
  setLastVerifiedSlot,
  setMismatchCount,
  setOrphanCount,
  setVerifierActive,
} from "../../../src/observability/integrity-metrics.js";

describe("integrity metrics", () => {
  beforeEach(() => {
    resetIntegrityMetrics();
  });

  it("updates counters and gauges", () => {
    incrementVerifyCycles();
    incrementVerifyCycles(2);
    setMismatchCount(4);
    setOrphanCount(9);
    setLastVerifiedSlot(12345n);
    setVerifierActive(true);

    expect(getIntegrityMetricsSnapshot()).toEqual({
      verifyCyclesTotal: 3,
      mismatchCount: 4,
      orphanCount: 9,
      lastVerifiedSlot: 12345,
      verifierActive: 1,
    });
  });

  it("renders prometheus text output", () => {
    incrementVerifyCycles();
    setMismatchCount(2);
    setOrphanCount(1);
    setLastVerifiedSlot(88);

    const output = renderIntegrityMetrics();

    expect(output).toContain("integrity_verify_cycles_total 1");
    expect(output).toContain("integrity_mismatch_count 2");
    expect(output).toContain("integrity_orphan_count 1");
    expect(output).toContain("integrity_last_verified_slot 88");
  });
});
