interface IntegrityMetricsState {
  verifyCyclesTotal: number;
  mismatchCount: number;
  orphanCount: number;
  lastVerifiedSlot: number;
  verifierActive: number;
}

const metrics: IntegrityMetricsState = {
  verifyCyclesTotal: 0,
  mismatchCount: 0,
  orphanCount: 0,
  lastVerifiedSlot: 0,
  verifierActive: 0,
};

function toNonNegativeInt(value: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    return 0;
  }
  return Math.floor(value);
}

function toSlot(value: bigint | number): number {
  const asNumber = typeof value === "bigint" ? Number(value) : value;
  if (!Number.isFinite(asNumber) || asNumber < 0) {
    return 0;
  }
  return Math.floor(asNumber);
}

export function resetIntegrityMetrics(): void {
  metrics.verifyCyclesTotal = 0;
  metrics.mismatchCount = 0;
  metrics.orphanCount = 0;
  metrics.lastVerifiedSlot = 0;
  metrics.verifierActive = 0;
}

export function incrementVerifyCycles(by = 1): void {
  metrics.verifyCyclesTotal += toNonNegativeInt(by);
}

export function setMismatchCount(value: number): void {
  metrics.mismatchCount = toNonNegativeInt(value);
}

export function setOrphanCount(value: number): void {
  metrics.orphanCount = toNonNegativeInt(value);
}

export function setLastVerifiedSlot(value: bigint | number): void {
  metrics.lastVerifiedSlot = toSlot(value);
}

export function setVerifierActive(active: boolean): void {
  metrics.verifierActive = active ? 1 : 0;
}

export function getIntegrityMetricsSnapshot(): IntegrityMetricsState {
  return { ...metrics };
}

export function renderIntegrityMetrics(): string {
  return [
    "# HELP integrity_verify_cycles_total Total completed verification cycles.",
    "# TYPE integrity_verify_cycles_total counter",
    `integrity_verify_cycles_total ${metrics.verifyCyclesTotal}`,
    "# HELP integrity_mismatch_count Total hash-chain mismatches detected.",
    "# TYPE integrity_mismatch_count gauge",
    `integrity_mismatch_count ${metrics.mismatchCount}`,
    "# HELP integrity_orphan_count Total records marked as orphaned.",
    "# TYPE integrity_orphan_count gauge",
    `integrity_orphan_count ${metrics.orphanCount}`,
    "# HELP integrity_last_verified_slot Last finalized cutoff slot used by verifier.",
    "# TYPE integrity_last_verified_slot gauge",
    `integrity_last_verified_slot ${metrics.lastVerifiedSlot}`,
    "# HELP integrity_verifier_active Whether verifier is running (1/0).",
    "# TYPE integrity_verifier_active gauge",
    `integrity_verifier_active ${metrics.verifierActive}`,
    "",
  ].join("\n");
}
