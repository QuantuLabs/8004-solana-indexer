const SOLANA_GENESIS_UNIX_MS = 1584316800000;
const SOLANA_SLOT_TIME_MS = 400;
const MAX_FUTURE_SKEW_MS = 5 * 60 * 1000;

function normalizeSlot(slot: number | bigint): number {
  const numericSlot = typeof slot === "bigint" ? Number(slot) : slot;
  if (!Number.isFinite(numericSlot) || numericSlot < 0) {
    return 0;
  }
  return Math.min(Math.floor(numericSlot), Number.MAX_SAFE_INTEGER);
}

export function resolveEventBlockTime(
  blockTimeSeconds: number | null | undefined,
  slot: number | bigint
): Date {
  const slotDerivedMs = SOLANA_GENESIS_UNIX_MS + normalizeSlot(slot) * SOLANA_SLOT_TIME_MS;
  const chainBlockTimeMs =
    typeof blockTimeSeconds === "number"
    && Number.isFinite(blockTimeSeconds)
    && blockTimeSeconds > 0
      ? Math.floor(blockTimeSeconds * 1000)
      : null;

  // Prefer chain blockTime when available, but clamp impossible future values.
  if (chainBlockTimeMs !== null) {
    const nowMs = Date.now();
    const capped = Math.min(chainBlockTimeMs, nowMs + MAX_FUTURE_SKEW_MS);
    return new Date(capped);
  }

  // Deterministic fallback when chain blockTime is missing.
  return new Date(slotDerivedMs);
}
