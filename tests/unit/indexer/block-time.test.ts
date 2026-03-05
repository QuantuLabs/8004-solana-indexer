import { afterEach, describe, expect, it, vi } from "vitest";
import { resolveEventBlockTime } from "../../../src/indexer/block-time.js";

const SOLANA_GENESIS_UNIX_MS = 1584316800000;
const SOLANA_SLOT_TIME_MS = 400;

describe("resolveEventBlockTime", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("uses deterministic slot-derived time when chain blockTime is missing", () => {
    const slot = 123456;
    const expected = SOLANA_GENESIS_UNIX_MS + slot * SOLANA_SLOT_TIME_MS;

    expect(resolveEventBlockTime(null, slot).getTime()).toBe(expected);
  });

  it("uses chain blockTime when available and sane", () => {
    const slot = 400_000_000;
    const chainBlockTimeSeconds = 1700000000;
    const chainMs = chainBlockTimeSeconds * 1000;

    expect(resolveEventBlockTime(chainBlockTimeSeconds, slot).getTime()).toBe(chainMs);
  });

  it("clamps future chain blockTime values to now + 5min", () => {
    const now = new Date("2026-03-05T12:00:00.000Z");
    vi.useFakeTimers();
    vi.setSystemTime(now);
    const slot = 42;
    const futureSeconds = Math.floor((now.getTime() + 10 * 60 * 1000) / 1000);

    expect(resolveEventBlockTime(futureSeconds, slot).toISOString()).toBe("2026-03-05T12:05:00.000Z");
  });
});
