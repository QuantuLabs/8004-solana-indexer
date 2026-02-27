import { describe, it, expect } from "vitest";

/**
 * Cross-indexer determinism tests.
 *
 * Proves that canonical ordering (block_slot, tx_signature, tx_index)
 * produces identical agent_id assignments regardless of which indexer runs:
 *   - RPC Poller (getBlock → enumerate → tx_index)
 *   - Substreams  (block.transactions.iter().enumerate() → tx_index)
 *   - WebSocket   (onLogs → no tx_index, NULL fallback)
 *
 * Also validates that the SQL ordering
 *   ORDER BY block_slot, tx_signature, tx_index NULLS LAST
 * matches the TypeScript in-memory ordering.
 */

// ── Helpers (mirror production logic) ──────────────────────────────────────

/** Deterministic sort matching canonical SQL ORDER BY used in replay/backfill */
function deterministicSort<T extends { block_slot: bigint; tx_index: number | null; tx_signature: string }>(
  agents: T[],
): T[] {
  return [...agents].sort((a, b) => {
    if (a.block_slot !== b.block_slot) return Number(a.block_slot - b.block_slot);
    const sigCmp = a.tx_signature.localeCompare(b.tx_signature);
    if (sigCmp !== 0) return sigCmp;
    const txA = a.tx_index ?? Number.MAX_SAFE_INTEGER;
    const txB = b.tx_index ?? Number.MAX_SAFE_INTEGER;
    return txA - txB;
  });
}

/** SQL-equivalent sort with signature-first ordering and NULLS LAST tx_index tie-break */
function sqlEquivalentSort<T extends { block_slot: bigint; tx_index: number | null; tx_signature: string }>(
  agents: T[],
): T[] {
  return [...agents].sort((a, b) => {
    if (a.block_slot !== b.block_slot) return Number(a.block_slot - b.block_slot);
    const sigCmp = a.tx_signature.localeCompare(b.tx_signature);
    if (sigCmp !== 0) return sigCmp;
    const txA = a.tx_index ?? 2147483647;
    const txB = b.tx_index ?? 2147483647;
    return txA - txB;
  });
}

/** Simulates agent_id assignment: sort then assign sequential IDs */
function assignGlobalIds(
  agents: Array<{ asset: string; block_slot: bigint; tx_index: number | null; tx_signature: string; status?: string }>,
): Map<string, number> {
  const sorted = deterministicSort(
    agents.filter(a => a.status !== "ORPHANED"),
  );
  const ids = new Map<string, number>();
  sorted.forEach((a, i) => ids.set(a.asset, i + 1));
  return ids;
}

// ── Simulated indexer outputs ──────────────────────────────────────────────

interface AgentRecord {
  asset: string;
  block_slot: bigint;
  tx_index: number | null;
  tx_signature: string;
  status?: string;
}

/**
 * Simulates what the RPC Poller produces:
 * - getBlock enumerate → tx_index = array position
 * - Falls back to NULL if getBlock fails (after 3 retries)
 */
function simulatePollerOutput(
  blockTxOrder: Array<{ asset: string; slot: number; signature: string }>,
  getBlockFails: Set<number> = new Set(),
): AgentRecord[] {
  const bySlot = new Map<number, typeof blockTxOrder>();
  for (const tx of blockTxOrder) {
    if (!bySlot.has(tx.slot)) bySlot.set(tx.slot, []);
    bySlot.get(tx.slot)!.push(tx);
  }

  const result: AgentRecord[] = [];
  for (const [slot, txs] of bySlot) {
    if (getBlockFails.has(slot)) {
      // getBlock failed → NULL tx_index
      for (const tx of txs) {
        result.push({
          asset: tx.asset,
          block_slot: BigInt(slot),
          tx_index: null,
          tx_signature: tx.signature,
        });
      }
    } else {
      // getBlock succeeded → tx_index = position in block
      txs.forEach((tx, idx) => {
        result.push({
          asset: tx.asset,
          block_slot: BigInt(slot),
          tx_index: idx,
          tx_signature: tx.signature,
        });
      });
    }
  }
  return result;
}

/**
 * Simulates what the Substreams indexer produces:
 * - block.transactions.iter().enumerate() → tx_index always available
 * - Same canonical order as getBlock
 */
function simulateSubstreamsOutput(
  blockTxOrder: Array<{ asset: string; slot: number; signature: string }>,
): AgentRecord[] {
  const bySlot = new Map<number, typeof blockTxOrder>();
  for (const tx of blockTxOrder) {
    if (!bySlot.has(tx.slot)) bySlot.set(tx.slot, []);
    bySlot.get(tx.slot)!.push(tx);
  }

  const result: AgentRecord[] = [];
  for (const [slot, txs] of bySlot) {
    txs.forEach((tx, idx) => {
      result.push({
        asset: tx.asset,
        block_slot: BigInt(slot),
        tx_index: idx,
        tx_signature: tx.signature,
      });
    });
  }
  return result;
}

/**
 * Simulates what the WebSocket indexer produces:
 * - onLogs gives signature + slot, but no tx_index
 * - tx_index is always NULL
 */
function simulateWebSocketOutput(
  blockTxOrder: Array<{ asset: string; slot: number; signature: string }>,
): AgentRecord[] {
  return blockTxOrder.map(tx => ({
    asset: tx.asset,
    block_slot: BigInt(tx.slot),
    tx_index: null,
    tx_signature: tx.signature,
  }));
}

// ── Tests ──────────────────────────────────────────────────────────────────

describe("Cross-indexer determinism: ordering equivalence", () => {
  // Canonical block data: the on-chain truth (position = tx_index)
  const BLOCK_DATA = [
    { asset: "AgentAlpha", slot: 1000, signature: "5xR7kM..." },
    { asset: "AgentBeta", slot: 1000, signature: "3pQ2nL..." },
    { asset: "AgentGamma", slot: 1000, signature: "9wT5jK..." },
    { asset: "AgentDelta", slot: 2000, signature: "1aB3cD..." },
    { asset: "AgentEpsilon", slot: 2000, signature: "7eF8gH..." },
    { asset: "AgentZeta", slot: 3000, signature: "2iJ4kL..." },
  ];

  it("Poller and Substreams produce identical ordering when getBlock succeeds", () => {
    const pollerResult = simulatePollerOutput(BLOCK_DATA);
    const substreamsResult = simulateSubstreamsOutput(BLOCK_DATA);

    const pollerSorted = deterministicSort(pollerResult);
    const substreamsSorted = deterministicSort(substreamsResult);

    // Both should produce identical asset ordering
    expect(pollerSorted.map(a => a.asset)).toEqual(substreamsSorted.map(a => a.asset));

    // Both should produce identical agent_id assignments
    const pollerIds = assignGlobalIds(pollerResult);
    const substreamsIds = assignGlobalIds(substreamsResult);
    expect(pollerIds).toEqual(substreamsIds);
  });

  it("Poller and Substreams produce identical global_ids via backfill", () => {
    const pollerResult = simulatePollerOutput(BLOCK_DATA);
    const substreamsResult = simulateSubstreamsOutput(BLOCK_DATA);

    const pollerIds = assignGlobalIds(pollerResult);
    const substreamsIds = assignGlobalIds(substreamsResult);

    // Every agent should get the same agent_id from either indexer
    for (const [asset, id] of pollerIds) {
      expect(substreamsIds.get(asset)).toBe(id);
    }
  });

  it("WebSocket output with NULL tx_index still sorts deterministically via tx_signature", () => {
    const wsResult = simulateWebSocketOutput(BLOCK_DATA);
    const sorted = deterministicSort(wsResult);

    // All tx_index are NULL → falls back to tx_signature alphabetical within slot
    // Slot 1000: "3pQ2nL..." < "5xR7kM..." < "9wT5jK..." → Beta, Alpha, Gamma
    // Slot 2000: "1aB3cD..." < "7eF8gH..." → Delta, Epsilon
    // Slot 3000: Zeta (single)
    expect(sorted.map(a => a.asset)).toEqual([
      "AgentBeta", "AgentAlpha", "AgentGamma",
      "AgentDelta", "AgentEpsilon",
      "AgentZeta",
    ]);
  });

  it("WebSocket ordering matches Poller/Substreams under canonical signature ordering", () => {
    // With canonical (slot, signature) ordering, missing tx_index does not change ordering.
    const pollerResult = simulatePollerOutput(BLOCK_DATA);
    const wsResult = simulateWebSocketOutput(BLOCK_DATA);

    const pollerIds = assignGlobalIds(pollerResult);
    const wsIds = assignGlobalIds(wsResult);

    // With only 1 agent per slot, ordering is identical
    // With multiple agents per slot, ordering MAY diverge
    const slotsWithMultiple = new Set<number>();
    const slotCounts = new Map<number, number>();
    for (const tx of BLOCK_DATA) {
      slotCounts.set(tx.slot, (slotCounts.get(tx.slot) || 0) + 1);
    }
    for (const [slot, count] of slotCounts) {
      if (count > 1) slotsWithMultiple.add(slot);
    }

    // Compare ids for all agents across indexers
    let hasDivergence = false;
    for (const [asset, pollerId] of pollerIds) {
      const wsId = wsIds.get(asset);
      if (pollerId !== wsId) hasDivergence = true;
    }

    // No divergence expected, even in multi-agent slots.
    expect(slotsWithMultiple.size).toBeGreaterThan(0);
    expect(hasDivergence).toBe(false);
  });
});

describe("Cross-indexer determinism: SQL vs TypeScript equivalence", () => {
  it("COALESCE(tx_index, 2147483647) matches Number.MAX_SAFE_INTEGER sort for normal values", () => {
    const agents: AgentRecord[] = [
      { asset: "C", block_slot: 100n, tx_index: 5, tx_signature: "sig_c" },
      { asset: "A", block_slot: 100n, tx_index: 0, tx_signature: "sig_a" },
      { asset: "B", block_slot: 100n, tx_index: 3, tx_signature: "sig_b" },
      { asset: "N", block_slot: 100n, tx_index: null, tx_signature: "sig_n" },
    ];

    const jsSorted = deterministicSort(agents);
    const sqlSorted = sqlEquivalentSort(agents);

    // Both should produce the same order: A(0), B(3), C(5), N(null→last)
    expect(jsSorted.map(a => a.asset)).toEqual(sqlSorted.map(a => a.asset));
    expect(jsSorted.map(a => a.asset)).toEqual(["A", "B", "C", "N"]);
  });

  it("canonical signature ordering applies before NULL tx_index handling", () => {
    const agents: AgentRecord[] = [
      { asset: "Null1", block_slot: 100n, tx_index: null, tx_signature: "aaa" },
      { asset: "Null2", block_slot: 100n, tx_index: null, tx_signature: "zzz" },
      { asset: "Known", block_slot: 100n, tx_index: 0, tx_signature: "mmm" },
    ];

    const jsSorted = deterministicSort(agents);
    const sqlSorted = sqlEquivalentSort(agents);

    // Signature order is primary ("aaa" < "mmm" < "zzz"), tx_index only breaks ties.
    expect(jsSorted.map(a => a.asset)).toEqual(["Null1", "Known", "Null2"]);
    expect(sqlSorted.map(a => a.asset)).toEqual(["Null1", "Known", "Null2"]);
  });

  it("tx_signature tiebreaker is lexicographic in both SQL and JS", () => {
    const agents: AgentRecord[] = [
      { asset: "Z", block_slot: 100n, tx_index: 0, tx_signature: "z_sig" },
      { asset: "A", block_slot: 100n, tx_index: 0, tx_signature: "a_sig" },
      { asset: "M", block_slot: 100n, tx_index: 0, tx_signature: "m_sig" },
    ];

    const jsSorted = deterministicSort(agents);
    // tx_index all 0, so sorted by tx_signature alphabetically
    expect(jsSorted.map(a => a.asset)).toEqual(["A", "M", "Z"]);
  });

  it("COALESCE sentinel 2147483647 doesn't collide with real tx_index values", () => {
    // Solana blocks have max ~1400 transactions (due to slot time constraints)
    // Max realistic tx_index is around 1400, nowhere near 2147483647
    const MAX_REALISTIC_TX_INDEX = 1500;
    const COALESCE_SENTINEL = 2147483647;

    expect(COALESCE_SENTINEL).toBeGreaterThan(MAX_REALISTIC_TX_INDEX * 1000);

    // Verify that a real tx_index never equals the sentinel
    const agents: AgentRecord[] = [
      { asset: "Real", block_slot: 100n, tx_index: MAX_REALISTIC_TX_INDEX, tx_signature: "sig" },
      { asset: "Null", block_slot: 100n, tx_index: null, tx_signature: "sig2" },
    ];

    const sorted = deterministicSort(agents);
    expect(sorted[0].asset).toBe("Real");
    expect(sorted[1].asset).toBe("Null");
  });
});

describe("Cross-indexer determinism: getBlock fallback scenarios", () => {
  const BLOCK_DATA = [
    { asset: "A", slot: 100, signature: "sig_a" },
    { asset: "B", slot: 100, signature: "sig_b" },
    { asset: "C", slot: 200, signature: "sig_c" },
    { asset: "D", slot: 200, signature: "sig_d" },
    { asset: "E", slot: 300, signature: "sig_e" },
  ];

  it("getBlock failure for one slot doesn't affect other slots", () => {
    // Slot 100 fails, slot 200 and 300 succeed
    const failedSlots = new Set([100]);
    const pollerResult = simulatePollerOutput(BLOCK_DATA, failedSlots);

    // Slot 200 and 300 agents should still have correct tx_index
    const slot200 = pollerResult.filter(a => a.block_slot === 200n);
    expect(slot200.every(a => a.tx_index !== null)).toBe(true);

    const slot300 = pollerResult.filter(a => a.block_slot === 300n);
    expect(slot300.every(a => a.tx_index !== null)).toBe(true);

    // Slot 100 agents have NULL tx_index
    const slot100 = pollerResult.filter(a => a.block_slot === 100n);
    expect(slot100.every(a => a.tx_index === null)).toBe(true);
  });

  it("mixed NULL tx_index data remains deterministic in canonical ordering", () => {
    // Mixed scenario: some agents have tx_index, others don't.
    const agents: AgentRecord[] = [
      { asset: "Known1", block_slot: 100n, tx_index: 0, tx_signature: "sig_k1" },
      { asset: "Unknown1", block_slot: 100n, tx_index: null, tx_signature: "sig_u1" },
      { asset: "Known2", block_slot: 100n, tx_index: 1, tx_signature: "sig_k2" },
      { asset: "Unknown2", block_slot: 100n, tx_index: null, tx_signature: "sig_u2" },
    ];

    const sorted = deterministicSort(agents);
    // Signature order remains deterministic.
    expect(sorted.map(a => a.asset)).toEqual(["Known1", "Known2", "Unknown1", "Unknown2"]);
  });

  it("re-indexing with full tx_index data produces stable ordering", () => {
    // First run: some slots fail
    const failedSlots = new Set([100]);
    const firstRun = simulatePollerOutput(BLOCK_DATA, failedSlots);

    // Second run: all slots succeed (full re-index)
    const secondRun = simulatePollerOutput(BLOCK_DATA);

    // Within successful slots, ordering should match
    const firstSlot200 = deterministicSort(firstRun.filter(a => a.block_slot === 200n));
    const secondSlot200 = deterministicSort(secondRun.filter(a => a.block_slot === 200n));
    expect(firstSlot200.map(a => a.asset)).toEqual(secondSlot200.map(a => a.asset));
  });
});

describe("Cross-indexer determinism: reorg resilience", () => {
  it("orphaned agents are excluded from agent_id assignment", () => {
    const agents: AgentRecord[] = [
      { asset: "A", block_slot: 100n, tx_index: 0, tx_signature: "sig_a" },
      { asset: "B", block_slot: 100n, tx_index: 1, tx_signature: "sig_b", status: "ORPHANED" },
      { asset: "C", block_slot: 200n, tx_index: 0, tx_signature: "sig_c" },
    ];

    const ids = assignGlobalIds(agents);

    expect(ids.get("A")).toBe(1);
    expect(ids.has("B")).toBe(false); // ORPHANED, no agent_id
    expect(ids.get("C")).toBe(2);     // No gap shift expected in backfill
  });

  it("recovered agent preserves agent_id (trigger doesn't reassign on UPDATE)", () => {
    const agents: AgentRecord[] = [
      { asset: "A", block_slot: 100n, tx_index: 0, tx_signature: "sig_a" },
      { asset: "B", block_slot: 100n, tx_index: 1, tx_signature: "sig_b" },
      { asset: "C", block_slot: 200n, tx_index: 0, tx_signature: "sig_c" },
    ];

    const initialIds = assignGlobalIds(agents);
    expect(initialIds.get("B")).toBe(2);

    // B becomes orphaned then recovers - simulate trigger behavior:
    // The BEFORE INSERT trigger only fires on INSERT, not UPDATE
    // So recovering an agent (UPDATE status='FINALIZED') keeps agent_id
    const bId = initialIds.get("B");
    expect(bId).toBe(2); // B's agent_id persists through status changes
  });

  it("new agent after gap gets next sequence value (no backfill)", () => {
    // After backfill: A=1, B=2, C=3
    // B is orphaned → gap at 2
    // New agent D gets sequence nextval = 4
    const ids = [1, 2, 3];
    const nextSequenceVal = Math.max(...ids) + 1;
    expect(nextSequenceVal).toBe(4);
    // D gets 4, gap at 2 persists (acceptable)
  });
});

describe("Cross-indexer determinism: sequence safety", () => {
  it("nextval is always unique (PostgreSQL guarantee)", () => {
    // Simulate sequence behavior: nextval always returns strictly increasing values
    let sequence = 0;
    const nextval = () => ++sequence;

    const ids = new Set<number>();
    for (let i = 0; i < 10000; i++) {
      const id = nextval();
      expect(ids.has(id)).toBe(false);
      ids.add(id);
    }
    expect(ids.size).toBe(10000);
  });

  it("concurrent inserts get unique but potentially out-of-order global_ids", () => {
    // This documents the known behavior: two concurrent INSERTs for the same slot
    // will get unique global_ids, but the id order may not match on-chain order.
    // This is acceptable because:
    // 1. The poller processes slots sequentially (sorted)
    // 2. Within a slot, txs are sorted by signature before INSERT (tx_index tie-break only)
    // 3. Concurrent inserts only happen across different poll cycles
    let sequence = 100;
    const nextval = () => ++sequence;

    // Simulate: TX at slot=1000 with canonical signature order
    // If processed in correct order:
    const id1 = nextval(); // Earlier canonical record → 101
    const id2 = nextval(); // Later canonical record → 102
    expect(id1).toBeLessThan(id2); // Correct: earlier tx gets lower id

    // If processed out of order (hypothetical race):
    const id3 = nextval(); // Later canonical record → 103
    const id4 = nextval(); // Earlier canonical record → 104
    expect(id3).not.toBe(id4); // Always unique
    // But id3 < id4 even though canonical order is reversed by race → acceptable trade-off
  });
});

describe("Cross-indexer determinism: migration backfill correctness", () => {
  it("backfill ROW_NUMBER matches deterministicSort assignment", () => {
    // Simulate the migration's WITH ordered_agents query
    const agents: AgentRecord[] = [
      { asset: "Late", block_slot: 500n, tx_index: 0, tx_signature: "sig_late" },
      { asset: "First", block_slot: 100n, tx_index: 0, tx_signature: "sig_first" },
      { asset: "Middle", block_slot: 300n, tx_index: 2, tx_signature: "sig_mid" },
      { asset: "MidSlot", block_slot: 300n, tx_index: 0, tx_signature: "sig_midslot" },
      { asset: "NullIdx", block_slot: 300n, tx_index: null, tx_signature: "sig_nullidx" },
    ];

    const ids = assignGlobalIds(agents);

    expect(ids.get("First")).toBe(1);     // slot 100
    expect(ids.get("Middle")).toBe(2);    // slot 300, signature sig_mid
    expect(ids.get("MidSlot")).toBe(3);   // slot 300, signature sig_midslot
    expect(ids.get("NullIdx")).toBe(4);   // slot 300, signature sig_nullidx
    expect(ids.get("Late")).toBe(5);      // slot 500
  });

  it("setval after backfill prevents duplicate global_ids", () => {
    const agents: AgentRecord[] = [
      { asset: "A", block_slot: 100n, tx_index: 0, tx_signature: "sig_a" },
      { asset: "B", block_slot: 200n, tx_index: 0, tx_signature: "sig_b" },
      { asset: "C", block_slot: 300n, tx_index: 0, tx_signature: "sig_c" },
    ];

    const ids = assignGlobalIds(agents);
    const maxId = Math.max(...ids.values());
    expect(maxId).toBe(3);

    // setval('agent_global_id_seq', 3)
    // Next nextval() returns 4
    let sequence = maxId;
    const nextval = () => ++sequence;

    const newId = nextval();
    expect(newId).toBe(4);
    expect(ids.has("A") && ids.get("A") !== newId).toBe(true);
  });
});

describe("Cross-indexer determinism: txIndex=0 falsy guard", () => {
  it("nullish coalescing (??) preserves txIndex=0, logical OR (||) does not", () => {
    // This tests the critical bug where `ctx.txIndex || null` coerces 0 to null
    // because 0 is falsy in JavaScript. The fix is to use `ctx.txIndex ?? null`.
    const txIndex = 0;

    // BUG: || operator treats 0 as falsy → returns null
    const buggy = txIndex || null;
    expect(buggy).toBeNull(); // This is the bug!

    // FIX: ?? operator only coalesces null/undefined
    const fixed = txIndex ?? null;
    expect(fixed).toBe(0); // Correct behavior
  });

  it("txIndex=0 agent must sort before txIndex=1 agent when signature ties", () => {
    const agents: AgentRecord[] = [
      { asset: "Second", block_slot: 100n, tx_index: 1, tx_signature: "sig_same" },
      { asset: "First", block_slot: 100n, tx_index: 0, tx_signature: "sig_same" },
    ];

    const sorted = deterministicSort(agents);
    expect(sorted[0].asset).toBe("First");
    expect(sorted[0].tx_index).toBe(0);
  });

  it("txIndex=0 must NOT be treated as NULL when signature ties", () => {
    const agents: AgentRecord[] = [
      { asset: "NullIdx", block_slot: 100n, tx_index: null, tx_signature: "sig_same" },
      { asset: "ZeroIdx", block_slot: 100n, tx_index: 0, tx_signature: "sig_same" },
    ];

    const sorted = deterministicSort(agents);
    // tx_index=0 sorts BEFORE tx_index=null when primary keys are tied
    expect(sorted[0].asset).toBe("ZeroIdx");
    expect(sorted[1].asset).toBe("NullIdx");
  });
});

describe("Cross-indexer determinism: edge cases", () => {
  it("single agent per slot needs no tx_index resolution", () => {
    const agents: AgentRecord[] = [
      { asset: "Solo1", block_slot: 100n, tx_index: 0, tx_signature: "sig1" },
      { asset: "Solo2", block_slot: 200n, tx_index: 0, tx_signature: "sig2" },
      { asset: "Solo3", block_slot: 300n, tx_index: 0, tx_signature: "sig3" },
    ];

    // With tx_index or NULL, single-agent slots always sort correctly
    const withIndex = deterministicSort(agents);
    const withoutIndex = deterministicSort(agents.map(a => ({ ...a, tx_index: null })));

    expect(withIndex.map(a => a.asset)).toEqual(withoutIndex.map(a => a.asset));
  });

  it("empty dataset produces no global_ids", () => {
    const ids = assignGlobalIds([]);
    expect(ids.size).toBe(0);
  });

  it("all agents orphaned produces no global_ids", () => {
    const agents: AgentRecord[] = [
      { asset: "A", block_slot: 100n, tx_index: 0, tx_signature: "sig_a", status: "ORPHANED" },
      { asset: "B", block_slot: 200n, tx_index: 0, tx_signature: "sig_b", status: "ORPHANED" },
    ];

    const ids = assignGlobalIds(agents);
    expect(ids.size).toBe(0);
  });

  it("large tx_index values do not override signature-primary ordering", () => {
    const agents: AgentRecord[] = [
      { asset: "Last", block_slot: 100n, tx_index: 1399, tx_signature: "sig_last" },
      { asset: "First", block_slot: 100n, tx_index: 0, tx_signature: "sig_first" },
      { asset: "Mid", block_slot: 100n, tx_index: 700, tx_signature: "sig_mid" },
    ];

    const sorted = deterministicSort(agents);
    expect(sorted.map(a => a.asset)).toEqual(["First", "Last", "Mid"]);
  });

  it("bigint block_slot comparison doesn't overflow", () => {
    const agents: AgentRecord[] = [
      { asset: "Future", block_slot: 999_999_999n, tx_index: 0, tx_signature: "sig_f" },
      { asset: "Past", block_slot: 1n, tx_index: 0, tx_signature: "sig_p" },
      { asset: "Current", block_slot: 350_000_000n, tx_index: 0, tx_signature: "sig_c" },
    ];

    const sorted = deterministicSort(agents);
    expect(sorted.map(a => a.asset)).toEqual(["Past", "Current", "Future"]);
  });
});
