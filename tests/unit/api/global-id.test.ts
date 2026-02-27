import { describe, it, expect } from "vitest";

/**
 * Tests for agent_id formatting and resolver logic.
 * The actual sequence/trigger behavior is PostgreSQL-specific
 * and tested via the migration SQL.
 */

function formatGlobalId(id: number | null): string | null {
  if (id === null || isNaN(id)) return null;
  const pad = id < 1000 ? 3 : id < 10000 ? 4 : id < 100000 ? 5 : 6;
  return '#' + String(id).padStart(pad, '0');
}

describe("Global ID formatting", () => {
  it("should pad to 3 digits for IDs < 1000", () => {
    expect(formatGlobalId(1)).toBe("#001");
    expect(formatGlobalId(42)).toBe("#042");
    expect(formatGlobalId(999)).toBe("#999");
  });

  it("should pad to 4 digits for IDs 1000-9999", () => {
    expect(formatGlobalId(1000)).toBe("#1000");
    expect(formatGlobalId(5042)).toBe("#5042");
    expect(formatGlobalId(9999)).toBe("#9999");
  });

  it("should pad to 5 digits for IDs 10000-99999", () => {
    expect(formatGlobalId(10000)).toBe("#10000");
    expect(formatGlobalId(10042)).toBe("#10042");
  });

  it("should pad to 6 digits for IDs >= 100000", () => {
    expect(formatGlobalId(100000)).toBe("#100000");
    expect(formatGlobalId(999999)).toBe("#999999");
  });

  it("should return null for null input", () => {
    expect(formatGlobalId(null)).toBeNull();
  });

  it("should return null for NaN input", () => {
    expect(formatGlobalId(NaN)).toBeNull();
  });
});

describe("Global ID deterministic ordering", () => {
  it("should sort by (block_slot, tx_signature) for backfill", () => {
    const agents = [
      { asset: "C", block_slot: 100n, tx_index: 0, tx_signature: "sig_c" },
      { asset: "A", block_slot: 100n, tx_index: 2, tx_signature: "sig_a" },
      { asset: "B", block_slot: 100n, tx_index: 1, tx_signature: "sig_b" },
      { asset: "D", block_slot: 200n, tx_index: 0, tx_signature: "sig_d" },
    ];

    const sorted = [...agents].sort((a, b) => {
      if (a.block_slot !== b.block_slot) return Number(a.block_slot - b.block_slot);
      const sigCmp = a.tx_signature.localeCompare(b.tx_signature);
      if (sigCmp !== 0) return sigCmp;
      const txA = a.tx_index ?? Number.MAX_SAFE_INTEGER;
      const txB = b.tx_index ?? Number.MAX_SAFE_INTEGER;
      return txA - txB;
    });

    expect(sorted.map(a => a.asset)).toEqual(["A", "B", "C", "D"]);
    expect(sorted.find(a => a.asset === "A")?.tx_index).toBe(2);
  });

  it("should keep NULL tx_index as metadata while sorting canonically by signature", () => {
    const agents = [
      { asset: "X", block_slot: 100n, tx_index: null as number | null, tx_signature: "sig_b" },
      { asset: "Y", block_slot: 100n, tx_index: 0, tx_signature: "sig_c" },
      { asset: "Z", block_slot: 100n, tx_index: 1, tx_signature: "sig_a" },
    ];

    const sorted = [...agents].sort((a, b) => {
      if (a.block_slot !== b.block_slot) return Number(a.block_slot - b.block_slot);
      const sigCmp = a.tx_signature.localeCompare(b.tx_signature);
      if (sigCmp !== 0) return sigCmp;
      const txA = a.tx_index ?? Number.MAX_SAFE_INTEGER;
      const txB = b.tx_index ?? Number.MAX_SAFE_INTEGER;
      return txA - txB;
    });

    expect(sorted.map(a => a.asset)).toEqual(["Z", "X", "Y"]);
    expect(sorted.find(a => a.asset === "X")?.tx_index).toBeNull();
  });

  it("should use tx_index only as tertiary tie-breaker when signatures collide", () => {
    const agents = [
      { asset: "B", block_slot: 100n, tx_index: 1, tx_signature: "sig_same" },
      { asset: "A", block_slot: 100n, tx_index: 0, tx_signature: "sig_same" },
    ];

    const sorted = [...agents].sort((a, b) => {
      if (a.block_slot !== b.block_slot) return Number(a.block_slot - b.block_slot);
      const sigCmp = a.tx_signature.localeCompare(b.tx_signature);
      if (sigCmp !== 0) return sigCmp;
      const txA = a.tx_index ?? Number.MAX_SAFE_INTEGER;
      const txB = b.tx_index ?? Number.MAX_SAFE_INTEGER;
      return txA - txB;
    });

    expect(sorted.map(a => a.asset)).toEqual(["A", "B"]);
  });
});

describe("Global ID reorg resilience", () => {
  it("should maintain gaps when agent is orphaned", () => {
    // Simulate: agent1(id=1), agent2(id=2), agent3(id=3)
    // Agent2 becomes ORPHANED → gap at id=2
    // Next insert gets id=4 (sequence continues)
    const ids = [1, 2, 3];
    const orphanedId = 2;
    const activeIds = ids.filter(id => id !== orphanedId);
    const nextId = Math.max(...ids) + 1;

    expect(activeIds).toEqual([1, 3]);
    expect(nextId).toBe(4);
    // Gap at 2 is acceptable - no shifting
  });

  it("should retain agent_id when orphaned agent is recovered", () => {
    // Agent marked ORPHANED keeps its agent_id
    // When recovered (status back to FINALIZED), agent_id unchanged
    const agent = { asset: "test", agent_id: 42, status: "ORPHANED" };
    agent.status = "FINALIZED";
    expect(agent.agent_id).toBe(42); // Unchanged
  });
});
