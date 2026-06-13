import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { isCanonicalEventOrderNewerOrEqual } from "../../../src/db/handlers.js";

function ctx(eventOrdinal: number | undefined, signature = "sig-next") {
  return {
    signature,
    slot: 100n,
    blockTime: new Date("2026-01-01T00:00:00.000Z"),
    txIndex: 7,
    eventOrdinal,
  };
}

describe("canonical mutable-write ordering", () => {
  it("rejects same-transaction stale local writes by event ordinal", () => {
    const current = {
      slot: 100n,
      txIndex: 7,
      eventOrdinal: 5,
      signature: "sig-current",
    };

    expect(isCanonicalEventOrderNewerOrEqual(current, ctx(4))).toBe(false);
    expect(isCanonicalEventOrderNewerOrEqual(current, ctx(6))).toBe(true);
  });

  it("keeps event ordinal before signature with NULLS LAST semantics locally", () => {
    expect(isCanonicalEventOrderNewerOrEqual(
      { slot: 100n, txIndex: 7, eventOrdinal: null, signature: "sig-current" },
      ctx(6),
    )).toBe(false);
    expect(isCanonicalEventOrderNewerOrEqual(
      { slot: 100n, txIndex: 7, eventOrdinal: 6, signature: "sig-current" },
      ctx(undefined),
    )).toBe(true);
    expect(isCanonicalEventOrderNewerOrEqual(
      { slot: 100n, txIndex: 7, eventOrdinal: 6, signature: "sig-z" },
      ctx(6, "sig-a"),
    )).toBe(false);
  });

  it("threads event ordinal through Supabase and batch SQL guards before signature", () => {
    for (const file of ["src/db/supabase.ts", "src/indexer/batch-processor.ts"]) {
      const source = readFileSync(resolve(process.cwd(), file), "utf8");
      const ordinalCompare = "COALESCE(${tableRef}.${eventOrdinalColumn}, ${NULLS_LAST_SQL_EVENT_ORDINAL}) < COALESCE(${eventOrdinalExpr}, ${NULLS_LAST_SQL_EVENT_ORDINAL})";
      const signatureCompare = "COALESCE(${tableRef}.${signatureColumn}, '') COLLATE \"C\" <= ${signatureExpr} COLLATE \"C\"";

      expect(source).toContain("eventOrdinalExpr: string");
      expect(source).toContain(ordinalCompare);
      expect(source.indexOf(ordinalCompare)).toBeLessThan(source.indexOf(signatureCompare));
      expect(source).not.toContain('agentOrderGuardSql("EXCLUDED.block_slot", "EXCLUDED.tx_index", "EXCLUDED.tx_signature")');
      expect(source).not.toContain('metadataOrderGuardSql("EXCLUDED.block_slot", "EXCLUDED.tx_index", "EXCLUDED.tx_signature")');
    }
  });
});
