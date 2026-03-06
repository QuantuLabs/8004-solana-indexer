import { readFileSync } from "fs";
import { resolve } from "path";
import { describe, expect, it } from "vitest";
import {
  compareRevocationEventOrder,
  shouldReplaceRevocationByEventOrder,
  REVOCATION_CONFLICT_UPDATE_WHERE_SQL,
} from "../../../src/db/revocation-upsert-order.js";

type RevocationLikeRow = {
  slot: bigint;
  txIndex: number;
  eventOrdinal: number;
  txSignature: string;
  status: string;
};

function applyUpsertsInOrder(rows: RevocationLikeRow[]): RevocationLikeRow | null {
  let current: RevocationLikeRow | null = null;
  for (const incoming of rows) {
    if (!current || shouldReplaceRevocationByEventOrder(current, incoming)) {
      current = incoming;
    }
  }
  return current;
}

describe("revocation upsert deterministic event ordering", () => {
  it("produces same final row regardless insertion order", () => {
    const older: RevocationLikeRow = {
      slot: 100n,
      txIndex: 1,
      eventOrdinal: 3,
      txSignature: "3abcOld",
      status: "ORPHANED",
    };
    const newer: RevocationLikeRow = {
      slot: 100n,
      txIndex: 1,
      eventOrdinal: 4,
      txSignature: "9xyzNew",
      status: "PENDING",
    };

    const forward = applyUpsertsInOrder([older, newer]);
    const reverse = applyUpsertsInOrder([newer, older]);

    expect(forward).toEqual(newer);
    expect(reverse).toEqual(newer);
  });

  it("uses tx_signature as tie-break when slot/tx_index/event_ordinal are equal", () => {
    const lowerSig: RevocationLikeRow = {
      slot: 222n,
      txIndex: 8,
      eventOrdinal: 2,
      txSignature: "2sigLower",
      status: "ORPHANED",
    };
    const higherSig: RevocationLikeRow = {
      slot: 222n,
      txIndex: 8,
      eventOrdinal: 2,
      txSignature: "7sigHigher",
      status: "PENDING",
    };

    expect(compareRevocationEventOrder(lowerSig, higherSig)).toBeLessThan(0);
    expect(compareRevocationEventOrder(higherSig, lowerSig)).toBeGreaterThan(0);
    expect(applyUpsertsInOrder([higherSig, lowerSig])).toEqual(higherSig);
    expect(applyUpsertsInOrder([lowerSig, higherSig])).toEqual(higherSig);
  });
});

describe("revocation upsert SQL guard wiring", () => {
  it("is applied in both supabase handler revocation upserts", () => {
    const supabaseSource = readFileSync(resolve(process.cwd(), "src/db/supabase.ts"), "utf8");
    const occurrences = supabaseSource.match(/\$\{REVOCATION_CONFLICT_UPDATE_WHERE_SQL\}/g) ?? [];
    expect(occurrences).toHaveLength(2);
  });

  it("is applied in batch supabase revocation upsert path", () => {
    const batchSource = readFileSync(resolve(process.cwd(), "src/indexer/batch-processor.ts"), "utf8");
    const occurrences = batchSource.match(/\$\{REVOCATION_CONFLICT_UPDATE_WHERE_SQL\}/g) ?? [];
    expect(occurrences).toHaveLength(1);
  });

  it("targets event key columns and tx_signature tie-break", () => {
    expect(REVOCATION_CONFLICT_UPDATE_WHERE_SQL).toContain("COALESCE(revocations.slot, -1)");
    expect(REVOCATION_CONFLICT_UPDATE_WHERE_SQL).toContain("COALESCE(revocations.tx_index, -1)");
    expect(REVOCATION_CONFLICT_UPDATE_WHERE_SQL).toContain("COALESCE(revocations.event_ordinal, -1)");
    expect(REVOCATION_CONFLICT_UPDATE_WHERE_SQL).toContain(
      "COALESCE(revocations.tx_signature, '') COLLATE \"C\" <= COALESCE(EXCLUDED.tx_signature, '') COLLATE \"C\""
    );
  });
});
