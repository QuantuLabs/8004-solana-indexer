export interface RevocationEventOrderKey {
  slot: bigint | number | string | null | undefined;
  txIndex: number | bigint | string | null | undefined;
  eventOrdinal: number | bigint | string | null | undefined;
  txSignature: string | null | undefined;
}

function normalizeSlot(value: RevocationEventOrderKey["slot"]): bigint {
  if (typeof value === "bigint") return value;
  if (typeof value === "number" && Number.isFinite(value)) return BigInt(Math.trunc(value));
  if (typeof value === "string" && /^-?\d+$/.test(value.trim())) return BigInt(value.trim());
  return -1n;
}

function normalizeEventIndex(
  value: RevocationEventOrderKey["txIndex"] | RevocationEventOrderKey["eventOrdinal"]
): number {
  if (typeof value === "number" && Number.isFinite(value)) return Math.trunc(value);
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "string" && /^-?\d+$/.test(value.trim())) return parseInt(value.trim(), 10);
  return -1;
}

function normalizeSignature(value: RevocationEventOrderKey["txSignature"]): string {
  if (typeof value !== "string") return "";
  return value;
}

export function compareRevocationEventOrder(a: RevocationEventOrderKey, b: RevocationEventOrderKey): number {
  const aSlot = normalizeSlot(a.slot);
  const bSlot = normalizeSlot(b.slot);
  if (aSlot !== bSlot) return aSlot < bSlot ? -1 : 1;

  const aTxIndex = normalizeEventIndex(a.txIndex);
  const bTxIndex = normalizeEventIndex(b.txIndex);
  if (aTxIndex !== bTxIndex) return aTxIndex < bTxIndex ? -1 : 1;

  const aEventOrdinal = normalizeEventIndex(a.eventOrdinal);
  const bEventOrdinal = normalizeEventIndex(b.eventOrdinal);
  if (aEventOrdinal !== bEventOrdinal) return aEventOrdinal < bEventOrdinal ? -1 : 1;

  const aSignature = normalizeSignature(a.txSignature);
  const bSignature = normalizeSignature(b.txSignature);
  if (aSignature < bSignature) return -1;
  if (aSignature > bSignature) return 1;
  return 0;
}

export function shouldReplaceRevocationByEventOrder(
  existing: RevocationEventOrderKey,
  incoming: RevocationEventOrderKey
): boolean {
  return compareRevocationEventOrder(existing, incoming) <= 0;
}

export const REVOCATION_CONFLICT_UPDATE_WHERE_SQL = `WHERE (
       COALESCE(revocations.slot, -1) < COALESCE(EXCLUDED.slot, -1)
       OR (
         COALESCE(revocations.slot, -1) = COALESCE(EXCLUDED.slot, -1)
         AND (
           COALESCE(revocations.tx_index, -1) < COALESCE(EXCLUDED.tx_index, -1)
           OR (
             COALESCE(revocations.tx_index, -1) = COALESCE(EXCLUDED.tx_index, -1)
             AND (
               COALESCE(revocations.event_ordinal, -1) < COALESCE(EXCLUDED.event_ordinal, -1)
               OR (
                 COALESCE(revocations.event_ordinal, -1) = COALESCE(EXCLUDED.event_ordinal, -1)
                 AND COALESCE(revocations.tx_signature, '') COLLATE "C" <= COALESCE(EXCLUDED.tx_signature, '') COLLATE "C"
               )
             )
           )
         )
       )
     )`;
