export type CanonicalTransactionOrderKey = {
  slot: bigint | number;
  txIndex: number | null | undefined;
  signature: string;
};

export function compareCanonicalTransactionOrder(
  a: CanonicalTransactionOrderKey,
  b: CanonicalTransactionOrderKey
): number {
  const aSlot = typeof a.slot === "bigint" ? a.slot : BigInt(a.slot);
  const bSlot = typeof b.slot === "bigint" ? b.slot : BigInt(b.slot);
  if (aSlot !== bSlot) {
    return aSlot < bSlot ? -1 : 1;
  }

  const aTxIndex = a.txIndex ?? Number.MAX_SAFE_INTEGER;
  const bTxIndex = b.txIndex ?? Number.MAX_SAFE_INTEGER;
  if (aTxIndex !== bTxIndex) {
    return aTxIndex - bTxIndex;
  }

  if (a.signature === b.signature) {
    return 0;
  }
  return a.signature < b.signature ? -1 : 1;
}
