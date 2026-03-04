export type RevocationStatus = "ORPHANED" | "PENDING";

/**
 * Revocation classification is based only on feedback presence.
 * A seal hash mismatch is logged but remains non-orphan (PENDING).
 */
export function classifyRevocationStatus(hasFeedback: boolean): RevocationStatus {
  return hasFeedback ? "PENDING" : "ORPHANED";
}
