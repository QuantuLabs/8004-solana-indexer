export type RevocationStatus = "ORPHANED" | "PENDING";

export function classifyRevocationStatus(
  hasFeedback: boolean,
  sealMismatch = false,
): RevocationStatus {
  void sealMismatch;
  return hasFeedback ? "PENDING" : "ORPHANED";
}
