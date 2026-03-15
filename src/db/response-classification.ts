export type ResponseStatus = "ORPHANED" | "PENDING";

export function classifyResponseStatus(hasFeedback: boolean, sealMismatch = false): ResponseStatus {
  void sealMismatch;
  if (!hasFeedback) return "ORPHANED";
  return "PENDING";
}
