import bs58 from 'bs58';

const SOL_PREFIX = 'sol';
const SEP = ':';

export function encodeAgentId(asset: string): string {
  return asset;
}

export function decodeAgentId(id: string): string | null {
  if (!id) return null;

  // Primary v2 model: Agent.id is the raw asset pubkey string.
  if (!id.includes(SEP)) return id;

  // Backward-compatible decode for legacy sol:<asset> inputs.
  const parts = id.split(SEP);
  if (parts.length !== 2 || parts[0] !== SOL_PREFIX || !parts[1]) return null;
  return parts[1];
}

export function encodeFeedbackId(
  asset: string,
  client: string,
  index: bigint | string,
): string {
  return `${SOL_PREFIX}${SEP}${asset}${SEP}${client}${SEP}${String(index)}`;
}

export function decodeFeedbackId(
  id: string,
): { asset: string; client: string; index: string } | null {
  const parts = id.split(SEP);
  if (parts.length !== 4 || parts[0] !== SOL_PREFIX) return null;
  if (!parts[1] || !parts[2] || !parts[3]) return null;
  return { asset: parts[1], client: parts[2], index: parts[3] };
}

export function encodeResponseId(
  asset: string,
  client: string,
  index: bigint | string,
  responder: string,
  txSig: string,
): string {
  return `${SOL_PREFIX}${SEP}${asset}${SEP}${client}${SEP}${String(index)}${SEP}${responder}${SEP}${txSig}`;
}

export function decodeResponseId(
  id: string,
): { asset: string; client: string; index: string; responder: string; sig: string } | null {
  const parts = id.split(SEP);
  if (parts.length !== 6 || parts[0] !== SOL_PREFIX) return null;
  if (!parts[1] || !parts[2] || !parts[3] || !parts[4]) return null;
  return {
    asset: parts[1],
    client: parts[2],
    index: parts[3],
    responder: parts[4],
    sig: parts[5] ?? '',
  };
}

export function encodeValidationId(
  asset: string,
  validator: string,
  nonce: bigint | string,
): string {
  return `${SOL_PREFIX}${SEP}${asset}${SEP}${validator}${SEP}${String(nonce)}`;
}

export function decodeValidationId(
  id: string,
): { asset: string; validator: string; nonce: string } | null {
  const parts = id.split(SEP);
  if (parts.length !== 4 || parts[0] !== SOL_PREFIX) return null;
  if (!parts[1] || !parts[2] || !parts[3]) return null;
  return { asset: parts[1], validator: parts[2], nonce: parts[3] };
}

export function encodeMetadataId(asset: string, key: string): string {
  return `${SOL_PREFIX}${SEP}${asset}${SEP}${key}`;
}

export function decodeMetadataId(
  id: string,
): { asset: string; key: string } | null {
  const parts = id.split(SEP);
  if (parts.length !== 3 || parts[0] !== SOL_PREFIX) return null;
  if (!parts[1] || !parts[2]) return null;
  return { asset: parts[1], key: parts[2] };
}

export function numericAgentId(asset: string): bigint {
  try {
    const bytes = bs58.decode(asset);
    if (bytes.length < 8) return 0n;
    const view = new DataView(bytes.buffer, bytes.byteOffset, 8);
    return view.getBigUint64(0, false);
  } catch {
    return 0n;
  }
}
