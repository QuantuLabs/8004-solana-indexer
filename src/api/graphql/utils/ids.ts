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
  return `${asset}${SEP}${client}${SEP}${String(index)}`;
}

export function decodeFeedbackId(
  id: string,
): { asset: string; client: string; index: string } | null {
  const parts = id.split(SEP);
  if (parts.length === 3) {
    const [asset, client, index] = parts;
    if (asset === SOL_PREFIX) return null;
    if (!asset || !client || !index) return null;
    return { asset, client, index };
  }
  if (parts.length === 4 && parts[0] === SOL_PREFIX) {
    const [, asset, client, index] = parts;
    if (!asset || !client || !index) return null;
    return { asset, client, index };
  }
  return null;
}

export function encodeResponseId(
  asset: string,
  client: string,
  index: bigint | string,
  responder: string,
  sequenceOrSig: bigint | string,
): string {
  return `${asset}${SEP}${client}${SEP}${String(index)}${SEP}${responder}${SEP}${String(sequenceOrSig)}`;
}

export function decodeResponseId(
  id: string,
): { asset: string; client: string; index: string; responder: string; sig: string } | null {
  const parts = id.split(SEP);
  if (parts.length === 5) {
    const [asset, client, index, responder, sig] = parts;
    if (asset === SOL_PREFIX) return null;
    if (!asset || !client || !index || !responder) return null;
    return { asset, client, index, responder, sig: sig ?? '' };
  }
  if (parts.length === 6 && parts[0] === SOL_PREFIX) {
    const [, asset, client, index, responder, sig] = parts;
    if (!asset || !client || !index || !responder) return null;
    return { asset, client, index, responder, sig: sig ?? '' };
  }
  return null;
}

export function encodeValidationId(
  asset: string,
  validator: string,
  nonce: bigint | string,
): string {
  return `${asset}${SEP}${validator}${SEP}${String(nonce)}`;
}

export function decodeValidationId(
  id: string,
): { asset: string; validator: string; nonce: string } | null {
  const parts = id.split(SEP);
  if (parts.length === 3) {
    const [asset, validator, nonce] = parts;
    if (asset === SOL_PREFIX) return null;
    if (!asset || !validator || !nonce) return null;
    return { asset, validator, nonce };
  }
  if (parts.length === 4 && parts[0] === SOL_PREFIX) {
    const [, asset, validator, nonce] = parts;
    if (!asset || !validator || !nonce) return null;
    return { asset, validator, nonce };
  }
  return null;
}

export function encodeMetadataId(asset: string, key: string): string {
  return `${asset}${SEP}${key}`;
}

export function decodeMetadataId(
  id: string,
): { asset: string; key: string } | null {
  const parts = id.split(SEP);
  if (parts.length === 2) {
    const [asset, key] = parts;
    if (asset === SOL_PREFIX) return null;
    if (!asset || !key) return null;
    return { asset, key };
  }
  if (parts.length === 3 && parts[0] === SOL_PREFIX) {
    const [, asset, key] = parts;
    if (!asset || !key) return null;
    return { asset, key };
  }
  return null;
}
