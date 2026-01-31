/**
 * Sanitize binary data for PostgreSQL TEXT storage
 * Strips NULL bytes and other control characters that break UTF-8 encoding
 */

/**
 * Control characters that break PostgreSQL TEXT columns:
 * 0x00 (NULL) - causes "invalid byte sequence for encoding UTF8: 0x00"
 * 0x01-0x08, 0x0B, 0x0C, 0x0E-0x1F - other control chars
 *
 * We keep: 0x09 (tab), 0x0A (newline), 0x0D (carriage return)
 */
function isValidUtf8Byte(byte: number): boolean {
  // Allow printable ASCII (0x20-0x7E) and valid control chars
  if (byte >= 0x20) return true;
  // Allow tab, newline, carriage return
  if (byte === 0x09 || byte === 0x0A || byte === 0x0D) return true;
  // Allow UTF-8 continuation bytes (0x80-0xFF)
  if (byte >= 0x80) return true;
  return false;
}

/**
 * Strip NULL bytes and invalid control characters from a Uint8Array
 * Returns a clean buffer safe for PostgreSQL TEXT storage
 */
export function stripNullBytes(data: Uint8Array): Buffer {
  const result: number[] = [];
  for (let i = 0; i < data.length; i++) {
    if (isValidUtf8Byte(data[i])) {
      result.push(data[i]);
    }
  }
  return Buffer.from(result);
}

/**
 * Check if a Uint8Array contains NULL bytes or invalid control characters
 */
export function hasNullBytes(data: Uint8Array): boolean {
  for (let i = 0; i < data.length; i++) {
    if (!isValidUtf8Byte(data[i])) {
      return true;
    }
  }
  return false;
}
