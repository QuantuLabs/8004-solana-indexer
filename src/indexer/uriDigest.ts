/**
 * URI Digest Module
 * Fetches agent URI metadata and extracts standard fields for indexing
 */

import { createHash } from "crypto";
import { lookup } from "dns/promises";
import { config } from "../config.js";
import { createChildLogger } from "../logger.js";

const logger = createChildLogger("uri-digest");

// SSRF protection: blocked hostnames and IP patterns
const BLOCKED_HOSTS = new Set([
  "localhost",
  "127.0.0.1",
  "0.0.0.0",
  "[::1]",
  "metadata.google.internal",
  "169.254.169.254", // AWS/GCP metadata
]);

// IPv4 private ranges (dotted-decimal only - normalization handles other forms)
const PRIVATE_IPV4_RANGES = [
  /^10\./,                    // 10.0.0.0/8
  /^172\.(1[6-9]|2[0-9]|3[01])\./, // 172.16.0.0/12
  /^192\.168\./,              // 192.168.0.0/16
  /^169\.254\./,              // Link-local
  /^127\./,                   // Loopback
  /^0\./,                     // Current network
];

// IPv6 private ranges (fc00::/7 = fc00-fdff, fe80::/10 = link-local)
const PRIVATE_IPV6_RANGES = [
  /^f[cd][0-9a-f]{2}:/i,      // IPv6 unique local (fc00::/7)
  /^fe80:/i,                  // IPv6 link-local
];

/**
 * Check if IPv6 address is loopback (::1) or unspecified (::)
 * Handles all textual forms: 0:0:0:0:0:0:0:1, ::0001, ::1, etc.
 */
function isIPv6LoopbackOrUnspecified(ipv6: string): boolean {
  // Remove brackets if present
  let addr = ipv6.startsWith('[') ? ipv6.slice(1, -1) : ipv6;

  // Strip zone ID
  const zoneIdx = addr.indexOf('%');
  if (zoneIdx !== -1) addr = addr.slice(0, zoneIdx);

  // Reject multiple :: (invalid IPv6)
  if ((addr.match(/::/g) || []).length > 1) return false;

  // Parse segments, handling :: compression
  const segments: number[] = [];

  if (addr.includes('::')) {
    // Split on :: to get left and right parts
    const [left, right] = addr.split('::');
    const leftParts = left ? left.split(':') : [];
    const rightParts = right ? right.split(':') : [];

    // Parse left segments
    for (const p of leftParts) {
      const val = parseInt(p, 16);
      if (isNaN(val) || val < 0 || val > 0xffff) return false;
      segments.push(val);
    }

    // Fill zeros for :: compression
    const zerosNeeded = 8 - leftParts.length - rightParts.length;
    for (let i = 0; i < zerosNeeded; i++) {
      segments.push(0);
    }

    // Parse right segments
    for (const p of rightParts) {
      const val = parseInt(p, 16);
      if (isNaN(val) || val < 0 || val > 0xffff) return false;
      segments.push(val);
    }
  } else {
    // No compression, split directly
    const parts = addr.split(':');
    if (parts.length !== 8) return false;
    for (const p of parts) {
      const val = parseInt(p, 16);
      if (isNaN(val) || val < 0 || val > 0xffff) return false;
      segments.push(val);
    }
  }

  if (segments.length !== 8) return false;

  // Check for loopback (::1) - all zeros except last is 1
  const isLoopback = segments.slice(0, 7).every(s => s === 0) && segments[7] === 1;

  // Check for unspecified (::) - all zeros
  const isUnspecified = segments.every(s => s === 0);

  return isLoopback || isUnspecified;
}

/**
 * Canonicalize IP address to standard dotted-decimal (IPv4) or detect IPv6 private
 * Handles: IPv4-mapped IPv6 (all forms), hex IPv4, decimal IPv4, octal IPv4, shorthand
 * Returns { ipv4: string } for IPv4 addresses (including mapped)
 * Returns { ipv6: string } for native IPv6 addresses
 * Returns null if not a recognized IP format
 */
function canonicalizeIP(ip: string): { ipv4: string } | { ipv6: string } | null {
  let lower = ip.toLowerCase();

  // Strip IPv6 zone ID (%interface) - e.g., ::1%lo0 or ::1%25lo0 (URL-encoded)
  const zoneIndex = lower.indexOf('%');
  if (zoneIndex !== -1) {
    lower = lower.slice(0, zoneIndex);
  }

  // Handle IPv4-mapped IPv6 in dotted form: ::ffff:127.0.0.1
  const mappedDottedMatch = lower.match(/^::ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/);
  if (mappedDottedMatch) {
    const normalized = canonicalizeIP(mappedDottedMatch[1]);
    return normalized && 'ipv4' in normalized ? normalized : null;
  }

  // Handle IPv4-mapped IPv6 full form: 0:0:0:0:0:ffff:7f00:1 or 0000:0000:0000:0000:0000:ffff:7f00:0001
  const mappedFullMatch = lower.match(/^(?:0+:){5}ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/);
  if (mappedFullMatch) {
    const high = parseInt(mappedFullMatch[1], 16);
    const low = parseInt(mappedFullMatch[2], 16);
    if (high >= 0 && high <= 0xffff && low >= 0 && low <= 0xffff) {
      const ipv4 = `${(high >> 8) & 0xff}.${high & 0xff}.${(low >> 8) & 0xff}.${low & 0xff}`;
      return { ipv4 };
    }
  }

  // Handle IPv4-mapped IPv6 full form with dotted: 0:0:0:0:0:ffff:127.0.0.1
  const mappedFullDottedMatch = lower.match(/^(?:0+:){5}ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/);
  if (mappedFullDottedMatch) {
    const normalized = canonicalizeIP(mappedFullDottedMatch[1]);
    return normalized && 'ipv4' in normalized ? normalized : null;
  }

  // Handle IPv4-mapped IPv6 in hex form: ::ffff:7f00:1 or ::ffff:7f00:0001
  const mappedHexMatch = lower.match(/^::ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/);
  if (mappedHexMatch) {
    const high = parseInt(mappedHexMatch[1], 16);
    const low = parseInt(mappedHexMatch[2], 16);
    if (high >= 0 && high <= 0xffff && low >= 0 && low <= 0xffff) {
      const ipv4 = `${(high >> 8) & 0xff}.${high & 0xff}.${(low >> 8) & 0xff}.${low & 0xff}`;
      return { ipv4 };
    }
  }

  // Handle IPv4-compatible IPv6 (deprecated but still routable): ::127.0.0.1
  const compatibleMatch = lower.match(/^::(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/);
  if (compatibleMatch) {
    const normalized = canonicalizeIP(compatibleMatch[1]);
    return normalized && 'ipv4' in normalized ? normalized : null;
  }

  // Detect hex IPv4 (0x7f000001)
  if (/^0x[0-9a-f]+$/i.test(lower)) {
    const num = parseInt(lower, 16);
    if (num >= 0 && num <= 0xffffffff) {
      return { ipv4: `${(num >> 24) & 0xff}.${(num >> 16) & 0xff}.${(num >> 8) & 0xff}.${num & 0xff}` };
    }
  }

  // Detect decimal IPv4 (2130706433)
  if (/^\d+$/.test(lower) && !lower.includes('.')) {
    const num = parseInt(lower, 10);
    if (num >= 0 && num <= 0xffffffff) {
      return { ipv4: `${(num >> 24) & 0xff}.${(num >> 16) & 0xff}.${(num >> 8) & 0xff}.${num & 0xff}` };
    }
  }

  // Handle IPv4 shorthand forms: 127.1 -> 127.0.0.1, 127.0.1 -> 127.0.0.1
  const shorthandMatch = lower.match(/^(\d{1,3})\.(\d{1,3})$/);
  if (shorthandMatch) {
    const a = parseInt(shorthandMatch[1], 10);
    const d = parseInt(shorthandMatch[2], 10);
    if (a >= 0 && a <= 255 && d >= 0 && d <= 255) {
      return { ipv4: `${a}.0.0.${d}` };
    }
  }

  const shorthandMatch3 = lower.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
  if (shorthandMatch3) {
    const a = parseInt(shorthandMatch3[1], 10);
    const b = parseInt(shorthandMatch3[2], 10);
    const d = parseInt(shorthandMatch3[3], 10);
    if (a >= 0 && a <= 255 && b >= 0 && b <= 255 && d >= 0 && d <= 255) {
      return { ipv4: `${a}.${b}.0.${d}` };
    }
  }

  // Detect octal IPv4 (0177.0.0.1)
  if (/^0\d+\./.test(lower)) {
    const parts = lower.split('.');
    if (parts.length === 4) {
      const normalized = parts.map(p => parseInt(p, p.startsWith('0') && p.length > 1 ? 8 : 10));
      if (normalized.every(n => n >= 0 && n <= 255)) {
        return { ipv4: normalized.join('.') };
      }
    }
  }

  // Standard dotted-decimal IPv4
  if (/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(lower)) {
    return { ipv4: lower };
  }

  // Native IPv6 (contains colon, not IPv4-mapped)
  if (lower.includes(':')) {
    return { ipv6: lower };
  }

  return null;
}

function isBlockedHost(hostname: string): boolean {
  const lower = hostname.toLowerCase();
  if (BLOCKED_HOSTS.has(lower)) return true;

  // Canonicalize and check against private ranges
  // This handles: IPv4-mapped IPv6 (hex and dotted), hex/decimal/octal IPv4
  return isPrivateIP(lower);
}

/**
 * Check if an IP address is private/internal
 * Canonicalizes the IP first to catch obfuscated forms
 */
function isPrivateIP(ip: string): boolean {
  // Check blocked hosts first
  if (BLOCKED_HOSTS.has(ip.toLowerCase())) return true;

  // Canonicalize to detect obfuscated forms
  const canonical = canonicalizeIP(ip);

  if (canonical && 'ipv4' in canonical) {
    // Check IPv4 against private ranges
    for (const pattern of PRIVATE_IPV4_RANGES) {
      if (pattern.test(canonical.ipv4)) return true;
    }
  } else if (canonical && 'ipv6' in canonical) {
    // Check IPv6 loopback/unspecified (handles all forms: 0:0:0:0:0:0:0:1, ::1, etc.)
    if (isIPv6LoopbackOrUnspecified(canonical.ipv6)) return true;

    // Check IPv6 against private ranges
    for (const pattern of PRIVATE_IPV6_RANGES) {
      if (pattern.test(canonical.ipv6)) return true;
    }
  }

  return false;
}

/**
 * Resolve hostname to IP and validate it's not private
 * Returns the first valid public IP for pinning, or null if blocked
 * Prevents DNS rebinding attacks by returning the resolved IP to use for fetch
 */
async function validateHostResolution(hostname: string): Promise<{ ip: string; family: number } | null> {
  try {
    // Skip validation and pinning for known safe gateways
    // These are trusted CDNs that handle their own security
    if (hostname === "ipfs.io" || hostname === "arweave.net") {
      return { ip: hostname, family: 0 }; // family 0 = use hostname directly
    }

    const result = await lookup(hostname, { all: true });

    // Find first non-private IP to use for pinning
    for (const record of result) {
      if (isPrivateIP(record.address)) {
        logger.warn({ hostname, ip: record.address }, "DNS resolved to private IP");
        continue; // Try next record
      }
      // Found a valid public IP - return it for pinning
      return { ip: record.address, family: record.family };
    }

    // All resolved IPs were private
    logger.warn({ hostname }, "All DNS records resolved to private IPs");
    return null;
  } catch (error) {
    // DNS resolution failed - block by default (fail-closed)
    // This prevents attacks where DNS times out but later resolves to private IP
    logger.warn({ hostname, error: String(error) }, "DNS resolution failed, blocking for safety");
    return null;
  }
}

export type DigestStatus = "ok" | "timeout" | "error" | "oversize" | "invalid_json" | "blocked";

export interface UriDigestResult {
  status: DigestStatus;
  error?: string;
  bytes?: number;
  hash?: string;
  fields?: Record<string, unknown>;
  truncatedKeys?: boolean;
}

/**
 * Standard ERC-8004 registration file fields
 * Maps builder field names to metadata keys
 */
// Prefix "_uri:" for indexer-derived metadata to avoid collision with on-chain metadata
// Users can set on-chain metadata with any key, so we use underscore prefix for internal keys
const STANDARD_FIELDS: Record<string, string> = {
  type: "_uri:type",
  name: "_uri:name",
  description: "_uri:description",
  image: "_uri:image",
  endpoints: "_uri:endpoints",
  registrations: "_uri:registrations",
  supportedTrusts: "_uri:supported_trusts",
  active: "_uri:active",
  x402support: "_uri:x402_support",
  skills: "_uri:skills",
  domains: "_uri:domains",
};

// Maximum redirect depth to prevent infinite loops
const MAX_REDIRECT_DEPTH = 5;

/**
 * Fetch and digest URI metadata
 * @param uri - The URI to fetch (IPFS, HTTPS, etc.)
 * @param redirectDepth - Current redirect depth (internal use)
 * @returns Digest result with status, fields, and metadata
 */
export async function digestUri(uri: string, redirectDepth: number = 0): Promise<UriDigestResult> {
  if (!uri) {
    return { status: "error", error: "Empty URI" };
  }

  // Prevent redirect loops
  if (redirectDepth >= MAX_REDIRECT_DEPTH) {
    return { status: "error", error: "Too many redirects" };
  }

  // Convert IPFS URI to gateway URL
  const fetchUrl = convertToFetchUrl(uri);
  if (!fetchUrl) {
    return { status: "error", error: "Unsupported URI scheme" };
  }

  // SSRF protection: block private/internal hosts
  let url: URL;
  try {
    url = new URL(fetchUrl);
    if (isBlockedHost(url.hostname)) {
      logger.warn({ uri, hostname: url.hostname }, "Blocked SSRF attempt");
      return { status: "blocked", error: "Internal host blocked" };
    }
  } catch {
    return { status: "error", error: "Invalid URL" };
  }

  // DNS resolution check to prevent rebinding attacks - returns pinned IP
  const resolved = await validateHostResolution(url.hostname);
  if (!resolved) {
    return { status: "blocked", error: "DNS resolved to private IP" };
  }

  // Build fetch URL with pinned IP to prevent DNS rebinding
  // family 0 means use hostname directly (trusted gateways)
  let pinnedFetchUrl = fetchUrl;
  const originalHost = url.hostname;
  if (resolved.family !== 0) {
    // Replace hostname with resolved IP
    const pinnedUrl = new URL(fetchUrl);
    pinnedUrl.hostname = resolved.family === 6 ? `[${resolved.ip}]` : resolved.ip;
    pinnedFetchUrl = pinnedUrl.toString();
  }

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), config.metadataTimeoutMs);

  try {
    // Disable automatic redirects to prevent SSRF via redirect
    const response = await fetch(pinnedFetchUrl, {
      signal: controller.signal,
      redirect: "manual", // Don't follow redirects automatically
      headers: {
        Accept: "application/json",
        "User-Agent": "8004-Indexer/1.0",
        // Set Host header when using pinned IP for virtual hosting
        ...(resolved.family !== 0 && { Host: originalHost }),
      },
    });

    // Handle redirects manually with validation
    if (response.status >= 300 && response.status < 400) {
      const location = response.headers.get("location");
      if (!location) {
        return { status: "error", error: `Redirect ${response.status} without location` };
      }
      try {
        const redirectUrl = new URL(location, fetchUrl);
        if (isBlockedHost(redirectUrl.hostname)) {
          logger.warn({ uri, redirectTo: redirectUrl.hostname }, "Blocked redirect to internal host");
          return { status: "blocked", error: "Redirect to internal host blocked" };
        }
        // Validate redirect target DNS
        const redirectDnsValid = await validateHostResolution(redirectUrl.hostname);
        if (!redirectDnsValid) {
          return { status: "blocked", error: "Redirect DNS resolved to private IP" };
        }
        // Follow the redirect with same safety checks (increment depth)
        return digestUri(redirectUrl.toString(), redirectDepth + 1);
      } catch {
        return { status: "error", error: "Invalid redirect URL" };
      }
    }

    if (!response.ok) {
      return { status: "error", error: `HTTP ${response.status}` };
    }

    // Check content length before reading body
    const contentLength = response.headers.get("content-length");
    if (contentLength && parseInt(contentLength, 10) > config.metadataMaxBytes) {
      return {
        status: "oversize",
        bytes: parseInt(contentLength, 10),
      };
    }

    // Read body with size limit
    const reader = response.body?.getReader();
    if (!reader) {
      return { status: "error", error: "No response body" };
    }

    const chunks: Uint8Array[] = [];
    let totalBytes = 0;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      totalBytes += value.length;
      if (totalBytes > config.metadataMaxBytes) {
        reader.cancel();
        return { status: "oversize", bytes: totalBytes };
      }
      chunks.push(value);
    }

    // Combine chunks and parse JSON
    const buffer = Buffer.concat(chunks);
    const text = buffer.toString("utf-8");
    const hash = createHash("sha256").update(buffer).digest("hex");

    let json: Record<string, unknown>;
    try {
      json = JSON.parse(text);
    } catch {
      return { status: "invalid_json", bytes: totalBytes, hash };
    }

    // Extract standard fields
    const fields: Record<string, unknown> = {};
    const standardKeys = new Set(Object.keys(STANDARD_FIELDS));

    for (const [jsonKey, metaKey] of Object.entries(STANDARD_FIELDS)) {
      if (json[jsonKey] !== undefined) {
        fields[metaKey] = json[jsonKey];
      }
    }

    // In "full" mode, store unknown keys individually (with DoS protection)
    let truncatedKeys = false;
    if (config.metadataIndexMode === "full") {
      const MAX_EXTRA_KEYS = 50; // Protection against malicious JSON with many keys
      let extraKeyCount = 0;

      for (const [key, value] of Object.entries(json)) {
        if (!standardKeys.has(key)) {
          if (extraKeyCount >= MAX_EXTRA_KEYS) {
            logger.warn({ uri }, `Exceeded ${MAX_EXTRA_KEYS} extra keys, truncating`);
            truncatedKeys = true;
            break;
          }
          // Store with _uri: prefix (internal) to avoid collision with on-chain metadata
          fields[`_uri:${key}`] = value;
          extraKeyCount++;
        }
      }
    }

    return {
      status: "ok",
      bytes: totalBytes,
      hash,
      fields,
      truncatedKeys,
    };
  } catch (error: unknown) {
    if (error instanceof Error) {
      if (error.name === "AbortError") {
        return { status: "timeout" };
      }
      return { status: "error", error: error.message };
    }
    return { status: "error", error: String(error) };
  } finally {
    // Always clear timeout to prevent resource leak
    clearTimeout(timeoutId);
  }
}

/**
 * Convert various URI formats to fetchable URLs
 */
function convertToFetchUrl(uri: string): string | null {
  // IPFS URI
  if (uri.startsWith("ipfs://")) {
    const cid = uri.slice(7);
    return `https://ipfs.io/ipfs/${cid}`;
  }

  // IPFS path
  if (uri.startsWith("/ipfs/")) {
    return `https://ipfs.io${uri}`;
  }

  // Arweave
  if (uri.startsWith("ar://")) {
    const id = uri.slice(5);
    return `https://arweave.net/${id}`;
  }

  // HTTPS
  if (uri.startsWith("https://")) {
    return uri;
  }

  // HTTP (only for local testing, not recommended)
  if (uri.startsWith("http://")) {
    logger.warn({ uri }, "HTTP URI used (insecure)");
    return uri;
  }

  return null;
}

/**
 * Serialize a value for storage
 * Returns null if value exceeds maxBytes
 */
export function serializeValue(
  value: unknown,
  maxBytes: number
): { value: string; oversize: boolean; bytes: number } {
  const serialized = typeof value === "string" ? value : JSON.stringify(value);
  const bytes = Buffer.byteLength(serialized, "utf-8");

  if (bytes > maxBytes) {
    return { value: "", oversize: true, bytes };
  }

  return { value: serialized, oversize: false, bytes };
}
