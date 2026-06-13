/**
 * URI Digest Module
 * Fetches agent URI metadata and extracts standard fields for indexing
 */

import { createHash } from "crypto";
import DOMPurify from "isomorphic-dompurify";
import { config } from "../config.js";
import { createChildLogger } from "../logger.js";
import { hardenedFetchBuffer, shouldAllowTrustedLocalHostForUri } from "./hardenedFetch.js";

const logger = createChildLogger("uri-digest");

// ============================================================================
// XSS/Injection Protection
// ============================================================================

/** Allowed URL protocols for metadata fields (image, endpoints, etc.) */
const ALLOWED_URL_PROTOCOLS = new Set(["https:", "http:", "ipfs:", "ar:"]);
const ENS_NAME_REGEX = /^(?=.{1,255}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$/;
const SNS_NAME_REGEX = /^(?=.{1,255}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+sol$/;
const DID_URI_REGEX = /^did:[a-z0-9]+:[A-Za-z0-9._:%-]+(?::[A-Za-z0-9._:%-]+)*(?:[/?#][^\s]*)?$/;
const EIP155_ACCOUNT_REGEX = /^eip155:\d+:0x[a-fA-F0-9]{40}$/;

/** Max string length before DOMPurify (prevent CPU exhaustion from nested HTML) */
const MAX_SANITIZE_INPUT_LENGTH = 1000;

/**
 * Sanitize text fields - strips all HTML tags and dangerous content
 * Used for: name, description, type, and other text fields
 * Truncates input to prevent CPU exhaustion from deeply nested HTML
 */
export function sanitizeText(text: string): string {
  if (!text || typeof text !== "string") return "";
  // Truncate before DOMPurify to prevent CPU exhaustion (ReDoS-like with nested HTML)
  const truncated = text.length > MAX_SANITIZE_INPUT_LENGTH
    ? text.slice(0, MAX_SANITIZE_INPUT_LENGTH)
    : text;
  // DOMPurify with ALLOWED_TAGS=[] strips all HTML, leaving plain text
  const cleaned = DOMPurify.sanitize(truncated, { ALLOWED_TAGS: [], ALLOWED_ATTR: [] }).trim();
  // Strip NULL bytes and control chars that break PostgreSQL UTF-8 (keep tab, newline, CR)
  return cleaned.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, "");
}

/**
 * Validate and sanitize URL fields
 * Only allows https, http, ipfs, ar protocols
 * Returns empty string if URL is invalid or uses forbidden protocol
 */
export function sanitizeUrl(url: string): string {
  if (!url || typeof url !== "string") return "";

  // Truncate before DOMPurify to prevent CPU exhaustion (same as sanitizeText)
  const truncated = url.length > MAX_SANITIZE_INPUT_LENGTH
    ? url.slice(0, MAX_SANITIZE_INPUT_LENGTH)
    : url;

  // Handle IPFS and Arweave URIs (non-standard protocol format)
  if (truncated.startsWith("ipfs://") || truncated.startsWith("ar://")) {
    // Strip any HTML/script injection attempts in the path
    const sanitized = DOMPurify.sanitize(truncated, { ALLOWED_TAGS: [], ALLOWED_ATTR: [] }).trim();
    // Verify it's still a valid URI after sanitization and has a valid path
    if (sanitized.startsWith("ipfs://") && sanitized.length > 7) {
      // IPFS CID must be alphanumeric (base58/base32)
      const cid = sanitized.slice(7);
      if (/^[a-zA-Z0-9]+$/.test(cid.split("/")[0])) {
        return sanitized;
      }
    }
    if (sanitized.startsWith("ar://") && sanitized.length > 5) {
      // Arweave TX ID is base64url
      const txId = sanitized.slice(5);
      if (/^[a-zA-Z0-9_-]+$/.test(txId.split("/")[0])) {
        return sanitized;
      }
    }
    return "";
  }

  try {
    const parsed = new URL(truncated);
    if (!ALLOWED_URL_PROTOCOLS.has(parsed.protocol)) {
      logger.warn({ url: truncated, protocol: parsed.protocol }, "Blocked URL with forbidden protocol");
      return "";
    }
    // Return the normalized URL (handles encoding issues)
    return parsed.toString();
  } catch {
    logger.warn({ url: truncated }, "Invalid URL format");
    return "";
  }
}

function sanitizeEnsName(value: string): string {
  const candidate = sanitizeText(value).toLowerCase();
  return ENS_NAME_REGEX.test(candidate) ? candidate : "";
}

function sanitizeSnsName(value: string): string {
  const candidate = sanitizeText(value).toLowerCase();
  return SNS_NAME_REGEX.test(candidate) ? candidate : "";
}

function sanitizeDidUri(value: string): string {
  const candidate = sanitizeText(value);
  if (!candidate || /\s/.test(candidate)) return "";
  return DID_URI_REGEX.test(candidate) ? candidate : "";
}

/**
 * Sanitize services array (ERC-8004 spec)
 * Format: [{ name: "MCP"|"A2A"|"OASF"|..., endpoint: "https://...", version: "1.0", ... }]
 */
function sanitizeServices(arr: unknown): Array<Record<string, unknown>> {
  if (!Array.isArray(arr)) return [];

  const validServiceNames = new Set(["mcp", "a2a", "oasf", "ens", "sns", "did", "agentwallet", "wallet"]);
  const slugRegex = /^[a-z0-9]+(?:[a-z0-9_-]*[a-z0-9])?(?:\/[a-z0-9]+(?:[a-z0-9_-]*[a-z0-9])?)*$/;

  // SLICE FIRST to prevent CPU exhaustion via large arrays
  return arr
    .slice(0, 20) // Limit BEFORE processing
    .filter((item): item is Record<string, unknown> => typeof item === "object" && item !== null)
    .map((item) => {
      const service: Record<string, unknown> = {};

      // Required: canonical service kind from name or type.
      const normalizedName = typeof item.name === "string"
        ? sanitizeText(item.name.toLowerCase())
        : "";
      const normalizedType = typeof item.type === "string"
        ? sanitizeText(item.type.toLowerCase())
        : "";
      const hasValidName = validServiceNames.has(normalizedName);
      const hasValidType = validServiceNames.has(normalizedType);
      const serviceKind = hasValidName
        ? normalizedName
        : hasValidType
          ? normalizedType
          : "";
      if (!serviceKind) return null;
      service.name = serviceKind;
      if (normalizedType && normalizedType === serviceKind) {
        service.type = normalizedType;
      }

      // Required: endpoint URL
      if (typeof item.endpoint === "string") {
        const canonicalServiceName = service.name as string;
        const isWalletService = canonicalServiceName === "agentwallet" || canonicalServiceName === "wallet";
        const isEnsService = canonicalServiceName === "ens";
        const isSnsService = canonicalServiceName === "sns";
        const isDidService = canonicalServiceName === "did";
        const endpointRef = sanitizeText(item.endpoint);
        const endpoint = isWalletService
          ? (EIP155_ACCOUNT_REGEX.test(endpointRef) ? endpointRef : "")
          : isEnsService
            ? sanitizeEnsName(endpointRef)
            : isSnsService
              ? sanitizeSnsName(endpointRef)
            : isDidService
              ? sanitizeDidUri(endpointRef)
              : sanitizeUrl(item.endpoint);
        if (endpoint) {
          service.endpoint = endpoint;
        } else {
          return null; // Invalid endpoint
        }
      }

      // Optional: version
      if (typeof item.version === "string") {
        service.version = sanitizeText(item.version).slice(0, 20);
      }

      // Optional: mcpTools (array of tool names for MCP services)
      if (Array.isArray(item.mcpTools)) {
        service.mcpTools = item.mcpTools
          .filter((t): t is string => typeof t === "string")
          .map((t) => sanitizeText(t))
          .filter((t) => t.length > 0)
          .slice(0, 100);
      }

      // Optional: a2aSkills (array of skill paths for A2A services)
      if (Array.isArray(item.a2aSkills)) {
        service.a2aSkills = item.a2aSkills
          .filter((s): s is string => typeof s === "string")
          .map((s) => sanitizeText(s))
          .filter((s) => s.length > 0)
          .slice(0, 100);
      }

      // Optional: skills (OASF taxonomy)
      if (Array.isArray(item.skills)) {
        service.skills = item.skills
          .filter((s): s is string => typeof s === "string" && slugRegex.test(s))
          .slice(0, 50);
      }

      // Optional: domains (OASF taxonomy)
      if (Array.isArray(item.domains)) {
        service.domains = item.domains
          .filter((d): d is string => typeof d === "string" && slugRegex.test(d))
          .slice(0, 50);
      }

      return service;
    })
    .filter((s): s is Record<string, unknown> => s !== null);
    // Note: slice is done FIRST (before map) to prevent CPU exhaustion
}

/**
 * Sanitize registrations array (ERC-8004 spec)
 * Format: [{ agentId: number, agentRegistry: "namespace:chainId:contractAddress" }]
 */
function sanitizeRegistrationsArray(arr: unknown): Array<{ agentId: number | string; agentRegistry: string }> {
  if (!Array.isArray(arr)) return [];

  // Registry format: namespace:chainId:contractAddress (e.g., "eip155:1:0x...")
  const registryRegex = /^[a-z0-9]+:[a-z0-9]+:[a-zA-Z0-9]+$/;

  // SLICE FIRST to prevent CPU exhaustion via large arrays
  return arr
    .slice(0, 20) // Limit BEFORE processing
    .filter((item): item is Record<string, unknown> => typeof item === "object" && item !== null)
    .map((item) => {
      // agentId can be number or string (for large IDs)
      let agentId: number | string | null = null;
      if (typeof item.agentId === "number") {
        if (Number.isInteger(item.agentId) && item.agentId >= 0) {
          agentId = item.agentId;
        }
      } else if (typeof item.agentId === "string") {
        const parsed = sanitizeText(item.agentId);
        if (/^\d+$/.test(parsed)) {
          agentId = parsed;
        }
      }

      // agentRegistry must match format
      let agentRegistry: string | null = null;
      if (typeof item.agentRegistry === "string") {
        const reg = sanitizeText(item.agentRegistry);
        if (registryRegex.test(reg)) {
          agentRegistry = reg;
        }
      }

      if (agentId !== null && agentRegistry !== null) {
        return { agentId, agentRegistry };
      }
      return null;
    })
    .filter((r): r is { agentId: number | string; agentRegistry: string } => r !== null);
    // Note: slice is done FIRST (before map) to prevent CPU exhaustion
}

/**
 * Apply appropriate sanitization based on field key
 * Follows ERC-8004 Registration File v1 spec
 */
function sanitizeField(key: string, value: unknown): unknown {
  // Text fields - strip all HTML
  if (key === "_uri:name" || key === "_uri:description" || key === "_uri:type") {
    return typeof value === "string" ? sanitizeText(value) : null;
  }

  // URL fields - validate protocol
  if (key === "_uri:image") {
    return typeof value === "string" ? sanitizeUrl(value) : null;
  }

  // Services array (ERC-8004 spec)
  if (key === "_uri:services") {
    return sanitizeServices(value);
  }

  // Registrations array (ERC-8004 spec)
  if (key === "_uri:registrations") {
    return sanitizeRegistrationsArray(value);
  }

  // Trust mechanisms array (ERC-8004 spec)
  if (key === "_uri:supported_trust") {
    if (!Array.isArray(value)) return [];
    const validTrusts = ["reputation", "crypto-economic", "tee-attestation", "8004", "oasf", "x402"];
    // SLICE FIRST to prevent CPU exhaustion, then filter/sanitize
    return value
      .slice(0, 20) // Limit BEFORE processing
      .filter((v): v is string => typeof v === "string")
      .map((v) => sanitizeText(v.toLowerCase()))
      .filter((v) => validTrusts.includes(v));
  }

  // Boolean fields
  if (key === "_uri:active" || key === "_uri:x402_support") {
    return typeof value === "boolean" ? value : null;
  }

  // Unknown fields (in full mode) - sanitize as text if string, otherwise serialize safely
  if (typeof value === "string") {
    return sanitizeText(value);
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return value;
  }
  if (Array.isArray(value)) {
    // Sanitize array items
    return value.slice(0, 100).map((item) =>
      typeof item === "string" ? sanitizeText(item) : item
    );
  }
  if (typeof value === "object" && value !== null) {
    // Return stringified for safety (prevents nested injection)
    return JSON.stringify(value).slice(0, 10000);
  }

  return null;
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

export function toDeterministicUriStatus(result: UriDigestResult): Record<string, unknown> {
  if (result.status === "ok") {
    return {
      status: "ok",
      bytes: result.bytes ?? null,
      hash: result.hash ?? null,
      fieldCount: Object.keys(result.fields ?? {}).length,
      truncatedKeys: Boolean(result.truncatedKeys),
    };
  }

  if (result.status === "blocked") {
    return { status: "error", kind: "blocked", retryable: false };
  }

  if (result.status === "oversize") {
    return {
      status: "error",
      kind: "oversize",
      retryable: false,
      bytes: result.bytes ?? null,
    };
  }

  if (result.status === "invalid_json") {
    return {
      status: "error",
      kind: "invalid_json",
      retryable: false,
      bytes: result.bytes ?? null,
      hash: result.hash ?? null,
    };
  }

  // timeout + generic fetch errors are stored in a deterministic retryable bucket.
  return { status: "error", kind: "fetch_failed", retryable: true };
}

/**
 * Standard ERC-8004 registration file fields
 * Maps builder field names to metadata keys
 */
// Prefix "_uri:" for indexer-derived metadata to avoid collision with on-chain metadata
// Users can set on-chain metadata with any key, so we use underscore prefix for internal keys
/**
 * ERC-8004 Registration File v1 Standard Fields
 * Ref: https://github.com/erc-8004/best-practices/blob/main/Registration.md
 */
const STANDARD_FIELDS: Record<string, string> = {
  // Required fields
  type: "_uri:type",                    // "https://eips.ethereum.org/EIPS/eip-8004#registration-v1"
  name: "_uri:name",                    // Agent display name
  description: "_uri:description",      // Detailed capabilities description
  image: "_uri:image",                  // Logo URL (PNG, SVG, WebP)
  services: "_uri:services",            // Communication methods and capabilities
  registrations: "_uri:registrations",  // On-chain identity references
  // Optional fields
  active: "_uri:active",                // Production readiness flag
  x402Support: "_uri:x402_support",     // Proof-of-payment protocol support
  supportedTrust: "_uri:supported_trust", // Trust mechanisms array
};

// Maximum redirect depth to prevent infinite loops and SSRF via redirect chains
const MAX_REDIRECT_DEPTH = 3;

/**
 * Fetch and digest URI metadata
 * @param uri - The URI to fetch (IPFS, HTTPS, etc.)
 * @param redirectDepth - Current redirect depth (internal use)
 * @returns Digest result with status, fields, and metadata
 */
export async function digestUri(
  uri: string,
  redirectDepth: number = 0,
  allowTrustedLocalHostOverride: boolean = false
): Promise<UriDigestResult> {
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

  let effectiveAllowTrustedLocalHostOverride = allowTrustedLocalHostOverride;
  try {
    const url = new URL(fetchUrl);
    effectiveAllowTrustedLocalHostOverride ||= shouldAllowTrustedLocalHostForUri(uri, url.hostname);
  } catch {
    return { status: "error", error: "Invalid URL" };
  }

  const fetched = await hardenedFetchBuffer(fetchUrl, {
    sourceUri: uri,
    allowTrustedLocalHostOverride: effectiveAllowTrustedLocalHostOverride,
  });

  if (fetched.status !== "ok") {
    return fetched;
  }

  try {
    const buffer = fetched.body;
    const text = buffer.toString("utf-8");
    const hash = createHash("sha256").update(buffer).digest("hex");
    const totalBytes = fetched.bytes;

    let json: Record<string, unknown>;
    try {
      json = JSON.parse(text);
    } catch {
      return { status: "invalid_json", bytes: totalBytes, hash };
    }

    // Extract and sanitize standard fields
    const fields: Record<string, unknown> = {};
    const standardKeys = new Set(Object.keys(STANDARD_FIELDS));

    for (const [jsonKey, metaKey] of Object.entries(STANDARD_FIELDS)) {
      if (json[jsonKey] !== undefined) {
        // Apply sanitization based on field type
        const sanitized = sanitizeField(metaKey, json[jsonKey]);
        if (sanitized !== null && sanitized !== "" && !(Array.isArray(sanitized) && sanitized.length === 0)) {
          fields[metaKey] = sanitized;
        }
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
          // Store with _uri: prefix (internal) and sanitize
          const metaKey = `_uri:${key}`;
          const sanitized = sanitizeField(metaKey, value);
          if (sanitized !== null && sanitized !== "") {
            fields[metaKey] = sanitized;
          }
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
    return { status: "error", error: error instanceof Error ? error.message : String(error) };
  }
}

/**
 * Convert various URI formats to fetchable URLs
 */
export function buildIpfsGatewayUrl(ipfsPath: string): string {
  const base = (config.ipfsGatewayBase || "https://ipfs.io").replace(/\/+$/, "");
  const normalizedPath = ipfsPath.startsWith("/") ? ipfsPath : `/${ipfsPath}`;
  return `${base}${normalizedPath}`;
}

function convertToFetchUrl(uri: string): string | null {
  // IPFS URI
  if (uri.startsWith("ipfs://")) {
    const cid = uri.slice(7);
    return buildIpfsGatewayUrl(`/ipfs/${cid}`);
  }

  // IPFS path
  if (uri.startsWith("/ipfs/")) {
    return buildIpfsGatewayUrl(uri);
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

  // HTTP (rejected by default, allow only with ALLOW_INSECURE_URI=true)
  if (uri.startsWith("http://")) {
    if (process.env.ALLOW_INSECURE_URI === "true") {
      logger.warn({ uri }, "HTTP URI allowed via ALLOW_INSECURE_URI");
      return uri;
    }
    logger.warn({ uri }, "HTTP URI rejected (set ALLOW_INSECURE_URI=true to allow)");
    return null;
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
