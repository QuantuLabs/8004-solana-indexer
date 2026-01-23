/**
 * URI Digest Module
 * Fetches agent URI metadata and extracts standard fields for indexing
 */

import { createHash } from "crypto";
import { config } from "../config.js";
import { createChildLogger } from "../logger.js";

const logger = createChildLogger("uri-digest");

export type DigestStatus = "ok" | "timeout" | "error" | "oversize" | "invalid_json";

export interface UriDigestResult {
  status: DigestStatus;
  error?: string;
  bytes?: number;
  hash?: string;
  fields?: Record<string, unknown>;
}

/**
 * Standard ERC-8004 registration file fields
 * Maps builder field names to metadata keys
 */
const STANDARD_FIELDS: Record<string, string> = {
  type: "uri:type",
  name: "uri:name",
  description: "uri:description",
  image: "uri:image",
  endpoints: "uri:endpoints",
  registrations: "uri:registrations",
  supportedTrusts: "uri:supported_trusts",
  active: "uri:active",
  x402support: "uri:x402_support",
  skills: "uri:skills",
  domains: "uri:domains",
};

/**
 * Fetch and digest URI metadata
 * @param uri - The URI to fetch (IPFS, HTTPS, etc.)
 * @returns Digest result with status, fields, and metadata
 */
export async function digestUri(uri: string): Promise<UriDigestResult> {
  if (!uri) {
    return { status: "error", error: "Empty URI" };
  }

  // Convert IPFS URI to gateway URL
  const fetchUrl = convertToFetchUrl(uri);
  if (!fetchUrl) {
    return { status: "error", error: "Unsupported URI scheme" };
  }

  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), config.metadataTimeoutMs);

    const response = await fetch(fetchUrl, {
      signal: controller.signal,
      headers: {
        Accept: "application/json",
        "User-Agent": "8004-Indexer/1.0",
      },
    });

    clearTimeout(timeoutId);

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
    if (config.metadataIndexMode === "full") {
      const MAX_EXTRA_KEYS = 50; // Protection against malicious JSON with many keys
      let extraKeyCount = 0;

      for (const [key, value] of Object.entries(json)) {
        if (!standardKeys.has(key)) {
          if (extraKeyCount >= MAX_EXTRA_KEYS) {
            logger.warn({ uri }, `Exceeded ${MAX_EXTRA_KEYS} extra keys, truncating`);
            break;
          }
          // Store with uri: prefix for queryability
          fields[`uri:${key}`] = value;
          extraKeyCount++;
        }
      }
    }

    return {
      status: "ok",
      bytes: totalBytes,
      hash,
      fields,
    };
  } catch (error: unknown) {
    if (error instanceof Error) {
      if (error.name === "AbortError") {
        return { status: "timeout" };
      }
      return { status: "error", error: error.message };
    }
    return { status: "error", error: String(error) };
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
