import { createHash } from "crypto";
import { config } from "../config.js";
import { createChildLogger } from "../logger.js";
import { buildIpfsGatewayUrl, sanitizeText, sanitizeUrl } from "./uriDigest.js";

const logger = createChildLogger("collection-digest");

const COLLECTION_POINTER_PREFIX = "c1:";
const CID_PATH_REGEX = /^[a-zA-Z0-9]+(?:\/[a-zA-Z0-9._-]+)*$/;

export type CollectionDigestStatus = "ok" | "timeout" | "error" | "oversize" | "invalid_json";

export interface CollectionDocFields {
  version: string | null;
  name: string | null;
  symbol: string | null;
  description: string | null;
  image: string | null;
  bannerImage: string | null;
  socialWebsite: string | null;
  socialX: string | null;
  socialDiscord: string | null;
}

export interface CollectionDigestResult {
  status: CollectionDigestStatus;
  error?: string;
  bytes?: number;
  hash?: string;
  fields?: CollectionDocFields;
}

function parseCollectionPointerToFetchUrl(pointer: string): string | null {
  if (!pointer || !pointer.startsWith(COLLECTION_POINTER_PREFIX)) {
    return null;
  }

  const cidPath = pointer.slice(COLLECTION_POINTER_PREFIX.length).trim();
  if (!cidPath || !CID_PATH_REGEX.test(cidPath)) {
    return null;
  }

  return buildIpfsGatewayUrl(`/ipfs/${cidPath}`);
}

function sanitizeOptionalText(value: unknown, maxLen: number): string | null {
  if (typeof value !== "string") return null;
  const cleaned = sanitizeText(value).slice(0, maxLen).trim();
  return cleaned.length > 0 ? cleaned : null;
}

function sanitizeOptionalUrl(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const cleaned = sanitizeUrl(value);
  return cleaned.length > 0 ? cleaned : null;
}

function emptyCollectionFields(): CollectionDocFields {
  return {
    version: null,
    name: null,
    symbol: null,
    description: null,
    image: null,
    bannerImage: null,
    socialWebsite: null,
    socialX: null,
    socialDiscord: null,
  };
}

export async function digestCollectionPointerDoc(pointer: string): Promise<CollectionDigestResult> {
  const fetchUrl = parseCollectionPointerToFetchUrl(pointer);
  if (!fetchUrl) {
    return { status: "error", error: "Invalid canonical collection pointer format (expected c1:<cid>)" };
  }

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), config.metadataTimeoutMs);

  try {
    const response = await fetch(fetchUrl, {
      signal: controller.signal,
      redirect: "follow",
      headers: {
        Accept: "application/json",
        "User-Agent": "8004-Indexer/1.0",
      },
    });

    if (!response.ok) {
      return { status: "error", error: `HTTP ${response.status}` };
    }

    const contentLength = response.headers.get("content-length");
    if (contentLength && parseInt(contentLength, 10) > config.metadataMaxBytes) {
      return {
        status: "oversize",
        bytes: parseInt(contentLength, 10),
      };
    }

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

    const buffer = Buffer.concat(chunks);
    const hash = createHash("sha256").update(buffer).digest("hex");

    let json: Record<string, unknown>;
    try {
      json = JSON.parse(buffer.toString("utf-8"));
    } catch {
      return { status: "invalid_json", bytes: totalBytes, hash };
    }

    const fields = emptyCollectionFields();
    fields.version = sanitizeOptionalText(json.version, 32);
    fields.name = sanitizeOptionalText(json.name, 128);
    fields.symbol = sanitizeOptionalText(json.symbol, 32);
    fields.description = sanitizeOptionalText(json.description, 4096);
    fields.image = sanitizeOptionalUrl(json.image);
    fields.bannerImage = sanitizeOptionalUrl(json.banner_image);

    // parent is intentionally ignored here (it is authoritative on-chain only).
    if (json.socials && typeof json.socials === "object") {
      const socials = json.socials as Record<string, unknown>;
      fields.socialWebsite = sanitizeOptionalUrl(socials.website);
      fields.socialX = sanitizeOptionalText(socials.x, 128);
      fields.socialDiscord = sanitizeOptionalUrl(socials.discord);
    }

    return {
      status: "ok",
      bytes: totalBytes,
      hash,
      fields,
    };
  } catch (error: unknown) {
    if (error instanceof Error && error.name === "AbortError") {
      return { status: "timeout" };
    }
    logger.warn({ pointer, error: String(error) }, "Collection digest failed");
    return { status: "error", error: String(error) };
  } finally {
    clearTimeout(timeoutId);
  }
}
