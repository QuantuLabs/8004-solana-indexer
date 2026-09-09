import { lookup } from "dns/promises";
import http from "http";
import https from "https";
import { Readable } from "stream";
import { createBrotliDecompress, createGunzip, createInflate } from "zlib";
import { config } from "../config.js";
import { createChildLogger } from "../logger.js";

const logger = createChildLogger("hardened-fetch");

const BLOCKED_HOSTS = new Set([
  "localhost",
  "127.0.0.1",
  "0.0.0.0",
  "[::1]",
  "metadata.google.internal",
  "169.254.169.254",
]);
const ALWAYS_TRUSTED_HOSTS = new Set(["ipfs.io", "arweave.net"]);
const LOCALHOST_TRUST_OVERRIDE_ALLOWLIST = new Set(["localhost", "127.0.0.1"]);
const DEFAULT_DNS_NEGATIVE_TTL_MS = 10 * 60_000;
const MIN_DNS_NEGATIVE_TTL_MS = 10_000;
const DEFAULT_DNS_NEGATIVE_CACHE_MAX_ENTRIES = 10_000;
const MIN_DNS_NEGATIVE_CACHE_MAX_ENTRIES = 100;
const MAX_REDIRECT_DEPTH = 3;

const PRIVATE_IPV4_RANGES = [
  /^10\./,
  /^100\.(6[4-9]|[7-9][0-9]|1[01][0-9]|12[0-7])\./,
  /^172\.(1[6-9]|2[0-9]|3[01])\./,
  /^192\.0\.0\./,
  /^192\.168\./,
  /^198\.(1[89])\./,
  /^169\.254\./,
  /^127\./,
  /^0\./,
];

const PRIVATE_IPV6_RANGES = [
  /^f[cd][0-9a-f]{2}:/i,
  /^fe80:/i,
];

type NegativeDnsEntry = {
  expiresAt: number;
  error: string;
};

export type HostResolution = {
  ip: string;
  family: number;
};

type HeaderMap = Record<string, string>;

export type HardenedTransportRequest = {
  url: URL;
  resolved: HostResolution;
  headers: HeaderMap;
  timeoutMs: number;
  maxBytes: number;
  signal: AbortSignal;
};

export type HardenedTransportResponse = {
  statusCode: number;
  headers: HeaderMap;
  body: Buffer;
  bytes: number;
  oversize?: boolean;
};

export type HardenedFetchResult =
  | {
      status: "ok";
      statusCode: number;
      headers: HeaderMap;
      body: Buffer;
      bytes: number;
      finalUrl: string;
    }
  | { status: "timeout"; error?: string }
  | { status: "error"; error: string }
  | { status: "oversize"; bytes: number }
  | { status: "blocked"; error: string };

export type HardenedFetchOptions = {
  sourceUri?: string;
  allowTrustedLocalHostOverride?: boolean;
  maxBytes?: number;
  timeoutMs?: number;
  headers?: HeaderMap;
};

type HardenedFetchTransport = (request: HardenedTransportRequest) => Promise<HardenedTransportResponse>;
type PinnedLookupCallback = (
  err: NodeJS.ErrnoException | null,
  address: string | Array<{ address: string; family: number }>,
  family?: number
) => void;
type PinnedLookupOptions = { all?: boolean } | undefined;

const negativeDnsCache = new Map<string, NegativeDnsEntry>();
const hostLimitStates = new Map<string, { active: number; waiters: Array<() => void> }>();
let transportOverride: HardenedFetchTransport | null = null;

function parseDnsNegativeTtlMs(raw: string | undefined): number {
  if (!raw || raw.trim() === "") {
    return DEFAULT_DNS_NEGATIVE_TTL_MS;
  }
  const parsed = parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < MIN_DNS_NEGATIVE_TTL_MS) {
    return DEFAULT_DNS_NEGATIVE_TTL_MS;
  }
  return parsed;
}

const DNS_NEGATIVE_TTL_MS = parseDnsNegativeTtlMs(process.env.URI_DIGEST_DNS_NEGATIVE_TTL_MS);

function parseDnsNegativeCacheMaxEntries(raw: string | undefined): number {
  if (!raw || raw.trim() === "") {
    return DEFAULT_DNS_NEGATIVE_CACHE_MAX_ENTRIES;
  }
  const parsed = parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < MIN_DNS_NEGATIVE_CACHE_MAX_ENTRIES) {
    return DEFAULT_DNS_NEGATIVE_CACHE_MAX_ENTRIES;
  }
  return parsed;
}

const DNS_NEGATIVE_CACHE_MAX_ENTRIES = parseDnsNegativeCacheMaxEntries(
  process.env.URI_DIGEST_DNS_NEGATIVE_CACHE_MAX_ENTRIES
);

function getConfiguredIpfsGatewayHost(): string | null {
  try {
    return new URL(config.ipfsGatewayBase).hostname.toLowerCase();
  } catch {
    return null;
  }
}

const configuredIpfsGatewayHost = getConfiguredIpfsGatewayHost();
const trustedLocalOverrideHosts = new Set(
  (config.uriDigestTrustedHosts ?? [])
    .map((host) => host.toLowerCase())
    .filter((host) => LOCALHOST_TRUST_OVERRIDE_ALLOWLIST.has(host))
);

export function setHardenedFetchTransportForTests(transport: HardenedFetchTransport | null): void {
  transportOverride = transport;
}

export function resetHardenedFetchStateForTests(): void {
  negativeDnsCache.clear();
  hostLimitStates.clear();
  transportOverride = null;
}

export function createPinnedLookup(resolved: HostResolution) {
  return (_hostname: string, options: PinnedLookupOptions, callback: PinnedLookupCallback): void => {
    if (options?.all === true) {
      callback(null, [{ address: resolved.ip, family: resolved.family }]);
      return;
    }
    callback(null, resolved.ip, resolved.family);
  };
}

export function shouldAllowTrustedLocalHostForUri(uri: string, hostname: string): boolean {
  if (!configuredIpfsGatewayHost || configuredIpfsGatewayHost !== hostname.toLowerCase()) {
    return false;
  }
  if (!trustedLocalOverrideHosts.has(hostname.toLowerCase())) {
    return false;
  }
  return uri.startsWith("ipfs://") || uri.startsWith("/ipfs/") || uri.startsWith("c1:");
}

function isTrustedHost(hostname: string, allowTrustedLocalHostOverride: boolean): boolean {
  const normalized = hostname.toLowerCase();
  if (ALWAYS_TRUSTED_HOSTS.has(normalized)) {
    return true;
  }
  return allowTrustedLocalHostOverride && trustedLocalOverrideHosts.has(normalized);
}

function purgeExpiredNegativeDnsEntries(nowMs: number): void {
  for (const [hostname, entry] of negativeDnsCache.entries()) {
    if (entry.expiresAt <= nowMs) {
      negativeDnsCache.delete(hostname);
    }
  }
}

function getActiveNegativeDnsEntry(hostname: string): NegativeDnsEntry | null {
  const key = hostname.toLowerCase();
  const existing = negativeDnsCache.get(key);
  if (!existing) return null;
  if (existing.expiresAt <= Date.now()) {
    negativeDnsCache.delete(key);
    return null;
  }
  return existing;
}

function shouldCacheDnsFailure(errorText: string): boolean {
  return /ENOTFOUND|EAI_AGAIN|EAI_FAIL|EAI_NONAME|SERVFAIL|NXDOMAIN/i.test(errorText);
}

function cacheNegativeDnsFailure(hostname: string, errorText: string, nowMs: number): void {
  if (negativeDnsCache.has(hostname)) {
    negativeDnsCache.delete(hostname);
  }
  negativeDnsCache.set(hostname, {
    expiresAt: nowMs + DNS_NEGATIVE_TTL_MS,
    error: errorText,
  });
  while (negativeDnsCache.size > DNS_NEGATIVE_CACHE_MAX_ENTRIES) {
    const oldest = negativeDnsCache.keys().next().value as string | undefined;
    if (!oldest) break;
    negativeDnsCache.delete(oldest);
  }
}

function isIPv6LoopbackOrUnspecified(ipv6: string): boolean {
  let addr = ipv6.startsWith("[") ? ipv6.slice(1, -1) : ipv6;
  const zoneIdx = addr.indexOf("%");
  if (zoneIdx !== -1) addr = addr.slice(0, zoneIdx);
  if ((addr.match(/::/g) || []).length > 1) return false;

  const segments: number[] = [];
  if (addr.includes("::")) {
    const [left, right] = addr.split("::");
    const leftParts = left ? left.split(":") : [];
    const rightParts = right ? right.split(":") : [];
    for (const p of leftParts) {
      const val = parseInt(p, 16);
      if (isNaN(val) || val < 0 || val > 0xffff) return false;
      segments.push(val);
    }
    const zerosNeeded = 8 - leftParts.length - rightParts.length;
    for (let i = 0; i < zerosNeeded; i++) {
      segments.push(0);
    }
    for (const p of rightParts) {
      const val = parseInt(p, 16);
      if (isNaN(val) || val < 0 || val > 0xffff) return false;
      segments.push(val);
    }
  } else {
    const parts = addr.split(":");
    if (parts.length !== 8) return false;
    for (const p of parts) {
      const val = parseInt(p, 16);
      if (isNaN(val) || val < 0 || val > 0xffff) return false;
      segments.push(val);
    }
  }

  if (segments.length !== 8) return false;
  const isLoopback = segments.slice(0, 7).every((s) => s === 0) && segments[7] === 1;
  const isUnspecified = segments.every((s) => s === 0);
  return isLoopback || isUnspecified;
}

function canonicalizeIP(ip: string): { ipv4: string } | { ipv6: string } | null {
  if (ip.length > 100) return null;

  let lower = ip.toLowerCase();
  const zoneIndex = lower.indexOf("%");
  if (zoneIndex !== -1) {
    lower = lower.slice(0, zoneIndex);
  }

  const mappedDottedMatch = lower.match(/^::ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/);
  if (mappedDottedMatch) {
    const normalized = canonicalizeIP(mappedDottedMatch[1]);
    return normalized && "ipv4" in normalized ? normalized : null;
  }

  const mappedFullMatch = lower.match(/^(?:0+:){5}ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/);
  if (mappedFullMatch) {
    const high = parseInt(mappedFullMatch[1], 16);
    const low = parseInt(mappedFullMatch[2], 16);
    if (high >= 0 && high <= 0xffff && low >= 0 && low <= 0xffff) {
      const ipv4 = `${(high >> 8) & 0xff}.${high & 0xff}.${(low >> 8) & 0xff}.${low & 0xff}`;
      return { ipv4 };
    }
  }

  const mappedFullDottedMatch = lower.match(/^(?:0+:){5}ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/);
  if (mappedFullDottedMatch) {
    const normalized = canonicalizeIP(mappedFullDottedMatch[1]);
    return normalized && "ipv4" in normalized ? normalized : null;
  }

  const mappedHexMatch = lower.match(/^::ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/);
  if (mappedHexMatch) {
    const high = parseInt(mappedHexMatch[1], 16);
    const low = parseInt(mappedHexMatch[2], 16);
    if (high >= 0 && high <= 0xffff && low >= 0 && low <= 0xffff) {
      const ipv4 = `${(high >> 8) & 0xff}.${high & 0xff}.${(low >> 8) & 0xff}.${low & 0xff}`;
      return { ipv4 };
    }
  }

  const compatibleMatch = lower.match(/^::(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/);
  if (compatibleMatch) {
    const normalized = canonicalizeIP(compatibleMatch[1]);
    return normalized && "ipv4" in normalized ? normalized : null;
  }

  if (/^0x[0-9a-f]+$/i.test(lower)) {
    const num = parseInt(lower, 16);
    if (num >= 0 && num <= 0xffffffff) {
      return { ipv4: `${(num >> 24) & 0xff}.${(num >> 16) & 0xff}.${(num >> 8) & 0xff}.${num & 0xff}` };
    }
  }

  if (/^\d+$/.test(lower) && !lower.includes(".")) {
    const num = parseInt(lower, 10);
    if (num >= 0 && num <= 0xffffffff) {
      return { ipv4: `${(num >> 24) & 0xff}.${(num >> 16) & 0xff}.${(num >> 8) & 0xff}.${num & 0xff}` };
    }
  }

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

  if (/^0\d+\./.test(lower)) {
    const parts = lower.split(".");
    if (parts.length === 4) {
      const normalized = parts.map((p) => parseInt(p, p.startsWith("0") && p.length > 1 ? 8 : 10));
      if (normalized.every((n) => n >= 0 && n <= 255)) {
        return { ipv4: normalized.join(".") };
      }
    }
  }

  if (/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(lower)) {
    return { ipv4: lower };
  }

  if (lower.includes(":")) {
    return { ipv6: lower };
  }

  return null;
}

export function isPrivateIP(ip: string): boolean {
  if (BLOCKED_HOSTS.has(ip.toLowerCase())) return true;

  const canonical = canonicalizeIP(ip);
  if (canonical && "ipv4" in canonical) {
    for (const pattern of PRIVATE_IPV4_RANGES) {
      if (pattern.test(canonical.ipv4)) return true;
    }
  } else if (canonical && "ipv6" in canonical) {
    if (isIPv6LoopbackOrUnspecified(canonical.ipv6)) return true;
    for (const pattern of PRIVATE_IPV6_RANGES) {
      if (pattern.test(canonical.ipv6)) return true;
    }
  }

  return false;
}

export function isBlockedHost(hostname: string): boolean {
  const lower = hostname.toLowerCase();
  if (BLOCKED_HOSTS.has(lower)) return true;
  return isPrivateIP(lower);
}

async function validateHostResolution(
  hostname: string,
  allowTrustedLocalHostOverride: boolean
): Promise<HostResolution | null> {
  try {
    const normalizedHost = hostname.toLowerCase();
    purgeExpiredNegativeDnsEntries(Date.now());

    if (isTrustedHost(hostname, allowTrustedLocalHostOverride)) {
      return { ip: hostname, family: 0 };
    }

    const cachedFailure = getActiveNegativeDnsEntry(normalizedHost);
    if (cachedFailure) {
      logger.debug(
        {
          hostname,
          retryAfterMs: Math.max(0, cachedFailure.expiresAt - Date.now()),
          cachedError: cachedFailure.error,
        },
        "DNS resolution failed recently, using cached block"
      );
      return null;
    }

    const result = await lookup(hostname, { all: true });
    negativeDnsCache.delete(normalizedHost);

    for (const record of result) {
      if (isPrivateIP(record.address)) {
        logger.warn({ hostname, ip: record.address }, "DNS resolved to private IP");
        continue;
      }
      return { ip: record.address, family: record.family };
    }

    logger.warn({ hostname }, "All DNS records resolved to private IPs");
    return null;
  } catch (error) {
    const errorText = String(error);
    if (shouldCacheDnsFailure(errorText)) {
      cacheNegativeDnsFailure(hostname.toLowerCase(), errorText, Date.now());
      logger.warn(
        { hostname, error: errorText, ttlMs: DNS_NEGATIVE_TTL_MS },
        "DNS resolution failed, cached and blocking for safety"
      );
      return null;
    }
    logger.warn({ hostname, error: errorText }, "DNS resolution failed, blocking for safety");
    return null;
  }
}

function isRedirectStatus(statusCode: number): boolean {
  return statusCode >= 300 && statusCode < 400;
}

function getHeader(headers: HeaderMap, name: string): string | undefined {
  return headers[name.toLowerCase()];
}

function normalizeNodeHeaders(headers: http.IncomingHttpHeaders): HeaderMap {
  const normalized: HeaderMap = {};
  for (const [key, value] of Object.entries(headers)) {
    if (typeof value === "string") {
      normalized[key.toLowerCase()] = value;
    } else if (Array.isArray(value)) {
      normalized[key.toLowerCase()] = value.join(", ");
    }
  }
  return normalized;
}

function normalizeFetchHeaders(headers: Headers): HeaderMap {
  const normalized: HeaderMap = {};
  headers.forEach((value, key) => {
    normalized[key.toLowerCase()] = value;
  });
  return normalized;
}

function contentLengthOversize(headers: HeaderMap, maxBytes: number): number | null {
  const contentLength = getHeader(headers, "content-length");
  if (!contentLength) return null;
  const parsed = parseInt(contentLength, 10);
  return Number.isFinite(parsed) && parsed > maxBytes ? parsed : null;
}

function streamForEncoding(stream: Readable, encoding: string | undefined): Readable {
  const normalized = (encoding || "").toLowerCase().split(",")[0].trim();
  if (normalized === "gzip" || normalized === "x-gzip") {
    return stream.pipe(createGunzip());
  }
  if (normalized === "br") {
    return stream.pipe(createBrotliDecompress());
  }
  if (normalized === "deflate") {
    return stream.pipe(createInflate());
  }
  return stream;
}

function readNodeStreamWithLimit(stream: Readable, source: Readable, maxBytes: number): Promise<{ body: Buffer; bytes: number } | { oversize: true; bytes: number }> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let totalBytes = 0;
    let settled = false;

    const settle = (value: { body: Buffer; bytes: number } | { oversize: true; bytes: number }): void => {
      if (settled) return;
      settled = true;
      resolve(value);
    };

    stream.on("data", (chunk: Buffer) => {
      totalBytes += chunk.length;
      if (totalBytes > maxBytes) {
        source.destroy();
        if (stream !== source) stream.destroy();
        settle({ oversize: true, bytes: totalBytes });
        return;
      }
      chunks.push(Buffer.from(chunk));
    });

    stream.on("end", () => {
      settle({ body: Buffer.concat(chunks), bytes: totalBytes });
    });

    stream.on("error", (error) => {
      if (!settled) {
        settled = true;
        reject(error);
      }
    });

    source.on("error", (error) => {
      if (!settled) {
        settled = true;
        reject(error);
      }
    });
  });
}

async function readFetchResponseWithLimit(response: Response, maxBytes: number): Promise<{ body: Buffer; bytes: number } | { oversize: true; bytes: number } | { error: string }> {
  const reader = response.body?.getReader();
  if (!reader) {
    return { error: "No response body" };
  }

  const chunks: Uint8Array[] = [];
  let totalBytes = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    totalBytes += value.length;
    if (totalBytes > maxBytes) {
      await reader.cancel();
      return { oversize: true, bytes: totalBytes };
    }
    chunks.push(value);
  }

  return { body: Buffer.concat(chunks), bytes: totalBytes };
}

function abortError(): Error {
  const error = new Error("The operation was aborted");
  error.name = "AbortError";
  return error;
}

async function fetchTransport(request: HardenedTransportRequest): Promise<HardenedTransportResponse> {
  const response = await fetch(request.url.toString(), {
    signal: request.signal,
    redirect: "manual",
    headers: request.headers,
  });
  const headers = response.headers ? normalizeFetchHeaders(response.headers) : {};
  if (isRedirectStatus(response.status) || response.status < 200 || response.status >= 300) {
    return { statusCode: response.status, headers, body: Buffer.alloc(0), bytes: 0 };
  }

  const lengthOversize = contentLengthOversize(headers, request.maxBytes);
  if (lengthOversize !== null) {
    return { statusCode: response.status, headers, body: Buffer.alloc(0), bytes: lengthOversize, oversize: true };
  }

  const read = await readFetchResponseWithLimit(response, request.maxBytes);
  if ("error" in read) {
    throw new Error(read.error);
  }
  if ("oversize" in read) {
    return { statusCode: response.status, headers, body: Buffer.alloc(0), bytes: read.bytes, oversize: true };
  }
  return { statusCode: response.status, headers, body: read.body, bytes: read.bytes };
}

function nodeTransport(request: HardenedTransportRequest): Promise<HardenedTransportResponse> {
  if (request.signal.aborted) return Promise.reject(abortError());

  return new Promise((resolve, reject) => {
    const requestUrl = request.url;
    const client = requestUrl.protocol === "https:" ? https : http;
    const path = `${requestUrl.pathname}${requestUrl.search}`;
    const req = client.request(
      {
        protocol: requestUrl.protocol,
        hostname: requestUrl.hostname,
        port: requestUrl.port,
        path,
        method: "GET",
        headers: request.headers,
        timeout: request.timeoutMs,
        servername: requestUrl.hostname,
        lookup: request.resolved.family === 0
          ? undefined
          : createPinnedLookup(request.resolved),
      },
      async (res) => {
        const statusCode = res.statusCode ?? 0;
        const headers = normalizeNodeHeaders(res.headers);
        if (isRedirectStatus(statusCode) || statusCode < 200 || statusCode >= 300) {
          res.destroy();
          resolve({ statusCode, headers, body: Buffer.alloc(0), bytes: 0 });
          return;
        }

        const lengthOversize = contentLengthOversize(headers, request.maxBytes);
        if (lengthOversize !== null) {
          res.destroy();
          resolve({ statusCode, headers, body: Buffer.alloc(0), bytes: lengthOversize, oversize: true });
          return;
        }

        try {
          const bodyStream = streamForEncoding(res, getHeader(headers, "content-encoding"));
          const read = await readNodeStreamWithLimit(bodyStream, res, request.maxBytes);
          if ("oversize" in read) {
            resolve({ statusCode, headers, body: Buffer.alloc(0), bytes: read.bytes, oversize: true });
            return;
          }
          resolve({ statusCode, headers, body: read.body, bytes: read.bytes });
        } catch (error) {
          reject(error);
        }
      }
    );

    const onAbort = (): void => {
      req.destroy(abortError());
    };

    request.signal.addEventListener("abort", onAbort, { once: true });
    req.on("timeout", () => req.destroy(abortError()));
    req.on("error", (error) => reject(error));
    req.on("close", () => {
      request.signal.removeEventListener("abort", onAbort);
    });
    req.end();
  });
}

function getTransport(): HardenedFetchTransport {
  if (transportOverride) return transportOverride;
  if (process.env.VITEST !== undefined) return fetchTransport;
  return nodeTransport;
}

function hostLimit(): number {
  const configured = config.metadataPerHostFetchConcurrency ?? 2;
  return Math.max(1, configured);
}

function hostLimitKey(hostname: string, resolved: HostResolution): string {
  return resolved.family === 0 ? hostname.toLowerCase() : resolved.ip;
}

function acquireHostSlot(key: string, signal: AbortSignal): Promise<() => void> {
  if (signal.aborted) return Promise.reject(abortError());

  const limit = hostLimit();
  let state = hostLimitStates.get(key);
  if (!state) {
    state = { active: 0, waiters: [] };
    hostLimitStates.set(key, state);
  }

  if (state.active < limit) {
    state.active++;
    return Promise.resolve(() => releaseHostSlot(key));
  }

  return new Promise((resolve, reject) => {
    const waiter = (): void => {
      signal.removeEventListener("abort", onAbort);
      state.active++;
      resolve(() => releaseHostSlot(key));
    };
    const onAbort = (): void => {
      const index = state.waiters.indexOf(waiter);
      if (index !== -1) state.waiters.splice(index, 1);
      reject(abortError());
    };
    state.waiters.push(waiter);
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

function releaseHostSlot(key: string): void {
  const state = hostLimitStates.get(key);
  if (!state) return;
  state.active = Math.max(0, state.active - 1);
  const next = state.waiters.shift();
  if (next) {
    next();
    return;
  }
  if (state.active === 0) {
    hostLimitStates.delete(key);
  }
}

function httpAllowed(url: URL, allowTrustedLocalHostOverride: boolean): boolean {
  return url.protocol === "https:"
    || process.env.ALLOW_INSECURE_URI === "true"
    || isTrustedHost(url.hostname, allowTrustedLocalHostOverride);
}

async function validateFetchUrl(
  url: URL,
  sourceUri: string,
  allowTrustedLocalHostOverride: boolean
): Promise<{ status: "ok"; resolved: HostResolution } | { status: "blocked"; error: string } | { status: "error"; error: string }> {
  if (url.protocol !== "https:" && url.protocol !== "http:") {
    return { status: "error", error: "Unsupported URI scheme" };
  }

  if (isBlockedHost(url.hostname) && !isTrustedHost(url.hostname, allowTrustedLocalHostOverride)) {
    logger.warn({ uri: sourceUri, hostname: url.hostname }, "Blocked SSRF attempt");
    return { status: "blocked", error: "Internal host blocked" };
  }

  if (!httpAllowed(url, allowTrustedLocalHostOverride)) {
    logger.warn({ uri: sourceUri, url: url.toString() }, "HTTP URI rejected (set ALLOW_INSECURE_URI=true to allow)");
    return { status: "error", error: "Unsupported URI scheme" };
  }

  const resolved = await validateHostResolution(url.hostname, allowTrustedLocalHostOverride);
  if (!resolved) {
    return { status: "blocked", error: "DNS resolved to private IP" };
  }

  return { status: "ok", resolved };
}

export async function hardenedFetchBuffer(fetchUrl: string, options: HardenedFetchOptions = {}): Promise<HardenedFetchResult> {
  const sourceUri = options.sourceUri ?? fetchUrl;
  let currentUrl: URL;
  try {
    currentUrl = new URL(fetchUrl);
  } catch {
    return { status: "error", error: "Invalid URL" };
  }

  const maxBytes = options.maxBytes ?? config.metadataMaxBytes;
  const timeoutMs = options.timeoutMs ?? config.metadataTimeoutMs;
  const headers = {
    Accept: "application/json",
    "Accept-Encoding": "gzip, deflate, br",
    "User-Agent": "8004-Indexer/1.0",
    ...(options.headers ?? {}),
  };
  const allowTrustedLocalHostOverride = Boolean(options.allowTrustedLocalHostOverride);

  for (let hop = 0; ; hop++) {
    const validated = await validateFetchUrl(currentUrl, sourceUri, allowTrustedLocalHostOverride);
    if (validated.status !== "ok") {
      if (hop > 0 && validated.status === "blocked" && validated.error === "DNS resolved to private IP") {
        return { status: "blocked", error: "Redirect DNS resolved to private IP" };
      }
      return validated;
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    const slotKey = hostLimitKey(currentUrl.hostname, validated.resolved);
    let releaseSlot: (() => void) | undefined;

    try {
      releaseSlot = await acquireHostSlot(slotKey, controller.signal);
      if (controller.signal.aborted) throw abortError();

      const response = await getTransport()({
        url: currentUrl,
        resolved: validated.resolved,
        headers,
        timeoutMs,
        maxBytes,
        signal: controller.signal,
      });

      if (response.oversize) {
        return { status: "oversize", bytes: response.bytes };
      }

      if (isRedirectStatus(response.statusCode)) {
        const location = getHeader(response.headers, "location");
        if (!location) {
          return { status: "error", error: `Redirect ${response.statusCode} without location` };
        }
        let redirectUrl: URL;
        try {
          redirectUrl = new URL(location, currentUrl.toString());
        } catch {
          return { status: "error", error: "Invalid redirect URL" };
        }
        if (isBlockedHost(redirectUrl.hostname) && !isTrustedHost(redirectUrl.hostname, allowTrustedLocalHostOverride)) {
          logger.warn({ uri: sourceUri, redirectTo: redirectUrl.hostname }, "Blocked redirect to internal host");
          return { status: "blocked", error: "Redirect to internal host blocked" };
        }
        if (hop + 1 >= MAX_REDIRECT_DEPTH) {
          return { status: "error", error: "Too many redirects" };
        }
        currentUrl = redirectUrl;
        continue;
      }

      if (response.statusCode < 200 || response.statusCode >= 300) {
        return { status: "error", error: `HTTP ${response.statusCode}` };
      }

      return {
        status: "ok",
        statusCode: response.statusCode,
        headers: response.headers,
        body: response.body,
        bytes: response.bytes,
        finalUrl: currentUrl.toString(),
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
      releaseSlot?.();
      clearTimeout(timeoutId);
    }
  }
}
