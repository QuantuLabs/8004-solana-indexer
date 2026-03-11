import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const stackContent = readFileSync(
  new URL("../../docker/stack/indexer-stack.yml", import.meta.url),
  "utf8"
);

describe("Docker Stack Config", () => {
  it("propagates runtime env vars that affect the indexer container", () => {
    for (const pattern of [
      "DB_MODE: ${DB_MODE:?set DB_MODE}",
      "API_MODE: ${API_MODE:?set API_MODE}",
      "API_PORT: ${API_PORT:-3001}",
      "RPC_URL: ${RPC_URL:-}",
      "WS_URL: ${WS_URL:-}",
      "MAX_SUPPORTED_TRANSACTION_VERSION: ${MAX_SUPPORTED_TRANSACTION_VERSION:-0}",
      "GRAPHQL_RATE_LIMIT_MAX_REQUESTS: ${GRAPHQL_RATE_LIMIT_MAX_REQUESTS:-30}",
      "RATE_LIMIT_MAX_REQUESTS: ${RATE_LIMIT_MAX_REQUESTS:-100}",
      "INDEXER_MODE: ${INDEXER_MODE:-polling}",
      "POLLER_BATCH_RPC_ENABLED: ${POLLER_BATCH_RPC_ENABLED:-true}",
      "POLLER_RPC_CHUNK_SIZE: ${POLLER_RPC_CHUNK_SIZE:-100}",
      "POLLER_RPC_CHUNK_CONCURRENCY: ${POLLER_RPC_CHUNK_CONCURRENCY:-3}",
      "INDEXER_STOP_SLOT: ${INDEXER_STOP_SLOT:-}",
      "METRICS_ENDPOINT_ENABLED: ${METRICS_ENDPOINT_ENABLED:-false}",
      "VERIFY_INTERVAL_MS: ${VERIFY_INTERVAL_MS:-60000}",
      "VERIFY_BATCH_SIZE: ${VERIFY_BATCH_SIZE:-100}",
      "VERIFY_SAFETY_MARGIN_SLOTS: ${VERIFY_SAFETY_MARGIN_SLOTS:-32}",
      "VERIFY_MAX_RETRIES: ${VERIFY_MAX_RETRIES:-3}",
      "VERIFY_RECOVERY_CYCLES: ${VERIFY_RECOVERY_CYCLES:-10}",
      "VERIFY_RECOVERY_BATCH_SIZE: ${VERIFY_RECOVERY_BATCH_SIZE:-50}",
      "INDEX_COLLECTION_METADATA: ${INDEX_COLLECTION_METADATA:-true}",
      "INDEX_VALIDATIONS: ${INDEX_VALIDATIONS:-false}",
      "METADATA_MAX_BYTES: ${METADATA_MAX_BYTES:-262144}",
      "METADATA_MAX_VALUE_BYTES: ${METADATA_MAX_VALUE_BYTES:-10000}",
      "IPFS_GATEWAY_BASE: ${IPFS_GATEWAY_BASE:-https://ipfs.io}",
      "URI_DIGEST_TRUSTED_HOSTS: ${URI_DIGEST_TRUSTED_HOSTS:-}",
      "ALLOW_INSECURE_URI: ${ALLOW_INSECURE_URI:-false}",
      "SUPABASE_DSN: ${SUPABASE_DSN:-}",
    ]) {
      expect(stackContent).toContain(pattern);
    }
  });

  it("keeps API port wiring consistent across port mapping, env, and healthcheck", () => {
    expect(stackContent).toContain('- "${API_PORT:-3001}:${API_PORT:-3001}"');
    expect(stackContent).toContain("API_PORT: ${API_PORT:-3001}");
    expect(stackContent).toContain("127.0.0.1:${API_PORT:-3001}/ready");
  });
});
