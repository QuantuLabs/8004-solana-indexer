#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INDEXER_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SOLANA_ROOT="$(cd "$INDEXER_ROOT/../8004-solana" && pwd)"

RUN_ID="${INTEGRITY_RUN_ID:-$(date -u +"%Y%m%dT%H%M%SZ")}"
ARTIFACT_DIR="${INTEGRITY_ARTIFACT_DIR:-$INDEXER_ROOT/artifacts/localnet-websocket-integrity}"
mkdir -p "$ARTIFACT_DIR"

DB_PATH="${INTEGRITY_DB_PATH:-$ARTIFACT_DIR/localnet-integrity-${RUN_ID}.db}"
DB_URL="file:${DB_PATH}"
STRESS_JSONL_PATH="${INTEGRITY_STRESS_JSONL_PATH:-$ARTIFACT_DIR/stress-${RUN_ID}.jsonl}"
STRESS_SUMMARY_PATH="${INTEGRITY_STRESS_SUMMARY_PATH:-$ARTIFACT_DIR/stress-${RUN_ID}.json}"
REPORT_PATH="${INTEGRITY_REPORT_PATH:-$ARTIFACT_DIR/integrity-report-${RUN_ID}.json}"
INDEXER_LOG_PATH="${INTEGRITY_INDEXER_LOG_PATH:-$ARTIFACT_DIR/indexer-${RUN_ID}.log}"
INDEXER_PID_FILE="/tmp/8004-solana-indexer-integrity-${RUN_ID}.pid"

RPC_URL="${LOCALNET_RPC_URL:-http://127.0.0.1:8899}"
WS_URL="${LOCALNET_WS_URL:-ws://127.0.0.1:8900}"
PROGRAM_ID="${PROGRAM_ID:-8oo4dC4JvBLwy5tGgiH3WwK4B9PWxL9Z4XjA2jzkQMbQ}"
PROGRAM_KEYPAIR_PATH="${LOCALNET_PROGRAM_KEYPAIR:-$SOLANA_ROOT/keys/mainnet-program/8oo4dC4JvBLwy5tGgiH3WwK4B9PWxL9Z4XjA2jzkQMbQ.json}"
API_PORT="${API_PORT:-3001}"

cleanup() {
  local exit_code=$?

  if [[ -f "$INDEXER_PID_FILE" ]]; then
    local pid
    pid="$(cat "$INDEXER_PID_FILE" || true)"
    if [[ -n "${pid:-}" ]]; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
    rm -f "$INDEXER_PID_FILE"
  fi

  if [[ "${KEEP_LOCALNET:-0}" != "1" ]]; then
    "$SCRIPT_DIR/localnet-stop.sh" >/dev/null 2>&1 || true
  fi

  if [[ "$exit_code" -ne 0 ]]; then
    echo "[integrity] failed (exit=${exit_code})" >&2
    echo "[integrity] indexer_log=${INDEXER_LOG_PATH}" >&2
  fi
}

trap cleanup EXIT INT TERM

echo "[integrity] artifacts_dir=${ARTIFACT_DIR}"
echo "[integrity] run_id=${RUN_ID}"

echo "[integrity] stopping stale validator"
"$SCRIPT_DIR/localnet-stop.sh" >/dev/null 2>&1 || true

echo "[integrity] starting validator + deploying program"
LOCALNET_CLONE_ATOM_PROGRAM="${LOCALNET_CLONE_ATOM_PROGRAM:-1}" \
LOCALNET_PROGRAM_KEYPAIR="$PROGRAM_KEYPAIR_PATH" \
  "$SCRIPT_DIR/localnet-start.sh"

echo "[integrity] initializing localnet state"
"$SCRIPT_DIR/localnet-init.sh"

echo "[integrity] airdropping test funds"
solana -u "$RPC_URL" airdrop "${INTEGRITY_AIRDROP_SOL:-1000}" >/dev/null 2>&1 || true

echo "[integrity] preparing local sqlite db"
rm -f "$DB_PATH"
(
  cd "$INDEXER_ROOT"
  DATABASE_URL="$DB_URL" bunx prisma db push --skip-generate >/dev/null
)

echo "[integrity] starting indexer in websocket mode"
(
  cd "$INDEXER_ROOT"
  DB_MODE=local \
  DATABASE_URL="$DB_URL" \
  API_MODE=rest \
  ENABLE_GRAPHQL=false \
  RPC_URL="$RPC_URL" \
  WS_URL="$WS_URL" \
  PROGRAM_ID="$PROGRAM_ID" \
  INDEXER_MODE=websocket \
  POLLING_INTERVAL="${POLLING_INTERVAL:-2000}" \
  LOG_LEVEL="${INDEXER_LOG_LEVEL:-info}" \
  API_PORT="$API_PORT" \
  bunx tsx src/index.ts >"$INDEXER_LOG_PATH" 2>&1 &
  echo $! > "$INDEXER_PID_FILE"
)

echo "[integrity] waiting for API health"
ready=0
for _attempt in $(seq 1 90); do
  if curl -fsS "http://127.0.0.1:${API_PORT}/health" >/dev/null 2>&1; then
    ready=1
    break
  fi

  if [[ -f "$INDEXER_PID_FILE" ]]; then
    pid="$(cat "$INDEXER_PID_FILE")"
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      echo "[integrity] indexer exited early. Last logs:" >&2
      tail -n 120 "$INDEXER_LOG_PATH" >&2 || true
      exit 1
    fi
  fi

  sleep 1
done

if [[ "$ready" != "1" ]]; then
  echo "[integrity] indexer health check timed out. Last logs:" >&2
  tail -n 120 "$INDEXER_LOG_PATH" >&2 || true
  exit 1
fi

echo "[integrity] generating on-chain actions (~100 each)"
(
  cd "$SOLANA_ROOT"
  ANCHOR_PROVIDER_URL="${ANCHOR_PROVIDER_URL:-$RPC_URL}" \
  ANCHOR_WALLET="${ANCHOR_WALLET:-$HOME/.config/solana/id.json}" \
  STRESS_MODE=run \
  STRESS_ESTIMATE_ONLY=false \
  STRESS_CLUSTER=localnet \
  STRESS_RPC_URL="$RPC_URL" \
  STRESS_PROGRAM_ID="$PROGRAM_ID" \
  STRESS_AGENTS="${STRESS_AGENTS:-100}" \
  STRESS_FEEDBACKS="${STRESS_FEEDBACKS:-100}" \
  STRESS_RESPONSES="${STRESS_RESPONSES:-100}" \
  STRESS_REVOKES="${STRESS_REVOKES:-100}" \
  STRESS_WALLETS="${STRESS_WALLETS:-100}" \
  STRESS_RESPONDER_WALLETS="${STRESS_RESPONDER_WALLETS:-20}" \
  STRESS_CONCURRENCY="${STRESS_CONCURRENCY:-8}" \
  STRESS_RETRIES="${STRESS_RETRIES:-3}" \
  STRESS_INVALID_RATIO="${STRESS_INVALID_RATIO:-0}" \
  STRESS_RECOVER_FUNDS="${STRESS_RECOVER_FUNDS:-false}" \
  STRESS_BUDGET_CAP_SOL="${STRESS_BUDGET_CAP_SOL:-100}" \
  STRESS_STRICT_BUDGET=false \
  STRESS_INDEXER_WAIT_MS=0 \
  FORCE_ON_CHAIN=true \
  STRESS_SUMMARY_PATH="$STRESS_SUMMARY_PATH" \
  STRESS_JSONL_PATH="$STRESS_JSONL_PATH" \
  npm run stress:devnet:massive -- --mode=run
)

echo "[integrity] comparing indexed output + deterministic keys"
(
  cd "$INDEXER_ROOT"
  DATABASE_URL="$DB_URL" \
    bunx tsx scripts/localnet-websocket-integrity-report.ts \
    --jsonl "$STRESS_JSONL_PATH" \
    --out "$REPORT_PATH" \
    --database-url "$DB_URL" \
    --timeout-ms "${INTEGRITY_TIMEOUT_MS:-240000}" \
    --poll-ms "${INTEGRITY_POLL_MS:-2000}"
)

echo "[integrity] done"
echo "[integrity] stress_summary=${STRESS_SUMMARY_PATH}"
echo "[integrity] stress_jsonl=${STRESS_JSONL_PATH}"
echo "[integrity] integrity_report=${REPORT_PATH}"
echo "[integrity] indexer_log=${INDEXER_LOG_PATH}"
