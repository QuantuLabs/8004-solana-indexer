#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INDEXER_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SOLANA_ROOT="$(cd "$INDEXER_ROOT/../8004-solana" && pwd)"
RPC_URL="${LOCALNET_RPC_URL:-http://127.0.0.1:8899}"
LOG_FILE="/tmp/8004-solana-test-validator.log"
PID_FILE="/tmp/8004-solana-test-validator.pid"
CLONE_ATOM_PROGRAM="${LOCALNET_CLONE_ATOM_PROGRAM:-0}"
PROGRAM_KEYPAIR="${LOCALNET_PROGRAM_KEYPAIR:-$SOLANA_ROOT/keys/devnet-program/8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C.json}"

validator_args=(
  --reset
  --ledger test-ledger
  --rpc-port 8899
  --bind-address 127.0.0.1
  --url https://api.devnet.solana.com
  --clone-upgradeable-program CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d
)
if [[ "$CLONE_ATOM_PROGRAM" == "1" ]]; then
  validator_args+=(--clone-upgradeable-program AToMufS4QD6hEXvcvBDg9m1AHeCLpmZQsyfYa5h9MwAF)
fi

"$SCRIPT_DIR/localnet-stop.sh" >/dev/null 2>&1 || true

echo "[localnet] starting solana-test-validator from $SOLANA_ROOT"
(
  cd "$SOLANA_ROOT/.anchor"
  nohup solana-test-validator \
    "${validator_args[@]}" \
    >"$LOG_FILE" 2>&1 &
  echo $! > "$PID_FILE"
)

max_attempts=60
attempt=1
ready=0
while (( attempt <= max_attempts )); do
  if solana -u "$RPC_URL" slot >/dev/null 2>&1; then
    ready=1
    break
  fi

  if [[ -f "$PID_FILE" ]]; then
    pid="$(cat "$PID_FILE")"
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      echo "[localnet] solana-test-validator exited early. Last logs:" >&2
      tail -n 120 "$LOG_FILE" >&2 || true
      exit 1
    fi
  fi

  sleep 1
  ((attempt++))
done

if [[ "$ready" != "1" ]]; then
  echo "[localnet] validator did not become ready after ${max_attempts}s. Last logs:" >&2
  tail -n 120 "$LOG_FILE" >&2 || true
  exit 1
fi

echo "[localnet] validator ready at $RPC_URL"
echo "[localnet] deploying 8004 program"
if ! (
  cd "$SOLANA_ROOT"
  anchor deploy \
    --provider.cluster http://127.0.0.1:8899 \
    --program-name agent_registry_8004 \
    --program-keypair "$PROGRAM_KEYPAIR"
); then
  echo "[localnet] deploy failed. validator logs:" >&2
  tail -n 120 "$LOG_FILE" >&2 || true
  exit 1
fi

echo "[localnet] validator ready and program deployed"
