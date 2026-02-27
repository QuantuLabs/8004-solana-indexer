#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INDEXER_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cleanup() {
  if [[ "${KEEP_LOCALNET:-0}" != "1" ]]; then
    echo "[localnet] stopping validator"
    "$SCRIPT_DIR/localnet-stop.sh"
  else
    echo "[localnet] KEEP_LOCALNET=1 -> validator left running"
  fi
}

trap cleanup EXIT INT TERM

echo "[localnet] stopping any stale validator"
"$SCRIPT_DIR/localnet-stop.sh"

echo "[localnet] starting validator"
LOCALNET_CLONE_ATOM_PROGRAM="${LOCALNET_CLONE_ATOM_PROGRAM:-1}" \
  "$SCRIPT_DIR/localnet-start.sh"

echo "[localnet] initializing localnet state"
"$SCRIPT_DIR/localnet-init.sh"

echo "[localnet] running indexer Localnet E2E + metadata digestion checks"
(
  cd "$INDEXER_ROOT"
  VITEST_E2E_MAX_WORKERS="${VITEST_E2E_MAX_WORKERS:-1}" \
  bun run test:localnet:only
)
