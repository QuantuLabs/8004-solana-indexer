#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env.devnet.monitor.alchemy}"
INTERVAL_SECONDS="${INTERVAL_SECONDS:-30}"
LOCAL_PORT="${LOCAL_PORT:-3951}"
SUPABASE_PORT="${SUPABASE_PORT:-3952}"
LOCAL_DB_PATH="${LOCAL_DB_PATH:-$ROOT_DIR/prisma/prisma/.tmp/alchemy-full-local.db}"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

curl_status() {
  local port="$1"
  local path="$2"
  : > /tmp/monitor-body.$$
  curl -s -o /tmp/monitor-body.$$ -w '%{http_code}' --max-time 5 "http://127.0.0.1:${port}${path}" 2>/dev/null || true
}

show_endpoint() {
  local name="$1"
  local port="$2"
  local health_code ready_code
  health_code="$(curl_status "$port" "/health")"
  local health_body
  health_body="$(cat /tmp/monitor-body.$$ 2>/dev/null || true)"
  ready_code="$(curl_status "$port" "/ready")"
  local ready_body
  ready_body="$(cat /tmp/monitor-body.$$ 2>/dev/null || true)"
  log "${name} port=${port} health=${health_code:-down} ready=${ready_code:-down} health_body=${health_body:-n/a} ready_body=${ready_body:-n/a}"
}

show_local_db() {
  if [[ ! -f "$LOCAL_DB_PATH" ]]; then
    log "local_db missing path=$LOCAL_DB_PATH"
    return
  fi

  sqlite3 "$LOCAL_DB_PATH" "
    select 'local.state|'||count(*) from \"IndexerState\" where id like 'historical-scan:%'
    union all
    select 'local.main|'||coalesce(lastSlot,'null')||'|'||coalesce(lastTxIndex,'null') from \"IndexerState\" where id='main'
    union all
    select 'local.agents|'||count(*) from \"Agent\"
    union all
    select 'local.feedbacks|'||count(*) from \"Feedback\"
    union all
    select 'local.responses|'||count(*) from \"FeedbackResponse\"
    union all
    select 'local.revocations|'||count(*) from \"Revocation\"
    union all
    select 'local.collections|'||count(*) from \"CollectionPointer\"
    union all
    select 'local.orphanf|'||count(*) from \"OrphanFeedback\"
    union all
    select 'local.orphanr|'||count(*) from \"OrphanResponse\";
  " | while IFS= read -r line; do
    log "$line"
  done
}

show_supabase_db() {
  if [[ -z "${SUPABASE_DSN:-}" ]]; then
    log "supabase_dsn missing"
    return
  fi

  if ! psql "$SUPABASE_DSN" -Atqc "
    select 'supabase.state|'||count(*) from indexer_state where id like 'historical-scan:%'
    union all
    select 'supabase.main|'||coalesce(last_slot::text,'null')||'|'||coalesce(last_tx_index::text,'null') from indexer_state where id='main'
    union all
    select 'supabase.agents|'||count(*) from agents
    union all
    select 'supabase.feedbacks|'||count(*) from feedbacks
    union all
    select 'supabase.responses|'||count(*) from feedback_responses
    union all
    select 'supabase.revocations|'||count(*) from revocations
    union all
    select 'supabase.collections|'||count(*) from collection_pointers
    union all
    select 'supabase.orphanf|'||count(*) from orphan_feedbacks
    union all
    select 'supabase.orphanr|'||count(*) from orphan_responses;
  " 2>/dev/null | while IFS= read -r line; do
    log "$line"
  done; then
    log "supabase_db unavailable"
  fi
}

show_processes() {
  local local_pid supabase_pid
  local_pid="$(lsof -ti tcp:${LOCAL_PORT} 2>/dev/null | head -n 1 || true)"
  supabase_pid="$(lsof -ti tcp:${SUPABASE_PORT} 2>/dev/null | head -n 1 || true)"
  log "pids local=${local_pid:-none} supabase=${supabase_pid:-none}"
  if [[ -n "${local_pid:-}" || -n "${supabase_pid:-}" ]]; then
    ps -o pid,stat,etime,command= -p "${local_pid:-0},${supabase_pid:-0}" 2>/dev/null | sed '1d' | while IFS= read -r line; do
      [[ -n "$line" ]] && log "proc $line"
    done
  fi
}

trap 'rm -f /tmp/monitor-body.$$' EXIT

while true; do
  show_processes
  show_endpoint "local" "$LOCAL_PORT"
  show_endpoint "supabase" "$SUPABASE_PORT"
  show_local_db
  show_supabase_db
  sleep "$INTERVAL_SECONDS"
done
