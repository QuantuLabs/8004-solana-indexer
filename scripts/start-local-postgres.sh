#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PG_BIN="${PG_BIN:-/opt/homebrew/opt/postgresql@17/bin}"
PGDATA="${PGDATA:-$ROOT_DIR/.pg/devnet-local}"
PGPORT="${PGPORT:-55432}"
PGHOST="${PGHOST:-127.0.0.1}"
PGDATABASE="${PGDATABASE:-indexer}"
PGUSER_ADMIN="${PGUSER_ADMIN:-postgres}"
INDEXER_ROLE="${INDEXER_ROLE:-indexer}"
INDEXER_PASSWORD="${INDEXER_PASSWORD:-indexer}"

if [ ! -x "$PG_BIN/postgres" ] || [ ! -x "$PG_BIN/pg_ctl" ] || [ ! -x "$PG_BIN/initdb" ] || [ ! -x "$PG_BIN/psql" ] || [ ! -x "$PG_BIN/createdb" ]; then
  echo "PostgreSQL binaries not found under PG_BIN=$PG_BIN" >&2
  exit 1
fi

mkdir -p "$(dirname "$PGDATA")"

if [ ! -f "$PGDATA/PG_VERSION" ]; then
  "$PG_BIN/initdb" -D "$PGDATA" --locale=C --encoding=UTF8 --username="$PGUSER_ADMIN" --auth=trust >/tmp/indexer-initdb.log
fi

"$PG_BIN/pg_ctl" -D "$PGDATA" -l "$PGDATA/server.log" -o "-p $PGPORT -h $PGHOST" start >/tmp/indexer-pgctl-start.log || true
sleep 2

"$PG_BIN/psql" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER_ADMIN" -d postgres -v ON_ERROR_STOP=1 <<SQL
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '$INDEXER_ROLE') THEN
    CREATE ROLE $INDEXER_ROLE LOGIN PASSWORD '$INDEXER_PASSWORD';
  ELSE
    ALTER ROLE $INDEXER_ROLE WITH LOGIN PASSWORD '$INDEXER_PASSWORD';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
    CREATE ROLE anon NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
    CREATE ROLE authenticated NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
    CREATE ROLE service_role NOLOGIN;
  END IF;
END \$\$;
SQL

if ! "$PG_BIN/psql" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER_ADMIN" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$PGDATABASE'" | grep -q 1; then
  "$PG_BIN/createdb" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER_ADMIN" -O "$INDEXER_ROLE" "$PGDATABASE"
fi

echo "Postgres ready on postgres://$INDEXER_ROLE:$INDEXER_PASSWORD@$PGHOST:$PGPORT/$PGDATABASE"
echo "Load fresh schema with:"
echo "  PGPASSWORD=$INDEXER_PASSWORD $PG_BIN/psql -h $PGHOST -p $PGPORT -U $INDEXER_ROLE -d $PGDATABASE -f $ROOT_DIR/supabase/schema.sql"
