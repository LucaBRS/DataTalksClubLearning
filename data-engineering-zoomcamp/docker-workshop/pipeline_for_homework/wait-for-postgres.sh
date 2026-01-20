#!/usr/bin/env bash
set -e

: "${PGHOST:=postgres}"
: "${PGPORT:=5432}"
: "${PGUSER:=root}"
: "${PGPASSWORD:=root}"
: "${PGDATABASE:=ny_taxi}"

export PGPASSWORD="$PGPASSWORD"

echo "Waiting for Postgres at ${PGHOST}:${PGPORT}..."
until pg_isready -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" >/dev/null 2>&1; do
  sleep 1
done

echo "Postgres is ready. Starting ingestion..."
exec "$@"
