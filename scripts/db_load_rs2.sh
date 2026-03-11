#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VENV_DIR="$ROOT_DIR/.venv_rs2db"
PYTHON_BIN="${PYTHON_BIN:-python3}"
CONTAINER_NAME="${RS2_PG_CONTAINER:-rs2-postgres}"
DB_URL="${DATABASE_URL:-postgresql://rs2:rs2@127.0.0.1:5433/rs2_dashboard?gssencmode=disable}"

if [[ ! -d "$VENV_DIR" ]]; then
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

"$VENV_DIR/bin/python" -m pip install --upgrade pip >/dev/null
"$VENV_DIR/bin/python" -m pip install -r "$ROOT_DIR/scripts/requirements-rs2-db.txt" >/dev/null

(cd "$ROOT_DIR" && docker compose up -d postgres >/dev/null)

ready=0
for _ in $(seq 1 60); do
  if docker exec "$CONTAINER_NAME" psql -U rs2 -d rs2_dashboard -tAc "SELECT 1" >/dev/null 2>&1; then
    ready=1
    break
  fi
  if docker exec "$CONTAINER_NAME" psql -U postgres -d postgres -tAc "SELECT 1" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done

if [[ "$ready" -ne 1 ]]; then
  echo "PostgreSQL container did not become ready." >&2
  docker logs "$CONTAINER_NAME" --tail 200 >&2 || true
  exit 1
fi

if docker exec "$CONTAINER_NAME" psql -U postgres -d postgres -tAc "SELECT 1" >/dev/null 2>&1; then
  ADMIN_USER="postgres"
else
  ADMIN_USER="rs2"
fi

docker exec "$CONTAINER_NAME" psql -U "$ADMIN_USER" -d postgres -v ON_ERROR_STOP=1 -c \
  "DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rs2') THEN CREATE ROLE rs2 LOGIN PASSWORD 'rs2'; ELSE ALTER ROLE rs2 WITH LOGIN PASSWORD 'rs2'; END IF; END \$\$;"

if ! docker exec "$CONTAINER_NAME" psql -U "$ADMIN_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = 'rs2_dashboard'" | grep -q 1; then
  docker exec "$CONTAINER_NAME" createdb -U "$ADMIN_USER" -O rs2 rs2_dashboard
fi

exec "$VENV_DIR/bin/python" "$ROOT_DIR/scripts/load_rs2_postgres.py" --db-url "$DB_URL" "$@"
