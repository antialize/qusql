#!/bin/bash
set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

DB_CONTAINER="qusql-py-test-db"
DB_HOST=127.0.0.1
DB_PORT=1239
DB_PASS=test
DB_NAME=test

VENV="$SCRIPT_DIR/.venv"

cleanup() {
    echo "--- Stopping database ---"
    podman rm -f "$DB_CONTAINER" 2>/dev/null || true
    echo "--- Removing virtual environment ---"
    rm -rf "$VENV"
}
trap cleanup EXIT

# -- Database ------------------------------------------------------------------
echo "--- Starting MariaDB ---"
mkdir -p /dev/shm/mysql-py-test
podman run --replace --name "$DB_CONTAINER" --rm -d \
    -e MYSQL_ROOT_PASSWORD="$DB_PASS" \
    -e MYSQL_DATABASE="$DB_NAME" \
    -p "$DB_PORT:3306" \
    -v /dev/shm/mysql-py-test:/var/lib/mysql \
    docker.io/mariadb:10.5 \
    --innodb-flush-method=nosync

echo "--- Waiting for MariaDB to be ready ---"
for i in $(seq 1 30); do
    if podman exec "$DB_CONTAINER" mysqladmin ping --silent 2>/dev/null; then
        echo "    Ready after ${i}s"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "MariaDB did not become ready in time" >&2
        exit 1
    fi
    sleep 1
done

# -- Python environment --------------------------------------------------------
echo "--- Setting up Python virtual environment ---"
uv venv "$VENV"

uv pip install --python "$VENV" mysqlclient types-mysqlclient mypy

echo "--- Installing qusql-mysql-type ---"
uv pip install --python "$VENV" "$REPO_ROOT/qusql-py-mysql-type"

echo "--- Building and installing qusql-mysql-type-plugin ---"
uv pip install --python "$VENV" "$REPO_ROOT/qusql-py-mysql-type-plugin"

# -- Tests --------------------------------------------------------------------
cd "$SCRIPT_DIR"

echo "--- Running mypy ---"
"$VENV/bin/mypy" test.py

echo "--- Running integration tests ---"
DB_HOST="$DB_HOST" DB_PORT="$DB_PORT" DB_USER=root DB_PASS="$DB_PASS" DB_NAME="$DB_NAME" \
    "$VENV/bin/python" test.py

echo ""
echo "All checks passed."
