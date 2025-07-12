#!/usr/bin/env bash
set -e

# Load environment variables from .env file for database and app configuration
export $(awk -F= '/^[A-Za-z_][A-Za-z0-9_]*=/{gsub(/#.*/,"",$2); gsub(/^[ \t]+|[ \t]+$/,"",$2); print $1"="$2}' .env)

DATABASE_URL="postgresql://$DATABASE_USER:$DATABASE_PASS@$DATABASE_HOST:$DATABASE_PORT/$DATABASE_NAME"

# Function to mask password in DATABASE_URL for logs
get_masked_url() {
  # Mask the password in the URL for logging
  # e.g., postgresql://user:****@host:port/db
  echo "$DATABASE_URL" | sed -E 's#(postgresql://[^:]+:)[^@]+(@.*)#\1****\2#'
}

MAX_TRIES=30
TRIES=0
until OUTPUT=$(poetry run python -c "import psycopg; psycopg.connect('$DATABASE_URL').close()" 2>&1); do
  TRIES=$((TRIES+1))
  MASKED_URL=$(get_masked_url)
  echo "[entrypoint][DEBUG] Attempt $TRIES: DATABASE_URL='$MASKED_URL'"
  echo "[entrypoint][DEBUG] psycopg output: $OUTPUT"
  if [ $TRIES -ge $MAX_TRIES ]; then
    echo "[entrypoint] ERROR: Could not connect to Postgres at $MASKED_URL after $MAX_TRIES attempts."
    exit 1
  fi
  echo "[entrypoint] Waiting for Postgres at $MASKED_URL... ($TRIES/$MAX_TRIES)"
  sleep 3
done

echo "[entrypoint] Postgres is available. Initializing DB schema..."
poetry run python -c "import db; import os; db_url=os.environ.get('DATABASE_URL', '$DATABASE_URL'); conn=db.get_connection(db_url); db.create_schema(conn); conn.close()"

echo "[entrypoint] Starting FastAPI app with Uvicorn..."
exec poetry run uvicorn api:app --host 0.0.0.0 --port 8000 