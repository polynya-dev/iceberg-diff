#!/usr/bin/env bash
# Orchestrate the TPC-H integration test end-to-end.
#
# Pipeline:
#   1. docker compose up the stack (postgres, minio, iceberg-rest)
#   2. generate TPC-H data with DuckDB at sf=$TPCH_SF (default 10)
#   3. load into postgres via psql \copy
#   4. run pg2iceberg --snapshot-only twice (tpch_a, tpch_b namespaces)
#   5. run the Rust integration test which diffs each table A vs B
#
# Prereqs on host:
#   - docker + docker compose
#   - duckdb CLI         (brew install duckdb)
#   - psql client        (brew install libpq)
#   - go (for building pg2iceberg)
#   - cargo
#
# Colima users: the testcontainers-style stack here uses docker compose only,
# so TESTCONTAINERS_RYUK_DISABLED isn't relevant. See memory/reference_integration_tests.md.

set -euo pipefail
# Make any failure in a piped command (e.g. `cargo test ... | tee`) fail the
# script — without this, tee's exit code 0 masks test failures.
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CRATE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
PG2ICEBERG_DIR="$REPO_ROOT/pg2iceberg"

: "${TPCH_SF:=10}"
: "${SKIP_TEARDOWN:=0}"
: "${SKIP_DATA_LOAD:=0}"

DATA_DIR="${TPCH_DATA_DIR:-/tmp/iceberg-diff-tpch-sf${TPCH_SF}}"
PG2ICEBERG_BIN="${PG2ICEBERG_BIN:-/tmp/pg2iceberg-bin}"

PSQL_URL="postgres://postgres:postgres@127.0.0.1:55432/tpch?sslmode=disable"

log() { printf '\n=== %s ===\n' "$*" >&2; }

teardown() {
  if [[ "$SKIP_TEARDOWN" == "1" ]]; then
    log "SKIP_TEARDOWN=1 — leaving stack running at $SCRIPT_DIR"
    return
  fi
  log "tearing down docker-compose stack"
  (cd "$SCRIPT_DIR" && docker compose down -v) || true
}
trap teardown EXIT

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing required tool: $1" >&2; exit 1; }
}

require docker
require duckdb
require psql
require go
require cargo

log "building pg2iceberg binary"
(cd "$PG2ICEBERG_DIR" && go build -o "$PG2ICEBERG_BIN" ./cmd/pg2iceberg)

log "starting docker-compose stack"
(cd "$SCRIPT_DIR" && docker compose up -d --wait)

if [[ "$SKIP_DATA_LOAD" == "1" ]]; then
  log "SKIP_DATA_LOAD=1 — reusing existing postgres data"
else
  log "generating TPC-H sf=$TPCH_SF with DuckDB → $DATA_DIR"
  mkdir -p "$DATA_DIR"
  duckdb :memory: <<SQL
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=$TPCH_SF);
COPY region    TO '$DATA_DIR/region.csv'    (FORMAT CSV, HEADER false, DELIMITER '|');
COPY nation    TO '$DATA_DIR/nation.csv'    (FORMAT CSV, HEADER false, DELIMITER '|');
COPY part      TO '$DATA_DIR/part.csv'      (FORMAT CSV, HEADER false, DELIMITER '|');
COPY supplier  TO '$DATA_DIR/supplier.csv'  (FORMAT CSV, HEADER false, DELIMITER '|');
COPY partsupp  TO '$DATA_DIR/partsupp.csv'  (FORMAT CSV, HEADER false, DELIMITER '|');
COPY customer  TO '$DATA_DIR/customer.csv'  (FORMAT CSV, HEADER false, DELIMITER '|');
COPY orders    TO '$DATA_DIR/orders.csv'    (FORMAT CSV, HEADER false, DELIMITER '|');
COPY lineitem  TO '$DATA_DIR/lineitem.csv'  (FORMAT CSV, HEADER false, DELIMITER '|');
SQL

  log "creating postgres schema"
  psql "$PSQL_URL" -v ON_ERROR_STOP=1 -f "$SCRIPT_DIR/schema.sql"

  log "loading TPC-H data into postgres (this takes a while at sf=$TPCH_SF)"
  # Load parents before children so FK-less but semantically-dependent order is preserved.
  for t in region nation part supplier partsupp customer orders lineitem; do
    start=$SECONDS
    psql "$PSQL_URL" -v ON_ERROR_STOP=1 -c "\\copy $t FROM '$DATA_DIR/$t.csv' CSV DELIMITER '|'"
    echo "  loaded $t in $((SECONDS - start))s"
  done

  log "analyzing tables"
  psql "$PSQL_URL" -v ON_ERROR_STOP=1 -c "ANALYZE;"
fi

log "running pg2iceberg --snapshot-only for namespace tpch_a"
start_a=$SECONDS
"$PG2ICEBERG_BIN" --snapshot-only --config "$SCRIPT_DIR/pg2iceberg-a.yaml"
elapsed_a=$((SECONDS - start_a))

log "clearing pg2iceberg checkpoint state before snapshot B (--cleanup drops _pg2iceberg schema)"
# Both snapshot runs share the default pipeline_id and would see the same
# SnapshotComplete=true in _pg2iceberg.checkpoints. The built-in --cleanup
# subcommand resets the state.
"$PG2ICEBERG_BIN" --cleanup --config "$SCRIPT_DIR/pg2iceberg-a.yaml"

log "running pg2iceberg --snapshot-only for namespace tpch_b"
start_b=$SECONDS
"$PG2ICEBERG_BIN" --snapshot-only --config "$SCRIPT_DIR/pg2iceberg-b.yaml"
elapsed_b=$((SECONDS - start_b))

log "snapshot A: ${elapsed_a}s   snapshot B: ${elapsed_b}s"

log "running iceberg-diff Rust integration test"
export TPCH_REST_URL="http://127.0.0.1:58181"
export TPCH_NAMESPACE_A="tpch_a"
export TPCH_NAMESPACE_B="tpch_b"
# The local iceberg-rest-fixture does not return vended credentials. Inject
# MinIO's static creds directly; see tpch_integration.rs:catalog_spec().
export TPCH_S3_ENDPOINT="http://127.0.0.1:59000"
export TPCH_S3_ACCESS_KEY="admin"
export TPCH_S3_SECRET_KEY="password"
export TPCH_S3_REGION="us-east-1"
export TPCH_S3_PATH_STYLE="true"
(
  cd "$CRATE_DIR"
  cargo test --test tpch_integration --release -- --ignored --nocapture --test-threads=1
)
