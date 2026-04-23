#!/usr/bin/env bash
# Orchestrate the TPC-H integration test end-to-end without pg2iceberg.
#
#   1. docker compose up the stack (minio, iceberg-rest, iceberg-postgres)
#   2. run the Rust integration test — it drives tpchgen-cli to generate data
#      locally and writes two Iceberg copies via iceberg-rust to the running
#      REST catalog, then diffs them.
#
# Prereqs on host:
#   - docker + docker compose
#   - tpchgen-cli on PATH (or set TPCHGEN_BIN)
#     install via: cargo install tpchgen-cli
#   - cargo

set -euo pipefail
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

: "${TPCH_SF:=1}"
: "${SKIP_TEARDOWN:=0}"

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
require cargo

log "starting docker-compose stack (minio + iceberg-rest)"
(cd "$SCRIPT_DIR" && docker compose up -d --wait)

log "running iceberg-diff Rust integration test (sf=$TPCH_SF)"
export TPCH_SF
export TPCH_REST_URL="http://127.0.0.1:58181"
export TPCH_NAMESPACE_A="tpch_a"
export TPCH_NAMESPACE_B="tpch_b"
# The iceberg-rest-fixture does not return vended credentials. Inject
# MinIO's static creds directly; see tpch_integration.rs.
export TPCH_S3_ENDPOINT="http://127.0.0.1:59000"
export TPCH_S3_ACCESS_KEY="admin"
export TPCH_S3_SECRET_KEY="password"
export TPCH_S3_REGION="us-east-1"
export TPCH_S3_PATH_STYLE="true"

(
  cd "$CRATE_DIR"
  cargo test --test tpch_integration --release tpch_diff_self_contained -- --ignored --nocapture --test-threads=1
)
