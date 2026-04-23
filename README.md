# iceberg-diff

Fast, parallel, schema-aware diff between two Iceberg snapshots.

Accepts two `catalog + table + snapshot_id` references (possibly from different
REST catalogs) and answers: **do the two datasets match?**

## Requirements

**Both tables must have a primary key.** iceberg-diff reads `identifier-field-ids`
from the Iceberg schema; if either side is missing it, the tool fails with a
schema error. A PK is required because the diff buckets rows by `hash(PK)` so
corresponding rows land in the same bucket on both sides — and because a future
drill-down step needs a way to uniquely identify rows when reporting cell-level
differences. The PK column set must match exactly (same names) on both sides.

## How it decides (cheapest → most expensive, short-circuits at each step)

1. **Identity** — same catalog + table + snapshot → EQUAL.
2. **Schema + partition spec + PK** — strict compare; mismatch = hard error, not a diff result.
3. **Snapshot summary** — `total-records` differs → UNEQUAL.
4. **File-set fingerprint** — identical manifest data-file set → EQUAL.
5. **Row-hash bucket scan** — the real work. Scan each side once, bucket each
   row by `hash(PK) % N`, sum `hash(row)` per bucket. Two sides match iff every
   bucket's `(sum, count)` pair matches.

## Why SUM-of-hashes, not XOR

XOR cancels duplicate rows: two genuinely different datasets with paired
duplicates can XOR to the same value. SUM mod 2^128 is commutative, associative,
and duplicate-safe. We also keep `COUNT(*)` per bucket to catch pure-cardinality
cases where SUM coincidentally collides.

## Parallelism

- **Between stages**: each prior stage fully short-circuits the next.
- **Between sides**: stages 4 and 5 run both sides concurrently via `tokio::try_join`.
- **Inside a scan**: DataFusion threads the Iceberg table scan across files and
  row groups; hashing happens on a rayon pool via `spawn_blocking`, one batch at
  a time with bounded prefetch.

## REST + vended credentials

The REST catalog is configured with the
`header.X-Iceberg-Access-Delegation: vended-credentials` header, so `LoadTable`
returns `s3.access-key-id / secret-access-key / session-token / region / endpoint`
in the table config. iceberg-rust's `FileIO` picks these up automatically when
reading data files. v1 assumes the diff finishes before the vended token
expires; longer-running scans will need a refresh wrapper.

## CLI

```
iceberg-diff \
  --a-uri https://catalog-a.example/iceberg \
  --a-warehouse wh1 \
  --a-table ns.orders \
  --a-snapshot 1234567890 \
  --b-uri https://catalog-b.example/iceberg \
  --b-warehouse wh2 \
  --b-table ns.orders \
  --b-snapshot 9876543210 \
  [--a-token ...] [--b-token ...] \
  [--buckets 128] \
  [--skip-row-count] [--skip-file-fingerprint]
```

Exit codes: `0` equal, `1` unequal, `2` schema/partition/PK mismatch, `3` runtime error.

## TPC-H integration test

`tests/tpch/run.sh` spins up a postgres + minio + iceberg-rest stack, generates
TPC-H data at `TPCH_SF` (default 10) with DuckDB, loads it into postgres, runs
`pg2iceberg --snapshot-only` twice into `tpch_a` and `tpch_b` namespaces, then
invokes the Rust integration test which asserts every TPC-H table diffs EQUAL.
The test is `#[ignore]`d by default; `run.sh` passes `--ignored`. It doubles as
a benchmark — per-table wall time for the diff is printed to stdout, separate
from the pg2iceberg snapshot times printed by the shell script.

Prerequisites on host: `docker`, `duckdb`, `psql`, `go`, `cargo`.
