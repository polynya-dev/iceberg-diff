# iceberg-diff

Fast, parallel, schema-aware diff between two Iceberg snapshots.

Accepts two `catalog + table + snapshot_id` references (possibly from
different REST catalogs) and answers: **do the two datasets match?**

## Requirements

**Both tables must have a primary key.** iceberg-diff reads
`identifier-field-ids` from the Iceberg schema; if either side is missing
it, the tool fails with a schema error. The PK is required because the diff
buckets rows by `hash(PK)` so corresponding rows land in the same bucket on
both sides — and because a future drill-down step needs a way to uniquely
identify rows when reporting cell-level differences. The PK column set must
match exactly (same names) on both sides.

## How it decides

Five stages, cheapest → most expensive, short-circuiting at each step:

1. **Identity** — same catalog + table + snapshot → EQUAL.
2. **Schema + partition spec + PK** — strict compare; mismatch = hard error,
   not a diff result.
3. **Snapshot summary** — `total-records` differs → UNEQUAL.
4. **File-set fingerprint** — identical manifest data-file set → EQUAL.
5. **Row-hash bucket scan** — the real work. Scan each side once, bucket
   each row by `hash(PK) % N`, sum `hash(row)` per bucket. Two sides match
   iff every bucket's `(sum, count)` pair matches.

## Why SUM-of-hashes, not XOR

XOR cancels duplicate rows: two genuinely different datasets with paired
duplicates would XOR to the same value. SUM mod 2^128 is still
commutative/associative (order-independent across parallel shards) and
duplicate-safe. The paired COUNT(*) per bucket catches the rare case where
SUMs coincidentally collide on differing cardinalities.

## Parallelism

- **Between stages**: each prior stage fully short-circuits the next.
- **Between sides**: stages 4 and 5 run both sides concurrently via
  `tokio::try_join`.
- **Inside a scan**: iceberg-rust's `TableScan` threads across data files;
  each batch is hashed on a `spawn_blocking` task with bounded prefetch so
  memory stays bounded on wide tables.

## REST + vended credentials

The REST catalog is configured with the
`header.X-Iceberg-Access-Delegation: vended-credentials` header, so
`LoadTable` returns `s3.access-key-id / secret-access-key / session-token /
region / endpoint` in the table config (when the catalog supports vending).
iceberg-rust's `FileIO` picks these up automatically when reading data
files.

For catalogs that do **not** vend credentials (e.g., the local JDBC-backed
`iceberg-rest-fixture` used by the integration test), pass static S3
properties via `--prop`, see CLI below.

v1 assumes the diff finishes before the vended token expires; longer-
running scans will need a refresh wrapper.

## CLI

```
iceberg-diff \
  --a-uri https://catalog-a.example/iceberg \
  --a-warehouse wh1 \
  --a-table ns.orders \
  [--a-snapshot 1234567890] \
  [--a-token ... | --a-client-id ... --a-client-secret ...] \
  \
  --b-uri https://catalog-b.example/iceberg \
  --b-warehouse wh2 \
  --b-table ns.orders \
  [--b-snapshot 9876543210] \
  [--b-token ... | --b-client-id ... --b-client-secret ...] \
  \
  [--prop KEY=VALUE]...          # repeatable; applies to both sides
  [--buckets 128]
  [--skip-row-count]             # skip stage 3
  [--skip-file-fingerprint]      # skip stage 4
```

`--a-snapshot`/`--b-snapshot` default to the table's current snapshot if
omitted. `--prop` is the escape hatch for catalogs that don't vend S3
credentials — e.g.:

```
--prop s3.endpoint=http://localhost:9000 \
--prop s3.access-key-id=admin \
--prop s3.secret-access-key=password \
--prop s3.region=us-east-1 \
--prop s3.path-style-access=true
```

Exit codes: `0` equal, `1` unequal, `2` schema/partition/PK mismatch,
`3` runtime error.

## TPC-H integration test

Make sure to install `tpchgen-cli` before running:

```sh
cargo install tpchgen-cli
```

To run:

```sh
# default TPCH_SF=1 (smoke). Set TPCH_SF=10 for the full-spec run.
bash tests/tpch/run.sh

# keep the stack up between runs for fast iteration
SKIP_TEARDOWN=1 bash tests/tpch/run.sh
```

Example run with `SF=10`:

```sh
running 1 test
test tpch_diff_self_contained ... run namespaces: tpch_a_d45c47f5, tpch_b_d45c47f5
tpchgen-cli sf=10: 4.85s
write both namespaces: 86.63s
region      EQUAL        32 ms  (AllBucketsMatch { rows: 5 })
nation      EQUAL        23 ms  (AllBucketsMatch { rows: 25 })
supplier    EQUAL       135 ms  (AllBucketsMatch { rows: 100000 })
part        EQUAL       978 ms  (AllBucketsMatch { rows: 2000000 })
partsupp    EQUAL      6089 ms  (AllBucketsMatch { rows: 8000000 })
customer    EQUAL      1446 ms  (AllBucketsMatch { rows: 1500000 })
orders      EQUAL      7454 ms  (AllBucketsMatch { rows: 15000000 })
lineitem    EQUAL     26422 ms  (AllBucketsMatch { rows: 59986052 })

TPC-H diff total wall time: 42.58s
ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 134.14s
```