# iceberg-diff

Diff two Iceberg tables. Inspired by https://github.com/datafold/data-diff.

## Install

### Prebuilt binary

```sh
# pick the asset for your platform from
#   https://github.com/polynya-dev/iceberg-diff/releases/latest
# e.g. Linux x86_64:
curl -fsSL \
  https://github.com/polynya-dev/iceberg-diff/releases/latest/download/iceberg-diff-x86_64-unknown-linux-musl.tar.gz \
  | tar -xz -C /usr/local/bin iceberg-diff
iceberg-diff --help
```

### Docker

```sh
docker run --rm ghcr.io/polynya-dev/iceberg-diff:latest \
  --a-uri ... \
  --a-table ns.t \
  --b-uri ... \
  --b-table ns.t
```

### From source via cargo

```sh
cargo install --git https://github.com/polynya-dev/iceberg-diff --bin iceberg-diff
```

## Usage

```sh
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
  [--check-file-fingerprint]     # opt into stage 4 (off by default)
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

On mismatch, currently it will only tell you that it does not match. The actual row diff will be implemented soon.

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
4. **File-set fingerprint** (opt-in, `--check-file-fingerprint`) — identical
   manifest data-file set → EQUAL. Off by default: it only helps when both
   sides reference the same physical files (e.g., a cross-catalog clone);
   for independently-written tables it's pure `plan_files` overhead.
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

Currently it assumes the diff finishes before the vended token expires; longer-
running scans will need a refresh wrapper.

## Tests

The test lives in [`tests/scenarios_integration.rs`](tests/scenarios_integration.rs)
and runs against an Iceberg REST catalog hosted on
[Cloudflare R2 Data Catalog](https://developers.cloudflare.com/r2/data-catalog/) (chosen because it's the simplest to setup).
The fixture is generated once and persisted; every CI run just reads from R2.

### Scenarios

Each row is one namespace in R2 with two tables `a` and `b`, built and
expected as follows (see [`src/scenarios.rs`](src/scenarios.rs) for the
authoritative list):

| Scenario              | What B does differently                             | Expected         |
|-----------------------|-----------------------------------------------------|------------------|
| `eq_identical`        | identical to A                                      | Equal            |
| `eq_shuffled`         | same rows, reverse insertion order                  | Equal            |
| `neq_missing_row`     | one row dropped                                     | Unequal          |
| `neq_extra_row`       | one extra row                                       | Unequal          |
| `neq_cell_modified`   | same rows, one cell value changed                   | Unequal          |
| `neq_many_cells`      | 1% of rows have a cell modified                     | Unequal          |
| `err_schema_types`    | `balance` column is `Double` instead of `Decimal`   | SchemaMismatch   |
| `err_no_pk_a`         | side A has no `identifier-field-ids`                | MissingPrimaryKey|
| `err_pk_differs`      | A PK `[id]` vs B PK `[id, email]`                   | SchemaMismatch   |
| `bench_sf1_lineitem`  | full TPC-H lineitem at sf=1, identical on both      | Equal     |

`neq_cell_modified` is the most load-bearing test — it's the only one where
stages 1–4 can't short-circuit, so it proves the row-hash bucket pipeline
actually works. Synthetic tables are 1000 rows with types chosen to stress
the hasher (bigint, varchar, decimal(12,2), date, boolean, nullable string).

### Running the test

```sh
# export your R2 Data Catalog credentials (see setup below)
export ICEBERG_DIFF_R2_URI=https://catalog.cloudflarestorage.com/<account>/<bucket>
export ICEBERG_DIFF_R2_WAREHOUSE=<account>_<bucket>
export ICEBERG_DIFF_R2_TOKEN=<cloudflare-api-token>

# correctness scenarios only (~25s against R2)
cargo test --test scenarios_integration --release -- --ignored --nocapture

# include the bench scenario (~3 min against R2)
ICEBERG_DIFF_BENCH=1 cargo test --test scenarios_integration --release -- --ignored --nocapture
```

### Regenerating the fixture

Only needed when the scenario list or synthetic table schema changes.

```sh
cargo install tpchgen-cli                  # only required for bench
cargo run --release --bin gen_fixture      # writes missing scenarios to R2

# nuke + rebuild every scenario (e.g., after a schema change)
FORCE_REBUILD=1 cargo run --release --bin gen_fixture
```

The generator is **idempotent**: if a namespace already contains both
expected tables it's skipped; if it's in partial state (say a previous run
crashed mid-way) it's cleaned up and rebuilt. R2 Data Catalog rate-limits
write-heavy bursts with 429s — the generator paces itself with 5-second
gaps between scenarios.
