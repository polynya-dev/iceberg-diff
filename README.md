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

## Integration test suite

The test lives in [`tests/scenarios_integration.rs`](tests/scenarios_integration.rs)
and runs against an Iceberg REST catalog hosted on
**[Cloudflare R2 Data Catalog](https://developers.cloudflare.com/r2/data-catalog/)**.
The fixture is generated once and persisted; every CI run just reads from R2.
No Docker, no MinIO, no local iceberg-rest.

### Scenarios (= namespaces)

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
| `bench_sf1_lineitem`  | full TPC-H lineitem at sf=1, identical on both      | Equal (slow)     |

`neq_cell_modified` is the most load-bearing test — it's the only one where
stages 1–4 can't short-circuit, so it proves the row-hash bucket pipeline
actually works. Synthetic tables are 1000 rows with types chosen to stress
the hasher (bigint, varchar, decimal(12,2), date, boolean, nullable string).
The bench scenario uses TPC-H `lineitem` at sf=1 (~6M rows) split across
100 small parquets; see [Benchmark caveat](#benchmark-caveat) below.

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

If any of `ICEBERG_DIFF_R2_*` is unset, the test self-skips with a notice —
safe for CI on PRs from forks where secrets aren't exposed.

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

### R2 Data Catalog setup

1. Create an R2 bucket. Note the `<account>` and `<bucket>` names.
2. In the Cloudflare dashboard, enable **Data Catalog** on the bucket.
3. Create an API token scoped to the R2 account with read+write on the
   bucket and Data Catalog access.
4. The warehouse URI and id follow the standard pattern:
   - `uri = https://catalog.cloudflarestorage.com/<account>/<bucket>`
   - `warehouse = <account>_<bucket>`
5. Export them as env vars above, or drop them into an `r2.txt` file in
   the crate root (line 1 = uri, line 2 = warehouse, line 3 = blank,
   line 4 = token). **Gitignore `r2.txt`** before committing anything.

### Benchmark caveat

The `bench_sf1_lineitem` scenario writes lineitem as 100 small parquet
files (~4 MiB each) to dodge an R2 multipart-upload incompatibility: R2
enforces AWS S3's "all non-trailing multipart parts must have the same
length" rule, but iceberg-rust 0.5 calls opendal's default writer which
emits non-uniform parts. Small-files-only keeps every write a single
`PutObject`. This is functionally equivalent to what production CDC-fed
Iceberg tables look like anyway.

Proper upstream fix: teach iceberg-rust's FileIO to pass a configurable
`chunk()` into `op.writer_with()`. Tracked as a TODO; the workaround in
the benchmark builder ([`src/bin/gen_fixture.rs`](src/bin/gen_fixture.rs))
is a pointer for anyone else running Iceberg-on-R2 from Rust.

### CI

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on every push
and PR to `main`:

- `build` — `cargo build --release --all-targets` + `cargo test --lib`
  (no creds needed).
- `scenarios` — runs the full correctness suite against R2 using secrets
  `ICEBERG_DIFF_R2_URI`, `ICEBERG_DIFF_R2_WAREHOUSE`, `ICEBERG_DIFF_R2_TOKEN`.

Benchmark can be triggered on-demand via `workflow_dispatch` with the
`include_bench` input checked.