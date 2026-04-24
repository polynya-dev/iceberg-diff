//! Fixture generator: writes every scenario in `iceberg_diff::scenarios` to
//! the R2 Data Catalog. One-shot, idempotent (drops the scenario's namespace
//! first then re-creates).
//!
//! Invocation:
//!     ICEBERG_DIFF_R2_URI=...       (https://catalog.cloudflarestorage.com/<account>/<bucket>)
//!     ICEBERG_DIFF_R2_WAREHOUSE=... (<account>_<bucket>)
//!     ICEBERG_DIFF_R2_TOKEN=...     (Cloudflare API token with catalog+R2 perms)
//!     cargo run --release --bin gen_fixture
//!
//! As a convenience, if `$ICEBERG_DIFF_R2_URI` is unset, we read 3 lines from
//! `./r2.txt` in the working dir — which must be gitignored.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array, StringArray,
};
use arrow::record_batch::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema as IceSchema, Type};
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_diff::scenarios::{Scenario, ScenarioKind, ALL_SCENARIOS};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

struct R2Config {
    uri: String,
    warehouse: String,
    token: String,
}

fn load_r2_config() -> Result<R2Config> {
    if let (Ok(uri), Ok(warehouse), Ok(token)) = (
        std::env::var("ICEBERG_DIFF_R2_URI"),
        std::env::var("ICEBERG_DIFF_R2_WAREHOUSE"),
        std::env::var("ICEBERG_DIFF_R2_TOKEN"),
    ) {
        return Ok(R2Config {
            uri,
            warehouse,
            token,
        });
    }
    // Fallback: r2.txt with three lines: URI, warehouse, <blank>, token.
    let text = std::fs::read_to_string("r2.txt")
        .context("r2.txt not found and ICEBERG_DIFF_R2_* env vars not set")?;
    let lines: Vec<&str> = text.lines().collect();
    if lines.len() < 4 {
        bail!("r2.txt expected format: line1=URI, line2=warehouse, line3=blank, line4=token");
    }
    Ok(R2Config {
        uri: lines[0].trim().to_string(),
        warehouse: lines[1].trim().to_string(),
        token: lines[3].trim().to_string(),
    })
}

fn build_catalog(cfg: &R2Config) -> Arc<RestCatalog> {
    let mut props = HashMap::new();
    props.insert("token".into(), cfg.token.clone());
    props.insert(
        "header.X-Iceberg-Access-Delegation".into(),
        "vended-credentials".into(),
    );
    let rcfg = RestCatalogConfig::builder()
        .uri(cfg.uri.clone())
        .warehouse(cfg.warehouse.clone())
        .props(props)
        .build();
    Arc::new(RestCatalog::new(rcfg))
}

// -----------------------------------------------------------------------------
// Synthetic 1000-row "customer-like" table used by correctness scenarios.
// Deterministic generation (seeded purely by row id).
// -----------------------------------------------------------------------------

const SYNTH_ROWS: i64 = 1000;

/// One synthetic row. Explicit struct so mutations (modify one cell, drop a
/// row, add a row) are straightforward Vec<SynthRow> edits before
/// materialization.
#[derive(Clone, Debug)]
struct SynthRow {
    id: i64,
    name: Option<String>,
    email: String,
    balance: Option<i128>,
    signup_date: Option<i32>,
    is_active: bool,
    tags: Option<String>,
}

fn synth_rows(count: i64) -> Vec<SynthRow> {
    (1..=count)
        .map(|i| SynthRow {
            id: i,
            name: Some(format!("name-{i:05}")),
            email: format!("user{i}@example.test"),
            balance: Some(((i * 137) % 100000) as i128 - 50000),
            signup_date: Some(18000 + (i % 365) as i32),
            is_active: i % 3 != 0,
            tags: if i % 5 == 0 {
                None
            } else {
                Some(format!("tag-{}", i % 10))
            },
        })
        .collect()
}

/// Iceberg schema used by the correctness synthetic table. PK is `id`.
fn synth_iceberg_schema() -> Result<IceSchema> {
    let fields = vec![
        Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::optional(
            2,
            "name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(
            3,
            "email",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            4,
            "balance",
            Type::Primitive(PrimitiveType::Decimal {
                precision: 12,
                scale: 2,
            }),
        )),
        Arc::new(NestedField::optional(
            5,
            "signup_date",
            Type::Primitive(PrimitiveType::Date),
        )),
        Arc::new(NestedField::required(
            6,
            "is_active",
            Type::Primitive(PrimitiveType::Boolean),
        )),
        Arc::new(NestedField::optional(
            7,
            "tags",
            Type::Primitive(PrimitiveType::String),
        )),
    ];
    Ok(IceSchema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .with_identifier_field_ids(vec![1])
        .build()?)
}

fn rows_to_batch(rows: &[SynthRow], schema: &IceSchema) -> Result<RecordBatch> {
    use iceberg::arrow::schema_to_arrow_schema;
    let arrow_schema: Arc<ArrowSchema> = Arc::new(schema_to_arrow_schema(schema)?);

    let id_arr: ArrayRef = Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.id)));
    let name_arr: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|r| r.name.clone()).collect::<Vec<_>>(),
    ));
    let email_arr: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|r| r.email.clone()).collect::<Vec<_>>(),
    ));
    let balance_arr: ArrayRef = Arc::new(
        Decimal128Array::from(rows.iter().map(|r| r.balance).collect::<Vec<_>>())
            .with_precision_and_scale(12, 2)
            .context("decimal precision/scale")?,
    );
    let signup_arr: ArrayRef = Arc::new(Date32Array::from(
        rows.iter().map(|r| r.signup_date).collect::<Vec<_>>(),
    ));
    let active_arr: ArrayRef = Arc::new(BooleanArray::from(
        rows.iter().map(|r| r.is_active).collect::<Vec<_>>(),
    ));
    let tags_arr: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|r| r.tags.clone()).collect::<Vec<_>>(),
    ));

    Ok(RecordBatch::try_new(
        arrow_schema,
        vec![
            id_arr,
            name_arr,
            email_arr,
            balance_arr,
            signup_arr,
            active_arr,
            tags_arr,
        ],
    )?)
}

// -----------------------------------------------------------------------------
// Catalog helpers
// -----------------------------------------------------------------------------

/// Drop a namespace and everything in it if it exists. Errors are swallowed —
/// we only use this on `--force`.
async fn cleanup_namespace(catalog: &Arc<RestCatalog>, ns: &NamespaceIdent) {
    if let Ok(tables) = catalog.list_tables(ns).await {
        for t in tables {
            let _ = catalog.drop_table(&t).await;
        }
    }
    let _ = catalog.drop_namespace(ns).await;
}

/// Inspect the namespace's state relative to the expected table list.
#[derive(Debug)]
enum NamespaceState {
    /// Namespace doesn't exist (or is empty) — safe to create fresh.
    Missing,
    /// All expected tables are present — can skip this scenario.
    Complete,
    /// Namespace exists with only a subset of expected tables. Usually the
    /// result of a failed generator run; we clean up and rebuild rather than
    /// trying to surgically add the missing ones, because the set-of-snapshot
    /// we want from the scenario is atomic (one fast_append per table, not
    /// accreted across partial runs).
    Partial,
}

async fn namespace_state(
    catalog: &Arc<RestCatalog>,
    ns: &NamespaceIdent,
    expected: &[&str],
) -> NamespaceState {
    let Ok(listed) = catalog.list_tables(ns).await else {
        return NamespaceState::Missing;
    };
    if listed.is_empty() {
        return NamespaceState::Missing;
    }
    let names: std::collections::HashSet<&str> = listed.iter().map(|t| t.name()).collect();
    if expected.iter().all(|t| names.contains(t)) {
        NamespaceState::Complete
    } else {
        NamespaceState::Partial
    }
}

/// Respect `$FORCE_REBUILD=1` as the single opt-in to nuke + rebuild every
/// scenario. By default, reruns are a no-op for already-built scenarios.
fn force_rebuild() -> bool {
    matches!(
        std::env::var("FORCE_REBUILD").as_deref(),
        Ok("1" | "true" | "yes")
    )
}

/// Shared prelude for every builder: cleanup if `--force`, otherwise early-
/// return if both tables already exist. Callers proceed with `create_namespace`
/// only when they actually need to build.
async fn prepare_namespace(
    catalog: &Arc<RestCatalog>,
    ns: &NamespaceIdent,
    tables: &[&str],
) -> Result<bool> {
    if force_rebuild() {
        cleanup_namespace(catalog, ns).await;
    } else {
        match namespace_state(catalog, ns, tables).await {
            NamespaceState::Complete => {
                println!("  already built, skipping (set FORCE_REBUILD=1 to redo)");
                return Ok(false);
            }
            NamespaceState::Partial => {
                println!("  partial state detected, cleaning up before rebuild");
                cleanup_namespace(catalog, ns).await;
            }
            NamespaceState::Missing => {}
        }
    }
    // Best-effort create; ignore "already exists" which can happen if a
    // prior run left the (now-empty) namespace behind.
    let _ = catalog.create_namespace(ns, HashMap::new()).await;
    Ok(true)
}

async fn create_and_write(
    catalog: &Arc<RestCatalog>,
    ns: &NamespaceIdent,
    table: &str,
    schema: IceSchema,
    batches: Vec<RecordBatch>,
    partition_spec: Option<iceberg::spec::UnboundPartitionSpec>,
) -> Result<()> {
    let ident = TableIdent::new(ns.clone(), table.to_string());
    let creation = TableCreation::builder()
        .name(table.to_string())
        .schema(schema)
        .partition_spec_opt(partition_spec)
        .build();
    let table = catalog
        .create_table(ns, creation)
        .await
        .with_context(|| format!("create {ident}"))?;

    let loc_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
    let name_gen = DefaultFileNameGenerator::new(
        "data".into(),
        Some(Uuid::new_v4().to_string()),
        DataFileFormat::Parquet,
    );
    let pq = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        table.metadata().current_schema().clone(),
        table.file_io().clone(),
        loc_gen,
        name_gen,
    );
    let df = DataFileWriterBuilder::new(pq, None, 0);
    let mut writer = df.build().await?;
    for batch in batches {
        writer.write(batch).await?;
    }
    let data_files = writer.close().await?;

    let tx = Transaction::new(&table);
    let mut action = tx.fast_append(None, vec![])?;
    action.add_data_files(data_files)?;
    let tx = action.apply().await?;
    tx.commit(&**catalog)
        .await
        .with_context(|| format!("commit {ident}"))?;
    Ok(())
}

// -----------------------------------------------------------------------------
// Per-scenario builders
// -----------------------------------------------------------------------------

/// Build a correctness scenario. `mutate_b` edits side B's row vector
/// before materialization; side A is always the unmodified baseline.
async fn build_correctness(
    catalog: &Arc<RestCatalog>,
    s: &Scenario,
    schema_a: IceSchema,
    schema_b: IceSchema,
    mutate_b: impl FnOnce(&mut Vec<SynthRow>),
    partition_spec_b: Option<iceberg::spec::UnboundPartitionSpec>,
) -> Result<()> {
    let ns = NamespaceIdent::from_strs([s.ns_a])?;
    if !prepare_namespace(catalog, &ns, &[s.table_a, s.table_b]).await? {
        return Ok(());
    }

    let mut rows_a = synth_rows(SYNTH_ROWS);
    let mut rows_b = rows_a.clone();
    mutate_b(&mut rows_b);

    let batch_a = rows_to_batch(&rows_a, &schema_a)?;
    let _ = &mut rows_a; // explicit drop not needed
    let batch_b = rows_to_batch(&rows_b, &schema_b)?;

    create_and_write(catalog, &ns, s.table_a, schema_a, vec![batch_a], None).await?;
    create_and_write(catalog, &ns, s.table_b, schema_b, vec![batch_b], partition_spec_b).await?;
    Ok(())
}

async fn build_eq_identical(catalog: &Arc<RestCatalog>, s: &Scenario) -> Result<()> {
    let schema = synth_iceberg_schema()?;
    build_correctness(catalog, s, schema.clone(), schema, |_| {}, None).await
}

async fn build_eq_shuffled(catalog: &Arc<RestCatalog>, s: &Scenario) -> Result<()> {
    let schema = synth_iceberg_schema()?;
    build_correctness(catalog, s, schema.clone(), schema, |b| b.reverse(), None).await
}

/// Build a table with identical synth rows, controlling the Iceberg format
/// version via `properties["format-version"]`. Side-of-the-world equivalent
/// of `create_and_write` but parameterized on format version.
async fn create_and_write_with_version(
    catalog: &Arc<RestCatalog>,
    ns: &NamespaceIdent,
    table_name: &str,
    schema: IceSchema,
    batch: RecordBatch,
    format_version: u32,
) -> Result<()> {
    let mut props: HashMap<String, String> = HashMap::new();
    props.insert("format-version".into(), format_version.to_string());

    let ident = TableIdent::new(ns.clone(), table_name.to_string());
    let creation = TableCreation::builder()
        .name(table_name.to_string())
        .schema(schema)
        .properties(props)
        .build();
    let table = catalog
        .create_table(ns, creation)
        .await
        .with_context(|| format!("create {ident}"))?;
    println!(
        "    {table_name}: requested v{format_version}, got {:?}",
        table.metadata().format_version()
    );

    let loc_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
    let name_gen = DefaultFileNameGenerator::new(
        "data".into(),
        Some(Uuid::new_v4().to_string()),
        DataFileFormat::Parquet,
    );
    let pq = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        table.metadata().current_schema().clone(),
        table.file_io().clone(),
        loc_gen,
        name_gen,
    );
    let df = DataFileWriterBuilder::new(pq, None, 0);
    let mut writer = df.build().await?;
    writer.write(batch).await?;
    let data_files = writer.close().await?;

    let tx = Transaction::new(&table);
    let mut action = tx.fast_append(None, vec![])?;
    action.add_data_files(data_files)?;
    let tx = action.apply().await?;
    tx.commit(&**catalog)
        .await
        .with_context(|| format!("commit {ident}"))?;
    Ok(())
}

async fn build_eq_cross_version(catalog: &Arc<RestCatalog>, s: &Scenario) -> Result<()> {
    let ns = NamespaceIdent::from_strs([s.ns_a])?;
    if !prepare_namespace(catalog, &ns, &[s.table_a, s.table_b]).await? {
        return Ok(());
    }
    let schema = synth_iceberg_schema()?;
    let batch = rows_to_batch(&synth_rows(SYNTH_ROWS), &schema)?;
    create_and_write_with_version(catalog, &ns, s.table_a, schema.clone(), batch.clone(), 1).await?;
    create_and_write_with_version(catalog, &ns, s.table_b, schema, batch, 2).await?;
    Ok(())
}

async fn build_neq_missing_row(catalog: &Arc<RestCatalog>, s: &Scenario) -> Result<()> {
    let schema = synth_iceberg_schema()?;
    build_correctness(
        catalog,
        s,
        schema.clone(),
        schema,
        |b| b.retain(|r| r.id != 500),
        None,
    )
    .await
}

async fn build_neq_extra_row(catalog: &Arc<RestCatalog>, s: &Scenario) -> Result<()> {
    let schema = synth_iceberg_schema()?;
    build_correctness(
        catalog,
        s,
        schema.clone(),
        schema,
        |b| {
            b.push(SynthRow {
                id: 9999,
                name: Some("extra".into()),
                email: "extra@example.test".into(),
                balance: Some(0),
                signup_date: Some(18000),
                is_active: true,
                tags: Some("extra".into()),
            })
        },
        None,
    )
    .await
}

async fn build_neq_cell_modified(catalog: &Arc<RestCatalog>, s: &Scenario) -> Result<()> {
    let schema = synth_iceberg_schema()?;
    build_correctness(
        catalog,
        s,
        schema.clone(),
        schema,
        |b| {
            if let Some(row) = b.iter_mut().find(|r| r.id == 500) {
                row.name = Some("MUTATED".into());
            }
        },
        None,
    )
    .await
}

async fn build_neq_many_cells(catalog: &Arc<RestCatalog>, s: &Scenario) -> Result<()> {
    let schema = synth_iceberg_schema()?;
    build_correctness(
        catalog,
        s,
        schema.clone(),
        schema,
        |b| {
            for row in b.iter_mut().filter(|r| r.id % 100 == 0) {
                row.name = Some(format!("MUTATED-{}", row.id));
            }
        },
        None,
    )
    .await
}

/// Schema variant where `balance` is Float on B instead of Decimal. All other
/// columns match. Triggers SchemaMismatch.
fn synth_schema_b_floatbalance() -> Result<IceSchema> {
    let fields = vec![
        Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::optional(
            2,
            "name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(
            3,
            "email",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            4,
            "balance",
            Type::Primitive(PrimitiveType::Double), // <-- changed
        )),
        Arc::new(NestedField::optional(
            5,
            "signup_date",
            Type::Primitive(PrimitiveType::Date),
        )),
        Arc::new(NestedField::required(
            6,
            "is_active",
            Type::Primitive(PrimitiveType::Boolean),
        )),
        Arc::new(NestedField::optional(
            7,
            "tags",
            Type::Primitive(PrimitiveType::String),
        )),
    ];
    Ok(IceSchema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .with_identifier_field_ids(vec![1])
        .build()?)
}

/// Build a 1-row batch against the balance-as-Double schema. Needed because
/// our synthetic row generator only emits Decimal128 — for the schema-
/// mismatch scenario we have to hand-craft a tiny batch matching side B's
/// divergent type so B has a snapshot to load. The diff fails at stage 2
/// (schema compare) long before scanning, so 1 row is enough.
fn tiny_float_balance_batch(schema: &IceSchema) -> Result<RecordBatch> {
    use iceberg::arrow::schema_to_arrow_schema;
    let arrow_schema: Arc<ArrowSchema> = Arc::new(schema_to_arrow_schema(schema)?);
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values([1i64])),
        Arc::new(StringArray::from(vec![Some("probe")])),
        Arc::new(StringArray::from(vec!["probe@example.test"])),
        Arc::new(Float64Array::from(vec![Some(0.0)])),
        Arc::new(Date32Array::from(vec![Some(18000)])),
        Arc::new(BooleanArray::from(vec![true])),
        Arc::new(StringArray::from(vec![Some("probe")])),
    ];
    Ok(RecordBatch::try_new(arrow_schema, cols)?)
}

async fn build_err_schema_types(catalog: &Arc<RestCatalog>, s: &Scenario) -> Result<()> {
    let ns = NamespaceIdent::from_strs([s.ns_a])?;
    if !prepare_namespace(catalog, &ns, &[s.table_a, s.table_b]).await? {
        return Ok(());
    }

    let schema_a = synth_iceberg_schema()?;
    let schema_b = synth_schema_b_floatbalance()?;

    let rows_a = synth_rows(SYNTH_ROWS);
    let batch_a = rows_to_batch(&rows_a, &schema_a)?;
    create_and_write(catalog, &ns, s.table_a, schema_a, vec![batch_a], None).await?;

    // Side B: 1 probe row matching the divergent schema. Enough to produce a
    // snapshot; the diff fails at stage 2 (schema compare) without scanning.
    let batch_b = tiny_float_balance_batch(&schema_b)?;
    create_and_write(catalog, &ns, s.table_b, schema_b, vec![batch_b], None).await?;
    Ok(())
}

/// 3-field synthetic schema (id, name, email) with or without PK. Used by
/// err_no_pk_a.
fn three_col_schema(with_pk: bool) -> Result<IceSchema> {
    let fields = vec![
        Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::optional(
            2,
            "name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(
            3,
            "email",
            Type::Primitive(PrimitiveType::String),
        )),
    ];
    let mut b = IceSchema::builder().with_schema_id(0).with_fields(fields);
    if with_pk {
        b = b.with_identifier_field_ids(vec![1]);
    }
    Ok(b.build()?)
}

fn tiny_three_col_batch(schema: &IceSchema) -> Result<RecordBatch> {
    use iceberg::arrow::schema_to_arrow_schema;
    let arrow_schema: Arc<ArrowSchema> = Arc::new(schema_to_arrow_schema(schema)?);
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values([1i64, 2])),
        Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
        Arc::new(StringArray::from(vec!["a@x", "b@x"])),
    ];
    Ok(RecordBatch::try_new(arrow_schema, cols)?)
}

async fn build_err_no_pk_a(catalog: &Arc<RestCatalog>, s: &Scenario) -> Result<()> {
    let ns = NamespaceIdent::from_strs([s.ns_a])?;
    if !prepare_namespace(catalog, &ns, &[s.table_a, s.table_b]).await? {
        return Ok(());
    }

    let schema_a = three_col_schema(false)?;
    let schema_b = three_col_schema(true)?;
    let batch_a = tiny_three_col_batch(&schema_a)?;
    let batch_b = tiny_three_col_batch(&schema_b)?;

    create_and_write(catalog, &ns, s.table_a, schema_a, vec![batch_a], None).await?;
    create_and_write(catalog, &ns, s.table_b, schema_b, vec![batch_b], None).await?;
    Ok(())
}

/// Build the TPC-H `lineitem` benchmark scenario via a "many small files"
/// layout that works around R2's strict S3 multipart-uniform-size rule.
///
/// Background: iceberg-rust 0.5's `ParquetWriter` calls `op.writer(path)`
/// with opendal defaults, which emits non-uniform multipart parts (last
/// row group + footer are smaller). AWS S3 accepts this; R2 rejects it
/// with `InvalidPart: All non-trailing parts must have the same length`.
/// Fix until the upstream writer exposes a chunk config: keep each output
/// parquet under R2's 5 MiB multipart threshold so every write is a single
/// `PutObject` (no multipart, no uniform-size constraint).
///
/// We use `tpchgen-cli --parts=N` to split lineitem at generation time,
/// then write one iceberg `DataFile` per input part. All DataFiles are
/// committed in a single `fast_append` so the table has one snapshot
/// regardless of part count.
async fn build_bench_lineitem(
    catalog: &Arc<RestCatalog>,
    s: &Scenario,
    scale_factor: u32,
) -> Result<()> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let ns = NamespaceIdent::from_strs([s.ns_a])?;
    if !prepare_namespace(catalog, &ns, &[s.table_a, s.table_b]).await? {
        return Ok(());
    }

    // Size the parts so each output parquet stays ~3-4 MiB (under R2's 5 MiB
    // multipart threshold). TPC-H lineitem is ~400 MiB at sf=1, ~4 GiB at
    // sf=10.
    let parts: u32 = std::cmp::max(100, scale_factor * 100);
    let bin = tpchgen_bin();
    let tmp = tempfile::TempDir::new()?;
    println!(
        "  running {} -s {scale_factor} --parts={parts} --tables=lineitem --format=parquet",
        bin.display()
    );
    let status = std::process::Command::new(&bin)
        .arg("-s")
        .arg(scale_factor.to_string())
        .arg("--tables=lineitem")
        .arg("--parts")
        .arg(parts.to_string())
        .arg("--format=parquet")
        .arg("--output-dir")
        .arg(tmp.path())
        .status()
        .with_context(|| format!("launch {}", bin.display()))?;
    if !status.success() {
        bail!("tpchgen-cli exited with {status}");
    }

    // Find every .parquet file produced under the tempdir (tpchgen puts them
    // in a `lineitem/` subdir when --parts is used).
    let mut part_files = Vec::new();
    for entry in walkdir_parquets(tmp.path())? {
        part_files.push(entry);
    }
    part_files.sort();
    if part_files.is_empty() {
        bail!(
            "tpchgen-cli produced no parquet files in {}",
            tmp.path().display()
        );
    }
    println!("  {} part files produced", part_files.len());

    // Derive the iceberg schema from the first part (they all share the
    // same arrow schema) and restamp each batch against the iceberg-aware
    // arrow schema (the one with PARQUET:field_id metadata).
    let first_file = std::fs::File::open(&part_files[0])?;
    let first_builder = ParquetRecordBatchReaderBuilder::try_new(first_file)?;
    let src_arrow = first_builder.schema().clone();
    let schema = lineitem_iceberg_schema(&src_arrow)?;
    let target_arrow: Arc<ArrowSchema> =
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&schema)?);

    for table_name in [s.table_a, s.table_b] {
        let ident = TableIdent::new(ns.clone(), table_name.to_string());
        let creation = TableCreation::builder()
            .name(table_name.to_string())
            .schema(schema.clone())
            .build();
        let table = catalog
            .create_table(&ns, creation)
            .await
            .with_context(|| format!("create {ident}"))?;

        // One DataFileWriter per input part → one parquet per part under the
        // table's data/ dir. Each stays small enough to PutObject in one shot.
        let mut all_files: Vec<iceberg::spec::DataFile> =
            Vec::with_capacity(part_files.len());
        for (i, part_path) in part_files.iter().enumerate() {
            let file = std::fs::File::open(part_path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let reader = builder.build()?;
            let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;
            let restamped: Vec<RecordBatch> = batches
                .into_iter()
                .map(|b| {
                    RecordBatch::try_new(target_arrow.clone(), b.columns().to_vec())
                        .map_err(anyhow::Error::from)
                })
                .collect::<Result<_>>()?;

            let loc_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
            let name_gen = DefaultFileNameGenerator::new(
                "data".into(),
                Some(Uuid::new_v4().to_string()),
                DataFileFormat::Parquet,
            );
            let pq = ParquetWriterBuilder::new(
                WriterProperties::builder().build(),
                table.metadata().current_schema().clone(),
                table.file_io().clone(),
                loc_gen,
                name_gen,
            );
            let df = DataFileWriterBuilder::new(pq, None, 0);
            let mut writer = df.build().await?;
            for batch in restamped {
                writer.write(batch).await?;
            }
            let part_data_files = writer.close().await?;
            all_files.extend(part_data_files);
            if (i + 1) % 25 == 0 || i + 1 == part_files.len() {
                println!("    {table_name}: {}/{} parts written", i + 1, part_files.len());
            }
        }

        // Single commit for all data files.
        let tx = Transaction::new(&table);
        let mut action = tx.fast_append(None, vec![])?;
        action.add_data_files(all_files)?;
        let tx = action.apply().await?;
        tx.commit(&**catalog)
            .await
            .with_context(|| format!("commit {ident}"))?;
    }
    Ok(())
}

/// Recursive parquet walker (no external dep — we only have `std::fs`).
fn walkdir_parquets(root: &std::path::Path) -> Result<Vec<std::path::PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                out.push(path);
            }
        }
    }
    Ok(out)
}

fn tpchgen_bin() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("TPCHGEN_BIN") {
        return std::path::PathBuf::from(p);
    }
    if let Ok(home) = std::env::var("HOME") {
        let cargo_bin = std::path::PathBuf::from(home).join(".cargo/bin/tpchgen-cli");
        if cargo_bin.exists() {
            return cargo_bin;
        }
    }
    std::path::PathBuf::from("tpchgen-cli")
}

/// Build the lineitem iceberg schema from the source parquet's arrow schema,
/// assigning sequential field IDs 1..=N and marking PK = [l_orderkey, l_linenumber].
fn lineitem_iceberg_schema(arrow_schema: &ArrowSchema) -> Result<IceSchema> {
    use iceberg::arrow::arrow_type_to_type;

    let mut fields = Vec::with_capacity(arrow_schema.fields().len());
    let mut name_to_id: HashMap<String, i32> = HashMap::new();
    for (i, f) in arrow_schema.fields().iter().enumerate() {
        let id = (i as i32) + 1;
        let ty = arrow_type_to_type(f.data_type())
            .with_context(|| format!("convert type for '{}'", f.name()))?;
        let is_pk = matches!(f.name().as_str(), "l_orderkey" | "l_linenumber");
        let nested = if f.is_nullable() && !is_pk {
            NestedField::optional(id, f.name(), ty)
        } else {
            NestedField::required(id, f.name(), ty)
        };
        fields.push(Arc::new(nested));
        name_to_id.insert(f.name().clone(), id);
    }
    let pk_ids = vec![
        *name_to_id
            .get("l_orderkey")
            .context("l_orderkey missing from lineitem parquet")?,
        *name_to_id
            .get("l_linenumber")
            .context("l_linenumber missing from lineitem parquet")?,
    ];
    Ok(IceSchema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .with_identifier_field_ids(pk_ids)
        .build()?)
}

async fn build_err_pk_differs(catalog: &Arc<RestCatalog>, s: &Scenario) -> Result<()> {
    let ns = NamespaceIdent::from_strs([s.ns_a])?;
    if !prepare_namespace(catalog, &ns, &[s.table_a, s.table_b]).await? {
        return Ok(());
    }

    let schema_a = synth_iceberg_schema()?; // PK = [id]
    let schema_b_double_pk = IceSchema::builder()
        .with_schema_id(0)
        .with_fields(synth_iceberg_schema()?.as_struct().fields().iter().cloned())
        .with_identifier_field_ids(vec![1, 3]) // PK = [id, email]
        .build()?;

    // Write a small number of rows on both sides — same columns, so the
    // normal synthetic generator works. PK must be unique on (id, email) on
    // side B too; our generator satisfies this.
    let rows = synth_rows(10);
    let batch_a = rows_to_batch(&rows, &schema_a)?;
    let batch_b = rows_to_batch(&rows, &schema_b_double_pk)?;

    create_and_write(catalog, &ns, s.table_a, schema_a, vec![batch_a], None).await?;
    create_and_write(
        catalog,
        &ns,
        s.table_b,
        schema_b_double_pk,
        vec![batch_b],
        None,
    )
    .await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,iceberg=warn".into()),
        )
        .init();

    let r2 = load_r2_config()?;
    println!("R2 uri      : {}", r2.uri);
    println!("R2 warehouse: {}", r2.warehouse);
    let catalog = build_catalog(&r2);

    let inter_scenario_pause = std::time::Duration::from_secs(5);
    for (idx, s) in ALL_SCENARIOS.iter().enumerate() {
        if idx > 0 {
            // R2 Data Catalog has a per-catalog write rate limit; a brief
            // pause between scenarios keeps us under it.
            tokio::time::sleep(inter_scenario_pause).await;
        }
        let started = std::time::Instant::now();
        println!("\n=== building '{}' (kind={:?}) ===", s.id, s.kind);
        match s.kind {
            ScenarioKind::EqIdentical => build_eq_identical(&catalog, s).await?,
            ScenarioKind::EqShuffled => build_eq_shuffled(&catalog, s).await?,
            ScenarioKind::EqCrossVersion => build_eq_cross_version(&catalog, s).await?,
            ScenarioKind::NeqMissingRow => build_neq_missing_row(&catalog, s).await?,
            ScenarioKind::NeqExtraRow => build_neq_extra_row(&catalog, s).await?,
            ScenarioKind::NeqCellModified => build_neq_cell_modified(&catalog, s).await?,
            ScenarioKind::NeqManyCells => build_neq_many_cells(&catalog, s).await?,
            ScenarioKind::ErrSchemaTypes => build_err_schema_types(&catalog, s).await?,
            ScenarioKind::ErrNoPkA => build_err_no_pk_a(&catalog, s).await?,
            ScenarioKind::ErrPkDiffers => build_err_pk_differs(&catalog, s).await?,
            ScenarioKind::BenchTpchLineitem { scale_factor } => {
                build_bench_lineitem(&catalog, s, scale_factor).await?
            }
        }
        println!(
            "built '{}' in {:.2}s",
            s.id,
            started.elapsed().as_secs_f64()
        );
    }
    Ok(())
}
