//! Self-contained TPC-H integration test (REST catalog flavor).
//!
//! Doubles as a benchmark. Pipeline:
//!
//!   1. `tpchgen-cli -s $TPCH_SF --format=parquet` writes 8 TPC-H tables to a
//!      tempdir as raw Parquet files.
//!   2. We connect to an Iceberg REST catalog at `$TPCH_REST_URL` (brought up
//!      by `tests/tpch/run.sh` via docker-compose).
//!   3. For each TPC-H table × each namespace (`tpch_a_<run>`, `tpch_b_<run>`),
//!      we build an Iceberg schema from the source Arrow schema, attach
//!      `identifier-field-ids` for the table's PK, create the Iceberg table,
//!      and write the data through iceberg-rust's `DataFileWriter` +
//!      `fast_append`. Two independent write passes per table → two
//!      independent data-file sets → iceberg-diff's stage-4 shortcut does NOT
//!      fire and stage 5 (hash-bucket scan) actually runs.
//!   4. For each table, we diff `tpch_a.T` against `tpch_b.T` and assert EQUAL.
//!
//! Intentionally has NO dependency on pg2iceberg or postgres — the
//! circular-dependency concern. Does depend on minio + iceberg-rest via
//! docker-compose since iceberg-rust 0.5's in-process MemoryCatalog doesn't
//! support `update_table` and the crate is yanked on crates.io.
//!
//! Prereqs: `tpchgen-cli` on PATH (or set `TPCHGEN_BIN`; install via
//! `cargo install tpchgen-cli`). Stack brought up by `tests/tpch/run.sh`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use arrow::record_batch::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use iceberg::arrow::{arrow_type_to_type, schema_to_arrow_schema};
use iceberg::spec::{DataFileFormat, NestedField, Schema as IceSchema};
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_diff::catalog::{DynCatalog, LoadedTable};
use iceberg_diff::{diff_tables, DiffOptions, DiffOutcome};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;
use uuid::Uuid;

/// (table name, PK column names) from the TPC-H spec.
const TPCH_TABLES: &[(&str, &[&str])] = &[
    ("region", &["r_regionkey"]),
    ("nation", &["n_nationkey"]),
    ("supplier", &["s_suppkey"]),
    ("part", &["p_partkey"]),
    ("partsupp", &["ps_partkey", "ps_suppkey"]),
    ("customer", &["c_custkey"]),
    ("orders", &["o_orderkey"]),
    ("lineitem", &["l_orderkey", "l_linenumber"]),
];

fn tpchgen_bin() -> PathBuf {
    if let Ok(p) = std::env::var("TPCHGEN_BIN") {
        return PathBuf::from(p);
    }
    if let Ok(home) = std::env::var("HOME") {
        let cargo_bin = PathBuf::from(home).join(".cargo/bin/tpchgen-cli");
        if cargo_bin.exists() {
            return cargo_bin;
        }
    }
    PathBuf::from("tpchgen-cli")
}

fn gen_tpch(scale_factor: u32, out: &Path) -> Result<()> {
    std::fs::create_dir_all(out)?;
    let bin = tpchgen_bin();
    let status = Command::new(&bin)
        .arg("-s")
        .arg(scale_factor.to_string())
        .arg("--format")
        .arg("parquet")
        .arg("--output-dir")
        .arg(out)
        .status()
        .with_context(|| format!("launch {}", bin.display()))?;
    if !status.success() {
        anyhow::bail!("{} exited with {status}", bin.display());
    }
    Ok(())
}

fn read_parquet(path: &Path) -> Result<(Arc<ArrowSchema>, Vec<RecordBatch>)> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("open {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let reader = builder.build()?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
    Ok((schema, batches))
}

/// Build an Iceberg schema from the source Arrow schema by assigning
/// sequential field ids `1..=N` to the top-level columns. `iceberg::arrow::
/// arrow_schema_to_schema` requires each arrow field to already carry a
/// `PARQUET:field_id` metadata entry, which raw parquet from tpchgen-cli
/// doesn't have. TPC-H is flat (no nested types), so sequential top-level IDs
/// are sufficient. PK columns are promoted to `required` and added to
/// `identifier-field-ids`.
fn build_iceberg_schema(arrow_schema: &ArrowSchema, pk_cols: &[&str]) -> Result<IceSchema> {
    let mut fields = Vec::with_capacity(arrow_schema.fields().len());
    let mut name_to_id: HashMap<String, i32> = HashMap::new();
    for (i, f) in arrow_schema.fields().iter().enumerate() {
        let id = (i as i32) + 1;
        let ty = arrow_type_to_type(f.data_type())
            .with_context(|| format!("convert type for '{}'", f.name()))?;
        let is_pk = pk_cols.iter().any(|pk| *pk == f.name());
        let nested = if f.is_nullable() && !is_pk {
            NestedField::optional(id, f.name(), ty)
        } else {
            NestedField::required(id, f.name(), ty)
        };
        fields.push(Arc::new(nested));
        name_to_id.insert(f.name().clone(), id);
    }
    let pk_ids: Vec<i32> = pk_cols
        .iter()
        .map(|name| {
            name_to_id
                .get(*name)
                .copied()
                .ok_or_else(|| anyhow!("PK column '{name}' missing from source schema"))
        })
        .collect::<Result<_>>()?;
    Ok(IceSchema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .with_identifier_field_ids(pk_ids)
        .build()?)
}

/// Re-stamp a RecordBatch against a new Arrow schema. Only field metadata
/// differs (iceberg's `schema_to_arrow_schema` attaches `PARQUET:field_id`;
/// the source parquet's schema has no such metadata). Columns themselves are
/// reused by reference.
fn restamp(batch: &RecordBatch, target: Arc<ArrowSchema>) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(target, batch.columns().to_vec())?)
}

/// Create the Iceberg table + write the batches as a single fast-append snapshot.
async fn create_and_write(
    catalog: DynCatalog,
    namespace: &NamespaceIdent,
    table_name: &str,
    ice_schema: IceSchema,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    let ident = TableIdent::new(namespace.clone(), table_name.to_string());

    let creation = TableCreation::builder()
        .name(table_name.to_string())
        .schema(ice_schema)
        .build();
    let table = catalog
        .create_table(namespace, creation)
        .await
        .with_context(|| format!("create table {ident}"))?;

    let file_io = table.file_io().clone();
    let loc_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
    let name_gen = DefaultFileNameGenerator::new(
        "data".to_string(),
        Some(Uuid::new_v4().to_string()),
        DataFileFormat::Parquet,
    );
    let pq_builder = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        table.metadata().current_schema().clone(),
        file_io,
        loc_gen,
        name_gen,
    );
    let df_builder = DataFileWriterBuilder::new(pq_builder, None, 0);
    let mut writer = df_builder.build().await?;
    for batch in batches {
        writer.write(batch).await?;
    }
    let data_files = writer.close().await?;

    let tx = Transaction::new(&table);
    let mut action = tx.fast_append(None, vec![])?;
    action.add_data_files(data_files)?;
    let tx = action.apply().await?;
    tx.commit(&*catalog)
        .await
        .with_context(|| format!("commit {ident}"))?;
    Ok(())
}

/// Build a REST catalog from env: TPCH_REST_URL + optional TPCH_S3_*.
fn build_rest_catalog() -> Result<DynCatalog> {
    let uri = std::env::var("TPCH_REST_URL")
        .context("TPCH_REST_URL not set; run tests/tpch/run.sh which brings up the stack")?;
    let mut props: HashMap<String, String> = HashMap::new();
    for (env_key, prop_key) in [
        ("TPCH_S3_ENDPOINT", "s3.endpoint"),
        ("TPCH_S3_ACCESS_KEY", "s3.access-key-id"),
        ("TPCH_S3_SECRET_KEY", "s3.secret-access-key"),
        ("TPCH_S3_REGION", "s3.region"),
        ("TPCH_S3_PATH_STYLE", "s3.path-style-access"),
    ] {
        if let Ok(v) = std::env::var(env_key) {
            props.insert(prop_key.to_string(), v);
        }
    }
    let cfg = RestCatalogConfig::builder().uri(uri).props(props).build();
    Ok(Arc::new(RestCatalog::new(cfg)))
}

#[tokio::test]
#[ignore = "heavy: generates + writes TPC-H sf=TPCH_SF; run via tests/tpch/run.sh"]
async fn tpch_diff_self_contained() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,iceberg=warn".into()),
        )
        .try_init()
        .ok();

    let scale_factor: u32 = std::env::var("TPCH_SF")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    // Unique namespaces per run so re-runs against a reused catalog stack
    // don't collide with prior tables. `tpch_a_<uuid8>` / `tpch_b_<uuid8>`.
    let run_suffix = &Uuid::new_v4().simple().to_string()[..8];
    let ns_a_name = format!("tpch_a_{run_suffix}");
    let ns_b_name = format!("tpch_b_{run_suffix}");
    println!("run namespaces: {ns_a_name}, {ns_b_name}");

    let src_dir = TempDir::new()?;
    let t_gen = Instant::now();
    gen_tpch(scale_factor, src_dir.path()).context("tpchgen-cli")?;
    println!(
        "tpchgen-cli sf={scale_factor}: {:.2}s",
        t_gen.elapsed().as_secs_f64()
    );

    let catalog = build_rest_catalog()?;

    for ns in [&ns_a_name, &ns_b_name] {
        let ns_ident = NamespaceIdent::from_strs([ns])?;
        catalog
            .create_namespace(&ns_ident, HashMap::new())
            .await
            .with_context(|| format!("create namespace {ns}"))?;
    }

    let t_write = Instant::now();
    for (name, pk) in TPCH_TABLES {
        let src = src_dir.path().join(format!("{name}.parquet"));
        let (arrow_schema, batches) = read_parquet(&src)?;
        let ice_schema = build_iceberg_schema(&arrow_schema, pk)?;
        let target_arrow: Arc<ArrowSchema> = Arc::new(schema_to_arrow_schema(&ice_schema)?);
        let restamped: Vec<RecordBatch> = batches
            .iter()
            .map(|b| restamp(b, target_arrow.clone()))
            .collect::<Result<_>>()?;

        for ns in [&ns_a_name, &ns_b_name] {
            let ns_ident = NamespaceIdent::from_strs([ns])?;
            create_and_write(
                catalog.clone(),
                &ns_ident,
                name,
                ice_schema.clone(),
                restamped.clone(),
            )
            .await?;
        }
    }
    println!(
        "write both namespaces: {:.2}s",
        t_write.elapsed().as_secs_f64()
    );

    let t_diff_total = Instant::now();
    let mut failed = Vec::new();
    for (name, _pk) in TPCH_TABLES {
        let started = Instant::now();
        let (a, b) = tokio::try_join!(
            LoadedTable::from_catalog_current(
                catalog.clone(),
                vec![ns_a_name.clone()],
                name.to_string(),
            ),
            LoadedTable::from_catalog_current(
                catalog.clone(),
                vec![ns_b_name.clone()],
                name.to_string(),
            ),
        )?;
        let outcome = diff_tables(a, b, DiffOptions::default()).await?;
        let ms = started.elapsed().as_millis();
        match outcome {
            DiffOutcome::Equal(reason) => {
                println!("{name:<10}  EQUAL    {ms:>6} ms  ({reason:?})");
            }
            DiffOutcome::Unequal(reason) => {
                println!("{name:<10}  UNEQUAL  {ms:>6} ms  ({reason:?})");
                failed.push((*name).to_string());
            }
        }
    }
    println!(
        "\nTPC-H diff total wall time: {:.2}s",
        t_diff_total.elapsed().as_secs_f64()
    );

    assert!(
        failed.is_empty(),
        "expected all tables EQUAL; unequal: {failed:?}"
    );
    Ok(())
}
