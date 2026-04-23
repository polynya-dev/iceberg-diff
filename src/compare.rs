use std::collections::VecDeque;

use arrow::record_batch::RecordBatch;
use futures::stream::StreamExt;
use iceberg::scan::ArrowRecordBatchStream;
use iceberg::spec::PartitionSpec;
use iceberg::spec::Schema as IcebergSchema;
use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::bucket::{hash_batch, BucketAgg, HashContext, DEFAULT_BUCKETS};
use crate::catalog::LoadedTable;
use crate::error::{Error, Result};
use crate::schema::{self, PrimaryKey};
use crate::shortcut;

#[derive(Debug, Clone)]
pub struct DiffOptions {
    /// Number of hash buckets. Higher = finer drill-down granularity. Default 128.
    pub buckets: usize,
    pub skip_row_count: bool,
    pub skip_file_fingerprint: bool,
    /// Max in-flight hash tasks between the scan and the aggregate merge.
    pub prefetch_batches: usize,
    /// Data-file concurrency passed to iceberg-rust's TableScan.
    pub scan_concurrency: usize,
}

impl Default for DiffOptions {
    fn default() -> Self {
        let par = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            buckets: DEFAULT_BUCKETS,
            skip_row_count: false,
            skip_file_fingerprint: false,
            prefetch_batches: par,
            scan_concurrency: par,
        }
    }
}

#[derive(Debug, Clone)]
pub enum DiffOutcome {
    Equal(EqualReason),
    Unequal(UnequalReason),
}

#[derive(Debug, Clone)]
pub enum EqualReason {
    Identity,
    FileSet,
    AllBucketsMatch { rows: u64 },
}

#[derive(Debug, Clone)]
pub enum UnequalReason {
    RowCount {
        a: String,
        b: String,
    },
    Buckets {
        mismatched: Vec<usize>,
        total_buckets: usize,
        rows_a: u64,
        rows_b: u64,
    },
}

pub async fn diff_tables(
    a: LoadedTable,
    b: LoadedTable,
    opts: DiffOptions,
) -> Result<DiffOutcome> {
    // Stage 1: identity
    if shortcut::identity_match(&a, &b) {
        info!("stage 1: identical ref (catalog+table+snapshot); EQUAL");
        return Ok(DiffOutcome::Equal(EqualReason::Identity));
    }

    // Stage 2: schema + partition spec + PK (strict)
    let (a_schema, a_spec) = snapshot_schema_and_spec(&a)?;
    let (b_schema, b_spec) = snapshot_schema_and_spec(&b)?;
    let pk: PrimaryKey = schema::assert_compatible(&a_schema, &a_spec, &b_schema, &b_spec)?;
    info!("stage 2: schema + partition spec match; primary key = {:?}", pk);

    // Stage 3: snapshot-summary row count
    if !opts.skip_row_count {
        if let Some(false) = shortcut::row_count_match(&a, &b) {
            let ra = shortcut::row_count_values(&a).unwrap_or_default();
            let rb = shortcut::row_count_values(&b).unwrap_or_default();
            info!("stage 3: row counts differ ({} vs {}); UNEQUAL", ra, rb);
            return Ok(DiffOutcome::Unequal(UnequalReason::RowCount { a: ra, b: rb }));
        }
        debug!("stage 3: row counts match or unavailable");
    }

    // Stage 4: file-set fingerprint
    if !opts.skip_file_fingerprint {
        let (fa, fb) = tokio::try_join!(
            shortcut::collect_file_fingerprint(&a),
            shortcut::collect_file_fingerprint(&b),
        )?;
        if fa == fb {
            info!("stage 4: identical data-file set; EQUAL");
            return Ok(DiffOutcome::Equal(EqualReason::FileSet));
        }
        debug!("stage 4: data-file sets differ; proceeding to stage 5");
    }

    // Stage 5: row-hash bucket scan (bucket by hash(PK), sum hash(row))
    info!(
        "stage 5: scanning + hashing both sides (buckets={})",
        opts.buckets
    );
    let (agg_a, agg_b) =
        tokio::try_join!(scan_side(&a, &pk, &opts), scan_side(&b, &pk, &opts))?;

    if agg_a.total_rows != agg_b.total_rows {
        info!(
            "stage 5: scanned row counts differ ({} vs {})",
            agg_a.total_rows, agg_b.total_rows
        );
    }

    match agg_a.diff(&agg_b) {
        None => {
            info!(
                "stage 5: all {} buckets match ({} rows); EQUAL",
                opts.buckets, agg_a.total_rows
            );
            Ok(DiffOutcome::Equal(EqualReason::AllBucketsMatch {
                rows: agg_a.total_rows,
            }))
        }
        Some(mismatched) => {
            info!(
                "stage 5: {}/{} buckets differ; UNEQUAL",
                mismatched.len(),
                opts.buckets
            );
            Ok(DiffOutcome::Unequal(UnequalReason::Buckets {
                mismatched,
                total_buckets: opts.buckets,
                rows_a: agg_a.total_rows,
                rows_b: agg_b.total_rows,
            }))
        }
    }
}

fn snapshot_schema_and_spec(t: &LoadedTable) -> Result<(IcebergSchema, PartitionSpec)> {
    let md = t.table.metadata();
    let schema = if let Some(sid) = t.snapshot.schema_id() {
        md.schema_by_id(sid)
            .ok_or_else(|| Error::Other(format!("schema id {sid} not found in metadata")))?
            .as_ref()
            .clone()
    } else {
        md.current_schema().as_ref().clone()
    };
    let spec = md.default_partition_spec().as_ref().clone();
    Ok((schema, spec))
}

async fn scan_side(
    t: &LoadedTable,
    pk: &[String],
    opts: &DiffOptions,
) -> Result<BucketAgg> {
    let scan = t
        .table
        .scan()
        .snapshot_id(t.spec.snapshot_id)
        .with_concurrency_limit(opts.scan_concurrency)
        .build()?;
    let mut stream: ArrowRecordBatchStream = scan.to_arrow().await?;

    // Pull the first batch to lock in the arrow schema for the RowConverter.
    let first_batch: RecordBatch = match stream.next().await.transpose()? {
        Some(b) => b,
        None => return Ok(BucketAgg::new(opts.buckets)),
    };
    let ctx = HashContext::build(first_batch.schema_ref(), pk)?;

    let buckets = opts.buckets;
    let prefetch = opts.prefetch_batches.max(1);

    // Pipeline: issue up to `prefetch` in-flight blocking hash tasks, merge
    // completed ones into the running total as we go. One queue => FIFO merge;
    // order of merge doesn't matter because BucketAgg merge is commutative.
    let mut total = BucketAgg::new(buckets);
    let mut in_flight: VecDeque<JoinHandle<Result<BucketAgg>>> = VecDeque::with_capacity(prefetch);

    {
        let ctx = ctx.clone();
        in_flight.push_back(tokio::task::spawn_blocking(move || {
            hash_batch(&first_batch, &ctx, buckets)
        }));
    }

    while let Some(res) = stream.next().await {
        let batch = res?;
        let ctx = ctx.clone();
        in_flight.push_back(tokio::task::spawn_blocking(move || {
            hash_batch(&batch, &ctx, buckets)
        }));
        while in_flight.len() >= prefetch {
            let handle = in_flight.pop_front().expect("non-empty");
            let partial = handle
                .await
                .map_err(|e| Error::Other(format!("join: {e}")))??;
            total.merge(&partial);
        }
    }
    while let Some(handle) = in_flight.pop_front() {
        let partial = handle
            .await
            .map_err(|e| Error::Other(format!("join: {e}")))??;
        total.merge(&partial);
    }
    Ok(total)
}
