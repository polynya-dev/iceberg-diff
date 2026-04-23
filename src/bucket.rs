use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, SortField};
use arrow_schema::Schema;
use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

use crate::error::{Error, Result};

pub const DEFAULT_BUCKETS: usize = 128;

/// Per-bucket aggregate: sum of xxh3-128 row hashes + row count.
///
/// SUM is used rather than XOR because XOR cancels duplicate rows, which would
/// let two genuinely different datasets with paired duplicates compare equal.
/// SUM mod 2^128 is still commutative/associative (order-independent across
/// parallel shards) and duplicate-safe. The paired COUNT(*) catches the rare
/// case where SUMs coincidentally match but cardinalities differ.
#[derive(Debug, Clone)]
pub struct BucketAgg {
    pub buckets: usize,
    pub sum: Vec<u128>,
    pub count: Vec<u64>,
    pub total_rows: u64,
}

impl BucketAgg {
    pub fn new(buckets: usize) -> Self {
        Self {
            buckets,
            sum: vec![0u128; buckets],
            count: vec![0u64; buckets],
            total_rows: 0,
        }
    }

    pub fn merge(&mut self, other: &BucketAgg) {
        debug_assert_eq!(self.buckets, other.buckets);
        for i in 0..self.buckets {
            self.sum[i] = self.sum[i].wrapping_add(other.sum[i]);
            self.count[i] = self.count[i].wrapping_add(other.count[i]);
        }
        self.total_rows = self.total_rows.wrapping_add(other.total_rows);
    }

    /// Returns the list of bucket indices that differ, or `None` if all match.
    pub fn diff(&self, other: &BucketAgg) -> Option<Vec<usize>> {
        debug_assert_eq!(self.buckets, other.buckets);
        let mut mismatched = Vec::new();
        for i in 0..self.buckets {
            if self.sum[i] != other.sum[i] || self.count[i] != other.count[i] {
                mismatched.push(i);
            }
        }
        if mismatched.is_empty() {
            None
        } else {
            Some(mismatched)
        }
    }
}

/// Precomputed encoding context for hashing rows of a stable arrow schema.
///
/// We keep two RowConverters because buckets are assigned from the PK hash
/// (so same-PK rows always land in the same bucket on both sides), while the
/// per-bucket SUM uses the hash of the full row (so any column change on a
/// given PK is detected). A drill-down step can then look at rows in a
/// mismatched bucket and join them by PK to find which cells differ.
#[derive(Clone)]
pub struct HashContext {
    pub full: Arc<RowConverter>,
    pub pk: Arc<RowConverter>,
    pub pk_indices: Arc<Vec<usize>>,
}

impl HashContext {
    pub fn build(schema: &Schema, pk_columns: &[String]) -> Result<Self> {
        let full = build_converter(schema.fields().iter().map(|f| f.data_type().clone()))?;
        let mut pk_indices = Vec::with_capacity(pk_columns.len());
        let mut pk_types = Vec::with_capacity(pk_columns.len());
        for name in pk_columns {
            let (idx, field) = schema.fields().find(name).ok_or_else(|| {
                Error::Other(format!(
                    "primary key column '{name}' not found in arrow schema"
                ))
            })?;
            pk_indices.push(idx);
            pk_types.push(field.data_type().clone());
        }
        let pk = build_converter(pk_types.into_iter())?;
        Ok(Self {
            full: Arc::new(full),
            pk: Arc::new(pk),
            pk_indices: Arc::new(pk_indices),
        })
    }
}

fn build_converter(types: impl IntoIterator<Item = arrow_schema::DataType>) -> Result<RowConverter> {
    let fields: Vec<SortField> = types.into_iter().map(SortField::new).collect();
    RowConverter::new(fields).map_err(|e| Error::Other(format!("row converter: {e}")))
}

/// Hash every row of `batch`, bucket by `hash(PK)`, sum `hash(row)` per bucket.
pub fn hash_batch(batch: &RecordBatch, ctx: &HashContext, buckets: usize) -> Result<BucketAgg> {
    let full_rows = ctx
        .full
        .convert_columns(batch.columns())
        .map_err(|e| Error::Other(format!("row convert (full): {e}")))?;

    let pk_cols: Vec<ArrayRef> = ctx
        .pk_indices
        .iter()
        .map(|&i| batch.column(i).clone())
        .collect();
    let pk_rows = ctx
        .pk
        .convert_columns(&pk_cols)
        .map_err(|e| Error::Other(format!("row convert (pk): {e}")))?;

    let n = batch.num_rows();
    let mut agg = BucketAgg::new(buckets);
    for i in 0..n {
        let row_hash = xxh3_128(full_rows.row(i).as_ref());
        let pk_hash = xxh3_64(pk_rows.row(i).as_ref());
        let b = (pk_hash as usize) % buckets;
        agg.sum[b] = agg.sum[b].wrapping_add(row_hash);
        agg.count[b] += 1;
    }
    agg.total_rows = n as u64;
    Ok(agg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use std::sync::Arc;

    fn batch(ids: &[i64], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())) as _,
                Arc::new(StringArray::from(names.to_vec())) as _,
            ],
        )
        .unwrap()
    }

    fn ctx(schema: &Schema) -> HashContext {
        HashContext::build(schema, &["id".to_string()]).unwrap()
    }

    #[test]
    fn identical_data_matches_regardless_of_order() {
        let a = batch(&[1, 2, 3, 4], &["a", "b", "c", "d"]);
        let b = batch(&[4, 2, 1, 3], &["d", "b", "a", "c"]);
        let ctx_a = ctx(a.schema_ref());
        let ctx_b = ctx(b.schema_ref());

        let agg_a = hash_batch(&a, &ctx_a, 16).unwrap();
        let agg_b = hash_batch(&b, &ctx_b, 16).unwrap();
        assert!(agg_a.diff(&agg_b).is_none(), "shuffled rows should match");
    }

    #[test]
    fn different_values_on_same_pk_are_detected() {
        // Same PKs, different non-PK values → should NOT diff equal. This is
        // exactly what XOR-of-hashes would miss if a row's change happened to
        // toggle bits symmetrically; SUM catches it.
        let a = batch(&[1, 2], &["alice", "bob"]);
        let b = batch(&[1, 2], &["alice", "BOB"]);
        let ctx_a = ctx(a.schema_ref());
        let ctx_b = ctx(b.schema_ref());

        let agg_a = hash_batch(&a, &ctx_a, 16).unwrap();
        let agg_b = hash_batch(&b, &ctx_b, 16).unwrap();
        let diffed = agg_a.diff(&agg_b).expect("must differ");
        assert!(!diffed.is_empty());
    }

    #[test]
    fn duplicate_row_is_not_cancelled() {
        // Critical safety property: two occurrences of the same row must NOT
        // cancel (as they would under BIT_XOR). Compare {r} against {r, r}.
        let one = batch(&[1], &["a"]);
        let two = batch(&[1, 1], &["a", "a"]);
        let ctx_a = ctx(one.schema_ref());

        let agg_one = hash_batch(&one, &ctx_a, 16).unwrap();
        let agg_two = hash_batch(&two, &ctx_a, 16).unwrap();
        assert!(
            agg_one.diff(&agg_two).is_some(),
            "single-row vs duplicated-row must NOT compare equal"
        );
    }

    #[test]
    fn merge_is_order_independent() {
        // Hashing the whole batch in one shot must equal hashing two halves
        // and merging them — this is what lets us parallelize by batch.
        let whole = batch(&[1, 2, 3, 4, 5, 6], &["a", "b", "c", "d", "e", "f"]);
        let left = batch(&[1, 2, 3], &["a", "b", "c"]);
        let right = batch(&[4, 5, 6], &["d", "e", "f"]);
        let c = ctx(whole.schema_ref());

        let w = hash_batch(&whole, &c, 16).unwrap();
        let mut m = hash_batch(&left, &c, 16).unwrap();
        m.merge(&hash_batch(&right, &c, 16).unwrap());
        assert!(w.diff(&m).is_none(), "partition-and-merge must agree with one-shot");
    }
}
