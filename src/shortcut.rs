use std::collections::BTreeSet;

use futures::TryStreamExt;

use crate::catalog::LoadedTable;
use crate::error::Result;

/// Stage 1 — trivial identity: same catalog URI+warehouse, namespace, table, snapshot.
pub fn identity_match(a: &LoadedTable, b: &LoadedTable) -> bool {
    a.catalog_key() == b.catalog_key()
        && a.spec.namespace == b.spec.namespace
        && a.spec.table == b.spec.table
        && a.spec.snapshot_id == b.spec.snapshot_id
}

/// Stage 3 — compare `summary.total-records` from the snapshot summaries.
/// Returns `Some(true)` if both are present and equal, `Some(false)` if both
/// are present and differ, `None` if either side lacks the field.
pub fn row_count_match(a: &LoadedTable, b: &LoadedTable) -> Option<bool> {
    let ra = a.snapshot.summary().additional_properties.get("total-records")?;
    let rb = b.snapshot.summary().additional_properties.get("total-records")?;
    Some(ra == rb)
}

pub fn row_count_values(t: &LoadedTable) -> Option<String> {
    t.snapshot
        .summary()
        .additional_properties
        .get("total-records")
        .cloned()
}

pub type FileFingerprint = BTreeSet<(String, u64)>;

/// Stage 4 — collect a deterministic fingerprint of the data files referenced
/// by the snapshot. If both sides produce the same fingerprint, no data
/// comparison is needed: they are reading the exact same bytes.
pub async fn collect_file_fingerprint(t: &LoadedTable) -> Result<FileFingerprint> {
    let scan = t.table.scan().snapshot_id(t.spec.snapshot_id).build()?;
    let mut plan = scan.plan_files().await?;
    let mut out = FileFingerprint::new();
    while let Some(task) = plan.try_next().await? {
        out.insert((task.data_file_path.clone(), task.length));
    }
    Ok(out)
}
