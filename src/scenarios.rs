//! Scenario catalog for the integration test / fixture generator.
//!
//! A *scenario* is a labeled pair of iceberg tables whose expected diff
//! outcome is known ahead of time. The generator binary builds each scenario's
//! `a` and `b` tables on the R2 Data Catalog; the integration test asserts
//! that `iceberg_diff::diff_tables(a, b)` produces exactly the expected
//! outcome.
//!
//! Two families:
//!
//! - **Correctness** — a small synthetic table (~1000 rows, types chosen to
//!   stress the hasher) with a deliberate mutation or schema twist. Each
//!   covers one branch of the diff tool's decision tree. Cheap to build,
//!   cheap to run.
//! - **Benchmark** — full TPC-H `lineitem` at a given scale factor, EQUAL on
//!   both sides. Only `bench_sf1` runs by default; `bench_sf10` is gated by
//!   an env flag since it writes ~8 GB and the diff alone takes ~30 s.

use serde::{Deserialize, Serialize};

/// Coarse expected outcome. Maps 1:1 to the branches the diff tool can
/// produce, but without the nondeterministic payload (row count, bucket
/// indices) so scenarios compare cleanly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExpectedOutcome {
    Equal,
    Unequal,
    SchemaMismatch,
    PartitionMismatch,
    MissingPrimaryKey,
}

#[derive(Debug, Clone)]
pub struct Scenario {
    pub id: &'static str,
    pub ns_a: &'static str,
    pub table_a: &'static str,
    pub ns_b: &'static str,
    pub table_b: &'static str,
    pub expected: ExpectedOutcome,
    pub kind: ScenarioKind,
}

/// Discrete variants (not arbitrary closures) so both generator and test can
/// reason about scenarios as data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScenarioKind {
    /// Baseline — identical synthetic writes on both sides.
    EqIdentical,
    /// Same rows on both sides but B inserted in reverse order. Tests that
    /// hash-bucket aggregation is order-independent.
    EqShuffled,
    /// Side A is format-version 1, side B is format-version 2. Same logical
    /// rows. Verifies the diff is format-version-agnostic — useful for
    /// migration validation (v1 → v2) and future v2 → v3 work.
    EqCrossVersion,
    /// B is missing one row (id=500). Expect UNEQUAL (stage 3 if summary has
    /// total-records; else stage 5 with per-bucket count mismatch).
    NeqMissingRow,
    /// B has an extra row. Expect UNEQUAL.
    NeqExtraRow,
    /// Same rows + same row count, but one cell's value differs in B. This is
    /// the test the other unequal-family scenarios can't substitute for:
    /// stages 3 and 4 can't catch it, only stage 5's row-hash sum mismatch.
    NeqCellModified,
    /// 1% of rows have a cell mutated. Same logic as NeqCellModified at
    /// higher divergence.
    NeqManyCells,
    /// Column `balance` has type Decimal on A, Double on B. Expect
    /// SchemaMismatch at stage 2.
    ErrSchemaTypes,
    /// Side A has no `identifier-field-ids`. Expect MissingPrimaryKey.
    ErrNoPkA,
    /// Side A's PK is [id], side B's PK is [id, email]. Expect
    /// SchemaMismatch (PK column set differs).
    ErrPkDiffers,
    /// TPC-H `lineitem` at the given scale factor on both sides; identical
    /// writes. Benchmark; not a branch-coverage scenario.
    BenchTpchLineitem {
        scale_factor: u32,
    },
}

pub const ALL_SCENARIOS: &[Scenario] = &[
    Scenario {
        id: "eq_identical",
        ns_a: "eq_identical",
        table_a: "a",
        ns_b: "eq_identical",
        table_b: "b",
        expected: ExpectedOutcome::Equal,
        kind: ScenarioKind::EqIdentical,
    },
    Scenario {
        id: "eq_shuffled",
        ns_a: "eq_shuffled",
        table_a: "a",
        ns_b: "eq_shuffled",
        table_b: "b",
        expected: ExpectedOutcome::Equal,
        kind: ScenarioKind::EqShuffled,
    },
    Scenario {
        id: "eq_cross_version",
        ns_a: "eq_cross_version",
        table_a: "a",
        ns_b: "eq_cross_version",
        table_b: "b",
        expected: ExpectedOutcome::Equal,
        kind: ScenarioKind::EqCrossVersion,
    },
    Scenario {
        id: "neq_missing_row",
        ns_a: "neq_missing_row",
        table_a: "a",
        ns_b: "neq_missing_row",
        table_b: "b",
        expected: ExpectedOutcome::Unequal,
        kind: ScenarioKind::NeqMissingRow,
    },
    Scenario {
        id: "neq_extra_row",
        ns_a: "neq_extra_row",
        table_a: "a",
        ns_b: "neq_extra_row",
        table_b: "b",
        expected: ExpectedOutcome::Unequal,
        kind: ScenarioKind::NeqExtraRow,
    },
    Scenario {
        id: "neq_cell_modified",
        ns_a: "neq_cell_modified",
        table_a: "a",
        ns_b: "neq_cell_modified",
        table_b: "b",
        expected: ExpectedOutcome::Unequal,
        kind: ScenarioKind::NeqCellModified,
    },
    Scenario {
        id: "neq_many_cells",
        ns_a: "neq_many_cells",
        table_a: "a",
        ns_b: "neq_many_cells",
        table_b: "b",
        expected: ExpectedOutcome::Unequal,
        kind: ScenarioKind::NeqManyCells,
    },
    Scenario {
        id: "err_schema_types",
        ns_a: "err_schema_types",
        table_a: "a",
        ns_b: "err_schema_types",
        table_b: "b",
        expected: ExpectedOutcome::SchemaMismatch,
        kind: ScenarioKind::ErrSchemaTypes,
    },
    Scenario {
        id: "err_no_pk_a",
        ns_a: "err_no_pk_a",
        table_a: "a",
        ns_b: "err_no_pk_a",
        table_b: "b",
        expected: ExpectedOutcome::MissingPrimaryKey,
        kind: ScenarioKind::ErrNoPkA,
    },
    Scenario {
        id: "err_pk_differs",
        ns_a: "err_pk_differs",
        table_a: "a",
        ns_b: "err_pk_differs",
        table_b: "b",
        expected: ExpectedOutcome::SchemaMismatch,
        kind: ScenarioKind::ErrPkDiffers,
    },
    Scenario {
        id: "bench_sf1_lineitem",
        ns_a: "bench_sf1_lineitem",
        table_a: "a",
        ns_b: "bench_sf1_lineitem",
        table_b: "b",
        expected: ExpectedOutcome::Equal,
        kind: ScenarioKind::BenchTpchLineitem { scale_factor: 1 },
    },
];
