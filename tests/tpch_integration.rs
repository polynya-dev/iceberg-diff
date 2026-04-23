//! TPC-H integration test.
//!
//! This test is `#[ignore]`d by default; it is driven by
//! `tests/tpch/run.sh`, which handles stack orchestration, data generation,
//! and invoking pg2iceberg. This file assumes:
//!
//!   * an Iceberg REST catalog is reachable at `$TPCH_REST_URL`
//!   * two namespaces (`$TPCH_NAMESPACE_A`, `$TPCH_NAMESPACE_B`) each contain
//!     the eight TPC-H tables, snapshotted from the same postgres source
//!
//! It loads the current snapshot of each table from both sides and asserts the
//! diff result is `Equal`. It doubles as a benchmark because it prints per-
//! table wall time for the diff itself, separate from the pg2iceberg time
//! printed by `run.sh`.

use std::collections::HashMap;
use std::env;
use std::time::Instant;

use iceberg_diff::catalog::{Auth, CatalogSpec, LoadedTable};
use iceberg_diff::{diff_tables, DiffOptions, DiffOutcome};

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "part", "supplier", "partsupp", "customer", "orders", "lineitem",
];

fn catalog_spec() -> CatalogSpec {
    // The iceberg-rest-fixture used in this test stack does NOT return vended
    // S3 credentials even when asked. We inject static creds via `extra_props`
    // so iceberg-rust's FileIO can read from MinIO. In a real deployment
    // against a vending catalog, these env vars would be unset and the
    // `X-Iceberg-Access-Delegation` header would do the work.
    let mut props = HashMap::new();
    for (env_key, prop_key) in [
        ("TPCH_S3_ENDPOINT", "s3.endpoint"),
        ("TPCH_S3_ACCESS_KEY", "s3.access-key-id"),
        ("TPCH_S3_SECRET_KEY", "s3.secret-access-key"),
        ("TPCH_S3_REGION", "s3.region"),
        ("TPCH_S3_PATH_STYLE", "s3.path-style-access"),
    ] {
        if let Ok(v) = env::var(env_key) {
            props.insert(prop_key.to_string(), v);
        }
    }
    CatalogSpec {
        uri: env::var("TPCH_REST_URL").expect("TPCH_REST_URL not set"),
        warehouse: env::var("TPCH_WAREHOUSE").ok(),
        auth: Auth::None,
        extra_props: props,
    }
}

async fn diff_one(
    table: &str,
    ns_a: &str,
    ns_b: &str,
    opts: DiffOptions,
) -> anyhow::Result<DiffOutcome> {
    let cat = catalog_spec();
    let (a, b) = tokio::try_join!(
        LoadedTable::load_current(cat.clone(), vec![ns_a.to_string()], table.to_string()),
        LoadedTable::load_current(cat.clone(), vec![ns_b.to_string()], table.to_string()),
    )?;
    Ok(diff_tables(a, b, opts).await?)
}

#[tokio::test]
#[ignore = "requires TPC-H stack; run via tests/tpch/run.sh"]
async fn tpch_snapshot_a_equals_b() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,iceberg=warn".into()),
        )
        .try_init()
        .ok();

    let ns_a = env::var("TPCH_NAMESPACE_A").expect("TPCH_NAMESPACE_A not set");
    let ns_b = env::var("TPCH_NAMESPACE_B").expect("TPCH_NAMESPACE_B not set");

    let opts = DiffOptions::default();

    let mut failed = Vec::new();
    let overall = Instant::now();

    for table in TPCH_TABLES {
        let started = Instant::now();
        match diff_one(table, &ns_a, &ns_b, opts.clone()).await {
            Ok(DiffOutcome::Equal(reason)) => {
                println!(
                    "{:<10}  EQUAL    {:>8} ms   ({reason:?})",
                    table,
                    started.elapsed().as_millis()
                );
            }
            Ok(DiffOutcome::Unequal(reason)) => {
                println!(
                    "{:<10}  UNEQUAL  {:>8} ms   ({reason:?})",
                    table,
                    started.elapsed().as_millis()
                );
                failed.push((*table, format!("unequal: {reason:?}")));
            }
            Err(e) => {
                println!(
                    "{:<10}  ERROR    {:>8} ms   ({e})",
                    table,
                    started.elapsed().as_millis()
                );
                failed.push((*table, format!("error: {e}")));
            }
        }
    }
    println!(
        "\nTPC-H diff total wall time: {:.1}s",
        overall.elapsed().as_secs_f64()
    );

    assert!(
        failed.is_empty(),
        "one or more TPC-H tables did not diff EQUAL: {failed:?}"
    );
}

/// Negative test: a deliberately broken copy must diff UNEQUAL. We mutate
/// `customer` in namespace_b by dropping one row before running this test in
/// dev (the mutation step is not automated here; enable via env
/// `TPCH_NEGATIVE_CHECK=1` after you've externally mutated the table).
#[tokio::test]
#[ignore = "only run after manually mutating one table in namespace B"]
async fn tpch_mutated_customer_is_unequal() {
    if env::var("TPCH_NEGATIVE_CHECK").ok().as_deref() != Some("1") {
        eprintln!("set TPCH_NEGATIVE_CHECK=1 to opt in");
        return;
    }
    let ns_a = env::var("TPCH_NAMESPACE_A").expect("TPCH_NAMESPACE_A not set");
    let ns_b = env::var("TPCH_NAMESPACE_B").expect("TPCH_NAMESPACE_B not set");
    let outcome = diff_one("customer", &ns_a, &ns_b, DiffOptions::default())
        .await
        .expect("diff failed");
    match outcome {
        DiffOutcome::Unequal(_) => {}
        DiffOutcome::Equal(reason) => panic!("expected UNEQUAL but got EQUAL ({reason:?})"),
    }
}
