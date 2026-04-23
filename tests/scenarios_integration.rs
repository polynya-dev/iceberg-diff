//! Integration test: iterate `iceberg_diff::scenarios::ALL_SCENARIOS`, for
//! each one load `a` and `b` from the configured R2 Data Catalog, run the
//! diff, and assert the outcome matches the scenario's `expected`.
//!
//! Requires a fixture that has been materialized via
//! `cargo run --release --bin gen_fixture`. If the R2 creds (or `r2.txt`) are
//! not present, the test skips with a clear message — keeps CI happy when
//! secrets aren't mapped.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_diff::catalog::{DynCatalog, LoadedTable};
use iceberg_diff::scenarios::{ExpectedOutcome, ScenarioKind, ALL_SCENARIOS};
use iceberg_diff::{diff_tables, DiffOptions, DiffOutcome, Error};

struct R2Config {
    uri: String,
    warehouse: String,
    token: String,
}

fn load_r2_config() -> Option<R2Config> {
    if let (Ok(uri), Ok(warehouse), Ok(token)) = (
        std::env::var("ICEBERG_DIFF_R2_URI"),
        std::env::var("ICEBERG_DIFF_R2_WAREHOUSE"),
        std::env::var("ICEBERG_DIFF_R2_TOKEN"),
    ) {
        return Some(R2Config {
            uri,
            warehouse,
            token,
        });
    }
    let text = std::fs::read_to_string("r2.txt").ok()?;
    let lines: Vec<&str> = text.lines().collect();
    if lines.len() < 4 {
        return None;
    }
    Some(R2Config {
        uri: lines[0].trim().to_string(),
        warehouse: lines[1].trim().to_string(),
        token: lines[3].trim().to_string(),
    })
}

fn build_catalog(cfg: &R2Config) -> DynCatalog {
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
    Arc::new(RestCatalog::new(rcfg)) as DynCatalog
}

/// Classify the actual (Result<DiffOutcome, Error>) into the same coarse
/// taxonomy that ExpectedOutcome uses, so we can compare them directly.
fn classify(actual: &Result<DiffOutcome, Error>) -> ExpectedOutcome {
    match actual {
        Ok(DiffOutcome::Equal(_)) => ExpectedOutcome::Equal,
        Ok(DiffOutcome::Unequal(_)) => ExpectedOutcome::Unequal,
        Err(Error::SchemaMismatch(_)) => ExpectedOutcome::SchemaMismatch,
        Err(Error::PartitionMismatch(_)) => ExpectedOutcome::PartitionMismatch,
        Err(Error::MissingPrimaryKey(_)) => ExpectedOutcome::MissingPrimaryKey,
        Err(_) => ExpectedOutcome::Equal, // any unclassifiable error won't equal expected
    }
}

#[tokio::test]
#[ignore = "runs against R2 Data Catalog; requires creds. See README for setup."]
async fn all_scenarios_match_expected() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,iceberg=warn".into()),
        )
        .try_init()
        .ok();

    let Some(cfg) = load_r2_config() else {
        eprintln!("SKIP: R2 creds not configured (ICEBERG_DIFF_R2_* or r2.txt)");
        return Ok(());
    };
    let catalog = build_catalog(&cfg);

    // Bench scenarios are slow (minutes) and are gated behind an env var so
    // normal CI runs stay under a minute. Correctness scenarios always run.
    let include_bench = matches!(
        std::env::var("ICEBERG_DIFF_BENCH").as_deref(),
        Ok("1" | "true" | "yes")
    );
    let scenarios: Vec<_> = ALL_SCENARIOS
        .iter()
        .filter(|s| include_bench || !matches!(s.kind, ScenarioKind::BenchTpchLineitem { .. }))
        .collect();

    let mut failed: Vec<(String, ExpectedOutcome, ExpectedOutcome, String)> = Vec::new();
    let overall = Instant::now();

    for s in &scenarios {
        let started = Instant::now();
        let result = run_one(&catalog, s).await;
        let actual = classify(&result);
        let elapsed = started.elapsed().as_millis();
        let status = if actual == s.expected { "OK" } else { "MISMATCH" };
        match &result {
            Ok(o) => println!("{:<24}  {status:<8} {elapsed:>6} ms  actual={actual:?}  outcome={o:?}", s.id),
            Err(e) => println!("{:<24}  {status:<8} {elapsed:>6} ms  actual={actual:?}  err={e}", s.id),
        }
        if actual != s.expected {
            failed.push((
                s.id.to_string(),
                s.expected.clone(),
                actual,
                match result {
                    Ok(o) => format!("{o:?}"),
                    Err(e) => format!("{e}"),
                },
            ));
        }
    }
    println!(
        "\n{}/{} scenarios, total wall time: {:.2}s{}",
        scenarios.len(),
        ALL_SCENARIOS.len(),
        overall.elapsed().as_secs_f64(),
        if include_bench {
            ""
        } else {
            " (bench skipped; set ICEBERG_DIFF_BENCH=1 to include)"
        }
    );
    if !failed.is_empty() {
        eprintln!("\nFAILED scenarios:");
        for (id, expected, actual, detail) in &failed {
            eprintln!("  {id}: expected={expected:?}, actual={actual:?}  ({detail})");
        }
        anyhow::bail!("{} scenario(s) did not match expected outcome", failed.len());
    }
    Ok(())
}

async fn run_one(catalog: &DynCatalog, s: &iceberg_diff::scenarios::Scenario) -> Result<DiffOutcome, Error> {
    let a = LoadedTable::from_catalog_current(
        catalog.clone(),
        vec![s.ns_a.to_string()],
        s.table_a.to_string(),
    )
    .await?;
    let b = LoadedTable::from_catalog_current(
        catalog.clone(),
        vec![s.ns_b.to_string()],
        s.table_b.to_string(),
    )
    .await?;
    diff_tables(a, b, DiffOptions::default()).await
}

// Belt-and-suspenders: something you can `cargo run` as a sanity check that
// creds work even without the fixture being built.
#[allow(dead_code)]
fn _compile_check() -> Result<()> {
    let _ = build_catalog(&R2Config {
        uri: String::new(),
        warehouse: String::new(),
        token: String::new(),
    });
    Context::context(Ok::<_, anyhow::Error>(()), "")
}
