use std::collections::HashMap;
use std::process::ExitCode;

use clap::Parser;
use iceberg_diff::catalog::{Auth, CatalogSpec, LoadedTable, TableSpec};
use iceberg_diff::{diff_tables, DiffOptions, DiffOutcome, Error};

#[derive(Parser, Debug)]
#[command(name = "iceberg-diff", version, about = "Diff two Iceberg snapshots")]
struct Cli {
    // ---- side A ----
    #[arg(long)]
    a_uri: String,
    #[arg(long)]
    a_warehouse: Option<String>,
    /// Dotted name, e.g. `namespace.table` or `ns1.ns2.table`.
    #[arg(long)]
    a_table: String,
    /// Iceberg snapshot id on side A; defaults to the table's current snapshot.
    #[arg(long)]
    a_snapshot: Option<i64>,
    #[arg(long)]
    a_token: Option<String>,
    #[arg(long)]
    a_client_id: Option<String>,
    #[arg(long)]
    a_client_secret: Option<String>,
    #[arg(long)]
    a_scope: Option<String>,

    // ---- side B ----
    #[arg(long)]
    b_uri: String,
    #[arg(long)]
    b_warehouse: Option<String>,
    #[arg(long)]
    b_table: String,
    /// Iceberg snapshot id on side B; defaults to the table's current snapshot.
    #[arg(long)]
    b_snapshot: Option<i64>,
    #[arg(long)]
    b_token: Option<String>,
    #[arg(long)]
    b_client_id: Option<String>,
    #[arg(long)]
    b_client_secret: Option<String>,
    #[arg(long)]
    b_scope: Option<String>,

    // ---- options ----
    #[arg(long, default_value_t = 128)]
    buckets: usize,
    #[arg(long)]
    skip_row_count: bool,
    /// Opt into stage 4 (data-file-set fingerprint). Off by default because
    /// it only short-circuits when both sides reference the same data files;
    /// for typical diffs of independently-written tables it's pure overhead.
    #[arg(long)]
    check_file_fingerprint: bool,

    /// Extra catalog property `key=value`, repeatable. Applied to both sides.
    /// Use this to inject S3 credentials when the catalog does not vend them,
    /// e.g. `--prop s3.endpoint=http://localhost:9000 --prop s3.access-key-id=admin`.
    #[arg(long = "prop", value_parser = parse_kv, action = clap::ArgAction::Append)]
    props: Vec<(String, String)>,
}

fn parse_kv(s: &str) -> Result<(String, String), String> {
    match s.split_once('=') {
        Some((k, v)) if !k.is_empty() => Ok((k.to_string(), v.to_string())),
        _ => Err(format!("expected KEY=VALUE, got '{s}'")),
    }
}

fn parse_dotted(s: &str) -> Result<(Vec<String>, String), String> {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() < 2 {
        return Err(format!(
            "expected 'namespace.table' (at least one dot), got '{s}'"
        ));
    }
    let name = parts.last().unwrap().to_string();
    let ns = parts[..parts.len() - 1]
        .iter()
        .map(|p| (*p).to_string())
        .collect();
    Ok((ns, name))
}

fn auth_from(
    token: Option<String>,
    client_id: Option<String>,
    secret: Option<String>,
    scope: Option<String>,
) -> Auth {
    if let Some(t) = token {
        return Auth::Bearer { token: t };
    }
    match (client_id, secret) {
        (Some(id), Some(s)) => Auth::OAuth2ClientCredentials {
            client_id: id,
            client_secret: s,
            scope,
        },
        _ => Auth::None,
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();

    let (a_ns, a_name) = match parse_dotted(&cli.a_table) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("--a-table: {e}");
            return ExitCode::from(3);
        }
    };
    let (b_ns, b_name) = match parse_dotted(&cli.b_table) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("--b-table: {e}");
            return ExitCode::from(3);
        }
    };

    let shared_props: HashMap<String, String> = cli.props.into_iter().collect();

    let a_catalog = CatalogSpec {
        uri: cli.a_uri,
        warehouse: cli.a_warehouse,
        auth: auth_from(cli.a_token, cli.a_client_id, cli.a_client_secret, cli.a_scope),
        extra_props: shared_props.clone(),
    };
    let b_catalog = CatalogSpec {
        uri: cli.b_uri,
        warehouse: cli.b_warehouse,
        auth: auth_from(cli.b_token, cli.b_client_id, cli.b_client_secret, cli.b_scope),
        extra_props: shared_props,
    };

    let opts = DiffOptions {
        buckets: cli.buckets,
        skip_row_count: cli.skip_row_count,
        check_file_fingerprint: cli.check_file_fingerprint,
        ..Default::default()
    };

    let load_a = async {
        match cli.a_snapshot {
            Some(id) => {
                LoadedTable::load(TableSpec {
                    catalog: a_catalog,
                    namespace: a_ns,
                    table: a_name,
                    snapshot_id: id,
                })
                .await
            }
            None => LoadedTable::load_current(a_catalog, a_ns, a_name).await,
        }
    };
    let load_b = async {
        match cli.b_snapshot {
            Some(id) => {
                LoadedTable::load(TableSpec {
                    catalog: b_catalog,
                    namespace: b_ns,
                    table: b_name,
                    snapshot_id: id,
                })
                .await
            }
            None => LoadedTable::load_current(b_catalog, b_ns, b_name).await,
        }
    };

    let result = async {
        let (a, b) = tokio::try_join!(load_a, load_b)?;
        diff_tables(a, b, opts).await
    }
    .await;

    match result {
        Ok(DiffOutcome::Equal(reason)) => {
            println!(
                "{}",
                serde_json::json!({
                    "result": "equal",
                    "reason": format!("{reason:?}"),
                })
            );
            ExitCode::from(0)
        }
        Ok(DiffOutcome::Unequal(reason)) => {
            println!(
                "{}",
                serde_json::json!({
                    "result": "unequal",
                    "reason": format!("{reason:?}"),
                })
            );
            ExitCode::from(1)
        }
        Err(e @ Error::SchemaMismatch(_)) | Err(e @ Error::PartitionMismatch(_)) => {
            eprintln!("{e}");
            ExitCode::from(2)
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(3)
        }
    }
}
