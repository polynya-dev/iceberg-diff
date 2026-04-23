use std::collections::HashMap;
use std::sync::Arc;

use iceberg::spec::SnapshotRef;
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

/// Source of an already-loaded iceberg-rust Catalog. Kept as a trait object so
/// tests (and future Glue/SQL integrations) can construct a catalog however
/// they like and pass it straight to `LoadedTable::from_catalog`.
pub type DynCatalog = Arc<dyn Catalog>;

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct CatalogSpec {
    pub uri: String,
    pub warehouse: Option<String>,
    pub auth: Auth,
    /// Extra REST catalog properties. Keys starting with `header.` become HTTP
    /// headers on catalog requests.
    pub extra_props: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum Auth {
    None,
    Bearer {
        token: String,
    },
    OAuth2ClientCredentials {
        client_id: String,
        client_secret: String,
        scope: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct TableSpec {
    pub catalog: CatalogSpec,
    pub namespace: Vec<String>,
    pub table: String,
    pub snapshot_id: i64,
}

/// A REST-loaded, snapshot-pinned view of an Iceberg table.
///
/// The underlying `RestCatalogConfig` carries
/// `X-Iceberg-Access-Delegation: vended-credentials`, so `LoadTable` returns
/// scoped S3 credentials in the table config map. iceberg-rust's `FileIO`
/// picks up `s3.access-key-id / secret-access-key / session-token / region /
/// endpoint` from table properties automatically when data files are read.
pub struct LoadedTable {
    pub spec: TableSpec,
    pub catalog: DynCatalog,
    pub table: Table,
    pub snapshot: SnapshotRef,
}

impl LoadedTable {
    /// Build a `LoadedTable` from an already-constructed catalog + table ident
    /// + snapshot id. Lets callers bring their own catalog (MemoryCatalog for
    /// tests, Glue, SQL, etc.) instead of only REST.
    pub async fn from_catalog(
        catalog: DynCatalog,
        namespace: Vec<String>,
        table: String,
        snapshot_id: i64,
    ) -> Result<Self> {
        let ns = NamespaceIdent::from_strs(namespace.iter().cloned())
            .map_err(|e| Error::Other(format!("namespace: {e}")))?;
        let ident = TableIdent::new(ns, table.clone());
        let tbl = catalog.load_table(&ident).await?;
        let snapshot = tbl
            .metadata()
            .snapshot_by_id(snapshot_id)
            .cloned()
            .ok_or_else(|| {
                Error::Other(format!(
                    "snapshot {snapshot_id} not found in table {ident}"
                ))
            })?;
        // Synthesize a minimal TableSpec so identity/diagnostic paths still
        // have something to render. The `catalog` field becomes the URI label
        // (for trait-object catalogs we use their Debug impl).
        let spec = TableSpec {
            catalog: CatalogSpec {
                uri: format!("dyn:{}", std::any::type_name_of_val(&*catalog)),
                warehouse: None,
                auth: Auth::None,
                extra_props: HashMap::new(),
            },
            namespace,
            table,
            snapshot_id,
        };
        Ok(Self {
            spec,
            catalog,
            table: tbl,
            snapshot,
        })
    }

    /// Same as `from_catalog`, but resolves the table's current snapshot id
    /// automatically.
    pub async fn from_catalog_current(
        catalog: DynCatalog,
        namespace: Vec<String>,
        table: String,
    ) -> Result<Self> {
        let ns = NamespaceIdent::from_strs(namespace.iter().cloned())
            .map_err(|e| Error::Other(format!("namespace: {e}")))?;
        let ident = TableIdent::new(ns, table.clone());
        let tbl = catalog.load_table(&ident).await?;
        let snapshot_id = tbl
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .ok_or_else(|| {
                Error::Other(format!("table {ident} has no current snapshot"))
            })?;
        Self::from_catalog(catalog, namespace, table, snapshot_id).await
    }
}

async fn resolve_current_snapshot(
    catalog: &CatalogSpec,
    namespace: &[String],
    table: &str,
) -> Result<i64> {
    let rest = LoadedTable::build_rest_catalog(catalog);
    let ns = NamespaceIdent::from_strs(namespace.iter().cloned())
        .map_err(|e| Error::Other(format!("namespace: {e}")))?;
    let ident = TableIdent::new(ns, table.to_string());
    let tbl = rest.load_table(&ident).await?;
    tbl.metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id())
        .ok_or_else(|| Error::Other(format!("table {ident} has no current snapshot")))
}

impl LoadedTable {
    pub async fn load(spec: TableSpec) -> Result<Self> {
        let catalog: Arc<RestCatalog> = Self::build_rest_catalog(&spec.catalog);

        let ns = NamespaceIdent::from_strs(spec.namespace.iter().cloned())
            .map_err(|e| Error::Other(format!("namespace: {e}")))?;
        let ident = TableIdent::new(ns, spec.table.clone());
        let table = catalog.load_table(&ident).await?;

        let snapshot = table
            .metadata()
            .snapshot_by_id(spec.snapshot_id)
            .cloned()
            .ok_or_else(|| {
                Error::Other(format!(
                    "snapshot {} not found in table {}",
                    spec.snapshot_id, ident
                ))
            })?;

        Ok(Self {
            spec,
            catalog,
            table,
            snapshot,
        })
    }
}

impl LoadedTable {
    /// Convenience: load a table at whatever its current snapshot is. Useful
    /// when you want to diff "the latest" of two tables without having to look
    /// up snapshot ids externally first.
    pub async fn load_current(
        catalog: CatalogSpec,
        namespace: Vec<String>,
        table: String,
    ) -> Result<Self> {
        let snapshot_id = resolve_current_snapshot(&catalog, &namespace, &table).await?;
        Self::load(TableSpec {
            catalog,
            namespace,
            table,
            snapshot_id,
        })
        .await
    }

    /// Build a RestCatalog from a CatalogSpec. Factored out so `load` and
    /// `load_current` share identical auth + vended-creds + props wiring.
    fn build_rest_catalog(spec: &CatalogSpec) -> Arc<RestCatalog> {
        let mut props = spec.extra_props.clone();
        props
            .entry("header.X-Iceberg-Access-Delegation".to_string())
            .or_insert_with(|| "vended-credentials".to_string());
        match &spec.auth {
            Auth::None => {}
            Auth::Bearer { token } => {
                props.insert("token".into(), token.clone());
            }
            Auth::OAuth2ClientCredentials {
                client_id,
                client_secret,
                scope,
            } => {
                props.insert(
                    "credential".into(),
                    format!("{}:{}", client_id, client_secret),
                );
                if let Some(s) = scope {
                    props.insert("scope".into(), s.clone());
                }
            }
        }
        let cfg = RestCatalogConfig::builder()
            .uri(spec.uri.clone())
            .warehouse_opt(spec.warehouse.clone())
            .props(props)
            .build();
        Arc::new(RestCatalog::new(cfg))
    }

    /// Coarse identity key used to short-circuit when both sides reference the
    /// same catalog+warehouse+table+snapshot.
    pub fn catalog_key(&self) -> String {
        format!(
            "{}|{}",
            self.spec.catalog.uri,
            self.spec.catalog.warehouse.as_deref().unwrap_or("")
        )
    }
}
