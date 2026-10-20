//! Where queries get the logs table from.
//!
//! [`IcebergSource`] keeps the loaded Iceberg table and reloads it from the
//! catalog at most every `TABLE_REFRESH_MS`, so most queries make no catalog
//! call. A `Table` also caches the manifests it has read, so keeping one
//! instance between refreshes avoids re-reading them on every query.
//! Pagination pins the snapshot of the first page.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use logscan::LogsTableProvider;
use planner::SearchPlanner;
use tokio::sync::{Mutex, RwLock};

use crate::config::Config;

/// A table ready to query: the provider, the snapshot it reads and the
/// planner built for its schema
#[derive(Clone)]
pub struct TableHandle {
    pub provider: Arc<dyn TableProvider>,
    pub snapshot_id: Option<i64>,
    pub planner: Arc<SearchPlanner>,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum SourceError {
    #[error("logs table unavailable: {0}")]
    Unavailable(String),
    #[error("logs table schema: {0}")]
    Schema(String),
}

#[async_trait]
pub trait TableSource: Send + Sync {
    /// The current table, or the given snapshot of it
    async fn table(&self, snapshot_id: Option<i64>) -> Result<TableHandle, SourceError>;
    fn is_ready(&self) -> bool;
    /// Seconds since the table metadata was last loaded
    fn age_seconds(&self) -> Option<f64>;
    fn refresh_failures(&self) -> u64;
}

struct Loaded {
    table: Table,
    current: TableHandle,
    loaded_at: Instant,
}

pub struct IcebergSource {
    catalog: RestCatalog,
    ident: TableIdent,
    refresh: Duration,
    state: RwLock<Option<Arc<Loaded>>>,
    reload: Mutex<()>,
    ready: AtomicBool,
    failures: AtomicU64,
}

impl IcebergSource {
    pub async fn new(config: &Config) -> Result<Self, SourceError> {
        let mut props = config.storage_props();
        props.insert("uri".to_string(), config.catalog_uri.clone());
        if let Some(warehouse) = &config.warehouse {
            props.insert("warehouse".to_string(), warehouse.clone());
        }
        // Our S3 settings are applied after the catalog's per-table config,
        // so (as in the appender) the configured keys and endpoint win
        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
            .load("lake", props)
            .await
            .map_err(|e| SourceError::Unavailable(e.to_string()))?;
        Ok(Self {
            catalog,
            ident: TableIdent::new(NamespaceIdent::new(config.namespace.clone()), config.table.clone()),
            refresh: config.table_refresh,
            state: RwLock::new(None),
            reload: Mutex::new(()),
            ready: AtomicBool::new(false),
            failures: AtomicU64::new(0),
        })
    }

    pub fn ident(&self) -> &TableIdent {
        &self.ident
    }

    /// Loads the table now (used at startup)
    pub async fn load(&self) -> Result<(), SourceError> {
        let loaded = self.fetch().await?;
        *self.state.write().await = Some(Arc::new(loaded));
        self.ready.store(true, Ordering::Release);
        Ok(())
    }

    async fn fetch(&self) -> Result<Loaded, SourceError> {
        let table = self.catalog.load_table(&self.ident).await.map_err(|e| {
            self.failures.fetch_add(1, Ordering::Relaxed);
            SourceError::Unavailable(e.to_string())
        })?;
        let provider =
            LogsTableProvider::try_new(table.clone(), None).map_err(|e| SourceError::Schema(e.to_string()))?;
        let planner = SearchPlanner::new(&provider.schema()).map_err(|e| SourceError::Schema(e.to_string()))?;
        let snapshot_id = table.metadata().current_snapshot_id();
        Ok(Loaded {
            current: TableHandle { provider: Arc::new(provider), snapshot_id, planner: Arc::new(planner) },
            table,
            loaded_at: Instant::now(),
        })
    }

    /// The loaded table, reloading it first when it is older than the refresh
    /// interval. If the catalog is down, a stale table keeps serving.
    async fn fresh(&self) -> Result<Arc<Loaded>, SourceError> {
        if let Some(loaded) = self.state.read().await.clone()
            && loaded.loaded_at.elapsed() < self.refresh
        {
            return Ok(loaded);
        }
        let _single_flight = self.reload.lock().await;
        let stale = self.state.read().await.clone();
        if let Some(loaded) = &stale
            && loaded.loaded_at.elapsed() < self.refresh
        {
            return Ok(loaded.clone()); // another query reloaded it meanwhile
        }
        match self.fetch().await {
            Ok(loaded) => {
                let loaded = Arc::new(loaded);
                *self.state.write().await = Some(loaded.clone());
                self.ready.store(true, Ordering::Release);
                Ok(loaded)
            }
            Err(error) => match stale {
                Some(stale) => {
                    tracing::warn!(%error, "table refresh failed; serving the previous metadata");
                    Ok(stale)
                }
                None => Err(error),
            },
        }
    }
}

#[async_trait]
impl TableSource for IcebergSource {
    async fn table(&self, snapshot_id: Option<i64>) -> Result<TableHandle, SourceError> {
        let loaded = self.fresh().await?;
        let Some(id) = snapshot_id else { return Ok(loaded.current.clone()) };
        if loaded.current.snapshot_id == Some(id) {
            return Ok(loaded.current.clone());
        }
        if loaded.table.metadata().snapshot_by_id(id).is_none() {
            // Nessie keeps only the current snapshot in table metadata, and
            // other catalogs expire old ones. The logs table is append-only
            // and compaction keeps row keys, so continuing on the current
            // snapshot still neither repeats nor skips a row the cursor's
            // first page could see.
            tracing::debug!(snapshot = id, "cursor snapshot no longer in table metadata; using the current one");
            return Ok(loaded.current.clone());
        }
        let provider = LogsTableProvider::try_new(loaded.table.clone(), Some(id))
            .map_err(|e| SourceError::Schema(e.to_string()))?;
        // An older snapshot can have an older schema
        let planner = SearchPlanner::new(&provider.schema()).map_err(|e| SourceError::Schema(e.to_string()))?;
        Ok(TableHandle { provider: Arc::new(provider), snapshot_id: Some(id), planner: Arc::new(planner) })
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    fn age_seconds(&self) -> Option<f64> {
        self.state.try_read().ok()?.as_ref().map(|l| l.loaded_at.elapsed().as_secs_f64())
    }

    fn refresh_failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }
}

/// A fixed in-memory table (tests, local experiments)
pub struct StaticSource {
    pub handle: TableHandle,
}

#[async_trait]
impl TableSource for StaticSource {
    async fn table(&self, snapshot_id: Option<i64>) -> Result<TableHandle, SourceError> {
        // One snapshot only: any other falls back to it, as in IcebergSource
        let _ = snapshot_id;
        Ok(self.handle.clone())
    }

    fn is_ready(&self) -> bool {
        true
    }

    fn age_seconds(&self) -> Option<f64> {
        Some(0.0)
    }

    fn refresh_failures(&self) -> u64 {
        0
    }
}
