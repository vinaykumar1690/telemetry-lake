use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::scan::FileScanTask;
use iceberg::table::Table;
use iceberg_datafusion::read::convert_filters_to_predicate;
use iceberg_datafusion::to_datafusion_error;

use crate::exec::{LogsScanExec, bin_pack};

/// Drops planned files that cannot match a query (e.g. from a file-skipping
/// index). Must be conservative: returning `false` for a file that has a
/// matching row loses data.
pub trait FileFilter: Send + Sync + std::fmt::Debug {
    fn keep(&self, task: &FileScanTask, filters: &[Expr]) -> bool;
}

/// The logs table at one snapshot (the current one when `snapshot_id` is None)
#[derive(Debug, Clone)]
pub struct LogsTableProvider {
    table: Table,
    snapshot_id: Option<i64>,
    schema: SchemaRef,
    file_filter: Option<Arc<dyn FileFilter>>,
}

impl LogsTableProvider {
    pub fn try_new(table: Table, snapshot_id: Option<i64>) -> Result<Self> {
        let metadata = table.metadata();
        let iceberg_schema = match snapshot_id {
            Some(id) => {
                let snapshot = metadata.snapshot_by_id(id).ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!("snapshot {id} is not in the table metadata"))
                })?;
                snapshot.schema(metadata).map_err(to_datafusion_error)?
            }
            None => metadata.current_schema().clone(),
        };
        let schema = Arc::new(schema_to_arrow_schema(&iceberg_schema).map_err(to_datafusion_error)?);
        Ok(Self { table, snapshot_id, schema, file_filter: None })
    }

    pub fn with_file_filter(mut self, filter: Arc<dyn FileFilter>) -> Self {
        self.file_filter = Some(filter);
        self
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id.or_else(|| self.table.metadata().current_snapshot_id())
    }

    /// Plans the files to read (partition and manifest pruning by iceberg-rust)
    pub async fn plan(&self, column_names: Vec<String>, filters: &[Expr]) -> Result<Vec<FileScanTask>> {
        if self.table.metadata().current_snapshot_id().is_none() {
            return Ok(Vec::new()); // empty table
        }
        let mut builder = match self.snapshot_id {
            Some(id) => self.table.scan().snapshot_id(id),
            None => self.table.scan(),
        }
        .select(column_names);
        if let Some(predicate) = convert_filters_to_predicate(filters) {
            builder = builder.with_filter(predicate);
        }
        let scan = builder.build().map_err(to_datafusion_error)?;
        let tasks: Vec<FileScanTask> =
            scan.plan_files().await.map_err(to_datafusion_error)?.try_collect().await.map_err(to_datafusion_error)?;
        Ok(match &self.file_filter {
            Some(filter) => tasks.into_iter().filter(|t| filter.keep(t, filters)).collect(),
            None => tasks,
        })
    }
}

#[async_trait]
impl TableProvider for LogsTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => Arc::clone(&self.schema),
        };
        let names = schema.fields().iter().map(|f| f.name().clone()).collect();
        let tasks = self.plan(names, filters).await?;
        let partitions = state.config().target_partitions().min(tasks.len()).max(1);
        let predicate = convert_filters_to_predicate(filters).map(|p| p.to_string());
        Ok(Arc::new(LogsScanExec::new(
            self.table.file_io().clone(),
            bin_pack(tasks, partitions),
            schema,
            limit,
            predicate,
        )))
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        // Files and row groups are pruned with the filters, but rows are not
        // filtered exactly; DataFusion re-applies them
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}
