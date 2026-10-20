use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Statistics, internal_err};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties};
use futures::{StreamExt, TryStreamExt, stream};
use iceberg::Runtime;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use iceberg_datafusion::to_datafusion_error;

/// Reads groups of Iceberg file scan tasks, one group per output partition.
#[derive(Debug, Clone)]
pub struct LogsScanExec {
    file_io: FileIO,
    groups: Vec<Vec<FileScanTask>>,
    schema: SchemaRef,
    limit: Option<usize>,
    /// The Iceberg predicate, for EXPLAIN only (each task carries its bound form)
    predicate: Option<String>,
    properties: Arc<PlanProperties>,
}

impl LogsScanExec {
    /// `groups` may be empty or contain empty groups; each group is one partition
    pub fn new(
        file_io: FileIO,
        mut groups: Vec<Vec<FileScanTask>>,
        schema: SchemaRef,
        limit: Option<usize>,
        predicate: Option<String>,
    ) -> Self {
        if groups.is_empty() {
            groups.push(Vec::new()); // a plan needs at least one partition
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(groups.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self { file_io, groups, schema, limit, predicate, properties }
    }

    pub fn groups(&self) -> &[Vec<FileScanTask>] {
        &self.groups
    }

    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn predicate(&self) -> Option<&str> {
        self.predicate.as_deref()
    }

    pub fn tasks(&self) -> impl Iterator<Item = &FileScanTask> {
        self.groups.iter().flatten()
    }

    /// Bytes this scan reads (planned task lengths)
    pub fn total_bytes(&self) -> u64 {
        self.tasks().map(FileScanTask::length).sum()
    }

    /// Splits the scan into `count` scans over disjoint tasks, each with the
    /// same schema and partition count as this one (padded with empty
    /// partitions), as distributed task variants must be
    pub fn split(&self, count: usize) -> Vec<LogsScanExec> {
        let count = count.max(1);
        let partitions = self.groups.len();
        let tasks: Vec<FileScanTask> = self.tasks().cloned().collect();
        let mut packed = bin_pack(tasks, partitions * count).into_iter();
        (0..count)
            .map(|_| {
                let groups: Vec<_> = packed.by_ref().take(partitions).collect();
                LogsScanExec::new(
                    self.file_io.clone(),
                    groups,
                    Arc::clone(&self.schema),
                    self.limit,
                    self.predicate.clone(),
                )
            })
            .collect()
    }
}

/// Packs tasks into exactly `count` groups of similar total size: largest
/// task first, each into the currently smallest group
pub fn bin_pack(mut tasks: Vec<FileScanTask>, count: usize) -> Vec<Vec<FileScanTask>> {
    let count = count.max(1);
    tasks.sort_by_key(|t| std::cmp::Reverse(t.length()));
    let mut groups: Vec<(u64, Vec<FileScanTask>)> = (0..count).map(|_| (0, Vec::new())).collect();
    for task in tasks {
        let smallest = groups.iter_mut().min_by_key(|(bytes, _)| *bytes).expect("count >= 1");
        smallest.0 += task.length();
        smallest.1.push(task);
    }
    groups.into_iter().map(|(_, tasks)| tasks).collect()
}

impl DisplayAs for LogsScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let files = self.tasks().count();
        write!(
            f,
            "LogsScanExec: files={files}, bytes={}, partitions={}, projection=[{}]",
            self.total_bytes(),
            self.groups.len(),
            self.schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>().join(",")
        )?;
        if let Some(predicate) = &self.predicate {
            write!(f, ", predicate=[{predicate}]")?;
        }
        if let Some(limit) = self.limit {
            write!(f, ", limit={limit}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for LogsScanExec {
    fn name(&self) -> &str {
        "LogsScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("LogsScanExec is a leaf");
        }
        Ok(self)
    }

    fn execute(&self, partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let Some(tasks) = self.groups.get(partition).cloned() else {
            return internal_err!("LogsScanExec has no partition {partition}");
        };
        let file_io = self.file_io.clone();
        let batches = stream::once(async move {
            let reader = ArrowReaderBuilder::new(file_io, Runtime::current())
                .with_row_group_filtering_enabled(true)
                .with_row_selection_enabled(true)
                .build();
            let result = reader.read(stream::iter(tasks.into_iter().map(Ok)).boxed()).map_err(to_datafusion_error)?;
            Ok::<_, datafusion::error::DataFusionError>(result.stream().map_err(to_datafusion_error))
        })
        .try_flatten();

        let limited = match self.limit {
            None => batches.boxed(),
            Some(limit) => {
                let mut remaining = limit;
                batches
                    .try_filter_map(move |batch| {
                        let out = if remaining == 0 {
                            None
                        } else if batch.num_rows() <= remaining {
                            remaining -= batch.num_rows();
                            Some(batch)
                        } else {
                            let slice = batch.slice(0, remaining);
                            remaining = 0;
                            Some(slice)
                        };
                        futures::future::ready(Ok(out))
                    })
                    .boxed()
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&self.schema), limited)))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        let tasks: Vec<&FileScanTask> = match partition {
            Some(p) => self.groups.get(p).map(|g| g.iter().collect()).unwrap_or_default(),
            None => self.tasks().collect(),
        };
        let mut stats = Statistics::new_unknown(&self.schema);
        let rows: Option<u64> = tasks.iter().map(|t| t.record_count()).sum();
        if let Some(rows) = rows {
            stats.num_rows = Precision::Inexact(rows as usize);
        }
        stats.total_byte_size = Precision::Inexact(tasks.iter().map(|t| t.length()).sum::<u64>() as usize);
        Ok(Arc::new(stats))
    }
}
