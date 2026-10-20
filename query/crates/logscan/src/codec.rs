//! Serializes [`LogsScanExec`] so distributed workers can run its tasks.

use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::{exec_datafusion_err, internal_err};
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension};
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use serde::{Deserialize, Serialize};

use crate::exec::LogsScanExec;

/// Encodes a scan as JSON: its task groups (each task is self-contained:
/// file, byte range, projected field ids, bound predicate, delete files),
/// output schema and limit. Decoding attaches this process's `FileIO`, so a
/// worker needs the same S3 settings as the coordinator and no catalog.
#[derive(Debug, Clone)]
pub struct LogsScanCodec {
    file_io: FileIO,
}

impl LogsScanCodec {
    pub fn new(file_io: FileIO) -> Self {
        Self { file_io }
    }
}

#[derive(Serialize, Deserialize)]
struct EncodedScan {
    groups: Vec<Vec<FileScanTask>>,
    schema: Schema,
    limit: Option<usize>,
    predicate: Option<String>,
}

impl PhysicalExtensionCodec for LogsScanCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return internal_err!("LogsScanExec has no inputs, got {}", inputs.len());
        }
        let scan: EncodedScan =
            serde_json::from_slice(buf).map_err(|e| exec_datafusion_err!("cannot decode LogsScanExec: {e}"))?;
        Ok(Arc::new(LogsScanExec::new(
            self.file_io.clone(),
            scan.groups,
            Arc::new(scan.schema),
            scan.limit,
            scan.predicate,
        )))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let Some(scan) = node.downcast_ref::<LogsScanExec>() else {
            return internal_err!("LogsScanCodec cannot encode {}", node.name());
        };
        let encoded = EncodedScan {
            groups: scan.groups().to_vec(),
            schema: scan.schema().as_ref().clone(),
            limit: scan.limit(),
            predicate: scan.predicate().map(str::to_string),
        };
        serde_json::to_writer(buf, &encoded).map_err(|e| exec_datafusion_err!("cannot encode LogsScanExec: {e}"))
    }
}
