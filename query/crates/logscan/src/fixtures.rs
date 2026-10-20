//! A real Iceberg logs table on the local filesystem, holding the planner's
//! fixture rows (`planner::fixtures::logs_table`), written by several commits
//! so it has several files.
//!
//! Unpartitioned: at iceberg-rust 5b82ab99, DataFusion's INSERT into a
//! partitioned table fails on MAP columns (`strip_metadata_from_schema` has
//! no map-key hook: "Field stack underflow in primitive"). Reads are not
//! affected, and production files are written by DuckDB; partition pruning
//! is covered by the live tests.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_datafusion::{IcebergCatalogTableProvider, to_datafusion_error};

/// Writes the fixture rows in `commits` commits (rows `i % commits == k` in
/// commit k) and returns the table as loaded after the last one
pub async fn iceberg_logs_table(dir: &Path, commits: usize) -> Result<Table> {
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("fixture", HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), format!("file://{}", dir.display()))]))
        .await
        .map_err(to_datafusion_error)?;
    let catalog: Arc<dyn Catalog> = Arc::new(catalog);
    let namespace = NamespaceIdent::new("logs_ns".to_string());
    catalog.create_namespace(&namespace, HashMap::new()).await.map_err(to_datafusion_error)?;

    let schema =
        arrow_schema_to_schema(&with_field_ids(&planner::fixtures::logs_schema())).map_err(to_datafusion_error)?;
    let creation = TableCreation::builder().name("logs".to_string()).schema(schema).build();
    catalog.create_table(&namespace, creation).await.map_err(to_datafusion_error)?;

    let ctx = SessionContext::new();
    ctx.register_table("source", planner::fixtures::logs_table())?;
    let target = IcebergCatalogTableProvider::try_new(Arc::clone(&catalog), namespace.clone(), "logs").await?;
    ctx.register_table("target", Arc::new(target))?;
    for k in 0..commits {
        ctx.sql(&format!(
            "INSERT INTO target SELECT * FROM source WHERE CAST(log_attributes['seq'] AS INT) % {commits} = {k}"
        ))
        .await?
        .collect()
        .await?;
    }
    catalog.load_table(&TableIdent::new(namespace, "logs".to_string())).await.map_err(to_datafusion_error)
}

/// Assigns Iceberg field ids (as Parquet field-id metadata), depth first,
/// including map keys and values
fn with_field_ids(schema: &ArrowSchema) -> ArrowSchema {
    let mut next = 1;
    ArrowSchema::new(schema.fields().iter().map(|f| assign(f, &mut next)).collect::<Vec<_>>())
}

fn assign(field: &Field, next: &mut i32) -> Field {
    let id = *next;
    *next += 1;
    let data_type = match field.data_type() {
        DataType::Map(entries, sorted) => {
            let DataType::Struct(kv) = entries.data_type() else { unreachable!("map entries are a struct") };
            let kv: Fields = kv.iter().map(|f| assign(f, next)).collect();
            DataType::Map(Arc::new(Field::new(entries.name(), DataType::Struct(kv), false)), *sorted)
        }
        other => other.clone(),
    };
    let mut metadata = field.metadata().clone();
    metadata.insert("PARQUET:field_id".to_string(), id.to_string());
    Field::new(field.name(), data_type, field.is_nullable()).with_metadata(metadata)
}
