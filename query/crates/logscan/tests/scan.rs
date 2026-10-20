//! LogsScanExec against a real (local-filesystem) Iceberg table: the same
//! rows as iceberg-datafusion's own provider, split into disjoint
//! partitions, and intact through the codec.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::catalog::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::physical_plan::{DefaultPhysicalProtoConverter, PhysicalExtensionCodec};
use iceberg::table::Table;
use iceberg_datafusion::IcebergStaticTableProvider;
use logquery::{CompareOp, LogQuery, Order, Predicate, TextMatch, TimeRange, Value};
use logscan::fixtures::iceberg_logs_table;
use logscan::{LogsScanCodec, LogsScanExec, LogsTableProvider};
use planner::SearchPlanner;
use planner::fixtures::{BASE_US, ROWS};

const COMMITS: usize = 6;

async fn table(name: &str) -> Table {
    let dir: PathBuf = std::env::temp_dir().join(format!("logscan-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    iceberg_logs_table(&dir, COMMITS).await.unwrap()
}

fn ctx(partitions: usize) -> SessionContext {
    SessionContext::new_with_config(SessionConfig::new().with_target_partitions(partitions))
}

fn seqs(batches: &[RecordBatch]) -> Vec<usize> {
    let mut out = Vec::new();
    for batch in batches {
        let column = batch.column_by_name("attr.seq").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        out.extend(column.iter().map(|s| s.unwrap().parse::<usize>().unwrap()));
    }
    out
}

fn query(filter: Option<Predicate>) -> LogQuery {
    LogQuery {
        time: TimeRange { start_us: BASE_US, end_us: BASE_US + 3_600_000_000 },
        filter,
        fields: vec!["attr.seq".into()],
        order: Order::Desc,
        limit: 100,
        cursor: None,
    }
}

fn cases() -> Vec<Option<Predicate>> {
    let cmp = |field: &str, op, value| Some(Predicate::Compare { field: field.into(), op, value });
    let text = |matcher| Some(Predicate::Text { field: "body".into(), matcher });
    vec![
        None,
        cmp("service.name", CompareOp::Eq, Value::String("checkout".into())),
        cmp("severity_number", CompareOp::Ge, Value::Int(17)),
        cmp("attr.http.status_code", CompareOp::Ge, Value::Int(404)),
        cmp("k8s.pod.name", CompareOp::Eq, Value::String("pod-1".into())),
        Some(Predicate::Not(Box::new(Predicate::Exists { field: "attr.user".into() }))),
        text(TextMatch::Contains { needle: "timeout".into(), case_insensitive: true }),
        text(TextMatch::Regex { pattern: r"^request 1\d ".into(), case_insensitive: false }),
    ]
}

#[tokio::test]
async fn same_rows_as_iceberg_datafusion_for_every_search() {
    let table = table("same").await;
    let ours: Arc<dyn TableProvider> = Arc::new(LogsTableProvider::try_new(table.clone(), None).unwrap());
    let upstream: Arc<dyn TableProvider> =
        Arc::new(IcebergStaticTableProvider::try_new_from_table(table).await.unwrap());
    let planner = SearchPlanner::new(&ours.schema()).unwrap();
    for filter in cases() {
        let q = query(filter.clone());
        let a = planner.run(&ctx(4), Arc::clone(&ours), &q, None).await.unwrap();
        let b = planner.run(&ctx(4), Arc::clone(&upstream), &q, None).await.unwrap();
        assert_eq!(seqs(&a.batches), seqs(&b.batches), "{filter:?}");
        assert!(!seqs(&a.batches).is_empty() || filter.is_some());
    }
    // every row once
    let all = planner.run(&ctx(4), ours, &query(None), None).await.unwrap();
    assert_eq!(seqs(&all.batches).into_iter().collect::<BTreeSet<_>>().len(), ROWS);
}

async fn scan(table: &Table, partitions: usize) -> Arc<dyn ExecutionPlan> {
    let provider = LogsTableProvider::try_new(table.clone(), None).unwrap();
    let ctx = ctx(partitions);
    provider.scan(&ctx.state(), None, &[], None).await.unwrap()
}

fn files(scan: &LogsScanExec) -> Vec<String> {
    scan.tasks().map(|t| t.data_file_path().to_string()).collect()
}

#[tokio::test]
async fn files_are_split_into_disjoint_partitions() {
    let table = table("split").await;
    let plan = scan(&table, 4).await;
    let exec = plan.downcast_ref::<LogsScanExec>().unwrap();
    assert_eq!(exec.groups().len(), 4);
    let all = files(exec);
    assert!(all.len() >= COMMITS, "at least one file per commit, got {}", all.len());
    assert_eq!(all.iter().collect::<BTreeSet<_>>().len(), all.len());
    assert!(exec.groups().iter().all(|g| !g.is_empty()));

    let rows: usize =
        collect(Arc::clone(&plan), Arc::new(TaskContext::default())).await.unwrap().iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, ROWS);

    // Distributed variants: same partition count, disjoint, covering every file
    let variants = exec.split(3);
    assert_eq!(variants.len(), 3);
    assert!(variants.iter().all(|v| v.groups().len() == 4));
    let mut seen: Vec<String> = variants.iter().flat_map(files).collect();
    seen.sort();
    let mut expected = all.clone();
    expected.sort();
    assert_eq!(seen, expected);
    // Balanced when files are few: 2 files, 2 partitions, 2 tasks -> 1 file each
    // (filling groups in order would give one task both files)
    let two_files: Vec<_> = exec.tasks().take(2).cloned().collect();
    let small =
        LogsScanExec::new(exec.file_io().clone(), logscan::exec::bin_pack(two_files, 2), exec.schema(), None, None);
    let halves = small.split(2);
    assert_eq!(halves.iter().map(|v| v.tasks().count()).collect::<Vec<_>>(), vec![1, 1]);
    let mut rows = 0;
    for v in variants {
        rows += collect(Arc::new(v), Arc::new(TaskContext::default()))
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
    }
    assert_eq!(rows, ROWS);
}

#[tokio::test]
async fn pruned_and_limited() {
    let table = table("prune").await;
    let provider = LogsTableProvider::try_new(table, None).unwrap();
    let ctx = ctx(2);
    // A day with no data: every file pruned by the partition
    let none = ctx
        .read_table(Arc::new(provider.clone()))
        .unwrap()
        .filter(datafusion::prelude::col("timestamp").lt(datafusion::prelude::lit(
            datafusion::common::ScalarValue::TimestampMicrosecond(
                Some(BASE_US - 86_400_000_000),
                Some("+00:00".into()),
            ),
        )))
        .unwrap();
    let plan = none.clone().create_physical_plan().await.unwrap();
    let text = datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
    assert!(text.contains("LogsScanExec: files=0"), "{text}");
    assert_eq!(none.count().await.unwrap(), 0);

    let limited = ctx.read_table(Arc::new(provider)).unwrap().limit(0, Some(5)).unwrap().collect().await.unwrap();
    assert_eq!(limited.iter().map(|b| b.num_rows()).sum::<usize>(), 5);
}

#[tokio::test]
async fn codec_round_trip() {
    let table = table("codec").await;
    let plan = scan(&table, 3).await;
    let codec = LogsScanCodec::new(table.file_io().clone());
    let converter = DefaultPhysicalProtoConverter {};
    let mut buf = Vec::new();
    codec.try_encode(Arc::clone(&plan), &mut buf, &converter).unwrap();
    let decoded = codec.try_decode(&buf, &[], &TaskContext::default(), &converter).unwrap();

    let original = plan.downcast_ref::<LogsScanExec>().unwrap();
    let copy = decoded.downcast_ref::<LogsScanExec>().unwrap();
    assert_eq!(files(original), files(copy));
    assert_eq!(original.schema(), copy.schema());
    assert_eq!(decoded.properties().partitioning.partition_count(), 3);
    let rows = |p: Arc<dyn ExecutionPlan>| async move {
        collect(p, Arc::new(TaskContext::default())).await.unwrap().iter().map(|b| b.num_rows()).sum::<usize>()
    };
    assert_eq!(rows(decoded).await, ROWS);
}
