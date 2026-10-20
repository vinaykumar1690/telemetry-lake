//! Distributed execution against an in-process cluster: two gRPC workers on
//! localhost and a coordinator, over a real (local-filesystem) Iceberg table.
//! Distributed results must equal local ones, and small scans must not
//! leave the coordinator.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::catalog::TableProvider;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::Worker;
use iceberg::table::Table;
use logquery::{CompareOp, Cursor, LogQuery, Order, Predicate, TextMatch, TimeRange, Value};
use logscan::LogsTableProvider;
use logscan::fixtures::iceberg_logs_table;
use planner::SearchPlanner;
use planner::fixtures::{BASE_US, ROWS};
use query_server::distributed::{LogsWorkerSession, ScanPolicy, Workers, coordinator_state};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

async fn cluster(table: &Table, workers: usize) -> Workers {
    let runtime = query_server::runtime(512 << 20).unwrap();
    let mut urls = Vec::new();
    for _ in 0..workers {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        urls.push(format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port()));
        let worker = Worker::from_session_builder(LogsWorkerSession {
            file_io: table.file_io().clone(),
            runtime: runtime.clone(),
        })
        .with_runtime_env(runtime.clone());
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(worker.into_worker_server())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    Workers::parse(&urls.join(",")).unwrap()
}

fn coordinator(table: &Table, workers: Workers, policy: ScanPolicy) -> SessionContext {
    let runtime = query_server::runtime(512 << 20).unwrap();
    SessionContext::from(coordinator_state(
        SessionConfig::new().with_target_partitions(4),
        runtime,
        Arc::new(workers),
        table.file_io().clone(),
        policy,
    ))
}

async fn fixture(name: &str) -> Table {
    let dir: PathBuf = std::env::temp_dir().join(format!("distributed-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    iceberg_logs_table(&dir, 6).await.unwrap()
}

/// Every scan distributed: one task per byte, capped at the worker count
const ALWAYS: ScanPolicy = ScanPolicy { min_bytes: 0, bytes_per_task: 1 };
const NEVER: ScanPolicy = ScanPolicy { min_bytes: u64::MAX, bytes_per_task: 1 };

fn seqs(batches: &[RecordBatch]) -> Vec<usize> {
    let mut out = Vec::new();
    for batch in batches {
        let column = batch.column_by_name("attr.seq").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        out.extend(column.iter().map(|s| s.unwrap().parse::<usize>().unwrap()));
    }
    out
}

fn query(filter: Option<Predicate>, limit: usize) -> LogQuery {
    LogQuery {
        time: TimeRange { start_us: BASE_US, end_us: BASE_US + 3_600_000_000 },
        filter,
        fields: vec!["attr.seq".into()],
        order: Order::Desc,
        limit,
        cursor: None,
    }
}

async fn plan_text(ctx: &SessionContext, provider: Arc<dyn TableProvider>, q: &LogQuery) -> String {
    let planner = SearchPlanner::new(&provider.schema()).unwrap();
    let plan = planner.frame(ctx, provider, q).unwrap().create_physical_plan().await.unwrap();
    displayable(plan.as_ref()).indent(true).to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_search_matches_local_execution() {
    let table = fixture("search").await;
    let provider: Arc<dyn TableProvider> = Arc::new(LogsTableProvider::try_new(table.clone(), None).unwrap());
    let planner = SearchPlanner::new(&provider.schema()).unwrap();
    let distributed = coordinator(&table, cluster(&table, 2).await, ALWAYS);
    let local = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));

    let plan = plan_text(&distributed, Arc::clone(&provider), &query(None, 100)).await;
    assert!(plan.contains("NetworkCoalesceExec") && plan.contains("LogsScanExec"), "{plan}");

    let cmp = |field: &str, op, value| Some(Predicate::Compare { field: field.into(), op, value });
    let text = |matcher| Some(Predicate::Text { field: "body".into(), matcher });
    for filter in [
        None,
        cmp("service.name", CompareOp::Eq, Value::String("checkout".into())),
        cmp("attr.http.status_code", CompareOp::Ge, Value::Int(404)),
        cmp("k8s.pod.name", CompareOp::Eq, Value::String("pod-1".into())),
        Some(Predicate::Not(Box::new(Predicate::Exists { field: "attr.user".into() }))),
        text(TextMatch::Contains { needle: "timeout".into(), case_insensitive: true }),
        text(TextMatch::Regex { pattern: r"^request 1\d ".into(), case_insensitive: false }),
    ] {
        let q = query(filter.clone(), 100);
        let a = planner.run(&distributed, Arc::clone(&provider), &q, None).await.unwrap();
        let b = planner.run(&local, Arc::clone(&provider), &q, None).await.unwrap();
        assert_eq!(seqs(&a.batches), seqs(&b.batches), "{filter:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pagination_and_sql_aggregation() {
    let table = fixture("pages").await;
    let provider: Arc<dyn TableProvider> = Arc::new(LogsTableProvider::try_new(table.clone(), None).unwrap());
    let planner = SearchPlanner::new(&provider.schema()).unwrap();
    let distributed = coordinator(&table, cluster(&table, 2).await, ALWAYS);

    let mut q = query(None, 7);
    let mut all = Vec::new();
    loop {
        let page = planner.run(&distributed, Arc::clone(&provider), &q, Some(1)).await.unwrap();
        all.extend(seqs(&page.batches));
        match page.next_cursor {
            Some(c) => q.cursor = Some(Cursor::decode(&c.encode()).unwrap()),
            None => break,
        }
    }
    let mut sorted = all.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted, (0..ROWS).collect::<Vec<_>>(), "every row once across pages");

    distributed.register_table("logs", Arc::clone(&provider)).unwrap();
    let sql = "SELECT service_name, count(*) AS n FROM logs GROUP BY service_name ORDER BY service_name";
    let df = distributed.sql(sql).await.unwrap();
    let plan = displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true).to_string();
    assert!(plan.contains("Network"), "aggregation is distributed: {plan}");
    let text =
        datafusion::arrow::util::pretty::pretty_format_batches(&df.collect().await.unwrap()).unwrap().to_string();
    for service in ["auth", "cart", "checkout"] {
        assert!(text.contains(&format!("| {service:<12} | 10 |")) || text.contains(service), "{text}");
    }
    assert_eq!(text.matches("| 10 ").count(), 3, "{text}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_scans_stay_on_the_coordinator() {
    let table = fixture("small").await;
    let provider: Arc<dyn TableProvider> = Arc::new(LogsTableProvider::try_new(table.clone(), None).unwrap());
    let ctx = coordinator(&table, cluster(&table, 2).await, NEVER);
    let plan = plan_text(&ctx, Arc::clone(&provider), &query(None, 100)).await;
    assert!(!plan.contains("Network"), "{plan}");
    let planner = SearchPlanner::new(&provider.schema()).unwrap();
    let out = planner.run(&ctx, provider, &query(None, 100), None).await.unwrap();
    assert_eq!(out.rows, ROWS);
}
