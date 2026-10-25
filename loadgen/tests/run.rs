//! Runs against an in-process fake ingester: local mode, workers, rate,
//! in-flight limit and the API's validation

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use loadgen::api::{controller_router, worker_router};
use loadgen::controller::{Controller, Workers};
use loadgen::node::{Node, NodeConfig};
use loadgen::spec::{Compression, Limits, RunSpec, ServiceSpec, SeverityWeights, Wire};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;
use serde_json::{Value, json};
use tokio::net::TcpListener;

#[derive(Default)]
struct Ingested {
    requests: AtomicU64,
    records: AtomicU64,
}

async fn fake_ingester(delay: Duration, status: StatusCode) -> (String, Arc<Ingested>) {
    let ingested = Arc::new(Ingested::default());
    let app = Router::new()
        .route(
            "/v1/logs",
            post(move |State(seen): State<Arc<Ingested>>, headers: HeaderMap, body: Bytes| async move {
                assert_eq!(headers["content-type"], "application/x-protobuf");
                let request = ExportLogsServiceRequest::decode(body.as_ref()).expect("valid OTLP");
                let records: usize =
                    request.resource_logs.iter().flat_map(|rl| &rl.scope_logs).map(|sl| sl.log_records.len()).sum();
                seen.requests.fetch_add(1, Relaxed);
                seen.records.fetch_add(records as u64, Relaxed);
                tokio::time::sleep(delay).await;
                status
            }),
        )
        .with_state(ingested.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{address}/v1/logs"), ingested)
}

async fn serve(router: Router, bind: &str) -> Option<SocketAddr> {
    let listener = TcpListener::bind(bind).await.ok()?;
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    Some(address)
}

fn node(ingest_url: &str) -> Arc<Node> {
    let node = Node::new(
        NodeConfig { ingest_url: ingest_url.parse().unwrap(), tasks: 2, request_timeout: Duration::from_secs(5) },
        64,
    );
    node.spawn_sampler();
    node
}

fn spec(records_per_second: u64, max_in_flight: u32, duration_seconds: Option<u64>) -> RunSpec {
    RunSpec {
        records_per_second,
        records_per_request: 50,
        duration_seconds,
        max_in_flight,
        wire: Wire::Protobuf,
        compression: Compression::None,
        severity_weights: SeverityWeights::default(),
        services: vec![ServiceSpec { name: "checkout".into(), pods: 4 }],
        nodes: 2,
        service_namespace: "shop".into(),
        k8s_namespace: "shop".into(),
        environment: "it".into(),
        users: 100,
        extra_attributes: vec![],
        pod_churn_minutes: None,
        body_padding_bytes: 0,
        seed: 1,
    }
}

async fn api(address: SocketAddr, method: &str, body: Option<Value>) -> (u16, Value) {
    let client = loadgen::node::http_client(4);
    let request = hyper::Request::builder()
        .method(method)
        .uri(format!("http://{address}/v1/run"))
        .header("content-type", "application/json")
        .body(http_body_util::Full::new(Bytes::from(body.map(|b| b.to_string()).unwrap_or_default())))
        .unwrap();
    let response = client.request(request).await.unwrap();
    let status = response.status().as_u16();
    let bytes = http_body_util::BodyExt::collect(response.into_body()).await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_run_sends_the_target_rate_for_its_duration() {
    let (url, ingested) = fake_ingester(Duration::ZERO, StatusCode::OK).await;
    let node = node(&url);
    let controller = Controller::local(node.clone(), Limits::default());
    controller.spawn_reconciler();
    let address = serve(controller_router(controller, Some(node)), "127.0.0.1:0").await.unwrap();

    let (status, run) = api(address, "POST", Some(serde_json::to_value(spec(2000, 8, Some(2))).unwrap())).await;
    assert_eq!(status, 201, "{run}");
    assert!(run["id"].as_str().unwrap().starts_with("run-"));
    assert_eq!(api(address, "POST", Some(serde_json::to_value(spec(10, 1, None)).unwrap())).await.0, 409);

    tokio::time::sleep(Duration::from_millis(3500)).await;
    let (_, report) = api(address, "GET", None).await;
    assert_eq!(report["run"]["state"], "finished", "{report}");
    let records = ingested.records.load(Relaxed);
    // 2 s at 2000/s; the loop starts at once, the duration cuts it off
    assert!((3000..=4500).contains(&records), "{records} records");
    assert_eq!(report["totals"]["accepted_records"].as_u64(), Some(records));
    assert_eq!(report["totals"]["codes"]["200"].as_u64(), Some(ingested.requests.load(Relaxed)));
    assert_eq!(report["totals"]["behind"].as_u64(), Some(0));
    assert_eq!(api(address, "DELETE", None).await.0, 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_flight_limit_counts_behind_and_rejections_are_counted() {
    let (url, ingested) = fake_ingester(Duration::from_millis(500), StatusCode::SERVICE_UNAVAILABLE).await;
    let node = node(&url);
    let controller = Controller::local(node.clone(), Limits::default());
    controller.spawn_reconciler();
    let address = serve(controller_router(controller, Some(node.clone())), "127.0.0.1:0").await.unwrap();

    // 40 requests/s against 2 in flight that each take 0.5 s: about 4/s get out
    assert_eq!(api(address, "POST", Some(serde_json::to_value(spec(2000, 2, None)).unwrap())).await.0, 201);
    tokio::time::sleep(Duration::from_millis(2000)).await;
    assert!(node.stats().in_flight.load(Relaxed) <= 2);
    let (status, _) = api(address, "DELETE", None).await;
    assert_eq!(status, 200);
    tokio::time::sleep(Duration::from_millis(700)).await;
    let (_, report) = api(address, "GET", None).await;
    assert_eq!(report["run"]["state"], "stopped");
    let totals = &report["totals"];
    let requests = ingested.requests.load(Relaxed);
    assert!((4..=14).contains(&requests), "{requests} requests");
    assert!(totals["behind"].as_u64().unwrap() > 40, "{totals}");
    assert_eq!(totals["codes"]["503"].as_u64(), Some(requests));
    assert_eq!(totals["accepted_records"].as_u64(), Some(0));
    assert_eq!(totals["rejected_records"].as_u64(), Some(requests * 50));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rate_can_change_mid_run() {
    let (url, ingested) = fake_ingester(Duration::ZERO, StatusCode::OK).await;
    let node = node(&url);
    let controller = Controller::local(node.clone(), Limits::default());
    controller.spawn_reconciler();
    let address = serve(controller_router(controller, Some(node)), "127.0.0.1:0").await.unwrap();

    assert_eq!(api(address, "POST", Some(serde_json::to_value(spec(500, 8, None)).unwrap())).await.0, 201);
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let before = ingested.records.load(Relaxed);
    let (status, run) = api(address, "PATCH", Some(json!({ "records_per_second": 5000 }))).await;
    assert_eq!(status, 200, "{run}");
    assert_eq!(run["spec"]["records_per_second"], 5000);
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let after = ingested.records.load(Relaxed) - before;
    assert!(after > before * 4, "{before} then {after}");
    assert_eq!(api(address, "PATCH", Some(json!({ "records_per_second": 0 }))).await.0, 400);
    assert_eq!(api(address, "PATCH", Some(json!({ "nope": 1 }))).await.0, 400);
    api(address, "DELETE", None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_specs_are_refused() {
    let (url, _) = fake_ingester(Duration::ZERO, StatusCode::OK).await;
    let node = node(&url);
    let limits = Limits { max_request_bytes: 20_000, ..Limits::default() };
    let controller = Controller::local(node.clone(), limits);
    let address = serve(controller_router(controller, Some(node)), "127.0.0.1:0").await.unwrap();

    let (status, body) = api(address, "POST", Some(json!({ "records_per_second": 1 }))).await;
    assert_eq!(status, 400, "{body}");
    let mut big = spec(1000, 4, None);
    big.records_per_request = 1000;
    let (status, body) = api(address, "POST", Some(serde_json::to_value(big).unwrap())).await;
    assert_eq!(status, 400);
    assert!(body["error"].as_str().unwrap().contains("fewer records per request"), "{body}");

    let client = loadgen::node::http_client(1);
    let request = hyper::Request::post(format!("http://{address}/v1/estimate"))
        .header("content-type", "application/json")
        .body(http_body_util::Full::new(Bytes::from(serde_json::to_vec(&spec(1000, 4, None)).unwrap())))
        .unwrap();
    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), 200);
    let bytes = http_body_util::BodyExt::collect(response.into_body()).await.unwrap().to_bytes();
    let estimate: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(estimate["requests_per_second"], 20.0);
    let request_bytes = estimate["request_bytes"].as_f64().unwrap();
    assert!(request_bytes > 5_000.0 && request_bytes < 20_000.0, "{estimate}");
    assert_eq!(estimate["raw_bytes_per_second"].as_f64(), Some(request_bytes * 20.0));
}

/// Two workers on 127.0.0.1 and ::1 with one port, which `localhost` resolves
/// to both of, like a headless Service
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workers_split_the_rate() {
    let (url, ingested) = fake_ingester(Duration::ZERO, StatusCode::OK).await;
    let first = serve(worker_router(node(&url)), "127.0.0.1:0").await.unwrap();
    let Some(second) = serve(worker_router(node(&url)), &format!("[::1]:{}", first.port())).await else {
        eprintln!("no IPv6 loopback; skipping");
        return;
    };
    let resolved: Vec<_> = tokio::net::lookup_host(("localhost", first.port())).await.unwrap().collect();
    if !resolved.contains(&first) || !resolved.contains(&second) {
        eprintln!("localhost does not resolve to both loopbacks; skipping");
        return;
    }
    let controller = Controller::remote(Workers { dns: "localhost".into(), port: first.port() }, Limits::default());
    controller.spawn_reconciler();
    let address = serve(controller_router(controller, None), "127.0.0.1:0").await.unwrap();

    assert_eq!(api(address, "POST", Some(serde_json::to_value(spec(2000, 8, None)).unwrap())).await.0, 201);
    tokio::time::sleep(Duration::from_millis(3000)).await;
    let (_, report) = api(address, "GET", None).await;
    let workers = report["workers"].as_array().unwrap();
    assert_eq!(workers.len(), 2, "{report}");
    for worker in workers {
        assert_eq!(worker["state"], "running", "{worker}");
        assert_eq!(worker["records_per_second"].as_f64(), Some(1000.0));
        assert!(worker["totals"]["accepted_records"].as_u64().unwrap() > 0);
    }
    assert_eq!(report["mode"], "workers");
    api(address, "DELETE", None).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let settled = ingested.records.load(Relaxed);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(ingested.records.load(Relaxed), settled, "workers kept sending after the stop");
    let (_, report) = api(address, "GET", None).await;
    assert_eq!(report["totals"]["accepted_records"].as_u64(), Some(settled));
}
