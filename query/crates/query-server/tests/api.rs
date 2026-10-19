//! HTTP-level tests over the in-memory logs table (30 rows starting at
//! 2026-10-18T00:00:00Z, four per second; see planner::fixtures).

use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use datafusion::arrow::ipc::reader::StreamReader;
use planner::SearchPlanner;
use planner::fixtures::{ROWS, logs_schema, logs_table};
use query_server::api::{AppState, router};
use query_server::config::Config;
use query_server::source::{StaticSource, TableHandle};
use serde_json::{Value, json};
use tower::ServiceExt;

const SNAPSHOT: i64 = 42;

fn app_with(overrides: &[(&str, &str)]) -> axum::Router {
    let mut vars: Vec<(String, String)> = [
        ("ICEBERG_CATALOG_URI", "http://unused"),
        ("S3_ENDPOINT", "unused:9000"),
        ("S3_ACCESS_KEY", "k"),
        ("S3_SECRET_KEY", "s"),
        ("QUERY_SQL_MAX_ROWS", "1000"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    vars.extend(overrides.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    let config =
        Config::from_lookup(|name| vars.iter().rev().find(|(k, _)| k == name).map(|(_, v)| v.clone())).unwrap();
    let handle = TableHandle {
        provider: logs_table(),
        snapshot_id: Some(SNAPSHOT),
        planner: Arc::new(SearchPlanner::new(&logs_schema()).unwrap()),
    };
    let runtime = query_server::runtime(config.memory_limit_bytes).unwrap();
    router(AppState::new(config, Arc::new(StaticSource { handle }), runtime))
}

fn app() -> axum::Router {
    app_with(&[])
}

async fn call(app: &axum::Router, path: &str, body: Value) -> (StatusCode, Value) {
    let (status, bytes) = call_raw(app, path, body, None).await;
    let value =
        serde_json::from_slice(&bytes).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()));
    (status, value)
}

async fn call_raw(app: &axum::Router, path: &str, body: Value, accept: Option<&str>) -> (StatusCode, Vec<u8>) {
    let mut request = Request::post(path).header(header::CONTENT_TYPE, "application/json");
    if let Some(accept) = accept {
        request = request.header(header::ACCEPT, accept);
    }
    let response = app.clone().oneshot(request.body(Body::from(body.to_string())).unwrap()).await.unwrap();
    let status = response.status();
    (status, to_bytes(response.into_body(), usize::MAX).await.unwrap().to_vec())
}

fn window() -> Value {
    json!({"start": "2026-10-18T00:00:00Z", "end": "2026-10-18T01:00:00Z"})
}

fn with(mut base: Value, extra: Value) -> Value {
    base.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
    base
}

#[tokio::test]
async fn search_returns_rows_newest_first_with_json_maps() {
    let (status, body) = call(&app(), "/v1/search", with(window(), json!({"limit": 3}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["timestamp"], "2026-10-18T00:00:07.000000Z");
    assert!(rows[0]["log_attributes"]["seq"].is_string(), "maps are JSON objects: {}", rows[0]);
    assert_eq!(rows[0]["resource_attributes"]["service.name"], rows[0]["service_name"]);
    assert!(rows[0].get("trace_id").is_some());
    assert_eq!(body["snapshot_id"], SNAPSHOT);
    assert_eq!(body["stats"]["rows"], 3);
    assert!(body["next_cursor"].is_string());
}

#[tokio::test]
async fn cursor_pages_through_everything_once() {
    let app = app();
    let request = with(window(), json!({"limit": 8, "filters": {"service.name": ["checkout", "cart"]}}));
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let body = match &cursor {
            Some(c) => with(request.clone(), json!({"cursor": c})),
            None => request.clone(),
        };
        let (status, page) = call(&app, "/v1/search", body).await;
        assert_eq!(status, StatusCode::OK, "{page}");
        for row in page["rows"].as_array().unwrap() {
            seen.push(row["log_attributes"]["seq"].as_str().unwrap().parse::<usize>().unwrap());
        }
        match page["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_string()),
            None => break,
        }
    }
    let mut expected: Vec<usize> = (0..ROWS).filter(|i| i % 3 != 2).collect();
    seen.sort();
    expected.sort();
    assert_eq!(seen, expected);
}

#[tokio::test]
async fn cursor_rules() {
    let app = app();
    let (_, first) = call(&app, "/v1/search", with(window(), json!({"limit": 2, "grep": "request"}))).await;
    let cursor = first["next_cursor"].as_str().unwrap().to_string();

    // A cursor only works with the query that produced it
    let (status, body) = call(&app, "/v1/search", with(window(), json!({"grep": "other", "cursor": cursor}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "invalid_query");

    // A snapshot the table no longer has (Nessie keeps only the current one):
    // the search continues on the current snapshot
    let mut decoded = logquery::Cursor::decode(&cursor).unwrap();
    decoded.snapshot_id = Some(SNAPSHOT + 1);
    let stale = with(window(), json!({"limit": 2, "grep": "request", "cursor": decoded.encode()}));
    let (status, body) = call(&app, "/v1/search", stale).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["snapshot_id"], SNAPSHOT);
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn filters_grep_and_fields() {
    let request = with(
        window(),
        json!({
            "where": {"and": [
                {"field": "attr.http.status_code", "op": ">=", "value": 404},
                {"not": {"field": "attr.user", "op": "exists"}}
            ]},
            "grep": "timeout", "grep_ignore_case": true,
            "fields": ["k8s.pod.name"]
        }),
    );
    let (status, body) = call(&app(), "/v1/search", request).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut seqs: Vec<usize> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            assert!(r["k8s.pod.name"].as_str().unwrap().starts_with("pod-"), "requested field returned: {r}");
            r["log_attributes"]["seq"].as_str().unwrap().parse().unwrap()
        })
        .collect();
    seqs.sort();
    let expected: Vec<usize> = (0..ROWS).filter(|i| i % 3 != 0 && i % 4 != 0 && i % 5 == 0).collect();
    assert_eq!(seqs, expected);
}

#[tokio::test]
async fn invalid_queries_are_400() {
    let app = app();
    for (body, fragment) in [
        (json!({"end": "now"}), "missing field `start`"),
        (with(window(), json!({"regex": "(unclosed"})), "invalid regex"),
        (with(window(), json!({"filters": {"severity_number": "loud"}})), "cannot compare"),
        (json!({"start": "now-400d"}), "exceeds the maximum"),
    ] {
        let (status, response) = call(&app, "/v1/search", body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(response["error"]["message"].as_str().unwrap().contains(fragment), "{response}");
    }
}

#[tokio::test]
async fn sql_streams_ndjson() {
    let sql = "SELECT service_name, count(*) AS n FROM logs GROUP BY service_name ORDER BY service_name";
    let (status, bytes) = call_raw(&app(), "/v1/sql", json!({"sql": sql}), None).await;
    assert_eq!(status, StatusCode::OK);
    let lines: Vec<Value> =
        String::from_utf8(bytes).unwrap().lines().map(|l| serde_json::from_str(l).unwrap()).collect();
    assert_eq!(
        lines,
        vec![
            json!({"service_name": "auth", "n": 10}),
            json!({"service_name": "cart", "n": 10}),
            json!({"service_name": "checkout", "n": 10}),
        ]
    );
}

#[tokio::test]
async fn sql_row_cap_and_arrow_format() {
    let app = app();
    let (_, bytes) = call_raw(&app, "/v1/sql", json!({"sql": "SELECT body FROM logs", "max_rows": 7}), None).await;
    assert_eq!(String::from_utf8(bytes).unwrap().lines().count(), 7);

    let (status, bytes) = call_raw(
        &app,
        "/v1/sql",
        json!({"sql": "SELECT _kafka_offset FROM logs"}),
        Some("application/vnd.apache.arrow.stream"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
    let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(rows, ROWS);
}

#[tokio::test]
async fn sql_is_read_only() {
    let app = app();
    for sql in [
        "CREATE TABLE t AS SELECT 1",
        "INSERT INTO logs SELECT * FROM logs",
        "CREATE EXTERNAL TABLE x STORED AS PARQUET LOCATION '/etc/'",
        "SET datafusion.execution.batch_size = 1",
        "SELECT * FROM no_such_table",
    ] {
        let (status, body) = call(&app, "/v1/sql", json!({"sql": sql})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{sql}: {body}");
        assert_eq!(body["error"]["code"], "invalid_sql", "{sql}");
    }
}

#[tokio::test]
async fn health_ready_and_metrics() {
    let app = app();
    let get = |path: &'static str| {
        let app = app.clone();
        async move {
            let response = app.oneshot(Request::get(path).body(Body::empty()).unwrap()).await.unwrap();
            let status = response.status();
            (status, String::from_utf8(to_bytes(response.into_body(), usize::MAX).await.unwrap().to_vec()).unwrap())
        }
    };
    assert_eq!(get("/health").await.0, StatusCode::OK);
    assert_eq!(get("/ready").await.0, StatusCode::OK);
    call(&app, "/v1/search", with(window(), json!({"limit": 1}))).await;
    call(&app, "/v1/search", json!({"bad": true})).await;
    let (_, metrics) = get("/metrics").await;
    assert!(metrics.contains("query_requests_total{endpoint=\"search\",outcome=\"ok\"} 1"), "{metrics}");
    assert!(metrics.contains("query_requests_total{endpoint=\"search\",outcome=\"client_error\"} 1"), "{metrics}");
    assert!(metrics.contains("query_rows_returned_total{endpoint=\"search\"} 1"), "{metrics}");
    assert!(metrics.contains("query_table_ready 1"));
}
