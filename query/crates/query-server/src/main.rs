use std::io::IsTerminal;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use datafusion_distributed::Worker;
use query_server::api::{AppState, Distributed, router};
use query_server::config::{Config, Role};
use query_server::distributed::{LogsWorkerSession, ScanPolicy, Workers};
use query_server::source::IcebergSource;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        // Plain text when logs go to a file or a container runtime
        .with_ansi(std::io::stdout().is_terminal())
        .init();

    // `otel-query --worker`: a distributed execution worker (no HTTP API)
    let role = if std::env::args().any(|a| a == "--worker") { Role::Worker } else { Role::Coordinator };
    let config = match Config::from_env(role) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("Configuration error: {error}");
            return ExitCode::from(1);
        }
    };
    if role == Role::Worker {
        return run_worker(config).await;
    }
    let source = match IcebergSource::new(&config).await {
        Ok(source) => Arc::new(source),
        Err(error) => {
            eprintln!("Cannot create the catalog client: {error}");
            return ExitCode::from(1);
        }
    };
    let runtime = match query_server::runtime(config.memory_limit_bytes) {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("Cannot create the query runtime: {error}");
            return ExitCode::from(1);
        }
    };

    // Serve /health right away; /ready turns 200 once the table has loaded
    let loader = source.clone();
    tokio::spawn(async move {
        let mut delay = Duration::from_millis(500);
        loop {
            match loader.load().await {
                Ok(()) => {
                    tracing::info!(table = %loader.ident(), "logs table loaded");
                    return;
                }
                Err(error) => {
                    tracing::warn!(%error, retry_in = ?delay, "loading the logs table failed");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            }
        }
    });

    let port = config.http_port;
    let mut state = AppState::new(config.clone(), source, runtime);
    if let Some(spec) = &config.workers {
        match Workers::parse(spec) {
            Ok(workers) => {
                tracing::info!(workers = %spec, min_bytes = config.distribute_min_bytes, "distributed execution enabled");
                state = state.with_distributed(Distributed {
                    workers: Arc::new(workers),
                    file_io: config.file_io(),
                    policy: ScanPolicy::from_config(&config),
                });
            }
            Err(error) => {
                eprintln!("Configuration error: {error}");
                return ExitCode::from(1);
            }
        }
    }
    let app = router(state);
    let listener = match tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("Cannot listen on :{port}: {error}");
            return ExitCode::from(1);
        }
    };
    tracing::info!("query service listening on :{port} (/v1/search /v1/sql /health /ready /metrics)");
    match axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()).await {
        Ok(()) => {
            tracing::info!("query service stopped");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("Server error: {error}");
            ExitCode::from(2)
        }
    }
}

async fn shutdown_signal() {
    let interrupt = tokio::signal::ctrl_c();
    let mut terminate =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = interrupt => {}
        _ = terminate.recv() => {}
    }
    tracing::info!("shutting down: finishing in-flight queries");
}

/// Serves distributed tasks on WORKER_PORT (gRPC) and /health, /ready and
/// /metrics on HTTP_PORT. A worker reads files with its own S3 settings and
/// never talks to the catalog.
async fn run_worker(config: Config) -> ExitCode {
    let runtime = match query_server::runtime(config.memory_limit_bytes) {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("Cannot create the query runtime: {error}");
            return ExitCode::from(1);
        }
    };
    let worker =
        Worker::from_session_builder(LogsWorkerSession { file_io: config.file_io(), runtime: runtime.clone() })
            .with_runtime_env(runtime);

    let health = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/ready", get(|| async { "ready" }))
        .route("/metrics", get(|| async { "query_worker_up 1\n" }));
    let http_port = config.http_port;
    match tokio::net::TcpListener::bind(("0.0.0.0", http_port)).await {
        Ok(listener) => {
            tokio::spawn(async move {
                let _ = axum::serve(listener, health).await;
            });
        }
        Err(error) => {
            eprintln!("Cannot listen on :{http_port}: {error}");
            return ExitCode::from(1);
        }
    }

    let address = std::net::SocketAddr::from(([0, 0, 0, 0], config.worker_port));
    tracing::info!("query worker listening on :{} (gRPC), :{http_port} (/health /ready /metrics)", config.worker_port);
    match tonic::transport::Server::builder()
        .add_service(worker.into_worker_server())
        .serve_with_shutdown(address, shutdown_signal())
        .await
    {
        Ok(()) => {
            tracing::info!("query worker stopped");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("Worker server error: {error}");
            ExitCode::from(2)
        }
    }
}
