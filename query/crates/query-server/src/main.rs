use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use query_server::api::{AppState, router};
use query_server::config::Config;
use query_server::source::IcebergSource;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("Configuration error: {error}");
            return ExitCode::from(1);
        }
    };
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
    let app = router(AppState::new(config, source, runtime));
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
