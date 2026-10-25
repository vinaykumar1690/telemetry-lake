use std::io::IsTerminal;
use std::process::ExitCode;

use loadgen::api::{controller_router, worker_router};
use loadgen::config::{Config, Role};
use loadgen::controller::{Controller, Workers};
use loadgen::node::{Node, NodeConfig};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_ansi(std::io::stdout().is_terminal())
        .init();

    // `otel-loadgen --worker`: sends the share the controller assigns it
    let role = if std::env::args().any(|a| a == "--worker") { Role::Worker } else { Role::Controller };
    let config = match Config::from_env(role) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("Configuration error: {error}");
            return ExitCode::from(1);
        }
    };
    let node_config = NodeConfig {
        ingest_url: config.ingest_url.clone(),
        tasks: config.tasks,
        request_timeout: config.request_timeout,
    };

    let router = match (role, &config.workers_dns) {
        (Role::Worker, _) => {
            let node = Node::new(node_config, config.limits.max_in_flight as usize);
            node.spawn_sampler();
            worker_router(node)
        }
        (Role::Controller, Some(dns)) => {
            let controller =
                Controller::remote(Workers { dns: dns.clone(), port: config.workers_port }, config.limits.clone());
            controller.spawn_reconciler();
            controller_router(controller, None)
        }
        (Role::Controller, None) => {
            let node = Node::new(node_config, config.limits.max_in_flight as usize);
            node.spawn_sampler();
            let controller = Controller::local(node.clone(), config.limits.clone());
            controller.spawn_reconciler();
            controller_router(controller, Some(node))
        }
    };

    let address = std::net::SocketAddr::from(([0, 0, 0, 0], config.http_port));
    let listener = match tokio::net::TcpListener::bind(address).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("Cannot listen on {address}: {error}");
            return ExitCode::from(1);
        }
    };
    tracing::info!(
        %address,
        role = ?role,
        ingest_url = %config.ingest_url,
        workers = config.workers_dns.as_deref().unwrap_or("none (sends itself)"),
        tasks = config.tasks,
        "otel-loadgen listening"
    );
    let shutdown = async {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    };
    if let Err(error) = axum::serve(listener, router).with_graceful_shutdown(shutdown).await {
        eprintln!("Server error: {error}");
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}
