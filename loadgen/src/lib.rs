//! otel-loadgen: sustained OTLP log load against the ingester
//! (docs/load-testing.md)

pub mod api;
pub mod config;
pub mod controller;
pub mod generate;
pub mod metrics;
pub mod node;
pub mod spec;
pub mod stats;
