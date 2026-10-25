//! Environment configuration (README.md lists every variable)

use std::time::Duration;

use hyper::Uri;

use crate::spec::Limits;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Controller,
    Worker,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub role: Role,
    pub http_port: u16,
    /// The ingester's OTLP/HTTP logs endpoint; fixed here, never taken from a request
    pub ingest_url: Uri,
    /// Headless Service resolving to the workers; empty: the controller sends
    pub workers_dns: Option<String>,
    pub workers_port: u16,
    pub tasks: usize,
    pub request_timeout: Duration,
    pub limits: Limits,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(pub String);

impl Config {
    pub fn from_env(role: Role) -> Result<Config, ConfigError> {
        Self::from_lookup(role, |key| std::env::var(key).ok().filter(|v| !v.is_empty()))
    }

    pub fn from_lookup(role: Role, get: impl Fn(&str) -> Option<String>) -> Result<Config, ConfigError> {
        fn number<T: std::str::FromStr>(
            get: &impl Fn(&str) -> Option<String>,
            key: &str,
            default: T,
        ) -> Result<T, ConfigError> {
            match get(key) {
                None => Ok(default),
                Some(value) => value.parse().map_err(|_| ConfigError(format!("{key} must be a number, got '{value}'"))),
            }
        }
        let mut ingest = get("INGEST_URL").ok_or_else(|| ConfigError("INGEST_URL is required".into()))?;
        if !ingest.ends_with("/v1/logs") {
            ingest = format!("{}/v1/logs", ingest.trim_end_matches('/'));
        }
        let ingest_url: Uri = ingest.parse().map_err(|e| ConfigError(format!("INGEST_URL: {e}")))?;
        if ingest_url.scheme_str() != Some("http") {
            return Err(ConfigError("INGEST_URL must be an http:// URL".into()));
        }
        let http_port = number(&get, "HTTP_PORT", 8095u16)?;
        let parallelism = std::thread::available_parallelism().map_or(1, usize::from);
        let tasks = number(&get, "DRIVER_TASKS", parallelism)?.max(1);
        let defaults = Limits::default();
        let limits = Limits {
            max_records_per_second: number(&get, "MAX_RECORDS_PER_SECOND", defaults.max_records_per_second)?,
            max_in_flight: number(&get, "MAX_IN_FLIGHT", defaults.max_in_flight)?,
            max_request_bytes: number(&get, "MAX_REQUEST_BYTES", defaults.max_request_bytes)?,
        };
        Ok(Config {
            role,
            http_port,
            ingest_url,
            workers_dns: get("WORKERS_DNS"),
            workers_port: number(&get, "WORKERS_PORT", http_port)?,
            tasks,
            request_timeout: Duration::from_secs(number(&get, "REQUEST_TIMEOUT_SECONDS", 30u64)?.max(1)),
            limits,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(pairs: &[(&str, &str)]) -> Result<Config, ConfigError> {
        Config::from_lookup(Role::Controller, |key| pairs.iter().find(|(k, _)| *k == key).map(|(_, v)| v.to_string()))
    }

    #[test]
    fn defaults_and_path() {
        let c = config(&[("INGEST_URL", "http://ingester:4318")]).unwrap();
        assert_eq!(c.ingest_url.to_string(), "http://ingester:4318/v1/logs");
        assert_eq!(c.http_port, 8095);
        assert_eq!(c.workers_port, 8095);
        assert!(c.workers_dns.is_none());
        assert_eq!(c.limits.max_in_flight, 4096);
        let c = config(&[("INGEST_URL", "http://ingester:4318/v1/logs"), ("MAX_IN_FLIGHT", "10")]).unwrap();
        assert_eq!(c.ingest_url.path(), "/v1/logs");
        assert_eq!(c.limits.max_in_flight, 10);
    }

    #[test]
    fn errors() {
        assert!(config(&[]).unwrap_err().0.contains("INGEST_URL"));
        assert!(config(&[("INGEST_URL", "https://x")]).unwrap_err().0.contains("http://"));
        assert!(config(&[("INGEST_URL", "http://x"), ("HTTP_PORT", "abc")]).unwrap_err().0.contains("HTTP_PORT"));
    }
}
