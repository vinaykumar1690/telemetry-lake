//! Configuration from the environment. Catalog and S3 variables have the
//! same names as the appender's; invalid values fail startup.

use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use iceberg::io::{FileIO, FileIOBuilder};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use logquery::json::Limits;

#[derive(Debug, Clone)]
pub struct Config {
    pub catalog_uri: String,
    pub warehouse: Option<String>,
    pub namespace: String,
    pub table: String,

    /// With scheme; a bare host:port means plain http (as in the appender)
    pub s3_endpoint: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_region: String,
    pub s3_path_style: bool,

    pub http_port: u16,
    pub limits: Limits,
    pub timeout: Duration,
    pub max_concurrent: usize,
    pub sql_max_rows: usize,
    pub table_refresh: Duration,
    pub memory_limit_bytes: usize,
    pub target_partitions: usize,

    /// Distributed execution (off when None): a comma-separated list of worker
    /// URLs, or `dns:///host:port` for a headless Service resolved every 5 s
    pub workers: Option<String>,
    /// Scans planned to read less than this run in this process
    pub distribute_min_bytes: u64,
    /// Above the threshold, one worker task per this many scanned bytes
    pub bytes_per_task: u64,
    /// Worker mode: the gRPC port
    pub worker_port: u16,
}

/// What the process runs as: the HTTP query service (coordinator) or a
/// distributed execution worker (which needs no catalog, only storage)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Coordinator,
    Worker,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(pub String);

type Result<T> = std::result::Result<T, ConfigError>;

impl Config {
    pub fn from_env(role: Role) -> Result<Self> {
        Self::from_lookup_for(role, |name| env::var(name).ok().filter(|v| !v.is_empty()))
    }

    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        Self::from_lookup_for(Role::Coordinator, get)
    }

    pub fn from_lookup_for(role: Role, get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let required = |name: &str| get(name).ok_or_else(|| ConfigError(format!("{name} is required")));
        let integer = |name: &str, default: u64, min: u64, max: u64| -> Result<u64> {
            let Some(raw) = get(name) else { return Ok(default) };
            let value = u64::from_str(raw.trim())
                .map_err(|_| ConfigError(format!("{name} must be an integer, got '{raw}'")))?;
            if !(min..=max).contains(&value) {
                return Err(ConfigError(format!("{name} must be between {min} and {max}, got {value}")));
            }
            Ok(value)
        };

        let mut s3_endpoint = required("S3_ENDPOINT")?;
        if !s3_endpoint.starts_with("http://") && !s3_endpoint.starts_with("https://") {
            s3_endpoint = format!("http://{s3_endpoint}");
        }
        let s3_path_style = match get("S3_URL_STYLE").as_deref() {
            None | Some("path") => true,
            Some("vhost") => false,
            Some(other) => return Err(ConfigError(format!("S3_URL_STYLE must be path or vhost, got '{other}'"))),
        };
        let cpus = std::thread::available_parallelism().map_or(4, |n| n.get()) as u64;
        let default_limit = integer("QUERY_DEFAULT_LIMIT", 100, 1, 100_000)? as usize;
        let max_limit = integer("QUERY_MAX_LIMIT", 1000, 1, 100_000)? as usize;
        if default_limit > max_limit {
            return Err(ConfigError("QUERY_DEFAULT_LIMIT must not exceed QUERY_MAX_LIMIT".into()));
        }

        Ok(Config {
            catalog_uri: match role {
                Role::Coordinator => required("ICEBERG_CATALOG_URI")?,
                Role::Worker => get("ICEBERG_CATALOG_URI").unwrap_or_default(),
            },
            warehouse: get("ICEBERG_WAREHOUSE").or_else(|| get("S3_BUCKET")),
            namespace: get("ICEBERG_NAMESPACE").unwrap_or_else(|| "default".into()),
            table: get("ICEBERG_TABLE_NAME").unwrap_or_else(|| "logs".into()),
            s3_endpoint,
            s3_access_key: required("S3_ACCESS_KEY")?,
            s3_secret_key: required("S3_SECRET_KEY")?,
            s3_region: get("S3_REGION").unwrap_or_else(|| "us-east-1".into()),
            s3_path_style,
            http_port: integer("HTTP_PORT", 8090, 1, 65535)? as u16,
            limits: Limits {
                default_limit,
                max_limit,
                max_range_us: integer("QUERY_MAX_RANGE_HOURS", 168, 1, 24 * 366)? as i64 * 3_600_000_000,
            },
            timeout: Duration::from_secs(integer("QUERY_TIMEOUT_SECONDS", 30, 1, 3600)?),
            max_concurrent: integer("QUERY_MAX_CONCURRENT", 16, 1, 10_000)? as usize,
            sql_max_rows: integer("QUERY_SQL_MAX_ROWS", 100_000, 1, 100_000_000)? as usize,
            table_refresh: Duration::from_millis(integer("TABLE_REFRESH_MS", 5000, 100, 3_600_000)?),
            memory_limit_bytes: integer("QUERY_MEMORY_LIMIT_MB", 2048, 64, 1 << 20)? as usize * 1024 * 1024,
            target_partitions: integer("QUERY_TARGET_PARTITIONS", cpus, 1, 1024)? as usize,
            workers: get("QUERY_WORKERS"),
            distribute_min_bytes: integer("QUERY_DISTRIBUTE_MIN_BYTES", 2 << 30, 0, u64::MAX)?,
            bytes_per_task: integer("QUERY_BYTES_PER_TASK", 1 << 30, 1, u64::MAX)?,
            worker_port: integer("WORKER_PORT", 50051, 1, 65535)? as u16,
        })
    }

    /// iceberg-rust storage properties for S3 (catalog-vended settings are
    /// overridden by these, as in the appender)
    pub fn storage_props(&self) -> HashMap<String, String> {
        HashMap::from([
            ("s3.endpoint".to_string(), self.s3_endpoint.clone()),
            ("s3.access-key-id".to_string(), self.s3_access_key.clone()),
            ("s3.secret-access-key".to_string(), self.s3_secret_key.clone()),
            ("s3.region".to_string(), self.s3_region.clone()),
            ("s3.path-style-access".to_string(), self.s3_path_style.to_string()),
            // Static keys only: never fall back to the environment or EC2 metadata
            ("s3.disable-config-load".to_string(), "true".to_string()),
            ("s3.disable-ec2-metadata".to_string(), "true".to_string()),
        ])
    }

    /// A FileIO for the configured storage (workers read with it)
    pub fn file_io(&self) -> FileIO {
        FileIOBuilder::new(Arc::new(OpenDalResolvingStorageFactory::new())).with_props(self.storage_props()).build()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn config(extra: &[(&str, &str)]) -> Result<Config> {
        let mut vars: HashMap<String, String> = [
            ("ICEBERG_CATALOG_URI", "http://nessie:19120/iceberg/"),
            ("S3_ENDPOINT", "minio:9000"),
            ("S3_ACCESS_KEY", "k"),
            ("S3_SECRET_KEY", "s"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        for (k, v) in extra {
            vars.insert(k.to_string(), v.to_string());
        }
        Config::from_lookup(|name| vars.get(name).cloned())
    }

    #[test]
    fn defaults() {
        let c = config(&[("S3_BUCKET", "lake")]).unwrap();
        assert_eq!(c.s3_endpoint, "http://minio:9000");
        assert_eq!(c.warehouse.as_deref(), Some("lake"));
        assert_eq!((c.namespace.as_str(), c.table.as_str()), ("default", "logs"));
        assert!(c.s3_path_style);
        assert_eq!(c.http_port, 8090);
        assert_eq!(c.limits.max_limit, 1000);
        assert_eq!(c.limits.max_range_us, 168 * 3_600_000_000);
    }

    #[test]
    fn invalid_values_fail() {
        for extra in [
            ("HTTP_PORT", "0"),
            ("QUERY_TIMEOUT_SECONDS", "abc"),
            ("S3_URL_STYLE", "virtual"),
            ("QUERY_DEFAULT_LIMIT", "5000"),
            ("TABLE_REFRESH_MS", "1"),
            ("QUERY_BYTES_PER_TASK", "0"),
            ("WORKER_PORT", "70000"),
        ] {
            assert!(config(&[extra]).is_err(), "{extra:?}");
        }
        assert!(Config::from_lookup(|_| None).unwrap_err().0.contains("S3_ENDPOINT"));
    }

    #[test]
    fn workers_need_no_catalog() {
        let get = |name: &str| match name {
            "S3_ENDPOINT" | "S3_ACCESS_KEY" | "S3_SECRET_KEY" => Some("x".to_string()),
            _ => None,
        };
        assert!(Config::from_lookup_for(Role::Coordinator, get).is_err());
        let worker = Config::from_lookup_for(Role::Worker, get).unwrap();
        assert_eq!(worker.worker_port, 50051);
        assert_eq!(worker.workers, None);
        assert_eq!(worker.distribute_min_bytes, 2 << 30);
    }

    #[test]
    fn https_endpoint_kept() {
        assert_eq!(
            config(&[("S3_ENDPOINT", "https://s3.amazonaws.com")]).unwrap().s3_endpoint,
            "https://s3.amazonaws.com"
        );
    }
}
