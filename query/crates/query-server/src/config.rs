//! Configuration from the environment. Catalog and S3 variables have the
//! same names as the appender's; invalid values fail startup.

use std::env;
use std::str::FromStr;
use std::time::Duration;

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
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(pub String);

type Result<T> = std::result::Result<T, ConfigError>;

impl Config {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| env::var(name).ok().filter(|v| !v.is_empty()))
    }

    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
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
            catalog_uri: required("ICEBERG_CATALOG_URI")?,
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
        })
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
        ] {
            assert!(config(&[extra]).is_err(), "{extra:?}");
        }
        assert!(Config::from_lookup(|_| None).unwrap_err().0.contains("S3_ENDPOINT"));
    }

    #[test]
    fn https_endpoint_kept() {
        assert_eq!(
            config(&[("S3_ENDPOINT", "https://s3.amazonaws.com")]).unwrap().s3_endpoint,
            "https://s3.amazonaws.com"
        );
    }
}
