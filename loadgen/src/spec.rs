//! What a load test run sends: rate, request shape and label cardinality

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Wire {
    Protobuf,
    Json,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    None,
    Gzip,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceSpec {
    pub name: String,
    pub pods: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtraAttribute {
    pub key: String,
    pub distinct_values: u32,
}

/// Relative weights, as in the UI's Generate tab
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeverityWeights {
    pub debug: f64,
    pub info: f64,
    pub warn: f64,
    pub error: f64,
    pub fatal: f64,
}

impl Default for SeverityWeights {
    fn default() -> Self {
        SeverityWeights { debug: 15.0, info: 65.0, warn: 12.0, error: 7.0, fatal: 1.0 }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSpec {
    /// Target across all workers
    pub records_per_second: u64,
    pub records_per_request: u32,
    /// Runs until stopped when absent
    #[serde(default)]
    pub duration_seconds: Option<u64>,
    /// Requests in flight across all workers; a tick that finds none free is
    /// counted as behind instead of queueing
    pub max_in_flight: u32,
    pub wire: Wire,
    pub compression: Compression,
    #[serde(default)]
    pub severity_weights: SeverityWeights,

    // Cardinality
    pub services: Vec<ServiceSpec>,
    /// Distinct host.name values
    pub nodes: u32,
    pub service_namespace: String,
    pub k8s_namespace: String,
    pub environment: String,
    /// Distinct user.id values
    pub users: u32,
    #[serde(default)]
    pub extra_attributes: Vec<ExtraAttribute>,
    /// Replace every pod (new names and instance ids) this often, like a
    /// rolling deploy: series cardinality grows over time
    #[serde(default)]
    pub pod_churn_minutes: Option<u32>,
    /// Filler appended to every body, to reach a byte rate with fewer records
    #[serde(default)]
    pub body_padding_bytes: u32,
    /// Pod names and instance ids derive from it
    #[serde(default)]
    pub seed: u64,
}

/// Safety caps, from the environment (Config)
#[derive(Clone, Debug)]
pub struct Limits {
    pub max_records_per_second: u64,
    pub max_in_flight: u32,
    pub max_request_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits { max_records_per_second: 2_000_000, max_in_flight: 4096, max_request_bytes: 16 * 1024 * 1024 }
    }
}

const MAX_SERVICES: usize = 1000;
const MAX_PODS: u64 = 100_000;
const MAX_DISTINCT: u32 = 10_000_000;
const MAX_EXTRA_ATTRIBUTES: usize = 20;
const MAX_RECORDS_PER_REQUEST: u32 = 50_000;
const MAX_PADDING: u32 = 64 * 1024;

impl RunSpec {
    pub fn total_pods(&self) -> u64 {
        self.services.iter().map(|s| u64::from(s.pods)).sum()
    }

    /// Requests per second at the target rate
    pub fn requests_per_second(&self) -> f64 {
        self.records_per_second as f64 / f64::from(self.records_per_request.max(1))
    }

    /// Checks everything but the request size, which needs a sample (Generator)
    pub fn validate(&self, limits: &Limits) -> Result<(), String> {
        if self.records_per_second == 0 {
            return Err("records_per_second must be at least 1".into());
        }
        if self.records_per_second > limits.max_records_per_second {
            return Err(format!("records_per_second is capped at {}", limits.max_records_per_second));
        }
        if !(1..=MAX_RECORDS_PER_REQUEST).contains(&self.records_per_request) {
            return Err(format!("records_per_request must be 1 to {MAX_RECORDS_PER_REQUEST}"));
        }
        if self.max_in_flight == 0 || self.max_in_flight > limits.max_in_flight {
            return Err(format!("max_in_flight must be 1 to {}", limits.max_in_flight));
        }
        if self.duration_seconds == Some(0) {
            return Err("duration_seconds must be at least 1 (omit it to run until stopped)".into());
        }
        if self.services.is_empty() || self.services.len() > MAX_SERVICES {
            return Err(format!("give 1 to {MAX_SERVICES} services"));
        }
        if self.services.iter().any(|s| s.name.trim().is_empty()) {
            return Err("every service needs a name".into());
        }
        if self.services.iter().any(|s| s.pods == 0) {
            return Err("every service needs at least one pod".into());
        }
        if self.total_pods() > MAX_PODS {
            return Err(format!("at most {MAX_PODS} pods in total"));
        }
        if self.nodes == 0 || self.nodes > MAX_DISTINCT {
            return Err(format!("nodes must be 1 to {MAX_DISTINCT}"));
        }
        if self.users == 0 || self.users > MAX_DISTINCT {
            return Err(format!("users must be 1 to {MAX_DISTINCT}"));
        }
        if self.extra_attributes.len() > MAX_EXTRA_ATTRIBUTES {
            return Err(format!("at most {MAX_EXTRA_ATTRIBUTES} extra attributes"));
        }
        for attribute in &self.extra_attributes {
            if attribute.key.trim().is_empty() {
                return Err("every extra attribute needs a key".into());
            }
            if attribute.distinct_values == 0 || attribute.distinct_values > MAX_DISTINCT {
                return Err(format!("{}: distinct_values must be 1 to {MAX_DISTINCT}", attribute.key));
            }
        }
        if self.pod_churn_minutes == Some(0) {
            return Err("pod_churn_minutes must be at least 1 (omit it for no churn)".into());
        }
        if self.body_padding_bytes > MAX_PADDING {
            return Err(format!("body_padding_bytes is capped at {MAX_PADDING}"));
        }
        let w = &self.severity_weights;
        let weights = [w.debug, w.info, w.warn, w.error, w.fatal];
        if weights.iter().any(|x| !x.is_finite() || *x < 0.0) || weights.iter().all(|x| *x == 0.0) {
            return Err("severity weights must be non-negative, and at least one positive".into());
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn sample_spec() -> RunSpec {
    RunSpec {
        records_per_second: 1000,
        records_per_request: 100,
        duration_seconds: None,
        max_in_flight: 8,
        wire: Wire::Protobuf,
        compression: Compression::None,
        severity_weights: SeverityWeights::default(),
        services: vec![
            ServiceSpec { name: "frontend".into(), pods: 3 },
            ServiceSpec { name: "payment".into(), pods: 2 },
        ],
        nodes: 3,
        service_namespace: "shop".into(),
        k8s_namespace: "shop".into(),
        environment: "load".into(),
        users: 50,
        extra_attributes: vec![],
        pod_churn_minutes: None,
        body_padding_bytes: 0,
        seed: 7,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_is_valid() {
        assert_eq!(sample_spec().validate(&Limits::default()), Ok(()));
    }

    type Case = (fn(&mut RunSpec), &'static str);

    #[test]
    fn rejects_bad_specs() {
        let limits = Limits::default();
        let cases: Vec<Case> = vec![
            (|s| s.records_per_second = 0, "records_per_second"),
            (|s| s.records_per_second = 3_000_000, "capped"),
            (|s| s.records_per_request = 0, "records_per_request"),
            (|s| s.max_in_flight = 0, "max_in_flight"),
            (|s| s.max_in_flight = 5000, "max_in_flight"),
            (|s| s.duration_seconds = Some(0), "duration_seconds"),
            (|s| s.services.clear(), "services"),
            (|s| s.services[0].name = " ".into(), "name"),
            (|s| s.services[0].pods = 0, "pod"),
            (|s| s.users = 0, "users"),
            (|s| s.nodes = 0, "nodes"),
            (|s| s.extra_attributes.push(ExtraAttribute { key: "k".into(), distinct_values: 0 }), "distinct_values"),
            (|s| s.pod_churn_minutes = Some(0), "pod_churn_minutes"),
            (|s| s.body_padding_bytes = 1 << 20, "body_padding_bytes"),
            (
                |s| s.severity_weights = SeverityWeights { debug: 0.0, info: 0.0, warn: 0.0, error: 0.0, fatal: 0.0 },
                "severity",
            ),
        ];
        for (mutate, needle) in cases {
            let mut spec = sample_spec();
            mutate(&mut spec);
            let error = spec.validate(&limits).unwrap_err();
            assert!(error.contains(needle), "{error} should mention {needle}");
        }
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let mut value = serde_json::to_value(sample_spec()).unwrap();
        value["records_per_secnd"] = 5.into();
        assert!(serde_json::from_value::<RunSpec>(value).is_err());
    }
}
