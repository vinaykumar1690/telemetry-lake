//! Synthetic OTLP log requests (ExportLogsServiceRequest), encoded straight
//! to protobuf or OTLP/JSON bytes without building message structs.
//!
//! The message templates match the UI's Generate tab (ui/src/generator/
//! generate.ts), so both produce data that looks and searches alike.

use std::fmt::Write as _;
use std::io::Write as _;
use std::sync::Arc;

use bytes::Bytes;
use flate2::write::GzEncoder;

use crate::spec::{Compression, RunSpec, Wire};

/// wyrand: small and fast; quality is ample for test data
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9e37_79b9_7f4a_7c15)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0xa076_1d64_78bd_642f);
        let t = u128::from(self.0) * u128::from(self.0 ^ 0xe703_7ed1_a0b4_28db);
        ((t >> 64) ^ t) as u64
    }

    /// Uniform in [0, n)
    pub fn below(&mut self, n: u64) -> u64 {
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi - lo + 1)
    }

    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
}

const ALNUM: &[u8] = b"bcdfghjklmnpqrstvwxz2456789";
const HEX: &[u8] = b"0123456789abcdef";
/// 32 symbols: 5 random bits per padding character
const PAD: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";

fn alnum(rng: &mut Rng, chars: usize, out: &mut String) {
    for _ in 0..chars {
        out.push(*rng.pick(ALNUM) as char);
    }
}

fn hex(rng: &mut Rng, chars: usize, out: &mut String) {
    for _ in 0..chars {
        out.push(HEX[rng.below(16) as usize] as char);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pod {
    pub service: String,
    pub pod: String,
    pub instance_id: String,
    pub host: String,
}

/// The pods of one churn epoch, named like a Deployment's
/// (<service>-<replicaset hash>-<suffix>). Every worker derives the same set
/// from the spec's seed and the epoch, so cardinality is exactly the spec's.
pub fn make_pods(spec: &RunSpec, epoch: u64) -> Vec<Pod> {
    let mut rng = Rng::new(spec.seed ^ epoch.wrapping_mul(0x2545_f491_4f6c_dd1d));
    let mut pods = Vec::with_capacity(spec.total_pods() as usize);
    for service in &spec.services {
        let name = service.name.trim();
        let mut replica_set = String::new();
        alnum(&mut rng, 10, &mut replica_set);
        for _ in 0..service.pods {
            let mut pod = format!("{name}-{replica_set}-");
            alnum(&mut rng, 5, &mut pod);
            let mut instance_id = String::with_capacity(36);
            for (i, chars) in [8, 4, 4, 4, 12].into_iter().enumerate() {
                if i > 0 {
                    instance_id.push('-');
                }
                hex(&mut rng, chars, &mut instance_id);
            }
            // Round-robin, so min(pods, nodes) distinct host names
            let host = format!("node-{}", pods.len() as u64 % u64::from(spec.nodes) + 1);
            pods.push(Pod { service: name.to_string(), pod, instance_id, host });
        }
    }
    pods
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Severity {
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

impl Severity {
    const ALL: [Severity; 5] = [Severity::Debug, Severity::Info, Severity::Warn, Severity::Error, Severity::Fatal];

    pub fn text(self) -> &'static str {
        match self {
            Severity::Debug => "DEBUG",
            Severity::Info => "INFO",
            Severity::Warn => "WARN",
            Severity::Error => "ERROR",
            Severity::Fatal => "FATAL",
        }
    }

    pub fn number(self) -> u64 {
        match self {
            Severity::Debug => 5,
            Severity::Info => 9,
            Severity::Warn => 13,
            Severity::Error => 17,
            Severity::Fatal => 21,
        }
    }
}

fn routes(service: &str) -> Option<&'static [&'static str]> {
    Some(match service {
        "frontend" => &["/", "/product/{id}", "/cart", "/checkout", "/search"],
        "checkout" => &["/api/checkout", "/api/orders/{id}", "/api/shipping/quote"],
        "payment" => &["/api/charge", "/api/refund", "/api/payment-methods"],
        "cart" => &["/api/cart", "/api/cart/items", "/api/cart/{id}"],
        "auth" => &["/api/login", "/api/logout", "/api/token/refresh", "/api/users/{id}"],
        _ => return None,
    })
}

const METHODS: &[&str] = &["GET", "GET", "GET", "POST", "PUT", "DELETE"];
const PEERS: &[&str] = &["postgres", "redis", "kafka", "inventory", "shipping", "fraud-check"];

/// Per-record values, written into reused buffers
#[derive(Default)]
struct Record {
    body: String,
    route: String,
    user: String,
    status: u64,
    duration: u64,
    method: &'static str,
    trace_id: [u8; 16],
    span_id: [u8; 8],
    extra: Vec<String>,
}

pub struct Payload {
    pub body: Bytes,
    pub records: u32,
    /// Before compression
    pub raw_bytes: usize,
}

struct PodBlock {
    /// ResourceLogs field 1 (Resource), tag and length included
    proto_resource: Vec<u8>,
    /// `{"resource":{...},"scopeLogs":[{"scope":{...},"logRecords":[`
    json_prefix: Vec<u8>,
}

const SCOPE_NAME: &str = "telemetry-lake/loadgen";
const JSON_POD_SUFFIX: &[u8] = b"]}]}";

pub struct Generator {
    spec: Arc<RunSpec>,
    run_id: String,
    started_ms: u64,
    rng: Rng,
    cumulative_weights: [f64; 5],
    epoch: Option<u64>,
    pods: Vec<PodBlock>,
    pod_services: Vec<String>,
    proto_scope: Vec<u8>,
    groups: Vec<Vec<u8>>,
    touched: Vec<usize>,
    record: Record,
    scratch: Vec<u8>,
    out: Vec<u8>,
}

impl Generator {
    /// `seed` varies the records (each worker and task its own); pods derive
    /// from the spec's seed only
    pub fn new(spec: Arc<RunSpec>, run_id: String, started_ms: u64, seed: u64) -> Self {
        let w = spec.severity_weights;
        let mut cumulative_weights = [0.0; 5];
        let mut sum = 0.0;
        for (i, weight) in [w.debug, w.info, w.warn, w.error, w.fatal].into_iter().enumerate() {
            sum += weight.max(0.0);
            cumulative_weights[i] = sum;
        }
        let mut proto_scope = Vec::new();
        let mut scope = Vec::new();
        put_str(&mut scope, 1, SCOPE_NAME);
        put_str(&mut scope, 2, "1");
        put_bytes(&mut proto_scope, 1, &scope);
        let record = Record { extra: vec![String::new(); spec.extra_attributes.len()], ..Record::default() };
        Generator {
            spec,
            run_id,
            started_ms,
            rng: Rng::new(seed),
            cumulative_weights,
            epoch: None,
            pods: Vec::new(),
            pod_services: Vec::new(),
            proto_scope,
            groups: Vec::new(),
            touched: Vec::new(),
            record,
            scratch: Vec::new(),
            out: Vec::new(),
        }
    }

    fn ensure_pods(&mut self, now_ms: u64) {
        let epoch = match self.spec.pod_churn_minutes {
            Some(minutes) => now_ms.saturating_sub(self.started_ms) / (u64::from(minutes) * 60_000),
            None => 0,
        };
        if self.epoch == Some(epoch) {
            return;
        }
        self.epoch = Some(epoch);
        let pods = make_pods(&self.spec, epoch);
        self.pod_services = pods.iter().map(|p| p.service.clone()).collect();
        self.pods = pods.iter().map(|pod| self.pod_block(pod)).collect();
        self.groups = vec![Vec::new(); self.pods.len()];
        self.touched.clear();
    }

    fn resource_attributes<'a>(&'a self, pod: &'a Pod) -> [(&'static str, &'a str); 7] {
        [
            ("service.name", pod.service.as_str()),
            ("service.namespace", self.spec.service_namespace.as_str()),
            ("service.instance.id", pod.instance_id.as_str()),
            ("k8s.pod.name", pod.pod.as_str()),
            ("k8s.namespace.name", self.spec.k8s_namespace.as_str()),
            ("host.name", pod.host.as_str()),
            ("deployment.environment.name", self.spec.environment.as_str()),
        ]
    }

    fn pod_block(&self, pod: &Pod) -> PodBlock {
        let attributes = self.resource_attributes(pod);
        let mut resource = Vec::new();
        for (key, value) in attributes {
            put_kv(&mut resource, 1, key, value);
        }
        let mut proto_resource = Vec::new();
        put_bytes(&mut proto_resource, 1, &resource);

        let mut json = Vec::new();
        json.extend_from_slice(b"{\"resource\":{\"attributes\":[");
        for (i, (key, value)) in attributes.into_iter().enumerate() {
            if i > 0 {
                json.push(b',');
            }
            json_kv(&mut json, key, value);
        }
        json.extend_from_slice(b"]},\"scopeLogs\":[{\"scope\":{\"name\":");
        json_str(&mut json, SCOPE_NAME);
        json.extend_from_slice(b",\"version\":\"1\"},\"logRecords\":[");
        PodBlock { proto_resource, json_prefix: json }
    }

    fn severity(&mut self) -> Severity {
        let total = self.cumulative_weights[4];
        let r = self.rng.unit() * total;
        Severity::ALL.into_iter().zip(self.cumulative_weights).find(|(_, c)| r < *c).map_or(Severity::Info, |(s, _)| s)
    }

    /// Fills `self.record` (the port of generate.ts `message()`)
    fn fill_record(&mut self, pod: usize, severity: Severity) {
        let rng = &mut self.rng;
        let r = &mut self.record;
        let service = self.pod_services[pod].as_str();
        r.method = rng.pick(METHODS);
        r.route.clear();
        match routes(service) {
            Some(list) => r.route.push_str(rng.pick(list)),
            None => {
                let _ = write!(r.route, "/api/{service}");
                if rng.below(2) == 1 {
                    r.route.push_str("/{id}");
                }
            }
        }
        r.user.clear();
        let _ = write!(r.user, "user-{}", rng.below(u64::from(self.spec.users)));
        let mut order = String::new();
        if matches!(severity, Severity::Info | Severity::Error) {
            order.push_str("ord-");
            hex(rng, 8, &mut order);
        }
        let peer = *rng.pick(PEERS);
        r.duration = rng.range(2, 250);
        r.status = *rng.pick(&[200, 200, 200, 201, 204, 304]);
        let (method, route, user) = (r.method, r.route.as_str(), r.user.as_str());
        let b = &mut r.body;
        b.clear();
        let _ = match severity {
            Severity::Debug => match rng.below(3) {
                0 => write!(b, "cache lookup key=session:{user} hit={}", rng.unit() < 0.7),
                1 => write!(b, "acquired connection from {peer} pool in {}ms", rng.range(0, 5)),
                _ => write!(b, "parsed request headers count={}", rng.range(5, 30)),
            },
            Severity::Info => match rng.below(3) {
                0 => write!(b, "{method} {route} completed status={} in {}ms", r.status, r.duration),
                1 => write!(b, "request handled for {user} order={order}"),
                _ => write!(b, "{service} processed {} items for {user}", rng.range(1, 50)),
            },
            Severity::Warn => {
                r.duration = rng.range(800, 4000);
                r.status = *rng.pick(&[200, 404, 409, 429]);
                match rng.below(3) {
                    0 => write!(b, "slow request {method} {route} took {}ms", r.duration),
                    1 => write!(b, "retrying call to {peer} attempt={}", rng.range(2, 4)),
                    _ => write!(b, "rate limit approaching for {user}: {}% of quota", rng.range(80, 99)),
                }
            }
            Severity::Error => {
                r.duration = rng.range(1000, 30000);
                r.status = *rng.pick(&[500, 502, 503, 504]);
                match rng.below(3) {
                    0 => write!(
                        b,
                        "{method} {route} failed status={}: upstream {peer} timeout after {}ms",
                        r.status, r.duration
                    ),
                    1 => write!(b, "database query failed: connection reset by {peer}"),
                    _ if service == "payment" => write!(b, "payment declined for {order}: card_declined"),
                    _ => write!(b, "failed to process {order}: {peer} unavailable"),
                }
            }
            Severity::Fatal => {
                r.status = 500;
                match rng.below(2) {
                    0 => write!(b, "unrecoverable error: out of memory, shutting down"),
                    _ => write!(b, "panic: nil pointer dereference in handler {route}"),
                }
            }
        };
        let padding = self.spec.body_padding_bytes as usize;
        if padding > 0 {
            b.reserve(padding + 5);
            b.push_str(" pad=");
            let mut bits = 0u64;
            for i in 0..padding {
                if i % 12 == 0 {
                    bits = rng.next_u64();
                }
                b.push(PAD[(bits & 31) as usize] as char);
                bits >>= 5;
            }
        }
        for (value, attribute) in r.extra.iter_mut().zip(&self.spec.extra_attributes) {
            value.clear();
            let _ = write!(value, "v{}", rng.below(u64::from(attribute.distinct_values)));
        }
        r.trace_id = (u128::from(rng.next_u64()) << 64 | u128::from(rng.next_u64())).to_be_bytes();
        r.span_id = rng.next_u64().to_be_bytes();
    }

    /// One request of `records_per_request` records stamped around `now_ns`
    pub fn next(&mut self, now_ns: u64) -> Payload {
        self.ensure_pods(now_ns / 1_000_000);
        let wire = self.spec.wire;
        let records = self.spec.records_per_request;
        for _ in 0..records {
            let pod = self.rng.below(self.pods.len() as u64) as usize;
            let severity = self.severity();
            self.fill_record(pod, severity);
            let observed = now_ns;
            let time = now_ns.saturating_sub(self.rng.range(1, 500) * 1_000_000);
            if self.groups[pod].is_empty() {
                self.touched.push(pod);
            }
            match wire {
                Wire::Protobuf => {
                    self.scratch.clear();
                    encode_record(&mut self.scratch, &self.record, severity, time, observed, &self.run_id, &self.spec);
                    put_bytes(&mut self.groups[pod], 2, &self.scratch);
                }
                Wire::Json => {
                    let group = &mut self.groups[pod];
                    if !group.is_empty() {
                        group.push(b',');
                    }
                    json_record(group, &self.record, severity, time, observed, &self.run_id, &self.spec);
                }
            }
        }

        self.out.clear();
        match wire {
            Wire::Protobuf => {
                for &pod in &self.touched {
                    let block = &self.pods[pod];
                    let group = &self.groups[pod];
                    let scope_logs_len = self.proto_scope.len() + group.len();
                    let resource_logs_len =
                        block.proto_resource.len() + 1 + varint_len(scope_logs_len as u64) + scope_logs_len;
                    put_tag(&mut self.out, 1, LEN);
                    put_varint(&mut self.out, resource_logs_len as u64);
                    self.out.extend_from_slice(&block.proto_resource);
                    put_tag(&mut self.out, 2, LEN);
                    put_varint(&mut self.out, scope_logs_len as u64);
                    self.out.extend_from_slice(&self.proto_scope);
                    self.out.extend_from_slice(group);
                }
            }
            Wire::Json => {
                self.out.extend_from_slice(b"{\"resourceLogs\":[");
                for (i, &pod) in self.touched.iter().enumerate() {
                    if i > 0 {
                        self.out.push(b',');
                    }
                    self.out.extend_from_slice(&self.pods[pod].json_prefix);
                    self.out.extend_from_slice(&self.groups[pod]);
                    self.out.extend_from_slice(JSON_POD_SUFFIX);
                }
                self.out.extend_from_slice(b"]}");
            }
        }
        for &pod in &self.touched {
            self.groups[pod].clear();
        }
        self.touched.clear();

        let raw_bytes = self.out.len();
        let body = match self.spec.compression {
            Compression::None => Bytes::copy_from_slice(&self.out),
            Compression::Gzip => {
                let mut encoder = GzEncoder::new(Vec::with_capacity(raw_bytes / 3), flate2::Compression::fast());
                encoder.write_all(&self.out).expect("writing to a Vec");
                Bytes::from(encoder.finish().expect("writing to a Vec"))
            }
        };
        Payload { body, records, raw_bytes }
    }
}

pub fn content_type(wire: Wire) -> &'static str {
    match wire {
        Wire::Protobuf => "application/x-protobuf",
        Wire::Json => "application/json",
    }
}

// Protobuf wire format (the OTLP field numbers are in opentelemetry/proto/logs/v1/logs.proto)

const LEN: u8 = 2;
const I64: u8 = 1;
const VARINT: u8 = 0;

fn put_varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

fn varint_len(value: u64) -> usize {
    (64 - (value | 1).leading_zeros() as usize).div_ceil(7)
}

fn put_tag(buf: &mut Vec<u8>, field: u32, wire_type: u8) {
    put_varint(buf, u64::from(field << 3 | u32::from(wire_type)));
}

fn put_bytes(buf: &mut Vec<u8>, field: u32, data: &[u8]) {
    put_tag(buf, field, LEN);
    put_varint(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

fn put_str(buf: &mut Vec<u8>, field: u32, value: &str) {
    put_bytes(buf, field, value.as_bytes());
}

/// AnyValue{string_value}, as field `field` of the enclosing message
fn put_any_str(buf: &mut Vec<u8>, field: u32, value: &str) {
    let inner = 1 + varint_len(value.len() as u64) + value.len();
    put_tag(buf, field, LEN);
    put_varint(buf, inner as u64);
    put_str(buf, 1, value);
}

/// KeyValue{key, value: AnyValue{string_value}}
fn put_kv(buf: &mut Vec<u8>, field: u32, key: &str, value: &str) {
    let any = 1 + varint_len(value.len() as u64) + value.len();
    let inner = 1 + varint_len(key.len() as u64) + key.len() + 1 + varint_len(any as u64) + any;
    put_tag(buf, field, LEN);
    put_varint(buf, inner as u64);
    put_str(buf, 1, key);
    put_any_str(buf, 2, value);
}

fn put_fixed64(buf: &mut Vec<u8>, field: u32, value: u64) {
    put_tag(buf, field, I64);
    buf.extend_from_slice(&value.to_le_bytes());
}

/// LogRecord fields: 1 time, 2 severity_number, 3 severity_text, 5 body,
/// 6 attributes, 9 trace_id, 10 span_id, 11 observed_time
fn encode_record(
    buf: &mut Vec<u8>,
    r: &Record,
    severity: Severity,
    time: u64,
    observed: u64,
    run_id: &str,
    spec: &RunSpec,
) {
    let mut number = itoa::Buffer::new();
    put_fixed64(buf, 1, time);
    put_tag(buf, 2, VARINT);
    put_varint(buf, severity.number());
    put_str(buf, 3, severity.text());
    put_any_str(buf, 5, &r.body);
    put_kv(buf, 6, "http.method", r.method);
    put_kv(buf, 6, "http.route", &r.route);
    put_kv(buf, 6, "http.status_code", number.format(r.status));
    put_kv(buf, 6, "duration_ms", number.format(r.duration));
    put_kv(buf, 6, "user.id", &r.user);
    put_kv(buf, 6, "generator.run_id", run_id);
    for (value, attribute) in r.extra.iter().zip(&spec.extra_attributes) {
        put_kv(buf, 6, &attribute.key, value);
    }
    put_bytes(buf, 9, &r.trace_id);
    put_bytes(buf, 10, &r.span_id);
    put_fixed64(buf, 11, observed);
}

// OTLP/JSON (https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding)

fn json_str(buf: &mut Vec<u8>, value: &str) {
    buf.push(b'"');
    for &byte in value.as_bytes() {
        match byte {
            b'"' => buf.extend_from_slice(b"\\\""),
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            0..0x20 => {
                buf.extend_from_slice(b"\\u00");
                buf.push(HEX[usize::from(byte >> 4)]);
                buf.push(HEX[usize::from(byte & 15)]);
            }
            _ => buf.push(byte),
        }
    }
    buf.push(b'"');
}

fn json_kv(buf: &mut Vec<u8>, key: &str, value: &str) {
    buf.extend_from_slice(b"{\"key\":");
    json_str(buf, key);
    buf.extend_from_slice(b",\"value\":{\"stringValue\":");
    json_str(buf, value);
    buf.extend_from_slice(b"}}");
}

fn json_hex(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.push(b'"');
    for byte in bytes {
        buf.push(HEX[usize::from(byte >> 4)]);
        buf.push(HEX[usize::from(byte & 15)]);
    }
    buf.push(b'"');
}

fn json_record(
    buf: &mut Vec<u8>,
    r: &Record,
    severity: Severity,
    time: u64,
    observed: u64,
    run_id: &str,
    spec: &RunSpec,
) {
    let mut number = itoa::Buffer::new();
    buf.extend_from_slice(b"{\"timeUnixNano\":\"");
    buf.extend_from_slice(number.format(time).as_bytes());
    buf.extend_from_slice(b"\",\"observedTimeUnixNano\":\"");
    buf.extend_from_slice(number.format(observed).as_bytes());
    buf.extend_from_slice(b"\",\"severityNumber\":");
    buf.extend_from_slice(number.format(severity.number()).as_bytes());
    buf.extend_from_slice(b",\"severityText\":\"");
    buf.extend_from_slice(severity.text().as_bytes());
    buf.extend_from_slice(b"\",\"body\":{\"stringValue\":");
    json_str(buf, &r.body);
    buf.extend_from_slice(b"},\"attributes\":[");
    json_kv(buf, "http.method", r.method);
    buf.push(b',');
    json_kv(buf, "http.route", &r.route);
    buf.push(b',');
    json_kv(buf, "http.status_code", number.format(r.status));
    buf.push(b',');
    json_kv(buf, "duration_ms", number.format(r.duration));
    buf.push(b',');
    json_kv(buf, "user.id", &r.user);
    buf.push(b',');
    json_kv(buf, "generator.run_id", run_id);
    for (value, attribute) in r.extra.iter().zip(&spec.extra_attributes) {
        buf.push(b',');
        json_kv(buf, &attribute.key, value);
    }
    buf.extend_from_slice(b"],\"traceId\":");
    json_hex(buf, &r.trace_id);
    buf.extend_from_slice(b",\"spanId\":");
    json_hex(buf, &r.span_id);
    buf.push(b'}');
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::KeyValue;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use prost::Message;

    use super::*;
    use crate::spec::{ExtraAttribute, sample_spec};

    const NOW_NS: u64 = 1_790_000_000_000_000_000;

    fn string(kv: &[KeyValue], key: &str) -> Option<String> {
        kv.iter().find(|a| a.key == key).and_then(|a| match a.value.as_ref()?.value.as_ref()? {
            Value::StringValue(s) => Some(s.clone()),
            _ => None,
        })
    }

    fn decode(payload: &Payload, wire: Wire) -> ExportLogsServiceRequest {
        match wire {
            Wire::Protobuf => ExportLogsServiceRequest::decode(payload.body.as_ref()).expect("valid protobuf"),
            Wire::Json => serde_json::from_slice(&payload.body).expect("valid OTLP/JSON"),
        }
    }

    fn generator(spec: RunSpec) -> Generator {
        Generator::new(Arc::new(spec), "run-test".into(), NOW_NS / 1_000_000, 1)
    }

    #[test]
    fn varint_lengths() {
        for value in [0u64, 1, 127, 128, 16_383, 16_384, u64::from(u32::MAX), u64::MAX] {
            let mut buf = Vec::new();
            put_varint(&mut buf, value);
            assert_eq!(buf.len(), varint_len(value), "{value}");
        }
    }

    #[test]
    fn protobuf_and_json_decode_to_the_same_shape() {
        for wire in [Wire::Protobuf, Wire::Json] {
            let mut spec = sample_spec();
            spec.wire = wire;
            spec.extra_attributes = vec![ExtraAttribute { key: "tenant.id".into(), distinct_values: 4 }];
            let payload = generator(spec.clone()).next(NOW_NS);
            assert_eq!(payload.records, 100);
            let request = decode(&payload, wire);
            let records: Vec<_> =
                request.resource_logs.iter().flat_map(|rl| &rl.scope_logs).flat_map(|sl| &sl.log_records).collect();
            assert_eq!(records.len(), 100, "{wire:?}");
            for rl in &request.resource_logs {
                let resource = &rl.resource.as_ref().unwrap().attributes;
                assert!(string(resource, "service.name").is_some());
                assert_eq!(string(resource, "deployment.environment.name").as_deref(), Some("load"));
                assert_eq!(rl.scope_logs[0].scope.as_ref().unwrap().name, SCOPE_NAME);
            }
            for record in records {
                assert!(record.time_unix_nano <= NOW_NS && record.time_unix_nano > NOW_NS - 600_000_000);
                assert_eq!(record.observed_time_unix_nano, NOW_NS);
                assert!([5, 9, 13, 17, 21].contains(&record.severity_number));
                assert_eq!(record.trace_id.len(), 16);
                assert_eq!(record.span_id.len(), 8);
                assert_eq!(string(&record.attributes, "generator.run_id").as_deref(), Some("run-test"));
                assert!(string(&record.attributes, "tenant.id").unwrap().starts_with('v'));
                let Some(Value::StringValue(body)) = record.body.as_ref().and_then(|b| b.value.as_ref()) else {
                    panic!("string body")
                };
                assert!(!body.is_empty());
            }
        }
    }

    #[test]
    fn gzip_round_trips() {
        let mut spec = sample_spec();
        spec.compression = Compression::Gzip;
        let payload = generator(spec).next(NOW_NS);
        let mut raw = Vec::new();
        std::io::Read::read_to_end(&mut flate2::read::GzDecoder::new(payload.body.as_ref()), &mut raw).unwrap();
        assert_eq!(raw.len(), payload.raw_bytes);
        assert!(payload.body.len() < payload.raw_bytes);
        assert!(!ExportLogsServiceRequest::decode(raw.as_slice()).unwrap().resource_logs.is_empty());
    }

    #[test]
    fn cardinality_matches_the_spec() {
        let mut spec = sample_spec();
        spec.records_per_request = 5000;
        spec.users = 25;
        spec.nodes = 2;
        spec.extra_attributes = vec![ExtraAttribute { key: "tenant.id".into(), distinct_values: 7 }];
        let request = decode(&generator(spec).next(NOW_NS), Wire::Protobuf);
        let mut distinct: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
        for rl in &request.resource_logs {
            let resource = &rl.resource.as_ref().unwrap().attributes;
            for key in ["k8s.pod.name", "service.instance.id", "host.name", "service.name"] {
                distinct.entry(key).or_default().insert(string(resource, key).unwrap());
            }
            for record in rl.scope_logs.iter().flat_map(|s| &s.log_records) {
                for key in ["user.id", "tenant.id"] {
                    distinct.entry(key).or_default().insert(string(&record.attributes, key).unwrap());
                }
            }
        }
        let counts: BTreeMap<_, _> = distinct.iter().map(|(k, v)| (*k, v.len())).collect();
        assert_eq!(
            counts,
            BTreeMap::from([
                ("host.name", 2),
                ("k8s.pod.name", 5),
                ("service.instance.id", 5),
                ("service.name", 2),
                ("tenant.id", 7),
                ("user.id", 25),
            ])
        );
    }

    #[test]
    fn workers_agree_on_pods_and_churn_replaces_them() {
        let mut spec = sample_spec();
        spec.pod_churn_minutes = Some(10);
        assert_eq!(make_pods(&spec, 0), make_pods(&spec, 0));
        let (before, after) = (make_pods(&spec, 0), make_pods(&spec, 1));
        assert_eq!(before.len(), after.len());
        assert!(before.iter().zip(&after).all(|(a, b)| a.pod != b.pod && a.service == b.service));

        let started_ms = NOW_NS / 1_000_000;
        let pods_at = |offset_ms: u64| {
            let mut g = Generator::new(Arc::new(spec.clone()), "run-test".into(), started_ms, 3);
            let request = decode(&g.next(NOW_NS + offset_ms * 1_000_000), Wire::Protobuf);
            request
                .resource_logs
                .iter()
                .map(|rl| string(&rl.resource.as_ref().unwrap().attributes, "k8s.pod.name").unwrap())
                .collect::<BTreeSet<_>>()
        };
        let epoch0 = make_pods(&spec, 0).into_iter().map(|p| p.pod).collect::<BTreeSet<_>>();
        assert!(pods_at(0).is_subset(&epoch0));
        assert!(pods_at(11 * 60_000).is_disjoint(&epoch0));
    }

    #[test]
    fn padding_sets_the_body_size() {
        let mut spec = sample_spec();
        spec.body_padding_bytes = 1000;
        spec.records_per_request = 10;
        let request = decode(&generator(spec).next(NOW_NS), Wire::Protobuf);
        for record in request.resource_logs.iter().flat_map(|rl| &rl.scope_logs).flat_map(|sl| &sl.log_records) {
            let Some(Value::StringValue(body)) = record.body.as_ref().and_then(|b| b.value.as_ref()) else { panic!() };
            assert!(body.len() > 1000 && body.len() < 1200, "{}", body.len());
        }
    }

    #[test]
    fn json_escapes_names() {
        let mut spec = sample_spec();
        spec.wire = Wire::Json;
        spec.services[0].name = "quote\"back\\slash\ttab".into();
        spec.services.truncate(1);
        let request = decode(&generator(spec).next(NOW_NS), Wire::Json);
        let resource = &request.resource_logs[0].resource.as_ref().unwrap().attributes;
        assert_eq!(string(resource, "service.name").as_deref(), Some("quote\"back\\slash\ttab"));
    }

    #[test]
    fn severity_follows_the_weights() {
        let mut spec = sample_spec();
        spec.severity_weights =
            crate::spec::SeverityWeights { debug: 0.0, info: 0.0, warn: 0.0, error: 1.0, fatal: 0.0 };
        let request = decode(&generator(spec).next(NOW_NS), Wire::Protobuf);
        assert!(
            request
                .resource_logs
                .iter()
                .flat_map(|rl| &rl.scope_logs)
                .flat_map(|sl| &sl.log_records)
                .all(|r| r.severity_text == "ERROR")
        );
    }

    /// cargo test --release -- --ignored --nocapture throughput
    #[test]
    #[ignore]
    fn throughput() {
        for (wire, compression) in
            [(Wire::Protobuf, Compression::None), (Wire::Json, Compression::None), (Wire::Protobuf, Compression::Gzip)]
        {
            let mut spec = sample_spec();
            spec.records_per_request = 1000;
            spec.wire = wire;
            spec.compression = compression;
            let mut g = generator(spec);
            let started = std::time::Instant::now();
            let (mut wire_bytes, mut records) = (0usize, 0u64);
            while started.elapsed().as_secs_f64() < 3.0 {
                let p = g.next(NOW_NS);
                wire_bytes += p.body.len();
                records += u64::from(p.records);
            }
            let secs = started.elapsed().as_secs_f64();
            println!(
                "{wire:?}/{compression:?}: {:.0} records/s, {:.1} MB/s on the wire, one core",
                records as f64 / secs,
                wire_bytes as f64 / secs / 1e6
            );
        }
    }
}
