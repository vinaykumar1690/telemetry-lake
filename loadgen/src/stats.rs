//! Lock-free counters for the requests a node sends, and the windowed rates
//! and latency percentiles derived from snapshots of them

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// Latency bucket upper bounds in seconds: 0.5 ms × 1.5^i, up to about a minute
pub fn latency_bounds() -> &'static [f64] {
    static BOUNDS: std::sync::OnceLock<Vec<f64>> = std::sync::OnceLock::new();
    BOUNDS.get_or_init(|| (0..30).map(|i| (500.0 * 1.5f64.powi(i)).round() / 1e6).collect())
}

const STATUS_CODES: usize = 600;

/// Process-lifetime totals (they back /metrics); a run's numbers are the
/// difference from a snapshot taken when it started
pub struct Stats {
    requests: AtomicU64,
    records: AtomicU64,
    wire_bytes: AtomicU64,
    raw_bytes: AtomicU64,
    accepted_records: AtomicU64,
    rejected_records: AtomicU64,
    failed_records: AtomicU64,
    network_errors: AtomicU64,
    behind: AtomicU64,
    codes: Vec<AtomicU64>,
    /// One more than the bounds: the last counts everything slower
    latency: Vec<AtomicU64>,
    latency_micros: AtomicU64,
    pub in_flight: AtomicI64,
}

impl Default for Stats {
    fn default() -> Self {
        let zeros = |n| (0..n).map(|_| AtomicU64::new(0)).collect();
        Stats {
            requests: AtomicU64::new(0),
            records: AtomicU64::new(0),
            wire_bytes: AtomicU64::new(0),
            raw_bytes: AtomicU64::new(0),
            accepted_records: AtomicU64::new(0),
            rejected_records: AtomicU64::new(0),
            failed_records: AtomicU64::new(0),
            network_errors: AtomicU64::new(0),
            behind: AtomicU64::new(0),
            codes: zeros(STATUS_CODES),
            latency: zeros(latency_bounds().len() + 1),
            latency_micros: AtomicU64::new(0),
            in_flight: AtomicI64::new(0),
        }
    }
}

impl Stats {
    pub fn sent(&self, records: u32, wire_bytes: usize, raw_bytes: usize) {
        self.requests.fetch_add(1, Relaxed);
        self.records.fetch_add(u64::from(records), Relaxed);
        self.wire_bytes.fetch_add(wire_bytes as u64, Relaxed);
        self.raw_bytes.fetch_add(raw_bytes as u64, Relaxed);
    }

    pub fn response(&self, status: u16, records: u32, latency: Duration) {
        if let Some(counter) = self.codes.get(usize::from(status)) {
            counter.fetch_add(1, Relaxed);
        }
        let outcome = if (200..300).contains(&status) { &self.accepted_records } else { &self.rejected_records };
        outcome.fetch_add(u64::from(records), Relaxed);
        self.observe(latency);
    }

    pub fn network_error(&self, records: u32, latency: Duration) {
        self.network_errors.fetch_add(1, Relaxed);
        self.failed_records.fetch_add(u64::from(records), Relaxed);
        self.observe(latency);
    }

    /// Ticks that found the in-flight limit reached, or that the generator
    /// could not keep up with
    pub fn behind(&self, ticks: u64) {
        self.behind.fetch_add(ticks, Relaxed);
    }

    fn observe(&self, latency: Duration) {
        let seconds = latency.as_secs_f64();
        let bucket = latency_bounds().partition_point(|bound| *bound < seconds);
        self.latency[bucket].fetch_add(1, Relaxed);
        self.latency_micros.fetch_add(latency.as_micros() as u64, Relaxed);
    }

    pub fn snapshot(&self) -> Counters {
        let mut codes = BTreeMap::new();
        for (code, counter) in self.codes.iter().enumerate() {
            let n = counter.load(Relaxed);
            if n > 0 {
                codes.insert(code as u16, n);
            }
        }
        Counters {
            requests: self.requests.load(Relaxed),
            records: self.records.load(Relaxed),
            wire_bytes: self.wire_bytes.load(Relaxed),
            raw_bytes: self.raw_bytes.load(Relaxed),
            accepted_records: self.accepted_records.load(Relaxed),
            rejected_records: self.rejected_records.load(Relaxed),
            failed_records: self.failed_records.load(Relaxed),
            network_errors: self.network_errors.load(Relaxed),
            behind: self.behind.load(Relaxed),
            codes,
            latency: self.latency.iter().map(|c| c.load(Relaxed)).collect(),
            latency_seconds: self.latency_micros.load(Relaxed) as f64 / 1e6,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Counters {
    pub requests: u64,
    pub records: u64,
    pub wire_bytes: u64,
    pub raw_bytes: u64,
    pub accepted_records: u64,
    pub rejected_records: u64,
    pub failed_records: u64,
    pub network_errors: u64,
    pub behind: u64,
    /// Responses by HTTP status code
    pub codes: BTreeMap<u16, u64>,
    /// Counts per latency bucket (latency_bounds, then the overflow bucket)
    pub latency: Vec<u64>,
    pub latency_seconds: f64,
}

impl Counters {
    /// `self - base`, for counters that only grow
    pub fn since(&self, base: &Counters) -> Counters {
        let mut codes = self.codes.clone();
        for (code, n) in &base.codes {
            if let Some(v) = codes.get_mut(code) {
                *v = v.saturating_sub(*n);
            }
        }
        codes.retain(|_, n| *n > 0);
        Counters {
            requests: self.requests.saturating_sub(base.requests),
            records: self.records.saturating_sub(base.records),
            wire_bytes: self.wire_bytes.saturating_sub(base.wire_bytes),
            raw_bytes: self.raw_bytes.saturating_sub(base.raw_bytes),
            accepted_records: self.accepted_records.saturating_sub(base.accepted_records),
            rejected_records: self.rejected_records.saturating_sub(base.rejected_records),
            failed_records: self.failed_records.saturating_sub(base.failed_records),
            network_errors: self.network_errors.saturating_sub(base.network_errors),
            behind: self.behind.saturating_sub(base.behind),
            codes,
            latency: self
                .latency
                .iter()
                .enumerate()
                .map(|(i, n)| n.saturating_sub(base.latency.get(i).copied().unwrap_or(0)))
                .collect(),
            latency_seconds: (self.latency_seconds - base.latency_seconds).max(0.0),
        }
    }

    pub fn add(&mut self, other: &Counters) {
        self.requests += other.requests;
        self.records += other.records;
        self.wire_bytes += other.wire_bytes;
        self.raw_bytes += other.raw_bytes;
        self.accepted_records += other.accepted_records;
        self.rejected_records += other.rejected_records;
        self.failed_records += other.failed_records;
        self.network_errors += other.network_errors;
        self.behind += other.behind;
        for (code, n) in &other.codes {
            *self.codes.entry(*code).or_default() += n;
        }
        if self.latency.len() < other.latency.len() {
            self.latency.resize(other.latency.len(), 0);
        }
        for (i, n) in other.latency.iter().enumerate() {
            self.latency[i] += n;
        }
        self.latency_seconds += other.latency_seconds;
    }

    pub fn responses(&self) -> u64 {
        self.latency.iter().sum()
    }

    /// Upper bound of the bucket holding quantile `q` (None without responses)
    pub fn latency_quantile(&self, q: f64) -> Option<f64> {
        let total = self.responses();
        if total == 0 {
            return None;
        }
        let rank = (q * total as f64).ceil().max(1.0) as u64;
        let bounds = latency_bounds();
        let mut seen = 0;
        for (i, n) in self.latency.iter().enumerate() {
            seen += n;
            if seen >= rank {
                return Some(bounds.get(i).copied().unwrap_or(f64::INFINITY));
            }
        }
        None
    }
}

/// Per-second rates over a window, as the UI shows them
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Rates {
    pub records_per_second: f64,
    pub accepted_records_per_second: f64,
    pub requests_per_second: f64,
    pub wire_bytes_per_second: f64,
    pub raw_bytes_per_second: f64,
    /// Requests answered with something other than 2xx, or not answered
    pub failed_requests_per_second: f64,
    pub behind_per_second: f64,
    pub latency_p50_seconds: Option<f64>,
    pub latency_p99_seconds: Option<f64>,
}

impl Rates {
    pub fn from_window(delta: &Counters, seconds: f64) -> Rates {
        if seconds <= 0.0 {
            return Rates::default();
        }
        let ok: u64 = delta.codes.iter().filter(|(c, _)| (200..300).contains(*c)).map(|(_, n)| n).sum();
        let failed = delta.responses().saturating_sub(ok);
        Rates {
            records_per_second: delta.records as f64 / seconds,
            accepted_records_per_second: delta.accepted_records as f64 / seconds,
            requests_per_second: delta.requests as f64 / seconds,
            wire_bytes_per_second: delta.wire_bytes as f64 / seconds,
            raw_bytes_per_second: delta.raw_bytes as f64 / seconds,
            failed_requests_per_second: failed as f64 / seconds,
            behind_per_second: delta.behind as f64 / seconds,
            latency_p50_seconds: delta.latency_quantile(0.5),
            latency_p99_seconds: delta.latency_quantile(0.99),
        }
    }
}

/// Recent snapshots, sampled once a second
pub struct Window {
    span: Duration,
    samples: VecDeque<(Instant, Counters)>,
}

impl Window {
    pub fn new(span: Duration) -> Self {
        Window { span, samples: VecDeque::new() }
    }

    pub fn push(&mut self, at: Instant, counters: Counters) {
        self.samples.push_back((at, counters));
        while self.samples.len() > 2 && at.duration_since(self.samples[1].0) >= self.span {
            self.samples.pop_front();
        }
    }

    /// The change across the window and its length in seconds
    pub fn delta(&self) -> (Counters, f64) {
        match (self.samples.front(), self.samples.back()) {
            (Some((t0, first)), Some((t1, last))) if t1 > t0 => {
                (last.since(first), t1.duration_since(*t0).as_secs_f64())
            }
            _ => (Counters::default(), 0.0),
        }
    }

    pub fn clear(&mut self) {
        self.samples.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_and_differences() {
        let stats = Stats::default();
        let base = stats.snapshot();
        stats.sent(100, 1000, 3000);
        stats.response(200, 100, Duration::from_millis(3));
        stats.sent(100, 1000, 3000);
        stats.response(503, 100, Duration::from_millis(40));
        stats.sent(50, 500, 1500);
        stats.network_error(50, Duration::from_secs(30));
        stats.behind(4);
        let run = stats.snapshot().since(&base);
        assert_eq!((run.requests, run.records, run.wire_bytes, run.raw_bytes), (3, 250, 2500, 7500));
        assert_eq!((run.accepted_records, run.rejected_records, run.failed_records), (100, 100, 50));
        assert_eq!((run.network_errors, run.behind), (1, 4));
        assert_eq!(run.codes, BTreeMap::from([(200, 1), (503, 1)]));
        assert_eq!(run.responses(), 3);

        let rates = Rates::from_window(&run, 2.0);
        assert_eq!(rates.records_per_second, 125.0);
        assert_eq!(rates.failed_requests_per_second, 1.0);
        let p50 = rates.latency_p50_seconds.unwrap();
        assert!((0.04..0.06).contains(&p50), "{p50}");
        assert!(rates.latency_p99_seconds.unwrap() >= 30.0);
    }

    #[test]
    fn quantiles_are_bucket_upper_bounds() {
        let stats = Stats::default();
        for _ in 0..99 {
            stats.response(200, 1, Duration::from_micros(400));
        }
        stats.response(200, 1, Duration::from_millis(100));
        let counters = stats.snapshot();
        assert_eq!(counters.latency_quantile(0.5), Some(0.0005));
        assert!(counters.latency_quantile(1.0).unwrap() >= 0.1);
        assert_eq!(Counters::default().latency_quantile(0.5), None);
    }

    #[test]
    fn adding_merges_codes_and_buckets() {
        let a = Stats::default();
        a.response(200, 10, Duration::from_millis(1));
        let b = Stats::default();
        b.response(200, 5, Duration::from_millis(1));
        b.response(429, 5, Duration::from_millis(1));
        let mut sum = a.snapshot();
        sum.add(&b.snapshot());
        assert_eq!(sum.codes, BTreeMap::from([(200, 2), (429, 1)]));
        assert_eq!(sum.responses(), 3);
        assert_eq!(sum.accepted_records, 15);
    }

    #[test]
    fn window_keeps_its_span() {
        let start = Instant::now();
        let mut window = Window::new(Duration::from_secs(5));
        for second in 0..10u64 {
            let counters = Counters { records: second * 100, ..Counters::default() };
            window.push(start + Duration::from_secs(second), counters);
        }
        let (delta, seconds) = window.delta();
        assert_eq!(seconds, 5.0);
        assert_eq!(delta.records, 500);
    }
}
