//! Prometheus text metrics (same style as the appender's `/metrics`)

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::source::TableSource;

#[derive(Default)]
pub struct Metrics {
    search: Endpoint,
    sql: Endpoint,
    pub in_flight: AtomicU64,
    pub rejected: AtomicU64,
}

#[derive(Default)]
struct Endpoint {
    ok: AtomicU64,
    client_errors: AtomicU64,
    server_errors: AtomicU64,
    rows: AtomicU64,
    micros: AtomicU64,
}

#[derive(Clone, Copy)]
pub enum Kind {
    Search,
    Sql,
}

impl Metrics {
    fn endpoint(&self, kind: Kind) -> &Endpoint {
        match kind {
            Kind::Search => &self.search,
            Kind::Sql => &self.sql,
        }
    }

    pub fn record(&self, kind: Kind, status: u16, rows: usize, elapsed: Duration) {
        let e = self.endpoint(kind);
        match status {
            ..400 => &e.ok,
            400..500 => &e.client_errors,
            _ => &e.server_errors,
        }
        .fetch_add(1, Ordering::Relaxed);
        e.rows.fetch_add(rows as u64, Ordering::Relaxed);
        e.micros.fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
    }

    pub fn render(&self, source: &dyn TableSource) -> String {
        let mut out = String::new();
        for (name, kind) in [("search", Kind::Search), ("sql", Kind::Sql)] {
            let e = self.endpoint(kind);
            for (outcome, counter) in
                [("ok", &e.ok), ("client_error", &e.client_errors), ("server_error", &e.server_errors)]
            {
                let _ = writeln!(
                    out,
                    "query_requests_total{{endpoint=\"{name}\",outcome=\"{outcome}\"}} {}",
                    counter.load(Ordering::Relaxed)
                );
            }
            let _ =
                writeln!(out, "query_rows_returned_total{{endpoint=\"{name}\"}} {}", e.rows.load(Ordering::Relaxed));
            let _ = writeln!(
                out,
                "query_duration_seconds_sum{{endpoint=\"{name}\"}} {:.6}",
                e.micros.load(Ordering::Relaxed) as f64 / 1e6
            );
        }
        let _ = writeln!(out, "query_in_flight {}", self.in_flight.load(Ordering::Relaxed));
        let _ = writeln!(out, "query_rejected_total {}", self.rejected.load(Ordering::Relaxed));
        let _ = writeln!(out, "query_table_ready {}", u8::from(source.is_ready()));
        let _ = writeln!(out, "query_table_refresh_failures_total {}", source.refresh_failures());
        if let Some(age) = source.age_seconds() {
            let _ = writeln!(out, "query_table_metadata_age_seconds {age:.3}");
        }
        out
    }
}
