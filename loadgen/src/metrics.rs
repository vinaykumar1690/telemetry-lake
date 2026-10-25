//! Prometheus text exposition of a node's process-lifetime counters

use std::fmt::Write;
use std::sync::atomic::Ordering::Relaxed;

use crate::controller::{Controller, RunState};
use crate::node::{Node, NodeState};
use crate::stats::latency_bounds;

fn family(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
}

pub fn render(node: Option<&Node>, controller: Option<&Controller>) -> String {
    let mut out = String::new();
    family(&mut out, "loadgen_build_info", "gauge", "Version of otel-loadgen");
    let _ = writeln!(out, "loadgen_build_info{{version=\"{}\"}} 1", env!("CARGO_PKG_VERSION"));

    if let Some(controller) = controller {
        let report = controller.report();
        let running = report.run.as_ref().is_some_and(|r| r.state == RunState::Running);
        family(&mut out, "loadgen_run_active", "gauge", "1 while a run is going");
        let _ = writeln!(out, "loadgen_run_active {}", u8::from(running));
        family(&mut out, "loadgen_target_records_per_second", "gauge", "Target rate of the current run");
        let target = report.run.as_ref().filter(|_| running).map_or(0, |r| r.spec.records_per_second);
        let _ = writeln!(out, "loadgen_target_records_per_second {target}");
        family(&mut out, "loadgen_workers_up", "gauge", "Workers answering the controller");
        let up = report.workers.iter().filter(|w| w.up && w.address != "local").count();
        let _ = writeln!(out, "loadgen_workers_up {up}");
    }

    let Some(node) = node else { return out };
    let c = node.stats().snapshot();
    let report = node.report();
    family(&mut out, "loadgen_node_running", "gauge", "1 while this node sends load");
    let _ = writeln!(out, "loadgen_node_running {}", u8::from(report.state == NodeState::Running));
    family(&mut out, "loadgen_requests_total", "counter", "Requests sent to the ingester");
    let _ = writeln!(out, "loadgen_requests_total {}", c.requests);
    family(&mut out, "loadgen_responses_total", "counter", "Responses from the ingester by status code");
    for (code, n) in &c.codes {
        let _ = writeln!(out, "loadgen_responses_total{{code=\"{code}\"}} {n}");
    }
    family(
        &mut out,
        "loadgen_records_total",
        "counter",
        "Log records by outcome (sent, accepted: 2xx, rejected: other status, failed: no response)",
    );
    for (outcome, n) in [
        ("sent", c.records),
        ("accepted", c.accepted_records),
        ("rejected", c.rejected_records),
        ("failed", c.failed_records),
    ] {
        let _ = writeln!(out, "loadgen_records_total{{outcome=\"{outcome}\"}} {n}");
    }
    family(
        &mut out,
        "loadgen_sent_bytes_total",
        "counter",
        "Request bytes sent (wire: as sent, raw: before compression)",
    );
    let _ = writeln!(out, "loadgen_sent_bytes_total{{encoding=\"wire\"}} {}", c.wire_bytes);
    let _ = writeln!(out, "loadgen_sent_bytes_total{{encoding=\"raw\"}} {}", c.raw_bytes);
    family(
        &mut out,
        "loadgen_network_errors_total",
        "counter",
        "Requests without a response (connect error, reset, timeout)",
    );
    let _ = writeln!(out, "loadgen_network_errors_total {}", c.network_errors);
    family(
        &mut out,
        "loadgen_behind_ticks_total",
        "counter",
        "Scheduled requests not sent: in-flight limit reached or the generator could not keep up",
    );
    let _ = writeln!(out, "loadgen_behind_ticks_total {}", c.behind);
    family(&mut out, "loadgen_in_flight_requests", "gauge", "Requests awaiting a response");
    let _ = writeln!(out, "loadgen_in_flight_requests {}", node.stats().in_flight.load(Relaxed));
    family(&mut out, "loadgen_target_node_records_per_second", "gauge", "This node's share of the target rate");
    let share = if report.state == NodeState::Running { report.records_per_second } else { 0.0 };
    let _ = writeln!(out, "loadgen_target_node_records_per_second {share}");

    family(&mut out, "loadgen_request_duration_seconds", "histogram", "Time to a response (or to the failure)");
    let mut cumulative = 0;
    for (bound, n) in latency_bounds().iter().zip(&c.latency) {
        cumulative += n;
        let _ = writeln!(out, "loadgen_request_duration_seconds_bucket{{le=\"{bound}\"}} {cumulative}");
    }
    let total = c.responses();
    let _ = writeln!(out, "loadgen_request_duration_seconds_bucket{{le=\"+Inf\"}} {total}");
    let _ = writeln!(out, "loadgen_request_duration_seconds_sum {}", c.latency_seconds);
    let _ = writeln!(out, "loadgen_request_duration_seconds_count {total}");
    out
}
