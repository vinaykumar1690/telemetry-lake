import { useEffect, useMemo, useState } from "react";
import {
  type Estimate,
  type Report,
  type RunInfo,
  estimate as fetchEstimate,
  getReport,
  startRun,
  stopRun,
  updateRun,
} from "../api/loadgen";
import { NumberInput, SeverityWeightsEditor, ServicesEditor } from "../forms";
import type { SearchState } from "../search/types";
import { DEFAULT_STATE } from "../search/types";
import {
  DEFAULT_FORM,
  type LoadTestForm,
  cardinality,
  formatBytes,
  formatCount,
  formatElapsed,
  formatLatency,
  podsOverRun,
  searchWindow,
  toRunSpec,
  validate,
} from "./spec";

interface Props {
  /** The tab is showing: poll every second instead of every ten */
  active: boolean;
  onSearch: (state: SearchState) => void;
}

/** Polls the service's report */
function useReport(active: boolean): { report: Report | null; error: string | null; refresh: () => void } {
  const [report, setReport] = useState<Report | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [token, setToken] = useState(0);
  useEffect(() => {
    let stopped = false;
    const controller = new AbortController();
    const poll = async () => {
      try {
        const next = await getReport(controller.signal);
        if (!stopped) {
          setReport(next);
          setError(null);
        }
      } catch (e) {
        if (!stopped) setError((e as Error).message);
      }
    };
    void poll();
    const timer = setInterval(poll, active ? 1000 : 10_000);
    return () => {
      stopped = true;
      controller.abort();
      clearInterval(timer);
    };
  }, [token, active]);
  return { report, error, refresh: () => setToken((t) => t + 1) };
}

function Stat({ label, value, sub, tone }: { label: string; value: string; sub?: string; tone?: "warn" | "error" }) {
  return (
    <div className="stat">
      <div className="stat-label">{label}</div>
      <div className={`stat-value ${tone ? `${tone}-text` : ""}`}>{value}</div>
      {sub && <div className="stat-sub muted">{sub}</div>}
    </div>
  );
}

export function LoadTestPage({ active, onSearch }: Props) {
  const [form, setForm] = useState<LoadTestForm>(DEFAULT_FORM);
  const set = (patch: Partial<LoadTestForm>) => setForm((f) => ({ ...f, ...patch }));
  const spec = useMemo(() => toRunSpec(form), [form]);
  const problem = validate(spec);
  const { report, error: reportError, refresh } = useReport(active);
  const [estimate, setEstimate] = useState<Estimate | null>(null);
  const [estimateError, setEstimateError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [liveRate, setLiveRate] = useState(form.recordsPerSecond);
  const [liveInFlight, setLiveInFlight] = useState(form.maxInFlight);

  const specKey = JSON.stringify(spec);
  useEffect(() => {
    if (problem) {
      setEstimate(null);
      setEstimateError(null);
      return;
    }
    const controller = new AbortController();
    const timer = setTimeout(async () => {
      try {
        setEstimate(await fetchEstimate(spec, controller.signal));
        setEstimateError(null);
      } catch (e) {
        if (!controller.signal.aborted) {
          setEstimate(null);
          setEstimateError((e as Error).message);
        }
      }
    }, 400);
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [specKey, problem]);

  const run = report?.run ?? null;
  const running = run?.state === "running";

  const act = async (action: () => Promise<unknown>) => {
    setBusy(true);
    setActionError(null);
    try {
      await action();
      refresh();
    } catch (e) {
      setActionError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const start = () =>
    act(async () => {
      const started = await startRun(spec);
      setLiveRate(started.spec.records_per_second);
      setLiveInFlight(started.spec.max_in_flight);
    });

  const searchRun = (r: RunInfo) =>
    onSearch({
      ...DEFAULT_STATE,
      range: { kind: "absolute", ...searchWindow(r, Date.now()) },
      attrs: [{ field: "attr.generator.run_id", op: "=", value: r.id }],
    });

  const setExtra = (i: number, patch: Partial<LoadTestForm["extraAttributes"][number]>) =>
    set({ extraAttributes: form.extraAttributes.map((a, j) => (j === i ? { ...a, ...patch } : a)) });

  const card = cardinality(spec);
  const rates = report?.rates;
  const totals = report?.totals;
  const target = run?.spec.records_per_second ?? 0;
  const achieved = rates?.records_per_second ?? 0;
  const elapsedMs = run ? (run.ended_ms ?? Date.now()) - run.started_ms : 0;
  const failedRecords = totals ? totals.rejected_records + totals.failed_records : 0;
  const codes = Object.entries(totals?.codes ?? {}).sort(([a], [b]) => Number(a) - Number(b));

  return (
    <div className="generator loadtest">
      <p className="lede">
        Sends a sustained rate of OTLP logs from the load generator service (<code>otel-loadgen</code>) straight to the ingester; this
        page only configures and watches it. Records are stamped with the current time and carry a <code>generator.run_id</code>{" "}
        attribute.
      </p>
      <div className="gen-grid">
        <section className="card">
          <h2>Rate and requests</h2>
          <div className="form-grid">
            <label>
              Records per second
              <NumberInput value={form.recordsPerSecond} min={1} step={1000} onChange={(n) => set({ recordsPerSecond: n })} />
            </label>
            <label>
              Duration (s, 0 = until stopped)
              <NumberInput value={form.durationSeconds} min={0} step={60} onChange={(n) => set({ durationSeconds: n })} />
            </label>
            <label>
              Records per request
              <NumberInput value={form.recordsPerRequest} min={1} max={50000} step={100} onChange={(n) => set({ recordsPerRequest: n })} />
            </label>
            <label>
              Max requests in flight
              <NumberInput value={form.maxInFlight} min={1} step={8} onChange={(n) => set({ maxInFlight: n })} />
            </label>
            <label>
              Body padding (bytes)
              <NumberInput value={form.bodyPaddingBytes} min={0} max={65536} step={256} onChange={(n) => set({ bodyPaddingBytes: n })} />
            </label>
            <label>
              Seed (pod names)
              <NumberInput value={form.seed} onChange={(n) => set({ seed: n })} />
            </label>
          </div>
          <div className="form-row">
            <div className="segmented" role="group" aria-label="Wire format">
              <button className={form.wire === "protobuf" ? "active" : ""} onClick={() => set({ wire: "protobuf" })}>
                Protobuf
              </button>
              <button className={form.wire === "json" ? "active" : ""} onClick={() => set({ wire: "json" })}>
                JSON
              </button>
            </div>
            <label className="check">
              <input type="checkbox" checked={form.gzip} onChange={(e) => set({ gzip: e.target.checked })} /> gzip
            </label>
          </div>
          <h3>Severity mix</h3>
          <SeverityWeightsEditor weights={form.weights} setWeights={(update) => setForm((f) => ({ ...f, weights: update(f.weights) }))} />
        </section>

        <section className="card">
          <h2>Cardinality</h2>
          <ServicesEditor
            services={form.services}
            setServices={(update) => setForm((f) => ({ ...f, services: update(f.services) }))}
            maxPods={100000}
          />
          <div className="form-grid">
            <label>
              Nodes (host.name)
              <NumberInput value={form.nodes} min={1} onChange={(n) => set({ nodes: n })} />
            </label>
            <label>
              Distinct user.id
              <NumberInput value={form.users} min={1} step={1000} onChange={(n) => set({ users: n })} />
            </label>
            <label>
              Pod churn (min, 0 = none)
              <NumberInput value={form.podChurnMinutes} min={0} onChange={(n) => set({ podChurnMinutes: n })} />
            </label>
            <label>
              service.namespace
              <input value={form.serviceNamespace} onChange={(e) => set({ serviceNamespace: e.target.value })} />
            </label>
            <label>
              k8s.namespace.name
              <input value={form.k8sNamespace} onChange={(e) => set({ k8sNamespace: e.target.value })} />
            </label>
            <label>
              deployment.environment.name
              <input value={form.environment} onChange={(e) => set({ environment: e.target.value })} />
            </label>
          </div>
          <h3>Extra record attributes</h3>
          {form.extraAttributes.length > 0 && (
            <table className="services">
              <thead>
                <tr>
                  <th>Key</th>
                  <th>Distinct values</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {form.extraAttributes.map((a, i) => (
                  <tr key={i}>
                    <td>
                      <input value={a.key} onChange={(e) => setExtra(i, { key: e.target.value })} />
                    </td>
                    <td>
                      <NumberInput value={a.distinctValues} min={1} step={100} onChange={(n) => setExtra(i, { distinctValues: n })} />
                    </td>
                    <td>
                      <button
                        className="ghost small"
                        aria-label="Remove attribute"
                        onClick={() => set({ extraAttributes: form.extraAttributes.filter((_, j) => j !== i) })}
                      >
                        ✕
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          <button
            className="ghost small"
            onClick={() =>
              set({ extraAttributes: [...form.extraAttributes, { key: `attr.${form.extraAttributes.length + 1}`, distinctValues: 100 }] })
            }
          >
            + Attribute
          </button>
          <p className="muted small-text">
            {card.pods.toLocaleString()} pods on {card.hosts.toLocaleString()} hosts: {card.resourceSeries.toLocaleString()} resource
            attribute sets
            {spec.pod_churn_minutes
              ? `, replaced every ${spec.pod_churn_minutes} min (${podsOverRun(spec, form.durationSeconds || 3600).toLocaleString()} over ${form.durationSeconds ? "the run" : "an hour"})`
              : ""}
            . Per record: {card.recordAttributes.map((a) => `${a.key} ${formatCount(a.values)}`).join(", ")} distinct values.
          </p>
        </section>
      </div>

      <section className="card run-card">
        <div className="run-summary">
          {problem ? (
            <span className="error-text">{problem}</span>
          ) : estimateError ? (
            <span className="error-text">{estimateError}</span>
          ) : estimate ? (
            <span>
              {formatCount(estimate.requests_per_second)} requests/s of {formatBytes(estimate.request_wire_bytes)}
              {form.gzip ? ` (${formatBytes(estimate.request_bytes)} raw)` : ""} ≈ <strong>{formatBytes(estimate.wire_bytes_per_second)}/s</strong>{" "}
              on the wire
              {form.gzip ? `, ${formatBytes(estimate.raw_bytes_per_second)}/s raw` : ""}
            </span>
          ) : (
            <span className="muted">{reportError ?? "estimating…"}</span>
          )}
          <span className="spacer" />
          {running ? (
            <button className="danger" disabled={busy} onClick={() => void act(stopRun)}>
              Stop
            </button>
          ) : (
            <button className="primary" disabled={!!problem || !!estimateError || busy || !!reportError} onClick={() => void start()}>
              Start load
            </button>
          )}
        </div>
        {actionError && <div className="error-text small-text">{actionError}</div>}
        {reportError && <div className="warn-text small-text">Load generator: {reportError}</div>}

        {run && rates && totals && (
          <div className="run-progress">
            <div className="run-stats">
              <span>
                <strong>{running ? "Running" : run.state === "finished" ? "Finished" : "Stopped"}</strong> · {formatElapsed(elapsedMs)}
                {run.spec.duration_seconds ? ` of ${formatElapsed(run.spec.duration_seconds * 1000)}` : ""}
              </span>
              <span className="muted mono">{run.id}</span>
              <span className="muted">
                {report.mode === "workers" ? `${report.workers.filter((w) => w.up).length} workers` : "sent by the controller"}
              </span>
              <span className="spacer" />
              <button className="ghost small" onClick={() => searchRun(run)}>
                Search this run
              </button>
            </div>

            {running && (
              <div className="form-row">
                <label className="inline-field">
                  Records/s
                  <NumberInput value={liveRate} min={1} step={1000} onChange={setLiveRate} ariaLabel="Live records per second" />
                </label>
                <label className="inline-field">
                  In flight
                  <NumberInput value={liveInFlight} min={1} step={8} onChange={setLiveInFlight} ariaLabel="Live requests in flight" />
                </label>
                <button
                  className="small"
                  disabled={busy || (liveRate === run.spec.records_per_second && liveInFlight === run.spec.max_in_flight)}
                  onClick={() => void act(() => updateRun({ records_per_second: Math.floor(liveRate), max_in_flight: Math.floor(liveInFlight) }))}
                >
                  Apply
                </button>
              </div>
            )}

            <div className="stats-grid">
              <Stat
                label="Records/s"
                value={formatCount(achieved)}
                sub={running ? `target ${formatCount(target)}` : undefined}
                tone={running && target > 0 && achieved < target * 0.9 ? "warn" : undefined}
              />
              <Stat label="Accepted records/s" value={formatCount(rates.accepted_records_per_second)} />
              <Stat label="Requests/s" value={formatCount(rates.requests_per_second)} />
              <Stat
                label="Wire bytes/s"
                value={`${formatBytes(rates.wire_bytes_per_second)}/s`}
                sub={run.spec.compression === "gzip" ? `${formatBytes(rates.raw_bytes_per_second)}/s raw` : undefined}
              />
              <Stat label="Latency p50 / p99" value={formatLatency(rates.latency_p50_seconds)} sub={formatLatency(rates.latency_p99_seconds)} />
              <Stat
                label="Failed requests/s"
                value={formatCount(rates.failed_requests_per_second)}
                tone={rates.failed_requests_per_second > 0 ? "error" : undefined}
              />
              <Stat
                label="Behind/s"
                value={formatCount(rates.behind_per_second)}
                sub={`${report.in_flight} in flight`}
                tone={rates.behind_per_second > 0 ? "warn" : undefined}
              />
            </div>
            <div className="run-stats">
              <span>
                <strong>{totals.records.toLocaleString()}</strong> records sent ({formatBytes(totals.wire_bytes)})
              </span>
              <span>{totals.accepted_records.toLocaleString()} accepted</span>
              {failedRecords > 0 && <span className="error-text">{failedRecords.toLocaleString()} rejected or failed</span>}
              {totals.network_errors > 0 && <span className="error-text">{totals.network_errors.toLocaleString()} network errors</span>}
              {totals.behind > 0 && <span className="warn-text">{totals.behind.toLocaleString()} requests behind schedule</span>}
              {codes.length > 0 && (
                <span className="muted">
                  HTTP {codes.map(([code, n]) => `${code}: ${n.toLocaleString()}`).join(" · ")}
                </span>
              )}
            </div>
            {rates.behind_per_second > 0 && (
              <div className="muted small-text">
                Behind means scheduled requests were not sent: every allowed request was still in flight (raise the in-flight limit, or
                the ingester is the bottleneck), or the generator ran out of CPU (add workers).
              </div>
            )}

            {report.workers.length > 0 && report.mode === "workers" && (
              <div className="loadtest-scroll">
              <table className="services workers">
                <thead>
                  <tr>
                    <th>Worker</th>
                    <th>State</th>
                    <th>Share (records/s)</th>
                    <th>Records/s</th>
                    <th>Wire/s</th>
                    <th>p99</th>
                    <th>In flight</th>
                    <th>Behind/s</th>
                  </tr>
                </thead>
                <tbody>
                  {report.workers.map((w) => (
                    <tr key={w.address} className={w.up ? "" : "muted"}>
                      <td className="mono">{w.address}</td>
                      <td>{w.up ? w.state ?? "–" : <span className="error-text" title={w.error ?? ""}>gone</span>}</td>
                      <td>{formatCount(w.records_per_second)}</td>
                      <td>{formatCount(w.rates.records_per_second)}</td>
                      <td>{formatBytes(w.rates.wire_bytes_per_second)}/s</td>
                      <td>{formatLatency(w.rates.latency_p99_seconds)}</td>
                      <td>{w.in_flight}</td>
                      <td>{formatCount(w.rates.behind_per_second)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              </div>
            )}
            {!running && totals.accepted_records > 0 && (
              <div className="muted small-text">
                Accepted records become searchable after the appender's next commit (up to <code>appender.buffer.timeSeconds</code>).
              </div>
            )}
          </div>
        )}
      </section>
    </div>
  );
}
