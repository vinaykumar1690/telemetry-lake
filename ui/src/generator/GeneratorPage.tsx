import { useMemo, useRef, useState } from "react";
import type { SearchState } from "../search/types";
import { DEFAULT_STATE } from "../search/types";
import { fromLocalInput, toLocalInput } from "../time";
import {
  DEFAULT_SERVICES,
  DEFAULT_WEIGHTS,
  type GeneratorConfig,
  SEVERITIES,
  type ServiceSpec,
  type Severity,
  batchCount,
  generateBatches,
  newRunId,
  validate,
} from "./generate";
import { type Progress, sendBatches } from "./send";

interface Props {
  utc: boolean;
  onSearch: (state: SearchState) => void;
}

type RangeMode = "last" | "absolute";

interface Run {
  config: GeneratorConfig;
  progress: Progress;
  state: "running" | "done" | "cancelled";
}

const MAX_SEARCH_HOURS = 168;

function NumberInput({ value, onChange, min = 0, max, step = 1 }: { value: number; onChange: (n: number) => void; min?: number; max?: number; step?: number }) {
  return (
    <input
      type="number"
      value={Number.isFinite(value) ? value : ""}
      min={min}
      max={max}
      step={step}
      onChange={(e) => onChange(e.target.valueAsNumber)}
    />
  );
}

export function GeneratorPage({ utc, onSearch }: Props) {
  const [services, setServices] = useState<ServiceSpec[]>(DEFAULT_SERVICES);
  const [serviceNamespace, setServiceNamespace] = useState("shop");
  const [k8sNamespace, setK8sNamespace] = useState("shop");
  const [environment, setEnvironment] = useState("dev");
  const [nodes, setNodes] = useState(3);
  const [mode, setMode] = useState<RangeMode>("last");
  const [lastHours, setLastHours] = useState(6);
  const now = useMemo(() => Date.now(), []);
  const [start, setStart] = useState(() => toLocalInput(now - 6 * 3600_000, utc));
  const [end, setEnd] = useState(() => toLocalInput(now, utc));
  const [total, setTotal] = useState(10_000);
  const [batchSize, setBatchSize] = useState(1000);
  const [concurrency, setConcurrency] = useState(2);
  const [weights, setWeights] = useState<Record<Severity, number>>(DEFAULT_WEIGHTS);
  const [seed, setSeed] = useState(() => Math.floor(Math.random() * 1e9));
  const [run, setRun] = useState<Run | null>(null);
  const [showPreview, setShowPreview] = useState(false);
  const abort = useRef<AbortController | null>(null);

  /** The config as of now: "last N hours" ends at the moment of the call */
  const configAt = (at: number, runId: string): GeneratorConfig => {
    const [startMs, endMs] =
      mode === "last" ? [at - lastHours * 3600_000, at] : [fromLocalInput(start, utc), fromLocalInput(end, utc)];
    return {
      services: services.map((s) => ({ name: s.name.trim(), pods: Math.max(0, Math.floor(s.pods || 0)) })),
      serviceNamespace,
      k8sNamespace,
      environment,
      nodes: Math.max(1, Math.floor(nodes || 1)),
      startMs,
      endMs,
      total: Math.floor(total || 0),
      batchSize: Math.floor(batchSize || 0),
      severityWeights: weights,
      seed: Math.floor(seed || 0),
      runId,
    };
  };

  const draft = configAt(now, "run-preview");
  const problem = validate(draft);
  const rangeHours = (draft.endMs - draft.startMs) / 3600_000;
  const totalPods = draft.services.reduce((n, s) => n + s.pods, 0);
  const preview = useMemo(() => {
    if (!showPreview || problem) return "";
    const [batch] = generateBatches({ ...draft, total: 3, batchSize: 3 });
    return JSON.stringify(JSON.parse(batch.payload), null, 2);
  }, [showPreview, problem, JSON.stringify(draft)]);

  const running = run?.state === "running";

  const start_ = async () => {
    const config = configAt(Date.now(), newRunId());
    if (validate(config)) return;
    const controller = new AbortController();
    abort.current = controller;
    const empty: Progress = { batchesSent: 0, recordsSent: 0, batchesFailed: 0, recordsFailed: 0, retries: 0, elapsedMs: 0 };
    setRun({ config, progress: empty, state: "running" });
    const progress = await sendBatches(generateBatches(config), {
      concurrency: Math.max(1, Math.floor(concurrency || 1)),
      signal: controller.signal,
      onProgress: (p) => setRun((r) => (r ? { ...r, progress: p } : r)),
    });
    setRun({ config, progress, state: controller.signal.aborted ? "cancelled" : "done" });
  };

  const searchRun = (r: Run) =>
    onSearch({
      ...DEFAULT_STATE,
      range: { kind: "absolute", start: new Date(r.config.startMs).toISOString(), end: new Date(r.config.endMs + 1).toISOString() },
      attrs: [{ field: "attr.generator.run_id", op: "=", value: r.config.runId }],
    });

  const setService = (i: number, patch: Partial<ServiceSpec>) =>
    setServices((list) => list.map((s, j) => (j === i ? { ...s, ...patch } : s)));

  const weightSum = SEVERITIES.reduce((n, s) => n + Math.max(0, weights[s] || 0), 0);
  const p = run?.progress;
  const done = p ? p.recordsSent + p.recordsFailed : 0;
  const pct = run ? Math.min(100, (done / run.config.total) * 100) : 0;
  const rate = p && p.elapsedMs > 0 ? Math.round(p.recordsSent / (p.elapsedMs / 1000)) : 0;

  return (
    <div className="generator">
      <p className="lede">
        Builds OTLP/JSON log batches in the browser and POSTs them to the ingester's <code>/v1/logs</code>. Every record carries a{" "}
        <code>generator.run_id</code> attribute so a run can be found again.
      </p>
      <div className="gen-grid">
        <section className="card">
          <h2>Services and pods</h2>
          <table className="services">
            <thead>
              <tr>
                <th>service.name</th>
                <th>Pods</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {services.map((s, i) => (
                <tr key={i}>
                  <td>
                    <input value={s.name} onChange={(e) => setService(i, { name: e.target.value })} />
                  </td>
                  <td>
                    <NumberInput value={s.pods} min={1} max={100} onChange={(pods) => setService(i, { pods })} />
                  </td>
                  <td>
                    <button className="ghost small" aria-label="Remove service" onClick={() => setServices((l) => l.filter((_, j) => j !== i))}>
                      ✕
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <button className="ghost small" onClick={() => setServices((l) => [...l, { name: `service-${l.length + 1}`, pods: 2 }])}>
            + Service
          </button>
          <div className="form-grid">
            <label>
              service.namespace
              <input value={serviceNamespace} onChange={(e) => setServiceNamespace(e.target.value)} />
            </label>
            <label>
              k8s.namespace.name
              <input value={k8sNamespace} onChange={(e) => setK8sNamespace(e.target.value)} />
            </label>
            <label>
              deployment.environment.name
              <input value={environment} onChange={(e) => setEnvironment(e.target.value)} />
            </label>
            <label>
              Nodes (host.name)
              <NumberInput value={nodes} min={1} max={100} onChange={setNodes} />
            </label>
          </div>
        </section>

        <section className="card">
          <h2>Time range and volume</h2>
          <div className="segmented">
            <button className={mode === "last" ? "active" : ""} onClick={() => setMode("last")}>
              Last N hours
            </button>
            <button className={mode === "absolute" ? "active" : ""} onClick={() => setMode("absolute")}>
              Between
            </button>
          </div>
          {mode === "last" ? (
            <label className="inline">
              Hours before now
              <NumberInput value={lastHours} min={0.01} step={0.5} onChange={setLastHours} />
            </label>
          ) : (
            <div className="custom-range">
              <input type="datetime-local" step={1} value={start} onChange={(e) => setStart(e.target.value)} aria-label="Start" />
              <span className="muted">→</span>
              <input type="datetime-local" step={1} value={end} onChange={(e) => setEnd(e.target.value)} aria-label="End" />
            </div>
          )}
          <div className="form-grid">
            <label>
              Total logs
              <NumberInput value={total} min={1} step={1000} onChange={setTotal} />
            </label>
            <label>
              Records per request
              <NumberInput value={batchSize} min={1} max={20000} step={100} onChange={setBatchSize} />
            </label>
            <label>
              Requests in flight
              <NumberInput value={concurrency} min={1} max={16} onChange={setConcurrency} />
            </label>
            <label>
              Seed
              <span className="with-button">
                <NumberInput value={seed} onChange={setSeed} />
                <button className="ghost small" onClick={() => setSeed(Math.floor(Math.random() * 1e9))} title="New seed">
                  ↻
                </button>
              </span>
            </label>
          </div>
          <h3>Severity mix</h3>
          <div className="weights">
            {SEVERITIES.map((s) => (
              <label key={s}>
                <span className={`sev sev-${s.toLowerCase()}`}>{s}</span>
                <NumberInput value={weights[s]} min={0} onChange={(n) => setWeights((w) => ({ ...w, [s]: n }))} />
                <span className="muted small-text">{weightSum ? Math.round(((weights[s] || 0) / weightSum) * 100) : 0}%</span>
              </label>
            ))}
          </div>
        </section>
      </div>

      <section className="card run-card">
        <div className="run-summary">
          {problem ? (
            <span className="error-text">{problem}</span>
          ) : (
            <span>
              {draft.total.toLocaleString()} logs from {draft.services.length} services / {totalPods} pods over{" "}
              {rangeHours >= 48 ? `${(rangeHours / 24).toFixed(1)} days` : `${rangeHours.toFixed(rangeHours < 10 ? 2 : 1)} h`} in{" "}
              {batchCount(draft).toLocaleString()} requests
            </span>
          )}
          {!problem && rangeHours > MAX_SEARCH_HOURS && (
            <span className="warn-text"> · one search covers at most {MAX_SEARCH_HOURS} h; narrow the range to search it all at once</span>
          )}
          <span className="spacer" />
          <button className="ghost" onClick={() => setShowPreview((s) => !s)}>
            {showPreview ? "Hide sample" : "Sample payload"}
          </button>
          {running ? (
            <button className="danger" onClick={() => abort.current?.abort()}>
              Cancel
            </button>
          ) : (
            <button className="primary" disabled={!!problem} onClick={() => void start_()}>
              Generate and send
            </button>
          )}
        </div>

        {run && p && (
          <div className="run-progress">
            <div className="progress">
              <div className={`progress-fill ${p.recordsFailed ? "has-failures" : ""}`} style={{ width: `${pct}%` }} />
            </div>
            <div className="run-stats">
              <span>
                <strong>{run.state === "running" ? "Sending" : run.state === "done" ? "Done" : "Cancelled"}</strong> ·{" "}
                {p.recordsSent.toLocaleString()} / {run.config.total.toLocaleString()} sent
              </span>
              {p.recordsFailed > 0 && <span className="error-text">{p.recordsFailed.toLocaleString()} failed</span>}
              {p.retries > 0 && <span className="muted">{p.retries} retries</span>}
              <span className="muted">{rate.toLocaleString()} logs/s</span>
              <span className="muted mono">{run.config.runId}</span>
            </div>
            {p.lastError && <div className="muted small-text">last error: {p.lastError}</div>}
            {run.state !== "running" && p.recordsSent > 0 && (
              <div className="run-done">
                <span className="muted">
                  The ingester has accepted them; they become searchable after the appender's next commit (up to{" "}
                  <code>appender.buffer.timeSeconds</code>, 60 s by default).
                </span>
                <button className="primary" onClick={() => searchRun(run)}>
                  Search these logs
                </button>
              </div>
            )}
          </div>
        )}

        {showPreview && preview && <pre className="preview mono">{preview}</pre>}
      </section>
    </div>
  );
}
