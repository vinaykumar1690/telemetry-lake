// Load test form → RunSpec, client-side checks, cardinality and formatting.
// The service validates again (loadgen/src/spec.rs) and sizes the requests.
import type { RunInfo, RunSpec, Wire } from "../api/loadgen";
import { DEFAULT_SERVICES, DEFAULT_WEIGHTS, type ServiceSpec, type Severity } from "../generator/generate";

export interface ExtraAttributeForm {
  key: string;
  distinctValues: number;
}

export interface LoadTestForm {
  recordsPerSecond: number;
  /** 0: until stopped */
  durationSeconds: number;
  recordsPerRequest: number;
  maxInFlight: number;
  wire: Wire;
  gzip: boolean;
  bodyPaddingBytes: number;
  weights: Record<Severity, number>;
  services: ServiceSpec[];
  nodes: number;
  serviceNamespace: string;
  k8sNamespace: string;
  environment: string;
  users: number;
  extraAttributes: ExtraAttributeForm[];
  /** 0: no churn */
  podChurnMinutes: number;
  seed: number;
}

export const DEFAULT_FORM: LoadTestForm = {
  recordsPerSecond: 20_000,
  durationSeconds: 300,
  recordsPerRequest: 500,
  maxInFlight: 64,
  wire: "protobuf",
  gzip: false,
  bodyPaddingBytes: 0,
  weights: DEFAULT_WEIGHTS,
  services: DEFAULT_SERVICES,
  nodes: 3,
  serviceNamespace: "shop",
  k8sNamespace: "shop",
  environment: "load",
  users: 10_000,
  extraAttributes: [],
  podChurnMinutes: 0,
  seed: 1,
};

const whole = (n: number, min = 0) => Math.max(min, Math.floor(Number.isFinite(n) ? n : 0));

export function toRunSpec(form: LoadTestForm): RunSpec {
  const w = form.weights;
  const spec: RunSpec = {
    records_per_second: whole(form.recordsPerSecond),
    records_per_request: whole(form.recordsPerRequest),
    max_in_flight: whole(form.maxInFlight),
    wire: form.wire,
    compression: form.gzip ? "gzip" : "none",
    severity_weights: { debug: w.DEBUG || 0, info: w.INFO || 0, warn: w.WARN || 0, error: w.ERROR || 0, fatal: w.FATAL || 0 },
    services: form.services.map((s) => ({ name: s.name.trim(), pods: whole(s.pods) })),
    nodes: whole(form.nodes),
    service_namespace: form.serviceNamespace,
    k8s_namespace: form.k8sNamespace,
    environment: form.environment,
    users: whole(form.users),
    extra_attributes: form.extraAttributes.map((a) => ({ key: a.key.trim(), distinct_values: whole(a.distinctValues) })),
    body_padding_bytes: whole(form.bodyPaddingBytes),
    seed: whole(form.seed),
  };
  if (whole(form.durationSeconds) > 0) spec.duration_seconds = whole(form.durationSeconds);
  if (whole(form.podChurnMinutes) > 0) spec.pod_churn_minutes = whole(form.podChurnMinutes);
  return spec;
}

/** The checks worth showing before asking the service */
export function validate(spec: RunSpec): string | null {
  if (spec.records_per_second < 1) return "the rate must be at least 1 record/s";
  if (spec.records_per_request < 1) return "send at least 1 record per request";
  if (spec.max_in_flight < 1) return "allow at least 1 request in flight";
  if (!spec.services.length) return "add at least one service";
  if (spec.services.some((s) => !s.name)) return "every service needs a name";
  if (spec.services.some((s) => s.pods < 1)) return "every service needs at least one pod";
  if (spec.nodes < 1) return "at least one node";
  if (spec.users < 1) return "at least one user";
  if (spec.extra_attributes.some((a) => !a.key)) return "every extra attribute needs a key";
  if (spec.extra_attributes.some((a) => a.distinct_values < 1)) return "every extra attribute needs at least one value";
  const w = spec.severity_weights;
  if (![w.debug, w.info, w.warn, w.error, w.fatal].some((x) => x > 0)) return "give at least one severity a weight";
  return null;
}

export interface Cardinality {
  pods: number;
  hosts: number;
  /** Distinct resource attribute sets (one per pod) per churn period */
  resourceSeries: number;
  /** Distinct values per record attribute */
  recordAttributes: { key: string; values: number }[];
}

export function cardinality(spec: RunSpec): Cardinality {
  const pods = spec.services.reduce((n, s) => n + Math.max(0, s.pods), 0);
  return {
    pods,
    hosts: Math.min(pods, spec.nodes),
    resourceSeries: pods,
    recordAttributes: [{ key: "user.id", values: spec.users }, ...spec.extra_attributes.map((a) => ({ key: a.key, values: a.distinct_values }))],
  };
}

/** Pods seen over a run of `seconds`: a new set every churn period */
export function podsOverRun(spec: RunSpec, seconds: number): number {
  const pods = cardinality(spec).pods;
  if (!spec.pod_churn_minutes) return pods;
  return pods * Math.max(1, Math.ceil(seconds / (spec.pod_churn_minutes * 60)));
}

const UNITS = ["B", "KB", "MB", "GB", "TB"];

export function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes < 0) return "–";
  let value = bytes;
  let unit = 0;
  while (value >= 1000 && unit < UNITS.length - 1) {
    value /= 1000;
    unit++;
  }
  return `${value >= 100 || unit === 0 ? Math.round(value) : value.toFixed(1)} ${UNITS[unit]}`;
}

export function formatCount(n: number): string {
  if (!Number.isFinite(n)) return "–";
  if (n >= 1e6) return `${(n / 1e6).toFixed(n >= 1e7 ? 0 : 1)}M`;
  if (n >= 1e4) return `${(n / 1e3).toFixed(n >= 1e5 ? 0 : 1)}k`;
  return Math.round(n).toLocaleString();
}

export function formatLatency(seconds: number | null): string {
  if (seconds === null || !Number.isFinite(seconds)) return "–";
  if (seconds < 1) return `≤ ${Math.round(seconds * 1000 * 10) / 10} ms`;
  return `≤ ${seconds.toFixed(1)} s`;
}

export function formatElapsed(ms: number): string {
  const s = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const pad = (n: number) => String(n).padStart(2, "0");
  return h ? `${h}:${pad(m)}:${pad(s % 60)}` : `${m}:${pad(s % 60)}`;
}

/** Search range covering the run: records are stamped up to 500 ms before they are sent */
export function searchWindow(run: RunInfo, nowMs: number): { start: string; end: string } {
  const end = run.ended_ms ?? nowMs;
  return { start: new Date(run.started_ms - 1000).toISOString(), end: new Date(end + 1000).toISOString() };
}
