// The load generator service (otel-loadgen), reached through /api/loadgen.
// Types mirror loadgen/src/spec.rs, controller.rs and stats.rs.

export type Wire = "protobuf" | "json";
export type Compression = "none" | "gzip";

export interface RunSpec {
  records_per_second: number;
  records_per_request: number;
  duration_seconds?: number;
  max_in_flight: number;
  wire: Wire;
  compression: Compression;
  severity_weights: { debug: number; info: number; warn: number; error: number; fatal: number };
  services: { name: string; pods: number }[];
  nodes: number;
  service_namespace: string;
  k8s_namespace: string;
  environment: string;
  users: number;
  extra_attributes: { key: string; distinct_values: number }[];
  pod_churn_minutes?: number;
  body_padding_bytes: number;
  seed: number;
}

export type RunState = "running" | "stopped" | "finished";

export interface RunInfo {
  id: string;
  spec: RunSpec;
  state: RunState;
  started_ms: number;
  ended_ms: number | null;
  sample_request_bytes: number;
}

export interface Counters {
  requests: number;
  records: number;
  wire_bytes: number;
  raw_bytes: number;
  accepted_records: number;
  rejected_records: number;
  failed_records: number;
  network_errors: number;
  behind: number;
  codes: Record<string, number>;
  latency: number[];
  latency_seconds: number;
}

export interface Rates {
  records_per_second: number;
  accepted_records_per_second: number;
  requests_per_second: number;
  wire_bytes_per_second: number;
  raw_bytes_per_second: number;
  failed_requests_per_second: number;
  behind_per_second: number;
  latency_p50_seconds: number | null;
  latency_p99_seconds: number | null;
}

export interface WorkerReport {
  address: string;
  up: boolean;
  state: "idle" | "running" | "finished" | null;
  records_per_second: number;
  in_flight: number;
  rates: Rates;
  totals: Counters;
  error: string | null;
}

export interface Report {
  run: RunInfo | null;
  totals: Counters;
  rates: Rates;
  in_flight: number;
  workers: WorkerReport[];
  mode: "local" | "workers";
}

export interface Estimate {
  request_bytes: number;
  request_wire_bytes: number;
  requests_per_second: number;
  raw_bytes_per_second: number;
  wire_bytes_per_second: number;
}

export interface Limits {
  max_records_per_second: number;
  max_in_flight: number;
  max_request_bytes: number;
}

export class LoadgenError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

async function call<T>(method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`/api/loadgen${path}`, {
      method,
      headers: body === undefined ? undefined : { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal,
    });
  } catch (e) {
    if (signal?.aborted) throw e;
    throw new LoadgenError(0, `load generator unreachable: ${(e as Error).message}`);
  }
  const text = await response.text();
  if (!response.ok) {
    let message = text;
    try {
      message = (JSON.parse(text) as { error?: string }).error ?? text;
    } catch {
      // not JSON (e.g. the proxy's 502 page)
    }
    throw new LoadgenError(response.status, message ? message.slice(0, 300) : `HTTP ${response.status}`);
  }
  return JSON.parse(text) as T;
}

export const getReport = (signal?: AbortSignal) => call<Report>("GET", "/v1/run", undefined, signal);
export const startRun = (spec: RunSpec) => call<RunInfo>("POST", "/v1/run", spec);
export const updateRun = (patch: { records_per_second?: number; max_in_flight?: number }) =>
  call<RunInfo>("PATCH", "/v1/run", patch);
export const stopRun = () => call<RunInfo>("DELETE", "/v1/run");
export const estimate = (spec: RunSpec, signal?: AbortSignal) => call<Estimate>("POST", "/v1/estimate", spec, signal);
export const getLimits = () => call<Limits>("GET", "/v1/limits");
