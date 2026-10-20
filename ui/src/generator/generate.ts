// Synthetic OTLP/JSON logs (ExportLogsServiceRequest), deterministic per seed

export const SEVERITIES = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"] as const;
export type Severity = (typeof SEVERITIES)[number];
const SEVERITY_NUMBER: Record<Severity, number> = { DEBUG: 5, INFO: 9, WARN: 13, ERROR: 17, FATAL: 21 };

export interface ServiceSpec {
  name: string;
  pods: number;
}

export interface GeneratorConfig {
  services: ServiceSpec[];
  serviceNamespace: string;
  k8sNamespace: string;
  environment: string;
  nodes: number;
  startMs: number;
  endMs: number;
  total: number;
  batchSize: number;
  severityWeights: Record<Severity, number>;
  seed: number;
  runId: string;
}

export const DEFAULT_SERVICES: ServiceSpec[] = [
  { name: "frontend", pods: 3 },
  { name: "checkout", pods: 2 },
  { name: "payment", pods: 2 },
  { name: "cart", pods: 2 },
  { name: "auth", pods: 2 },
];

export const DEFAULT_WEIGHTS: Record<Severity, number> = { DEBUG: 15, INFO: 65, WARN: 12, ERROR: 7, FATAL: 1 };

/** mulberry32: small, fast, good enough for test data */
export function prng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

type Rand = () => number;
const pick = <T,>(rand: Rand, items: readonly T[]): T => items[Math.floor(rand() * items.length)];
const int = (rand: Rand, lo: number, hi: number) => lo + Math.floor(rand() * (hi - lo + 1));
const hex = (rand: Rand, chars: number) => {
  let out = "";
  for (let i = 0; i < chars; i++) out += Math.floor(rand() * 16).toString(16);
  return out;
};
const ALNUM = "bcdfghjklmnpqrstvwxz2456789";
const alnum = (rand: Rand, chars: number) => Array.from({ length: chars }, () => pick(rand, [...ALNUM])).join("");

export interface Pod {
  service: string;
  pod: string;
  instanceId: string;
  host: string;
}

/** Pods named like a Deployment's: <service>-<replicaset hash>-<suffix> */
export function makePods(config: GeneratorConfig): Pod[] {
  const rand = prng(config.seed ^ 0x9e3779b9);
  const pods: Pod[] = [];
  for (const service of config.services) {
    const replicaSet = alnum(rand, 10);
    for (let i = 0; i < service.pods; i++) {
      pods.push({
        service: service.name,
        pod: `${service.name}-${replicaSet}-${alnum(rand, 5)}`,
        instanceId: `${hex(rand, 8)}-${hex(rand, 4)}-${hex(rand, 4)}-${hex(rand, 4)}-${hex(rand, 12)}`,
        host: `node-${int(rand, 1, Math.max(1, config.nodes))}`,
      });
    }
  }
  return pods;
}

interface Message {
  body: string;
  attrs: Record<string, string>;
}

const ROUTES: Record<string, string[]> = {
  frontend: ["/", "/product/{id}", "/cart", "/checkout", "/search"],
  checkout: ["/api/checkout", "/api/orders/{id}", "/api/shipping/quote"],
  payment: ["/api/charge", "/api/refund", "/api/payment-methods"],
  cart: ["/api/cart", "/api/cart/items", "/api/cart/{id}"],
  auth: ["/api/login", "/api/logout", "/api/token/refresh", "/api/users/{id}"],
};
const METHODS = ["GET", "GET", "GET", "POST", "PUT", "DELETE"];

function message(rand: Rand, service: string, severity: Severity): Message {
  const routes = ROUTES[service] ?? ["/api/" + service, "/api/" + service + "/{id}"];
  const method = pick(rand, METHODS);
  const route = pick(rand, routes);
  const user = `user-${int(rand, 1000, 9999)}`;
  const order = `ord-${hex(rand, 8)}`;
  const peer = pick(rand, ["postgres", "redis", "kafka", "inventory", "shipping", "fraud-check"]);
  let duration = int(rand, 2, 250);
  let status = pick(rand, [200, 200, 200, 201, 204, 304]);
  let body: string;
  switch (severity) {
    case "DEBUG":
      body = pick(rand, [
        `cache lookup key=session:${user} hit=${rand() < 0.7}`,
        `acquired connection from ${peer} pool in ${int(rand, 0, 5)}ms`,
        `parsed request headers count=${int(rand, 5, 30)}`,
      ]);
      break;
    case "INFO":
      body = pick(rand, [
        `${method} ${route} completed status=${status} in ${duration}ms`,
        `request handled for ${user} order=${order}`,
        `${service} processed ${int(rand, 1, 50)} items for ${user}`,
      ]);
      break;
    case "WARN":
      duration = int(rand, 800, 4000);
      status = pick(rand, [200, 404, 409, 429]);
      body = pick(rand, [
        `slow request ${method} ${route} took ${duration}ms`,
        `retrying call to ${peer} attempt=${int(rand, 2, 4)}`,
        `rate limit approaching for ${user}: ${int(rand, 80, 99)}% of quota`,
      ]);
      break;
    case "ERROR":
      duration = int(rand, 1000, 30000);
      status = pick(rand, [500, 502, 503, 504]);
      body = pick(rand, [
        `${method} ${route} failed status=${status}: upstream ${peer} timeout after ${duration}ms`,
        `database query failed: connection reset by ${peer}`,
        service === "payment" ? `payment declined for ${order}: card_declined` : `failed to process ${order}: ${peer} unavailable`,
      ]);
      break;
    case "FATAL":
      status = 500;
      body = pick(rand, [
        `unrecoverable error: out of memory, shutting down`,
        `panic: nil pointer dereference in handler ${route}`,
      ]);
      break;
  }
  return {
    body,
    attrs: {
      "http.method": method,
      "http.route": route,
      "http.status_code": String(status),
      "duration_ms": String(duration),
      "user.id": user,
    },
  };
}

function weightedSeverity(rand: Rand, weights: Record<Severity, number>): Severity {
  const total = SEVERITIES.reduce((sum, s) => sum + Math.max(0, weights[s]), 0);
  if (total <= 0) return "INFO";
  let r = rand() * total;
  for (const s of SEVERITIES) {
    r -= Math.max(0, weights[s]);
    if (r < 0) return s;
  }
  return "INFO";
}

const str = (key: string, value: string) => ({ key, value: { stringValue: value } });

export interface Batch {
  /** ExportLogsServiceRequest as OTLP/JSON */
  payload: string;
  records: number;
}

export function validate(config: GeneratorConfig): string | null {
  if (!config.services.length || config.services.every((s) => s.pods < 1)) return "add at least one service with a pod";
  if (config.services.some((s) => !s.name.trim())) return "every service needs a name";
  if (!(config.endMs > config.startMs)) return "end must be after start";
  if (!(config.total >= 1)) return "log count must be at least 1";
  if (!(config.batchSize >= 1)) return "batch size must be at least 1";
  if (!SEVERITIES.some((s) => config.severityWeights[s] > 0)) return "give at least one severity a weight";
  return null;
}

/**
 * The run as batches of at most `batchSize` records, each one OTLP request
 * with one resourceLogs per pod. Timestamps are uniform in [startMs, endMs)
 * at nanosecond precision.
 */
export function* generateBatches(config: GeneratorConfig): Generator<Batch> {
  const pods = makePods(config).filter((p) => p.service.trim());
  const rand = prng(config.seed);
  const spanNs = BigInt(Math.round((config.endMs - config.startMs) * 1e6));
  const startNs = BigInt(Math.round(config.startMs)) * 1_000_000n;

  for (let done = 0; done < config.total; ) {
    const count = Math.min(config.batchSize, config.total - done);
    const byPod = new Map<Pod, object[]>();
    for (let i = 0; i < count; i++) {
      const pod = pick(rand, pods);
      const severity = weightedSeverity(rand, config.severityWeights);
      const { body, attrs } = message(rand, pod.service, severity);
      // two draws for full nanosecond resolution over long ranges
      const fraction = BigInt(Math.floor(rand() * 2 ** 26)) * 2n ** 26n + BigInt(Math.floor(rand() * 2 ** 26));
      const timeNs = startNs + (spanNs * fraction) / 2n ** 52n;
      const observedNs = timeNs + BigInt(int(rand, 1, 500)) * 1_000_000n;
      const record = {
        timeUnixNano: timeNs.toString(),
        observedTimeUnixNano: observedNs.toString(),
        severityNumber: SEVERITY_NUMBER[severity],
        severityText: severity,
        body: { stringValue: body },
        attributes: [...Object.entries(attrs).map(([k, v]) => str(k, v)), str("generator.run_id", config.runId)],
        traceId: hex(rand, 32),
        spanId: hex(rand, 16),
      };
      const list = byPod.get(pod);
      if (list) list.push(record);
      else byPod.set(pod, [record]);
    }
    const resourceLogs = [...byPod].map(([pod, logRecords]) => ({
      resource: {
        attributes: [
          str("service.name", pod.service),
          str("service.namespace", config.serviceNamespace),
          str("service.instance.id", pod.instanceId),
          str("k8s.pod.name", pod.pod),
          str("k8s.namespace.name", config.k8sNamespace),
          str("host.name", pod.host),
          str("deployment.environment.name", config.environment),
        ],
      },
      scopeLogs: [{ scope: { name: "telemetry-lake-ui/generator", version: "1" }, logRecords }],
    }));
    yield { payload: JSON.stringify({ resourceLogs }), records: count };
    done += count;
  }
}

export function batchCount(config: GeneratorConfig): number {
  return Math.ceil(config.total / Math.max(1, config.batchSize));
}

export function newRunId(rand: Rand = Math.random): string {
  return `run-${hex(rand, 8)}`;
}
