import { describe, expect, it } from "vitest";
import { DEFAULT_SERVICES, DEFAULT_WEIGHTS, type GeneratorConfig, batchCount, generateBatches, makePods, validate } from "./generate";
import { sendBatches } from "./send";
import { IngestError } from "../api/ingest";

const config = (patch: Partial<GeneratorConfig> = {}): GeneratorConfig => ({
  services: DEFAULT_SERVICES,
  serviceNamespace: "shop",
  k8sNamespace: "default",
  environment: "dev",
  nodes: 3,
  startMs: Date.parse("2026-10-19T00:00:00Z"),
  endMs: Date.parse("2026-10-19T06:00:00Z"),
  total: 2500,
  batchSize: 1000,
  severityWeights: DEFAULT_WEIGHTS,
  seed: 42,
  runId: "run-test",
  ...patch,
});

const records = (payload: string) =>
  JSON.parse(payload).resourceLogs.flatMap((r: any) => r.scopeLogs.flatMap((s: any) => s.logRecords.map((l: any) => ({ ...l, resource: r.resource }))));

describe("generateBatches", () => {
  it("produces exactly the requested count in batches", () => {
    const batches = [...generateBatches(config())];
    expect(batches.map((b) => b.records)).toEqual([1000, 1000, 500]);
    expect(batchCount(config())).toBe(3);
    expect(batches.flatMap((b) => records(b.payload))).toHaveLength(2500);
  });

  it("emits valid OTLP/JSON inside the time range", () => {
    const c = config();
    const all = [...generateBatches(c)].flatMap((b) => records(b.payload));
    const pods = new Set(makePods(c).map((p) => p.pod));
    for (const r of all) {
      const ms = Number(BigInt(r.timeUnixNano) / 1_000_000n);
      expect(ms).toBeGreaterThanOrEqual(c.startMs);
      expect(ms).toBeLessThan(c.endMs);
      expect(BigInt(r.observedTimeUnixNano) > BigInt(r.timeUnixNano)).toBe(true);
      expect(r.traceId).toMatch(/^[0-9a-f]{32}$/);
      expect(r.spanId).toMatch(/^[0-9a-f]{16}$/);
      expect(typeof r.body.stringValue).toBe("string");
      expect(r.attributes).toContainEqual({ key: "generator.run_id", value: { stringValue: "run-test" } });
      const resource = Object.fromEntries(r.resource.attributes.map((a: any) => [a.key, a.value.stringValue]));
      expect(pods.has(resource["k8s.pod.name"])).toBe(true);
      expect(resource["k8s.pod.name"].startsWith(resource["service.name"] + "-")).toBe(true);
      expect(resource["deployment.environment.name"]).toBe("dev");
    }
    // spread over the range, not clustered at the start
    const hours = new Set(all.map((r: any) => new Date(Number(BigInt(r.timeUnixNano) / 1_000_000n)).getUTCHours()));
    expect(hours.size).toBe(6);
    // every service shows up
    expect(new Set(all.map((r: any) => r.resource.attributes[0].value.stringValue)).size).toBe(5);
  });

  it("is deterministic for a seed", () => {
    const a = [...generateBatches(config())].map((b) => b.payload);
    const b = [...generateBatches(config())].map((b) => b.payload);
    const c = [...generateBatches(config({ seed: 7 }))].map((b) => b.payload);
    expect(a).toEqual(b);
    expect(a).not.toEqual(c);
  });

  it("respects severity weights", () => {
    const all = [...generateBatches(config({ severityWeights: { DEBUG: 0, INFO: 0, WARN: 0, ERROR: 1, FATAL: 0 } }))].flatMap((b) =>
      records(b.payload),
    );
    expect(new Set(all.map((r: any) => r.severityText))).toEqual(new Set(["ERROR"]));
    expect(all[0].severityNumber).toBe(17);
  });

  it("keeps a large batch well under the ingester's 16 MiB limit", () => {
    const [batch] = generateBatches(config({ total: 5000, batchSize: 5000 }));
    expect(batch.payload.length).toBeLessThan(16 * 1024 * 1024);
  });

  it("validates configs", () => {
    expect(validate(config())).toBeNull();
    expect(validate(config({ endMs: config().startMs }))).toMatch(/end/);
    expect(validate(config({ services: [] }))).toMatch(/service/);
    expect(validate(config({ severityWeights: { DEBUG: 0, INFO: 0, WARN: 0, ERROR: 0, FATAL: 0 } }))).toMatch(/severity/);
  });
});

describe("sendBatches", () => {
  const batches = [...generateBatches(config({ total: 50, batchSize: 10 }))];
  const noSleep = async () => {};

  it("sends everything and retries retryable failures", async () => {
    let calls = 0;
    const post = async () => {
      if (++calls % 3 === 0) throw new IngestError(503, "busy");
    };
    const progress = await sendBatches(batches, { concurrency: 2, signal: new AbortController().signal, onProgress: () => {}, post, sleep: noSleep });
    expect(progress).toMatchObject({ batchesSent: 5, recordsSent: 50, batchesFailed: 0 });
    expect(progress.retries).toBeGreaterThan(0);
  });

  it("counts non-retryable failures and continues", async () => {
    let calls = 0;
    const post = async () => {
      if (++calls === 1) throw new IngestError(400, "bad");
    };
    const progress = await sendBatches(batches, { concurrency: 1, signal: new AbortController().signal, onProgress: () => {}, post, sleep: noSleep });
    expect(progress).toMatchObject({ batchesSent: 4, batchesFailed: 1, recordsFailed: 10, retries: 0 });
  });

  it("stops on abort", async () => {
    const controller = new AbortController();
    let calls = 0;
    const post = async () => {
      if (++calls === 2) controller.abort();
    };
    const progress = await sendBatches(batches, { concurrency: 1, signal: controller.signal, onProgress: () => {}, post, sleep: noSleep });
    expect(progress.batchesSent).toBe(2);
  });
});
