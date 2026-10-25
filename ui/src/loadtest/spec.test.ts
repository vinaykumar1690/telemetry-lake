import { describe, expect, it } from "vitest";
import type { RunInfo } from "../api/loadgen";
import { DEFAULT_FORM, cardinality, formatBytes, formatCount, formatElapsed, formatLatency, podsOverRun, searchWindow, toRunSpec, validate } from "./spec";

describe("toRunSpec", () => {
  it("maps the default form to a valid spec", () => {
    const spec = toRunSpec(DEFAULT_FORM);
    expect(validate(spec)).toBeNull();
    expect(spec.records_per_second).toBe(20_000);
    expect(spec.duration_seconds).toBe(300);
    expect(spec.compression).toBe("none");
    expect(spec.severity_weights).toEqual({ debug: 15, info: 65, warn: 12, error: 7, fatal: 1 });
    expect(spec.pod_churn_minutes).toBeUndefined();
  });

  it("leaves out zero duration and churn, floors numbers and trims names", () => {
    const spec = toRunSpec({
      ...DEFAULT_FORM,
      durationSeconds: 0,
      podChurnMinutes: 0,
      recordsPerSecond: 1500.7,
      gzip: true,
      services: [{ name: "  api ", pods: 2.9 }],
      extraAttributes: [{ key: " tenant.id ", distinctValues: 50 }],
    });
    expect(spec.duration_seconds).toBeUndefined();
    expect(spec.pod_churn_minutes).toBeUndefined();
    expect(spec.records_per_second).toBe(1500);
    expect(spec.compression).toBe("gzip");
    expect(spec.services).toEqual([{ name: "api", pods: 2 }]);
    expect(spec.extra_attributes).toEqual([{ key: "tenant.id", distinct_values: 50 }]);
    // The service rejects unknown fields; keep the shape exact
    expect(Object.keys(spec).sort()).toEqual(
      [
        "body_padding_bytes",
        "compression",
        "environment",
        "extra_attributes",
        "k8s_namespace",
        "max_in_flight",
        "nodes",
        "records_per_request",
        "records_per_second",
        "seed",
        "service_namespace",
        "services",
        "severity_weights",
        "users",
        "wire",
      ].sort(),
    );
  });

  it("treats empty number inputs as zero, which validation catches", () => {
    expect(validate(toRunSpec({ ...DEFAULT_FORM, recordsPerSecond: Number.NaN }))).toMatch(/rate/);
  });
});

describe("validate", () => {
  const base = toRunSpec(DEFAULT_FORM);
  it.each([
    [{ ...base, records_per_request: 0 }, /per request/],
    [{ ...base, max_in_flight: 0 }, /in flight/],
    [{ ...base, services: [] }, /service/],
    [{ ...base, services: [{ name: "", pods: 1 }] }, /name/],
    [{ ...base, services: [{ name: "a", pods: 0 }] }, /pod/],
    [{ ...base, users: 0 }, /user/],
    [{ ...base, extra_attributes: [{ key: "", distinct_values: 1 }] }, /key/],
    [{ ...base, severity_weights: { debug: 0, info: 0, warn: 0, error: 0, fatal: 0 } }, /severity/],
  ])("rejects %#", (spec, message) => {
    expect(validate(spec)).toMatch(message);
  });
});

describe("cardinality", () => {
  it("counts pods, hosts and record attribute values", () => {
    const spec = toRunSpec({ ...DEFAULT_FORM, nodes: 50, users: 300, extraAttributes: [{ key: "tenant.id", distinctValues: 20 }] });
    const c = cardinality(spec);
    expect(c.pods).toBe(11);
    expect(c.hosts).toBe(11);
    expect(c.recordAttributes).toEqual([
      { key: "user.id", values: 300 },
      { key: "tenant.id", values: 20 },
    ]);
  });

  it("multiplies pods by churn periods", () => {
    const spec = toRunSpec({ ...DEFAULT_FORM, podChurnMinutes: 10 });
    expect(podsOverRun(spec, 3600)).toBe(66);
    expect(podsOverRun(toRunSpec(DEFAULT_FORM), 3600)).toBe(11);
  });
});

describe("formatting", () => {
  it("formats bytes, counts, latency and elapsed time", () => {
    expect(formatBytes(512)).toBe("512 B");
    expect(formatBytes(1_500_000)).toBe("1.5 MB");
    expect(formatBytes(250_000_000)).toBe("250 MB");
    expect(formatCount(950)).toBe("950");
    expect(formatCount(25_000)).toBe("25.0k");
    expect(formatCount(2_500_000)).toBe("2.5M");
    expect(formatLatency(null)).toBe("–");
    expect(formatLatency(0.0034)).toBe("≤ 3.4 ms");
    expect(formatLatency(2.5)).toBe("≤ 2.5 s");
    expect(formatElapsed(65_000)).toBe("1:05");
    expect(formatElapsed(3_725_000)).toBe("1:02:05");
  });
});

describe("searchWindow", () => {
  it("covers the run with a second either side, up to now while it runs", () => {
    const run = { started_ms: Date.UTC(2026, 9, 24, 12), ended_ms: null } as RunInfo;
    const now = Date.UTC(2026, 9, 24, 12, 5);
    expect(searchWindow(run, now)).toEqual({ start: "2026-10-24T11:59:59.000Z", end: "2026-10-24T12:05:01.000Z" });
    expect(searchWindow({ ...run, ended_ms: Date.UTC(2026, 9, 24, 12, 1) }, now).end).toBe("2026-10-24T12:01:01.000Z");
  });
});
