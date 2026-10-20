import { describe, expect, it } from "vitest";
import { buildRequest, resolveRange } from "./buildRequest";
import { DEFAULT_STATE, type SearchState } from "./types";

const state = (patch: Partial<SearchState>): SearchState => ({ ...DEFAULT_STATE, ...patch });

describe("buildRequest", () => {
  it("maps a relative range and defaults", () => {
    expect(buildRequest(state({ range: { kind: "relative", last: "15m" } }))).toEqual({
      start: "now-15m",
      end: "now",
      order: "desc",
      limit: 100,
    });
  });

  it("passes absolute ranges through", () => {
    const r = buildRequest(state({ range: { kind: "absolute", start: "2026-10-19T00:00:00.000Z", end: "2026-10-19T06:00:00.000Z" } }));
    expect([r.start, r.end]).toEqual(["2026-10-19T00:00:00.000Z", "2026-10-19T06:00:00.000Z"]);
  });

  it("sends one value as equality, several as IN, and drops empty selections", () => {
    const r = buildRequest(state({ filters: { "service.name": ["checkout"], severity_text: ["WARN", "ERROR"], "host.name": [] } }));
    expect(r.filters).toEqual({ "service.name": "checkout", severity_text: ["WARN", "ERROR"] });
  });

  it("builds where from attribute rows", () => {
    const one = buildRequest(state({ attrs: [{ field: "attr.http.status_code", op: "=", value: "500" }] }));
    expect(one.where).toEqual({ field: "attr.http.status_code", op: "=", value: "500" });

    const many = buildRequest(
      state({
        attrs: [
          { field: "user.id", op: "!=", value: "user-1" },
          { field: "trace_id", op: "exists", value: "ignored" },
          { field: "  ", op: "=", value: "skipped" },
        ],
      }),
    );
    expect(many.where).toEqual({
      and: [
        { field: "user.id", op: "!=", value: "user-1" },
        { field: "trace_id", op: "exists" },
      ],
    });
  });

  it("maps grep, case-insensitive grep and regex", () => {
    expect(buildRequest(state({ grep: "timeout", ignoreCase: false }))).toMatchObject({ grep: "timeout" });
    expect(buildRequest(state({ grep: "timeout", ignoreCase: false })).grep_ignore_case).toBeUndefined();
    expect(buildRequest(state({ grep: "Timeout", ignoreCase: true }))).toMatchObject({ grep: "Timeout", grep_ignore_case: true });
    const re = buildRequest(state({ grep: "status=5\\d\\d", regex: true, ignoreCase: true }));
    expect(re.regex).toBe("(?i)status=5\\d\\d");
    expect(re.grep).toBeUndefined();
  });

  it("adds fields and the cursor", () => {
    const r = buildRequest(state({}), "abc", ["k8s.pod.name"]);
    expect(r).toMatchObject({ cursor: "abc", fields: ["k8s.pod.name"] });
  });
});

describe("resolveRange", () => {
  it("resolves relative units", () => {
    expect(resolveRange({ kind: "relative", last: "2h" }, 10_000_000)).toEqual({ startMs: 10_000_000 - 7_200_000, endMs: 10_000_000 });
    expect(() => resolveRange({ kind: "relative", last: "2y" })).toThrow();
  });
});
