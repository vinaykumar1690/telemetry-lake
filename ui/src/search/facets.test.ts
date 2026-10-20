import { describe, expect, it } from "vitest";
import { bucketSeconds, facetSql, histogramFilterSql, histogramSql, sqlString } from "./facets";
import { DEFAULT_STATE, FACET_FIELDS } from "./types";

const START = Date.parse("2026-10-19T00:00:00Z");
const END = Date.parse("2026-10-19T01:00:00Z");

describe("facet SQL", () => {
  it("escapes string literals", () => {
    expect(sqlString("o'brien")).toBe("'o''brien'");
  });

  it("groups one expression over the time range", () => {
    expect(facetSql("resource_attributes['k8s.pod.name']", START, END)).toBe(
      "SELECT resource_attributes['k8s.pod.name'] AS v, count(*) AS n FROM logs WHERE " +
        "\"timestamp\" >= TIMESTAMP '2026-10-19T00:00:00.000Z' AND \"timestamp\" < TIMESTAMP '2026-10-19T01:00:00.000Z' " +
        "AND resource_attributes['k8s.pod.name'] IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 200",
    );
  });

  it("picks a familiar bucket width", () => {
    expect(bucketSeconds(START, END)).toBe(60);
    expect(bucketSeconds(START, START + 7 * 86_400_000)).toBe(10800);
    expect(bucketSeconds(START, START + 60_000)).toBe(1);
  });

  it("applies top-level filters and grep to the histogram", () => {
    const state = { ...DEFAULT_STATE, filters: { "service.name": ["a", "b'c"] }, grep: "Time'out", ignoreCase: true };
    const where = histogramFilterSql(state, FACET_FIELDS);
    expect(where).toBe(" AND service_name IN ('a', 'b''c') AND strpos(lower(body), lower('Time''out')) > 0");
    expect(histogramFilterSql({ ...state, filters: {}, regex: true, ignoreCase: false }, FACET_FIELDS)).toBe(" AND body ~ 'Time''out'");
    expect(histogramSql(START, END, 60, where)).toContain(`date_bin(INTERVAL '60 seconds', "timestamp")`);
  });
});
