import type { SearchState } from "./types";

export function sqlString(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

function timestampLiteral(ms: number): string {
  return `TIMESTAMP ${sqlString(new Date(ms).toISOString())}`;
}

export function timeWhere(startMs: number, endMs: number): string {
  return `"timestamp" >= ${timestampLiteral(startMs)} AND "timestamp" < ${timestampLiteral(endMs)}`;
}

/** Top values of one field over the time range (other filters are not applied) */
export function facetSql(expr: string, startMs: number, endMs: number, limit = 200): string {
  return (
    `SELECT ${expr} AS v, count(*) AS n FROM logs WHERE ${timeWhere(startMs, endMs)} ` +
    `AND ${expr} IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT ${limit}`
  );
}

/** Bucket width giving about `target` bars over the range, rounded to a familiar step */
export function bucketSeconds(startMs: number, endMs: number, target = 60): number {
  const steps = [1, 5, 10, 30, 60, 300, 600, 1800, 3600, 10800, 21600, 43200, 86400];
  const ideal = (endMs - startMs) / 1000 / target;
  return steps.find((s) => s >= ideal) ?? 86400;
}

/**
 * SQL for the parts of a search the histogram can express exactly: the
 * top-level filters (their columns are known) and grep/regex on the body.
 * Free-form attribute rows are not applied (attributes resolve through the
 * query service's field namespace, which SQL does not have).
 */
export function histogramFilterSql(state: SearchState, fields: { field: string; sql: string }[]): string {
  const parts: string[] = [];
  for (const { field, sql } of fields) {
    const values = state.filters[field] ?? [];
    if (values.length) parts.push(`${sql} IN (${values.map(sqlString).join(", ")})`);
  }
  if (state.grep) {
    if (state.regex) parts.push(`body ${state.ignoreCase ? "~*" : "~"} ${sqlString(state.grep)}`);
    else if (state.ignoreCase) parts.push(`strpos(lower(body), lower(${sqlString(state.grep)})) > 0`);
    else parts.push(`strpos(body, ${sqlString(state.grep)}) > 0`);
  }
  return parts.map((p) => ` AND ${p}`).join("");
}

/** Log volume per bucket of `seconds` over the range */
export function histogramSql(startMs: number, endMs: number, seconds: number, extraWhere = ""): string {
  return (
    `SELECT date_bin(INTERVAL '${seconds} seconds', "timestamp") AS t, count(*) AS n ` +
    `FROM logs WHERE ${timeWhere(startMs, endMs)}${extraWhere} GROUP BY 1 ORDER BY 1`
  );
}
