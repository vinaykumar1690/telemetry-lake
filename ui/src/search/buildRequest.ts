import type { Condition, SearchRequest } from "../api/query";
import type { AttrFilter, SearchState, TimeRange } from "./types";

const UNIT_MS: Record<string, number> = { s: 1e3, m: 6e4, h: 36e5, d: 864e5, w: 6048e5 };

export function rangeBounds(range: TimeRange): { start: string; end: string } {
  return range.kind === "relative" ? { start: `now-${range.last}`, end: "now" } : { start: range.start, end: range.end };
}

/** The range as concrete instants (relative ranges against `now`) */
export function resolveRange(range: TimeRange, now = Date.now()): { startMs: number; endMs: number } {
  if (range.kind === "absolute") return { startMs: Date.parse(range.start), endMs: Date.parse(range.end) };
  const match = /^(\d+)([smhdw])$/.exec(range.last);
  if (!match) throw new Error(`bad relative range ${range.last}`);
  return { startMs: now - Number(match[1]) * UNIT_MS[match[2]], endMs: now };
}

function attrCondition(attr: AttrFilter): Condition | null {
  const field = attr.field.trim();
  if (!field) return null;
  if (attr.op === "exists" || attr.op === "not_exists") return { field, op: attr.op };
  return { field, op: attr.op, value: attr.value };
}

export function buildRequest(state: SearchState, cursor?: string, extraFields: string[] = []): SearchRequest {
  const request: SearchRequest = { ...rangeBounds(state.range), order: state.order, limit: state.limit };

  const filters: Record<string, string | string[]> = {};
  for (const [field, values] of Object.entries(state.filters)) {
    if (values.length === 1) filters[field] = values[0];
    else if (values.length > 1) filters[field] = values;
  }
  if (Object.keys(filters).length) request.filters = filters;

  const conditions = state.attrs.map(attrCondition).filter((c): c is Condition => c !== null);
  if (conditions.length === 1) request.where = conditions[0];
  else if (conditions.length > 1) request.where = { and: conditions };

  if (state.grep) {
    if (state.regex) request.regex = state.ignoreCase ? `(?i)${state.grep}` : state.grep;
    else {
      request.grep = state.grep;
      if (state.ignoreCase) request.grep_ignore_case = true;
    }
  }
  if (extraFields.length) request.fields = extraFields;
  if (cursor) request.cursor = cursor;
  return request;
}
