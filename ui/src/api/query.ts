// Client for otel-query (query/README.md), reached through the /api/query proxy

const BASE = "/api/query";

export type Condition =
  | { and: Condition[] }
  | { or: Condition[] }
  | { not: Condition }
  | { field: string; op: string; value?: string | number; values?: string[] };

export interface SearchRequest {
  start: string;
  end?: string;
  filters?: Record<string, string | string[]>;
  where?: Condition;
  grep?: string;
  grep_ignore_case?: boolean;
  regex?: string;
  fields?: string[];
  order?: "asc" | "desc";
  limit?: number;
  cursor?: string;
}

export interface LogRow {
  timestamp: string;
  severity_text?: string;
  severity_number?: number;
  body?: string;
  service_name?: string;
  resource_attributes?: Record<string, string>;
  log_attributes?: Record<string, string>;
  _kafka_topic: string;
  _kafka_partition: number;
  _kafka_offset: number;
  _record_index: number;
  [field: string]: unknown;
}

export interface SearchResponse {
  rows: LogRow[];
  next_cursor?: string;
  snapshot_id?: number;
  stats: { rows: number; elapsed_ms: number };
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

async function failure(response: Response): Promise<ApiError> {
  const text = await response.text().catch(() => "");
  try {
    const { error } = JSON.parse(text);
    if (error?.message) return new ApiError(response.status, error.code ?? "error", error.message);
  } catch {
    // not the service's JSON error (e.g. the proxy's 502 page)
  }
  const hint = response.status === 502 || response.status === 504 ? " (is the query service up?)" : "";
  return new ApiError(response.status, "http", `HTTP ${response.status}${hint}`);
}

export async function search(request: SearchRequest, signal?: AbortSignal): Promise<SearchResponse> {
  const response = await fetch(`${BASE}/v1/search`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request),
    signal,
  });
  if (!response.ok) throw await failure(response);
  return response.json();
}

/** Parses an NDJSON body; a trailing error object (mid-stream failure) is thrown */
export function parseNdjson<T>(text: string): T[] {
  const rows: T[] = [];
  for (const line of text.split("\n")) {
    if (!line.trim()) continue;
    const value = JSON.parse(line);
    if (value && typeof value === "object" && "error" in value && Object.keys(value).length === 1) {
      throw new ApiError(500, value.error?.code ?? "error", value.error?.message ?? "query failed");
    }
    rows.push(value);
  }
  return rows;
}

export async function sql<T = Record<string, unknown>>(query: string, signal?: AbortSignal, maxRows?: number): Promise<T[]> {
  const response = await fetch(`${BASE}/v1/sql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sql: query, max_rows: maxRows }),
    signal,
  });
  if (!response.ok) throw await failure(response);
  return parseNdjson<T>(await response.text());
}

export async function ready(signal?: AbortSignal): Promise<boolean> {
  try {
    const response = await fetch(`${BASE}/ready`, { signal });
    return response.ok;
  } catch {
    return false;
  }
}
