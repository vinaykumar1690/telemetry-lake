import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError, type LogRow, search, sql } from "../api/query";
import { MultiSelect, type Option } from "../MultiSelect";
import { buildRequest, resolveRange } from "./buildRequest";
import { bucketSeconds, facetSql, histogramFilterSql, histogramSql } from "./facets";
import { type Bucket, Histogram } from "./Histogram";
import { EXTRA_FIELDS, ResultsTable } from "./ResultsTable";
import { TimeRangePicker } from "./TimeRangePicker";
import { type AttrOp, FACET_FIELDS, type SearchState } from "./types";

interface Props {
  state: SearchState;
  setState: (update: (s: SearchState) => SearchState) => void;
  runToken: number;
  utc: boolean;
}

const OPS: AttrOp[] = ["=", "!=", "contains", "exists", "not_exists"];

const message = (e: unknown) => (e instanceof ApiError ? `${e.message}${e.code !== "http" ? ` (${e.code})` : ""}` : String(e));
const aborted = (e: unknown) => e instanceof DOMException && e.name === "AbortError";

interface Results {
  rows: LogRow[];
  cursor?: string;
  elapsedMs: number;
  pages: number;
  /** The query page 1 ran; later pages must send the same one with the cursor */
  query: SearchState;
}

export function SearchPage({ state, setState, runToken, utc }: Props) {
  const [results, setResults] = useState<Results | null>(null);
  const [searching, setSearching] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [facets, setFacets] = useState<Record<string, Option[]>>({});
  const [facetsLoading, setFacetsLoading] = useState(false);
  const [histogram, setHistogram] = useState<{ buckets: Bucket[]; startMs: number; endMs: number; bucketMs: number; error?: string } | null>(null);
  const [histLoading, setHistLoading] = useState(false);
  const searchAbort = useRef<AbortController | null>(null);
  const sideAbort = useRef<AbortController | null>(null);

  const update = (patch: Partial<SearchState>) => setState((s) => ({ ...s, ...patch }));

  const loadSide = useCallback(async (query: SearchState) => {
    sideAbort.current?.abort();
    const controller = new AbortController();
    sideAbort.current = controller;
    let range;
    try {
      range = resolveRange(query.range);
    } catch {
      return;
    }
    const { startMs, endMs } = range;
    const seconds = bucketSeconds(startMs, endMs);
    setHistLoading(true);
    setFacetsLoading(true);

    const hist = sql<{ t: string; n: number }>(histogramSql(startMs, endMs, seconds, histogramFilterSql(query, FACET_FIELDS)), controller.signal)
      .then((rows) =>
        setHistogram({ buckets: rows.map((r) => ({ t: Date.parse(r.t), n: Number(r.n) })), startMs, endMs, bucketMs: seconds * 1000 }),
      )
      .catch((e) => !aborted(e) && setHistogram({ buckets: [], startMs, endMs, bucketMs: seconds * 1000, error: message(e) }))
      .finally(() => !controller.signal.aborted && setHistLoading(false));

    const facet = Promise.all(
      FACET_FIELDS.map(async (f) => {
        const rows = await sql<{ v: string; n: number }>(facetSql(f.sql, startMs, endMs), controller.signal);
        return [f.field, rows.map((r) => ({ value: String(r.v), count: Number(r.n) }))] as const;
      }),
    )
      .then((entries) => setFacets(Object.fromEntries(entries)))
      .catch((e) => !aborted(e) && setFacets({}))
      .finally(() => !controller.signal.aborted && setFacetsLoading(false));

    await Promise.all([hist, facet]);
  }, []);

  const run = useCallback(
    async (query: SearchState, more?: Results) => {
      searchAbort.current?.abort();
      const controller = new AbortController();
      searchAbort.current = controller;
      setSearching(true);
      setError(null);
      if (!more) void loadSide(query);
      try {
        const response = await search(buildRequest(query, more?.cursor, EXTRA_FIELDS), controller.signal);
        setResults({
          rows: more ? [...more.rows, ...response.rows] : response.rows,
          cursor: response.next_cursor,
          elapsedMs: response.stats.elapsed_ms,
          pages: (more?.pages ?? 0) + 1,
          query,
        });
      } catch (e) {
        if (aborted(e)) return;
        setError(message(e));
        if (!more) setResults(null);
      } finally {
        if (!controller.signal.aborted) setSearching(false);
      }
    },
    [loadSide],
  );

  // Initial search, and whenever another tab asks for one
  const latest = useRef(state);
  latest.current = state;
  useEffect(() => {
    void run(latest.current);
  }, [runToken, run]);

  const submit = (patch: Partial<SearchState> = {}) => {
    const next = { ...state, ...patch };
    setState(() => next);
    void run(next);
  };

  const setFilter = (field: string, values: string[]) => update({ filters: { ...state.filters, [field]: values } });

  /** From a click on a value in the results: top-level fields go to their dropdown, the rest become attribute rows */
  const addFilter = (field: string, value: string) => {
    const facet = FACET_FIELDS.find((f) => f.field === field || `resource.${f.field}` === field);
    if (facet) {
      const current = state.filters[facet.field] ?? [];
      if (!current.includes(value)) submit({ filters: { ...state.filters, [facet.field]: [...current, value] } });
    } else {
      submit({ attrs: [...state.attrs, { field, op: "=", value }] });
    }
  };

  const activeFilters = Object.values(state.filters).some((v) => v.length) || state.attrs.length > 0 || state.grep;
  const histNote =
    state.attrs.length > 0 ? "attribute rows not applied here · drag to zoom" : undefined;

  return (
    <div className="search-page">
      <form
        className="query-bar"
        onSubmit={(e) => {
          e.preventDefault();
          submit();
        }}
      >
        <div className="grep">
          <input
            className="grep-input mono"
            placeholder={state.regex ? "Regular expression on the body (Rust syntax)" : "Search the log body (substring)"}
            value={state.grep}
            onChange={(e) => update({ grep: e.target.value })}
          />
          <label className="check">
            <input type="checkbox" checked={state.ignoreCase} onChange={(e) => update({ ignoreCase: e.target.checked })} />
            Ignore case
          </label>
          <label className="check">
            <input type="checkbox" checked={state.regex} onChange={(e) => update({ regex: e.target.checked })} />
            Regex
          </label>
        </div>
        <TimeRangePicker range={state.range} utc={utc} onChange={(range) => submit({ range })} />
        <button type="submit" className="primary">
          {searching ? "Searching…" : "Search"}
        </button>
      </form>

      <div className="filters">
        {FACET_FIELDS.map((f) => (
          <MultiSelect
            key={f.field}
            label={f.label}
            options={facets[f.field] ?? []}
            loading={facetsLoading && !facets[f.field]}
            selected={state.filters[f.field] ?? []}
            onChange={(values) => setFilter(f.field, values)}
          />
        ))}
        <button
          type="button"
          className="ghost small"
          onClick={() => update({ attrs: [...state.attrs, { field: "", op: "=", value: "" }] })}
        >
          + Attribute filter
        </button>
        {activeFilters && (
          <button type="button" className="ghost small" onClick={() => submit({ filters: {}, attrs: [], grep: "" })}>
            Clear all
          </button>
        )}
      </div>

      {state.attrs.length > 0 && (
        <div className="attr-rows">
          {state.attrs.map((a, i) => {
            const set = (patch: Partial<typeof a>) =>
              update({ attrs: state.attrs.map((x, j) => (j === i ? { ...x, ...patch } : x)) });
            return (
              <div className="attr-row" key={i}>
                <input
                  className="mono"
                  placeholder="field, e.g. http.status_code or attr.user.id"
                  value={a.field}
                  onChange={(e) => set({ field: e.target.value })}
                />
                <select value={a.op} onChange={(e) => set({ op: e.target.value as AttrOp })}>
                  {OPS.map((op) => (
                    <option key={op}>{op}</option>
                  ))}
                </select>
                {a.op !== "exists" && a.op !== "not_exists" && (
                  <input className="mono" placeholder="value" value={a.value} onChange={(e) => set({ value: e.target.value })} />
                )}
                <button
                  type="button"
                  className="ghost small"
                  aria-label="Remove"
                  onClick={() => update({ attrs: state.attrs.filter((_, j) => j !== i) })}
                >
                  ✕
                </button>
              </div>
            );
          })}
        </div>
      )}

      {histogram && (
        <Histogram
          {...histogram}
          utc={utc}
          loading={histLoading}
          note={histNote}
          onSelect={(startMs, endMs) =>
            submit({ range: { kind: "absolute", start: new Date(startMs).toISOString(), end: new Date(endMs).toISOString() } })
          }
        />
      )}

      {error && <div className="error-banner">{error}</div>}

      {results && (
        <div className="results-wrap">
          <div className="results-head">
            <span>
              {results.rows.length.toLocaleString()} rows loaded{results.cursor ? ", more available" : ""}
            </span>
            <span className="muted">
              {results.pages > 1 ? `${results.pages} pages · ` : ""}last page {results.elapsedMs} ms
            </span>
            <span className="spacer" />
            <label className="check">
              Order
              <select value={state.order} onChange={(e) => submit({ order: e.target.value as "asc" | "desc" })}>
                <option value="desc">newest first</option>
                <option value="asc">oldest first</option>
              </select>
            </label>
            <label className="check">
              Page size
              <select value={state.limit} onChange={(e) => submit({ limit: Number(e.target.value) })}>
                {[50, 100, 250, 500, 1000].map((n) => (
                  <option key={n}>{n}</option>
                ))}
              </select>
            </label>
          </div>
          {results.rows.length === 0 ? (
            <div className="empty">
              No logs match. Widen the time range or remove filters. Logs you just sent appear after the appender's next commit
              (up to a minute by default).
            </div>
          ) : (
            <div className="results-scroll">
            <ResultsTable
              rows={results.rows}
              utc={utc}
              grep={results.query.grep}
              regex={results.query.regex}
              ignoreCase={results.query.ignoreCase}
              onFilter={addFilter}
            />
            </div>
          )}
          {results.cursor && (
            <button className="load-more" disabled={searching} onClick={() => void run(results.query, results)}>
              {searching ? "Loading…" : "Load more"}
            </button>
          )}
        </div>
      )}
    </div>
  );
}
