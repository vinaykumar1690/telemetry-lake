import { Fragment, useState, type ReactNode } from "react";
import type { LogRow } from "../api/query";
import { formatTimestamp } from "../time";

interface Props {
  rows: LogRow[];
  utc: boolean;
  grep: string;
  regex: boolean;
  ignoreCase: boolean;
  onFilter: (field: string, value: string) => void;
}

function matcher(grep: string, regex: boolean, ignoreCase: boolean): RegExp | null {
  if (!grep) return null;
  try {
    const source = regex ? grep.replace(/^\(\?i\)/, "") : grep.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    return new RegExp(source, ignoreCase ? "gi" : "g");
  } catch {
    return null; // Rust regex syntax the browser does not understand: no highlight
  }
}

function highlight(text: string, re: RegExp | null): ReactNode {
  if (!re) return text;
  const out: ReactNode[] = [];
  let last = 0;
  for (const m of text.matchAll(re)) {
    if (!m[0]) break;
    out.push(text.slice(last, m.index), <mark key={m.index}>{m[0]}</mark>);
    last = m.index! + m[0].length;
  }
  out.push(text.slice(last));
  return out;
}

const severityClass = (s?: string) => `sev sev-${(s ?? "unset").toLowerCase().replace(/[^a-z]/g, "")}`;

/** Columns the search does not return by default (DEFAULT_FIELDS in planner/src/schema.rs) */
export const EXTRA_FIELDS = ["service_namespace", "deployment_environment", "event_name", "scope_name"];

/** Row columns shown in the detail view, with the field name a filter on them uses */
const ROW_FIELDS: [keyof LogRow, string][] = [
  ["severity_number", "severity_number"],
  ["trace_id", "trace_id"],
  ["span_id", "span_id"],
  ["event_name", "event_name"],
  ["scope_name", "scope_name"],
  ["service_namespace", "service.namespace"],
  ["deployment_environment", "deployment.environment.name"],
  ["host_name", "host.name"],
];

function Attributes({ title, attrs, prefix, onFilter }: { title: string; attrs?: Record<string, string>; prefix: string; onFilter: Props["onFilter"] }) {
  const entries = Object.entries(attrs ?? {}).sort(([a], [b]) => a.localeCompare(b));
  if (!entries.length) return null;
  return (
    <div className="attr-group">
      <div className="attr-title">{title}</div>
      {entries.map(([k, v]) => (
        <div key={k} className="attr">
          <span className="attr-key">{k}</span>
          <button className="attr-value" title="Filter on this value" onClick={() => onFilter(`${prefix}${k}`, v)}>
            {v}
          </button>
        </div>
      ))}
    </div>
  );
}

export function ResultsTable({ rows, utc, grep, regex, ignoreCase, onFilter }: Props) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const re = matcher(grep, regex, ignoreCase);
  const key = (r: LogRow) => `${r._kafka_topic}/${r._kafka_partition}/${r._kafka_offset}/${r._record_index}`;

  const toggle = (k: string) =>
    setExpanded((s) => {
      const next = new Set(s);
      if (next.has(k)) next.delete(k);
      else next.add(k);
      return next;
    });

  return (
    <table className="results">
      <thead>
        <tr>
          <th className="col-time">Time</th>
          <th className="col-sev">Severity</th>
          <th className="col-svc">Service</th>
          <th className="col-pod">Pod</th>
          <th>Body</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => {
          const k = key(r);
          const open = expanded.has(k);
          const pod = r.resource_attributes?.["k8s.pod.name"];
          return (
            <Fragment key={k}>
              <tr className={open ? "row open" : "row"} onClick={() => toggle(k)}>
                <td className="col-time mono">{formatTimestamp(r.timestamp, utc)}</td>
                <td className="col-sev">
                  <span className={severityClass(r.severity_text)}>{r.severity_text ?? "—"}</span>
                </td>
                <td className="col-svc">{r.service_name ?? "—"}</td>
                <td className="col-pod mono">{pod ?? "—"}</td>
                <td className="body mono">{highlight(r.body ?? "", re)}</td>
              </tr>
              {open && (
                <tr className="detail">
                  <td colSpan={5}>
                    <div className="detail-grid">
                      <div className="attr-group">
                        <div className="attr-title">Record</div>
                        {r.service_name !== undefined && (
                          <div className="attr">
                            <span className="attr-key">service.name</span>
                            <button className="attr-value" onClick={() => onFilter("service.name", r.service_name!)}>
                              {r.service_name}
                            </button>
                          </div>
                        )}
                        {r.severity_text !== undefined && (
                          <div className="attr">
                            <span className="attr-key">severity_text</span>
                            <button className="attr-value" onClick={() => onFilter("severity_text", r.severity_text!)}>
                              {r.severity_text}
                            </button>
                          </div>
                        )}
                        {ROW_FIELDS.filter(([f]) => r[f] !== undefined && r[f] !== null).map(([f, field]) => (
                          <div key={String(f)} className="attr">
                            <span className="attr-key">{String(f)}</span>
                            <button className="attr-value" onClick={() => onFilter(field, String(r[f]))}>
                              {String(r[f])}
                            </button>
                          </div>
                        ))}
                        <div className="attr">
                          <span className="attr-key">kafka</span>
                          <span className="muted mono">
                            {r._kafka_topic}/{r._kafka_partition}@{r._kafka_offset}#{r._record_index}
                          </span>
                        </div>
                      </div>
                      <Attributes title="Resource attributes" attrs={r.resource_attributes} prefix="resource." onFilter={onFilter} />
                      <Attributes title="Log attributes" attrs={r.log_attributes} prefix="attr." onFilter={onFilter} />
                    </div>
                    <pre className="body-full mono">{r.body}</pre>
                  </td>
                </tr>
              )}
            </Fragment>
          );
        })}
      </tbody>
    </table>
  );
}
