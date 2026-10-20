export type TimeRange =
  | { kind: "relative"; last: string } // "15m", "1h", "7d"
  | { kind: "absolute"; start: string; end: string }; // RFC 3339

export type AttrOp = "=" | "!=" | "contains" | "exists" | "not_exists";

export interface AttrFilter {
  field: string;
  op: AttrOp;
  value: string;
}

export interface SearchState {
  range: TimeRange;
  /** Top-level field (query field name) -> selected values; more than one is IN */
  filters: Record<string, string[]>;
  attrs: AttrFilter[];
  grep: string;
  ignoreCase: boolean;
  regex: boolean;
  order: "asc" | "desc";
  limit: number;
}

/** The filter dropdowns: query field name and the SQL expression facets group by */
export interface FacetField {
  field: string;
  label: string;
  sql: string;
}

export const FACET_FIELDS: FacetField[] = [
  { field: "service.name", label: "Service", sql: "service_name" },
  { field: "severity_text", label: "Severity", sql: "severity_text" },
  { field: "deployment.environment.name", label: "Environment", sql: "deployment_environment" },
  { field: "service.namespace", label: "Service namespace", sql: "service_namespace" },
  { field: "k8s.namespace.name", label: "K8s namespace", sql: "resource_attributes['k8s.namespace.name']" },
  { field: "k8s.pod.name", label: "Pod", sql: "resource_attributes['k8s.pod.name']" },
  { field: "host.name", label: "Host", sql: "host_name" },
];

export const DEFAULT_STATE: SearchState = {
  range: { kind: "relative", last: "1h" },
  filters: {},
  attrs: [],
  grep: "",
  ignoreCase: true,
  regex: false,
  order: "desc",
  limit: 100,
};
