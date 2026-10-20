# Telemetry Lake UI

A web UI over the query service and the ingester:

- **Search**: a time range (presets or absolute, shown in UTC or local time),
  multi-select filters on the top-level fields (service, severity,
  environment, service namespace, k8s namespace, pod, host), free-form
  attribute conditions (`=`, `!=`, `contains`, `exists`, `not_exists`), grep
  (substring, optionally case-insensitive) or a regex on the body, a volume
  histogram (drag to zoom), and paged results. Expand a row to see every
  attribute; click a value to filter on it.
- **Generate logs**: synthetic OTLP/JSON logs from configurable services and
  pods over a time range (the last N hours, or between two times), with a
  severity mix, a batch size, requests in flight and a seed (the same seed
  gives the same logs). The browser builds the batches and POSTs them to the
  ingester's `/v1/logs`. Every record carries `generator.run_id`, and
  **Search these logs** opens that run in the Search tab. Rows are
  searchable after the appender's next commit (`BUFFER_TIME_SECONDS`, 60 s
  by default).

```
browser ─▶ nginx :8080 ─┬─ /             static app
                        ├─ /api/query/  → QUERY_UPSTREAM   (otel-query :8090)
                        └─ /api/ingest/ → INGEST_UPSTREAM  (ingester :4318)
```

The browser only talks to its own origin, so neither service needs CORS.
nginx resolves the upstreams per request. The UI therefore starts while a
backend is down and answers 502 on that route until it is back, and the
upstream host names must be fully qualified (no DNS search domains).

The dropdown values and the histogram come from `/v1/sql` over the selected
time range. The histogram applies the dropdown filters and grep/regex, but
not the free-form attribute conditions. The result rows apply everything.

## Develop

```bash
npm ci
npm run dev      # http://localhost:5173; /api/query -> localhost:8090, /api/ingest -> localhost:4318
npm test         # Vitest: request builder, facet SQL, generator, sender
npm run build    # type check + production build in dist/
```

Override the dev proxy targets with `QUERY_UPSTREAM` and `INGEST_UPSTREAM`.
To run the services, see [Running the Stack Locally](../README.md#running-the-stack-locally).

## Image and deployment

```bash
docker build -f docker/ui/Dockerfile -t telemetry-lake/ui:latest .   # from the repository root
docker run --rm -p 8080:8080 telemetry-lake/ui:latest                # proxies to host.docker.internal:8090 / :4318
```

The Helm chart deploys it as `svc/ui` (values under `ui:`), pointed at the
release's `query` and `ingester` Services:

```bash
kubectl port-forward -n telemetry-lake svc/ui 8080:80   # then http://localhost:8080
```

The generator posts at most `client_max_body_size` (16 MiB, the ingester's
default `MAX_BODY_BYTES`) per request. 1000 records per request is about
1 MB.
