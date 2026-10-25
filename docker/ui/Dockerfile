# Web UI: static Vite build served by nginx, which also proxies the query
# service (/api/query/), the ingester (/api/ingest/) and the load generator's
# control API (/api/loadgen/).
# Build from the repository root: docker build -f docker/ui/Dockerfile -t telemetry-lake/ui .
FROM node:22-alpine AS builder
WORKDIR /build
COPY ui/package.json ui/package-lock.json ./
RUN npm ci
COPY ui/ ./
RUN npm test && npm run build

FROM nginxinc/nginx-unprivileged:alpine
# Rendered to /etc/nginx/conf.d/default.conf at startup, replacing the stock server
COPY ui/nginx.conf.template /etc/nginx/templates/default.conf.template
COPY --from=builder /build/dist /usr/share/nginx/html
# Upstreams for local `docker run`; the Helm chart sets the in-cluster services
ENV QUERY_UPSTREAM=http://host.docker.internal:8090 \
    INGEST_UPSTREAM=http://host.docker.internal:4318 \
    LOADGEN_UPSTREAM=http://host.docker.internal:8095 \
    NGINX_ENTRYPOINT_LOCAL_RESOLVERS=1
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s CMD wget -qO- http://127.0.0.1:8080/healthz || exit 1
