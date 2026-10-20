# Multi-stage build for the query service (otel-query)
# Build from the repository root: docker build -f docker/query/Dockerfile -t telemetry-lake/query .
FROM rust:1-bookworm AS builder

# aws-lc-sys (rustls' crypto backend) builds with cmake
RUN apt-get update && apt-get install -y --no-install-recommends cmake clang \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
# Dependencies first, so source edits do not rebuild DataFusion
COPY query/Cargo.toml query/Cargo.lock ./
COPY query/crates/logquery/Cargo.toml crates/logquery/
COPY query/crates/planner/Cargo.toml crates/planner/
COPY query/crates/logscan/Cargo.toml crates/logscan/
COPY query/crates/query-server/Cargo.toml crates/query-server/
RUN for c in logquery planner logscan; do mkdir -p crates/$c/src && echo > crates/$c/src/lib.rs; done \
    && mkdir -p crates/query-server/src && echo 'fn main() {}' > crates/query-server/src/main.rs \
    && echo > crates/query-server/src/lib.rs \
    && cargo build --release --locked -p query-server \
    && rm -rf crates/*/src

COPY query/crates/ crates/
# Make sure cargo sees the real sources as newer than the stubs
RUN find crates -name '*.rs' -exec touch {} + \
    && cargo build --release --locked -p query-server \
    && strip target/release/otel-query

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 query
COPY --from=builder /build/target/release/otel-query /usr/local/bin/otel-query
USER query
ENV HTTP_PORT=8090
# 8090: HTTP API (coordinator) or health (worker); 50051: worker gRPC (otel-query --worker)
EXPOSE 8090 50051
HEALTHCHECK --interval=10s --timeout=3s CMD curl -sf http://localhost:8090/health || exit 1
ENTRYPOINT ["/usr/local/bin/otel-query"]
