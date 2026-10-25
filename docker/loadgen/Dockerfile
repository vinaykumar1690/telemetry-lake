# Load generator (otel-loadgen): controller (default) or worker (--worker)
# Build from the repository root: docker build -f docker/loadgen/Dockerfile -t telemetry-lake/loadgen .
FROM rust:1-bookworm AS builder

WORKDIR /build
# Dependencies first, so source edits do not rebuild them
COPY loadgen/Cargo.toml loadgen/Cargo.lock ./
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs && echo > src/lib.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY loadgen/src/ src/
# Make sure cargo sees the real sources as newer than the stubs
RUN find src -name '*.rs' -exec touch {} + \
    && cargo build --release --locked \
    && strip target/release/otel-loadgen

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 loadgen
COPY --from=builder /build/target/release/otel-loadgen /usr/local/bin/otel-loadgen
USER loadgen
ENV HTTP_PORT=8095
EXPOSE 8095
HEALTHCHECK --interval=10s --timeout=3s CMD curl -sf http://localhost:8095/health || exit 1
ENTRYPOINT ["/usr/local/bin/otel-loadgen"]
