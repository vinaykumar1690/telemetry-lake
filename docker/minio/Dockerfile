# MinIO server and client built from source at pinned release tags. MinIO no
# longer publishes community images (minio/minio, quay.io/minio/minio) or
# binaries (dl.min.io answers 410), so the chart's object store is built here.
# Build from the repository root: docker build -f docker/minio/Dockerfile -t telemetry-lake/minio .
FROM golang:1.25-bookworm AS builder
ARG MINIO_RELEASE=RELEASE.2025-10-15T17-29-55Z
ARG MC_RELEASE=RELEASE.2025-08-13T08-35-41Z
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
WORKDIR /src
# Version stamps as MinIO's own release builds set them (buildscripts/gen-ldflags.go):
# RELEASE.2025-10-15T17-29-55Z -> 2025-10-15T17:29:55Z
RUN git clone --depth 1 --branch ${MINIO_RELEASE} https://github.com/minio/minio.git \
    && cd minio && v=$(echo ${MINIO_RELEASE#RELEASE.} | sed -E 's/T([0-9]+)-([0-9]+)-([0-9]+)Z/T\1:\2:\3Z/') \
    && p=github.com/minio/minio/cmd \
    && go build -ldflags "-s -w -X $p.Version=$v -X $p.ReleaseTag=${MINIO_RELEASE} -X $p.CommitID=$(git rev-parse HEAD) -X $p.ShortCommitID=$(git rev-parse --short=12 HEAD)" -o /out/minio .
RUN git clone --depth 1 --branch ${MC_RELEASE} https://github.com/minio/mc.git \
    && cd mc && v=$(echo ${MC_RELEASE#RELEASE.} | sed -E 's/T([0-9]+)-([0-9]+)-([0-9]+)Z/T\1:\2:\3Z/') \
    && p=github.com/minio/mc/cmd \
    && go build -ldflags "-s -w -X $p.Version=$v -X $p.ReleaseTag=${MC_RELEASE} -X $p.CommitID=$(git rev-parse HEAD) -X $p.ShortCommitID=$(git rev-parse --short=12 HEAD)" -o /out/mc .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/minio /out/mc /usr/local/bin/
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
