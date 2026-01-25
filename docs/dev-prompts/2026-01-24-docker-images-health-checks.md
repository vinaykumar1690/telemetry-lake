# Docker Images and Health Check Endpoints

**Date:** 2026-01-24

## Initial Requests

1. Build container images for ingester and appender services
2. Add Dockerfiles to the repository for building the images
3. Add health check endpoints (`/health`, `/ready`) to the ingester HTTP server for Kubernetes probes

## Changes Made

### 1. Health Check Endpoints (Ingester HTTP Server)

**Files Modified:**
- `src/ingester/http_server.cpp` - Added `/health` and `/ready` routes
- `src/ingester/queue_producer.hpp` - Added `isReady()` method

**Implementation:**
- `/health` - Simple liveness probe, always returns 200 OK
- `/ready` - Readiness probe, checks if queue producer is initialized via `isReady()` method

```cpp
// Health check endpoint for Kubernetes liveness probe
CROW_ROUTE(app, "/health")
    ([](){
        return crow::response(200, "OK");
    });

// Readiness check endpoint for Kubernetes readiness probe
CROW_ROUTE(app, "/ready")
    ([queue_producer](){
        if (queue_producer && !queue_producer->isReady()) {
            return crow::response(503, "Queue producer not ready");
        }
        return crow::response(200, "OK");
    });
```

### 2. Dockerfiles

**Files Created:**
- `docker/ingester/Dockerfile` - Multi-stage build for otel_receiver
- `docker/appender/Dockerfile` - Multi-stage build for otel_appender
- `.dockerignore` - Excludes unnecessary files from build context

**Key Features:**
- Multi-stage builds (builder + runtime) for smaller final images
- Ubuntu 22.04 base image
- Non-root user for security
- Health check configured for ingester

### 3. DuckDB Binary Distribution (Build Optimization)

**Problem:** DuckDB was being compiled from source via FetchContent, which is extremely slow due to DuckDB's large codebase and unity builds.

**Solution:** Use pre-built libduckdb binary distribution instead.

**Files Modified:**
- `CMakeLists.txt` - Replaced FetchContent for DuckDB with find_library logic
- `docker/appender/Dockerfile` - Downloads libduckdb v1.4.3 binary from GitHub releases

**CMakeLists.txt Changes:**
```cmake
# Removed FetchContent for DuckDB
# Added find_library logic with DUCKDB_ROOT configuration option

find_library(DUCKDB_LIBRARY
  NAMES duckdb
  PATHS ${DUCKDB_ROOT}/lib /usr/local/lib /usr/lib
)

find_path(DUCKDB_INCLUDE_DIR
  NAMES duckdb.hpp
  PATHS ${DUCKDB_ROOT}/include /usr/local/include /usr/include
)
```

**Dockerfile Changes:**
```dockerfile
# Download pre-built DuckDB library
ARG DUCKDB_VERSION=v1.4.3
RUN curl -L -o /tmp/libduckdb.zip \
    "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/libduckdb-linux-amd64.zip" \
    && unzip /tmp/libduckdb.zip -d /tmp/duckdb \
    && cp /tmp/duckdb/duckdb.hpp /tmp/duckdb/duckdb.h /usr/local/include/ \
    && cp /tmp/duckdb/libduckdb.so /usr/local/lib/ \
    && ldconfig
```

### 4. Bug Fix: Missing Include

**File Modified:**
- `src/config.hpp` - Added `#include <cstring>` for `strlen()` function

## Final File Structure

```
docker/
├── ingester/
│   └── Dockerfile
└── appender/
    └── Dockerfile
.dockerignore
```

## Build Commands

```bash
# Build ingester image
docker build -f docker/ingester/Dockerfile -t telemetry-lake/ingester:latest .

# Build appender image
docker build -f docker/appender/Dockerfile -t telemetry-lake/appender:latest .
```

## Image Sizes

- `telemetry-lake/ingester:latest` - ~93 MB
- `telemetry-lake/appender:latest` - TBD (includes libduckdb runtime)
