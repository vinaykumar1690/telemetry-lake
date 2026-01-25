# Migrate FetchContent Dependencies to Precompiled Binaries

**Date:** 2026-01-24

## Request

Update CMakeLists.txt to prefer precompiled binaries instead of FetchContent for building dependencies locally. Requirements:
- Update one dependency at a time with version confirmation
- Don't hardcode "latest" when downloading precompiled binaries
- Ensure binaries work on macOS (local development)
- Update ingester and appender Dockerfiles accordingly

## Original FetchContent Dependencies

| Dependency | Version | Purpose |
|------------|---------|---------|
| Crow | v1.3.0.0 | Web framework |
| protobuf | v25.3 | Protocol Buffers |
| opentelemetry_proto | v1.3.1 | OTel .proto definitions |
| zlib | v1.3.1 | Compression (fallback) |
| googletest | v1.14.0 | Testing framework |
| cppkafka | v0.3.1 | Kafka C++ wrapper |

## Changes Made

### Dependencies Migrated to Precompiled

| Dependency | New Version | macOS Source | Docker Source |
|------------|-------------|--------------|---------------|
| Crow | 1.3.0 | Homebrew | GitHub release tarball |
| googletest | 1.17.0 (mac) / 1.11.0 (docker) | Homebrew | apt libgtest-dev |

### Dependencies Kept as FetchContent

| Dependency | Version | Reason |
|------------|---------|--------|
| protobuf | v25.3 | User preference for consistent builds across platforms |
| opentelemetry_proto | v1.3.1 | Just .proto source files, not a compiled library |
| cppkafka | v0.3.1 | Not available in Homebrew or apt |

## File Changes

### CMakeLists.txt

1. **Crow**: Changed from `FetchContent_Declare` to `find_package(Crow REQUIRED)`
   - Target name is `Crow::Crow` (not just `Crow`)

2. **googletest**: Changed from `FetchContent_Declare` to `find_package(GTest REQUIRED)`

3. **DuckDB**: Made optional with `DUCKDB_FOUND` flag
   - Appender target only builds when DuckDB is found
   - Allows ingester to build without DuckDB installed

### docker/ingester/Dockerfile

```dockerfile
# Added Crow installation
ARG CROW_VERSION=1.3.0
RUN curl -L -o /tmp/crow.tar.gz \
    "https://github.com/CrowCpp/Crow/releases/download/v${CROW_VERSION}/Crow-${CROW_VERSION}-Linux.tar.gz" \
    && tar -xzf /tmp/crow.tar.gz -C /tmp \
    && cp -r /tmp/Crow-${CROW_VERSION}-Linux/include/* /usr/local/include/ \
    && mkdir -p /usr/local/lib/cmake \
    && cp -r /tmp/Crow-${CROW_VERSION}-Linux/lib/cmake/* /usr/local/lib/cmake/ \
    && rm -rf /tmp/crow.tar.gz /tmp/Crow-${CROW_VERSION}-Linux

# Added to apt packages
libgtest-dev \
libgmock-dev \

# Added CMAKE_PREFIX_PATH to cmake command
-DCMAKE_PREFIX_PATH=/usr/local \
```

### docker/appender/Dockerfile

Same Crow installation added, plus `CMAKE_PREFIX_PATH=/usr/local` in cmake command.

## Issues Encountered and Solutions

### 1. Crow version check failed
- **Problem**: `find_package(Crow 1.3.0 REQUIRED)` failed because CrowConfig.cmake doesn't export version info
- **Solution**: Remove version requirement: `find_package(Crow REQUIRED)`

### 2. Linker error: library 'Crow' not found
- **Problem**: CMakeLists.txt linked against `Crow` but the actual target is `Crow::Crow`
- **Solution**: Update all `target_link_libraries` to use `Crow::Crow`

### 3. Docker build: CrowConfig.cmake not found
- **Problem**: Wrong release asset filename and missing cmake directory
- **Solution**:
  - Fixed URL: `Crow-${VERSION}-Linux.tar.gz` (capital C, no 'v' prefix in filename)
  - Added `mkdir -p /usr/local/lib/cmake` before copying
  - Added `-DCMAKE_PREFIX_PATH=/usr/local` to cmake command

### 4. Ingester Docker build failed on DuckDB
- **Problem**: CMakeLists.txt had `FATAL_ERROR` if DuckDB not found, but ingester doesn't need DuckDB
- **Solution**: Made DuckDB optional with `DUCKDB_FOUND` variable; wrapped appender target in `if(DUCKDB_FOUND)`

## Installation Commands

### macOS (Homebrew)

```bash
brew install crow
brew install googletest
brew install duckdb  # Only needed for appender
brew install librdkafka  # Already required
```

### Verify Build

```bash
# Clean build
rm -rf build && cmake -B build

# Build ingester only (no DuckDB needed)
cmake --build build --target otel_receiver

# Build appender (requires DuckDB)
cmake --build build --target otel_appender
```

## Docker Build Commands

```bash
# Build ingester image
docker build -f docker/ingester/Dockerfile -t otel-ingester .

# Build appender image
docker build -f docker/appender/Dockerfile -t otel-appender .
```
