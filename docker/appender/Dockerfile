# Multi-stage build for the appender (otel_appender)
FROM ubuntu:22.04 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    ninja-build \
    git \
    pkg-config \
    librdkafka-dev \
    libboost-all-dev \
    libasio-dev \
    zlib1g-dev \
    libssl-dev \
    libgtest-dev \
    libgmock-dev \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Download and install Crow (header-only library with CMake config)
ARG CROW_VERSION=1.3.0
RUN curl -L -o /tmp/crow.tar.gz \
    "https://github.com/CrowCpp/Crow/releases/download/v${CROW_VERSION}/Crow-${CROW_VERSION}-Linux.tar.gz" \
    && tar -xzf /tmp/crow.tar.gz -C /tmp \
    && cp -r /tmp/Crow-${CROW_VERSION}-Linux/include/* /usr/local/include/ \
    && mkdir -p /usr/local/lib/cmake \
    && cp -r /tmp/Crow-${CROW_VERSION}-Linux/lib/cmake/* /usr/local/lib/cmake/ \
    && rm -rf /tmp/crow.tar.gz /tmp/Crow-${CROW_VERSION}-Linux

# Download and install pre-built DuckDB library
ARG DUCKDB_VERSION=v1.4.3
RUN curl -L -o /tmp/libduckdb.zip \
    "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/libduckdb-linux-amd64.zip" \
    && unzip /tmp/libduckdb.zip -d /tmp/duckdb \
    && cp /tmp/duckdb/duckdb.hpp /tmp/duckdb/duckdb.h /usr/local/include/ \
    && cp /tmp/duckdb/libduckdb.so /usr/local/lib/ \
    && ldconfig \
    && rm -rf /tmp/libduckdb.zip /tmp/duckdb

# Copy source code
WORKDIR /build
COPY CMakeLists.txt .
COPY src/ src/
COPY proto/ proto/
COPY tests/ tests/

# Build the appender executable
RUN cmake -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_PREFIX_PATH=/usr/local \
    -DDUCKDB_ROOT=/usr/local \
    && cmake --build build --target otel_appender -j$(nproc)

# Runtime stage
FROM ubuntu:22.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    librdkafka1 \
    zlib1g \
    libssl3 \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Download DuckDB runtime library
ARG DUCKDB_VERSION=v1.4.3
RUN curl -L -o /tmp/libduckdb.zip \
    "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/libduckdb-linux-amd64.zip" \
    && unzip /tmp/libduckdb.zip -d /tmp/duckdb \
    && cp /tmp/duckdb/libduckdb.so /usr/local/lib/ \
    && ldconfig \
    && rm -rf /tmp/libduckdb.zip /tmp/duckdb \
    && apt-get purge -y curl unzip && apt-get autoremove -y

# Copy the built executable and cppkafka library
COPY --from=builder /build/build/otel_appender /usr/local/bin/
COPY --from=builder /build/build/_deps/cppkafka-build/lib/libcppkafka.so* /usr/local/lib/

# Update library cache
RUN ldconfig

# Create non-root user with home directory for DuckDB extensions
RUN useradd -r -s /bin/false -m -d /home/appender appender && \
    mkdir -p /home/appender/.duckdb && \
    chown -R appender:appender /home/appender

USER appender
ENV HOME=/home/appender

# Default command
ENTRYPOINT ["/usr/local/bin/otel_appender"]
