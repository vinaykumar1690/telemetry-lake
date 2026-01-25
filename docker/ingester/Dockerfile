# Multi-stage build for the ingester (otel_receiver)
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

# Copy source code
WORKDIR /build
COPY CMakeLists.txt .
COPY src/ src/
COPY proto/ proto/
COPY tests/ tests/

# Build the ingester executable
RUN cmake -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_PREFIX_PATH=/usr/local \
    && cmake --build build --target otel_receiver -j$(nproc)

# Runtime stage
FROM ubuntu:22.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    librdkafka1 \
    libboost-system1.74.0 \
    zlib1g \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the built executable
COPY --from=builder /build/build/otel_receiver /usr/local/bin/

# Create non-root user
RUN useradd -r -s /bin/false ingester
USER ingester

# Expose OTel HTTP port
EXPOSE 4318

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:4318/health || exit 1

# Default command
ENTRYPOINT ["/usr/local/bin/otel_receiver"]
