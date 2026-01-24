# DuckDB image with Iceberg and HTTPFS extensions pre-installed
FROM ubuntu:22.04

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set DuckDB version
ARG DUCKDB_VERSION=1.1.3

# Download and install DuckDB CLI
RUN curl -L "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip" -o /tmp/duckdb.zip \
    && unzip /tmp/duckdb.zip -d /usr/local/bin \
    && chmod +x /usr/local/bin/duckdb \
    && rm /tmp/duckdb.zip

# Create data directory
RUN mkdir -p /data
WORKDIR /data

# Pre-install extensions (they will be cached in the image)
RUN duckdb -c "INSTALL iceberg; INSTALL httpfs;"

# Default command - keep container running
CMD ["tail", "-f", "/dev/null"]
