# Telemetry Lake Helm Chart

This document describes the Helm chart deployment for running Kafka, MinIO, Nessie (Iceberg REST Catalog), and DuckDB in a Kubernetes cluster on Docker Desktop.

## Overview

The `telemetry-lake` Helm chart deploys four components that work together to provide a complete telemetry data lake development environment:

1. **Kafka** - Event streaming platform for buffering log events
2. **MinIO** - S3-compatible object storage for Iceberg data files
3. **Nessie** - Iceberg REST catalog (manages table metadata)
4. **DuckDB** - Interactive pod for creating/reading/writing Iceberg tables

### Data Flow

```
Log Events → Kafka (buffer) → DuckDB Appender → Iceberg Tables (MinIO + Nessie)
```

Kafka acts as a buffer for incoming telemetry/log events. The DuckDB appender consumes events from Kafka and writes them to Iceberg tables stored in MinIO, with metadata managed by Nessie.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster (Docker Desktop)                       │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                      telemetry-lake Namespace                           │ │
│  │                                                                          │ │
│  │   ┌─────────────────┐                                                   │ │
│  │   │   Kafka Pod     │                                                   │ │
│  │   │  (StatefulSet)  │                                                   │ │
│  │   │                 │                                                   │ │
│  │   │ Port: 9092      │◀──── Log Events (Producers)                       │ │
│  │   │ (Client)        │                                                   │ │
│  │   │ Port: 9093      │                                                   │ │
│  │   │ (Controller)    │                                                   │ │
│  │   └────────┬────────┘                                                   │ │
│  │            │                                                             │ │
│  │            │ Consume Events                                             │ │
│  │            ▼                                                             │ │
│  │   ┌─────────────────┐      ┌─────────────────┐                          │ │
│  │   │   DuckDB Pod    │      │   Nessie Pod    │                          │ │
│  │   │  (StatefulSet)  │      │  (Deployment)   │                          │ │
│  │   │                 │      │                 │                          │ │
│  │   │ - duckdb CLI    │─────▶│ Port: 19120     │                          │ │
│  │   │ - iceberg ext   │      │ REST Catalog    │                          │ │
│  │   │ - httpfs ext    │      │ Port: 9000      │                          │ │
│  │   │                 │      │ (Management)    │                          │ │
│  │   └────────┬────────┘      └────────┬────────┘                          │ │
│  │            │                        │                                    │ │
│  │            │    ┌───────────────────┘                                    │ │
│  │            │    │                                                        │ │
│  │            ▼    ▼                                                        │ │
│  │   ┌─────────────────────────────────────────┐                           │ │
│  │   │           MinIO (StatefulSet)           │                           │ │
│  │   │                                          │                           │ │
│  │   │  Port: 9000 (S3 API)                    │                           │ │
│  │   │  Port: 9001 (Console)                   │                           │ │
│  │   │                                          │                           │ │
│  │   │  Bucket: telemetrylake                  │                           │ │
│  │   │  PersistentVolume for data              │                           │ │
│  │   └─────────────────────────────────────────┘                           │ │
│  │                                                                          │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Kafka (StatefulSet)

Kafka provides event streaming for buffering telemetry log events before they are written to Iceberg tables. Uses KRaft mode (no Zookeeper dependency).

| Property | Value |
|----------|-------|
| Image | `apache/kafka:3.7.0` |
| Replicas | 1 |
| Client Port | 9092 |
| Controller Port | 9093 |
| Storage | PersistentVolumeClaim (2Gi default) |
| Mode | KRaft (Zookeeper-less) |

**Configuration:**
- Auto-create topics enabled
- Default partitions: 3
- Default replication factor: 1
- Log retention: 1 hour (configurable)
- Default topic for logs: `telemetry-logs`

**Services:**
- `kafka` (ClusterIP): Port 9092 - Client connections
- `kafka-headless` (Headless): Ports 9092, 9093 - StatefulSet DNS

**Usage:**
```bash
# List topics
kubectl exec -n telemetry-lake kafka-0 -- /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Create a topic
kubectl exec -n telemetry-lake kafka-0 -- /opt/kafka/bin/kafka-topics.sh --create --topic telemetry-logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Produce messages
kubectl exec -it -n telemetry-lake kafka-0 -- /opt/kafka/bin/kafka-console-producer.sh --topic telemetry-logs --bootstrap-server localhost:9092

# Consume messages
kubectl exec -it -n telemetry-lake kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh --topic telemetry-logs --from-beginning --bootstrap-server localhost:9092
```

### 2. MinIO (StatefulSet)

MinIO provides S3-compatible object storage where Iceberg stores its data files (Parquet) and metadata.

| Property | Value |
|----------|-------|
| Image | `minio/minio:latest` |
| Replicas | 1 |
| S3 API Port | 9000 |
| Console Port | 9001 |
| Storage | PersistentVolumeClaim (1Gi default) |

**Features:**
- Init container automatically creates the configured bucket on startup
- Bucket policy set to public for read/write access (required for Iceberg operations)
- Credentials stored in Kubernetes Secret
- Data persisted across pod restarts

**Services:**
- `minio` (ClusterIP): Port 9000 - S3 API endpoint
- `minio-console` (ClusterIP): Port 9001 - Web console (optional NodePort for external access)

### 3. Nessie (Deployment)

Nessie serves as the Iceberg REST catalog, managing table metadata and providing transactional guarantees.

| Property | Value |
|----------|-------|
| Image | `ghcr.io/projectnessie/nessie:latest` |
| Replicas | 1 |
| REST API Port | 19120 |
| Management Port | 9000 (health probes) |

**Configuration:**
- Warehouse name: `telemetrylake` (configurable)
- S3 endpoint: Points to MinIO service (`http://minio:9000`)
- S3 credentials: Configured via Quarkus secrets mechanism
- Path-style access enabled for MinIO compatibility
- Auth type: STATIC with Quarkus secret reference

**Environment Variables (key configuration):**
```
NESSIE_CATALOG_DEFAULT-WAREHOUSE=telemetrylake
NESSIE_CATALOG_WAREHOUSES_TELEMETRYLAKE_LOCATION=s3://telemetrylake
NESSIE_CATALOG_SERVICE_S3_DEFAULT-OPTIONS_ENDPOINT=http://minio:9000
NESSIE_CATALOG_SERVICE_S3_DEFAULT-OPTIONS_PATH-STYLE_ACCESS=true
NESSIE_CATALOG_SERVICE_S3_DEFAULT-OPTIONS_AUTH-TYPE=STATIC
NESSIE_CATALOG_SERVICE_S3_DEFAULT-OPTIONS_ACCESS-KEY=urn:nessie-secret:quarkus:my-secrets.s3-default
MY-SECRETS_S3-DEFAULT_NAME=<access-key>
MY-SECRETS_S3-DEFAULT_SECRET=<secret-key>
```

**Health Probes:**
- Liveness: `GET /q/health/live` on port 9000
- Readiness: `GET /q/health/ready` on port 9000

**Services:**
- `nessie` (ClusterIP): Port 19120 - Iceberg REST API

**Iceberg REST Endpoint:** `http://nessie:19120/iceberg/`

### 4. DuckDB (StatefulSet)

DuckDB runs as an interactive pod where you can create, write, and read Iceberg tables. The pod uses a standard Debian image and installs DuckDB at runtime.

| Property | Value |
|----------|-------|
| Image | `debian:bookworm-slim` |
| ImagePullPolicy | `IfNotPresent` |
| Replicas | 1 |
| Storage | PersistentVolumeClaim (1Gi default) |

**Runtime Installation:**
The DuckDB pod runs an init script that:
1. Installs curl, unzip, and ca-certificates via apt-get
2. Downloads DuckDB CLI v1.4.3 from GitHub releases (v1.4+ required for Iceberg REST catalog support)
3. Installs iceberg and httpfs extensions
4. Keeps the container running for interactive use

**Pre-installed Extensions:**
- `iceberg` - For reading/writing Iceberg tables
- `httpfs` - For S3/HTTP file system access

**Access Method:**
```bash
kubectl exec -it -n telemetry-lake duckdb-0 -- duckdb
```

## Service Discovery

All components communicate via Kubernetes internal DNS:

| Service | DNS Name | URL |
|---------|----------|-----|
| Kafka Bootstrap | `kafka` | `kafka:9092` |
| MinIO S3 API | `minio` | `http://minio:9000` |
| MinIO Console | `minio-console` | `http://minio-console:9001` |
| Nessie Catalog | `nessie` | `http://nessie:19120/iceberg/` |

## Helm Chart Structure

```
helm/telemetry-lake/
├── Chart.yaml                    # Chart metadata
├── values.yaml                   # Default configuration values
├── templates/
│   ├── _helpers.tpl              # Template helpers
│   ├── namespace.yaml            # Namespace definition
│   ├── secrets.yaml              # S3 credentials secret
│   ├── kafka-statefulset.yaml    # Kafka StatefulSet
│   ├── kafka-service.yaml        # Kafka Services
│   ├── minio-statefulset.yaml    # MinIO StatefulSet
│   ├── minio-service.yaml        # MinIO Services
│   ├── nessie-deployment.yaml    # Nessie Deployment
│   ├── nessie-service.yaml       # Nessie Service
│   ├── duckdb-statefulset.yaml   # DuckDB StatefulSet
│   ├── duckdb-service.yaml       # DuckDB Service
│   ├── duckdb-configmap.yaml     # DuckDB init scripts
│   └── NOTES.txt                 # Post-install instructions
└── duckdb-image/
    └── Dockerfile                # Custom DuckDB image (optional)
```

## Configuration (values.yaml)

```yaml
# Namespace for all resources
namespace: telemetry-lake

# Kafka Configuration
kafka:
  enabled: true
  image: apache/kafka:3.7.0
  replicas: 1
  storage: 2Gi
  config:
    autoCreateTopics: true
    numPartitions: 3
    defaultReplicationFactor: 1
    logRetentionHours: 1
  topics:
    logs: telemetry-logs
  resources:
    requests:
      memory: "512Mi"
      cpu: "250m"
    limits:
      memory: "1Gi"
      cpu: "1000m"

# MinIO Configuration
minio:
  image: minio/minio:latest
  accessKey: minioadmin
  secretKey: minioadmin
  bucket: telemetrylake
  storage: 1Gi
  resources:
    requests:
      memory: "256Mi"
      cpu: "100m"
    limits:
      memory: "512Mi"
      cpu: "500m"
  console:
    enabled: true
    nodePort: null  # Set to expose console externally

# Nessie Configuration
nessie:
  image: ghcr.io/projectnessie/nessie:latest
  warehouse: telemetrylake
  resources:
    requests:
      memory: "256Mi"
      cpu: "100m"
    limits:
      memory: "1Gi"
      cpu: "500m"

# DuckDB Configuration
duckdb:
  image: debian:bookworm-slim
  imagePullPolicy: IfNotPresent
  storage: 1Gi
  resources:
    requests:
      memory: "256Mi"
      cpu: "100m"
    limits:
      memory: "2Gi"
      cpu: "1000m"
```

## Deployment Instructions

### Prerequisites

1. Docker Desktop with Kubernetes enabled (or any Kubernetes cluster)
2. Helm 3.x installed
3. kubectl configured to use your cluster context

### Step 1: Install Helm Chart

```bash
cd helm/telemetry-lake
helm install telemetry-lake .
```

Note: No custom Docker image build is required. DuckDB is installed at runtime from the official GitHub releases.

### Step 2: Wait for Pods

```bash
kubectl wait --for=condition=ready pod -l app=kafka -n telemetry-lake --timeout=120s
kubectl wait --for=condition=ready pod -l app=minio -n telemetry-lake --timeout=120s
kubectl wait --for=condition=ready pod -l app=nessie -n telemetry-lake --timeout=120s
kubectl wait --for=condition=ready pod -l app=duckdb -n telemetry-lake --timeout=120s
```

### Step 3: Verify Connectivity

```bash
# Test Kafka
kubectl exec -n telemetry-lake kafka-0 -- /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Test MinIO
kubectl exec -n telemetry-lake duckdb-0 -- curl -s http://minio:9000/minio/health/live

# Test Nessie
kubectl exec -n telemetry-lake duckdb-0 -- curl -s http://nessie:19120/api/v2/config
```

### Step 4: Access DuckDB

```bash
kubectl exec -it -n telemetry-lake duckdb-0 -- duckdb
```

## Usage Examples

Once inside the DuckDB shell:

### Setup and Create an Iceberg Table

```sql
-- Load extensions
LOAD iceberg;
LOAD httpfs;

-- Configure S3 secret for MinIO
CREATE SECRET minio_secret (
    TYPE S3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    REGION 'us-east-1',
    ENDPOINT 'minio:9000',
    USE_SSL false,
    URL_STYLE 'path'
);

-- Attach to the Nessie catalog
ATTACH 'telemetrylake' AS telemetrylake (
    TYPE ICEBERG,
    ENDPOINT 'http://nessie:19120/iceberg/',
    AUTHORIZATION_TYPE 'NONE'
);

-- Create schema
CREATE SCHEMA telemetrylake.main;

-- Create a table
CREATE TABLE telemetrylake.main.users (
    id INTEGER,
    name VARCHAR,
    email VARCHAR,
    created_at TIMESTAMP
);

-- Insert data
INSERT INTO telemetrylake.main.users VALUES
    (1, 'Alice', 'alice@example.com', '2024-01-01 00:00:00'),
    (2, 'Bob', 'bob@example.com', '2024-01-02 00:00:00');

-- Query data
SELECT * FROM telemetrylake.main.users;
```

## Demo: End-to-End Verification

This section provides step-by-step commands to verify the entire stack is working correctly.

### Step 1: Verify All Pods Are Running

```bash
kubectl get pods -n telemetry-lake
```

Expected output:
```
NAME                      READY   STATUS    RESTARTS   AGE
kafka-0                   1/1     Running   0          5m
duckdb-0                  1/1     Running   0          5m
minio-0                   1/1     Running   0          5m
nessie-xxxxxxxxxx-xxxxx   1/1     Running   0          5m
```

### Step 2: Verify Kafka is Running

```bash
kubectl exec -n telemetry-lake kafka-0 -- /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Step 3: Verify DuckDB Version

```bash
kubectl exec -n telemetry-lake duckdb-0 -- duckdb -c "SELECT version();"
```

Expected output (v1.4+ required for Iceberg REST catalog):
```
┌─────────────┐
│ "version"() │
│   varchar   │
├─────────────┤
│ v1.4.3      │
└─────────────┘
```

### Step 4: Test Service Connectivity

```bash
# Test Nessie REST API
kubectl exec -n telemetry-lake duckdb-0 -- curl -s http://nessie:19120/api/v2/config
```

Expected output (JSON with Nessie configuration):
```json
{
  "defaultBranch" : "main",
  "minSupportedApiVersion" : 1,
  "maxSupportedApiVersion" : 2,
  ...
}
```

### Step 5: Create an Iceberg Table and Insert Data

Run this command to create a table and insert test data:

```bash
kubectl exec -n telemetry-lake duckdb-0 -- duckdb -c "
LOAD iceberg;
LOAD httpfs;

CREATE SECRET minio_secret (
    TYPE S3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    REGION 'us-east-1',
    ENDPOINT 'minio:9000',
    USE_SSL false,
    URL_STYLE 'path'
);

ATTACH 'telemetrylake' AS telemetrylake (
    TYPE ICEBERG,
    ENDPOINT 'http://nessie:19120/iceberg/',
    AUTHORIZATION_TYPE 'NONE'
);

CREATE SCHEMA IF NOT EXISTS telemetrylake.main;

CREATE TABLE IF NOT EXISTS telemetrylake.main.demo_users (
    id INTEGER,
    name VARCHAR,
    email VARCHAR,
    created_at TIMESTAMP
);

INSERT INTO telemetrylake.main.demo_users VALUES
    (1, 'Alice', 'alice@example.com', '2024-01-01 00:00:00'),
    (2, 'Bob', 'bob@example.com', '2024-01-02 00:00:00'),
    (3, 'Charlie', 'charlie@example.com', '2024-01-03 00:00:00');

SELECT 'Data inserted successfully' as status;
"
```

### Step 6: Query the Iceberg Table

```bash
kubectl exec -n telemetry-lake duckdb-0 -- duckdb -c "
LOAD iceberg;
LOAD httpfs;

CREATE SECRET minio_secret (
    TYPE S3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    REGION 'us-east-1',
    ENDPOINT 'minio:9000',
    USE_SSL false,
    URL_STYLE 'path'
);

ATTACH 'telemetrylake' AS telemetrylake (
    TYPE ICEBERG,
    ENDPOINT 'http://nessie:19120/iceberg/',
    AUTHORIZATION_TYPE 'NONE'
);

-- List tables
SHOW TABLES FROM telemetrylake.main;

-- Count rows
SELECT COUNT(*) as total_rows FROM telemetrylake.main.demo_users;

-- Query all data
SELECT * FROM telemetrylake.main.demo_users ORDER BY id;
"
```

Expected output:
```
┌────────────┐
│    name    │
│  varchar   │
├────────────┤
│ demo_users │
└────────────┘
┌────────────┐
│ total_rows │
│   int64    │
├────────────┤
│          3 │
└────────────┘
┌───────┬─────────┬─────────────────────┬─────────────────────┐
│  id   │  name   │        email        │     created_at      │
│ int32 │ varchar │       varchar       │      timestamp      │
├───────┼─────────┼─────────────────────┼─────────────────────┤
│     1 │ Alice   │ alice@example.com   │ 2024-01-01 00:00:00 │
│     2 │ Bob     │ bob@example.com     │ 2024-01-02 00:00:00 │
│     3 │ Charlie │ charlie@example.com │ 2024-01-03 00:00:00 │
└───────┴─────────┴─────────────────────┴─────────────────────┘
```

### Step 7: Verify Data Files in MinIO

Check that Iceberg data files (Parquet) and metadata are stored in MinIO:

```bash
kubectl exec -n telemetry-lake minio-0 -- mc alias set local http://localhost:9000 minioadmin minioadmin
kubectl exec -n telemetry-lake minio-0 -- mc ls --recursive local/telemetrylake/
```

Expected output (file names will vary):
```
[2024-01-01 00:00:00 UTC]   706B main/demo_users_.../data/xxx.parquet
[2024-01-01 00:00:00 UTC] 2.8KiB main/demo_users_.../data/xxx.avro
[2024-01-01 00:00:00 UTC] 1.7KiB main/demo_users_.../data/snap-xxx.avro
[2024-01-01 00:00:00 UTC] 1.2KiB main/demo_users_.../metadata/xxx.metadata.json
```

This confirms:
- `.parquet` - Data files containing the actual table data
- `.avro` - Manifest files tracking data files
- `.metadata.json` - Iceberg table metadata

### Step 8: Access MinIO Console (Optional)

Port-forward the MinIO console to view data via web UI:

```bash
kubectl port-forward -n telemetry-lake svc/minio-console 9001:9001
```

Then open http://localhost:9001 in your browser.
- Username: `minioadmin`
- Password: `minioadmin`

Navigate to the `telemetrylake` bucket to see the Iceberg table files.

## Troubleshooting

### Check Pod Status

```bash
kubectl get pods -n telemetry-lake
```

### View Pod Logs

```bash
# Kafka logs
kubectl logs -n telemetry-lake kafka-0

# MinIO logs
kubectl logs -n telemetry-lake minio-0

# Nessie logs
kubectl logs -n telemetry-lake -l app=nessie

# DuckDB logs
kubectl logs -n telemetry-lake duckdb-0
```

### Common Issues

**Kafka not starting:**
- Check that KRaft mode is properly configured
- Verify storage provisioner is available in your cluster
- Check logs: `kubectl logs -n telemetry-lake kafka-0`

**Nessie health check failing (503 errors):**
- Ensure the health probes are configured for port 9000 (management port), not 19120
- Check that S3 credentials are correctly configured with Quarkus secret format

**DuckDB image pull errors:**
- The default image is `debian:bookworm-slim` which should be publicly available
- Ensure `imagePullPolicy: IfNotPresent` is set

**DuckDB binary not found:**
- DuckDB requires glibc; ensure you're using a glibc-based image (Debian/Ubuntu), not Alpine

### Test MinIO Connectivity

```bash
kubectl exec -n telemetry-lake duckdb-0 -- curl http://minio:9000/minio/health/live
```

### Test Nessie Connectivity

```bash
kubectl exec -n telemetry-lake duckdb-0 -- curl http://nessie:19120/api/v2/config
```

### Test Kafka Connectivity

```bash
kubectl exec -n telemetry-lake kafka-0 -- /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

## Cleanup

```bash
helm uninstall telemetry-lake
kubectl delete namespace telemetry-lake
```

## Security Considerations

For production use, consider:

1. **Secrets Management**: Use external secrets management (e.g., Vault, AWS Secrets Manager)
2. **Network Policies**: Restrict pod-to-pod communication
3. **TLS**: Enable TLS for all services (Kafka, MinIO, Nessie)
4. **Authentication**: Configure Nessie authentication and Kafka SASL
5. **Resource Limits**: Set CPU/memory limits on all pods
6. **Kafka ACLs**: Configure access control lists for Kafka topics
