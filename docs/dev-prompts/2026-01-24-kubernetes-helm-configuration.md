# Kubernetes and Helm Configuration Updates

**Date:** 2026-01-24

## User Prompt

> This application will be deployed as a kubernetes service. The helm charts for kafka, minio (s3 object store substitute) and nessie (iceberg rest catalog) is already setup in the helm directory. We will eventually convert this code base also into 2 separate kubernetes services called ingester and appender which is connected by kafka in the middle. Look at all the configurations in src/config.hpp and update it to use idiomatic kubernetes / helm application configuration. There are some references to Redpanda in the code. Remove them and replace with kafka equivalents.

---

## Analysis

The codebase had environment variables named with `REDPANDA_*` prefix (a Kafka-compatible streaming platform). While the underlying librdkafka library works with any Kafka-compatible broker, the naming was inconsistent with the Helm chart which deploys Apache Kafka.

Key findings:
- Environment variables: `REDPANDA_BROKERS`, `REDPANDA_TOPIC`, `CONSUMER_GROUP`
- Error messages referenced Redpanda in multiple files
- Helm chart already configured Apache Kafka but lacked ingester/appender service definitions
- No Kubernetes deployment templates existed for the application services

---

## Completed Changes

### Environment Variable Renames

| Old Name | New Name |
|----------|----------|
| `REDPANDA_BROKERS` | `KAFKA_BROKERS` |
| `REDPANDA_TOPIC` | `KAFKA_TOPIC` |
| `CONSUMER_GROUP` | `KAFKA_CONSUMER_GROUP` |

### Modified Files

| File | Changes |
|------|---------|
| `src/config.hpp` | Renamed environment variables from `REDPANDA_*` to `KAFKA_*`, updated error messages |
| `src/main.cpp` | Updated error messages to reference `KAFKA_*` variables |
| `src/appender/main.cpp` | Updated error messages to reference `KAFKA_*` variables |
| `CMakeLists.txt` | Removed "Redpanda" from librdkafka comment |
| `README.md` | Updated architecture diagram, configuration tables, and example commands |
| `helm/telemetry-lake/values.yaml` | Added ingester and appender service configurations |
| `helm/telemetry-lake/templates/NOTES.txt` | Added ingester/appender service documentation |

### New Helm Templates

| File | Description |
|------|-------------|
| `helm/telemetry-lake/templates/ingester-deployment.yaml` | Kubernetes Deployment for HTTP receiver service |
| `helm/telemetry-lake/templates/ingester-service.yaml` | ClusterIP Service exposing port 4318 for OTLP logs |
| `helm/telemetry-lake/templates/appender-deployment.yaml` | Kubernetes Deployment for Kafka consumer/Iceberg writer |

### Helm Values Structure

```yaml
# Ingester Configuration
ingester:
  enabled: true
  image:
    repository: telemetry-lake/ingester
    tag: latest
  replicas: 1
  service:
    type: ClusterIP
    port: 4318
  kafka:
    brokers: ""  # Defaults to kafka:9092
    topic: "telemetry-logs"
    maxInFlight: 1000
    acks: -1
    compression: "snappy"
  resources:
    requests:
      memory: "128Mi"
      cpu: "100m"
    limits:
      memory: "512Mi"
      cpu: "500m"

# Appender Configuration
appender:
  enabled: true
  image:
    repository: telemetry-lake/appender
    tag: latest
  replicas: 1
  kafka:
    brokers: ""  # Defaults to kafka:9092
    topic: "telemetry-logs"
    consumerGroup: "otel-appender"
  iceberg:
    catalogUri: ""  # Defaults to nessie endpoint
    tableName: "logs"
  s3:
    endpoint: ""  # Defaults to minio endpoint
    bucket: "telemetrylake"
  buffer:
    sizeMB: 100
    timeSeconds: 300
  resources:
    requests:
      memory: "256Mi"
      cpu: "100m"
    limits:
      memory: "1Gi"
      cpu: "1000m"
```

### Architecture Overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ OTel Agent  │────▶│  Ingester   │────▶│    Kafka    │────▶│   Appender  │
│ (HTTP POST) │     │ (port 4318) │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘
                                                                   │
                                                                   ▼
                                                            ┌─────────────┐
                                                            │   Iceberg   │
                                                            │   (S3/MinIO)│
                                                            └─────────────┘
```

### Kubernetes Service Discovery

The Helm templates use built-in helpers to resolve service endpoints:

| Service | Internal Endpoint |
|---------|-------------------|
| Kafka | `kafka:9092` |
| MinIO | `http://minio:9000` |
| Nessie | `http://nessie:19120/iceberg/` |
| Ingester | `http://ingester:4318` |

Environment variables in deployments default to these internal endpoints but can be overridden in `values.yaml` for external services.

### S3 Credentials Handling

The appender deployment references S3 credentials from Kubernetes secrets:

```yaml
- name: S3_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: s3-credentials
      key: access-key
- name: S3_SECRET_KEY
  valueFrom:
    secretKeyRef:
      name: s3-credentials
      key: secret-key
```

This follows Kubernetes best practices for secret management rather than embedding credentials in environment variables.

---

## Next Steps

1. **Build container images** for ingester and appender services
2. **Add Dockerfiles** to the repository for building the images
3. **Add health check endpoints** (`/health`, `/ready`) to the ingester HTTP server for Kubernetes probes
4. **Consider HorizontalPodAutoscaler** for the ingester based on request rate
5. **Add PodDisruptionBudget** for high availability deployments
