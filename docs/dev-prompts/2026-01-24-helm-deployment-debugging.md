# Helm Deployment and Debugging

**Date:** 2026-01-24

## User Prompt

> We now have a working Dockerfile and image for ingester and appender service. We also have the helm charts for the ingester and appender service. Review these and upgrade the telemetry-lake helm chart with the new ingester and appender services. Make sure they come up successfully. If not, debug and fix.

---

## Analysis

The telemetry-lake Helm chart was ready with templates for all components including ingester and appender services. Docker images had been built locally. The task involved deploying the Helm chart and debugging any issues that arose during pod startup.

### Components Reviewed

| Component | Files |
|-----------|-------|
| Ingester Dockerfile | `docker/ingester/Dockerfile` |
| Appender Dockerfile | `docker/appender/Dockerfile` |
| Helm Chart | `helm/telemetry-lake/` |
| Ingester Deployment | `helm/telemetry-lake/templates/ingester-deployment.yaml` |
| Appender Deployment | `helm/telemetry-lake/templates/appender-deployment.yaml` |
| Values | `helm/telemetry-lake/values.yaml` |

---

## Issues Encountered and Fixed

### Issue 1: ImagePullBackOff on kind Cluster

**Problem:** After installing the Helm chart, ingester and appender pods showed `ImagePullBackOff` status. The cluster was trying to pull images from Docker Hub (`docker.io/telemetry-lake/ingester:latest`) but they only existed locally.

**Root Cause:** Docker Desktop's Kubernetes uses kind internally. Local Docker images are not automatically available to kind cluster nodes.

**Solution:** Load images directly into the kind cluster nodes using containerd:

```bash
for node in desktop-control-plane desktop-worker desktop-worker2 desktop-worker3; do
  docker save telemetry-lake/ingester:latest | docker exec -i $node ctr -n=k8s.io images import -
  docker save telemetry-lake/appender:latest | docker exec -i $node ctr -n=k8s.io images import -
done
```

### Issue 2: Missing libcppkafka.so in Appender Image

**Problem:** After loading images, the appender pod showed `Error` status with restart loop.

**Error Log:**
```
/usr/local/bin/otel_appender: error while loading shared libraries: libcppkafka.so.0.3.1: cannot open shared object file: No such file or directory
```

**Root Cause:** The appender Dockerfile copied only the executable from the builder stage, but `cppkafka` is compiled from source during build (via CMake FetchContent) and produces a shared library that wasn't being copied to the runtime image.

**Solution:** Updated `docker/appender/Dockerfile` to copy the cppkafka library:

```dockerfile
# Copy the built executable and cppkafka library
COPY --from=builder /build/build/otel_appender /usr/local/bin/
COPY --from=builder /build/build/_deps/cppkafka-build/lib/libcppkafka.so* /usr/local/lib/

# Update library cache
RUN ldconfig
```

---

## Modified Files

| File | Changes |
|------|---------|
| `docker/appender/Dockerfile` | Added COPY for libcppkafka.so and ldconfig |

---

## Deployment Commands Used

### Install Helm Chart
```bash
helm upgrade --install telemetry-lake ./helm/telemetry-lake --create-namespace
```

### Load Images into kind Cluster
```bash
docker save <image> | docker exec -i <node> ctr -n=k8s.io images import -
```

### Restart Failed Pods
```bash
kubectl delete pod -n telemetry-lake -l app=appender
```

---

## Final Deployment Status

All components successfully running:

| Pod | Status | Purpose |
|-----|--------|---------|
| ingester | Running | HTTP receiver (port 4318) → Kafka producer |
| appender | Running | Kafka consumer → Iceberg writer |
| kafka | Running | Message broker |
| minio | Running | S3-compatible object storage |
| nessie | Running | Iceberg REST catalog |
| duckdb | Running | Query engine |

### Services Exposed

| Service | Type | Port | Endpoint |
|---------|------|------|----------|
| ingester | ClusterIP | 4318 | `http://ingester:4318` |
| kafka | ClusterIP | 9092 | `kafka:9092` |
| minio | ClusterIP | 9000 | `http://minio:9000` |
| minio-console | ClusterIP | 9001 | `http://minio-console:9001` |
| nessie | ClusterIP | 19120 | `http://nessie:19120` |

### Verification Logs

**Ingester** (healthy):
```
(2026-01-25 07:48:57) [INFO] Request: GET /ready 200
(2026-01-25 07:48:52) [INFO] Request: GET /health 200
```

**Appender** (connected):
```
QueueConsumer initialized with brokers: kafka:9092, topic: telemetry-logs, group: otel-appender
Storage configured: S3 endpoint=http://minio:9000, bucket=telemetrylake
Iceberg table created or verified: logs
IcebergAppender initialized successfully
OTel Log Appender started successfully
```

---

## Key Learnings

1. **kind clusters require explicit image loading** - Local Docker images are not automatically available to kind nodes. Use `docker save | docker exec -i <node> ctr images import` to load them.

2. **FetchContent libraries need to be copied** - When CMake builds dependencies from source (like cppkafka), the resulting shared libraries must be explicitly copied to the runtime image in multi-stage Docker builds.

3. **Check library paths in build directories** - The cppkafka library was at `_deps/cppkafka-build/lib/`, not the initially guessed path. Use `find` to locate built artifacts.

---

## Next Steps

1. Consider setting up a local container registry for easier image management
2. Add CI/CD pipeline to build and push images automatically
3. Consider using `imagePullPolicy: Never` for local development (only works with native Docker Desktop Kubernetes, not kind)
