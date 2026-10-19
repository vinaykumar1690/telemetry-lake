# Iceberg table maintenance for the logs tables (compaction/compact.py):
# Spark with the Iceberg runtime, run as a batch job (Helm CronJob).
# Build from the repository root: docker build -f docker/compaction/Dockerfile -t telemetry-lake/compaction .
FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu

USER root
ARG ICEBERG_VERSION=1.11.0
ARG SPARK_RUNTIME_SHA256=d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242
ARG AWS_BUNDLE_SHA256=38f01da7e96850cdd05e6616d758b77b43314b712a8808e3f9a824d56976162f
RUN set -eux; \
    base=https://repo1.maven.org/maven2/org/apache/iceberg; \
    cd /opt/spark/jars; \
    curl -fsSLO "$base/iceberg-spark-runtime-4.1_2.13/$ICEBERG_VERSION/iceberg-spark-runtime-4.1_2.13-$ICEBERG_VERSION.jar"; \
    curl -fsSLO "$base/iceberg-aws-bundle/$ICEBERG_VERSION/iceberg-aws-bundle-$ICEBERG_VERSION.jar"; \
    echo "$SPARK_RUNTIME_SHA256  iceberg-spark-runtime-4.1_2.13-$ICEBERG_VERSION.jar" | sha256sum -c -; \
    echo "$AWS_BUNDLE_SHA256  iceberg-aws-bundle-$ICEBERG_VERSION.jar" | sha256sum -c -

COPY compaction/ /opt/compaction/
USER spark
WORKDIR /opt/compaction
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[*]", "--driver-memory", "2g", "/opt/compaction/compact.py"]
