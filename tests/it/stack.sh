#!/usr/bin/env bash
# Brings the integration-test stack up or down (see docs/integration-testing.md).
#
#   tests/it/stack.sh up      # Kafka + Nessie in Docker, bucket + topics created
#   tests/it/stack.sh down    # stop and remove the containers (Nessie is in-memory)
#   tests/it/stack.sh env     # print the env vars the tests expect
#   tests/it/stack.sh topic NAME [PARTITIONS]
#
# MinIO must already run on the host (brew services start minio).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE=(docker compose -f "$HERE/compose.yaml")
IT_WAREHOUSE=telemetrylakeit   # also the bucket; fixed in compose.yaml
export S3_ACCESS_KEY="${S3_ACCESS_KEY:-minioadmin}"
export S3_SECRET_KEY="${S3_SECRET_KEY:-minioadmin}"
S3_URL="${S3_URL:-http://localhost:9000}"
TOPIC="${KAFKA_TOPIC:-e2e-logs}"
PARTITIONS="${TOPIC_PARTITIONS:-4}"

print_env() {
    cat <<EOF
export KAFKA_BROKERS=localhost:9092
export KAFKA_TOPIC=$TOPIC
export ICEBERG_CATALOG_URI=http://localhost:19120/iceberg/
export ICEBERG_WAREHOUSE=$IT_WAREHOUSE
export S3_ENDPOINT=$S3_URL
export S3_ACCESS_KEY=$S3_ACCESS_KEY
export S3_SECRET_KEY=$S3_SECRET_KEY
export DUCKDB=$(command -v /usr/local/bin/duckdb /opt/homebrew/bin/duckdb 2>/dev/null | head -1)
EOF
}

wait_for() {  # $1 = description, rest = command
    local what=$1; shift
    for _ in $(seq 1 60); do
        if "$@" >/dev/null 2>&1; then echo "   $what ready"; return 0; fi
        sleep 2
    done
    echo "$what did not become ready" >&2
    return 1
}

create_topic() {  # $1 = name, $2 = partitions
    "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
        --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1 >/dev/null
}

case "${1:-}" in
up)
    echo "== MinIO ($S3_URL)"
    curl -sf "$S3_URL/minio/health/live" >/dev/null ||
        { echo "MinIO is not reachable at $S3_URL; run: brew services start minio" >&2; exit 1; }
    # Create the warehouse bucket (409 = it already exists)
    code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" \
        --user "$S3_ACCESS_KEY:$S3_SECRET_KEY" "$S3_URL/$IT_WAREHOUSE")
    [[ "$code" == 200 || "$code" == 409 ]] || { echo "bucket creation failed: HTTP $code" >&2; exit 1; }
    echo "   bucket $IT_WAREHOUSE ready"

    echo "== Kafka + Nessie"
    "${COMPOSE[@]}" up -d --wait kafka
    "${COMPOSE[@]}" up -d nessie
    wait_for "Nessie" curl -sf http://localhost:19120/iceberg/v1/config
    create_topic "$TOPIC" "$PARTITIONS"
    create_topic "$TOPIC-dlq" 1
    echo "   topics $TOPIC ($PARTITIONS partitions) and $TOPIC-dlq ready"
    echo
    echo "Environment for the tests:"
    print_env
    ;;
down)
    "${COMPOSE[@]}" down --remove-orphans
    ;;
env)
    print_env
    ;;
topic)  # topic NAME [PARTITIONS]
    create_topic "${2:?topic name}" "${3:-$PARTITIONS}"
    ;;
*)
    echo "usage: $0 up|down|env|topic NAME [PARTITIONS]" >&2
    exit 2
    ;;
esac
