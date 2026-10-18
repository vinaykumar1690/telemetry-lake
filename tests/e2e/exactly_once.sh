#!/usr/bin/env bash
# End-to-end exactly-once check: ingester -> Kafka -> appender(s) -> Iceberg,
# with crashes, a graceful restart and a rebalance in the middle.
#
# Opt-in (E2E=1). Needs the stack from tests/it/stack.sh (Kafka, Nessie,
# MinIO), built binaries, and on PATH: curl, kcat, duckdb >= 1.5.4 (or set
# DUCKDB). Each run uses a fresh topic and Iceberg namespace, so runs never
# see each other's data. See docs/integration-testing.md.
#
#   tests/it/stack.sh up && eval "$(tests/it/stack.sh env)"
#   E2E=1 tests/e2e/exactly_once.sh
set -euo pipefail

if [[ "${E2E:-0}" != "1" ]]; then
    echo "skipped: set E2E=1 to run against live services"
    exit 0
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
: "${KAFKA_BROKERS:?}" "${ICEBERG_CATALOG_URI:?}" "${S3_ENDPOINT:?}" "${S3_ACCESS_KEY:?}" "${S3_SECRET_KEY:?}"
INGESTER="${INGESTER:-$ROOT/build/otel_receiver}"
APPENDER="${APPENDER:-$ROOT/build/otel_appender}"
DUCKDB="${DUCKDB:-duckdb}"
BATCHES="${BATCHES:-200}"          # requests sent through the ingester
PER_BATCH="${PER_BATCH:-50}"       # log records per request
POISON="${POISON:-5}"              # undecodable messages produced straight to Kafka
PARTITIONS="${TOPIC_PARTITIONS:-4}"
RUN="$(date +%s)"
export KAFKA_TOPIC="e2e-$RUN"
export KAFKA_DLQ_TOPIC="$KAFKA_TOPIC-dlq"
export ICEBERG_NAMESPACE="e2e_$RUN"
export KAFKA_CONSUMER_GROUP="e2e-appender-$RUN"
TABLE="${ICEBERG_TABLE_NAME:-logs}"
OFFSETS="${ICEBERG_OFFSETS_TABLE:-_appender_offsets}"
INGESTER_PORT="${INGESTER_PORT:-14318}"
INGESTER_URL="http://localhost:$INGESTER_PORT"
WORK=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$WORK"' EXIT

export BUFFER_TIME_SECONDS="${BUFFER_TIME_SECONDS:-5}"   # commit often to widen the crash windows
export REVOKE_TIMEOUT_SECONDS="${REVOKE_TIMEOUT_SECONDS:-30}"
export DUCKDB_HOME_DIRECTORY="${DUCKDB_HOME_DIRECTORY:-$HOME/.duckdb}"

echo "== run $RUN: topic $KAFKA_TOPIC ($PARTITIONS partitions), namespace $ICEBERG_NAMESPACE"
"$ROOT/tests/it/stack.sh" topic "$KAFKA_TOPIC" "$PARTITIONS"
"$ROOT/tests/it/stack.sh" topic "$KAFKA_DLQ_TOPIC" 1

HTTP_PORT="$INGESTER_PORT" "$INGESTER" >>"$WORK/ingester.log" 2>&1 &
INGESTER_PID=$!
for _ in $(seq 1 30); do
    curl -s -o /dev/null "$INGESTER_URL/" && break
    sleep 1
done

start_appender() {  # $1 = variable that receives the pid, $2 = instance name, $3 = health port
    APPENDER_ID="$2" HEALTH_PORT="$3" "$APPENDER" >>"$WORK/$2.log" 2>&1 &
    printf -v "$1" '%s' "$!"
}

lake_sql() {  # prints only the output of the statements in $1
    "$DUCKDB" -csv -noheader -bail <<SQL
.output /dev/null
LOAD httpfs; LOAD iceberg;
CREATE SECRET (TYPE S3, KEY_ID '${S3_ACCESS_KEY}', SECRET '${S3_SECRET_KEY}',
               ENDPOINT '${S3_ENDPOINT#http*://}', URL_STYLE 'path', USE_SSL false);
ATTACH '${ICEBERG_WAREHOUSE:-}' AS lake (TYPE ICEBERG, ENDPOINT '${ICEBERG_CATALOG_URI}',
                                        AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none');
.output
$1
SQL
}

send_batch() {  # $1 = batch number; records carry a unique "seq" attribute
    local records="" seq
    for ((i = 0; i < PER_BATCH; i++)); do
        seq=$(( $1 * PER_BATCH + i ))
        records+="${records:+,}{\"timeUnixNano\":\"$(date +%s)000000000\",\"body\":{\"stringValue\":\"e2e $seq\"},"
        records+="\"attributes\":[{\"key\":\"seq\",\"value\":{\"stringValue\":\"$seq\"}}]}"
    done
    curl -sf -X POST "$INGESTER_URL/v1/logs" -H 'Content-Type: application/json' \
        -d "{\"resourceLogs\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"e2e\"}}]},\"scopeLogs\":[{\"logRecords\":[${records}]}]}]}" \
        >/dev/null
}

durable_offsets() {  # "partition next_offset" per partition with committed data
    lake_sql "SELECT \"partition\", max(last_offset) + 1 FROM lake.\"$ICEBERG_NAMESPACE\".\"$OFFSETS\"
              GROUP BY ALL ORDER BY 1;" 2>/dev/null | tr ',' ' ' || true
}

end_offsets() {  # "partition end_offset" per non-empty partition
    for ((p = 0; p < PARTITIONS; p++)); do
        kcat -Q -b "$KAFKA_BROKERS" -t "$KAFKA_TOPIC:$p:-1" 2>/dev/null | awk -v p="$p" '{print p, $NF}'
    done | awk '$2 > 0'
}

echo "== starting appender a"
start_appender A a 18080

echo "== sending $BATCHES batches with crashes, a graceful restart and a rebalance"
for ((b = 0; b < BATCHES; b++)); do
    send_batch "$b"
    case $b in
        $((BATCHES / 5)))      echo "   kill -9 a";  kill -9 "$A"; start_appender A a 18080 ;;
        $((BATCHES * 2 / 5)))  echo "   start b (rebalance)"; start_appender B b 18081 ;;
        $((BATCHES * 3 / 5)))  echo "   kill -9 b";  kill -9 "$B"; start_appender B b 18081 ;;
        $((BATCHES * 4 / 5)))  echo "   SIGTERM a";  kill -TERM "$A"
                               wait "$A" && echo "   a exited 0" || echo "   a exited $?"
                               start_appender A a 18080 ;;
    esac
    if (( b % 37 == 0 )); then sleep $(( RANDOM % 3 )); fi
done

echo "== producing $POISON poison messages straight to Kafka"
for ((p = 0; p < POISON; p++)); do
    printf 'poison-%d\xff\xfe' "$p" | kcat -P -b "$KAFKA_BROKERS" -t "$KAFKA_TOPIC"
done

echo "== waiting for the offsets table to reach the topic's end offsets"
EXPECTED_ROWS=$(( BATCHES * PER_BATCH ))
for ((t = 0; t < 120; t++)); do
    curl -sf -X POST localhost:18080/flush >/dev/null || true
    curl -sf -X POST localhost:18081/flush >/dev/null || true
    ROWS=$(lake_sql "SELECT count(*) FROM lake.\"$ICEBERG_NAMESPACE\".\"$TABLE\";" 2>/dev/null | tail -1 || echo 0)
    echo "   rows=$ROWS expected=$EXPECTED_ROWS"
    if [[ "$ROWS" -ge "$EXPECTED_ROWS" && "$(durable_offsets)" == "$(end_offsets)" ]]; then break; fi
    sleep 5
done

echo "== checking"
RESULT=$(lake_sql "
SELECT count(*), count(DISTINCT log_attributes['seq']),
       count(DISTINCT (_kafka_partition, _kafka_offset, log_attributes['seq']))
FROM lake.\"$ICEBERG_NAMESPACE\".\"$TABLE\";" | tail -1)
IFS=, read -r TOTAL DISTINCT_SEQ DISTINCT_ROWS <<<"$RESULT"
DLQ_COUNT=$(kcat -C -b "$KAFKA_BROKERS" -t "$KAFKA_DLQ_TOPIC" -o beginning -e -q -f '%o\n' 2>/dev/null | wc -l | tr -d ' ')
# Every partition's recovery point must be its end offset: nothing skipped, nothing pending
DURABLE=$(durable_offsets)
END_OFFSETS=$(end_offsets)
echo "   durable next offsets: $(echo $DURABLE)"
echo "   topic end offsets:    $(echo $END_OFFSETS)"

fail=0
[[ "$TOTAL" == "$EXPECTED_ROWS" ]] || { echo "FAIL: $TOTAL rows, expected $EXPECTED_ROWS (duplicates or loss)"; fail=1; }
[[ "$DISTINCT_SEQ" == "$EXPECTED_ROWS" ]] || { echo "FAIL: $DISTINCT_SEQ distinct seq values"; fail=1; }
[[ "$DISTINCT_ROWS" == "$TOTAL" ]] || { echo "FAIL: duplicate (partition, offset, seq) rows"; fail=1; }
[[ "$DURABLE" == "$END_OFFSETS" ]] || { echo "FAIL: offsets table does not match the topic end offsets"; fail=1; }
[[ "$DLQ_COUNT" -ge "$POISON" ]] || { echo "FAIL: $DLQ_COUNT DLQ messages, expected >= $POISON"; fail=1; }

kill -TERM "$A" "$B" "$INGESTER_PID" 2>/dev/null || true
wait || true
if (( fail )); then
    echo "appender logs: $WORK (kept)"; trap - EXIT; exit 1
fi
echo "PASS: $TOTAL rows, no duplicates, no loss; $DLQ_COUNT DLQ messages"
