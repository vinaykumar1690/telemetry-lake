#!/usr/bin/env bash
# End-to-end exactly-once check: ingester -> Kafka -> appender(s) -> Iceberg,
# with crashes, a graceful restart and a rebalance in the middle.
#
# Opt-in (E2E=1). Needs a running Kafka, MinIO and Nessie, a running
# ingester, the appender's env vars (see README), and on PATH: curl, kcat,
# duckdb (>= 1.5.4). The appender's topic should be fresh (or set
# ICEBERG_NAMESPACE to an unused namespace) so counts start from zero.
#
#   E2E=1 INGESTER_URL=http://localhost:4318 APPENDER=build/otel_appender \
#     KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC=e2e-logs ICEBERG_CATALOG_URI=... \
#     S3_ENDPOINT=... S3_ACCESS_KEY=... S3_SECRET_KEY=... ICEBERG_WAREHOUSE=... \
#     tests/e2e/exactly_once.sh
set -euo pipefail

if [[ "${E2E:-0}" != "1" ]]; then
    echo "skipped: set E2E=1 to run against live services"
    exit 0
fi

: "${INGESTER_URL:?}" "${APPENDER:?}" "${KAFKA_BROKERS:?}" "${KAFKA_TOPIC:?}"
: "${ICEBERG_CATALOG_URI:?}" "${S3_ENDPOINT:?}" "${S3_ACCESS_KEY:?}" "${S3_SECRET_KEY:?}"
BATCHES="${BATCHES:-200}"          # requests sent through the ingester
PER_BATCH="${PER_BATCH:-50}"       # log records per request
POISON="${POISON:-5}"              # undecodable messages produced straight to Kafka
NAMESPACE="${ICEBERG_NAMESPACE:-default}"
TABLE="${ICEBERG_TABLE_NAME:-logs}"
OFFSETS="${ICEBERG_OFFSETS_TABLE:-_appender_offsets}"
DLQ_TOPIC="${KAFKA_DLQ_TOPIC:-${KAFKA_TOPIC}-dlq}"
WORK=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$WORK"' EXIT

export BUFFER_TIME_SECONDS="${BUFFER_TIME_SECONDS:-5}"   # commit often to widen the crash windows
export REVOKE_TIMEOUT_SECONDS="${REVOKE_TIMEOUT_SECONDS:-30}"

start_appender() {  # $1 = instance name, $2 = health port
    APPENDER_ID="$1" HEALTH_PORT="$2" "$APPENDER" >>"$WORK/$1.log" 2>&1 &
    echo $!
}

lake_sql() {
    duckdb -csv -noheader -bail <<SQL
LOAD httpfs; LOAD iceberg;
CREATE SECRET (TYPE S3, KEY_ID '${S3_ACCESS_KEY}', SECRET '${S3_SECRET_KEY}',
               ENDPOINT '${S3_ENDPOINT#http*://}', URL_STYLE 'path', USE_SSL false);
ATTACH '${ICEBERG_WAREHOUSE:-}' AS lake (TYPE ICEBERG, ENDPOINT '${ICEBERG_CATALOG_URI}',
                                        AUTHORIZATION_TYPE 'none');
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

echo "== starting appender a"
A=$(start_appender a 18080)

echo "== sending $BATCHES batches with crashes, a graceful restart and a rebalance"
for ((b = 0; b < BATCHES; b++)); do
    send_batch "$b"
    case $b in
        $((BATCHES / 5)))      echo "   kill -9 a";  kill -9 "$A"; A=$(start_appender a 18080) ;;
        $((BATCHES * 2 / 5)))  echo "   start b (rebalance)"; B=$(start_appender b 18081) ;;
        $((BATCHES * 3 / 5)))  echo "   kill -9 b";  kill -9 "$B"; B=$(start_appender b 18081) ;;
        $((BATCHES * 4 / 5)))  echo "   SIGTERM a";  kill -TERM "$A"; wait "$A" || true; A=$(start_appender a 18080) ;;
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
    END=$(kcat -Q -b "$KAFKA_BROKERS" -t "$KAFKA_TOPIC:0:-1" 2>/dev/null | head -1 || true)
    ROWS=$(lake_sql "SELECT count(*) FROM lake.\"$NAMESPACE\".\"$TABLE\";" | tail -1)
    echo "   rows=$ROWS expected=$EXPECTED_ROWS ${END:+(end: $END)}"
    if [[ "$ROWS" -ge "$EXPECTED_ROWS" ]]; then break; fi
    sleep 5
done

echo "== checking"
RESULT=$(lake_sql "
SELECT count(*), count(DISTINCT log_attributes['seq']),
       count(DISTINCT (_kafka_partition, _kafka_offset, log_attributes['seq']))
FROM lake.\"$NAMESPACE\".\"$TABLE\";" | tail -1)
IFS=, read -r TOTAL DISTINCT_SEQ DISTINCT_ROWS <<<"$RESULT"
DLQ_COUNT=$(kcat -C -b "$KAFKA_BROKERS" -t "$DLQ_TOPIC" -o beginning -e -q -f '%o\n' 2>/dev/null | wc -l | tr -d ' ')
lake_sql "SELECT \"partition\", max(last_offset) FROM lake.\"$NAMESPACE\".\"$OFFSETS\" GROUP BY ALL ORDER BY 1;"

fail=0
[[ "$TOTAL" == "$EXPECTED_ROWS" ]] || { echo "FAIL: $TOTAL rows, expected $EXPECTED_ROWS (duplicates or loss)"; fail=1; }
[[ "$DISTINCT_SEQ" == "$EXPECTED_ROWS" ]] || { echo "FAIL: $DISTINCT_SEQ distinct seq values"; fail=1; }
[[ "$DISTINCT_ROWS" == "$TOTAL" ]] || { echo "FAIL: duplicate (partition, offset, seq) rows"; fail=1; }
[[ "$DLQ_COUNT" -ge "$POISON" ]] || { echo "FAIL: $DLQ_COUNT DLQ messages, expected >= $POISON"; fail=1; }

kill -TERM "$A" "$B" 2>/dev/null || true
wait || true
if (( fail )); then
    echo "appender logs: $WORK (kept)"; trap - EXIT; exit 1
fi
echo "PASS: $TOTAL rows, no duplicates, no loss; $DLQ_COUNT DLQ messages"
