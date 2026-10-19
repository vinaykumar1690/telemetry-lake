#!/usr/bin/env bash
# Verifies what the compaction job (compaction/compact.py) relies on, against
# the local stack. Opt-in: ICEBERG_IT=1. Needs the compaction image:
#   docker build -f docker/compaction/Dockerfile -t telemetry-lake/compaction:dev .
#
# Checks:
#   1. Spark (Iceberg Java, REST catalog) reads tables DuckDB wrote
#   2. DuckDB still appends after the job sets bloom/file-size properties
#   3. Whether DuckDB can ALTER TABLE ADD COLUMN on an Iceberg table
#   4. rewrite_data_files (sort) commits while DuckDB keeps appending
#   5. The rewritten files carry Parquet bloom filters
#   6. Offsets pruning, snapshot expiry, orphan removal and manifest rewrite
#      work against Nessie, and DuckDB still reads and appends afterwards
set -euo pipefail

if [[ "${ICEBERG_IT:-0}" != "1" ]]; then
    echo "skipped: set ICEBERG_IT=1 to run against a live catalog"
    exit 0
fi
: "${ICEBERG_CATALOG_URI:?}" "${S3_ENDPOINT:?}" "${S3_ACCESS_KEY:?}" "${S3_SECRET_KEY:?}"
DUCKDB="${DUCKDB:-duckdb}"
IMAGE="${COMPACTION_IMAGE:-telemetry-lake/compaction:dev}"
NS="cspike_$(date +%s)"

lake() {  # $1 = SQL; prints only the statements' output
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

# The job runs in a container, so catalog and S3 are reached via the Docker host
spark() {  # args passed to compact.py
    docker run --rm --add-host host.docker.internal:host-gateway \
        -e ICEBERG_CATALOG_URI="${ICEBERG_CATALOG_URI/localhost/host.docker.internal}" \
        -e S3_ENDPOINT="${S3_ENDPOINT/localhost/host.docker.internal}" \
        -e S3_ACCESS_KEY -e S3_SECRET_KEY -e ICEBERG_WAREHOUSE -e ICEBERG_NAMESPACE="$NS" \
        -e COMPACTION_STEPS -e COMPACTION_MIN_AGE_MINUTES -e OFFSETS_MIN_AGE_MINUTES \
        -e SNAPSHOT_RETENTION_HOURS -e SNAPSHOT_RETAIN_LAST -e ORPHAN_MIN_AGE_HOURS \
        "$IMAGE" "$@" 2>&1 | grep -E '^(compaction|  )' || return "${PIPESTATUS[0]}"
}

insert_logs() {  # $1 = batch number; 20 rows two days ago
    lake "INSERT INTO lake.$NS.logs (\"timestamp\", service_name, severity_number, trace_id, span_id,
                                     host_name, service_instance_id, body, log_attributes) SELECT
            now() - INTERVAL 2 DAY - INTERVAL ($1 * 60 + i) SECOND,
            ['checkout','cart','auth'][1 + (i % 3)], 9 + 4 * (i % 4),
            printf('%032x', $1 * 1000 + i), printf('%016x', i), 'host-' || (i % 2), 'inst-1',
            'request ' || i, MAP {'seq': ($1 * 1000 + i)::VARCHAR}
          FROM range(20) t(i);"
}

echo "== namespace $NS"
lake "CREATE SCHEMA lake.$NS;
CREATE TABLE lake.$NS.logs (\"timestamp\" TIMESTAMPTZ, service_name VARCHAR, severity_number INTEGER,
    trace_id VARCHAR, span_id VARCHAR, host_name VARCHAR, service_instance_id VARCHAR, body VARCHAR,
    log_attributes MAP(VARCHAR, VARCHAR)) PARTITIONED BY (day(\"timestamp\"));
CREATE TABLE lake.$NS._appender_offsets (consumer_group VARCHAR, topic VARCHAR, \"partition\" INTEGER,
    last_offset BIGINT, committed_at TIMESTAMPTZ, appender_id VARCHAR);"
for b in 1 2 3 4 5 6; do
    insert_logs "$b"
    lake "INSERT INTO lake.$NS._appender_offsets VALUES ('g', 't', 0, $b, now() - INTERVAL 2 DAY, 'a');"
done

echo "== 1. Spark reads DuckDB-written tables (expect 120 rows, 6 files, 6 offsets rows)"
spark --sql "SELECT count(*) FROM lake.\`$NS\`.logs; SELECT count(*) FROM lake.\`$NS\`.logs.files;
             SELECT count(*) FROM lake.\`$NS\`._appender_offsets"

echo "== 2. properties, then a DuckDB append"
COMPACTION_STEPS=properties spark
insert_logs 7
echo "   duckdb rows after append: $(lake "SELECT count(*) FROM lake.$NS.logs;")"

echo "== 3. DuckDB ALTER TABLE ADD COLUMN"
if out=$(lake "ALTER TABLE lake.$NS.logs ADD COLUMN k8s_pod_name VARCHAR;" 2>&1); then
    echo "   supported"
else
    echo "   NOT supported: $(echo "$out" | head -1)"
fi

echo "== 4. compaction while DuckDB appends"
( for b in 8 9 10 11; do insert_logs "$b"; sleep 2; done ) &
APPENDS=$!
COMPACTION_STEPS=compact,offsets,manifests COMPACTION_MIN_AGE_MINUTES=60 OFFSETS_MIN_AGE_MINUTES=60 spark
wait "$APPENDS"
echo "   rows: $(lake "SELECT count(*) FROM lake.$NS.logs;") (expect 220)," \
     "offsets rows: $(lake "SELECT count(*) FROM lake.$NS._appender_offsets;") (expect 1)," \
     "max offset: $(lake "SELECT max(last_offset) FROM lake.$NS._appender_offsets;") (expect 6)"

echo "== 5. bloom filters in the rewritten files"
FILES=$(spark --sql "SELECT file_path FROM lake.\`$NS\`.logs.files ORDER BY file_size_in_bytes DESC LIMIT 1" | sed -n 's/^  //p')
for f in $FILES; do
    lake "SELECT '   ' || path_in_schema || ': bloom_filter_offset=' || coalesce(bloom_filter_offset::VARCHAR, 'none')
          FROM parquet_metadata('$f') WHERE path_in_schema IN ('trace_id', 'body') AND row_group_id = 0;"
done
echo "   sorted by service within the file:"
lake "SELECT '   ' || string_agg(DISTINCT service_name, ',') FROM (SELECT service_name FROM read_parquet('${FILES%% *}') LIMIT 5);"

echo "== 6. expire and orphans (skipped where the catalog disables Iceberg GC, as Nessie does)"
spark --sql "SELECT count(*) AS snapshots FROM lake.\`$NS\`.logs.snapshots; SHOW TBLPROPERTIES lake.\`$NS\`.logs ('gc.enabled')"
COMPACTION_STEPS=expire,orphans SNAPSHOT_RETENTION_HOURS=0 SNAPSHOT_RETAIN_LAST=1 ORPHAN_MIN_AGE_HOURS=1 spark

echo "== 7. Nessie GC reclaims the files compaction replaced"
objects() {
    curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user "$S3_ACCESS_KEY:$S3_SECRET_KEY" \
        "${S3_ENDPOINT%/}/${ICEBERG_WAREHOUSE}?list-type=2&prefix=$NS/logs" | grep -o "<Key>[^<]*\.parquet</Key>" | wc -l | tr -d ' '
}
echo "   parquet files before: $(objects)"
docker run --rm --add-host host.docker.internal:host-gateway ghcr.io/projectnessie/nessie-gc:latest gc --inmemory \
    -u "$(echo "${ICEBERG_CATALOG_URI/localhost/host.docker.internal}" | sed 's|/iceberg/*$|/api/v2|')" \
    -c 1 --max-file-modification "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -I s3.access-key-id="$S3_ACCESS_KEY" -I s3.secret-access-key="$S3_SECRET_KEY" \
    -I s3.endpoint="${S3_ENDPOINT/localhost/host.docker.internal}" -I s3.path-style-access=true \
    -I client.region=us-east-1 2>&1 | grep -iE "expired|deleted|error|exception" | head -5 || true
echo "   parquet files after:  $(objects)"
echo "   rows: $(lake "SELECT count(*) FROM lake.$NS.logs;") (expect 220)"
insert_logs 12
echo "   rows: $(lake "SELECT count(*) FROM lake.$NS.logs;") (expect 240)"
echo "spike finished; namespace lake.$NS left for inspection"
