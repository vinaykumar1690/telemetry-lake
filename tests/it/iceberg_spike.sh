#!/usr/bin/env bash
# Verifies the DuckDB Iceberg capabilities the appender depends on, against a
# live REST catalog (Nessie) and S3 (MinIO). Opt-in: requires ICEBERG_IT=1.
#
# Checks:
#   1. CREATE TABLE ... PARTITIONED BY (day(ts)) with TIMESTAMPTZ and MAP columns
#   2. A transaction writing two tables lands atomically (both or neither)
#   3. A failed transaction leaves both tables unchanged
#   4. Which maintenance functions (iceberg_rewrite_data_files, ...) exist
#
# Usage:
#   ICEBERG_IT=1 ICEBERG_CATALOG_URI=http://localhost:19120/iceberg/ \
#   ICEBERG_WAREHOUSE=telemetrylake S3_ENDPOINT=localhost:9000 \
#   S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin tests/it/iceberg_spike.sh
set -euo pipefail

if [[ "${ICEBERG_IT:-0}" != "1" ]]; then
    echo "skipped: set ICEBERG_IT=1 to run against a live catalog"
    exit 0
fi

: "${ICEBERG_CATALOG_URI:?}" "${S3_ENDPOINT:?}" "${S3_ACCESS_KEY:?}" "${S3_SECRET_KEY:?}"
DUCKDB="${DUCKDB:-duckdb}"
NS="spike_$(date +%s)"

"$DUCKDB" -bail <<SQL
INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;
CREATE SECRET s3 (TYPE S3, KEY_ID '${S3_ACCESS_KEY}', SECRET '${S3_SECRET_KEY}',
                  ENDPOINT '${S3_ENDPOINT#http*://}', REGION '${S3_REGION:-us-east-1}',
                  USE_SSL false, URL_STYLE 'path');
ATTACH '${ICEBERG_WAREHOUSE:-}' AS lake (TYPE ICEBERG, ENDPOINT '${ICEBERG_CATALOG_URI}',
                                        AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none');
CREATE SCHEMA lake.${NS};

-- 1. Partitioned table with the column types the appender uses
CREATE TABLE lake.${NS}.logs (
    _kafka_partition INTEGER, _kafka_offset BIGINT,
    ts TIMESTAMPTZ, attrs MAP(VARCHAR, VARCHAR)
) PARTITIONED BY (day(ts));
CREATE TABLE lake.${NS}.offsets (partition INTEGER, last_offset BIGINT);

-- 2. Two-table transaction
BEGIN;
INSERT INTO lake.${NS}.logs VALUES
    (0, 1, TIMESTAMPTZ '2026-10-01 10:00:00+00', MAP {'k': 'v\\w'}),
    (0, 2, TIMESTAMPTZ '2026-10-02 10:00:00+00', MAP {});
INSERT INTO lake.${NS}.offsets VALUES (0, 2);
COMMIT;
SELECT 'two-table commit', (SELECT count(*) FROM lake.${NS}.logs) AS logs,
       (SELECT count(*) FROM lake.${NS}.offsets) AS offsets;
SELECT 'partitions', partition_field_name, partition_field_transform
FROM iceberg_partition_stats(lake.${NS}.logs);
SELECT 'snapshots', count(*) FROM iceberg_snapshots(lake.${NS}.logs);
SQL

# 3. A failing transaction must leave both tables unchanged (expect an error)
set +e
"$DUCKDB" -bail <<SQL
LOAD httpfs; LOAD iceberg;
CREATE SECRET s3 (TYPE S3, KEY_ID '${S3_ACCESS_KEY}', SECRET '${S3_SECRET_KEY}',
                  ENDPOINT '${S3_ENDPOINT#http*://}', REGION '${S3_REGION:-us-east-1}',
                  USE_SSL false, URL_STYLE 'path');
ATTACH '${ICEBERG_WAREHOUSE:-}' AS lake (TYPE ICEBERG, ENDPOINT '${ICEBERG_CATALOG_URI}',
                                        AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none');
BEGIN;
INSERT INTO lake.${NS}.logs VALUES (0, 3, now(), MAP {});
INSERT INTO lake.${NS}.offsets VALUES ('not a number', 3);
COMMIT;
SQL
set -e

"$DUCKDB" -bail <<SQL
LOAD httpfs; LOAD iceberg;
CREATE SECRET s3 (TYPE S3, KEY_ID '${S3_ACCESS_KEY}', SECRET '${S3_SECRET_KEY}',
                  ENDPOINT '${S3_ENDPOINT#http*://}', REGION '${S3_REGION:-us-east-1}',
                  USE_SSL false, URL_STYLE 'path');
ATTACH '${ICEBERG_WAREHOUSE:-}' AS lake (TYPE ICEBERG, ENDPOINT '${ICEBERG_CATALOG_URI}',
                                        AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none');
SELECT 'after failed txn (expect 2, 1)', (SELECT count(*) FROM lake.${NS}.logs),
       (SELECT count(*) FROM lake.${NS}.offsets);
SELECT 'backslash round-trip (expect v\\w)', attrs['k'] FROM lake.${NS}.logs WHERE _kafka_offset = 1;

-- 4. Maintenance functions available in this build
SELECT 'maintenance', function_name FROM duckdb_functions()
WHERE function_name LIKE 'iceberg_%' AND (function_name LIKE '%rewrite%' OR function_name LIKE '%expire%');
SQL
echo "spike finished; namespace lake.${NS} left in place for inspection"
