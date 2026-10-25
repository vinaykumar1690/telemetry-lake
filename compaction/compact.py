#!/usr/bin/env python3
"""Iceberg table maintenance for the telemetry-lake logs tables.

The appender commits one small Parquet file per partition-day per commit and
cannot compact (DuckDB has no Iceberg maintenance functions). This job, run
periodically (Helm CronJob), keeps the tables fast to query:

  properties  bloom filters and target file sizes on the logs table, and
              copy-on-write deletes on the offsets table (idempotent)
  compact     rewrites logs files older than COMPACTION_MIN_AGE_MINUTES into
              large files sorted by (service_name, severity_number, timestamp)
  offsets     deletes superseded _appender_offsets rows, then bin-packs it
  expire      expires snapshots older than SNAPSHOT_RETENTION_HOURS
  orphans     removes unreferenced files older than ORPHAN_MIN_AGE_HOURS
  manifests   rewrites the logs table's manifests

Safe to run while the appender commits: a rewrite is a validated "replace"
commit and the appender only appends, so they never conflict on data; rows
(and their Kafka offsets) are copied unchanged, so exactly-once delivery and
the query service's row keys are unaffected.

Usage (inside the compaction image):
  compact.py                       run COMPACTION_STEPS (default: all)
  compact.py --add-columns a,b     add VARCHAR columns (promoted attributes)
  compact.py --sql "SQL; SQL"      run statements and print their results

Configuration (environment; catalog and S3 names match the appender's):
  ICEBERG_CATALOG_URI, ICEBERG_WAREHOUSE, ICEBERG_NAMESPACE (default),
  ICEBERG_TABLE_NAME (logs), ICEBERG_OFFSETS_TABLE (_appender_offsets),
  S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_REGION (us-east-1), S3_URL_STYLE (path),
  PROMOTED_ATTRIBUTES (bloom filters on their columns),
  BLOOM_FILTER_COLUMNS (trace_id,span_id,host_name,service_instance_id),
  COMPACTION_STEPS, COMPACTION_MIN_AGE_MINUTES (60), TARGET_FILE_SIZE_MB (512),
  OFFSETS_MIN_AGE_MINUTES (60), SNAPSHOT_RETENTION_HOURS (24),
  SNAPSHOT_RETAIN_LAST (50), ORPHAN_MIN_AGE_HOURS (72)
"""

import argparse
import datetime
import os
import sys
import time

from pyspark.sql import SparkSession

ALL_STEPS = ["properties", "compact", "offsets", "expire", "orphans", "manifests"]
SORT_ORDER = "service_name ASC NULLS LAST, severity_number DESC NULLS LAST, timestamp DESC NULLS LAST"
CATALOG = "lake"


def env(name, default=None):
    value = os.environ.get(name, "").strip()
    if value:
        return value
    if default is None:
        sys.exit(f"compaction: {name} is required")
    return default


def env_int(name, default, minimum=0):
    raw = env(name, str(default))
    try:
        value = int(raw)
    except ValueError:
        sys.exit(f"compaction: {name} must be an integer, got '{raw}'")
    if value < minimum:
        sys.exit(f"compaction: {name} must be >= {minimum}, got {value}")
    return value


def log(message):
    print(f"compaction: {message}", flush=True)


def session():
    endpoint = env("S3_ENDPOINT")
    if "://" not in endpoint:
        endpoint = f"http://{endpoint}"
    prefix = f"spark.sql.catalog.{CATALOG}"
    builder = (
        SparkSession.builder.appName("telemetry-lake-compaction")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.session.timeZone", "UTC")
        .config(prefix, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{prefix}.type", "rest")
        .config(f"{prefix}.uri", env("ICEBERG_CATALOG_URI"))
        .config(f"{prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{prefix}.s3.endpoint", endpoint)
        # path for MinIO; AWS S3 uses virtual-hosted addressing (S3_URL_STYLE=vhost)
        .config(f"{prefix}.s3.path-style-access", str(env("S3_URL_STYLE", "path") == "path").lower())
        .config(f"{prefix}.s3.access-key-id", env("S3_ACCESS_KEY"))
        .config(f"{prefix}.s3.secret-access-key", env("S3_SECRET_KEY"))
        .config(f"{prefix}.client.region", env("S3_REGION", "us-east-1"))
        .config("spark.ui.enabled", "false")
    )
    warehouse = os.environ.get("ICEBERG_WAREHOUSE") or os.environ.get("S3_BUCKET")
    if warehouse:
        builder = builder.config(f"{prefix}.warehouse", warehouse)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ident(*parts):
    return ".".join(f"`{p}`" for p in parts)


class Tables:
    def __init__(self):
        namespace = env("ICEBERG_NAMESPACE", "default")
        self.logs_name = f"{ident(namespace)}.{ident(env('ICEBERG_TABLE_NAME', 'logs'))}"
        self.offsets_name = f"{ident(namespace)}.{ident(env('ICEBERG_OFFSETS_TABLE', '_appender_offsets'))}"
        self.logs = f"{CATALOG}.{self.logs_name}"
        self.offsets = f"{CATALOG}.{self.offsets_name}"


def ts_literal(delta):
    instant = datetime.datetime.now(datetime.timezone.utc) - delta
    return f"TIMESTAMP '{instant.strftime('%Y-%m-%d %H:%M:%S')}'"  # session time zone is UTC


def files(spark, table):
    row = spark.sql(f"SELECT count(*) AS n, coalesce(sum(file_size_in_bytes), 0) AS bytes FROM {table}.files").first()
    return row.n, row.bytes


def promoted_columns():
    keys = [k.strip() for k in os.environ.get("PROMOTED_ATTRIBUTES", "").split(",") if k.strip()]
    return [k.replace(".", "_") for k in keys]


def step_properties(spark, t):
    columns = set(spark.table(t.logs).columns)
    bloom = [c.strip() for c in env("BLOOM_FILTER_COLUMNS", "trace_id,span_id,host_name,service_instance_id").split(",")]
    bloom += promoted_columns()
    # No write.target-file-size-bytes here: DuckDB refuses to append to a
    # partitioned table that sets it, so the target size is a rewrite option.
    # Row groups keep Iceberg's 128 MB default.
    props = {f"write.parquet.bloom-filter-enabled.column.{c}": "true" for c in bloom if c in columns}
    assignments = ", ".join(f"'{k}' = '{v}'" for k, v in sorted(props.items()))
    spark.sql(f"ALTER TABLE {t.logs} SET TBLPROPERTIES ({assignments})")
    # Superseded offsets rows are deleted every run; rewrite files instead of
    # accumulating delete files the appender's recovery query would have to merge
    spark.sql(
        f"ALTER TABLE {t.offsets} SET TBLPROPERTIES ('write.delete.mode' = 'copy-on-write', "
        f"'write.update.mode' = 'copy-on-write', 'write.merge.mode' = 'copy-on-write')"
    )
    log(f"properties set (bloom filters on {sorted(c for c in bloom if c in columns)})")


def step_compact(spark, t):
    cutoff = ts_literal(datetime.timedelta(minutes=env_int("COMPACTION_MIN_AGE_MINUTES", 60)))
    before = files(spark, t.logs)
    result = spark.sql(
        f"""CALL {CATALOG}.system.rewrite_data_files(
              table => '{t.logs_name}',
              strategy => 'sort',
              sort_order => '{SORT_ORDER}',
              where => '`timestamp` < {cutoff.replace("'", "''")}',
              options => map('min-input-files', '2',
                             'target-file-size-bytes', '{env_int("TARGET_FILE_SIZE_MB", 512, 1) * 1024 * 1024}',
                             'partial-progress.enabled', 'true',
                             'partial-progress.max-commits', '10',
                             'max-concurrent-file-group-rewrites', '4'))"""
    ).first()
    after = files(spark, t.logs)
    log(
        f"logs: rewrote {result.rewritten_data_files_count} files into {result.added_data_files_count} "
        f"({result.rewritten_bytes_count} bytes); data files {before[0]} -> {after[0]}"
    )


def step_offsets(spark, t):
    cutoff = ts_literal(datetime.timedelta(minutes=env_int("OFFSETS_MIN_AGE_MINUTES", 60)))
    before = spark.table(t.offsets).count()
    # Recovery reads max(last_offset) per (topic, partition); a row with a
    # higher offset for the same group, topic and partition keeps that max
    spark.sql(
        f"""DELETE FROM {t.offsets} AS old
            WHERE old.committed_at < {cutoff}
              AND EXISTS (SELECT 1 FROM {t.offsets} AS newer
                          WHERE newer.consumer_group = old.consumer_group
                            AND newer.topic = old.topic
                            AND newer.`partition` = old.`partition`
                            AND newer.last_offset > old.last_offset)"""
    )
    after = spark.table(t.offsets).count()
    result = spark.sql(
        f"""CALL {CATALOG}.system.rewrite_data_files(
              table => '{t.offsets_name}', strategy => 'binpack',
              options => map('min-input-files', '2'))"""
    ).first()
    log(f"offsets: {before} -> {after} rows; rewrote {result.rewritten_data_files_count} files "
        f"into {result.added_data_files_count}")


def gc_enabled(spark, table):
    """Nessie sets gc.enabled=false: snapshots live in Nessie's commit log, and
    files are reclaimed by Nessie GC (see the nessie-gc CronJob), not by
    Iceberg's expire_snapshots / remove_orphan_files."""
    rows = spark.sql(f"SHOW TBLPROPERTIES {table} ('gc.enabled')").collect()
    return not rows or str(rows[0].value).lower() != "false"


def step_expire(spark, t):
    cutoff = ts_literal(datetime.timedelta(hours=env_int("SNAPSHOT_RETENTION_HOURS", 24)))
    retain = env_int("SNAPSHOT_RETAIN_LAST", 50, 1)
    for name in (t.logs_name, t.offsets_name):
        if not gc_enabled(spark, f"{CATALOG}.{name}"):
            log(f"{name}: gc.enabled=false (Nessie); snapshot expiry skipped, file GC is Nessie GC's job")
            continue
        result = spark.sql(
            f"CALL {CATALOG}.system.expire_snapshots(table => '{name}', older_than => {cutoff}, retain_last => {retain})"
        ).first()
        log(f"{name}: expired snapshots, deleted {result.deleted_data_files_count} data and "
            f"{result.deleted_manifest_files_count} manifest files")


def step_orphans(spark, t):
    cutoff = ts_literal(datetime.timedelta(hours=env_int("ORPHAN_MIN_AGE_HOURS", 72, 1)))
    for name in (t.logs_name, t.offsets_name):
        if not gc_enabled(spark, f"{CATALOG}.{name}"):
            log(f"{name}: gc.enabled=false (Nessie); orphan removal skipped, file GC is Nessie GC's job")
            continue
        removed = spark.sql(
            f"CALL {CATALOG}.system.remove_orphan_files(table => '{name}', older_than => {cutoff})"
        ).count()
        log(f"{name}: removed {removed} orphan files")


def step_manifests(spark, t):
    result = spark.sql(f"CALL {CATALOG}.system.rewrite_manifests('{t.logs_name}')").first()
    log(f"logs: rewrote {result.rewritten_manifests_count} manifests into {result.added_manifests_count}")


STEPS = {
    "properties": step_properties,
    "compact": step_compact,
    "offsets": step_offsets,
    "expire": step_expire,
    "orphans": step_orphans,
    "manifests": step_manifests,
}


def add_columns(spark, t, names):
    existing = set(spark.table(t.logs).columns)
    missing = [n for n in names if n not in existing]
    if missing:
        spark.sql(f"ALTER TABLE {t.logs} ADD COLUMNS ({', '.join(f'`{n}` STRING' for n in missing)})")
    log(f"added columns: {missing or 'none'}")


def run_sql(spark, text):
    for statement in [s.strip() for s in text.split(";") if s.strip()]:
        log(f"sql> {statement}")
        frame = spark.sql(statement)
        for row in frame.collect():
            print("  " + " | ".join(str(v) for v in row), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--add-columns", help="comma-separated VARCHAR columns to add to the logs table")
    parser.add_argument("--sql", help="semicolon-separated SQL statements to run")
    args = parser.parse_args()

    steps = [s.strip() for s in env("COMPACTION_STEPS", ",".join(ALL_STEPS)).split(",") if s.strip()]
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        sys.exit(f"compaction: unknown steps {unknown}; valid: {ALL_STEPS}")

    spark = session()
    tables = Tables()
    try:
        if args.sql:
            run_sql(spark, args.sql)
            return
        if args.add_columns:
            add_columns(spark, tables, [c.strip() for c in args.add_columns.split(",") if c.strip()])
            return
        for step in steps:
            started = time.time()
            STEPS[step](spark, tables)
            log(f"step {step} done in {time.time() - started:.1f}s")
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as error:  # the CronJob should fail visibly
        print(f"compaction: FAILED: {error}", file=sys.stderr, flush=True)
        sys.exit(1)
