#!/usr/bin/env python3
"""Query benchmark: local execution vs distributed workers (stdlib only).

  bench.py load [--rows N] [--days D]   bulk-load synthetic logs into the
                                        `bench` namespace with Spark (the
                                        compaction image), bypassing the appender
  bench.py run [--workers 0,1,2,4]      time each query shape against otel-query
                                        with that many local workers (0 = local)

Needs the stack from tests/it/stack.sh, the compaction image and a release
build of otel-query (query/target/release/otel-query, or QUERY_BIN). On one
machine the workers share the coordinator's CPUs and disk, so this measures
plan shapes and overhead more than speedup; run it on real nodes for that.
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "it"))
import query_it as q  # noqa: E402

NAMESPACE = "bench"
DAY_US = 86_400_000_000
IMAGE = os.environ.get("COMPACTION_IMAGE", "telemetry-lake/compaction:dev")


def spark_sql(statements):
    to_docker = lambda url: url.replace("localhost", "host.docker.internal")  # noqa: E731
    env = {"ICEBERG_CATALOG_URI": to_docker(q.env("ICEBERG_CATALOG_URI")), "S3_ENDPOINT": to_docker(q.env("S3_ENDPOINT")),
           "S3_ACCESS_KEY": q.env("S3_ACCESS_KEY"), "S3_SECRET_KEY": q.env("S3_SECRET_KEY"),
           "ICEBERG_WAREHOUSE": q.env("ICEBERG_WAREHOUSE", ""), "ICEBERG_NAMESPACE": NAMESPACE}
    argv = ["docker", "run", "--rm", "--add-host", "host.docker.internal:host-gateway"]
    for key, value in env.items():
        argv += ["-e", f"{key}={value}"]
    out = subprocess.run(argv + [IMAGE, "--sql", statements], capture_output=True, text=True)
    lines = [l for l in (out.stdout + out.stderr).splitlines() if l.startswith(("compaction", "  "))]
    if out.returncode != 0:
        sys.exit("spark failed:\n" + "\n".join(lines[-20:]))
    return lines


def load(rows, days):
    end_us = time.time_ns() // 1000
    start_us = end_us - days * DAY_US
    step_us = max(1, (end_us - start_us) // rows)
    table = f"lake.`{NAMESPACE}`.logs"
    started = time.time()
    # The appender's schema (src/appender/schema.hpp), day-partitioned
    spark_sql(f"""
        CREATE NAMESPACE IF NOT EXISTS lake.`{NAMESPACE}`;
        DROP TABLE IF EXISTS {table};
        CREATE TABLE {table} (
          _kafka_topic STRING, _kafka_partition INT, _kafka_offset BIGINT, _record_index INT,
          _ingested_at TIMESTAMP, `timestamp` TIMESTAMP, time_unix_nano BIGINT, observed_time_unix_nano BIGINT,
          severity_number INT, severity_text STRING, body STRING, trace_id STRING, span_id STRING, flags BIGINT,
          event_name STRING, service_name STRING, service_namespace STRING, service_instance_id STRING,
          deployment_environment STRING, host_name STRING, scope_name STRING, scope_version STRING,
          resource_attributes MAP<STRING, STRING>, log_attributes MAP<STRING, STRING>, dropped_attributes_count BIGINT
        ) USING iceberg PARTITIONED BY (days(`timestamp`));
        INSERT INTO {table}
        SELECT 'bench', CAST(id % 8 AS INT), id DIV 50, CAST(id % 50 AS INT), current_timestamp(),
               timestamp_micros({start_us} + id * {step_us}), ({start_us} + id * {step_us}) * 1000, 0,
               element_at(array(9, 13, 17, 21), CAST(id % 4 + 1 AS INT)),
               element_at(array('INFO', 'WARN', 'ERROR', 'FATAL'), CAST(id % 4 + 1 AS INT)),
               concat('request ', id, ' ', IF(id % 97 = 0, 'Timeout after 30s calling upstream', 'completed ok'),
                      ' path=/api/v1/items/', id % 1000, ' user=', id % 5000),
               lower(hex(id * 2654435761)), lower(hex(id)), 1, NULL,
               concat('svc-', id % 40), 'shop', concat('inst-', id % 400), 'prod', concat('node-', id % 100),
               'bench', '1.0',
               map('service.name', concat('svc-', id % 40), 'k8s.pod.name', concat('pod-', id % 400),
                   'k8s.namespace.name', concat('ns-', id % 10)),
               map('http.status_code', element_at(array('200', '200', '200', '404', '500'), CAST(id % 5 + 1 AS INT)),
                   'user', concat('u', id % 5000), 'seq', CAST(id AS STRING)),
               0
        FROM range({rows});
        SELECT count(*), sum(file_size_in_bytes) FROM {table}.files
    """)
    print(f"loaded {rows} rows over {days} days in {time.time() - started:.0f}s")


def queries(end_us):
    ms = lambda us: us // 1000  # noqa: E731  (integer times in the search API are epoch milliseconds)
    day = {"start": ms(end_us - DAY_US - DAY_US // 2), "end": ms(end_us - DAY_US // 2)}
    week = {"start": ms(end_us - 7 * DAY_US + 60_000_000), "end": ms(end_us)}
    hour = {"start": ms(end_us - 3_600_000_000), "end": ms(end_us)}
    return [
        ("newest 100 in the last hour", "search", dict(hour, limit=100)),
        ("grep over a day (newest 100)", "search", dict(day, grep="Timeout", limit=100)),
        ("map tag over a week (newest 100)", "search", dict(week, filters={"k8s.pod.name": "pod-7"}, limit=100)),
        ("count by service over a week (SQL)", "sql",
         {"sql": "SELECT service_name, count(*) FROM logs GROUP BY 1 ORDER BY 2 DESC LIMIT 5"}),
    ]


def time_query(url, kind, body, runs):
    samples = []
    for _ in range(runs):
        started = time.time()
        status, raw = q.http("POST", f"{url}/v1/{kind}", body)
        elapsed = (time.time() - started) * 1000
        if status != 200:
            sys.exit(f"{kind} failed: {status} {raw[:300]!r}")
        samples.append(elapsed)
    return samples


def run(worker_counts, runs):
    query_bin = os.environ.get("QUERY_BIN") or str(q.ROOT / "query/target/release/otel-query")
    if not Path(query_bin).exists():
        sys.exit("build the release binary first: (cd query && cargo build --release)")
    end_us = time.time_ns() // 1000
    results = {}
    for workers in worker_counts:
        work = Path(tempfile.mkdtemp(prefix="bench-"))
        children = []
        env = dict(os.environ, ICEBERG_NAMESPACE=NAMESPACE, RUST_LOG="warn")

        def start(name, argv, **extra):
            log = open(work / f"{name}.log", "w")
            children.append(subprocess.Popen(argv, env=dict(env, **extra), stdout=log, stderr=subprocess.STDOUT))

        try:
            extra = {}
            if workers:
                ports = [50081 + i for i in range(workers)]
                for i, port in enumerate(ports):
                    start(f"worker{i}", [query_bin, "--worker"], WORKER_PORT=str(port), HTTP_PORT=str(18120 + i))
                for i in range(workers):
                    q.wait_for("worker", lambda h=18120 + i: q.http("GET", f"http://localhost:{h}/ready")[0] == 200)
                # Distribute every scan, one task per 64 MiB
                extra = {"QUERY_WORKERS": ",".join(f"http://127.0.0.1:{p}" for p in ports),
                         "QUERY_DISTRIBUTE_MIN_BYTES": "0", "QUERY_BYTES_PER_TASK": str(64 << 20)}
            start("query", [query_bin], HTTP_PORT="18110", QUERY_TIMEOUT_SECONDS="300", **extra)
            url = "http://localhost:18110"
            q.wait_for("query service", lambda: q.http("GET", f"{url}/ready")[0] == 200, timeout=60)
            label = f"{workers} workers" if workers else "local"
            for name, kind, body in queries(end_us):
                time_query(url, kind, body, 1)  # warm caches
                samples = time_query(url, kind, body, runs)
                results.setdefault(name, {})[label] = samples
                print(f"   {label:>9}  {name}: p50 {statistics.median(samples):.0f} ms, max {max(samples):.0f} ms")
        finally:
            for child in children:
                child.terminate()
            for child in children:
                child.wait(timeout=30)
    print(json.dumps({k: {l: round(statistics.median(s)) for l, s in v.items()} for k, v in results.items()}, indent=1))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p_load = sub.add_parser("load")
    p_load.add_argument("--rows", type=int, default=20_000_000)
    p_load.add_argument("--days", type=int, default=7)
    p_run = sub.add_parser("run")
    p_run.add_argument("--workers", default="0,1,2,4")
    p_run.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()
    if args.command == "load":
        load(args.rows, args.days)
    else:
        run([int(w) for w in args.workers.split(",")], args.runs)


if __name__ == "__main__":
    main()
