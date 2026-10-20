#!/usr/bin/env python3
"""Live integration test of table compaction (stdlib only).

Writes logs through the real ingester and appender with a commit after every
request (many small files), with two promoted attributes. It then runs the
compaction job while more logs arrive, and checks that:

- the data-file count drops and the original rows are unchanged (count and
  checksum), with no duplicate row keys
- the offsets table is pruned and keeps every partition's maximum offset
- the rewritten files have bloom filters (including on a promoted column)
  and are sorted by service
- a search paginated across the compaction neither skips nor repeats rows
- every query_it search still matches DuckDB, and promoted fields filter on
  their column inside the Iceberg scan

Opt-in (COMPACTION_IT=1). Needs the stack from tests/it/stack.sh, the C++
and query service builds, and the compaction image:

    docker build -f docker/compaction/Dockerfile -t telemetry-lake/compaction:dev .
    tests/it/stack.sh up && eval "$(tests/it/stack.sh env)"
    COMPACTION_IT=1 tests/it/compaction_it.py
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import query_it as q  # noqa: E402  (shared helpers and the search cases)

ROOT = q.ROOT
IMAGE = os.environ.get("COMPACTION_IMAGE", "telemetry-lake/compaction:dev")
PROMOTED = "k8s.pod.name,http.status_code"
EXTRA_BATCHES = 5


def compaction(namespace, *args, **extra_env):
    """Runs compact.py in its container; returns its output lines"""
    to_docker = lambda url: url.replace("localhost", "host.docker.internal")  # noqa: E731
    env = {"ICEBERG_CATALOG_URI": to_docker(q.env("ICEBERG_CATALOG_URI")),
           "S3_ENDPOINT": to_docker(q.env("S3_ENDPOINT")),
           "S3_ACCESS_KEY": q.env("S3_ACCESS_KEY"), "S3_SECRET_KEY": q.env("S3_SECRET_KEY"),
           "ICEBERG_WAREHOUSE": q.env("ICEBERG_WAREHOUSE", ""), "ICEBERG_NAMESPACE": namespace,
           "PROMOTED_ATTRIBUTES": PROMOTED, **extra_env}
    argv = ["docker", "run", "--rm", "--add-host", "host.docker.internal:host-gateway"]
    for key, value in env.items():
        argv += ["-e", f"{key}={value}"]
    out = subprocess.run(argv + [IMAGE, *args], capture_output=True, text=True)
    lines = [l for l in out.stdout.splitlines() + out.stderr.splitlines() if l.startswith(("compaction", "  "))]
    if out.returncode != 0:
        sys.exit("compaction job failed:\n" + "\n".join(lines[-20:]))
    return lines


def data_files(namespace):
    lines = compaction(namespace, "--sql", f"SELECT count(*) FROM lake.`{namespace}`.logs.files")
    return int(lines[-1].strip())


def largest_file(namespace):
    lines = compaction(namespace, "--sql",
                       f"SELECT file_path FROM lake.`{namespace}`.logs.files ORDER BY file_size_in_bytes DESC LIMIT 1")
    return lines[-1].strip()


def staged_rows(appender):
    status, raw = q.http("GET", f"{appender}/metrics")
    for line in raw.decode().splitlines() if status == 200 else []:
        if line.startswith("appender_rows_staged_total "):
            return int(float(line.split()[1]))
    return 0


def send_and_commit(ingester, appender, batch, now_ns):
    """One request, one Iceberg commit: flush only once the appender staged it"""
    before = staged_rows(appender)
    q.send(ingester, batch, now_ns)
    q.wait_for(f"batch {batch} staged", lambda: staged_rows(appender) >= before + q.PER_BATCH, timeout=30,
               interval=0.05)
    q.http("POST", f"{appender}/flush")


def main():
    if os.environ.get("COMPACTION_IT") != "1":
        print("skipped: set COMPACTION_IT=1 to run against live services")
        return
    query_bin = next((p for p in [os.environ.get("QUERY_BIN"), ROOT / "query/target/release/otel-query",
                                  ROOT / "query/target/debug/otel-query"] if p and Path(p).exists()), None)
    if not query_bin:
        sys.exit("build the query service first: (cd query && cargo build)")

    run = str(int(time.time()))
    topic, namespace = f"cit-{run}", f"cit_{run}"
    work = Path(tempfile.mkdtemp(prefix="compaction-it-"))
    children = []
    child_env = dict(os.environ, KAFKA_TOPIC=topic, KAFKA_DLQ_TOPIC=f"{topic}-dlq", ICEBERG_NAMESPACE=namespace,
                     KAFKA_CONSUMER_GROUP=f"cit-{run}", PROMOTED_ATTRIBUTES=PROMOTED,
                     DUCKDB_HOME_DIRECTORY=os.environ.get("DUCKDB_HOME_DIRECTORY", str(Path.home() / ".duckdb")))

    def start(name, argv, **extra):
        log = open(work / f"{name}.log", "w")
        child = subprocess.Popen(argv, env=dict(child_env, **extra), stdout=log, stderr=subprocess.STDOUT)
        children.append(child)

    failures = []

    def check(ok, message):
        print(f"   {'ok ' if ok else 'FAIL'} {message}")
        if not ok:
            failures.append(message)

    try:
        print(f"== run {run}: topic {topic}, namespace {namespace}")
        for t, partitions in [(topic, "4"), (f"{topic}-dlq", "1")]:
            subprocess.run([str(ROOT / "tests/it/stack.sh"), "topic", t, partitions], check=True)
        start("ingester", [str(ROOT / "build/otel_receiver")], HTTP_PORT="14320")
        start("appender", [str(ROOT / "build/otel_appender")], HEALTH_PORT="18083", APPENDER_ID="cit")
        ingester, appender = "http://localhost:14320", "http://localhost:18083"
        q.wait_for("ingester", lambda: q.http("GET", f"{ingester}/health")[0] == 200)
        q.wait_for("appender", lambda: q.http("GET", f"{appender}/ready")[0] == 200)

        lake = q.Lake(namespace)
        now_ns = time.time_ns()
        total = q.BATCHES * q.PER_BATCH
        print(f"== {q.BATCHES} requests of {q.PER_BATCH} records, one commit each")
        for batch in range(q.BATCHES):
            send_and_commit(ingester, appender, batch, now_ns)
        q.wait_for("all rows in Iceberg", lambda: lake.scalar() == total, timeout=180, interval=2)

        checksum_sql = ("SELECT count(*), sum(hash(_kafka_topic, _kafka_partition, _kafka_offset, _record_index, "
                        "body, k8s_pod_name, http_status_code)) FROM {logs} WHERE CAST(log_attributes['seq'] AS INT) < "
                        + str(total))
        checksum_before = lake.rows(checksum_sql)
        max_before = dict(map(tuple, lake.rows('SELECT "partition", max(last_offset) FROM {offsets} GROUP BY 1')))
        offsets_rows_before = int(lake.rows("SELECT count(*) FROM {offsets}")[0][0])
        files_before = data_files(namespace)
        print(f"   {total} rows in {files_before} data files, {offsets_rows_before} offsets rows")

        start("query", [str(query_bin)], HTTP_PORT="18091", TABLE_REFRESH_MS="1000")
        query_url = "http://localhost:18091"
        q.wait_for("query service", lambda: q.http("GET", f"{query_url}/ready")[0] == 200, timeout=60)
        status, raw = q.http("POST", f"{query_url}/v1/search", dict(q.WINDOW, limit=700))
        first_page = json.loads(raw)
        cursor = first_page["next_cursor"]

        print("== compaction while more logs arrive")
        def more():
            # Dated in the past like the rest (the searches end at "now")
            earlier_ns = now_ns - (EXTRA_BATCHES + 1) * 100 * 1_000_000_000
            for batch in range(q.BATCHES, q.BATCHES + EXTRA_BATCHES):
                send_and_commit(ingester, appender, batch, earlier_ns)
                time.sleep(1)
        sender = threading.Thread(target=more)
        sender.start()
        started = time.time()
        for line in compaction(namespace, COMPACTION_STEPS="properties,compact,offsets,manifests",
                               COMPACTION_MIN_AGE_MINUTES="0", OFFSETS_MIN_AGE_MINUTES="0"):
            if line.startswith("compaction: ") and "step " not in line:
                print(f"   {line}")
        print(f"   compaction took {time.time() - started:.1f}s")
        sender.join()
        grand_total = total + EXTRA_BATCHES * q.PER_BATCH
        q.wait_for("the extra rows", lambda: lake.scalar() == grand_total, timeout=120, interval=2)

        print("== checks")
        files_after = data_files(namespace)
        # The rows sent during the compaction add a few uncompacted files
        check(files_after <= EXTRA_BATCHES + 2 < files_before, f"data files {files_before} -> {files_after}")
        check(lake.rows(checksum_sql) == checksum_before, f"original rows unchanged (count, checksum) {checksum_before}")
        keys = lake.rows("SELECT count(*), count(DISTINCT (_kafka_topic, _kafka_partition, _kafka_offset, "
                         "_record_index)) FROM {logs}")[0]
        check(keys[0] == keys[1] == str(grand_total), f"{keys[0]} rows, {keys[1]} distinct keys (expect {grand_total})")
        max_after = dict(map(tuple, lake.rows('SELECT "partition", max(last_offset) FROM {offsets} GROUP BY 1')))
        check(all(int(max_after[p]) >= int(v) for p, v in max_before.items()),
              f"offsets keep each partition's max: {max_before} -> {max_after}")
        offsets_rows_after = int(lake.rows("SELECT count(*) FROM {offsets}")[0][0])
        check(offsets_rows_after < offsets_rows_before,
              f"offsets rows pruned {offsets_rows_before} -> {offsets_rows_after}")

        biggest = largest_file(namespace)
        # Parquet skips the bloom filter of a fully dictionary-encoded column
        # (the dictionary answers membership already), e.g. a 2-value pod name
        meta = {r[0]: r[1:] for r in lake.rows(
            f"SELECT path_in_schema, coalesce(bloom_filter_offset::VARCHAR, 'none'), "
            f"replace(encodings, ',', ' ') FROM parquet_metadata('{biggest}') "
            "WHERE row_group_id = 0 AND path_in_schema IN ('trace_id', 'k8s_pod_name', 'body')")}
        indexed = lambda c: c in meta and (meta[c][0] != "none" or "DICTIONARY" in meta[c][1])  # noqa: E731
        check(meta.get("trace_id", ["none"])[0] != "none" and indexed("k8s_pod_name") and meta["body"][0] == "none",
              f"bloom filters (or dictionaries) in the largest file: {meta}")
        services = [r[0] for r in lake.rows(f"SELECT service_name FROM read_parquet('{biggest}')")]
        check(services == sorted(services), f"largest file sorted by service ({len(services)} rows)")

        # Continue the cursor from before the compaction
        seqs = [int(r["log_attributes"]["seq"]) for r in first_page["rows"]]
        while cursor:
            status, raw = q.http("POST", f"{query_url}/v1/search", dict(q.WINDOW, limit=700, cursor=cursor))
            page = json.loads(raw)
            if status != 200:
                failures.append(f"pagination failed: {status} {page}")
                break
            seqs += [int(r["log_attributes"]["seq"]) for r in page["rows"]]
            cursor = page.get("next_cursor")
        check(len(seqs) == len(set(seqs)) and set(range(total)) <= set(seqs),
              f"pagination across compaction: {len(seqs)} rows, no repeats, none of the {total} skipped")

        status, raw = q.http("POST", f"{query_url}/v1/sql", {"sql": "EXPLAIN SELECT body FROM logs "
                                                                     "WHERE k8s_pod_name = 'pod-1'"})
        check(status == 200 and q.scan_has_predicate(raw, "k8s_pod_name ="),
              "promoted column filter reaches the Iceberg scan")
        print("== query_it searches after compaction")
        if q.run_cases(query_url, lake):
            failures.append("query_it searches")
    finally:
        for child in children:
            child.terminate()
        for child in children:
            try:
                child.wait(timeout=60)
            except subprocess.TimeoutExpired:
                child.kill()
        if failures:
            print(f"logs kept in {work}")
        else:
            shutil.rmtree(work, ignore_errors=True)
    if failures:
        sys.exit(f"FAIL: {len(failures)} checks failed")
    print("PASS")


if __name__ == "__main__":
    main()
