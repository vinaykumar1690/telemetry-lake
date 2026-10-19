#!/usr/bin/env python3
"""Live integration test of the query service (stdlib only).

Loads known logs through the real ingester and appender into a fresh topic and
Iceberg namespace, starts the query service on that namespace, and checks
time, column-tag, map-tag, grep and regex searches, cursor pagination and SQL
against DuckDB reading the same Iceberg table as an oracle.

Opt-in (QUERY_IT=1). Needs the stack from tests/it/stack.sh, the C++ build
(build/otel_receiver, build/otel_appender) and the query service build
(query/target/{debug,release}/otel-query):

    tests/it/stack.sh up && eval "$(tests/it/stack.sh env)"
    QUERY_IT=1 tests/it/query_it.py
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BATCHES = int(os.environ.get("BATCHES", "60"))
PER_BATCH = int(os.environ.get("PER_BATCH", "50"))
SERVICES = ["checkout", "cart", "auth"]
CODES = ["200", "500", "404"]


def env(name, default=None):
    value = os.environ.get(name, default)
    if value is None:
        sys.exit(f"{name} is required (eval \"$(tests/it/stack.sh env)\")")
    return value


def http(method, url, body=None, accept=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    if accept:
        request.add_header("Accept", accept)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()
    except (urllib.error.URLError, ConnectionError):
        return None, b""


def wait_for(what, check, timeout=120, interval=1.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if check():
            return
        time.sleep(interval)
    sys.exit(f"timed out waiting for {what}")


class Lake:
    """DuckDB over the same Iceberg table: the oracle"""

    def __init__(self, namespace):
        self.namespace = namespace
        endpoint = env("S3_ENDPOINT").split("://")[-1]
        self.prelude = f"""
.output /dev/null
LOAD httpfs; LOAD iceberg;
CREATE SECRET (TYPE S3, KEY_ID '{env("S3_ACCESS_KEY")}', SECRET '{env("S3_SECRET_KEY")}',
               ENDPOINT '{endpoint}', URL_STYLE 'path', USE_SSL false);
ATTACH '{env("ICEBERG_WAREHOUSE", "")}' AS lake (TYPE ICEBERG, ENDPOINT '{env("ICEBERG_CATALOG_URI")}',
                                   AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none');
.output
"""

    def rows(self, sql):
        """Rows of a query (CSV fields as strings); {logs} and {offsets} name the tables"""
        sql = sql.format(logs=f'lake."{self.namespace}".logs', offsets=f'lake."{self.namespace}"._appender_offsets')
        out = subprocess.run([os.environ.get("DUCKDB", "duckdb"), "-csv", "-noheader", "-bail"],
                             input=self.prelude + sql + ";", capture_output=True, text=True)
        if out.returncode != 0:
            return None
        return [line.split(",") for line in out.stdout.strip().splitlines()]

    def scalar(self, where="TRUE"):
        rows = self.rows(f"SELECT count(*) FROM {{logs}} WHERE {where}")
        return int(rows[-1][0]) if rows else None


def records(batch, now_ns):
    out = []
    for i in range(PER_BATCH):
        seq = batch * PER_BATCH + i
        # Spread over the last ~100 minutes; every record of a request shares
        # one timestamp, so pagination has to break ties
        ts = now_ns - (BATCHES - batch) * 100 * 1_000_000_000
        attributes = [{"key": "seq", "value": {"stringValue": str(seq)}},
                      {"key": "http.status_code", "value": {"stringValue": CODES[seq % 3]}}]
        if seq % 4 == 0:
            attributes.append({"key": "user", "value": {"stringValue": f"u{seq}"}})
        body = f"request {seq} " + ("Timeout after 30s" if seq % 5 == 0 else "ok")
        out.append({"timeUnixNano": str(ts), "severityNumber": [9, 13, 17, 21][seq % 4],
                    "body": {"stringValue": body}, "attributes": attributes})
    return out


def send(ingester, batch, now_ns):
    service = SERVICES[batch % 3]
    payload = {"resourceLogs": [{
        "resource": {"attributes": [
            {"key": "service.name", "value": {"stringValue": service}},
            {"key": "k8s.pod.name", "value": {"stringValue": f"pod-{batch % 2}"}}]},
        "scopeLogs": [{"logRecords": records(batch, now_ns)}]}]}
    status, body = http("POST", f"{ingester}/v1/logs", payload)
    if status != 200:
        sys.exit(f"ingester returned {status}: {body!r}")


def search_all(query_url, request, page=250):
    """Every row of a search, following cursors; checks order and uniqueness"""
    rows, cursor = [], None
    while True:
        body = dict(request, limit=page, **({"cursor": cursor} if cursor else {}))
        status, raw = http("POST", f"{query_url}/v1/search", body)
        if status != 200:
            sys.exit(f"search {request} failed: {status} {raw[:300]!r}")
        result = json.loads(raw)
        rows.extend(result["rows"])
        cursor = result.get("next_cursor")
        if not cursor:
            break
    keys = [(r["timestamp"], r["_kafka_topic"], r["_kafka_partition"], r["_kafka_offset"], r["_record_index"])
            for r in rows]
    if len(set(keys)) != len(keys):
        sys.exit(f"duplicate rows across pages for {request}")
    if keys != sorted(keys, reverse=True):
        sys.exit(f"rows not newest-first for {request}")
    return rows


WINDOW = {"start": "now-3h", "end": "now"}

# (name, search request fields, the same condition as SQL for DuckDB)
CASES = [
    ("everything", {}, "TRUE"),
    ("column tag", {"filters": {"service.name": "cart"}}, "service_name = 'cart'"),
    ("map tag (log attr)", {"filters": {"attr.http.status_code": ["500", "404"]}},
     "log_attributes['http.status_code'] IN ('500', '404')"),
    ("map tag (resource attr)", {"filters": {"k8s.pod.name": "pod-1"}},
     "resource_attributes['k8s.pod.name'] = 'pod-1'"),
    ("numeric attr", {"where": {"field": "attr.http.status_code", "op": ">=", "value": 404}},
     "TRY_CAST(log_attributes['http.status_code'] AS DOUBLE) >= 404"),
    ("absent attr", {"where": {"field": "attr.user", "op": "not_exists"}},
     "log_attributes['user'] IS NULL"),
    ("grep", {"grep": "Timeout"}, "body LIKE '%Timeout%'"),
    ("grep ignore case", {"grep": "timeout", "grep_ignore_case": True}, "body ILIKE '%timeout%'"),
    ("regex", {"regex": r"^request 1\d* "}, r"regexp_matches(body, '^request 1\d* ')"),
    ("severity", {"where": {"field": "severity_number", "op": ">=", "value": 17}}, "severity_number >= 17"),
    ("combined", {"filters": {"service.name": "checkout"}, "grep": "Timeout",
                  "where": {"field": "attr.http.status_code", "op": "!=", "value": "200"}},
     "service_name = 'checkout' AND body LIKE '%Timeout%' "
     "AND log_attributes['http.status_code'] IS DISTINCT FROM '200'"),
]


def run_cases(query_url, lake, window=WINDOW):
    """Runs every search in CASES (all pages) and compares with DuckDB; returns the failure count"""
    failures = 0
    for name, extra, oracle_where in CASES:
        started = time.time()
        got = len(search_all(query_url, dict(window, **extra)))
        want = lake.scalar(oracle_where)
        ok_case = got == want and want is not None
        failures += not ok_case
        print(f"   {'ok ' if ok_case else 'FAIL'} {name}: {got} rows (oracle {want}) "
              f"in {time.time() - started:.2f}s")
    return failures


def main():
    if os.environ.get("QUERY_IT") != "1":
        print("skipped: set QUERY_IT=1 to run against live services")
        return
    for name in ["KAFKA_BROKERS", "ICEBERG_CATALOG_URI", "S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY"]:
        env(name)
    query_bin = next((p for p in [os.environ.get("QUERY_BIN"),
                                  ROOT / "query/target/release/otel-query",
                                  ROOT / "query/target/debug/otel-query"] if p and Path(p).exists()), None)
    if not query_bin:
        sys.exit("build the query service first: (cd query && cargo build)")

    run = str(int(time.time()))
    topic, namespace = f"qit-{run}", f"qit_{run}"
    work = Path(tempfile.mkdtemp(prefix="query-it-"))
    children = []
    child_env = dict(os.environ, KAFKA_TOPIC=topic, KAFKA_DLQ_TOPIC=f"{topic}-dlq", ICEBERG_NAMESPACE=namespace,
                     KAFKA_CONSUMER_GROUP=f"qit-{run}", BUFFER_TIME_SECONDS="2",
                     DUCKDB_HOME_DIRECTORY=os.environ.get("DUCKDB_HOME_DIRECTORY", str(Path.home() / ".duckdb")))

    def start(name, argv, **extra):
        log = open(work / f"{name}.log", "w")
        child = subprocess.Popen(argv, env=dict(child_env, **extra), stdout=log, stderr=subprocess.STDOUT)
        children.append(child)
        return child

    ok = False
    try:
        print(f"== run {run}: topic {topic}, namespace {namespace}")
        for t, partitions in [(topic, "4"), (f"{topic}-dlq", "1")]:
            subprocess.run([str(ROOT / "tests/it/stack.sh"), "topic", t, partitions], check=True)
        start("ingester", [str(ROOT / "build/otel_receiver")], HTTP_PORT="14319")
        start("appender", [str(ROOT / "build/otel_appender")], HEALTH_PORT="18082", APPENDER_ID="qit")
        ingester, appender = "http://localhost:14319", "http://localhost:18082"
        wait_for("ingester", lambda: http("GET", f"{ingester}/health")[0] == 200)
        wait_for("appender", lambda: http("GET", f"{appender}/ready")[0] == 200)

        now_ns = time.time_ns()
        print(f"== sending {BATCHES} x {PER_BATCH} records")
        for batch in range(BATCHES):
            send(ingester, batch, now_ns)
        total = BATCHES * PER_BATCH
        lake = Lake(namespace)

        def landed():
            http("POST", f"{appender}/flush")
            return lake.scalar() == total
        wait_for("all rows in Iceberg", landed, timeout=180, interval=3)

        start("query", [str(query_bin)], HTTP_PORT="18090", TABLE_REFRESH_MS="1000", RUST_LOG="info")
        query_url = "http://localhost:18090"
        wait_for("query service", lambda: http("GET", f"{query_url}/ready")[0] == 200, timeout=60)

        window = WINDOW
        failures = run_cases(query_url, lake)

        # Latency of the common case: the newest page of 100 rows
        for name, extra in [("newest 100", {}), ("newest 100, tag", {"filters": {"service.name": "auth"}}),
                            ("newest 100, map tag", {"filters": {"k8s.pod.name": "pod-0"}}),
                            ("newest 100, grep", {"grep": "Timeout"})]:
            samples = []
            for _ in range(5):
                started = time.time()
                status, _ = http("POST", f"{query_url}/v1/search", dict(window, limit=100, **extra))
                samples.append((time.time() - started) * 1000)
                failures += status != 200
            samples.sort()
            print(f"   latency {name}: p50 {samples[2]:.0f} ms, max {samples[-1]:.0f} ms")

        # A narrow time range comes back from pruned files
        status, raw = http("POST", f"{query_url}/v1/search", {"start": "now-10m", "limit": 1000})
        recent = len(json.loads(raw)["rows"]) if status == 200 else -1
        print(f"   {'ok ' if status == 200 else 'FAIL'} last 10 minutes: {recent} rows")
        failures += status != 200

        # SQL agrees with DuckDB, and the time filter reaches the Iceberg scan
        status, raw = http("POST", f"{query_url}/v1/sql", {"sql": "SELECT count(*) AS n FROM logs"})
        sql_count = json.loads(raw.splitlines()[0])["n"] if status == 200 else None
        print(f"   {'ok ' if sql_count == total else 'FAIL'} sql count: {sql_count}")
        failures += sql_count != total
        explain = "EXPLAIN SELECT body FROM logs WHERE \"timestamp\" >= now() - INTERVAL '10 minutes'"
        status, raw = http("POST", f"{query_url}/v1/sql", {"sql": explain})
        pushed = status == 200 and b"IcebergTableScan" in raw and b"predicate:[timestamp >=" in raw
        print(f"   {'ok ' if pushed else 'FAIL'} time filter pushed into IcebergTableScan")
        failures += not pushed

        status, raw = http("GET", f"{query_url}/metrics")
        served = [line for line in raw.decode().splitlines() if 'outcome="ok"' in line]
        print(f"   metrics: {served}")
        ok = failures == 0
        print("PASS" if ok else f"FAIL: {failures} checks failed")
    finally:
        for child in children:
            child.terminate()
        for child in children:
            try:
                child.wait(timeout=60)
            except subprocess.TimeoutExpired:
                child.kill()
        if ok:
            shutil.rmtree(work, ignore_errors=True)
        else:
            print(f"logs kept in {work}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
