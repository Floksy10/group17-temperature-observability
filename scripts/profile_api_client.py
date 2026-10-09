"""Timing client using exported real experiment histories, with burst/steady load.

Each JSONL result includes TCP connection, HTTP headers/body, optional isolated
server stage timings, and response consistency against the exported histories.
Uses only the Python standard library; suitable for running on client2.
"""

import argparse
import asyncio
import concurrent.futures
import http.client
import json
import math
import random
import statistics
import threading
import time
import urllib.parse
from pathlib import Path


def percentile(values, fraction):
    values = sorted(values)
    return values[min(len(values) - 1, max(0, math.ceil(len(values) * fraction) - 1))]


def summarize(records):
    good = [r for r in records if r.get("status") == 200 and r.get("valid")]
    metrics = {}
    names = sorted({k for r in records for k, v in r.items() if (k.endswith("_ms") or k.startswith("server_")) and isinstance(v, (int, float))})
    for name in names:
        values = [r[name] for r in good if name in r]
        if values:
            metrics[name] = {"mean": statistics.fmean(values), "p50": statistics.median(values), "p95": percentile(values, .95), "max": max(values)}
    hits = [r for r in good if r.get("cache") in {"True", "False"}]
    return {"requests": len(records), "valid": len(good), "errors": len(records) - len(good),
            "workers_seen": sorted({r.get("pid", "unavailable") for r in good}),
            "cache_hit_fraction": sum(r["cache"] == "True" for r in hits) / len(hits) if hits else None,
            "response_header_thresholds_ms": {str(limit): {"count": sum(r.get("ttfb_ms", float("inf")) <= limit for r in good), "fraction": sum(r.get("ttfb_ms", float("inf")) <= limit for r in good) / len(good) if good else None} for limit in [5, 10, 25, 50, 100, 250, 500]},
            "metrics": metrics}


def validate_deferred(records):
    for record in records:
        if "_body" in record:
            try:
                record["valid"] = json.loads(record.pop("_body")) == record.pop("_expected")
            except Exception as error:
                record.pop("_expected", None)
                record.update(valid=False, error=type(error).__name__ + ": " + str(error))


async def run_async(args, url, make_query):
    """Independent I/O scheduling check without fifty Python client threads."""
    queue = asyncio.Queue()

    async def worker():
        reader = writer = None
        while True:
            job = await queue.get()
            if job is None:
                if writer:
                    writer.close()
                    await writer.wait_closed()
                return
            index, query, submitted, future = job
            record = {"id": index, "started_utc_epoch": time.time(), "path": query[0].split("?", 1)[0], "experiment_id": query[2], "submission_wait_ms": (time.perf_counter() - submitted) * 1000}
            started = time.perf_counter()
            try:
                connect_start = time.perf_counter()
                if writer is None or args.new_connection:
                    reader, writer = await asyncio.wait_for(asyncio.open_connection(url.hostname, url.port or 80), 10)
                record["tcp_connect_ms"] = (time.perf_counter() - connect_start) * 1000
                send_start = time.perf_counter()
                request = f"GET {query[0]} HTTP/1.1\r\nHost: {url.netloc}\r\nX-Profile-Request-Id: {index}\r\n\r\n"
                writer.write(request.encode("ascii"))
                await writer.drain()
                record["client_send_ms"] = (time.perf_counter() - send_start) * 1000
                raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)
                headers_at = time.perf_counter()
                lines = raw.decode("latin1").split("\r\n")
                record["status"] = int(lines[0].split()[1])
                headers = dict(line.lower().split(": ", 1) for line in lines[1:] if ": " in line)
                record["ttfb_ms"] = (headers_at - started) * 1000
                record["pid"] = headers.get("x-profile-process", "unavailable")
                record["cache"] = headers.get("x-profile-cache", "unavailable").title()
                record["history_rows"] = int(headers.get("x-profile-rows", "0"))
                for item in headers.get("server-timing", "").split(","):
                    if ";dur=" in item:
                        name, value = item.strip().split(";dur=", 1)
                        record["server_" + name] = float(value)
                if "content-length" not in headers or "transfer-encoding" in headers:
                    raise ValueError("This diagnostic reader requires a Content-Length JSON response")
                body = await asyncio.wait_for(reader.readexactly(int(headers["content-length"])), 10)
                record["body_ms"] = (time.perf_counter() - headers_at) * 1000
                record["total_ms"] = (time.perf_counter() - started) * 1000
                record["bytes"] = len(body)
                if args.defer_validation:
                    record.update(_body=body, _expected=query[1])
                else:
                    record["valid"] = json.loads(body) == query[1]
                if "server_server_headers" in record:
                    record["transport_and_server_socket_queue_ms"] = record["ttfb_ms"] - record["server_server_headers"]
                    record["remaining_transport_socket_client_ms"] = record["transport_and_server_socket_queue_ms"] - record.get("server_protocol_queue", 0)
                if args.new_connection:
                    writer.close()
                    await writer.wait_closed()
                    reader = writer = None
            except Exception as error:
                record.update(error=type(error).__name__ + ": " + str(error), valid=False, total_ms=(time.perf_counter() - started) * 1000)
                if writer:
                    writer.close()
                reader = writer = None
            future.set_result(record)

    workers = [asyncio.create_task(worker()) for _ in range(1 if args.pattern == "serial" else args.concurrency)]

    def submit(index):
        query = make_query(index)
        future = asyncio.get_running_loop().create_future()
        queue.put_nowait((index, query, time.perf_counter(), future))
        return future

    if args.warmup:
        warm = [submit(-i - 1) for i in range(args.warmup)]
        warm_records = await asyncio.gather(*warm)
        validate_deferred(warm_records)
        print("warmup", json.dumps(summarize(warm_records)))
    began = time.perf_counter()
    began_utc = time.time()
    futures = []
    if args.pattern == "serial":
        for i in range(args.requests):
            future = submit(i)
            futures.append(future)
            await future
    elif args.pattern == "burst":
        for second in range(args.seconds):
            batch = [submit(second * args.rate + i) for i in range(args.rate)]
            futures.extend(batch)
            await asyncio.gather(*batch)
            await asyncio.sleep(max(0, began + second + 1 - time.perf_counter()))
    else:
        for i in range(args.seconds * args.rate):
            await asyncio.sleep(max(0, began + i / args.rate - time.perf_counter()))
            futures.append(submit(i))
    records = await asyncio.gather(*futures)
    elapsed = time.perf_counter() - began
    validate_deferred(records)
    for _ in workers:
        queue.put_nowait(None)
    await asyncio.gather(*workers)
    return records, elapsed, began_utc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--fixtures", required=True)
    parser.add_argument("--experiments", type=int, default=100)
    parser.add_argument("--pattern", choices=["serial", "burst", "steady"], default="burst")
    parser.add_argument("--rate", type=int, default=94)
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--warmup", type=int, default=0)
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--new-connection", action="store_true")
    parser.add_argument("--endpoint", choices=["mixed", "temperature", "out-of-range", "health"], default="mixed")
    parser.add_argument("--output", required=True)
    parser.add_argument("--client-mode", choices=["threads", "async"], default="threads")
    parser.add_argument("--defer-validation", action="store_true", help="Validate all bodies after timed requests complete, avoiding Python validation interference")
    args = parser.parse_args()
    url = urllib.parse.urlsplit(args.base_url)
    if url.scheme != "http":
        parser.error("diagnostic client currently supports HTTP only")
    fixtures = json.loads(Path(args.fixtures).read_text())[:args.experiments]
    if not fixtures:
        parser.error("no fixtures")
    rng = random.Random(args.seed)
    local = threading.local()

    def make_query(index):
        fixture = fixtures[index % len(fixtures)] if args.pattern == "serial" else rng.choice(fixtures)
        points = fixture["points"]
        endpoint = args.endpoint
        if endpoint == "mixed":
            endpoint = "temperature" if rng.randrange(2) else "out-of-range"
        if endpoint == "health":
            return "/health", {"status": "ok"}, fixture["id"]
        params = {"experiment-id": fixture["id"]}
        if endpoint == "temperature":
            a, b = sorted([rng.randrange(len(points)), rng.randrange(len(points))])
            start = math.floor(points[a]["timestamp"] * 1000) / 1000
            end = math.ceil(points[b]["timestamp"] * 1000) / 1000
            params.update({"start-time": start, "end-time": end})
            expected = [{"timestamp": p["timestamp"], "temperature": p["temperature"]} for p in points if start <= p["timestamp"] <= end]
            path = "/temperature"
        else:
            expected = [{"timestamp": p["timestamp"], "temperature": p["temperature"]} for p in points if p["outside"]]
            path = "/temperature/out-of-range"
        return path + "?" + urllib.parse.urlencode(params), expected, fixture["id"]

    def perform(index, query, submitted):
        record = {"id": index, "started_utc_epoch": time.time(), "path": query[0].split("?", 1)[0], "experiment_id": query[2], "submission_wait_ms": (time.perf_counter() - submitted) * 1000}
        started = time.perf_counter()
        try:
            conn = getattr(local, "connection", None)
            if conn is None or args.new_connection:
                conn = http.client.HTTPConnection(url.hostname, url.port or 80, timeout=10)
                local.connection = conn
            connect_start = time.perf_counter()
            if conn.sock is None:
                conn.connect()
            record["tcp_connect_ms"] = (time.perf_counter() - connect_start) * 1000
            send_start = time.perf_counter()
            conn.request("GET", query[0], headers={"X-Profile-Request-Id": str(index)})
            record["client_send_ms"] = (time.perf_counter() - send_start) * 1000
            response = conn.getresponse()
            headers_at = time.perf_counter()
            record["status"] = response.status
            record["ttfb_ms"] = (headers_at - started) * 1000
            record["pid"] = response.getheader("X-Profile-Process", "unavailable")
            record["cache"] = response.getheader("X-Profile-Cache", "unavailable")
            record["history_rows"] = int(response.getheader("X-Profile-Rows", "0"))
            timing = response.getheader("Server-Timing", "")
            for item in timing.split(","):
                if ";dur=" in item:
                    name, value = item.strip().split(";dur=", 1)
                    record["server_" + name] = float(value)
            body = response.read()
            record["body_ms"] = (time.perf_counter() - headers_at) * 1000
            record["total_ms"] = (time.perf_counter() - started) * 1000
            record["bytes"] = len(body)
            if args.defer_validation:
                record.update(_body=body, _expected=query[1])
            else:
                value = json.loads(body)
                record["valid"] = value == query[1]
                if not record["valid"]:
                    record["actual_rows"] = len(value) if isinstance(value, list) else None
                    record["expected_rows"] = len(query[1]) if isinstance(query[1], list) else None
            if "server_server_headers" in record:
                record["transport_and_server_socket_queue_ms"] = record["ttfb_ms"] - record["server_server_headers"]
            if args.new_connection:
                conn.close()
        except Exception as error:
            record.update(error=type(error).__name__+": "+str(error), valid=False, total_ms=(time.perf_counter()-started)*1000)
            if getattr(local, "connection", None):
                local.connection.close()
                local.connection = None
        return record

    if args.client_mode == "async":
        records, elapsed, began_utc = asyncio.run(run_async(args, url, make_query))
        write_results(args, records, elapsed, began_utc)
        return

    concurrency = 1 if args.pattern == "serial" else args.concurrency
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        barrier = threading.Barrier(concurrency + 1)
        warmed_threads = [executor.submit(barrier.wait) for _ in range(concurrency)]
        barrier.wait(timeout=20)
        for future in warmed_threads:
            future.result()
        if args.warmup:
            warm_futures = [executor.submit(perform, -i-1, make_query(i), time.perf_counter()) for i in range(args.warmup)]
            warm_results = [f.result() for f in warm_futures]
            validate_deferred(warm_results)
            print("warmup", json.dumps(summarize(warm_results)))
        began = time.perf_counter()
        began_utc = time.time()
        futures = []
        if args.pattern == "serial":
            for i in range(args.requests):
                futures.append(executor.submit(perform, i, make_query(i), time.perf_counter()))
                futures[-1].result()
        elif args.pattern == "burst":
            for second in range(args.seconds):
                batch = [executor.submit(perform, second*args.rate+i, make_query(second*args.rate+i), time.perf_counter()) for i in range(args.rate)]
                futures.extend(batch)
                for f in batch:
                    f.result()
                time.sleep(max(0, began + second + 1 - time.perf_counter()))
        else:
            for i in range(args.seconds*args.rate):
                time.sleep(max(0, began + i/args.rate - time.perf_counter()))
                futures.append(executor.submit(perform, i, make_query(i), time.perf_counter()))
        records = [future.result() for future in futures]
        elapsed = time.perf_counter() - began
        validate_deferred(records)
    write_results(args, records, elapsed, began_utc)


def write_results(args, records, elapsed, began_utc):
    output = Path(args.output)
    output.write_text("".join(json.dumps(record, separators=(",", ":"))+"\n" for record in records))
    summary = {"configuration": vars(args), "started_utc_epoch": began_utc, "elapsed_seconds": elapsed, "requests_per_second": len(records)/elapsed, **summarize(records)}
    output.with_suffix(".summary.json").write_text(json.dumps(summary, indent=2)+"\n")
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
