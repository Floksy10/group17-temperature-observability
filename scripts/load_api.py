"""Small, repeatable HTTP load probe for the Temperature Observability API."""

import argparse
import concurrent.futures
import statistics
import time
import urllib.parse
import urllib.request


def request(url: str) -> tuple[bool, float]:
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            response.read()
            return response.status == 200, time.perf_counter() - started
    except Exception:
        return False, time.perf_counter() - started


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--requests", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=20)
    arguments = parser.parse_args()

    if arguments.requests < 1 or arguments.concurrency < 1:
        parser.error("requests and concurrency must both be positive")

    query = urllib.parse.urlencode({"experiment-id": arguments.experiment_id})
    url = arguments.base_url.rstrip("/") + "/temperature/out-of-range?" + query
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=arguments.concurrency
    ) as executor:
        results = list(executor.map(request, [url] * arguments.requests))
    duration = time.perf_counter() - started
    latencies = sorted(latency for _, latency in results)
    successes = sum(ok for ok, _ in results)

    def percentile(fraction: float) -> float:
        return latencies[min(len(latencies) - 1, int(fraction * len(latencies)))]

    print(f"requests={arguments.requests} successful={successes}")
    print(f"duration_seconds={duration:.3f} requests_per_second={len(results)/duration:.1f}")
    print(
        f"latency_ms p50={statistics.median(latencies)*1000:.1f} "
        f"p95={percentile(0.95)*1000:.1f} p99={percentile(0.99)*1000:.1f}"
    )


if __name__ == "__main__":
    main()
