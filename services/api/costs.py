"""Estimate group-VM compute cost from locally scraped infrastructure metrics."""

import json
import math
import os
import time
from urllib.parse import urlencode
from urllib.request import urlopen


PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
HOURLY_RATE_USD = float(os.getenv("VM_HOURLY_COST_USD", "0.0408"))


def query_range(expression: str, start: int, end: int, step: int) -> dict[int, float]:
    parameters = urlencode(
        {"query": expression, "start": start, "end": end, "step": step}
    )
    with urlopen(
        f"{PROMETHEUS_URL}/api/v1/query_range?{parameters}", timeout=5
    ) as response:
        payload = json.load(response)
    if payload.get("status") != "success":
        raise ValueError("Prometheus query failed")
    series = payload.get("data", {}).get("result", [])
    if not series:
        return {}
    return {
        int(timestamp): float(value)
        for timestamp, value in series[0]["values"]
        if math.isfinite(float(value))
    }


def estimate_cost_series(
    availability: dict[int, float], hourly_rate: float, step: int
) -> dict[int, float]:
    """Integrate observed running time; omit missing monitoring intervals."""
    cost = 0.0
    previous_time = None
    previous_up = False
    result = {}
    for timestamp, up in sorted(availability.items()):
        if (
            previous_time is not None
            and previous_up
            and up >= 0.5
            and timestamp - previous_time <= step * 2
        ):
            elapsed = timestamp - previous_time
            cost += elapsed * hourly_rate / 3600
        result[timestamp] = round(cost, 6)
        previous_time = timestamp
        previous_up = up >= 0.5
    return result


def build_cost_overview(hours: int) -> dict:
    if HOURLY_RATE_USD < 0:
        raise ValueError("Hourly rate cannot be negative")
    end = int(time.time())
    start = end - hours * 3600
    step = max(15, math.ceil(hours * 3600 / 240))
    availability = query_range('up{job="node"}', start, end, step)
    if not availability:
        raise ValueError("Node exporter has no samples yet")
    cpu = query_range(
        '100 * (1 - avg(rate(node_cpu_seconds_total{job="node",mode="idle"}[2m])))',
        start, end, step,
    )
    memory = query_range(
        '100 * (1 - node_memory_MemAvailable_bytes{job="node"} '
        '/ node_memory_MemTotal_bytes{job="node"})',
        start, end, step,
    )
    costs = estimate_cost_series(availability, HOURLY_RATE_USD, step)
    points = [
        {
            "timestamp": timestamp,
            "running": availability[timestamp] >= 0.5,
            "cpu_percent": round(cpu[timestamp], 2) if timestamp in cpu else None,
            "memory_percent": round(memory[timestamp], 2)
            if timestamp in memory else None,
            "estimated_compute_cost_usd": costs[timestamp],
        }
        for timestamp in sorted(availability)
    ]
    return {
        "instance_type": "t3a.medium",
        "region": "eu-west-1",
        "window_hours": hours,
        "hourly_rate_usd": HOURLY_RATE_USD,
        "estimated_compute_cost_usd": points[-1]["estimated_compute_cost_usd"],
        "points": points,
    }
