"""Historic temperature REST API required by the course assignment."""

import os
import json
import math
import re
import time
from bisect import bisect_left, bisect_right
from collections import OrderedDict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock

from fastapi import FastAPI, HTTPException, Query, Request
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Histogram, generate_latest, multiprocess
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row
from starlette.responses import HTMLResponse, Response
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import QueryParams

from costs import build_cost_overview


REQUESTS = Counter(
    "group17_http_requests_total", "HTTP requests", ["path", "status"]
)
REQUEST_DURATION = Histogram(
    "group17_http_request_seconds", "HTTP request duration", ["path"]
)
CACHE_LOOKUPS = Counter(
    "group17_api_cache_lookups_total", "Historic API cache lookups", ["result"]
)


@dataclass(frozen=True)
class ExperimentSeries:
    timestamps: tuple[float, ...]
    points: tuple[dict, ...]
    out_of_range: tuple[dict, ...]
    encoded_points: tuple[bytes, ...] = field(init=False, repr=False)
    encoded_out_of_range: bytes = field(init=False, repr=False)

    def __post_init__(self) -> None:
        def encode(point: dict) -> bytes:
            return json.dumps(
                point, ensure_ascii=False, allow_nan=False, separators=(",", ":")
            ).encode("utf-8")

        object.__setattr__(self, "encoded_points", tuple(map(encode, self.points)))
        object.__setattr__(
            self, "encoded_out_of_range",
            b"[" + b",".join(map(encode, self.out_of_range)) + b"]",
        )

    def temperature_json(self, start_time: float, end_time: float) -> bytes:
        start = bisect_left(self.timestamps, start_time)
        end = bisect_right(self.timestamps, end_time)
        return b"[" + b",".join(self.encoded_points[start:end]) + b"]"


class ExperimentSeriesCache:
    """Bounded per-worker cache for immutable, terminated experiments."""

    def __init__(self, max_size: int) -> None:
        self.max_size = max_size
        self._items: OrderedDict[str, ExperimentSeries] = OrderedDict()
        self._lock = Lock()

    def get(self, experiment_id: str) -> ExperimentSeries | None:
        with self._lock:
            series = self._items.get(experiment_id)
            if series is not None:
                self._items.move_to_end(experiment_id)
            return series

    def put(self, experiment_id: str, series: ExperimentSeries) -> None:
        with self._lock:
            self._items[experiment_id] = series
            self._items.move_to_end(experiment_id)
            while len(self._items) > self.max_size:
                self._items.popitem(last=False)


@asynccontextmanager
async def lifespan(application: FastAPI):
    with ConnectionPool(
        os.environ["DATABASE_URL"], min_size=1, max_size=15
    ) as pool:
        pool.wait(timeout=30)
        application.state.pool = pool
        application.state.series_cache = ExperimentSeriesCache(
            int(os.getenv("API_CACHE_EXPERIMENTS", "1024"))
        )
        yield


class CachedResponsesMiddleware:
    """Serve validated, immutable cache hits without routing/thread handoffs.

    Every miss, unsupported method and unusual/invalid parameter delegates to
    FastAPI. Its regular routes retain the original validation/error behavior.
    Request metrics use the ASGI send interface rather than a streaming bridge.
    """

    number = re.compile(r"[-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][-+]?[0-9]+)?\Z")
    paths = {"/temperature", "/temperature/out-of-range"}

    def __init__(self, app):
        self.app = app

    def cached_body(self, scope) -> bytes | None:
        path = scope["path"]
        if scope["method"] != "GET" or path not in self.paths:
            return None
        params = QueryParams(scope.get("query_string", b""))
        required = ("experiment-id", "start-time", "end-time") if path == "/temperature" else ("experiment-id",)
        if any(len(params.getlist(name)) != 1 for name in required):
            return None
        if path == "/temperature":
            a, b = params["start-time"], params["end-time"]
            if not self.number.fullmatch(a) or not self.number.fullmatch(b):
                return None
            start, end = float(a), float(b)
            if not math.isfinite(start) or not math.isfinite(end) or start > end:
                return None
        series = scope["app"].state.series_cache.get(params["experiment-id"])
        if series is None:
            return None
        CACHE_LOOKUPS.labels("hit").inc()
        return series.temperature_json(start, end) if path == "/temperature" else series.encoded_out_of_range

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        started = time.monotonic()

        async def observe_send(message):
            if message["type"] == "http.response.start":
                REQUESTS.labels(scope["path"], str(message["status"])).inc()
                REQUEST_DURATION.labels(scope["path"]).observe(time.monotonic() - started)
            await send(message)

        body = self.cached_body(scope)
        if body is not None:
            return await Response(body, media_type="application/json")(scope, receive, observe_send)
        await self.app(scope, receive, observe_send)


app = FastAPI(title="Group 17 Temperature Observability", lifespan=lifespan)
app.add_middleware(CachedResponsesMiddleware)


def load_experiment_series(
    request: Request, experiment_id: str
) -> ExperimentSeries:
    cached = request.app.state.series_cache.get(experiment_id)
    if cached is not None:
        return cached

    with request.app.state.pool.connection() as connection:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                "SELECT terminated FROM experiments WHERE experiment_id = %s",
                (experiment_id,),
            )
            experiment = cursor.fetchone()
            cursor.execute(
                """
                SELECT measured_at AS timestamp, temperature, out_of_range
                FROM measurements
                WHERE experiment_id = %s AND during_experiment
                ORDER BY measured_at, measurement_id
                """,
                (experiment_id,),
            )
            rows = list(cursor.fetchall())

    points = tuple(
        {"timestamp": row["timestamp"], "temperature": row["temperature"]}
        for row in rows
    )
    series = ExperimentSeries(
        timestamps=tuple(point["timestamp"] for point in points),
        points=points,
        out_of_range=tuple(
            point for point, row in zip(points, rows) if row["out_of_range"]
        ),
    )
    if experiment is not None and experiment["terminated"]:
        request.app.state.series_cache.put(experiment_id, series)
    return series


async def get_experiment_series(request: Request, experiment_id: str) -> ExperimentSeries:
    # Capture the object once: subsequent eviction must not turn a hit into
    # blocking database I/O on the event loop.
    cached = request.app.state.series_cache.get(experiment_id)
    CACHE_LOOKUPS.labels("hit" if cached is not None else "miss").inc()
    if cached is not None:
        return cached
    return await run_in_threadpool(load_experiment_series, request, experiment_id)


@app.get("/temperature", response_model=list[dict])
async def temperature(
    request: Request,
    experiment_id: str = Query(alias="experiment-id"),
    start_time: float = Query(alias="start-time"),
    end_time: float = Query(alias="end-time"),
) -> Response:
    if start_time > end_time:
        raise HTTPException(422, "start-time must not be after end-time")
    series = await get_experiment_series(request, experiment_id)
    return Response(series.temperature_json(start_time, end_time), media_type="application/json")


@app.get("/temperature/out-of-range", response_model=list[dict])
async def out_of_range(
    request: Request,
    experiment_id: str = Query(alias="experiment-id"),
) -> Response:
    series = await get_experiment_series(request, experiment_id)
    return Response(series.encoded_out_of_range, media_type="application/json")


@app.get("/health")
def health(request: Request) -> dict:
    with request.app.state.pool.connection() as connection:
        connection.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    if os.getenv("PROMETHEUS_MULTIPROC_DIR"):
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        content = generate_latest(registry)
    else:
        content = generate_latest()
    return Response(content, media_type=CONTENT_TYPE_LATEST)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard() -> HTMLResponse:
    return HTMLResponse(Path("/app/dashboard.html").read_text(encoding="utf-8"))


@app.get("/costs", response_class=HTMLResponse)
def costs_dashboard() -> HTMLResponse:
    return HTMLResponse(Path("/app/costs.html").read_text(encoding="utf-8"))


@app.get("/costs/data")
def costs_data(hours: int = Query(default=1, ge=1, le=168)) -> dict:
    try:
        return build_cost_overview(hours)
    except (OSError, ValueError) as error:
        raise HTTPException(503, "Monitoring data is temporarily unavailable") from error
