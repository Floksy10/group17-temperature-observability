"""Historic temperature REST API required by the course assignment."""

import os
import time
from bisect import bisect_left, bisect_right
from collections import OrderedDict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Lock

from fastapi import FastAPI, HTTPException, Query, Request
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row
from starlette.responses import HTMLResponse, Response

from costs import build_cost_overview


REQUESTS = Counter(
    "group17_http_requests_total", "HTTP requests", ["path", "status"]
)
REQUEST_DURATION = Histogram(
    "group17_http_request_seconds", "HTTP request duration", ["path"]
)


@dataclass(frozen=True)
class ExperimentSeries:
    timestamps: tuple[float, ...]
    points: tuple[dict, ...]
    out_of_range: tuple[dict, ...]


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
            int(os.getenv("API_CACHE_EXPERIMENTS", "512"))
        )
        yield


app = FastAPI(title="Group 17 Temperature Observability", lifespan=lifespan)


@app.middleware("http")
async def record_request(request: Request, call_next):
    started = time.monotonic()
    path = request.url.path
    response = await call_next(request)
    REQUESTS.labels(path, str(response.status_code)).inc()
    REQUEST_DURATION.labels(path).observe(time.monotonic() - started)
    return response


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


@app.get("/temperature")
def temperature(
    request: Request,
    experiment_id: str = Query(alias="experiment-id"),
    start_time: float = Query(alias="start-time"),
    end_time: float = Query(alias="end-time"),
) -> list[dict]:
    if start_time > end_time:
        raise HTTPException(422, "start-time must not be after end-time")
    series = load_experiment_series(request, experiment_id)
    start = bisect_left(series.timestamps, start_time)
    end = bisect_right(series.timestamps, end_time)
    return list(series.points[start:end])


@app.get("/temperature/out-of-range")
def out_of_range(
    request: Request,
    experiment_id: str = Query(alias="experiment-id"),
) -> list[dict]:
    return list(load_experiment_series(request, experiment_id).out_of_range)


@app.get("/health")
def health(request: Request) -> dict:
    with request.app.state.pool.connection() as connection:
        connection.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


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
