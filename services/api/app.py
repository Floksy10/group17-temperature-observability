"""Historic temperature REST API required by the course assignment."""

import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row
from starlette.responses import HTMLResponse, Response


REQUESTS = Counter(
    "group17_http_requests_total", "HTTP requests", ["path", "status"]
)
REQUEST_DURATION = Histogram(
    "group17_http_request_seconds", "HTTP request duration", ["path"]
)


@asynccontextmanager
async def lifespan(application: FastAPI):
    with ConnectionPool(
        os.environ["DATABASE_URL"], min_size=1, max_size=15
    ) as pool:
        pool.wait(timeout=30)
        application.state.pool = pool
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


def query_points(request: Request, query: str, parameters: tuple) -> list[dict]:
    with request.app.state.pool.connection() as connection:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, parameters)
            return list(cursor.fetchall())


@app.get("/temperature")
def temperature(
    request: Request,
    experiment_id: str = Query(alias="experiment-id"),
    start_time: float = Query(alias="start-time"),
    end_time: float = Query(alias="end-time"),
) -> list[dict]:
    if start_time > end_time:
        raise HTTPException(422, "start-time must not be after end-time")
    return query_points(
        request,
        """
        SELECT measured_at AS timestamp, temperature
        FROM measurements
        WHERE experiment_id = %s AND during_experiment
          AND measured_at >= %s AND measured_at <= %s
        ORDER BY measured_at, measurement_id
        """,
        (experiment_id, start_time, end_time),
    )


@app.get("/temperature/out-of-range")
def out_of_range(
    request: Request,
    experiment_id: str = Query(alias="experiment-id"),
) -> list[dict]:
    return query_points(
        request,
        """
        SELECT measured_at AS timestamp, temperature
        FROM measurements
        WHERE experiment_id = %s AND during_experiment AND out_of_range
        ORDER BY measured_at, measurement_id
        """,
        (experiment_id,),
    )


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
