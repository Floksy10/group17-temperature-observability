"""Instrument an isolated copy of the API; never import this in production.

Run the existing API image with this file mounted at /app/profile_api_entry.py
and use ``uvicorn profile_api_entry:app``. Every response reports stage timing
in Server-Timing. PROFILE_VARIANT selects opt-in diagnostic prototypes; the
baseline preserves the original application and database queries. Internal
hooks target FastAPI 0.115.12, Starlette 0.46.2 and Uvicorn 0.34.0.
"""

import contextvars
from dataclasses import dataclass, field
import functools
import inspect
import json
import math
import os
import re
import time

import fastapi.routing
import fastapi._compat
from starlette.responses import JSONResponse, Response
from starlette.routing import request_response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.datastructures import QueryParams
from uvicorn.protocols.http.h11_impl import RequestResponseCycle

import app as api

if not hasattr(api, "record_request"):
    raise RuntimeError(
        "This historical profiler requires the original API image "
        "group17/api-profile-base:20261009. Benchmark the optimized API "
        "directly with profile_api_client.py."
    )


CURRENT = contextvars.ContextVar("profile_record", default=None)
original_cycle_init = RequestResponseCycle.__init__


@functools.wraps(original_cycle_init)
def profile_cycle_init(self, *args, **kwargs):
    parsed_at = time.perf_counter()
    original_cycle_init(self, *args, **kwargs)
    self.scope["profile_http_parsed_at"] = parsed_at


# The deployed image uses h11. This measures parsed-request -> ASGI dispatch,
# but cannot measure bytes waiting in the kernel before the read callback runs.
RequestResponseCycle.__init__ = profile_cycle_init


def record_duration(name, started):
    record = CURRENT.get()
    if record is not None:
        record[name] = record.get(name, 0.0) + (time.perf_counter() - started) * 1000


class CursorProxy:
    def __init__(self, cursor):
        self.cursor = cursor
        self.stage = "other_sql"

    def __enter__(self):
        self.cursor.__enter__()
        return self

    def __exit__(self, *args):
        return self.cursor.__exit__(*args)

    def execute(self, query, *args, **kwargs):
        self.stage = "status_sql" if "SELECT terminated" in str(query) else "measurements_sql"
        started = time.perf_counter()
        try:
            self.cursor.execute(query, *args, **kwargs)
            return self
        finally:
            record_duration(self.stage, started)

    def fetchone(self):
        started = time.perf_counter()
        try:
            return self.cursor.fetchone()
        finally:
            record_duration(self.stage, started)

    def fetchall(self):
        started = time.perf_counter()
        try:
            return self.cursor.fetchall()
        finally:
            record_duration(self.stage, started)

    def __getattr__(self, name):
        return getattr(self.cursor, name)


class ConnectionProxy:
    def __init__(self, connection):
        self.connection = connection

    def cursor(self, *args, **kwargs):
        return CursorProxy(self.connection.cursor(*args, **kwargs))

    def __getattr__(self, name):
        return getattr(self.connection, name)


class ConnectionContext:
    def __init__(self, context):
        self.context = context

    def __enter__(self):
        started = time.perf_counter()
        try:
            return ConnectionProxy(self.context.__enter__())
        finally:
            record_duration("pool_wait", started)

    def __exit__(self, *args):
        started = time.perf_counter()
        try:
            return self.context.__exit__(*args)
        finally:
            record_duration("pool_release", started)


OriginalPool = api.ConnectionPool


class ProfilePool(OriginalPool):
    def connection(self, *args, **kwargs):
        return ConnectionContext(super().connection(*args, **kwargs))


api.ConnectionPool = ProfilePool
OriginalCache = api.ExperimentSeriesCache


class ProfileCache(OriginalCache):
    def get(self, experiment_id):
        started = time.perf_counter()
        try:
            value = super().get(experiment_id)
            record = CURRENT.get()
            if record is not None:
                record["cache_hit"] = value is not None
            return value
        finally:
            record_duration("cache_lookup", started)

    def put(self, *args, **kwargs):
        started = time.perf_counter()
        try:
            return super().put(*args, **kwargs)
        finally:
            record_duration("cache_store", started)


api.ExperimentSeriesCache = ProfileCache
original_load = api.load_experiment_series


@functools.wraps(original_load)
def profile_load(*args, **kwargs):
    started = time.perf_counter()
    try:
        value = original_load(*args, **kwargs)
        record = CURRENT.get()
        if record is not None:
            record["history_rows"] = len(value.points)
        return value
    finally:
        record_duration("series_load", started)


api.load_experiment_series = profile_load
original_threadpool = fastapi.routing.run_in_threadpool


async def profile_threadpool(func, *args, **kwargs):
    queued = time.perf_counter()
    record = CURRENT.get()
    if record is not None and "endpoint_end" in record:
        stage = "response_queue"
    elif record is not None and "endpoint_start" in record:
        stage = "db_queue"
    else:
        stage = "endpoint_queue"

    def entered(*inner_args, **inner_kwargs):
        record_duration(stage, queued)
        return func(*inner_args, **inner_kwargs)

    return await original_threadpool(entered, *args, **kwargs)


fastapi.routing.run_in_threadpool = profile_threadpool
original_serialize = fastapi.routing.serialize_response


@functools.wraps(original_serialize)
async def profile_serialize(*args, **kwargs):
    started = time.perf_counter()
    try:
        return await original_serialize(*args, **kwargs)
    finally:
        record_duration("serialization_inclusive", started)


fastapi.routing.serialize_response = profile_serialize
original_validate = fastapi._compat.ModelField.validate


@functools.wraps(original_validate)
def profile_validate(self, *args, **kwargs):
    record = CURRENT.get()
    stage = "response_validate" if record is not None and "endpoint_end" in record else "request_validate"
    started = time.perf_counter()
    try:
        return original_validate(self, *args, **kwargs)
    finally:
        record_duration(stage, started)


fastapi._compat.ModelField.validate = profile_validate
if hasattr(fastapi._compat.ModelField, "serialize"):
    original_model_serialize = fastapi._compat.ModelField.serialize

    @functools.wraps(original_model_serialize)
    def profile_model_serialize(self, *args, **kwargs):
        started = time.perf_counter()
        try:
            return original_model_serialize(self, *args, **kwargs)
        finally:
            record_duration("model_serialize", started)

    fastapi._compat.ModelField.serialize = profile_model_serialize
original_render = JSONResponse.render


def profile_render(self, content):
    started = time.perf_counter()
    try:
        return original_render(self, content)
    finally:
        record_duration("json_render", started)
        record = CURRENT.get()
        if record is not None and "endpoint_start" in record and "endpoint_end" not in record:
            record_duration("json_render_inside_endpoint", started)


JSONResponse.render = profile_render


def wrap_endpoint(endpoint):
    def begin():
        record = CURRENT.get()
        started = time.perf_counter()
        if record is not None:
            record["endpoint_start"] = started
        return record, started

    def finish(record, started):
        record_duration("endpoint_total", started)
        if record is not None:
            record["endpoint_end"] = time.perf_counter()

    @functools.wraps(endpoint)
    def wrapped(*args, **kwargs):
        record, started = begin()
        try:
            result = endpoint(*args, **kwargs)
            if record is not None and isinstance(result, list):
                record["response_rows"] = len(result)
            return result
        finally:
            finish(record, started)

    @functools.wraps(endpoint)
    async def async_wrapped(*args, **kwargs):
        record, started = begin()
        try:
            result = await endpoint(*args, **kwargs)
            if record is not None and isinstance(result, list):
                record["response_rows"] = len(result)
            return result
        finally:
            finish(record, started)

    return async_wrapped if inspect.iscoroutinefunction(endpoint) else wrapped


VARIANT = os.getenv("PROFILE_VARIANT", "baseline")
ALLOWED_VARIANTS = {"baseline", "direct_json", "async_cached", "baseline_asgi", "async_cached_asgi", "encoded_asgi", "encoded_fast_asgi"}
if VARIANT not in ALLOWED_VARIANTS:
    raise ValueError("Unknown profiling variant")


@dataclass(frozen=True)
class EncodedSeries(api.ExperimentSeries):
    encoded_points: tuple[bytes, ...] = field(init=False, repr=False)
    encoded_out_of_range: bytes = field(init=False, repr=False)

    def __post_init__(self):
        def encode(point):
            return json.dumps(point, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
        object.__setattr__(self, "encoded_points", tuple(encode(point) for point in self.points))
        object.__setattr__(self, "encoded_out_of_range", b"[" + b",".join(encode(point) for point in self.out_of_range) + b"]")


if VARIANT in {"encoded_asgi", "encoded_fast_asgi"}:
    api.ExperimentSeries = EncodedSeries


async def get_series_async(request, experiment_id):
    # Capture the cached object once; eviction cannot turn a hit into blocking I/O.
    started = time.perf_counter()
    series = request.app.state.series_cache.get(experiment_id)
    record_duration("series_load", started)
    if series is None:
        return await profile_threadpool(api.load_experiment_series, request, experiment_id)
    record = CURRENT.get()
    if record is not None:
        record["history_rows"] = len(series.points)
    return series


async def temperature_async(request, experiment_id, start_time, end_time):
    if start_time > end_time:
        raise api.HTTPException(422, "start-time must not be after end-time")
    series = await get_series_async(request, experiment_id)
    start = api.bisect_left(series.timestamps, start_time)
    end = api.bisect_right(series.timestamps, end_time)
    return list(series.points[start:end])


async def out_of_range_async(request, experiment_id):
    return list((await get_series_async(request, experiment_id)).out_of_range)


async def temperature_encoded(request, experiment_id, start_time, end_time):
    if start_time > end_time:
        raise api.HTTPException(422, "start-time must not be after end-time")
    series = await get_series_async(request, experiment_id)
    start = api.bisect_left(series.timestamps, start_time)
    end = api.bisect_right(series.timestamps, end_time)
    return Response(b"[" + b",".join(series.encoded_points[start:end]) + b"]", media_type="application/json")


async def out_of_range_encoded(request, experiment_id):
    series = await get_series_async(request, experiment_id)
    return Response(series.encoded_out_of_range, media_type="application/json")


def direct_json_endpoint(endpoint):
    @functools.wraps(endpoint)
    def wrapped(*args, **kwargs):
        return JSONResponse(endpoint(*args, **kwargs))
    return wrapped


for route in api.app.routes:
    if getattr(route, "path", None) in {"/temperature", "/temperature/out-of-range"}:
        endpoint = route.dependant.call
        if VARIANT == "direct_json":
            endpoint = direct_json_endpoint(endpoint)
        elif VARIANT.startswith("async_cached"):
            endpoint = temperature_async if route.path == "/temperature" else out_of_range_async
        elif VARIANT in {"encoded_asgi", "encoded_fast_asgi"}:
            endpoint = temperature_encoded if route.path == "/temperature" else out_of_range_encoded
        route.dependant.call = wrap_endpoint(endpoint)
        route.app = request_response(route.get_route_handler())


class MetricsASGI:
    def __init__(self, app):
        self.application = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.application(scope, receive, send)
        started = time.monotonic()

        async def observe_send(message):
            if message["type"] == "http.response.start":
                api.REQUESTS.labels(scope["path"], str(message["status"])).inc()
                api.REQUEST_DURATION.labels(scope["path"]).observe(time.monotonic() - started)
            await send(message)

        await self.application(scope, receive, observe_send)


class FastCachedASGI:
    """Serve only unambiguous valid cached queries; delegate every other case."""
    number = re.compile(r"[-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][-+]?[0-9]+)?\Z")

    def __init__(self, application):
        self.application = application

    async def __call__(self, scope, receive, send):
        path = scope.get("path")
        if scope["type"] != "http" or scope["method"] != "GET" or path not in {"/temperature", "/temperature/out-of-range"}:
            return await self.application(scope, receive, send)
        started = time.monotonic()
        params = QueryParams(scope.get("query_string", b""))
        experiment_id = params.get("experiment-id")
        if experiment_id is None:
            return await self.application(scope, receive, send)
        if path == "/temperature":
            a, b = params.get("start-time"), params.get("end-time")
            if a is None or b is None or not self.number.fullmatch(a) or not self.number.fullmatch(b):
                return await self.application(scope, receive, send)
            start, end = float(a), float(b)
            if not math.isfinite(start) or not math.isfinite(end) or start > end:
                return await self.application(scope, receive, send)
        series = api.app.state.series_cache.get(experiment_id)
        if series is None:
            return await self.application(scope, receive, send)
        if path == "/temperature":
            left = api.bisect_left(series.timestamps, start)
            right = api.bisect_right(series.timestamps, end)
            body = b"[" + b",".join(series.encoded_points[left:right]) + b"]"
        else:
            body = series.encoded_out_of_range
        api.REQUESTS.labels(path, "200").inc()
        api.REQUEST_DURATION.labels(path).observe(time.monotonic() - started)
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode("ascii"))]})
        await send({"type": "http.response.body", "body": body})


if VARIANT.endswith("_asgi"):
    middleware = api.app.user_middleware
    retained = [m for m in middleware if not (m.cls is BaseHTTPMiddleware and m.kwargs.get("dispatch") is api.record_request)]
    assert len(middleware) - len(retained) == 1
    api.app.user_middleware = retained
    api.app.add_middleware(MetricsASGI)


class ProfileASGI:
    def __init__(self, application):
        self.application = application

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.application(scope, receive, send)
        started = time.perf_counter()
        headers = dict(scope.get("headers", []))
        record = {
            "id": headers.get(b"x-profile-request-id", b"").decode("ascii", errors="replace"),
            "path": scope["path"], "pid": os.getpid(), "start": started,
        }
        if "profile_http_parsed_at" in scope:
            record["protocol_queue"] = (started - scope["profile_http_parsed_at"]) * 1000
        token = CURRENT.set(record)

        async def profiled_send(message):
            if message["type"] == "http.response.start":
                now = time.perf_counter()
                record["server_headers"] = (now - started) * 1000
                record["status"] = message["status"]
                record["framework_before"] = (record.get("endpoint_start", started) - started) * 1000 - record.get("endpoint_queue", 0) - record.get("request_validate", 0)
                record["framework_after"] = (now - record.get("endpoint_end", now)) * 1000 - record.get("serialization_inclusive", 0) - record.get("json_render", 0) + record.get("json_render_inside_endpoint", 0)
                record["serialization_dispatch"] = record.get("serialization_inclusive", 0) - sum(record.get(s, 0) for s in ["response_queue", "response_validate", "model_serialize"])
                load_parts = ["cache_lookup", "cache_store", "pool_wait", "status_sql", "measurements_sql", "pool_release"]
                record["series_assembly"] = record.get("series_load", 0) - sum(record.get(s, 0) for s in load_parts)
                record["endpoint_work"] = record.get("endpoint_total", 0) - record.get("series_load", 0) - record.get("db_queue", 0) - record.get("json_render_inside_endpoint", 0)
                stages = ["framework_before", "request_validate", "endpoint_queue", "db_queue", *load_parts, "series_assembly", "endpoint_work", "response_queue", "response_validate", "model_serialize", "serialization_dispatch", "json_render", "framework_after", "server_headers", "protocol_queue"]
                extra = [
                    (b"server-timing", ", ".join(f"{name};dur={record.get(name, 0):.6f}" for name in stages).encode()),
                    (b"x-profile-process", str(record["pid"]).encode()),
                    (b"x-profile-cache", str(record.get("cache_hit", "unknown")).encode()),
                    (b"x-profile-rows", str(record.get("history_rows", 0)).encode()),
                ]
                message = {**message, "headers": [*message.get("headers", []), *extra]}
            if message["type"] == "http.response.body":
                record["response_bytes"] = record.get("response_bytes", 0) + len(message.get("body", b""))
            await send(message)

        try:
            await self.application(scope, receive, profiled_send)
        finally:
            record["server_return"] = (time.perf_counter() - started) * 1000
            for field in ["start", "endpoint_start", "endpoint_end"]:
                record.pop(field, None)
            path = os.getenv("PROFILE_JSONL")
            if path:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
                try:
                    os.write(fd, (json.dumps(record, separators=(",", ":")) + "\n").encode())
                finally:
                    os.close(fd)
            CURRENT.reset(token)


if os.getenv("PROFILE_TIMING", "full") == "off":
    # Check latency without trace-header size or the per-stage profiling hooks.
    api.ConnectionPool = OriginalPool
    api.ExperimentSeriesCache = OriginalCache
    api.load_experiment_series = original_load
    fastapi.routing.run_in_threadpool = original_threadpool
    fastapi.routing.serialize_response = original_serialize
    fastapi._compat.ModelField.validate = original_validate
    if hasattr(fastapi._compat.ModelField, "serialize"):
        fastapi._compat.ModelField.serialize = original_model_serialize
    JSONResponse.render = original_render
    RequestResponseCycle.__init__ = original_cycle_init
    app = FastCachedASGI(api.app) if VARIANT == "encoded_fast_asgi" else api.app
else:
    if VARIANT == "encoded_fast_asgi":
        raise ValueError("The direct cached path requires PROFILE_TIMING=off; use whole-request timing")
    app = ProfileASGI(api.app)
