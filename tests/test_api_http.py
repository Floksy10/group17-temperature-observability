"""HTTP contract checks using the preserved real experiment histories."""

import gzip
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
from urllib.parse import urlencode

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root / "services" / "api" if (root / "services" / "api").exists() else Path("/app")))
import app as api


async def request(path, params=(), method="GET"):
    messages = []
    scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
             "scheme": "http", "method": method, "path": path, "raw_path": path.encode(),
             "root_path": "", "query_string": urlencode(params).encode(), "headers": [],
             "server": ("contract-test", 80), "client": ("127.0.0.1", 1)}
    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}
    async def send(message):
        messages.append(message)
    await api.app(scope, receive, send)
    start = next(m for m in messages if m["type"] == "http.response.start")
    body = b"".join(m.get("body", b"") for m in messages if m["type"] == "http.response.body")
    return start["status"], dict(start["headers"]), json.loads(body)


class HistoricAPIContractTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        path = Path(os.getenv("API_TEST_FIXTURES", root / "docs/latency-evidence/2026-10-09/fixtures.json.gz"))
        with gzip.open(path, "rt") as stream:
            self.fixture = json.load(stream)[0]
        self.points = [{"timestamp": p["timestamp"], "temperature": p["temperature"]} for p in self.fixture["points"]]
        self.outside = [p for p, original in zip(self.points, self.fixture["points"]) if original["outside"]]
        self.series = api.ExperimentSeries(tuple(p["timestamp"] for p in self.points), tuple(self.points), tuple(self.outside))
        api.app.state.series_cache = api.ExperimentSeriesCache(512)
        api.app.state.series_cache.put(self.fixture["id"], self.series)
        self.id = self.fixture["id"]
        self.start = self.points[0]["timestamp"]
        self.end = self.points[-1]["timestamp"]

    def params(self, start=None, end=None):
        return [("experiment-id", self.id), ("start-time", self.start if start is None else start),
                ("end-time", self.end if end is None else end)]

    async def test_cached_range_preserves_exact_values_order_and_boundaries(self):
        a, b = self.points[10]["timestamp"], self.points[30]["timestamp"]
        status, headers, body = await request("/temperature", self.params(a, b))
        self.assertEqual(status, 200)
        self.assertEqual(body, [p for p in self.points if a <= p["timestamp"] <= b])
        self.assertEqual(headers[b"content-type"], b"application/json")
        self.assertEqual(body[0]["timestamp"], a)
        self.assertEqual(body[-1]["timestamp"], b)

    async def test_single_instant_and_empty_window(self):
        instant = self.points[10]["timestamp"]
        self.assertEqual((await request("/temperature", self.params(instant, instant)))[2], [p for p in self.points if p["timestamp"] == instant])
        self.assertEqual((await request("/temperature", self.params(self.end + 1, self.end + 2)))[2], [])

    async def test_cached_out_of_range_matches_real_flags(self):
        self.assertEqual((await request("/temperature/out-of-range", [("experiment-id", self.id)]))[2], self.outside)

    async def test_missing_invalid_and_reversed_parameters_keep_422(self):
        cases = [[], [("experiment-id", self.id)], self.params("not-a-time"), self.params(self.end, self.start)]
        for params in cases:
            with self.subTest(params=params):
                self.assertEqual((await request("/temperature", params))[0], 422)
        self.assertEqual((await request("/temperature/out-of-range"))[0], 422)
        response = await request("/temperature", self.params(self.end, self.start))
        self.assertEqual(response[2], {"detail": "start-time must not be after end-time"})

    async def test_duplicate_and_unusual_numbers_delegate_to_regular_routes(self):
        params = self.params() + [("start-time", self.end)]
        self.assertEqual((await request("/temperature", params))[2], [p for p in self.points if p["timestamp"] == self.end])
        self.assertEqual((await request("/temperature", self.params("-inf", "inf")))[2], self.points)
        self.assertEqual((await request("/temperature", self.params(" " + str(self.start), str(self.end) + " ")))[2], self.points)

    async def test_other_methods_still_return_405(self):
        self.assertEqual((await request("/temperature", self.params(), method="POST"))[0], 405)

    async def test_unknown_experiment_keeps_empty_success(self):
        pool = MagicMock()
        cursor = pool.connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = None
        cursor.fetchall.return_value = []
        api.app.state.pool = pool
        params = [("experiment-id", "missing-" + self.id), ("start-time", self.start), ("end-time", self.end)]
        self.assertEqual((await request("/temperature", params))[2], [])
        self.assertIsNone(api.app.state.series_cache.get("missing-" + self.id))

    async def test_active_history_is_not_cached_but_completed_history_is(self):
        api.app.state.series_cache = api.ExperimentSeriesCache(512)
        pool = MagicMock()
        cursor = pool.connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = {"terminated": False}
        cursor.fetchall.return_value = [{**p, "out_of_range": original["outside"]} for p, original in zip(self.points, self.fixture["points"])]
        api.app.state.pool = pool
        for _ in range(2):
            self.assertEqual((await request("/temperature", self.params()))[2], self.points)
            self.assertIsNone(api.app.state.series_cache.get(self.id))
        cursor.fetchone.return_value = {"terminated": True}
        await request("/temperature", self.params())
        count = cursor.execute.call_count
        await request("/temperature", self.params())
        self.assertEqual(cursor.execute.call_count, count)
        self.assertIsNotNone(api.app.state.series_cache.get(self.id))

    async def test_fast_and_fallback_responses_are_counted_once(self):
        before = api.REQUESTS.labels("/temperature", "200")._value.get()
        await request("/temperature", self.params())
        await request("/temperature", self.params() + [("start-time", self.end)])
        self.assertEqual(api.REQUESTS.labels("/temperature", "200")._value.get() - before, 2)


if __name__ == "__main__":
    unittest.main()
