import sys
import unittest
from pathlib import Path
from unittest.mock import patch


source_root = Path(__file__).parents[1] / "services" / "api"
if not source_root.exists():
    source_root = Path("/app")
sys.path.insert(0, str(source_root))
import app as api  # noqa: E402


def series(*points: tuple[float, float, bool]) -> api.ExperimentSeries:
    values = tuple(
        {"timestamp": timestamp, "temperature": temperature}
        for timestamp, temperature, _ in points
    )
    return api.ExperimentSeries(
        timestamps=tuple(point["timestamp"] for point in values),
        points=values,
        out_of_range=tuple(
            point
            for point, (_, _, outside) in zip(values, points)
            if outside
        ),
    )


class ExperimentSeriesCacheTest(unittest.TestCase):
    def test_evicts_least_recently_used_experiment(self) -> None:
        cache = api.ExperimentSeriesCache(max_size=2)
        first = series((1.0, 20.0, False))
        second = series((2.0, 21.0, False))
        third = series((3.0, 22.0, False))

        cache.put("first", first)
        cache.put("second", second)
        self.assertIs(cache.get("first"), first)
        cache.put("third", third)

        self.assertIsNone(cache.get("second"))
        self.assertIs(cache.get("first"), first)
        self.assertIs(cache.get("third"), third)

    def test_temperature_uses_inclusive_timestamp_range(self) -> None:
        values = series(
            (1.0, 20.0, False),
            (2.0, 21.0, True),
            (3.0, 22.0, False),
        )
        with patch.object(api, "load_experiment_series", return_value=values):
            result = api.temperature(object(), "experiment", 2.0, 3.0)

        self.assertEqual(
            result,
            [
                {"timestamp": 2.0, "temperature": 21.0},
                {"timestamp": 3.0, "temperature": 22.0},
            ],
        )

    def test_out_of_range_returns_only_flagged_points(self) -> None:
        values = series(
            (1.0, 20.0, False),
            (2.0, 21.0, True),
            (3.0, 22.0, False),
        )
        with patch.object(api, "load_experiment_series", return_value=values):
            result = api.out_of_range(object(), "experiment")

        self.assertEqual(result, [{"timestamp": 2.0, "temperature": 21.0}])


if __name__ == "__main__":
    unittest.main()
