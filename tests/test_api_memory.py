import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
from types import SimpleNamespace
sys.path.insert(0, str(Path(__file__).parents[1] / 'services' / 'api'))
import app as api


def series(n=10):
    points = tuple({'timestamp': float(i), 'temperature': 26.} for i in range(n))
    return api.ExperimentSeries(tuple(float(i) for i in range(n)), points, ())


class MemoryTest(unittest.TestCase):
    def test_byte_budget_evicts_even_below_entry_limit(self):
        a = series()
        probe = api.ExperimentSeriesCache(100)
        probe.put('a', a)
        budget = probe.bytes_used
        cache = api.ExperimentSeriesCache(100, budget)
        cache.put('a', a)
        cache.put('b', series())
        self.assertIsNone(cache.get('a'))
        self.assertIsNotNone(cache.get('b'))
        self.assertLessEqual(cache.bytes_used, budget)

    def test_oversized_series_not_cached(self):
        cache = api.ExperimentSeriesCache(100, 100)
        cache.put('large', series(500))
        self.assertIsNone(cache.get('large'))
        self.assertEqual(cache.bytes_used, 0)

    def test_active_range_and_outside_filters_are_pushed_to_database(self):
        pool = MagicMock()
        cursor = pool.connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = {'terminated': False}
        cursor.fetchall.return_value = []
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
            pool=pool, series_cache=api.ExperimentSeriesCache(10))))
        api.load_experiment_series(request, 'e', 10., 11., True)
        sql, params = cursor.execute.call_args.args
        self.assertIn('measured_at >= %s AND measured_at <= %s', sql)
        self.assertIn('AND out_of_range', sql)
        self.assertEqual(params, ['e', 10., 11.])
        self.assertIsNone(request.app.state.series_cache.get('e'))

    def test_oversized_completed_history_falls_back_to_filtered_query(self):
        pool = MagicMock()
        cursor = pool.connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = {'terminated': True}
        cursor.fetchall.side_effect = [[{}] * 3, []]
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
            pool=pool, series_cache=api.ExperimentSeriesCache(10))))
        with patch.dict('os.environ', {'API_CACHE_MAX_POINTS': '2'}):
            api.load_experiment_series(request, 'e', 10., 11.)
        self.assertIn('measured_at >=', cursor.execute.call_args.args[0])
        self.assertIsNone(request.app.state.series_cache.get('e'))
