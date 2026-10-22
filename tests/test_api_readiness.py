"""Readiness must not consume scarce database connections under load."""
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
sys.path.insert(0, str(Path(__file__).parents[1] / 'services' / 'api'))
import app as api

class ReadinessTest(unittest.IsolatedAsyncioTestCase):
    async def test_ready_does_not_acquire_saturated_pool_connection(self):
        pool = MagicMock(closed=False)
        pool.connection.side_effect = AssertionError('pool is saturated')
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(pool=pool)))
        self.assertEqual(await api.ready(request), {'status': 'ok'})
        pool.connection.assert_not_called()

    async def test_uninitialized_or_closed_pool_returns_503(self):
        for state in [SimpleNamespace(), SimpleNamespace(pool=MagicMock(closed=True))]:
            request = SimpleNamespace(app=SimpleNamespace(state=state))
            with self.assertRaises(api.HTTPException) as failure:
                await api.ready(request)
            self.assertEqual(failure.exception.status_code, 503)
