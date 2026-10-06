import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "services" / "api"))
from costs import estimate_cost_series


class CostSeriesTest(unittest.TestCase):
    def test_charges_observed_uptime_but_not_downtime_or_monitoring_gaps(self):
        series = estimate_cost_series(
            {0: 1, 60: 1, 120: 0, 180: 1, 240: 1, 1000: 1},
            hourly_rate=0.04,
            step=60,
        )
        self.assertEqual(series[60], round(60 * 0.04 / 3600, 6))
        self.assertEqual(series[180], series[60])
        self.assertEqual(series[1000], round(120 * 0.04 / 3600, 6))


if __name__ == "__main__":
    unittest.main()
