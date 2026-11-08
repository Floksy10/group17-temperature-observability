"""Unusable Kafka messages are counted and acknowledged instead of retried."""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from prometheus_client import REGISTRY

sys.path.insert(0, str(Path(__file__).parents[1] / 'services' / 'consumer'))
import consumer
from storage_pipeline import SkippedEvent


def skipped(reason):
    return REGISTRY.get_sample_value('group17_skipped_events_total', {'reason': reason}) or 0.0


class ConsumerSkipTest(unittest.TestCase):
    def test_message_without_header_or_valid_avro_is_skipped(self):
        before = skipped('undecodable')
        no_header = MagicMock()
        no_header.headers.return_value = []
        bad_payload = MagicMock()
        bad_payload.headers.return_value = [('record_name', b'experiment_started')]
        bad_payload.value.return_value = b'not avro'
        self.assertEqual(consumer.decode_message(no_header), [])
        self.assertEqual(consumer.decode_message(bad_payload), [])
        self.assertEqual(skipped('undecodable'), before + 2)

    def test_skipped_results_are_counted_per_reading(self):
        before = skipped('unknown_experiment')
        record = {'experiment': 'missed', '_record_count': 3}
        consumer.record_results([('sensor_temperature_measured', record,
                                  SkippedEvent('missed', 'unknown_experiment'))])
        self.assertEqual(skipped('unknown_experiment'), before + 3)


if __name__ == '__main__':
    unittest.main()
