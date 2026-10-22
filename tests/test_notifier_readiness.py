"""Do not retire old notifiers until replacements have connected to PostgreSQL."""
import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

class NotifierReadinessTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        token = Path(self.temp.name)/'token'
        token.write_text('test-only')
        path=Path(__file__).parents[1]/'services/notifier/notifier.py'
        spec=importlib.util.spec_from_file_location('notifier_tested',path)
        self.module=importlib.util.module_from_spec(spec)
        with patch.dict(os.environ, DATABASE_URL='postgresql://unused', NOTIFICATIONS_TOKEN_FILE=str(token)):
            spec.loader.exec_module(self.module)
        self.module.READINESS_FILE=Path(self.temp.name)/'ready'

    def test_ready_only_after_connection_and_removed_on_shutdown(self):
        m=self.module
        def send(connection, session):
            self.assertTrue(m.READINESS_FILE.exists())
            m.RUNNING=False
            return True
        with patch.object(m.psycopg, 'connect') as connect, patch.object(m, 'send_next', side_effect=send):
            m.main()
            connect.assert_called_once()
        self.assertFalse(m.READINESS_FILE.exists())

    def test_failed_database_connection_never_marks_ready(self):
        m=self.module
        def stop_sleep(seconds):
            self.assertFalse(m.READINESS_FILE.exists())
            m.RUNNING=False
        with patch.object(m.psycopg,'connect',side_effect=m.psycopg.OperationalError('unavailable')), patch.object(m.time,'sleep',side_effect=stop_sleep), self.assertLogs(m.LOGGER,level='ERROR'):
            m.main()
        self.assertFalse(m.READINESS_FILE.exists())
