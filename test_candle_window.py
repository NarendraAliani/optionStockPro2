from datetime import datetime
from types import SimpleNamespace
import unittest

from app.services.angel_api import AngelOneAPI
from app.services.scanner_engine import ScannerEngine


class CandleWindowRuleTests(unittest.TestCase):
    def setUp(self):
        self.api = AngelOneAPI(api_key='', client_id='', password='')
        self.engine = ScannerEngine(
            user_id=0,
            config=SimpleNamespace(id=1, timeframe='15min', refresh_interval=900, price_multiplier=1.2, strike_range=0),
            angel_api=self.api,
            workers=1
        )

    def _assert_window(self, stamp, interval, expected_previous, expected_current):
        now = datetime.strptime(stamp, '%Y-%m-%d %H:%M:%S')
        current_label, previous_label = self.api._live_candle_window(now, interval)
        self.assertEqual(previous_label.strftime('%H:%M'), expected_previous)
        self.assertEqual(current_label.strftime('%H:%M'), expected_current)

    def test_15m_examples_follow_closed_candle_rule(self):
        self._assert_window('2026-03-13 13:40:00', 15, '13:15', '13:30')
        self._assert_window('2026-03-13 13:46:00', 15, '13:30', '13:45')

    def test_other_timeframes_follow_same_rule(self):
        self._assert_window('2026-03-13 13:07:00', 5, '13:00', '13:05')
        self._assert_window('2026-03-13 13:07:00', 3, '13:03', '13:06')
        self._assert_window('2026-03-13 13:59:00', 30, '13:00', '13:30')
        self._assert_window('2026-03-13 13:59:00', 60, '12:00', '13:00')


if __name__ == '__main__':
    unittest.main()
