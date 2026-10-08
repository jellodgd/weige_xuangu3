"""Calendar regressions with no live QMT calls."""
import datetime as dt
from pathlib import Path
import unittest


class CalendarSafetyTest(unittest.TestCase):
    def setUp(self):
        self.n = {}
        p = Path(__file__).with_name('DAqmt2_jingjia_filter_20261008_v2.py')
        exec(compile(p.read_bytes(), str(p), 'exec'), self.n)
        self.logs = []
        self.n['_log'] = self.logs.append

    def test_full_cache_after_long_closure(self):
        days = [dt.date(2026, 9, 30), dt.date(2026, 10, 8), dt.date(2026, 10, 9)]
        class C:
            def get_trading_dates(self, code, start, end, *args):
                return [d.strftime('%Y%m%d') for d in days
                        if start <= d.strftime('%Y%m%d') <= end]
        self.n['STATE'].update(trade_date=dt.date(2026, 10, 12), universe={'A': {}},
                               daily={'A': dict(zip(['rank_base_date', 'prior_t2_date', 'prior_t1_date'], days))})
        missing, expected = self.n['_daily_cache_missing_codes'](C())
        self.assertEqual(missing, [])
        self.assertEqual(expected, days)

    def test_empty_future_calendar_blocks_startup(self):
        scheduled = []
        class C:
            def get_trading_dates(self, *args): return []
            def schedule_run(self, *args): scheduled.append(args[-1])
        self.n['_guard'] = lambda label, work: work()
        self.n['_load_universe'] = lambda c: None
        self.n['_write_status'] = lambda *args: None
        self.n['after_init'](C())
        self.assertIsNone(self.n['STATE']['trade_date'])
        self.assertEqual(scheduled, ['account_preflight_retry'])
        self.assertTrue(any(r['event'] == 'trading_calendar_unavailable' for r in self.logs))

    def test_exception_does_not_fallback(self):
        class C:
            def get_trading_dates(self, *args): raise RuntimeError('offline')
        self.assertIsNone(self.n['_next_trade_date'](C()))
        self.assertTrue(any(r['event'] == 'trading_dates_error' for r in self.logs))

    def test_empty_historical_calendar_is_not_cache_missing(self):
        class C:
            def get_trading_dates(self, *args): return []
        self.n['STATE']['trade_date'] = dt.date(2026, 10, 12)
        with self.assertRaises(RuntimeError):
            self.n['_daily_cache_missing_codes'](C())

    def test_future_calendar_honors_actual_date(self):
        class C:
            def get_trading_dates(self, code, start, end, *args):
                day = dt.datetime.strptime(start, '%Y%m%d').date() + dt.timedelta(days=12)
                return [day.strftime('%Y%m%d')]
        now = dt.datetime.now()
        start = now.date() if now.time() < self.n['FINAL_SAMPLE_UNTIL'] else now.date() + dt.timedelta(days=1)
        self.assertEqual(self.n['_next_trade_date'](C()), start + dt.timedelta(days=12))


if __name__ == '__main__':
    unittest.main()
