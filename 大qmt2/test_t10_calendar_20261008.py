"""Offline regression: exercise the real calendar helper and daily preparation."""
import datetime as dt
from pathlib import Path
import unittest


class CalendarTest(unittest.TestCase):
    def test_full_history_and_strict_filter(self):
        path = Path(__file__).with_name('DAqmt2_jingjia_filter_20261008_v1.py')
        n = {}
        exec(compile(path.read_bytes(), str(path), 'exec'), n)
        trade = dt.date(2026, 10, 8)
        # Deterministic fixture, not a request to a live QMT terminal.
        dates = [dt.date(2026, 9, day) for day in
                 (8, 9, 10, 11, 14, 15, 16, 17, 18, 21, 22, 23, 24, 28, 29, 30)]
        calls = []
        class Table:
            def __init__(self, missing=False):
                self.missing = missing
            def iterrows(self):
                return iter((str(day), {'low': 10, 'close': 10})
                            for day in dates if not (self.missing and day == dates[-10]))
        class Context:
            def get_trading_dates(self, code, start, end, count, period):
                calls.append((start, end))
                return [day.strftime('%Y%m%d') for day in dates
                        if start <= day.strftime('%Y%m%d') <= end]
            def get_market_data_ex(self, *args, **kwargs):
                return {'A': Table(), 'B': Table(True)}
        context = Context()
        n['_log'] = lambda record: None
        n['STATE'].update(trade_date=trade,
                          universe={'A': {'price_tick': .01}, 'B': {'price_tick': .01}})
        n['_prepare_daily'](context)
        self.assertEqual(calls[0], ('20260908', '20261007'))
        self.assertEqual(n['STATE']['daily']['A']['t10_date'], dates[-10])
        self.assertEqual(n['STATE']['daily']['A']['t10_close'], 10)
        self.assertEqual(n['_t1_vs_t10_failure'](n['STATE']['daily']['B']), 'missing_t10_daily')
        n['_trade_dates'](context, dt.date(2026, 9, 8))
        self.assertEqual(calls[-1], ('20260908', '20260918'))
        check = n['_t1_vs_t10_failure']
        self.assertIsNone(check({'t1_close': 12.99, 't10_close': 10}))
        self.assertEqual(check({'t1_close': 13, 't10_close': 10}), 't1_vs_t10_rise_at_or_above_max')
        n['ENABLE_T1_VS_T10_RISE_FILTER'] = False
        self.assertIsNone(check({}))


if __name__ == '__main__':
    unittest.main()
