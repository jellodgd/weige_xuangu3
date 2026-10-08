"""Offline control-flow tests; no QMT API or order calls are made."""
import copy
import datetime as dt
from pathlib import Path
import threading
import unittest

SOURCE = Path(__file__).with_name('DAqmt2_jingjia_filter_20260927_v1.py')


class DailySyncTest(unittest.TestCase):
    def setUp(self):
        self.ns = {}
        exec(compile(SOURCE.read_bytes(), str(SOURCE), 'exec'), self.ns)
        n = self.ns
        n['_guard'] = lambda name, work: work()
        self.logs = []
        n['_log'] = self.logs.append
        n['STATE']['universe'] = {str(i): {} for i in range(100)}
        n['STATE']['trade_date'] = dt.date.today()
        self.calls = []
        self.counts = [0]
        def prepare(ctx):
            n['STATE']['daily'] = copy.deepcopy(n['STATE']['universe'])
        n['_prepare_daily'] = prepare
        def missing(ctx):
            count = self.counts.pop(0) if len(self.counts) > 1 else self.counts[0]
            return list(n['STATE']['universe'])[:count], []
        n['_daily_cache_missing_codes'] = missing
        n['_prepare_preclose'] = lambda ctx: n['STATE'].update(preclose=dict(n['STATE']['daily']))
        def download(ctx, codes, seconds, mode):
            self.calls.append((len(codes), mode))
            return {'attempted': len(codes)}
        n['_download_daily'] = download

    def test_threshold_inclusive_and_idempotent(self):
        self.counts = [20]
        self.ns['_on_download_and_prepare'](None)
        self.ns['_on_download_and_prepare'](None)
        self.assertEqual(self.calls, [])
        self.assertEqual(len(self.ns['STATE']['daily']), 80)
        self.assertTrue(self.ns['STATE']['daily_cache_ready'])

    def test_retry_uses_partial_valid_data(self):
        self.counts = [40, 30]
        self.ns['_on_download_and_prepare'](None)
        self.assertEqual(self.calls, [(100, 'morning_full_retry')])
        self.assertEqual(len(self.ns['STATE']['daily']), 70)
        self.assertTrue(self.ns['STATE']['daily_cache_ready'])

    def test_no_valid_data_no_selection(self):
        self.counts = [100]
        self.ns['_on_download_and_prepare'](None)
        self.assertFalse(self.ns['STATE']['daily_cache_ready'])

    def test_startup_crossing_nine_waits_and_resumes(self):
        n = self.ns
        n['STATE']['trade_date'] = dt.date.today() - dt.timedelta(days=1)
        n['STATE']['startup_sync_pending'] = True
        original = n['_download_daily']
        def download(ctx, codes, seconds, mode):
            n['_on_download_and_prepare'](ctx)
            self.assertEqual(n['STATE']['daily'], {})
            return original(ctx, codes, seconds, mode)
        n['_download_daily'] = download
        n['_on_nightly_daily_sync'](None)
        self.assertEqual(self.calls, [(100, 'startup_full')])
        self.assertTrue(n['STATE']['morning_prepare_done'])
        self.assertFalse(n['STATE']['startup_sync_pending'])

    def test_download_lock_prevents_cache_read(self):
        self.ns['DAILY_SYNC_LOCK'].acquire()
        try:
            self.ns['_on_download_and_prepare'](None)
            self.assertEqual(self.ns['STATE']['daily'], {})
        finally:
            self.ns['DAILY_SYNC_LOCK'].release()

    def test_scheduling_reserves_before_callback(self):
        n = self.ns
        n['STATE']['trade_date'] = dt.date.today() + dt.timedelta(days=1)
        class Context:
            def schedule_run(inner, *args):
                self.assertTrue(n['STATE']['startup_sync_pending'])
        self.assertTrue(n['_schedule_nightly_daily_sync'](Context()))


if __name__ == '__main__':
    unittest.main()
