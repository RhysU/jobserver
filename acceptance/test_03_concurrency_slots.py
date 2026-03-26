"""Acceptance 2.1: Slot Saturation.

Acceptance Criteria
-------------------
- At most N functions execute simultaneously when slots=N.
- All submitted work completes without deadlock.
- Serialized execution (slots=1) handles 100 items.
"""

import time
import unittest

from jobserver import Jobserver

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import record_timestamps, return_value, sleep_and_return


def setUpModule():
    bootstrap_forkserver()


class TestSlotSaturation(unittest.TestCase):
    """Verify that concurrency is bounded by the slot count."""

    def _max_concurrent(self, timestamps):
        """Given list of (start, end), compute max overlapping intervals."""
        events = []
        for start, end in timestamps:
            events.append((start, +1))
            events.append((end, -1))
        events.sort()
        max_conc = 0
        current = 0
        for _, delta in events:
            current += delta
            max_conc = max(max_conc, current)
        return max_conc

    def test_exactly_fill_slots(self):
        """2.1.1: Submit exactly N jobs into N slots."""
        N = 3
        js = Jobserver(context=FAST_METHOD, slots=N)
        futures = [
            js.submit(fn=record_timestamps, args=(0.05,), timeout=TIMEOUT)
            for _ in range(N)
        ]
        timestamps = [f.result(timeout=TIMEOUT) for f in futures]
        self.assertEqual(len(timestamps), N)
        self.assertLessEqual(self._max_concurrent(timestamps), N)

    def test_ten_x_slots(self):
        """2.1.2: Submit 10x more jobs than slots."""
        N = 3
        count = N * 10
        js = Jobserver(context=FAST_METHOD, slots=N)
        futures = [
            js.submit(fn=record_timestamps, args=(0.02,), timeout=TIMEOUT)
            for _ in range(count)
        ]
        timestamps = [f.result(timeout=TIMEOUT) for f in futures]
        self.assertEqual(len(timestamps), count)
        self.assertLessEqual(self._max_concurrent(timestamps), N)

    def test_single_slot_serial(self):
        """2.1.3: slots=1, 100 items processed serially."""
        js = Jobserver(context=FAST_METHOD, slots=1)
        futures = []
        for i in range(100):
            f = js.submit(fn=return_value, args=(i,), timeout=TIMEOUT)
            futures.append(f)
        for i, f in enumerate(futures):
            self.assertEqual(i, f.result(timeout=TIMEOUT))

    def test_single_slot_rapid_fire(self):
        """2.1.4: slots=1, rapid-fire submit with timeout=None."""
        js = Jobserver(context=FAST_METHOD, slots=1)
        futures = []
        for i in range(50):
            f = js.submit(fn=return_value, args=(i,), timeout=None)
            futures.append(f)
        for i, f in enumerate(futures):
            self.assertEqual(i, f.result(timeout=TIMEOUT))


if __name__ == "__main__":
    unittest.main()
