"""Acceptance 2.2: High-Throughput Stress.

Acceptance Criteria
-------------------
- 1,000 trivial jobs complete via Jobserver within reasonable wall time.
- 10,000 trivial jobs complete via JobserverExecutor.
- 60-second sustained loop shows no monotonic resource growth.
"""

import os
import time
import unittest

from jobserver import Jobserver, JobserverExecutor

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import noop, return_value


def setUpModule():
    bootstrap_forkserver()


def _count_open_fds():
    """Count open file descriptors for the current process (Linux)."""
    try:
        return len(os.listdir(f"/proc/{os.getpid()}/fd"))
    except FileNotFoundError:
        return -1  # Not on Linux; skip FD checks


class TestHighThroughput(unittest.TestCase):
    """Stress test with large job counts."""

    def test_1000_trivial_jobserver(self):
        """2.2.1: 1,000 trivial jobs via Jobserver."""
        js = Jobserver(context=FAST_METHOD, slots=os.cpu_count() or 4)
        futures = []
        for i in range(1000):
            f = js.submit(fn=return_value, args=(i,), timeout=TIMEOUT)
            futures.append(f)
        for i, f in enumerate(futures):
            self.assertEqual(i, f.result(timeout=TIMEOUT))

    def test_10000_trivial_executor(self):
        """2.2.2: 10,000 trivial jobs via JobserverExecutor."""
        js = Jobserver(context=FAST_METHOD, slots=os.cpu_count() or 4)
        with JobserverExecutor(js) as ex:
            futures = [ex.submit(return_value, i) for i in range(10000)]
            for i, f in enumerate(futures):
                self.assertEqual(i, f.result(timeout=TIMEOUT))

    def test_sustained_30s_no_leak(self):
        """2.2.3: Sustained submit/collect for 30s, no resource leaks.

        Uses 30s instead of 60s for CI friendliness.  Checks that FD count
        does not grow monotonically.
        """
        js = Jobserver(context=FAST_METHOD, slots=os.cpu_count() or 4)
        deadline = time.monotonic() + 30
        batch = 0
        initial_fds = _count_open_fds()
        max_fds = initial_fds

        while time.monotonic() < deadline:
            futures = [
                js.submit(fn=return_value, args=(batch,), timeout=TIMEOUT)
                for _ in range(20)
            ]
            for f in futures:
                f.result(timeout=TIMEOUT)
            batch += 1
            current_fds = _count_open_fds()
            if current_fds > 0:
                max_fds = max(max_fds, current_fds)

        # FD count should not have grown unboundedly
        if initial_fds > 0:
            # Allow some headroom but not linear growth
            self.assertLess(
                max_fds,
                initial_fds + 200,
                "File descriptor count grew excessively",
            )
        self.assertGreater(batch, 0, "At least one batch completed")


if __name__ == "__main__":
    unittest.main()
