"""Acceptance 6.2: Shutdown Robustness.

Acceptance Criteria
-------------------
- shutdown(wait=True) blocks until all work is done.
- shutdown(wait=False) returns promptly.
- Double/triple shutdown is idempotent; no errors.
- Concurrent shutdowns from multiple threads: no crash, no deadlock.
- shutdown() with no submitted work is clean.
"""

import threading
import time
import unittest

from jobserver import Jobserver, JobserverExecutor

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import return_value, sleep_and_return


def setUpModule():
    bootstrap_forkserver()


class TestExecutorShutdown(unittest.TestCase):
    """Verify shutdown robustness."""

    def test_shutdown_wait_blocks(self):
        """6.2.1: shutdown(wait=True) blocks until all work done."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        ex = JobserverExecutor(js)
        futures = [ex.submit(sleep_and_return, 0.1, i) for i in range(5)]
        ex.shutdown(wait=True)
        # After shutdown returns, all futures should be done
        for f in futures:
            self.assertTrue(f.done())

    def test_shutdown_no_wait_returns_promptly(self):
        """6.2.2: shutdown(wait=False) returns promptly."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        ex = JobserverExecutor(js)
        ex.submit(sleep_and_return, 5, "slow")
        start = time.monotonic()
        ex.shutdown(wait=False)
        elapsed = time.monotonic() - start
        self.assertLess(elapsed, 2.0)
        # Clean up: wait for actual completion
        ex.shutdown(wait=True)

    def test_double_shutdown(self):
        """6.2.3: Double shutdown is idempotent."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        ex = JobserverExecutor(js)
        ex.submit(return_value, 1)
        ex.shutdown(wait=True)
        # Second shutdown should not raise
        ex.shutdown(wait=True)

    def test_triple_shutdown(self):
        """6.2.3b: Triple shutdown is idempotent."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        ex = JobserverExecutor(js)
        ex.shutdown(wait=True)
        ex.shutdown(wait=True)
        ex.shutdown(wait=True)

    def test_concurrent_shutdowns(self):
        """6.2.4: 3 threads shut down simultaneously."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        ex = JobserverExecutor(js)
        ex.submit(return_value, 1)
        errors = []
        barrier = threading.Barrier(3)

        def shutdown_thread():
            try:
                barrier.wait(timeout=TIMEOUT)
                ex.shutdown(wait=True)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=shutdown_thread) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=TIMEOUT)
        self.assertEqual(errors, [])

    def test_shutdown_empty(self):
        """6.2.5: shutdown() with no submitted work is clean."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        ex = JobserverExecutor(js)
        ex.shutdown(wait=True)

    def test_submit_many_then_immediate_shutdown_cancel(self):
        """6.2.6: 1000 jobs, immediate shutdown(cancel_futures=True)."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        ex = JobserverExecutor(js)
        futures = [ex.submit(return_value, i) for i in range(1000)]
        ex.shutdown(wait=True, cancel_futures=True)
        # All futures should be in a terminal state
        for f in futures:
            self.assertTrue(f.done() or f.cancelled())


if __name__ == "__main__":
    unittest.main()
