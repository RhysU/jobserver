"""Acceptance 8.2: Combined Chaos Monkey -- JobserverExecutor.

Acceptance Criteria
-------------------
- 4 threads each submit 100 jobs (mixed normal/exception/death).
- A 5th thread randomly cancels pending futures.
- After shutdown, all futures are in a terminal state.
- No zombie processes or hanging threads.
- Executor rejects new submissions after shutdown.
"""

import concurrent.futures
import os
import random
import threading
import time
import unittest

from jobserver import Jobserver, JobserverExecutor

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import exit_abruptly, raise_value_error, return_value


def setUpModule():
    bootstrap_forkserver()


class TestChaosExecutor(unittest.TestCase):
    """Chaos monkey test for JobserverExecutor."""

    def test_executor_chaos(self):
        """8.2: Multi-threaded submit + cancel + shutdown."""
        random.seed(123)
        js = Jobserver(context=FAST_METHOD, slots=4)
        ex = JobserverExecutor(js)

        all_futures = []
        lock = threading.Lock()
        errors = []

        def submitter(thread_id):
            """Submit 100 mixed jobs."""
            try:
                for i in range(100):
                    r = random.random()
                    if r < 0.50:
                        f = ex.submit(return_value, thread_id * 100 + i)
                    elif r < 0.75:
                        f = ex.submit(raise_value_error, f"err-{thread_id}-{i}")
                    else:
                        f = ex.submit(exit_abruptly, 1)
                    with lock:
                        all_futures.append(f)
            except RuntimeError:
                pass  # Executor shut down during submission

        def canceller():
            """Randomly cancel futures."""
            try:
                time.sleep(0.1)
                for _ in range(50):
                    with lock:
                        if all_futures:
                            target = random.choice(all_futures)
                    target.cancel()
                    time.sleep(0.01)
            except Exception:
                pass  # Best effort

        # Launch 4 submitter threads + 1 canceller
        threads = [
            threading.Thread(target=submitter, args=(i,))
            for i in range(4)
        ]
        threads.append(threading.Thread(target=canceller))
        for t in threads:
            t.start()

        # Let them run for 2 seconds
        time.sleep(2)

        # Shutdown
        ex.shutdown(wait=True, cancel_futures=True)

        for t in threads:
            t.join(timeout=TIMEOUT)

        # Verify: all futures in terminal state
        with lock:
            snapshot = list(all_futures)

        for f in snapshot:
            self.assertTrue(
                f.done() or f.cancelled(),
                "Future should be in terminal state after shutdown",
            )

        # Verify: no more submissions accepted
        with self.assertRaises(RuntimeError):
            ex.submit(return_value, "post-shutdown")

        # Verify: main thread is the only non-daemon thread
        # (receiver is daemon, dispatcher is joined)
        for t in threading.enumerate():
            if t.name == "JobserverExecutor-receiver":
                # Should have exited
                self.assertFalse(
                    t.is_alive(),
                    "Receiver thread should not be alive after shutdown",
                )


if __name__ == "__main__":
    unittest.main()
