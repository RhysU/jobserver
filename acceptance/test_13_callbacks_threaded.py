"""Acceptance 4.4: Callback + Thread Safety.

Acceptance Criteria
-------------------
- No deadlock when done() and when_done() race from different threads.
- 10 threads calling done() on same future: all see True, no crash.
- 10 threads registering callbacks while future completes: all fire once.
"""

import threading
import unittest

from jobserver import CallbackRaised, Jobserver

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import return_value, sleep_and_return


def setUpModule():
    bootstrap_forkserver()


class TestCallbacksThreaded(unittest.TestCase):
    """Verify thread safety of Future.done() and Future.when_done()."""

    def test_done_and_when_done_race(self):
        """4.4.1: T1 calls done(), T2 calls when_done() concurrently."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=sleep_and_return, args=(0.1, 99), timeout=TIMEOUT)
        results = []
        errors = []

        def thread_done():
            try:
                while True:
                    try:
                        if f.done(timeout=0.01):
                            break
                    except CallbackRaised:
                        pass
            except Exception as e:
                errors.append(e)

        def thread_when_done():
            try:
                f.when_done(lambda: results.append("cb"))
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=thread_done)
        t2 = threading.Thread(target=thread_when_done)
        t1.start()
        t2.start()
        t1.join(timeout=TIMEOUT)
        t2.join(timeout=TIMEOUT)

        self.assertEqual(errors, [])
        self.assertEqual(results, ["cb"])

    def test_10_threads_done_on_same_future(self):
        """4.4.2: 10 threads all call done(); all see True, no crash."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(42,), timeout=TIMEOUT)
        results = []
        errors = []
        barrier = threading.Barrier(10)

        def thread_fn():
            try:
                barrier.wait(timeout=TIMEOUT)
                while True:
                    try:
                        if f.done(timeout=1):
                            results.append(True)
                            break
                    except CallbackRaised:
                        pass
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=thread_fn) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=TIMEOUT)

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 10)
        self.assertTrue(all(results))

    def test_10_threads_register_callbacks(self):
        """4.4.3: 10 threads register callbacks while future completes."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=sleep_and_return, args=(0.1, 1), timeout=TIMEOUT)
        results = []
        lock = threading.Lock()
        errors = []

        def thread_fn(idx):
            try:
                def cb(i=idx):
                    with lock:
                        results.append(i)
                f.when_done(cb)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=thread_fn, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=TIMEOUT)

        # Drain any pending callbacks
        while True:
            try:
                f.done(timeout=TIMEOUT)
                break
            except CallbackRaised:
                pass

        self.assertEqual(errors, [])
        self.assertEqual(sorted(results), list(range(10)))


if __name__ == "__main__":
    unittest.main()
