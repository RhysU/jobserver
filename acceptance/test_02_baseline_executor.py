"""Acceptance 1.2: Baseline Functional Correctness -- JobserverExecutor.

Acceptance Criteria
-------------------
- executor.submit() returns concurrent.futures.Future resolving correctly.
- executor.map() yields results in submission order.
- Context manager calls shutdown on exit.
- as_completed() / wait() from concurrent.futures work without modification.
"""

import concurrent.futures
import unittest

from jobserver import Jobserver, JobserverExecutor

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import add_kw, identity, return_value


def setUpModule():
    bootstrap_forkserver()


class TestBaselineExecutor(unittest.TestCase):
    """Verify happy-path round-trips for JobserverExecutor."""

    def test_submit_returns_cf_future(self):
        """1.2.1: submit() returns concurrent.futures.Future with correct value."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        ex = JobserverExecutor(js)
        try:
            f = ex.submit(return_value, 99)
            self.assertIsInstance(f, concurrent.futures.Future)
            self.assertEqual(99, f.result(timeout=TIMEOUT))
        finally:
            ex.shutdown(wait=True)

    def test_map_preserves_order(self):
        """1.2.2: map() yields results in submission order."""
        js = Jobserver(context=FAST_METHOD, slots=4)
        ex = JobserverExecutor(js)
        try:
            values = list(range(20))
            results = list(ex.map(identity, values, timeout=TIMEOUT))
            self.assertEqual(values, results)
        finally:
            ex.shutdown(wait=True)

    def test_context_manager(self):
        """1.2.3: Context manager calls shutdown on exit."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        with JobserverExecutor(js) as ex:
            f = ex.submit(return_value, "hello")
            self.assertEqual("hello", f.result(timeout=TIMEOUT))
        # After context exit, submit should raise
        with self.assertRaises(RuntimeError):
            ex.submit(return_value, "fail")

    def test_as_completed_interop(self):
        """1.2.4a: as_completed() from concurrent.futures works."""
        js = Jobserver(context=FAST_METHOD, slots=4)
        with JobserverExecutor(js) as ex:
            futures = [ex.submit(return_value, i) for i in range(10)]
            results = set()
            for f in concurrent.futures.as_completed(futures, timeout=TIMEOUT):
                results.add(f.result())
            self.assertEqual(set(range(10)), results)

    def test_wait_interop(self):
        """1.2.4b: wait() from concurrent.futures works."""
        js = Jobserver(context=FAST_METHOD, slots=4)
        with JobserverExecutor(js) as ex:
            futures = [ex.submit(return_value, i) for i in range(10)]
            done, not_done = concurrent.futures.wait(
                futures, timeout=TIMEOUT
            )
            self.assertEqual(len(done), 10)
            self.assertEqual(len(not_done), 0)

    def test_submit_with_kwargs(self):
        """1.2 bonus: submit() forwards keyword arguments."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        with JobserverExecutor(js) as ex:
            f = ex.submit(add_kw, 1, 2, c=10)
            self.assertEqual(13, f.result(timeout=TIMEOUT))


if __name__ == "__main__":
    unittest.main()
