"""Acceptance 1.1: Baseline Functional Correctness -- Jobserver Round-Trip.

Acceptance Criteria
-------------------
- All 7 scenarios pass on every supported start method.
- No exceptions other than those explicitly expected.
- result() returns the exact expected value and type.
"""

import unittest

from multiprocessing import get_all_start_methods

from jobserver import Jobserver

from .conftest import TIMEOUT, bootstrap_forkserver
from .helpers import (
    add_kw,
    identity,
    length,
    noop,
    return_exception,
    return_none,
    return_value,
)


def setUpModule():
    bootstrap_forkserver()


class TestBaselineJobserver(unittest.TestCase):
    """Verify happy-path round-trips for the Jobserver."""

    def test_submit_and_result(self):
        """1.1.1: Submit a simple function and collect result."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                f = js.submit(fn=return_value, args=(42,), timeout=TIMEOUT)
                self.assertEqual(42, f.result(timeout=TIMEOUT))

    def test_call_shorthand(self):
        """1.1.2: Submit via __call__ shorthand."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                f = js(length, (1, 2, 3))
                self.assertEqual(3, f.result(timeout=TIMEOUT))

    def test_args_and_kwargs(self):
        """1.1.3: Submit with args and kwargs, verify all received."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                f = js.submit(
                    fn=add_kw, args=(1, 2), kwargs={"c": 10}, timeout=TIMEOUT
                )
                self.assertEqual(13, f.result(timeout=TIMEOUT))

    def test_consume_zero(self):
        """1.1.4: Submit with consume=0, no slot consumed."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
                # Fill the only slot
                f1 = js.submit(fn=return_value, args=(1,), timeout=TIMEOUT)
                # consume=0 should NOT block even though slot is taken
                f2 = js.submit(
                    fn=return_value,
                    args=(2,),
                    consume=0,
                    timeout=0,
                )
                self.assertEqual(2, f2.result(timeout=TIMEOUT))
                self.assertEqual(1, f1.result(timeout=TIMEOUT))

    def test_return_none(self):
        """1.1.5: Return None explicitly, not SubmissionDied."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                f = js.submit(fn=return_none, timeout=TIMEOUT)
                self.assertIsNone(f.result(timeout=TIMEOUT))

    def test_return_exception_object(self):
        """1.1.6: Return an Exception object (not raised)."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                f = js.submit(fn=return_exception, timeout=TIMEOUT)
                result = f.result(timeout=TIMEOUT)
                self.assertIsInstance(result, ValueError)

    def test_context_property(self):
        """1.1.7: Verify context property returns a BaseContext."""
        from multiprocessing.context import BaseContext

        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                self.assertIsInstance(js.context, BaseContext)


if __name__ == "__main__":
    unittest.main()
