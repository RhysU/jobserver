"""Acceptance 3.1: Work Exceptions.

Acceptance Criteria
-------------------
- result() re-raises the exact exception type from the child.
- BaseExceptions (SystemExit, KeyboardInterrupt) produce SubmissionDied.
- preexec_fn exceptions propagate correctly.
"""

import unittest

from multiprocessing import get_all_start_methods

from jobserver import Jobserver, SubmissionDied

from .conftest import TIMEOUT, bootstrap_forkserver
from .helpers import (
    CustomTestError,
    preexec_raise,
    raise_custom_error,
    raise_keyboard_interrupt,
    raise_runtime_error,
    raise_system_exit,
    raise_type_error,
    raise_value_error,
    return_value,
)


def setUpModule():
    bootstrap_forkserver()


class TestWorkExceptions(unittest.TestCase):
    """Verify exception propagation from child to parent."""

    def test_value_error(self):
        """3.1.1: fn raises ValueError."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                f = js.submit(fn=raise_value_error, timeout=TIMEOUT)
                with self.assertRaises(ValueError) as ctx:
                    f.result(timeout=TIMEOUT)
                self.assertIn("boom", str(ctx.exception))

    def test_custom_exception(self):
        """3.1.2: fn raises a custom Exception subclass."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                f = js.submit(fn=raise_custom_error, timeout=TIMEOUT)
                with self.assertRaises(CustomTestError):
                    f.result(timeout=TIMEOUT)

    def test_system_exit_becomes_submission_died(self):
        """3.1.3: SystemExit (BaseException) -> SubmissionDied."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                f = js.submit(fn=raise_system_exit, timeout=TIMEOUT)
                with self.assertRaises(SubmissionDied):
                    f.result(timeout=TIMEOUT)

    def test_keyboard_interrupt_becomes_submission_died(self):
        """3.1.4: KeyboardInterrupt (BaseException) -> SubmissionDied."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                f = js.submit(fn=raise_keyboard_interrupt, timeout=TIMEOUT)
                with self.assertRaises(SubmissionDied):
                    f.result(timeout=TIMEOUT)

    def test_preexec_fn_exception(self):
        """3.1.5: Exception in preexec_fn propagates."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                f = js.submit(
                    fn=return_value,
                    args=(1,),
                    preexec_fn=preexec_raise,
                    timeout=TIMEOUT,
                )
                with self.assertRaises(RuntimeError) as ctx:
                    f.result(timeout=TIMEOUT)
                self.assertIn("preexec failed", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
