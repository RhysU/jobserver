"""Acceptance 3.4: JobserverExecutor Exception Propagation.

Acceptance Criteria
-------------------
- Worker exceptions propagate through concurrent.futures.Future.
- Submit after shutdown raises RuntimeError.
- Unpicklable fn produces an exception (not a hang).
"""

import unittest

from jobserver import Jobserver, JobserverExecutor

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import raise_value_error, return_value


def setUpModule():
    bootstrap_forkserver()


class TestExecutorExceptions(unittest.TestCase):
    """Verify exception propagation through JobserverExecutor."""

    def test_worker_exception(self):
        """3.4.1: fn raises ValueError, future.result() re-raises."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        with JobserverExecutor(js) as ex:
            f = ex.submit(raise_value_error, "executor boom")
            with self.assertRaises(ValueError) as ctx:
                f.result(timeout=TIMEOUT)
            self.assertIn("executor boom", str(ctx.exception))

    def test_submit_after_shutdown(self):
        """3.4.2: Submit after shutdown() raises RuntimeError."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        ex = JobserverExecutor(js)
        ex.shutdown(wait=True)
        with self.assertRaises(RuntimeError):
            ex.submit(return_value, 1)

    def test_unpicklable_fn_in_spawn(self):
        """3.4.3: Unpicklable fn fails with an exception, not a hang.

        This test uses 'spawn' context where lambdas are not picklable.
        The pickling error surfaces at submit() (during serialization of
        the request into the dispatcher queue), not at result().
        """
        js = Jobserver(context="spawn", slots=2)
        with JobserverExecutor(js) as ex:
            with self.assertRaises(Exception):
                ex.submit(lambda: 42)


if __name__ == "__main__":
    unittest.main()
