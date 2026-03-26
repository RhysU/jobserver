"""Acceptance 3.3: Blocked / Timeout Behavior.

Acceptance Criteria
-------------------
- submit(timeout=0) raises Blocked when slots are exhausted.
- submit(timeout=T) raises Blocked after approximately T seconds.
- done(timeout=0) returns False (never raises Blocked).
- result(timeout=0) raises Blocked on incomplete future.
- Blocking calls with timeout=None return when work eventually completes.
"""

import time
import unittest

from jobserver import Blocked, Jobserver

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import return_value, sleep_and_return


def setUpModule():
    bootstrap_forkserver()


class TestBlockedTimeouts(unittest.TestCase):
    """Verify Blocked and timeout semantics."""

    def test_submit_timeout_zero_blocked(self):
        """3.3.1: submit(timeout=0) when all slots busy raises Blocked."""
        js = Jobserver(context=FAST_METHOD, slots=1)
        f = js.submit(fn=sleep_and_return, args=(5, "a"), timeout=TIMEOUT)
        with self.assertRaises(Blocked):
            js.submit(
                fn=return_value,
                args=(1,),
                callbacks=False,
                timeout=0,
            )
        # Clean up: let the running future complete
        # (we don't wait for it here to keep the test fast)

    def test_submit_timeout_short_blocked(self):
        """3.3.2: submit(timeout=0.5) raises Blocked after ~0.5s."""
        js = Jobserver(context=FAST_METHOD, slots=1)
        f = js.submit(fn=sleep_and_return, args=(10, "a"), timeout=TIMEOUT)
        start = time.monotonic()
        with self.assertRaises(Blocked):
            js.submit(
                fn=return_value,
                args=(1,),
                callbacks=False,
                timeout=0.5,
            )
        elapsed = time.monotonic() - start
        self.assertGreaterEqual(elapsed, 0.4)
        self.assertLess(elapsed, 2.0)

    def test_result_timeout_zero_blocked(self):
        """3.3.3: result(timeout=0) on incomplete future raises Blocked."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=sleep_and_return, args=(5, "a"), timeout=TIMEOUT)
        with self.assertRaises(Blocked):
            f.result(timeout=0)

    def test_done_timeout_zero_returns_false(self):
        """3.3.4: done(timeout=0) returns False, never raises Blocked."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=sleep_and_return, args=(5, "a"), timeout=TIMEOUT)
        result = f.done(timeout=0)
        self.assertFalse(result)

    def test_done_none_blocks_until_complete(self):
        """3.3.5: done(timeout=None) returns True after work finishes."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(77,), timeout=TIMEOUT)
        self.assertTrue(f.done(timeout=None))

    def test_result_none_blocks_until_complete(self):
        """3.3.6: result(timeout=None) blocks until completion."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(88,), timeout=TIMEOUT)
        self.assertEqual(88, f.result(timeout=None))


if __name__ == "__main__":
    unittest.main()
