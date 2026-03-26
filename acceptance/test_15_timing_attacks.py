"""Acceptance 5.2: Timing Attacks.

Acceptance Criteria
-------------------
- Tight-loop done(timeout=0.001) eventually returns True.
- Near-zero timeout on submit either succeeds or raises Blocked cleanly.
- Near-zero timeout on result either returns or raises Blocked cleanly.
- Rapid submit/done alternation maintains consistent state.
"""

import time
import unittest

from jobserver import Blocked, Jobserver

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import return_value, sleep_and_return


def setUpModule():
    bootstrap_forkserver()


class TestTimingAttacks(unittest.TestCase):
    """Verify correctness under adversarial timing."""

    def test_tight_loop_done(self):
        """5.2.1: done(timeout=0.001) in tight loop eventually True."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(77,), timeout=TIMEOUT)
        for _ in range(10000):
            if f.done(timeout=0.001):
                break
        self.assertTrue(f.done(timeout=0))

    def test_near_zero_submit(self):
        """5.2.2: submit(timeout=0.001) either succeeds or raises Blocked."""
        js = Jobserver(context=FAST_METHOD, slots=1)
        f = js.submit(fn=sleep_and_return, args=(0.5, 1), timeout=TIMEOUT)
        # Slot is busy; near-zero timeout should cleanly raise Blocked
        try:
            g = js.submit(
                fn=return_value,
                args=(2,),
                callbacks=False,
                timeout=0.001,
            )
            # If we got here, it succeeded (slot freed just in time)
            g.result(timeout=TIMEOUT)
        except Blocked:
            pass  # Expected
        f.result(timeout=TIMEOUT)

    def test_near_zero_result(self):
        """5.2.3: result(timeout=1e-9) either returns or raises Blocked."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(88,), timeout=TIMEOUT)
        try:
            val = f.result(timeout=1e-9)
            self.assertEqual(88, val)
        except Blocked:
            pass  # Expected if result not ready yet
        # Regardless, result should be available eventually
        self.assertEqual(88, f.result(timeout=TIMEOUT))

    def test_rapid_submit_done_alternation(self):
        """5.2.4: Rapid submit/done(0) alternation is consistent."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        for i in range(50):
            f = js.submit(fn=return_value, args=(i,), timeout=TIMEOUT)
            f.done(timeout=0)  # May or may not be done yet
            self.assertEqual(i, f.result(timeout=TIMEOUT))


if __name__ == "__main__":
    unittest.main()
