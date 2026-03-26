"""Acceptance 5.1: Resource Exhaustion Attacks.

Acceptance Criteria
-------------------
- With callbacks=True, eager reclamation prevents deadlock on submit().
- With callbacks=False, manual reclaim_resources() frees slots.
- Slot is not corrupted after Blocked on a saturated jobserver.
"""

import unittest

from jobserver import Blocked, Jobserver

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import return_value


def setUpModule():
    bootstrap_forkserver()


class TestResourceExhaustion(unittest.TestCase):
    """Verify behavior under resource exhaustion."""

    def test_eager_reclamation_prevents_deadlock(self):
        """5.1.1: slots=1, 500 jobs, callbacks=True reclaims eagerly."""
        js = Jobserver(context=FAST_METHOD, slots=1)
        # With callbacks=True (default), each submit() eagerly scans
        # completed futures and reclaims their slots.
        for i in range(500):
            f = js.submit(fn=return_value, args=(i,), timeout=TIMEOUT)
        # If we get here without deadlock, eager reclamation works.
        self.assertEqual(f.result(timeout=TIMEOUT), 499)

    def test_callbacks_false_needs_manual_reclaim(self):
        """5.1.2: slots=1, callbacks=False, manual reclaim."""
        js = Jobserver(context=FAST_METHOD, slots=1)
        f = js.submit(
            fn=return_value, args=(1,), callbacks=False, timeout=TIMEOUT
        )
        # Slot is consumed; with callbacks=False, submit won't reclaim it.
        f.result(timeout=TIMEOUT)
        # Slot not yet returned (callbacks=False means no auto-return).
        # Manual reclaim via done() triggers the internal callback.
        js.reclaim_resources()
        # Now the slot should be free for another submission.
        g = js.submit(
            fn=return_value, args=(2,), callbacks=False, timeout=TIMEOUT
        )
        self.assertEqual(2, g.result(timeout=TIMEOUT))

    def test_blocked_does_not_corrupt_slot(self):
        """5.1.4: Blocked on saturated jobserver doesn't corrupt slots."""
        js = Jobserver(context=FAST_METHOD, slots=1)
        f = js.submit(fn=return_value, args=("a",), timeout=TIMEOUT)
        # All slots taken; submit with timeout=0 should raise Blocked
        with self.assertRaises(Blocked):
            js.submit(
                fn=return_value,
                args=("b",),
                callbacks=False,
                timeout=0,
            )
        # Original job completes fine
        self.assertEqual("a", f.result(timeout=TIMEOUT))
        # Slot freed; new submission works
        g = js.submit(fn=return_value, args=("c",), timeout=TIMEOUT)
        self.assertEqual("c", g.result(timeout=TIMEOUT))


if __name__ == "__main__":
    unittest.main()
