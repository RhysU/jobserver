"""Acceptance 5.4: sleep_fn Adversarial Behavior.

Acceptance Criteria
-------------------
- Always-vetoing sleep_fn with timeout produces Blocked.
- Countdown sleep_fn eventually allows submission.
- Huge sleep value clamped by timeout (doesn't sleep 1000s).
- sleep_fn that raises propagates the exception.
"""

import time
import unittest

from jobserver import Blocked, Jobserver

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import SleepFnCountdown, return_value, sleep_fn_always_veto


def setUpModule():
    bootstrap_forkserver()


class TestSleepFn(unittest.TestCase):
    """Verify sleep_fn adversarial scenarios."""

    def test_always_veto_raises_blocked(self):
        """5.4.1: sleep_fn always vetoes with timeout=1 -> Blocked."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        start = time.monotonic()
        with self.assertRaises(Blocked):
            js.submit(
                fn=return_value,
                args=(1,),
                sleep_fn=sleep_fn_always_veto,
                timeout=1,
            )
        elapsed = time.monotonic() - start
        self.assertGreaterEqual(elapsed, 0.9)
        self.assertLess(elapsed, 3.0)

    def test_countdown_eventually_accepts(self):
        """5.4.2: sleep_fn vetoes 5 times then accepts."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        countdown = SleepFnCountdown(5)
        f = js.submit(
            fn=return_value,
            args=(42,),
            sleep_fn=countdown,
            timeout=TIMEOUT,
        )
        self.assertEqual(42, f.result(timeout=TIMEOUT))
        self.assertEqual(countdown.remaining, 0)

    def test_huge_sleep_clamped_by_timeout(self):
        """5.4.3: sleep_fn returns 1000; timeout=1 still raises in ~1s."""
        js = Jobserver(context=FAST_METHOD, slots=2)

        def huge_sleep():
            return 1000.0

        start = time.monotonic()
        with self.assertRaises(Blocked):
            js.submit(
                fn=return_value,
                args=(1,),
                sleep_fn=huge_sleep,
                timeout=1,
            )
        elapsed = time.monotonic() - start
        self.assertGreaterEqual(elapsed, 0.9)
        self.assertLess(elapsed, 5.0)

    def test_sleep_fn_raises_propagates(self):
        """5.4.4: sleep_fn raises -> exception propagates from submit()."""
        js = Jobserver(context=FAST_METHOD, slots=2)

        def exploding_sleep():
            raise RuntimeError("sleep exploded")

        with self.assertRaises(RuntimeError) as ctx:
            js.submit(
                fn=return_value,
                args=(1,),
                sleep_fn=exploding_sleep,
                timeout=TIMEOUT,
            )
        self.assertIn("sleep exploded", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
