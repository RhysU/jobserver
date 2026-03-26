"""Acceptance 4.2: Callback Error Draining.

Acceptance Criteria
-------------------
- A raising callback produces CallbackRaised with __cause__ set.
- Mixed OK/RAISE callbacks require multiple done() calls to drain.
- All 5 raising callbacks require 5 done() calls.
- result() may raise CallbackRaised; once drained, returns result.
"""

import unittest

from jobserver import CallbackRaised, Jobserver

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import return_value


def setUpModule():
    bootstrap_forkserver()


class TestCallbackErrors(unittest.TestCase):
    """Verify callback error draining semantics."""

    def test_single_raising_callback(self):
        """4.2.1: Single callback raises -> CallbackRaised with __cause__."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(1,), timeout=TIMEOUT)

        def bad_callback():
            raise RuntimeError("cb fail")

        f.when_done(bad_callback)
        with self.assertRaises(CallbackRaised) as ctx:
            f.done(timeout=TIMEOUT)
        self.assertIsInstance(ctx.exception.__cause__, RuntimeError)
        self.assertIn("cb fail", str(ctx.exception.__cause__))
        # Now done() should succeed (no more callbacks)
        self.assertTrue(f.done(timeout=0))

    def test_mixed_ok_raise_ok(self):
        """4.2.2: OK, RAISE, OK -> two done() calls needed."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(1,), timeout=TIMEOUT)
        results = []

        f.when_done(results.append, "first")
        f.when_done(lambda: (_ for _ in ()).throw(ValueError("mid")))
        f.when_done(results.append, "third")

        # First done() runs "first" OK, then raises on the ValueError
        with self.assertRaises(CallbackRaised):
            f.done(timeout=TIMEOUT)
        self.assertEqual(results, ["first"])

        # Second done() runs "third"
        self.assertTrue(f.done(timeout=0))
        self.assertEqual(results, ["first", "third"])

    def test_five_raising_callbacks(self):
        """4.2.3: All 5 callbacks raise -> 5 done() calls, each raises."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(1,), timeout=TIMEOUT)

        for i in range(5):
            def raiser(idx=i):
                raise ValueError(f"error {idx}")
            f.when_done(raiser)

        caught = []
        for _ in range(10):
            try:
                f.done(timeout=TIMEOUT)
                break  # All drained
            except CallbackRaised as e:
                caught.append(str(e.__cause__))
        self.assertEqual(len(caught), 5)

    def test_result_raises_callback_raised_then_returns(self):
        """4.2.4: result() raises CallbackRaised; once drained, returns."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(42,), timeout=TIMEOUT)

        f.when_done(lambda: (_ for _ in ()).throw(RuntimeError("oops")))

        with self.assertRaises(CallbackRaised):
            f.result(timeout=TIMEOUT)

        # Now callbacks drained, result available
        self.assertEqual(42, f.result(timeout=0))


if __name__ == "__main__":
    unittest.main()
