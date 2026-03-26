"""Acceptance 4.3: Re-Entrant Callbacks.

Acceptance Criteria
-------------------
- Callback A registering callback B from within when_done: both fire.
- Three-level re-entrant chain: A -> B -> C all fire.
- Re-entrant callback that raises produces CallbackRaised.
"""

import unittest

from jobserver import CallbackRaised, Jobserver

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import return_value


def setUpModule():
    bootstrap_forkserver()


class TestCallbacksReentrant(unittest.TestCase):
    """Verify re-entrant (nested) callback registration."""

    def test_callback_registers_callback(self):
        """4.3.1: Callback A registers callback B; both fire."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(1,), timeout=TIMEOUT)
        results = []

        def callback_a():
            results.append("A")
            f.when_done(lambda: results.append("B"))

        f.when_done(callback_a)
        f.done(timeout=TIMEOUT)
        self.assertEqual(results, ["A", "B"])

    def test_three_level_chain(self):
        """4.3.2: A -> B -> C, all three fire."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(1,), timeout=TIMEOUT)
        results = []

        def callback_a():
            results.append("A")
            def callback_b():
                results.append("B")
                f.when_done(lambda: results.append("C"))
            f.when_done(callback_b)

        f.when_done(callback_a)
        f.done(timeout=TIMEOUT)
        self.assertEqual(results, ["A", "B", "C"])

    def test_reentrant_callback_raises(self):
        """4.3.3: Re-entrant callback that raises -> CallbackRaised.

        When callback_a calls when_done(bad_b) on an already-done future,
        bad_b fires immediately inside when_done, raising CallbackRaised.
        That exception propagates out of callback_a, which is itself a
        non-internal callback being run by _issue_callbacks.  So
        _issue_callbacks wraps callback_a's exception (the inner
        CallbackRaised) in an outer CallbackRaised.  The original
        RuntimeError is at __cause__.__cause__.
        """
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(1,), timeout=TIMEOUT)
        results = []

        def callback_a():
            results.append("A")
            def bad_b():
                raise RuntimeError("reentrant boom")
            f.when_done(bad_b)

        f.when_done(callback_a)
        with self.assertRaises(CallbackRaised) as ctx:
            f.done(timeout=TIMEOUT)
        # Outer __cause__ is the inner CallbackRaised from when_done(bad_b)
        inner = ctx.exception.__cause__
        self.assertIsInstance(inner, CallbackRaised)
        # Inner __cause__ is the original RuntimeError
        self.assertIsInstance(inner.__cause__, RuntimeError)
        self.assertIn("reentrant boom", str(inner.__cause__))
        self.assertEqual(results, ["A"])
        # Future should still report done
        self.assertTrue(f.done(timeout=0))


if __name__ == "__main__":
    unittest.main()
