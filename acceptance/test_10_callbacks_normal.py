"""Acceptance 4.1: Normal Callback Operation.

Acceptance Criteria
-------------------
- Single callback fires exactly once after done().
- Callback on already-complete future fires immediately.
- 100 callbacks all fire in registration order.
- Callback receives forwarded args and kwargs.
"""

import unittest

from jobserver import Jobserver

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import return_value


def setUpModule():
    bootstrap_forkserver()


class TestCallbacksNormal(unittest.TestCase):
    """Verify normal callback behavior."""

    def test_single_callback(self):
        """4.1.1: One callback fires exactly once after done()."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(1,), timeout=TIMEOUT)
        results = []
        f.when_done(lambda: results.append("fired"))
        f.done(timeout=TIMEOUT)
        self.assertEqual(results, ["fired"])
        # Call done() again; callback should NOT fire again
        f.done(timeout=0)
        self.assertEqual(results, ["fired"])

    def test_callback_on_completed_future(self):
        """4.1.2: Callback on already-complete future fires immediately."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(1,), timeout=TIMEOUT)
        f.done(timeout=TIMEOUT)
        # Future is now complete
        results = []
        f.when_done(lambda: results.append("immediate"))
        self.assertEqual(results, ["immediate"])

    def test_100_callbacks_in_order(self):
        """4.1.3: 100 callbacks fire in registration order."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(1,), timeout=TIMEOUT)
        results = []
        for i in range(100):
            f.when_done(results.append, i)
        f.done(timeout=TIMEOUT)
        self.assertEqual(results, list(range(100)))

    def test_callback_receives_args_kwargs(self):
        """4.1.4: Callback receives forwarded args and kwargs."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(1,), timeout=TIMEOUT)
        results = []

        def collector(*args, **kwargs):
            results.append((args, kwargs))

        f.when_done(collector, "a", "b", key="val")
        f.done(timeout=TIMEOUT)
        self.assertEqual(results, [(("a", "b"), {"key": "val"})])


if __name__ == "__main__":
    unittest.main()
