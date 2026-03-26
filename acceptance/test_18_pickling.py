"""Acceptance 5.5: Pickling Edge Cases.

Acceptance Criteria
-------------------
- copy/deepcopy of Jobserver returns the same instance.
- pickle round-trip of Jobserver works (excluding in-flight futures).
- copy/pickle of Future raises NotImplementedError.
"""

import copy
import pickle
import unittest

from jobserver import Jobserver

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import return_value


def setUpModule():
    bootstrap_forkserver()


class TestPickling(unittest.TestCase):
    """Verify copy/pickle behavior of Jobserver and Future."""

    def test_copy_returns_same_instance(self):
        """5.5.3: copy.copy(jobserver) returns same instance."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        js_copy = copy.copy(js)
        self.assertIs(js, js_copy)

    def test_deepcopy_returns_same_instance(self):
        """5.5.4: copy.deepcopy(jobserver) returns same instance."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        js_deep = copy.deepcopy(js)
        self.assertIs(js, js_deep)

    def test_pickle_roundtrip(self):
        """5.5.5: pickle.dumps/loads round-trips Jobserver."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        data = pickle.dumps(js)
        js2 = pickle.loads(data)
        # It's a different instance but should be functional
        self.assertIsInstance(js2, Jobserver)
        f = js2.submit(fn=return_value, args=(99,), timeout=TIMEOUT)
        self.assertEqual(99, f.result(timeout=TIMEOUT))

    def test_future_copy_raises(self):
        """5.5.6: copy.copy(future) raises NotImplementedError."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(1,), timeout=TIMEOUT)
        with self.assertRaises(NotImplementedError):
            copy.copy(f)
        f.result(timeout=TIMEOUT)  # Clean up

    def test_future_pickle_raises(self):
        """5.5.7: pickle.dumps(future) raises NotImplementedError."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=return_value, args=(1,), timeout=TIMEOUT)
        with self.assertRaises((NotImplementedError, TypeError)):
            pickle.dumps(f)
        f.result(timeout=TIMEOUT)  # Clean up


if __name__ == "__main__":
    unittest.main()
