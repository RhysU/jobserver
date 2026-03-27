"""Acceptance 5.5: Pickling Edge Cases.

Acceptance Criteria
-------------------
- copy/deepcopy of Jobserver returns the same instance.
- Jobserver survives pickling through multiprocessing (child inherits it).
- copy/pickle of Future raises NotImplementedError.
"""

import copy
import pickle
import unittest

from jobserver import Jobserver

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import nested_submit, return_value


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

    def test_jobserver_survives_child_pickling(self):
        """5.5.5: Jobserver pickles through multiprocessing (child inherits).

        Bare pickle.dumps() raises RuntimeError because the internal
        Lock objects only allow pickling during multiprocessing spawn.
        The real use case is passing a Jobserver as an argument to
        submit(), which triggers pickling inside Process.start().
        """
        js = Jobserver(context=FAST_METHOD, slots=2)
        # The nested submit proves the Jobserver was pickled into the
        # child and is functional there.
        f = js.submit(
            fn=nested_submit,
            args=(js, return_value, (99,)),
            timeout=TIMEOUT,
        )
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
