"""Acceptance 7: Multiprocessing Context Compatibility.

Acceptance Criteria
-------------------
- Core submit/result round-trip works under fork, forkserver, and spawn.
- Module-level functions work under spawn.
- Closures fail under spawn with a pickling error (expected).
- Nested jobserver shared via pickle works under all contexts.
- Explicit context construction works correctly.
"""

import unittest

from multiprocessing import get_all_start_methods

from jobserver import Jobserver

from .conftest import TIMEOUT, bootstrap_forkserver
from .helpers import identity, length, nested_submit, return_value


def setUpModule():
    bootstrap_forkserver()


class TestContextCompat(unittest.TestCase):
    """Verify all tests work across multiprocessing start methods."""

    def test_round_trip_all_contexts(self):
        """7.1.1: Submit module-level function under each context."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                f = js.submit(fn=return_value, args=(42,), timeout=TIMEOUT)
                self.assertEqual(42, f.result(timeout=TIMEOUT))

    def test_closure_fails_under_spawn(self):
        """7.1.2: Closure fails under spawn with pickling error."""
        if "spawn" not in get_all_start_methods():
            self.skipTest("spawn not available")

        js = Jobserver(context="spawn", slots=2)
        local_var = 99

        # This closure captures local_var and is not picklable
        def closure():
            return local_var

        # Under spawn, this should fail at submission or result time
        with self.assertRaises(Exception):
            f = js.submit(fn=closure, timeout=TIMEOUT)
            f.result(timeout=TIMEOUT)

    def test_staticmethod_all_contexts(self):
        """7.1.3: Submit a staticmethod (module-level) under all contexts."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                f = js.submit(fn=length, args=([1, 2, 3],), timeout=TIMEOUT)
                self.assertEqual(3, f.result(timeout=TIMEOUT))

    def test_nested_jobserver_all_contexts(self):
        """7.1.4: Nested jobserver shared via pickle under all contexts."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                f = js.submit(
                    fn=nested_submit,
                    args=(js, return_value, (42,)),
                    timeout=TIMEOUT,
                )
                self.assertEqual(42, f.result(timeout=TIMEOUT))

    def test_explicit_context_construction(self):
        """7.1.5: Explicit Jobserver(context='spawn', slots=2)."""
        if "spawn" not in get_all_start_methods():
            self.skipTest("spawn not available")
        js = Jobserver(context="spawn", slots=2)
        f = js.submit(fn=identity, args=("hello",), timeout=TIMEOUT)
        self.assertEqual("hello", f.result(timeout=TIMEOUT))


if __name__ == "__main__":
    unittest.main()
