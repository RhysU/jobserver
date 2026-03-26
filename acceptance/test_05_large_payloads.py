"""Acceptance 2.3: Large Payloads.

Acceptance Criteria
-------------------
- 1 MB and 50 MB results transfer correctly.
- 128 MB result transfers correctly (matching existing unit tests).
- 10 MB argument transfers correctly into the child.
"""

import unittest

from jobserver import Jobserver

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import identity, length, make_bytes


def setUpModule():
    bootstrap_forkserver()


class TestLargePayloads(unittest.TestCase):
    """Verify that large data transfers through pipes work or fail cleanly."""

    def test_1mb_result(self):
        """2.3.1: Return a 1 MB bytes object."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        size = 1 * 1024 * 1024
        f = js.submit(fn=make_bytes, args=(size,), timeout=TIMEOUT)
        result = f.result(timeout=TIMEOUT)
        self.assertEqual(len(result), size)
        self.assertEqual(result, b"\x42" * size)

    def test_50mb_result(self):
        """2.3.2: Return a 50 MB bytes object."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        size = 50 * 1024 * 1024
        f = js.submit(fn=make_bytes, args=(size,), timeout=TIMEOUT)
        result = f.result(timeout=TIMEOUT)
        self.assertEqual(len(result), size)

    def test_128mb_result(self):
        """2.3.3: Return 128 MB transfers correctly.

        The existing unit test suite (test_large_objects) already validates
        payloads up to 2**27 = 128 MB via multiprocessing.Connection.send().
        The pipe handles this fine on modern Linux; verify it here too.
        """
        js = Jobserver(context=FAST_METHOD, slots=2)
        size = 128 * 1024 * 1024
        f = js.submit(fn=make_bytes, args=(size,), timeout=TIMEOUT)
        result = f.result(timeout=60)
        self.assertEqual(len(result), size)

    def test_10mb_argument(self):
        """2.3.4: Pass a 10 MB argument into fn."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        size = 10 * 1024 * 1024
        big_arg = b"\xAB" * size
        f = js.submit(fn=length, args=(big_arg,), timeout=TIMEOUT)
        self.assertEqual(size, f.result(timeout=TIMEOUT))


if __name__ == "__main__":
    unittest.main()
