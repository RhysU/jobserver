"""Acceptance 3.2: Process Death.

Acceptance Criteria
-------------------
- All forms of abrupt process termination produce SubmissionDied.
- done(timeout=T) returns False for an incomplete future (not killed).
"""

import os
import signal
import unittest

from jobserver import Jobserver, SubmissionDied

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import (
    exit_abruptly,
    infinite_loop,
    self_sigkill,
    self_sigsegv,
    self_sigterm,
)


def setUpModule():
    bootstrap_forkserver()


class TestProcessDeath(unittest.TestCase):
    """Verify that abrupt child process death is detected cleanly."""

    def test_os_exit(self):
        """3.2.1: fn calls os._exit(1)."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=exit_abruptly, args=(1,), timeout=TIMEOUT)
        with self.assertRaises(SubmissionDied):
            f.result(timeout=TIMEOUT)

    def test_sigkill(self):
        """3.2.2: fn sends SIGKILL to self."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=self_sigkill, timeout=TIMEOUT)
        with self.assertRaises(SubmissionDied):
            f.result(timeout=TIMEOUT)

    def test_sigterm(self):
        """3.2.3: fn sends SIGTERM to self."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=self_sigterm, timeout=TIMEOUT)
        with self.assertRaises(SubmissionDied):
            f.result(timeout=TIMEOUT)

    def test_sigsegv(self):
        """3.2.4: fn sends SIGSEGV to self."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=self_sigsegv, timeout=TIMEOUT)
        with self.assertRaises(SubmissionDied):
            f.result(timeout=TIMEOUT)

    def test_infinite_loop_timeout(self):
        """3.2.5: fn loops forever, done(timeout=2) returns False."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(fn=infinite_loop, timeout=TIMEOUT)
        self.assertFalse(f.done(timeout=2))
        # The library does not kill children -- it reports they aren't done.
        # Kill the child ourselves to prevent a process leak from the test.
        # Uses _process (internal) only for cleanup, not to test behavior.
        os.kill(f._process.pid, signal.SIGKILL)
        with self.assertRaises(SubmissionDied):
            f.result(timeout=TIMEOUT)


if __name__ == "__main__":
    unittest.main()
