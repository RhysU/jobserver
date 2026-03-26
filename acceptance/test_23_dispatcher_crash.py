"""Acceptance 6.4: Dispatcher Crash Recovery.

Acceptance Criteria
-------------------
- Killing the dispatcher makes outstanding futures fail with RuntimeError.
- Executor rejects new submissions after dispatcher dies.
"""

import os
import signal
import time
import unittest

from jobserver import Jobserver, JobserverExecutor

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import sleep_and_return


def setUpModule():
    bootstrap_forkserver()


class TestDispatcherCrash(unittest.TestCase):
    """Verify behavior when the dispatcher process dies unexpectedly."""

    def test_kill_dispatcher(self):
        """6.4.1: Kill dispatcher; outstanding futures get RuntimeError."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        ex = JobserverExecutor(js)
        # Submit work that sleeps long enough for us to kill the dispatcher.
        # Use a short sleep so the orphaned worker exits promptly.
        f = ex.submit(sleep_and_return, 2, "never")
        time.sleep(0.5)  # Let dispatcher start the work
        # Kill the dispatcher
        dispatcher_pid = ex._dispatcher.pid
        os.kill(dispatcher_pid, signal.SIGKILL)
        # Outstanding future should fail
        with self.assertRaises(Exception):
            f.result(timeout=TIMEOUT)
        # New submissions should fail
        with self.assertRaises(RuntimeError):
            ex.submit(sleep_and_return, 0, "fail")
        # Cleanup: shutdown the executor and wait for the orphaned worker
        # to finish its short sleep so no processes leak.
        ex.shutdown(wait=True)


if __name__ == "__main__":
    unittest.main()
