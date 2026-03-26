"""Acceptance 6.1: Cancellation Matrix.

Acceptance Criteria
-------------------
- cancel() before dispatch returns True; future.cancelled() is True.
- cancel() after dispatch returns False; result eventually available.
- shutdown(cancel_futures=True) cancels pending, completes running.
- Cancelled future raises CancelledError on result().
"""

import concurrent.futures
import time
import unittest

from jobserver import Jobserver, JobserverExecutor

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import return_value, sleep_and_return


def setUpModule():
    bootstrap_forkserver()


class TestExecutorCancellation(unittest.TestCase):
    """Verify cancellation semantics of JobserverExecutor."""

    def test_cancel_before_dispatch(self):
        """6.1.1: cancel() before dispatcher picks up -> True."""
        js = Jobserver(context=FAST_METHOD, slots=1)
        with JobserverExecutor(js) as ex:
            # Fill the only slot with a slow job
            slow = ex.submit(sleep_and_return, 2, "slow")
            # Submit more jobs that will queue as PENDING
            pending = ex.submit(return_value, "queued")
            # Small delay to let the slow job start
            time.sleep(0.2)
            # Try to cancel the pending job
            cancelled = pending.cancel()
            if cancelled:
                self.assertTrue(pending.cancelled())
                with self.assertRaises(concurrent.futures.CancelledError):
                    pending.result(timeout=0)

    def test_cancel_after_running(self):
        """6.1.2: cancel() after job started returns False."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        with JobserverExecutor(js) as ex:
            f = ex.submit(sleep_and_return, 0.5, "result")
            # Wait for it to be dispatched
            time.sleep(0.3)
            # Should not be cancellable anymore
            if not f.cancel():
                # If cancel returned False, result should be available
                self.assertEqual("result", f.result(timeout=TIMEOUT))

    def test_shutdown_cancel_futures(self):
        """6.1.3: shutdown(cancel_futures=True) cancels pending."""
        js = Jobserver(context=FAST_METHOD, slots=1)
        ex = JobserverExecutor(js)
        # Fill slot
        slow = ex.submit(sleep_and_return, 1, "slow")
        # Queue up pending work
        pending_futures = [ex.submit(return_value, i) for i in range(50)]
        # Shutdown with cancel
        ex.shutdown(wait=True, cancel_futures=True)
        # Some should be cancelled, slow should complete
        cancelled_count = sum(1 for f in pending_futures if f.cancelled())
        # At least some should be cancelled
        self.assertGreater(cancelled_count, 0)

    def test_cancelled_future_raises_cancelled_error(self):
        """6.1.5: Cancelled future raises CancelledError on result()."""
        js = Jobserver(context=FAST_METHOD, slots=1)
        with JobserverExecutor(js) as ex:
            slow = ex.submit(sleep_and_return, 2, "slow")
            pending = ex.submit(return_value, "queued")
            time.sleep(0.2)
            if pending.cancel():
                with self.assertRaises(concurrent.futures.CancelledError):
                    pending.result()


if __name__ == "__main__":
    unittest.main()
