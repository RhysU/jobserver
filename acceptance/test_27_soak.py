"""Acceptance 8.3: Long-Running Soak Test.

Acceptance Criteria
-------------------
- 3-minute sustained submit/collect with bounded resource usage.
- FD count does not grow monotonically.
- Slot queue returns to full capacity after draining all work.
- No OOM or FD exhaustion.

Note: Duration reduced from 10 minutes (plan) to 3 minutes for CI.
Set ACCEPTANCE_SOAK_MINUTES env var to override.
"""

import os
import time
import unittest

from jobserver import Jobserver

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import return_value


def setUpModule():
    bootstrap_forkserver()


def _count_open_fds():
    """Count open file descriptors (Linux only)."""
    try:
        return len(os.listdir(f"/proc/{os.getpid()}/fd"))
    except FileNotFoundError:
        return -1


class TestSoak(unittest.TestCase):
    """Long-running soak test for resource leak detection."""

    def test_sustained_load(self):
        """8.3: Sustained submit/collect loop with resource monitoring."""
        duration_minutes = float(
            os.environ.get("ACCEPTANCE_SOAK_MINUTES", "3")
        )
        duration_seconds = duration_minutes * 60
        slots = os.cpu_count() or 4

        js = Jobserver(context=FAST_METHOD, slots=slots)
        deadline = time.monotonic() + duration_seconds
        batch = 0
        snapshots = []
        batch_size = 20

        while time.monotonic() < deadline:
            # Submit and collect a batch
            futures = [
                js.submit(fn=return_value, args=(batch,), timeout=TIMEOUT)
                for _ in range(batch_size)
            ]
            for f in futures:
                self.assertEqual(batch, f.result(timeout=TIMEOUT))

            # Snapshot resources every 10 batches
            if batch % 10 == 0:
                fds = _count_open_fds()
                snapshots.append({
                    "batch": batch,
                    "time": time.monotonic(),
                    "fds": fds,
                })

            batch += 1

        self.assertGreater(batch, 0, "At least one batch completed")

        # Verify: FD count is bounded (no monotonic growth)
        if snapshots and snapshots[0]["fds"] > 0:
            initial_fds = snapshots[0]["fds"]
            max_fds = max(s["fds"] for s in snapshots)
            # Allow reasonable headroom but not unbounded growth
            self.assertLess(
                max_fds,
                initial_fds + 300,
                f"FD count grew from {initial_fds} to {max_fds} "
                f"over {batch} batches ({len(snapshots)} snapshots)",
            )

        # Verify: slots return to full capacity after all work drained
        # Submit exactly `slots` jobs to confirm all are available
        final_futures = [
            js.submit(
                fn=return_value,
                args=("final",),
                callbacks=False,
                timeout=0,
            )
            for _ in range(slots)
        ]
        for f in final_futures:
            self.assertEqual("final", f.result(timeout=TIMEOUT))


if __name__ == "__main__":
    unittest.main()
