# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 9 shows timeout handling for submit(), done(), wait(), result()."""

import time
from logging import INFO, basicConfig, captureWarnings, info

from jobserver import Blocked, Jobserver


def main() -> None:
    """Shows the timeout argument on submit(), done(), wait(), result()."""
    # Timeouts are in seconds: None blocks (default for wait()/result()), 0
    # polls, and a positive value waits at most that long.  On expiry submit()
    # and result() raise Blocked while done() and wait() return False.
    with Jobserver(context="spawn", slots=1) as jobserver:
        # Submit a slow task that occupies the only slot
        future = jobserver.submit(fn=task_slow, args=(0.5,))

        # timeout=0 polls without blocking; done() is exactly wait(timeout=0)
        info("done() before: %s", future.done())

        # done(timeout=0.1) waits up to 0.1s, returning False if not ready
        info("done(timeout=0.1): %s", future.done(timeout=0.1))

        # A positive timeout waits at most that long.  wait() then returns
        # False rather than raising when the result is not yet ready.
        info("wait(timeout=0.1) not ready: %s", future.wait(timeout=0.1))

        # result() with the same finite timeout instead raises Blocked
        try:
            future.result(timeout=0.1)
            raise RuntimeError("Expected Blocked was not raised")
        except Blocked:
            info("Caught expected Blocked from result(timeout=0.1)")

        # submit(timeout=0) is non-blocking and raises Blocked at once when no
        # slot is available; timeout=None would instead block until one frees
        try:
            jobserver.submit(fn=len, args=("rejected",), timeout=0)
            raise RuntimeError("Expected Blocked was not raised")
        except Blocked:
            info("Caught expected Blocked: no slots for new work")

        # timeout=None (the default) blocks indefinitely until the result is in
        info("wait() after: %s", future.wait())
        # done() on a completed future also returns True (non-blocking)
        info("done() after: %s", future.done())
        info("Slow task result: %s", future.result())


def task_slow(seconds: float) -> str:
    """Sleep then return a confirmation."""
    time.sleep(seconds)
    return f"done after {seconds:.2f}s"


if __name__ == "__main__":
    basicConfig(
        level=INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    captureWarnings(True)
    main()
