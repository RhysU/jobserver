# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 7 shows non-blocking polling, finite deadlines, and Blocked."""

import time
from logging import INFO, basicConfig, captureWarnings, info

from jobserver import Blocked, Jobserver


def main() -> None:
    """Shows non-blocking polling, finite deadlines, and Blocked."""
    jobserver = Jobserver(context="spawn", slots=1)

    # Submit a slow task that occupies the only slot
    future = jobserver.submit(fn=task_slow, args=(0.5,))

    # done() is a non-blocking poll; False while the task is still running
    info("done() before: %s", future.done())

    # result() with a finite timeout raises Blocked if not ready
    try:
        future.result(timeout=0.1)
        raise RuntimeError("Expected Blocked was not raised")
    except Blocked:
        info("Caught expected Blocked from result(timeout=0.1)")

    # submit() with timeout=0 raises Blocked when no slots available
    try:
        jobserver.submit(fn=len, args=("rejected",), timeout=0)
        raise RuntimeError("Expected Blocked was not raised")
    except Blocked:
        info("Caught expected Blocked: no slots for new work")

    # wait() blocks until the future is ready and returns True
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
