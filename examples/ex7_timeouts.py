# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 7 shows non-blocking polling, finite deadlines, and Blocked."""
import time
from logging import basicConfig, info, DEBUG

from jobserver import Blocked, Jobserver


def main() -> None:
    jobserver = Jobserver(context="spawn", slots=1)

    # Submit a slow task that occupies the only slot
    future = jobserver.submit(fn=task_slow, args=(0.75,))

    # Zero timeout: done() returns False, result() raises Blocked
    info("done(timeout=0): %s", future.done(timeout=0))
    try:
        future.result(timeout=0)
        raise RuntimeError("Expected Blocked was not raised")
    except Blocked:
        info("Caught expected Blocked from result(timeout=0)")

    # Finite timeout on result: waits then gives up
    start = time.monotonic()
    try:
        future.result(timeout=0.25)
        raise RuntimeError("Expected Blocked was not raised")
    except Blocked:
        elapsed = time.monotonic() - start
        info(
            "Caught expected Blocked from result(timeout=0.25)" " after %.2fs",
            elapsed,
        )

    # submit() with timeout=0 raises Blocked when the slot is occupied
    try:
        jobserver.submit(fn=len, args=("rejected",), timeout=0)
        raise RuntimeError("Expected Blocked was not raised")
    except Blocked:
        info("Caught expected Blocked: no slots for new work")

    # submit() with a finite timeout waits then raises Blocked
    start = time.monotonic()
    try:
        jobserver.submit(fn=len, args=("rejected",), timeout=0.25)
        raise RuntimeError("Expected Blocked was not raised")
    except Blocked:
        elapsed = time.monotonic() - start
        info(
            "Caught expected Blocked from submit(timeout=0.25)" " after %.2fs",
            elapsed,
        )

    # After the slow task finishes, its result is available
    info("Slow task result: %s", future.result())


def task_slow(seconds: float) -> str:
    """Sleep then return a confirmation."""
    time.sleep(seconds)
    return "done after %.2fs" % seconds


if __name__ == "__main__":
    basicConfig(
        level=DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
