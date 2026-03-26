# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 9 shows JobserverExecutor as a context manager."""
import time
from logging import INFO, basicConfig, info

from jobserver import Jobserver, JobserverExecutor


def main() -> None:
    """Shows JobserverExecutor: context manager, map, submit, and cancel."""
    jobserver = Jobserver(context="spawn", slots=1)

    with JobserverExecutor(jobserver) as executor:
        # map() applies a function to every item and yields results in order
        squares = list(executor.map(task_square, [1, 2, 3, 4, 5]))
        info("squares via map: %s", squares)

        # Submit a slow task that holds the only available slot
        future_slow = executor.submit(task_slow, 0.5)
        time.sleep(0.2)  # let the slow task start and claim the slot

        # With no slot free, this future queues as PENDING and is cancellable
        future_pending = executor.submit(task_square, 99)
        time.sleep(0.1)  # let the request reach the dispatcher

        # Cancel the PENDING future before it is dispatched to a worker
        cancelled = future_pending.cancel()
        info("future_pending cancelled: %s", cancelled)

        # Collect the slow task's result; executor shuts down cleanly on exit
        info("slow task: %s", future_slow.result())


def task_square(n: int) -> int:
    """Return n squared."""
    return n * n


def task_slow(seconds: float) -> str:
    """Sleep then return a confirmation."""
    time.sleep(seconds)
    return "done after %.2fs" % seconds


if __name__ == "__main__":
    basicConfig(
        level=INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
