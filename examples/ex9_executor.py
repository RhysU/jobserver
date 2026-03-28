# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 9 shows JobserverExecutor as a context manager."""
import os
import time
from concurrent.futures import CancelledError
from logging import DEBUG, INFO, basicConfig, getLogger, info

from jobserver import Jobserver, JobserverExecutor


def process_start() -> None:
    """Log message from processes at startup."""
    basicConfig(
        level=INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    info("started (pid=%d)", os.getpid())


def main() -> None:
    """Shows JobserverExecutor: context manager, map, submit, and cancel."""
    # Jobserver configuration applies to any JobserverExecutor backed by it.
    js = Jobserver(context="spawn", slots=1, preexec_fn=process_start)
    with JobserverExecutor(js) as executor:
        # map() applies a function to every item and yields results in order
        lengths = list(executor.map(len, ["a", "bb", "ccc", "dddd", "eeeee"]))
        info("lengths via map: %s", lengths)

        # Submit a slow task that holds the only available slot
        slow = executor.submit(time.sleep, 0.5)

        # Because of slow, this future queues as PENDING and is cancellable
        pending = executor.submit(len, "pending")

        # Cancel PENDING future before it is dispatched to a worker
        cancelled = pending.cancel()
        info("pending cancelled: %s", cancelled)
        try:
            pending.result()
            raise AssertionError("Unexpected")
        except CancelledError:
            pass

        # Collect the slow task's result; executor shuts down cleanly on exit
        info("slow task complete: %s", slow.done())
        assert slow.result() is None


if __name__ == "__main__":
    # Configure logging, showing lifecycle messages, and announce startup
    process_start()
    getLogger("jobserver").setLevel(DEBUG)
    main()
