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
    with JobserverExecutor(Jobserver(context="spawn", slots=1)) as executor:
        # map() applies a function to every item and yields results in order
        lengths = list(executor.map(len, ["a", "bb", "ccc", "dddd", "eeeee"]))
        info("lengths via map: %s", lengths)

        # Submit a slow task that holds the only available slot
        future_slow = executor.submit(time.sleep, 0.5)
        time.sleep(0.5)  # let the slow task start and claim the slot

        # With no slot free, this future queues as PENDING and is cancellable
        future_pending = executor.submit(len, "pending")
        time.sleep(0.2)  # let the request reach the dispatcher

        # Cancel the PENDING future before it is dispatched to a worker
        cancelled = future_pending.cancel()
        info("future_pending cancelled: %s", cancelled)

        # Collect the slow task's result; executor shuts down cleanly on exit
        info("slow task: %s", future_slow.result())


if __name__ == "__main__":
    import logging

    basicConfig(
        level=INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("jobserver").setLevel(logging.DEBUG)
    main()
