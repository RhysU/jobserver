# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 2 shows nested submissions sharing slot constraints."""
from logging import basicConfig, info, DEBUG

from jobserver import Blocked, Jobserver


def main() -> None:
    # Using slots=2 here; slots=None would use os.sched_getaffinity(0)
    # to match the number of usable CPUs for the current process.
    jobserver = Jobserver(context="fork", slots=2)

    # Parent submits task_recurse which itself submits more work.
    # Recursion depth is bounded by the number of available slots.
    future = jobserver.submit(fn=task_recurse, args=(jobserver, 10), timeout=5)
    depth = future.result()
    info("Reached recursion depth %d with 2 slots", depth)

    # With more slots, deeper recursion is possible
    jobserver_wide = Jobserver(context="fork", slots=4)
    future_wide = jobserver_wide.submit(
        fn=task_recurse, args=(jobserver_wide, 10), timeout=5
    )
    depth_wide = future_wide.result()
    info("Reached recursion depth %d with 4 slots", depth_wide)


def task_recurse(jobserver: Jobserver, max_depth: int) -> int:
    """Submit nested work until either Blocked or max_depth reached."""
    if max_depth < 1:
        return 0
    try:
        future = jobserver.submit(
            fn=task_recurse, args=(jobserver, max_depth - 1), timeout=0
        )
    except Blocked:
        return 0
    return 1 + future.result(timeout=None)


if __name__ == "__main__":
    basicConfig(
        level=DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
