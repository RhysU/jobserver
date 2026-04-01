# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 2 shows nested submissions sharing slot constraints."""

from logging import INFO, basicConfig, captureWarnings, info
from multiprocessing import get_all_start_methods

from jobserver import Blocked, Jobserver


def main() -> None:
    """Shows nested submissions for all available multiprocessing contexts."""
    for method in get_all_start_methods():
        example(method)


def example(context: str) -> None:
    """Shows nested submissions sharing slot constraints."""
    # Using slots=2 here; slots=None would use os.sched_getaffinity(0)
    # to match the number of usable CPUs for the current process.
    # Jobserver may be used as a context manager but isn't required.
    jobserver_a = Jobserver(context=context, slots=2)

    # slots=2: parent(1) + child(1) + grandchild(Blocked) -> depth 1
    future_a = jobserver_a.submit(
        fn=task_recurse, args=(jobserver_a, 10), timeout=5
    )
    depth_a = future_a.result()
    info("context=%s: Reached depth %d with 2 slots", context, depth_a)
    assert depth_a == 1, depth_a

    # slots=4: depth 3 (N slots -> depth N-1)
    # Jobserver may be used as a context manager but isn't required.
    with Jobserver(context=context, slots=4) as jobserver_b:
        future_b = jobserver_b.submit(
            fn=task_recurse, args=(jobserver_b, 10), timeout=5
        )
        depth_b = future_b.result()
        info("context=%s: Reached depth %d with 4 slots", context, depth_b)
        assert depth_b == 3, depth_b


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
        level=INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    captureWarnings(True)
    main()
