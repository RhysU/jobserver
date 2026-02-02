# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Demo: Nested submissions share the global slot limit."""
from ..impl import Blocked, Jobserver


def child_work(jobserver: Jobserver, depth: int) -> int:
    """Recursively submit work until slots exhausted or depth reached."""
    if depth <= 0:
        return 0
    try:
        future = jobserver.submit(
            fn=child_work, args=(jobserver, depth - 1), timeout=0
        )
    except Blocked:
        return 0
    return 1 + future.result()


if __name__ == "__main__":
    # With 3 slots and depth 10, recursion stops at slot limit
    jobserver = Jobserver(slots=3)
    achieved_depth = child_work(jobserver, depth=10)

    # Recursion limited by slots, not by depth
    assert achieved_depth == 3

    # With more slots, deeper recursion is possible
    jobserver_big = Jobserver(slots=5)
    achieved_depth_big = child_work(jobserver_big, depth=10)
    assert achieved_depth_big == 5

    print("nested: OK")
