# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Demo: Custom work acceptance via sleep_fn (e.g., RAM gating)."""
import itertools
from typing import Iterator, Optional

from ..impl import Blocked, Jobserver


if __name__ == "__main__":
    jobserver = Jobserver(slots=2)

    # sleep_fn returning None means "proceed immediately"
    attempts: Iterator[Optional[float]] = iter([None])
    future = jobserver.submit(
        fn=len, args=("abc",), sleep_fn=lambda: next(attempts)
    )
    assert future.result() == 3

    # sleep_fn can delay, then allow (returns sleep duration, then None)
    attempts = iter([0.01, 0.01, None])
    future = jobserver.submit(
        fn=len, args=("abcd",), sleep_fn=lambda: next(attempts)
    )
    assert future.result() == 4

    # sleep_fn that never returns None causes Blocked on timeout
    never_ready = iter(itertools.repeat(0.05))
    try:
        jobserver.submit(
            fn=len,
            args=("x",),
            sleep_fn=lambda: next(never_ready),
            timeout=0.2,
        )
        assert False, "Should have raised Blocked"
    except Blocked:
        pass  # Expected: timed out waiting for sleep_fn to return None

    print("gating: OK")
