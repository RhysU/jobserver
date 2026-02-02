# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Demo: Custom work acceptance via sleep_fn (e.g., RAM gating)."""
from typing import Optional

from ..impl import Blocked, Jobserver


class ReadyAfterRetries:
    """Returns None (ready) after a number of retries, else sleep duration."""

    def __init__(self, retries: int):
        self.remaining = retries

    def __call__(self) -> Optional[float]:
        if self.remaining > 0:
            self.remaining -= 1
            return 0.01
        return None


if __name__ == "__main__":
    jobserver = Jobserver(slots=2)

    # sleep_fn returning None means "proceed immediately"
    future = jobserver.submit(
        fn=len, args=("abc",), sleep_fn=ReadyAfterRetries(0)
    )
    assert future.result() == 3

    # sleep_fn can delay, then allow (retries twice, then proceeds)
    future = jobserver.submit(
        fn=len, args=("abcd",), sleep_fn=ReadyAfterRetries(2)
    )
    assert future.result() == 4

    # sleep_fn that never returns None causes Blocked on timeout
    try:
        jobserver.submit(
            fn=len, args=("x",), sleep_fn=lambda: 0.05, timeout=0.2
        )
        assert False, "Should have raised Blocked"
    except Blocked:
        pass  # Expected: timed out waiting for sleep_fn to return None

    print("gating: OK")
