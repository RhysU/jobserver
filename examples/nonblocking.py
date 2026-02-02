# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Demo: Non-blocking operations with timeouts."""
from jobserver import Blocked, Jobserver, MinimalQueue

if __name__ == "__main__":
    jobserver = Jobserver(slots=1)
    queue: MinimalQueue[str] = MinimalQueue()

    # Submit work that blocks until we send a signal
    future = jobserver.submit(fn=queue.get, kwargs={"timeout": 60.0})

    # Poll without blocking: done() returns False
    assert future.done(timeout=0) is False

    # result() with timeout raises Blocked
    try:
        future.result(timeout=0)
        assert False, "Should have raised Blocked"
    except Blocked:
        pass

    # submit() with timeout raises Blocked when slots exhausted
    try:
        jobserver.submit(fn=len, args=("x",), timeout=0)
        assert False, "Should have raised Blocked"
    except Blocked:
        pass

    # Unblock the worker
    queue.put("go")

    # Now result is available
    assert future.result() == "go"
    assert future.done() is True

    print("nonblocking: OK")
