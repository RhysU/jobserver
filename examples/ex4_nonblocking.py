# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Poll and timeout without blocking."""
import logging

from jobserver import Blocked, Jobserver, MinimalQueue

log = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    jobserver = Jobserver(slots=1)
    queue: MinimalQueue[str] = MinimalQueue()

    log.debug("Submitting work that blocks until we send a signal")
    future = jobserver.submit(fn=queue.get, kwargs={"timeout": 60.0})

    log.debug("Poll without blocking: done() returns False")
    assert future.done(timeout=0) is False

    log.debug("result() with timeout=0 raises Blocked")
    try:
        future.result(timeout=0)
        assert False, "Should have raised Blocked"
    except Blocked:
        pass

    log.debug("submit() with timeout=0 raises Blocked when slots exhausted")
    try:
        jobserver.submit(fn=len, args=("x",), timeout=0)
        assert False, "Should have raised Blocked"
    except Blocked:
        pass

    log.debug("Unblocking the worker")
    queue.put("go")

    log.debug("Now result is available")
    assert future.result() == "go"
    assert future.done() is True

    log.info("ex4_nonblocking: OK")
