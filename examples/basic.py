# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Submit work and retrieve results."""
import logging

from jobserver import Jobserver

log = logging.getLogger(__name__)


def square(x: int) -> int:
    return x * x


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    log.debug("Creating jobserver with 2 slots")
    jobserver = Jobserver(slots=2)

    log.debug("Submitting work using __call__ shorthand")
    future_a = jobserver(square, 5)
    future_b = jobserver(square, 7)

    log.debug("Submitting work using explicit submit() method")
    future_c = jobserver.submit(fn=len, args=("hello",))

    log.debug("Retrieving results (blocks until complete)")
    assert future_a.result() == 25
    assert future_b.result() == 49
    assert future_c.result() == 5

    log.debug("Results can be retrieved multiple times")
    assert future_a.result() == 25

    log.info("basic: OK")
