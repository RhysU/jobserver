# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Handle exceptions from workers and callbacks."""
import logging
import os
import signal

from jobserver import CallbackRaised, Jobserver, SubmissionDied

log = logging.getLogger(__name__)


def raise_error(message: str) -> None:
    raise ValueError(message)


def kill_self() -> None:
    os.kill(os.getpid(), signal.SIGKILL)


def bad_callback() -> None:
    raise RuntimeError("callback failed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    jobserver = Jobserver(slots=2)

    log.debug("Exceptions in workers propagate to the caller")
    try:
        jobserver(raise_error, "oops").result()
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "oops" in str(e)

    log.debug("Worker death is detected via SubmissionDied")
    try:
        jobserver(kill_self).result()
        assert False, "Should have raised SubmissionDied"
    except SubmissionDied:
        pass

    log.debug("Callback exceptions are reported via CallbackRaised")
    future = jobserver.submit(fn=len, args=("hello",))
    future.when_done(bad_callback)
    try:
        future.done()
        assert False, "Should have raised CallbackRaised"
    except CallbackRaised as e:
        assert isinstance(e.__cause__, RuntimeError)

    log.debug("After callback error is reported, result is still available")
    assert future.result() == 5

    log.info("ex5_errors: OK")
