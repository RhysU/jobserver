# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Demo: Exception handling from workers and callbacks."""
import os
import signal

from ..impl import CallbackRaised, Jobserver, SubmissionDied


def raise_error(message: str) -> None:
    raise ValueError(message)


def kill_self() -> None:
    os.kill(os.getpid(), signal.SIGKILL)


def bad_callback() -> None:
    raise RuntimeError("callback failed")


def length(s: str) -> int:
    return len(s)


if __name__ == "__main__":
    jobserver = Jobserver(slots=2)

    # Exceptions in workers propagate to the caller
    future_err = jobserver(raise_error, "oops")
    try:
        future_err.result()
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "oops" in str(e)

    # Worker death is detected via SubmissionDied
    future_died = jobserver(kill_self)
    try:
        future_died.result()
        assert False, "Should have raised SubmissionDied"
    except SubmissionDied:
        pass

    # Callback exceptions are reported via CallbackRaised
    future_cb = jobserver(length, "hello")
    future_cb.when_done(bad_callback)
    try:
        future_cb.done()
        assert False, "Should have raised CallbackRaised"
    except CallbackRaised as e:
        assert isinstance(e.__cause__, RuntimeError)

    # After callback error is reported, result is still available
    assert future_cb.result() == 5

    print("errors: OK")
