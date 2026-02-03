# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 3 shows detecting when a worker process dies unexpectedly."""
import os
import signal
from logging import basicConfig, info, DEBUG

from jobserver import Jobserver, SubmissionDied


def main() -> None:
    jobserver = Jobserver(context="forkserver", slots=2)

    # Submit work that will be killed
    future_killed = jobserver.submit(
        fn=task_self_signal, args=(signal.SIGKILL,)
    )

    # Submit normal work alongside the doomed submission
    future_ok = jobserver.submit(fn=len, args=("hello",))

    # done() returns True even for dead submissions
    info("Killed worker done: %s", future_killed.done())
    info("Normal worker done: %s", future_ok.done())
    info("Normal result: %s", future_ok.result())

    # result() raises SubmissionDied for the killed worker
    try:
        future_killed.result()
        raise RuntimeError("Expected SubmissionDied was not raised")
    except SubmissionDied:
        info("Caught expected SubmissionDied from SIGKILL worker")


def task_self_signal(sig: signal.Signals) -> None:
    """Send the given signal to the current process."""
    os.kill(os.getpid(), sig)


if __name__ == "__main__":
    basicConfig(
        level=DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
