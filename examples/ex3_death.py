# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 3 shows detecting when a worker process dies unexpectedly."""

import signal
from logging import INFO, basicConfig, info

from jobserver import Jobserver, SubmissionDied


def main() -> None:
    """Shows detecting when a worker process dies unexpectedly."""
    jobserver = Jobserver(context="forkserver", slots=2)

    # Submit work that will be killed
    future_killed = jobserver.submit(
        fn=signal.raise_signal, args=(signal.SIGKILL,)
    )

    # Submit normal work alongside the doomed submission
    future_ok = jobserver.submit(fn=len, args=("hello",))

    # wait() returns True even for dead submissions
    info("Killed worker wait: %s", future_killed.wait())  # Blocks by default
    info("Normal result: %s", future_ok.result())  # Also blocks by default

    # result() raises SubmissionDied for the killed worker
    try:
        future_killed.result()
        raise RuntimeError("Expected SubmissionDied was not raised")
    except SubmissionDied:
        info("Caught expected SubmissionDied from SIGKILL worker")


if __name__ == "__main__":
    basicConfig(
        level=INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    main()
