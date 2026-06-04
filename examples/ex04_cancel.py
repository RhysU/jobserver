# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 4 shows cancelling running work by signalling the worker."""

import signal
import time
from logging import INFO, basicConfig, captureWarnings, info

from jobserver import Jobserver, SubmissionDied


def main() -> None:
    """Shows cancelling running work via SIGTERM through Future.wait()."""
    with Jobserver(slots=2) as jobserver:
        # Submit long-running work that would otherwise never finish here.
        future_cancel = jobserver.submit(fn=time.sleep, args=(3600,))

        # Submit normal work alongside the doomed submission.
        future_ok = jobserver.submit(fn=len, args=("hello",))

        # wait(signal=...) sends a signal to the running worker, then waits.
        # wait() returns True once completion (here, death) is confirmed.
        info(
            "Cancelled worker wait: %s",
            future_cancel.wait(signal=signal.SIGTERM),
        )
        info("Normal result: %s", future_ok.result())

        # result() raises SubmissionDied for the cancelled worker.
        try:
            future_cancel.result()
            raise RuntimeError("Expected SubmissionDied was not raised")
        except SubmissionDied:
            info("Caught expected SubmissionDied from cancelled worker")


if __name__ == "__main__":
    basicConfig(
        level=INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    captureWarnings(True)
    main()
