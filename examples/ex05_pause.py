# Copyright (C) 2019-2026 Rhys Ulerich
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 5 shows pausing and resuming a worker via SIGSTOP/SIGCONT."""

import signal
import time
from logging import INFO, basicConfig, captureWarnings, info

from jobserver import Jobserver


def main() -> None:
    """Shows pausing and resuming a worker via SIGSTOP/SIGCONT."""
    with Jobserver(context="spawn", slots=2) as jobserver:
        # Submit work that will complete once allowed to run
        future = jobserver.submit(fn=time.sleep, args=(0.1,))

        # Pause the worker with SIGSTOP (does not kill it)
        paused = future.wait(timeout=0, signal=signal.SIGSTOP)
        info("Pausing worker: %s", paused)

        # The future is not done while the worker is stopped
        info("Done while stopped: %s", future.done())

        # Resume the worker with SIGCONT so it can finish
        info("Resuming worker: %s", future.wait(signal=signal.SIGCONT))

        # Now the result is available
        info("Result after resume: %s", future.result())


if __name__ == "__main__":
    basicConfig(
        level=INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    captureWarnings(True)
    main()
