# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 5 shows detecting a submission that ends without a result."""

import signal
from logging import INFO, basicConfig, captureWarnings, info

from jobserver import Jobserver, LostResult


def main() -> None:
    """Shows detecting a submission that ends without a result."""
    with Jobserver(context="spawn", slots=2) as jobserver:
        # Submit work that SIGKILLs itself, closing the result pipe
        # without sending a result.
        future_killed = jobserver(signal.raise_signal, signal.SIGKILL)

        # Submit normal work alongside the doomed submission
        future_ok = jobserver.submit(fn=len, args=("hello",))

        # wait() returns True once settled, even when the submission died
        info("Killed worker wait: %s", future_killed.wait())
        info("Normal result: %s", future_ok.result())

        # result() raises LostResult for the killed worker
        try:
            future_killed.result()
            raise RuntimeError("Expected LostResult was not raised")
        except LostResult:
            info("Caught expected LostResult from SIGKILL worker")


if __name__ == "__main__":
    basicConfig(
        level=INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    captureWarnings(True)
    main()
