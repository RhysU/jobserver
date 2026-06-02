# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 2 shows the Future lifecycle, polling, and resource cleanup."""

from logging import INFO, basicConfig, captureWarnings, info

from jobserver import Jobserver


def main() -> None:
    """Shows the Future lifecycle, polling, and resource cleanup."""
    # A "with" block is optional but convenient: on exit it reclaims any
    # outstanding Futures, avoiding a ResourceWarning at finalization.
    with Jobserver(slots=2) as jobserver:
        # jobserver.submit(...) can be abbreviated to
        # jobserver(fn, *args, **kwargs).
        future1 = jobserver(len, "lifecycle")

        # done(): a cheap progress check that never blocks the caller.
        info("done() poll: %s", future1.done())

        # wait(): block for completion when the value itself is not needed.
        info("wait() blocks then returns: %s", future1.wait())

        # result(): block for and return the value, re-raising on failure.
        info("result(): %r", future1.result())

        # reclaim_resources() frees resources and issues callbacks for
        # finished Futures you never waited on, like invoking the garbage
        # collector, generally unnecessary but occasionally useful.
        reclaimed: list = []
        future2 = jobserver(len, "second")
        future2.when_done(reclaimed.append, "callback fired")

        # Exiting the "with" block would reclaim this implicitly.
        while not reclaimed:
            jobserver.reclaim_resources()

        info("reclaim_resources() handled future2: %s", reclaimed)


if __name__ == "__main__":
    basicConfig(
        level=INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    captureWarnings(True)
    main()
