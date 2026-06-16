# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Example 6 shows gating work acceptance on external conditions.

Availability of RAM is a more common use case but ill-suited for an example.
"""

import os
import tempfile
from logging import INFO, basicConfig, captureWarnings, info
from typing import Optional

from jobserver import Blocked, Jobserver


def main() -> None:
    """Shows gating work acceptance on external conditions."""
    with Jobserver(context="spawn", slots=2) as jobserver:
        with tempfile.NamedTemporaryFile() as tmp:
            gate_path = tmp.name
            info("Gate file: %s", gate_path)

            # sleep_gate returns None (proceed) when gate exists, 0.1 otherwise
            def sleep_gate() -> Optional[float]:
                if os.path.exists(gate_path):
                    return None
                return 0.1

            # Submission proceeds because the gate file exists
            gated = jobserver.replace_sleep(sleep_gate)
            future = gated.submit(fn=sorted, args=([3, 1, 2],))
            info("With gate file: %s", future.result())

        # Gate file is now removed; sleep_gate keeps returning 0.1 to timeout
        try:
            gated.submit(
                fn=sorted,
                args=([3, 1, 2],),
                timeout=0.35,
            )
            raise RuntimeError("Expected Blocked was not raised")
        except Blocked:
            info("Caught expected Blocked: gate file absent")


if __name__ == "__main__":
    basicConfig(
        level=INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    captureWarnings(True)
    main()
