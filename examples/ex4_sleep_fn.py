# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 4 shows gating work acceptance on external conditions."""
import os
import tempfile
from logging import basicConfig, info, DEBUG

from jobserver import Blocked, Jobserver


def main() -> None:
    jobserver = Jobserver(context="spawn", slots=2)

    # Create a gate file that controls whether work is accepted
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        gate_path = tmp.name
    info("Gate file: %s", gate_path)

    # sleep_fn returns None (proceed) when gate exists, 0.1 (wait) otherwise
    def sleep_fn_gate() -> float:
        if os.path.exists(gate_path):
            return None  # type: ignore[return-value]
        return 0.1

    # Submission proceeds because the gate file exists
    future_a = jobserver.submit(
        fn=task_greet, args=("accepted",), sleep_fn=sleep_fn_gate
    )
    info("With gate file: %s", future_a.result())

    # Remove the gate so sleep_fn keeps returning 0.1 until timeout
    os.unlink(gate_path)
    try:
        jobserver.submit(
            fn=task_greet,
            args=("rejected",),
            sleep_fn=sleep_fn_gate,
            timeout=0.35,
        )
        raise RuntimeError("Expected Blocked was not raised")
    except Blocked:
        info("Caught expected Blocked: gate file absent")


def task_greet(name: str) -> str:
    """Return a greeting."""
    return "Hello, " + name


if __name__ == "__main__":
    basicConfig(
        level=DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
