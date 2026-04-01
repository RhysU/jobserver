# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 6 shows environment variable injection for child processes."""

import os
from logging import INFO, basicConfig, captureWarnings, info

from jobserver import Jobserver


def main() -> None:
    """Shows environment variable injection for child processes."""
    with Jobserver(context="spawn", slots=2) as jobserver:
        # Set an environment variable in the child process
        future_set = jobserver.submit(
            fn=task_getenv_missing,
            args=("DEMO_KEY",),
            env={"DEMO_KEY": "hello"},
        )
        info("env set: %s", future_set.result())

        # Unset an environment variable by passing None
        future_unset = jobserver.submit(
            fn=task_getenv_missing,
            args=("DEMO_KEY",),
            env={"DEMO_KEY": None},
        )
        info("env unset: %s", future_unset.result())


def task_getenv_missing(key: str) -> str:
    """Return os.getenv(key, 'MISSING')."""
    return os.getenv(key, "MISSING")


if __name__ == "__main__":
    basicConfig(
        level=INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    captureWarnings(True)
    main()
