# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 6 shows environment variable injection for child processes."""
import os
from logging import INFO, basicConfig, info

from jobserver import Jobserver


def main() -> None:
    """Shows environment variable injection for child processes."""
    jobserver = Jobserver(context="forkserver", slots=2)

    # Set an environment variable in the child process
    future_set = jobserver.submit(
        fn=task_getenv,
        args=("DEMO_KEY",),
        env={"DEMO_KEY": "hello"},
    )
    info("env set: %s", future_set.result())

    # Unset an environment variable by passing None
    future_unset = jobserver.submit(
        fn=task_getenv,
        args=("DEMO_KEY",),
        env={"DEMO_KEY": None},
    )
    info("env unset: %s", future_unset.result())


def task_getenv(key: str) -> str:
    """Return os.environ.get(key, 'MISSING')."""
    return os.environ.get(key, "MISSING")


if __name__ == "__main__":
    basicConfig(
        level=INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
