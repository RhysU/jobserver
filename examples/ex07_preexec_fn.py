# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 7 shows preexec_fn as a plain callable and context manager factory.

preexec_fn is called in the worker before the task function.  If it returns
None, it acts as a plain pre-execution hook.  If it returns a context manager,
the task function runs inside it, gaining entry/exit semantics.
"""

import logging
from logging import INFO, basicConfig, captureWarnings, info

from jobserver import Jobserver


def main() -> None:
    """Shows preexec_fn as a callable and as a context manager factory."""
    with Jobserver(context="spawn", slots=2) as jobserver:
        # Plain callable: configure logging in the worker
        future_plain = jobserver.submit(
            fn=task_logger_level,
            preexec_fn=preexec_configure_logging,
        )
        info("plain preexec_fn: level=%s", future_plain.result())

        # Context manager factory: basicConfig on enter, shutdown on exit
        future_cm = jobserver.submit(
            fn=task_logger_level,
            preexec_fn=LoggingContext,
        )
        info("context manager preexec_fn: level=%s", future_cm.result())


def preexec_configure_logging() -> None:
    """Plain preexec_fn that configures logging in the worker."""
    logging.basicConfig(level=logging.DEBUG)


def task_logger_level() -> int:
    """Return the root logger's effective level."""
    return logging.getLogger().getEffectiveLevel()


class LoggingContext:
    """Context manager: basicConfig on enter, shutdown on exit."""

    def __enter__(self):
        logging.basicConfig(level=logging.WARNING)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.shutdown()
        return False


if __name__ == "__main__":
    basicConfig(
        level=INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    captureWarnings(True)
    main()
