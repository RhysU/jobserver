# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 9 shows replace_preexec with a callable and a context manager.

The replace_preexec callable runs fn(*args, **kwargs) in the worker before the
task function.  A non-None return is assumed to be a context manager wrapping
the task function's execution.
"""

import logging
from logging import INFO, basicConfig, captureWarnings, info

from jobserver import Jobserver


def main() -> None:
    """Shows replace_preexec with a callable and a context manager factory."""
    # replace_preexec(...) configures logging in all workers
    with Jobserver(context="spawn", slots=2).replace_preexec(
        logging.basicConfig, level=logging.DEBUG
    ) as jobserver:
        future_plain = jobserver.submit(fn=task_logger_level)
        info("plain preexec: level=%s", future_plain.result())

        # Context manager factory: basicConfig on enter, shutdown on exit
        future_cm = jobserver.replace_preexec(LoggingContext).submit(
            fn=task_logger_level,
        )
        info("context manager preexec: level=%s", future_cm.result())


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
