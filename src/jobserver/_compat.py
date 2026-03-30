# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Portability across interpreters and platforms."""

import os
import signal


def ignore_sigpipe() -> None:
    """Ignore SIGPIPE so broken pipes raise BrokenPipeError.

    CPython does this at startup; PyPy does not.
    """
    if hasattr(signal, "SIGPIPE"):
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)


def sched_getaffinity0() -> int:
    """Return the number of usable CPUs for the current process.

    Falls back to os.cpu_count() when os.sched_getaffinity is
    not available (e.g. PyPy, macOS).
    """
    if hasattr(os, "sched_getaffinity"):
        return len(os.sched_getaffinity(0))
    # os.cpu_count() can return None
    return os.cpu_count() or 1
