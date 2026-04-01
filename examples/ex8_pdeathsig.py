# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Example 8 shows preexec_fn setting PR_SET_PDEATHSIG via prctl.

Falls back gracefully with warnings when prctl is unavailable.
"""

import ctypes
import ctypes.util
import os
import signal
from logging import INFO, basicConfig, info, warning

from jobserver import Jobserver

_PR_SET_PDEATHSIG = 1
_PR_GET_PDEATHSIG = 2


def _check_pdeathsig_available() -> bool:
    """Return True if prctl(PR_SET_PDEATHSIG, ...) works on this platform."""
    lib = ctypes.util.find_library("c")
    if lib is None:
        return False
    libc = ctypes.CDLL(lib, use_errno=True)
    try:
        result = libc.prctl(_PR_SET_PDEATHSIG, signal.SIGTERM)
    except OSError:
        return False
    if result != 0:
        return False
    # Undo immediately
    libc.prctl(_PR_SET_PDEATHSIG, 0)
    return True


def main() -> None:
    """Shows preexec_fn setting PR_SET_PDEATHSIG via prctl."""
    if not _check_pdeathsig_available():
        warning(
            "prctl(PR_SET_PDEATHSIG) unavailable on this platform;"
            " skipping example"
        )
        return

    jobserver = Jobserver(context="spawn", slots=2)

    # preexec_fn runs before the task function, here establishing
    # PR_SET_PDEATHSIG so the child receives SIGTERM if the parent dies
    future = jobserver.submit(
        fn=task_check_pdeathsig,
        preexec_fn=preexec_set_pdeathsig,
    )
    info("pdeathsig active: %s", future.result())


def task_check_pdeathsig() -> bool:
    """Return True if PR_SET_PDEATHSIG has been set to a nonzero signal."""
    lib = ctypes.util.find_library("c")
    if lib is None:
        return False
    libc = ctypes.CDLL(lib, use_errno=True)
    sig = ctypes.c_int(0)
    try:
        result = libc.prctl(_PR_GET_PDEATHSIG, ctypes.byref(sig))
    except OSError:
        return False
    if result != 0:
        return False
    return sig.value != 0


def preexec_set_pdeathsig() -> None:
    """Set PR_SET_PDEATHSIG so child receives SIGTERM when parent dies."""
    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    result = libc.prctl(_PR_SET_PDEATHSIG, signal.SIGTERM)
    if result != 0:
        errno = ctypes.get_errno()
        raise OSError(errno, os.strerror(errno))


if __name__ == "__main__":
    basicConfig(
        level=INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    main()
