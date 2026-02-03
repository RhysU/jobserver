# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 8 shows preexec_fn setting PR_SET_PDEATHSIG via prctl."""
import ctypes
import ctypes.util
import os
import signal
from logging import INFO, basicConfig, info

from jobserver import Jobserver


def main() -> None:
    """Shows preexec_fn setting PR_SET_PDEATHSIG via prctl."""
    jobserver = Jobserver(context="forkserver", slots=2)

    # preexec_fn runs before the task function, here establishing
    # PR_SET_PDEATHSIG so the child receives SIGTERM if the parent dies
    future = jobserver.submit(
        fn=task_check_pdeathsig,
        preexec_fn=preexec_set_pdeathsig,
    )
    info("pdeathsig active: %s", future.result())


def task_check_pdeathsig() -> bool:
    """Return True if PR_SET_PDEATHSIG has been set to a nonzero signal."""
    PR_GET_PDEATHSIG = 2
    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    sig = ctypes.c_int(0)
    result = libc.prctl(PR_GET_PDEATHSIG, ctypes.byref(sig))
    if result != 0:
        return False
    return sig.value != 0


def preexec_set_pdeathsig() -> None:
    """Set PR_SET_PDEATHSIG so child receives SIGTERM when parent dies."""
    PR_SET_PDEATHSIG = 1
    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    result = libc.prctl(PR_SET_PDEATHSIG, signal.SIGTERM)
    if result != 0:
        errno = ctypes.get_errno()
        raise OSError(errno, os.strerror(errno))


if __name__ == "__main__":
    basicConfig(
        level=INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
