# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 6 shows environment injection and preexec_fn for children."""
import ctypes
import ctypes.util
import os
import signal
from logging import basicConfig, info, DEBUG

from jobserver import Jobserver


def main() -> None:
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

    # preexec_fn runs before the task function, here establishing
    # PR_SET_PDEATHSIG so the child receives SIGTERM if the parent dies
    future_pdeathsig = jobserver.submit(
        fn=task_check_pdeathsig,
        preexec_fn=preexec_set_pdeathsig,
    )
    info("pdeathsig active: %s", future_pdeathsig.result())


def task_getenv(key: str) -> str:
    """Return os.environ.get(key, 'MISSING')."""
    return os.environ.get(key, "MISSING")


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
        level=DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
