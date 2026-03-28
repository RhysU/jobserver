# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Shared test helpers used across multiple test modules.

All module-level functions must be picklable for use with spawn/forkserver
start methods.
"""

from __future__ import annotations

import multiprocessing
import os
import signal
import sys
import time
import typing

from jobserver import Jobserver, JobserverExecutor, MinimalQueue

T = typing.TypeVar("T")

# Most tests use the fastest start method.  On Python 3.12+ "fork" is
# deprecated when the process is multi-threaded, so fall back to
# "forkserver".
FAST = "forkserver" if sys.version_info >= (3, 12) else "fork"

TIMEOUT = 30  # generous per-future timeout to avoid flakes


def silence_forkserver() -> None:
    """Start the forkserver with stderr silenced.

    The forkserver process inherits the parent's fd 2 at start time.
    By pointing fd 2 at /dev/null before the first forkserver spawn,
    benign tracebacks from the forkserver (e.g. FileNotFoundError
    when a SemLock backing file is unlinked during cleanup) go to
    /dev/null instead of polluting the test trace.  The main
    process's stderr is restored immediately afterward.
    """
    if FAST != "forkserver":
        return
    devnull_fd = os.open(os.devnull, os.O_WRONLY)
    saved_fd = os.dup(2)
    os.dup2(devnull_fd, 2)
    os.close(devnull_fd)
    try:
        ctx = multiprocessing.get_context("forkserver")
        p = ctx.Process(target=os.getpid)
        p.start()
        p.join()
    finally:
        os.dup2(saved_fd, 2)
        os.close(saved_fd)


# ---- Module-level helpers (must be picklable for spawn) ----


def round_trip_bytes(n: int) -> bytes:
    """Create and return n bytes."""
    return b"x" * n


def barrier_wait(path: str) -> str:
    """Block until a file appears, then return."""
    deadline = time.monotonic() + 30
    while not os.path.exists(path):
        if time.monotonic() > deadline:
            raise TimeoutError("barrier file never appeared")
        time.sleep(0.01)
    return "released"


def raising_at_position(i: int, fail_at: int) -> int:
    if i == fail_at:
        raise ValueError(f"fail at {fail_at}")
    return i


def executor_in_child(method: str) -> int:
    """Create an executor inside a child process and run work."""
    js = Jobserver(context=method, slots=1)
    with JobserverExecutor(js) as exe:
        return exe.submit(len, (1, 2, 3)).result(timeout=10)


def executor_in_child_via_queue(
    method: str, q: multiprocessing.Queue[int]
) -> None:
    """Wrapper for executor_in_child that puts result on a queue."""
    q.put(executor_in_child(method))


def helper_nonblocking(mq: MinimalQueue[str]) -> str:
    """Receive and return a handshake from a MinimalQueue."""
    # Timeout allows heavy OS load while also detecting complete breakage
    return mq.get(timeout=60.0)


def helper_callback(lizt: list, index: int, increment: int) -> None:
    """Helper permitting tests to observe callbacks firing."""
    lizt[index] += increment


def helper_return(arg: T) -> T:
    """Helper returning its lone argument."""
    return arg


def helper_raise(klass: type, *args) -> typing.NoReturn:
    """Helper raising the requested Exception class."""
    raise klass(*args)


def helper_signal(sig: signal.Signals) -> typing.NoReturn:
    """Helper sending the given signal to the current process."""
    signal.raise_signal(sig)
    raise AssertionError("Unreachable")


def helper_noop() -> None:
    """A do-nothing callable picklable for spawn/forkserver contexts."""


def helper_preexec_fn() -> None:
    """Mutates os.environ so that the change can be observed."""
    os.environ["JOBSERVER_TEST_ENVIRON"] = "PREEXEC_FN"


def helper_current_process_name() -> str:
    """Return the name of the current process."""
    return multiprocessing.current_process().name


def helper_recurse(js: Jobserver, max_depth: int) -> int:
    """Helper submitting work until either Blocked or max_depth reached."""
    from jobserver import Blocked

    if max_depth < 1:
        return 0
    try:
        f = js.submit(fn=helper_recurse, args=(js, max_depth - 1), timeout=0)
    except Blocked:
        return 0
    return 1 + f.result(timeout=None)
