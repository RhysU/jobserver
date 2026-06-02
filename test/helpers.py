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
import sys
import time
import typing
from multiprocessing import get_all_start_methods

from jobserver import Jobserver, JobserverExecutor
from jobserver._queue import SPSCQueue

T = typing.TypeVar("T")

# Most tests use the fastest start method.  On Python 3.12+ "fork" is
# deprecated when the process is multi-threaded, so fall back to
# "forkserver".
FAST = "forkserver" if sys.version_info >= (3, 12) else "fork"

TIMEOUT = 30  # generous per-future timeout to avoid flakes


def start_methods(*, threaded: bool = False) -> list[str]:
    """Multiprocessing start methods to parametrize a test over.

    The canonical source for ``for method in start_methods():`` loops so
    the platform/Python policy lives in exactly one place.

    Pass ``threaded=True`` from a process that owns background threads
    (e.g. JobserverExecutor's dispatcher): "fork" is then omitted on
    Python 3.12+, where forking a multithreaded process is deprecated
    and unsafe.
    """
    methods = get_all_start_methods()
    if threaded and sys.version_info >= (3, 12):
        methods = [m for m in methods if m != "fork"]
    return methods


def wait_until(
    predicate: typing.Callable[[], bool],
    *,
    timeout: float = TIMEOUT,
    interval: float = 0.01,
) -> bool:
    """Poll predicate() until it is truthy or timeout elapses.

    The single home for the "spin on a condition with a deadline" pattern
    that test bodies use to wait on process/thread/filesystem state instead
    of a fixed, flaky sleep.  Returns True once predicate() is truthy or
    False on timeout; callers decide what a timeout means (fail, assert,
    or simply proceed).
    """
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            return False
        time.sleep(interval)
    return True


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


# NB: silence_forkserver() must be called from a unittest setUpModule (run
# in the runner process only), never at import time.  An import-time call
# would also run inside spawn/forkserver workers as they import their
# target's module, forking the forkserver mid-import and corrupting the
# child's view of that module.


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


def create_marker(path: str) -> str:
    """Create a file at path, evidence the call actually executed."""
    with open(path, "w") as handle:
        handle.write("ran")
    return "ran"


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


def helper_nonblocking(mq: SPSCQueue[str]) -> str:
    """Receive and return a handshake from a SPSCQueue."""
    # Timeout allows heavy OS load while also detecting complete breakage
    return mq.get(timeout=60.0)


def helper_callback(lizt: list, index: int, increment: int) -> None:
    """Helper permitting tests to observe callbacks firing."""
    lizt[index] += increment


def helper_return(arg: T) -> T:
    """Helper returning its lone argument."""
    return arg


def helper_return_kwargs(**kwargs: typing.Any) -> dict:
    """Helper returning whatever keyword arguments it received."""
    return kwargs


def helper_marker_return(directory: str, arg: T) -> T:
    """Create a marker file named for arg under directory then return arg.

    Lets a test observe, without itself reclaiming slots, that a worker has
    actually finished: the marker exists once fn has run to completion.
    """
    with open(os.path.join(directory, str(arg)), "w") as handle:
        handle.write("ran")
    return arg


def helper_return_connection(context):
    """Return a live Connection that pickles but cannot rebuild remotely."""
    recv, _send = context.Pipe(duplex=False)
    return recv


def helper_raise(klass: type, *args) -> typing.NoReturn:
    """Helper raising the requested Exception class."""
    raise klass(*args)


def helper_noop() -> None:
    """A do-nothing callable picklable for spawn/forkserver contexts."""


def helper_preexec_fn() -> None:
    """Mutates os.environ so that the change can be observed."""
    os.environ["JOBSERVER_TEST_ENVIRON"] = "PREEXEC_FN"


class HelperContextManager:
    """A picklable context manager recording entry/exit via os.environ."""

    def __enter__(self):
        os.environ["JOBSERVER_TEST_CM"] = "ENTERED"
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.environ["JOBSERVER_TEST_CM"] = "EXITED"
        return False


class HelperSuppressingContextManager:
    """A picklable context manager that suppresses exceptions."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return exc_type is not None


def helper_preexec_cm() -> HelperContextManager:
    """Factory returning a context manager that records entry/exit."""
    return HelperContextManager()


def helper_preexec_suppressing_cm() -> HelperSuppressingContextManager:
    """Factory returning a context manager that suppresses exceptions."""
    return HelperSuppressingContextManager()


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
