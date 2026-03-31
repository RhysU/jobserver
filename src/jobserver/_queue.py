# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""MinimalQueue and related utility functions."""

import queue
import time

# Implementation depends upon an explicit subset of multiprocessing
from multiprocessing import get_context
from multiprocessing.connection import Connection
from multiprocessing.context import BaseContext
from multiprocessing.reduction import ForkingPickler
from typing import Any, Generic, Optional, TypeVar, Union

# Maximum seconds safely passable to poll() without overflow.  poll(2) on
# Linux takes timeout in milliseconds as a signed 32-bit int, so the ceiling
# is INT_MAX ms = 2**31-1 ms = 2_147_483_647 ms = 2_147_483.647 s (~24.85 d).
# Cannot use float('inf'), sys.float_info.max, sys.maxsize/1000, nor the
# _PyTime_t max; all overflow somewhere in the call chain.
# See https://stackoverflow.com/q/45704243
_MAX_TIMEOUT_SECS: float = (2**31 - 1) / 1000  # INT_MAX ms ≈ 24.85 days

__all__ = (
    "MinimalQueue",
    "absolute_deadline",
    "relative_timeout",
    "resolve_context",
)

T = TypeVar("T")


def absolute_deadline(relative_timeout: Optional[float]) -> float:
    """
    Convert relative_timeout in seconds into a monotonic, absolute deadline.

    When relative_timeout is None the deadline is set _MAX_TIMEOUT_SECS from
    now, the largest value that does not overflow CPython's _PyTime_t when
    later converted back to a relative timeout and passed to select/poll.
    """
    return time.monotonic() + (
        _MAX_TIMEOUT_SECS
        if relative_timeout is None
        else min(relative_timeout, _MAX_TIMEOUT_SECS)
    )


def relative_timeout(deadline: float) -> float:
    """Return seconds remaining until deadline, strictly non-negative.

    Intended to be paired with absolute_deadline().
    """
    return max(0.0, deadline - time.monotonic())


def resolve_context(context: Union[None, str, BaseContext]) -> BaseContext:
    """Return a multiprocessing BaseContext, resolving None/str as needed."""
    if context is None or isinstance(context, str):
        return get_context(context)
    return context


class MinimalQueue(Generic[T]):
    """
    An unbounded SimpleQueue-variant with minimal functionality.

    Specifically, only functionality needed by Jobserver and JobserverExecutor.
    Vanilla multiprocessing.SimpleQueue lacks timeout on get(...).
    Vanilla multiprocessing.Queue has wildly undesired threading machinery.
    Both get(...) and put(...) detect and report when one end hangs up.
    """

    __slots__ = ("_reader", "_writer", "_read_lock", "_write_lock")

    def __init__(self, context: Union[None, str, BaseContext] = None) -> None:
        """Use given context with default of multiprocessing.get_context()."""
        context = resolve_context(context)
        reader, writer = context.Pipe(duplex=False)
        self._reader: Optional[Connection] = reader
        self._writer: Optional[Connection] = writer
        self._read_lock = context.Lock()
        self._write_lock = context.Lock()

    def __repr__(self) -> str:
        rd = self._reader
        wr = self._writer
        r = "closed" if rd is None else f"open(fd={rd.fileno()})"
        w = "closed" if wr is None else f"open(fd={wr.fileno()})"
        return f"MinimalQueue(reader={r}, writer={w})"

    def __enter__(self) -> "MinimalQueue":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close_put()
        self.close_get()

    def __copy__(self) -> "MinimalQueue":
        """Shallow copies return the original MinimalQueue unchanged."""
        # Because any "copy" should and can only mutate same pipe/locks
        return self

    def __deepcopy__(self, _: Any) -> "MinimalQueue":
        """Deep copies return the original MinimalQueue unchanged."""
        # Because any "copy" should and can only mutate same pipe/locks
        return self

    def waitable(self) -> int:
        """The object on which to wait(...) to get(...) new data."""
        assert self._reader is not None, "waitable() after close_get()"
        return self._reader.fileno()

    def close_get(self) -> None:
        """Close the receiving end; get() may no longer be called.

        Closes the underlying pipe end so EOF propagates on crash.
        """
        if self._reader is not None:
            self._reader.close()
            self._reader = None
            # _read_lock must NOT be set to None here.  In spawn/forkserver
            # mode, setting it to None would drop the last parent-side
            # reference, causing CPython to call sem_unlink() on the named
            # semaphore before the child process has had a chance to open it,
            # producing FileNotFoundError during the child's unpickling.

    def close_put(self) -> None:
        """Close the sending end; put() may no longer be called.

        Closes the underlying pipe end so EOF propagates on crash.
        """
        if self._writer is not None:
            self._writer.close()
            self._writer = None
            # _write_lock must NOT be set to None here; same race condition
            # as described in close_get().

    def get(self, timeout: Optional[float] = None) -> T:
        """
        Get one object from the queue raising queue.Empty if unavailable.

        Raises EOFError on exhausted queue whenever sending half has hung up.
        """
        if self._reader is None:
            raise ValueError("MinimalQueue.get() after close_get()")
        # Accounting for lock acquisition time is easiest with a deadline
        # and conditionals repeatedly checking for negative situations
        # Otherwise, this turns into an unpleasantly messy stretch of code
        deadline = absolute_deadline(timeout)
        if not self._read_lock.acquire(block=True, timeout=timeout):
            raise queue.Empty
        try:
            if not self._reader.poll(relative_timeout(deadline)):
                raise queue.Empty
            recv = self._reader.recv_bytes()
        finally:
            self._read_lock.release()

        # Deserialize outside the critical section
        return ForkingPickler.loads(recv)

    def put(self, *args: T) -> None:
        """
        Put zero or more objects into the queue, contiguously.

        Raises BrokenPipeError if the receiving half has hung up.
        """
        if self._writer is None:
            raise ValueError("MinimalQueue.put() after close_put()")
        if args:
            # Serialize outside the critical section
            send = [ForkingPickler.dumps(arg) for arg in args]
            with self._write_lock:
                for item in send:
                    self._writer.send_bytes(item)
