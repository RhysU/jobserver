# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""MinimalQueue and related utility functions."""

import queue
import threading
import time

# Implementation depends upon an explicit subset of multiprocessing
from multiprocessing import get_context
from multiprocessing.connection import Connection
from multiprocessing.context import BaseContext
from multiprocessing.reduction import ForkingPickler
from typing import Any, Generic, Optional, TypeVar, Union

__all__ = (
    "MinimalQueue",
    "timeout_to_deadline",
    "deadline_to_timeout",
    "resolve_context",
)

T = TypeVar("T")

# Maximum seconds safely passable to poll() without overflow.  poll(2) on
# Linux takes timeout in milliseconds as a signed 32-bit int, so the ceiling
# is INT_MAX ms = 2**31-1 ms = 2_147_483_647 ms = 2_147_483.647 s (~24.85 d).
# Cannot use float('inf'), sys.float_info.max, sys.maxsize/1000, nor the
# _PyTime_t max; all overflow somewhere in the call chain.
# See https://stackoverflow.com/q/45704243
_MAX_TIMEOUT_SECS = (2**31 - 1) / 1000  # INT_MAX ms ≈ 24.85 days


def timeout_to_deadline(timeout: Optional[float]) -> float:
    """
    Convert timeout in seconds into a monotonic, absolute deadline.

    When timeout is None a large, finite deadline is returned per
    poll()/select() restrictions.
    """
    return (
        _MAX_TIMEOUT_SECS if timeout is None else timeout
    ) + time.monotonic()


def deadline_to_timeout(deadline: float) -> float:
    """Return seconds remaining until deadline, non-negative and finite.

    Intended to be paired with timeout_to_deadline().  The result is safe to
    pass directly to poll()/select() on any platform.
    """
    return min(_MAX_TIMEOUT_SECS, max(0.0, deadline - time.monotonic()))


def resolve_context(context: Union[None, str, BaseContext]) -> BaseContext:
    """Return a multiprocessing BaseContext, resolving None/str as needed."""
    if context is None or isinstance(context, str):
        return get_context(context)
    return context


def _conn_repr(conn: Optional[Connection]) -> str:
    """Describe a pipe end for MinimalQueue.__repr__, tolerating closed fds."""
    if conn is None:
        return "closed"
    # fileno() raises ValueError/OSError on a closed fd.
    try:
        return f"open(fd={conn.fileno()})"
    except (ValueError, OSError):
        return "open(fd=closed)"


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
        self._read_lock = threading.Lock()
        self._write_lock = threading.Lock()

    def __repr__(self) -> str:
        return (
            f"MinimalQueue(reader={_conn_repr(self._reader)},"
            f" writer={_conn_repr(self._writer)})"
        )

    def __getstate__(
        self,
    ) -> tuple[Optional[Connection], Optional[Connection]]:
        # Locks are omitted; each process creates its own after unpickling.
        return (self._reader, self._writer)

    def __setstate__(
        self,
        state: tuple[Optional[Connection], Optional[Connection]],
    ) -> None:
        self._reader, self._writer = state
        self._read_lock = threading.Lock()
        self._write_lock = threading.Lock()

    def __enter__(self) -> "MinimalQueue":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close_put()
        self.close_get()

    def __del__(self) -> None:
        """Close pipe ends in a defined order on finalization."""
        # A partially-constructed instance whose __init__ raised before
        # _reader/_writer were assigned surfaces as AttributeError here.
        try:
            self.close_put()
            self.close_get()
        except AttributeError:
            pass

    def __copy__(self) -> "MinimalQueue":
        """Shallow copies return the original MinimalQueue unchanged."""
        # Because any "copy" should and can only mutate same pipe/locks
        return self

    def __deepcopy__(self, _: Any) -> "MinimalQueue":
        """Deep copies return the original MinimalQueue unchanged."""
        # Because any "copy" should and can only mutate same pipe/locks
        return self

    def waitable(self) -> Connection:
        """The object on which to wait(...) to get(...) new data."""
        assert self._reader is not None, "waitable() after close_get()"
        return self._reader

    def close_get(self) -> None:
        """Close the receiving end; get() may no longer be called.

        Closes the underlying pipe end so EOF propagates on crash.
        """
        if self._reader is not None:
            self._reader.close()
            self._reader = None

    def close_put(self) -> None:
        """Close the sending end; put() may no longer be called.

        Closes the underlying pipe end so EOF propagates on crash.
        """
        if self._writer is not None:
            self._writer.close()
            self._writer = None

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
        deadline = timeout_to_deadline(timeout)
        if not self._read_lock.acquire(
            blocking=True, timeout=deadline_to_timeout(deadline)
        ):
            raise queue.Empty
        try:
            if not self._reader.poll(deadline_to_timeout(deadline)):
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
