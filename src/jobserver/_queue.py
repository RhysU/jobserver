# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""MPMCQueue, SPSCQueue, and related utility functions."""

import abc
import queue
import threading
import time

# Implementation depends upon an explicit subset of multiprocessing
from multiprocessing import get_context
from multiprocessing.connection import Connection
from multiprocessing.context import BaseContext
from multiprocessing.reduction import ForkingPickler
from multiprocessing.synchronize import Lock
from typing import Any, Generic, Optional, TypeVar, Union, final

__all__ = (
    "MPMCQueue",
    "SPSCQueue",
    "timeout_to_deadline",
    "deadline_to_timeout",
    "resolve_context",
)

T = TypeVar("T")

# Bound TypeVar emulating the Python 3.11+ Self type so that methods
# returning self report the concrete subclass type under mypy.
Self = TypeVar("Self", bound="AbstractQueue")

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
    """Describe a pipe end for __repr__, tolerating closed fds."""
    if conn is None:
        return "closed"
    # fileno() raises ValueError/OSError on a closed fd.
    try:
        return f"open(fd={conn.fileno()})"
    except (ValueError, OSError):
        return "open(fd=closed)"


class AbstractQueue(Generic[T], abc.ABC):
    """
    An unbounded SimpleQueue-variant with minimal functionality.

    Specifically, only functionality needed by Jobserver and JobserverExecutor.
    Vanilla multiprocessing.SimpleQueue lacks timeout on get(...).
    Vanilla multiprocessing.Queue has wildly undesired threading machinery.
    Both get(...) and put(...) detect and report when one end hangs up.
    """

    __slots__ = ("_reader", "_writer", "_read_lock", "_write_lock")

    # Lock attributes are populated by _setstate_locks in concrete
    # subclasses; their concrete type is left to the subclass.
    _read_lock: Any
    _write_lock: Any

    @abc.abstractmethod
    def _getstate_locks(self) -> Any:
        """Return a picklable representation of this queue's locks."""
        ...

    @abc.abstractmethod
    def _setstate_locks(self, state_locks: Any) -> None:
        """Restore this queue's locks from _getstate_locks() output."""
        ...

    def __init__(self, context: Union[None, str, BaseContext] = None) -> None:
        """Use given context with default of multiprocessing.get_context()."""
        context = resolve_context(context)
        reader, writer = context.Pipe(duplex=False)
        # Delegate reader/writer/lock wiring to the single __setstate__ path.
        self.__setstate__((reader, writer, None))

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(reader={_conn_repr(self._reader)},"
            f" writer={_conn_repr(self._writer)})"
        )

    def __getstate__(
        self,
    ) -> tuple[Optional[Connection], Optional[Connection], Any]:
        return (self._reader, self._writer, self._getstate_locks())

    def __setstate__(
        self,
        state: tuple[Optional[Connection], Optional[Connection], Any],
    ) -> None:
        self._reader, self._writer, state_locks = state
        self._setstate_locks(state_locks)

    def __enter__(self: Self) -> Self:
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

    def __copy__(self: Self) -> Self:
        """Shallow copies return the original queue unchanged."""
        # Because any "copy" should and can only mutate same pipe/locks
        return self

    def __deepcopy__(self: Self, _: Any) -> Self:
        """Deep copies return the original queue unchanged."""
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


class AbstractPicklingQueue(AbstractQueue[T]):
    """AbstractQueue whose get()/put() pickle generic objects.

    Objects are serialized/deserialized with ForkingPickler and moved
    across the pipe as length-prefixed byte frames.  Both get(...) and
    put(...) detect and report when one end hangs up.
    """

    __slots__ = ()

    def get(self, timeout: Optional[float] = None) -> T:
        """
        Get one object from the queue raising queue.Empty if unavailable.

        Raises EOFError on exhausted queue whenever sending half has hung up.
        """
        # Accounting for lock acquisition time is easiest with a deadline
        # and conditionals repeatedly checking for negative situations
        # Otherwise, this turns into an unpleasantly messy stretch of code
        deadline = timeout_to_deadline(timeout)
        # Call acquire() positionally: threading locks name the first
        # argument "blocking" while multiprocessing locks name it "block",
        # so a keyword would not work across both lock implementations.
        if not self._read_lock.acquire(True, deadline_to_timeout(deadline)):
            raise queue.Empty
        try:
            # Snapshot self._reader so a concurrent close_get() yields a clean
            # ValueError here rather than a None-dereference at recv time.
            reader = self._reader
            if reader is None:
                raise ValueError(
                    f"{type(self).__name__}.get() after close_get()"
                )
            if not reader.poll(deadline_to_timeout(deadline)):
                raise queue.Empty
            # Reading under the lock preserves frame integrity if multiple
            # readers ever shared a large-object queue; harmless single-reader
            recv = reader.recv_bytes()
        finally:
            self._read_lock.release()

        # Deserialize outside the critical section
        return ForkingPickler.loads(recv)

    def put(self, obj: T) -> None:
        """
        Put one object into the queue.

        Raises BrokenPipeError if the receiving half has hung up.
        """
        # Serialize outside the critical section
        bytez = ForkingPickler.dumps(obj)
        with self._write_lock:
            # Snapshot self._writer so a concurrent close_put() yields a clean
            # ValueError here rather than a None-dereference at send time.
            writer = self._writer
            if writer is None:
                raise ValueError(
                    f"{type(self).__name__}.put() after close_put()"
                )
            writer.send_bytes(bytez)


@final
class SPSCQueue(AbstractPicklingQueue[T]):
    """A single-producer, single-consumer AbstractQueue.

    Here "single" means single process: the threading.Lock guards
    serialize producers and consumers within one process only and are
    recreated fresh on unpickle, giving no cross-process exclusion.
    """

    __slots__ = ()

    def _getstate_locks(self) -> None:
        return None  # locks are intra-process; never pickled

    def _setstate_locks(self, state_locks: Any) -> None:
        self._read_lock = threading.Lock()
        self._write_lock = threading.Lock()


@final
class MPMCQueue(AbstractPicklingQueue[T]):
    """A multiple-producer, multiple-consumer AbstractQueue.

    Safe for concurrent producers and consumers across processes: the
    read/write guards are multiprocessing IPC locks that are preserved
    across pickling, providing genuine cross-process mutual exclusion.
    """

    __slots__ = ("_context",)

    def __init__(self, context: Union[None, str, BaseContext] = None) -> None:
        """Use given context with default of multiprocessing.get_context()."""
        # Retain the resolved context so _setstate_locks can mint IPC locks
        # from it at construction, before any locks have been inherited.
        self._context = resolve_context(context)
        super().__init__(self._context)

    def _getstate_locks(self) -> tuple[Lock, Lock]:
        # Preserve the IPC locks so cross-process mutual exclusion survives
        # pickling into child processes (via multiprocessing inheritance).
        return (self._read_lock, self._write_lock)

    def _setstate_locks(self, state_locks: Any) -> None:
        if state_locks is None:
            # Fresh construction: mint new IPC locks from the context.
            self._read_lock = self._context.Lock()
            self._write_lock = self._context.Lock()
        else:
            # Unpickled into another process: reuse the inherited locks.
            self._read_lock, self._write_lock = state_locks
