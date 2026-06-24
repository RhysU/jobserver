# Copyright (C) 2019-2026 Rhys Ulerich
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""FixedBytesQueue, MPMCQueue, SPSCQueue, and related utility functions."""

import abc
import math
import os
import queue
import threading
import time

# Implementation depends upon an explicit subset of multiprocessing
from multiprocessing import get_context
from multiprocessing.connection import Connection
from multiprocessing.context import BaseContext
from multiprocessing.reduction import ForkingPickler
from multiprocessing.synchronize import Lock
from typing import Any, Callable, Generic, Optional, TypeVar, Union, final

from ._compat import pipe_buf

__all__ = (
    "EndsFactory",
    "FixedBytesQueue",
    "MPMCQueue",
    "SPSCQueue",
    "Source",
    "deadline_to_timeout",
    "from_fds",
    "from_fifo",
    "resolve_context",
    "timeout_to_deadline",
)

EndsFactory = Callable[[], tuple[Connection, Connection]]
Source = Union[None, str, BaseContext, EndsFactory]

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
    if timeout is None:
        return _MAX_TIMEOUT_SECS + time.monotonic()
    if isinstance(timeout, (int, float)) and not isinstance(timeout, bool):
        if not math.isfinite(timeout):
            raise ValueError(f"timeout must be finite, got {timeout!r}")
        if timeout < 0:
            raise ValueError(f"timeout must be non-negative, got {timeout!r}")
        return timeout + time.monotonic()
    raise TypeError(
        f"timeout: None or a real number, got {type(timeout).__name__}"
    )


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


def from_fds(read_fd: int, write_fd: int) -> EndsFactory:
    """A source over a private dup of the given fds."""

    def from_fds_make() -> tuple[Connection, Connection]:
        rd = os.dup(read_fd)
        try:
            wd = os.dup(write_fd)
        except BaseException:
            os.close(rd)
            raise
        return (
            Connection(rd, readable=True, writable=False),
            Connection(wd, readable=False, writable=True),
        )

    return from_fds_make


def from_fifo(path: str) -> EndsFactory:
    """A source over a named FIFO the queue will own."""

    def from_fifo_make() -> tuple[Connection, Connection]:
        rd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
        try:
            wd = os.open(path, os.O_WRONLY)
        except BaseException:
            os.close(rd)
            raise
        os.set_blocking(rd, True)
        return (
            Connection(rd, readable=True, writable=False),
            Connection(wd, readable=False, writable=True),
        )

    return from_fifo_make


def to_ends_factory(source: Source, /) -> EndsFactory:
    """Pass a source through, or wrap a context as a pipe source."""
    if callable(source):
        return source
    context = resolve_context(source)
    return lambda: context.Pipe(duplex=False)


def check_fixedlen(fixedlen: int) -> None:
    """Validate a fixedlen token length."""
    if not isinstance(fixedlen, int):
        raise TypeError(f"fixedlen: int, got {type(fixedlen).__name__}")
    pb = pipe_buf()
    if not 1 <= fixedlen < pb:
        raise ValueError(
            f"fixedlen must satisfy 1 <= fixedlen < {pb}, got {fixedlen!r}"
        )


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
    """Abstract pipe-backed queue; subclasses implement get()/put().

    Provides the pipe lifecycle -- repr, pickling, waitable(), and
    close_get()/close_put().  Used instead of multiprocessing.SimpleQueue
    (no get() timeout) and multiprocessing.Queue (unwanted threading).
    """

    __slots__ = ("_reader", "_writer")

    _reader: Optional[Connection]
    _writer: Optional[Connection]

    def __init__(self, state: Callable[[], tuple[Any, ...]], /) -> None:
        self.__setstate__(state())

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(reader={_conn_repr(self._reader)},"
            f" writer={_conn_repr(self._writer)})"
        )

    def __getstate__(
        self,
    ) -> tuple[Optional[Connection], Optional[Connection]]:
        return (self._reader, self._writer)

    def __setstate__(
        self, state: tuple[Optional[Connection], Optional[Connection]]
    ) -> None:
        self._reader, self._writer = state

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
        # Because any "copy" should and can only mutate the same pipe
        return self

    def __deepcopy__(self: Self, _: Any) -> Self:
        """Deep copies return the original queue unchanged."""
        # Because any "copy" should and can only mutate the same pipe
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
            try:
                self._reader.close()
            except OSError:
                # Tolerate EBADF from an already-closed or duplicated fd.
                pass
            self._reader = None

    def close_put(self) -> None:
        """Close the sending end; put() may no longer be called.

        Closes the underlying pipe end so EOF propagates on crash.
        """
        if self._writer is not None:
            try:
                self._writer.close()
            except OSError:
                # Tolerate EBADF from an already-closed or duplicated fd.
                pass
            self._writer = None

    @abc.abstractmethod
    def get(self, timeout: Optional[float] = None) -> T:
        """Get one object, raising queue.Empty when none is available.

        Raises EOFError on an exhausted queue once the sending half hangs up.
        """
        ...

    @abc.abstractmethod
    def put(self, obj: T) -> None:
        """Put one object into the queue.

        Raises BrokenPipeError if the receiving half has hung up.
        """
        ...


class AbstractPicklingQueue(AbstractQueue[T]):
    """AbstractQueue whose get()/put() pickle generic objects.

    Objects are serialized/deserialized with ForkingPickler and moved
    across the pipe as length-prefixed byte frames.  The read/write guards
    keep those multi-syscall frames intact under concurrent access.
    """

    __slots__ = ("_read_lock", "_write_lock")

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

    def __init__(self, source: Source = None, /) -> None:
        ends = to_ends_factory(source)
        super().__init__(lambda: (*ends(), None))

    def __getstate__(self) -> tuple[Any, ...]:
        return (*super().__getstate__(), self._getstate_locks())

    def __setstate__(self, state: tuple[Any, ...]) -> None:
        reader, writer, state_locks = state
        super().__setstate__((reader, writer))
        self._setstate_locks(state_locks)

    def get(self, timeout: Optional[float] = None) -> T:
        """Get one object, raising queue.Empty when none is available.

        Raises EOFError on an exhausted queue once the sending half hangs up.
        """
        # A deadline lets lock acquisition and poll() share one time budget.
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
            # Hold the lock across recv_bytes() so concurrent consumers
            # cannot interleave frames (cross-process for MPMCQueue).
            recv = reader.recv_bytes()
        finally:
            self._read_lock.release()

        # Deserialize outside the critical section
        return ForkingPickler.loads(recv)

    def put(self, obj: T) -> None:
        """Put one object into the queue.

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

    __slots__ = ()

    def __init__(
        self,
        source: Source = None,
        /,
        *,
        context: Union[None, str, BaseContext] = None,
    ) -> None:
        if context is None and not callable(source):
            context = source
        ctx = resolve_context(context)
        ends = to_ends_factory(source if callable(source) else ctx)
        locks = (ctx.Lock(), ctx.Lock())
        AbstractQueue.__init__(self, lambda: (*ends(), locks))

    def _getstate_locks(self) -> tuple[Lock, Lock]:
        # Preserve the IPC locks so cross-process mutual exclusion survives
        # pickling into child processes (via multiprocessing inheritance).
        return (self._read_lock, self._write_lock)

    def _setstate_locks(self, state_locks: Any) -> None:
        # Minted at construction, inherited on unpickle: always a real pair.
        self._read_lock, self._write_lock = state_locks


@final
class FixedBytesQueue(AbstractQueue[bytes]):
    """A lockless queue of fixed-length byte tokens.

    Tokens are fixedlen bytes with 1 <= fixedlen < PIPE_BUF.  Each get() is a
    single indivisible read() and each put() one atomic <= PIPE_BUF write(),
    so whole tokens move across processes losslessly without any lock.
    """

    __slots__ = ("_fixedlen",)

    def __init__(
        self,
        source: Source = None,
        /,
        *,
        fixedlen: int,
    ) -> None:
        """Use fixedlen-byte tokens; requires 1 <= fixedlen < pipe_buf()."""
        check_fixedlen(fixedlen)
        ends = to_ends_factory(source)
        super().__init__(lambda: (*ends(), fixedlen))

    def __getstate__(self) -> tuple[Any, ...]:
        return (*super().__getstate__(), self._fixedlen)

    def __setstate__(self, state: tuple[Any, ...]) -> None:
        reader, writer, fixedlen = state
        super().__setstate__((reader, writer))
        self._fixedlen = fixedlen
        # Non-blocking reads let a consumer that loses the post-poll() race
        # fall through and retry rather than block on a drained pipe.
        if self._reader is not None:
            os.set_blocking(self._reader.fileno(), False)

    def get(self, timeout: Optional[float] = None) -> bytes:
        """Get one fixedlen-byte token, raising queue.Empty if unavailable.

        Raises EOFError once the sending half has hung up and drained.
        """
        deadline = timeout_to_deadline(timeout)
        while True:
            reader = self._reader
            if reader is None:
                raise ValueError(
                    f"{type(self).__name__}.get() after close_get()"
                )
            if not reader.poll(deadline_to_timeout(deadline)):
                raise queue.Empty
            # One indivisible read() of exactly fixedlen bytes, never looped,
            # so consumers can't split a token.  Losing the post-poll() race
            # surfaces as EAGAIN on the non-blocking reader, so retry.
            try:
                token = os.read(reader.fileno(), self._fixedlen)
            except BlockingIOError:
                continue
            if not token:
                raise EOFError
            if len(token) != self._fixedlen:
                # Cannot happen while every put()/get() honors fixedlen, but
                # fail loudly rather than hand back a truncated token.
                raise OSError(
                    f"read {len(token)} bytes, expected exactly "
                    f"{self._fixedlen}"
                )
            return token

    def put(self, obj: bytes) -> None:
        """Put one or more fixedlen-byte tokens in a single atomic write.

        len(obj) must be a positive multiple of fixedlen and at most
        select.PIPE_BUF so the write stays atomic.  Raises ValueError
        otherwise, or BrokenPipeError if the receiving half has hung up.
        """
        n = len(obj)
        # Fast path: a single token is always valid (fixedlen checked in
        # __init__); only multi-token writes run the fuller check.
        if n != self._fixedlen:
            pb = pipe_buf()
            if n == 0 or n % self._fixedlen != 0 or n > pb:
                raise ValueError(
                    f"put() requires a positive multiple of {self._fixedlen} "
                    f"bytes up to {pb}, got {n}"
                )
        writer = self._writer
        if writer is None:
            raise ValueError(f"{type(self).__name__}.put() after close_put()")
        # One atomic write() of <= PIPE_BUF bytes (whole tokens).
        os.write(writer.fileno(), obj)
