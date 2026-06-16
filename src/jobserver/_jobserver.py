# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Implementation of the Jobserver and related classes."""

import abc
import functools
import heapq
import os
import queue
import signal
import threading
import time
import traceback
import types
import warnings
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import AbstractContextManager, ExitStack
from dataclasses import dataclass
from itertools import islice

# Implementation depends upon an explicit subset of multiprocessing
from multiprocessing.connection import Connection, wait
from multiprocessing.context import BaseContext
from multiprocessing.process import BaseProcess
from multiprocessing.reduction import ForkingPickler
from selectors import EVENT_READ, DefaultSelector
from typing import (
    Any,
    Generic,
    NoReturn,
    Optional,
    TypeVar,
    Union,
    final,
)

from ._compat import (
    PICKLE_DUMP_ERRORS,
    ignore_sigpipe,
    pipe_buf,
    sched_getaffinity0,
)
from ._queue import (
    FixedBytesQueue,
    deadline_to_timeout,
    resolve_context,
    timeout_to_deadline,
)

# The entirety of the public API; everything else is an implementation detail.
__all__ = (
    "Blocked",
    "CallbackRaised",
    "Future",
    "Jobserver",
    "LostResult",
)

T = TypeVar("T")


@final
class Blocked(Exception):
    """Reports that Jobserver.submit(...) or Future.result(...) is blocked."""


@final
class CallbackRaised(Exception):
    """
    Reports an Exception raised from callbacks registered with a Future.

    Instances of this type must have non-None __cause__ members (see PEP 3134).
    The __cause__ member will be the Exception raised by client code.  The
    __cause__ will never be a non-Exception BaseException, e.g.
    KeyboardInterrupt.

    When raised by some method, e.g. by Future.done(...), Future.wait(...),
    or Future.result(...), the caller MAY choose to re-invoke that same
    method immediately to continue processing any additional callbacks.
    If the caller requires that all callbacks are attempted, the caller
    MUST re-invoke the same method until no CallbackRaised occurs.
    These MAY/MUST semantics allow the caller to decide how much additional
    processing to perform after seeing the 1st, 2nd, or Nth error.

    The seqno member is the value Future.when_done(...) returned for the
    callback registration, tying this exception back to that call.  The
    first user-registered callback has seqno 0, the second has 1, etc.
    """

    def __init__(self, seqno: int) -> None:
        if not isinstance(seqno, int):
            raise TypeError(f"seqno: int, got {type(seqno).__name__}")
        super().__init__()
        self.seqno = seqno

    def __repr__(self) -> str:
        return f"{type(self).__name__}(seqno={self.seqno})"

    # Exceptions need an explicit __str__; BaseException.__str__ ignores it.
    __str__ = __repr__


@final
class LostResult(Exception):
    """
    Reports that a submission failed to return a result.

    This happens when a worker is killed, exits, or replaces itself with
    another program via the exec call before sending a result on the
    shared result pipe. Exactly what transpired is not reported. Do not
    attempt to recover.
    """


class Wrapper(abc.ABC, Generic[T]):
    """Allows Futures to track whether a value was raised or returned."""

    __slots__ = ()

    @abc.abstractmethod
    def unwrap(self) -> T:
        """Raise any wrapped Exception otherwise return some result."""
        ...

    @abc.abstractmethod
    def describe(self) -> str:
        """Short label naming what is wrapped, for diagnostics."""
        ...


# Down the road, ResultWrapper might be extended with "big object"
# support that chooses to place data in shared memory or on disk
# Likely only necessary if/when sending results via pipe breaks down
class ResultWrapper(Wrapper[T]):
    """Specialization of Wrapper for when a result is available."""

    __slots__ = ("_result",)

    def __init__(self, result: T) -> None:
        """Wrap the provided result for use during unwrap()."""
        self._result = result

    def unwrap(self) -> T:
        return self._result

    def describe(self) -> str:
        return f"Result of type {type(self._result).__name__}"


# TODO: revisit once Python 3.11 is the minimum version (Exception.add_note).
class RemoteTraceback(Exception):
    """Carries a child's formatted traceback string.

    Surfaced only via __cause__ on exceptions Future.result() re-raises.
    """

    def __init__(self, traceback: str) -> None:
        self._traceback = traceback

    def __str__(self) -> str:
        return self._traceback


class ExceptionWrapper(Wrapper[Any]):
    """Specialization of Wrapper for when an Exception has been raised.

    Captures the raised exception's traceback as a string at construction
    so that the child stack survives pickle (which drops __traceback__).
    On unpickling, the captured string is re-attached as a RemoteTraceback
    via __cause__ so the parent sees the originating frames on unwrap().
    Any pre-existing __cause__ chain renders inside that string but is
    collapsed into the single RemoteTraceback programmatically.
    """

    __slots__ = ("_raised", "_raised_tb")

    _raised: Exception
    _raised_tb: str

    def __init__(
        self,
        raised: Exception,
        cause: Optional["Wrapper"] = None,
    ) -> None:
        """Wrap raised; inherit cause's captured tb when cause is an
        ExceptionWrapper, else format from raised.__traceback__."""
        assert isinstance(raised, Exception), type(raised)
        self._raised = raised
        if isinstance(cause, ExceptionWrapper):
            self._raised_tb = cause._raised_tb
        elif raised.__traceback__ is not None:
            self._raised_tb = "".join(
                traceback.format_exception(
                    type(raised),
                    raised,
                    raised.__traceback__,
                )
            )
        else:
            self._raised_tb = ""

    def __getstate__(self) -> tuple:
        return (self._raised, self._raised_tb)

    def __setstate__(self, state: tuple) -> None:
        # Pickle dropped __traceback__ in transit; re-attach the captured
        # string via __cause__ so the child's stack renders in the parent.
        self._raised, self._raised_tb = state
        if self._raised_tb:
            self._raised.__cause__ = RemoteTraceback(self._raised_tb)

    def unwrap(self) -> NoReturn:
        raise self._raised

    def describe(self) -> str:
        return f"Exception of type {type(self._raised).__name__}"


# Internal callbacks may run before and after user callbacks.
#
# _PRIORITY_AFTER callbacks may not run because of (a) the user
# observing a CallbackRaised without draining remaining callbacks
# or (b) BaseExceptions escaping user callbacks.  Consider
# _PRIORITY_AFTER best-effort for resource cleanup purposes.
_PRIORITY_BEFORE = 0
_PRIORITY_USER = 1
_PRIORITY_AFTER = 2


def _callback_wrapper(seqno: int, fn, *args, **kwargs) -> None:
    """Call fn(*args, **kwargs) wrapping any Exception in CallbackRaised.

    The seqno is the when_done(...) sequence number from registering fn
    and is stamped on any CallbackRaised raised here.  A non-Exception
    BaseException is not wrapped and propagates raw.
    """
    try:
        fn(*args, **kwargs)
    except Exception as e:
        # A re-entrant when_done() on an already-completed future
        # triggers a nested _issue_callbacks(); without this
        # guard the caller sees CallbackRaised wrapping another
        # CallbackRaised instead of one layer around the cause.
        # The inner CallbackRaised already carries its own seqno.
        if isinstance(e, CallbackRaised):
            raise
        raise CallbackRaised(seqno) from e


@final
class Future(Generic[T]):
    """
    Future instances are obtained by submitting work to a Jobserver.

    Futures report if a submission is done(), its result(), and may
    additionally be used to register callbacks issued at completion.
    Futures are threadsafe.  Futures can be neither copied nor pickled.
    """

    __slots__ = (
        "_rlock",
        "_process",
        "_connection",
        "_wrapper",
        "_callbacks",
        "_callbacks_issuing",
        "_callbacks_seqno",
    )

    def __init__(self, process: BaseProcess, connection: Connection) -> None:
        """
        An instance expecting a Process to send(...) a result to a Connection.
        """
        # Re-entrant so callbacks can register and issue new callbacks
        self._rlock = threading.RLock()

        assert process is not None  # Becomes None after BaseProcess.join()
        self._process: Optional[BaseProcess] = process

        assert connection is not None  # Becomes None after Connection.close()
        self._connection: Optional[Connection] = connection

        self._wrapper: Optional[Wrapper[T]] = None

        # Populated by calls to when_done(...).  A heapq ordered by
        # (priority, callback_seqno, ...) so token restoration fires before
        # user callbacks, which fire before sentinel cleanup.
        self._callbacks: list[tuple] = []

        # True iff callbacks are actively being issued.
        # Flag should only be observed/manipulated within _issue_callbacks()
        self._callbacks_issuing = False

        # Monotonic seqno needed due to reentrancy.  Starting at -3 lets the
        # three internal registrations submit() makes (token restoration plus
        # sentinel and connection cleanup) consume -3, -2, -1 so user 0-based.
        self._callbacks_seqno = -3

    def __repr__(self) -> str:
        with self._rlock:
            done = self._connection is None
            ncallbacks = len(self._callbacks)
            pid = None if self._process is None else self._process.pid
        return (
            f"Future({'done' if done else 'running'}"
            f", callbacks={ncallbacks}"
            f", pid={pid})"
        )

    def __copy__(self) -> NoReturn:
        """Disallow copying as duplicates cannot sensibly share resources."""
        # In particular, which copy would call self._process.join()?
        # TypeError matches Python convention for operations unsupported
        # by a type, e.g. copy.copy(threading.Lock()).
        raise TypeError("Futures cannot be copied.")

    def __reduce__(self) -> NoReturn:
        """Disallow pickling as duplicates cannot sensibly share resources."""
        # In particular, because pickles create copies.
        raise TypeError("Futures cannot be pickled.")

    def __del__(self) -> None:
        """Emit ResourceWarning if this Future still holds OS resources.

        The Jobserver keeps an in-flight Future alive while it remains open,
        so this warning mostly covers a Future that outlives a closed
        Jobserver.  A Future dropped while the Jobserver is open keeps the
        worker running and the Jobserver reports it on finalization instead.
        """
        if getattr(self, "_connection", None) is not None:
            warnings.warn(
                f"Finalizing {self!r} with open connection",
                ResourceWarning,
                stacklevel=2,
                source=self,
            )

    def when_done(self, fn: Callable, *args: Any, **kwargs: Any) -> int:
        """
        Register fn(*args, **kwargs) for execution after Future.done(...).

        Callbacks run synchronously, inline, on whichever thread first
        observes completion.  There is no background thread or timeout.
        When already done(...) function fn(...) will be invoked
        immediately after completion of any currently executing or
        registered callbacks for *this* Future.

        The Future is not automatically passed; to receive it, bind it
        explicitly via args, e.g. future.when_done(cb, future).
        May raise CallbackRaised from at most this new callback.

        Returns a seqno identifying this registration.  Should fn raise,
        the resulting CallbackRaised reports it via CallbackRaised.seqno.
        """
        # _when_done assigns and returns the seqno under the same lock.
        # self._callbacks_seqno is an int, so the partial curries a copy.
        with self._rlock:
            return self._when_done(
                fn=functools.partial(
                    _callback_wrapper, self._callbacks_seqno, fn
                ),
                args=args,
                kwargs=kwargs,
                priority=_PRIORITY_USER,
            )

    def _when_done(
        self,
        fn: Callable,
        priority: int,
        args: tuple[Any, ...] = (),
        kwargs: Optional[Mapping[str, Any]] = None,
    ) -> int:
        """Register a callback with some priority, returning its seqno."""
        with self._rlock:
            seqno = self._callbacks_seqno
            heapq.heappush(
                self._callbacks,
                (
                    priority,
                    seqno,
                    fn,
                    args,
                    {} if kwargs is None else kwargs,
                ),
            )
            self._callbacks_seqno += 1
            if self._connection is None:
                self._issue_callbacks()
            return seqno

    def done(self, timeout: Optional[float] = 0) -> bool:
        """
        Never raising Blocked, returns True if result(timeout=0) must succeed.

        Returns whether completion can be confirmed within the timeout.
        Timeout is given in seconds with None meaning to block indefinitely.
        Never raises Blocked but instead returns False on unavailable result.

        May raise CallbackRaised from at most one registered callback.
        See CallbackRaised documentation for callback error semantics.
        """
        # Method done(...) is nothing but an impatient, simplified wait(...)
        # Both done(...) and wait(...) in API because (a) concurrent.futures
        # API anchored peoples' expectations that done() non-blocking by
        # default and because (b) wait(...) more natural for signaling/joining.
        return self.wait(timeout=timeout)  # Notice default is 0

    def wait(
        self,
        timeout: Optional[float] = None,
        *,
        signal: Union[None, int, signal.Signals] = None,
    ) -> bool:
        """
        Waiting at most timeout, return True if result(timeout=0) must succeed.

        First, sends any provided signal to any underlying, running process.
        Second, returns True once the result pipe yields a result or closes
        without one. The work is not done until the pipe is closed.
        Timeout is given in seconds with None meaning to block indefinitely.
        Never raises Blocked but instead returns False on unavailable result.

        Allows, e.g., a SIGTERM followed by waiting 1 second for termination.
        A signal need not force termination, e.g. SIGUSR1 or SIGCONT.
        Caution: SIGSTOP suspends the child indefinitely; the Future
        cannot complete until a subsequent wait(signal=SIGCONT).

        May raise CallbackRaised from at most one registered callback.
        See CallbackRaised documentation for callback error semantics.
        """
        # Deadline computed before lock so acquisition time is deducted
        deadline = timeout_to_deadline(timeout)

        # Acquire the lock, respecting the caller's timeout budget
        if not self._rlock.acquire(timeout=deadline_to_timeout(deadline)):
            return False

        try:
            # Multiple calls may be required to issue all callbacks
            if self._connection is None:
                self._issue_callbacks()
                return True

            # Optionally, send any provided signal to the underlying process
            # Invalid signal numbers, e.g., manifest as general OSErrors
            # so only silently ignore the race-driven ProcessLookupError.
            if self._process and self._process.pid and signal is not None:
                try:
                    os.kill(self._process.pid, signal)
                except ProcessLookupError:
                    pass

            # The result pipe is the contract: a readable connection has data
            # or has closed without one. Wait on it, not on process exit.
            assert self._process is not None
            if not wait(
                [self._connection],
                timeout=deadline_to_timeout(deadline),
            ):
                return False

            # The connection is readable, so recv() will not block past the
            # deadline.  EOFError is the contract closing without a result.
            try:
                self._wrapper = self._connection.recv()
            except EOFError:
                self._wrapper = ExceptionWrapper(LostResult())
            except Exception as e:
                # A value can pickle in the child yet fail to reconstitute
                # in the parent (e.g. a returned Connection raises
                # FileNotFoundError deep in recv()).
                self._wrapper = ExceptionWrapper(
                    RuntimeError(f"Result not reconstructable: {e!r}")
                )
            else:
                assert isinstance(self._wrapper, Wrapper), type(self._wrapper)

            # Now join() and set to None reclaiming OS/Python resources
            assert self._process is not None
            self._process.join()
            self._process = None

            # Drop our reference so _issue_callbacks's invariant holds
            # and __del__ will not warn.  The actual close() is deferred
            # to a _PRIORITY_BEFORE callback (see _deregister_connection)
            # so the fd is unregistered from the selector before reuse.
            self._connection = None
            self._issue_callbacks()
            return True
        finally:
            self._rlock.release()

    def _issue_callbacks(self):
        # _is_owned is expected on RLock but not guaranteed; skip if absent
        assert getattr(self._rlock, "_is_owned", object)(), "must hold _rlock"
        assert self._connection is None and self._process is None, "Invariant"

        # Avoiding nested drains converts unbounded recursion into a flat loop
        # Otherwise callbacks re-registering themselves blow out the stack
        if self._callbacks_issuing:
            return

        self._callbacks_issuing = True
        try:
            while self._callbacks:
                _, _, fn, args, kwargs = heapq.heappop(self._callbacks)
                fn(*args, **kwargs)
        finally:
            self._callbacks_issuing = False

    def result(self, timeout: Optional[float] = None) -> T:
        """
        Obtain result when ready.  Raises Blocked if result unavailable.

        Timeout is given in seconds with None meaning to block indefinitely.
        May raise CallbackRaised from at most one registered callback.
        See CallbackRaised documentation for callback error semantics.
        """
        if not self.wait(timeout):
            raise Blocked()

        assert self._wrapper is not None
        return self._wrapper.unwrap()


@dataclass(frozen=True)
class SlotsSentinel:
    """Selector data for the slots waitable; its no-op done() lets
    reclaim_resources() treat it like a Future.  Frozen, hence hashable,
    so reclaim can dedup selector data with a set instead of via id().
    """

    def done(self) -> None:
        return None


def _restore_token(slots: FixedBytesQueue, token: Optional[bytes]) -> None:
    """Restore token to slots, tolerating a closed queue."""
    if token is not None:
        try:
            slots.put(token)
        except ValueError:
            pass  # Queue closed; Jobserver is shutting down


def _deregister_sentinel(
    selector: DefaultSelector,
    sentinel: int,
    _process: BaseProcess,
) -> None:
    """Unregister sentinel from selector, tolerating prior close."""
    # Argument _process is unused but required to prevent garbage
    # collection of the Process until after this callback fires.
    try:
        selector.unregister(sentinel)
    except KeyError:
        pass  # Already unregistered or selector closed


def _deregister_connection(
    selector: DefaultSelector,
    connection: Connection,
) -> None:
    """Unregister connection from selector then close it.

    Mirrors the sentinel discipline: the connection fd is kept open (this
    callback holds the only remaining reference) until after unregister so
    its integer cannot be reused by a concurrent submit()'s new Pipe, which
    would otherwise collide as an already-registered fd.  Future.wait()
    therefore defers the close here rather than closing inline.
    """
    try:
        selector.unregister(connection)
    except KeyError:
        pass  # Already unregistered or selector closed
    connection.close()


@final
class Resources:
    """
    The slots and selector shared by a Jobserver and its variants.

    A slot is one unit of process concurrency, roughly one running worker.
    Slots default to the number of CPUs available to the current process.

    A Jobserver is a thin handle over a Resources instance; its replace_*(...)
    methods produce sibling handles sharing the same Resources.  Teardown is
    reference-counted across context-manager entries.  These resources are
    closed only when the last open with-block exits or, failing that, when the
    final handle is finalized.
    """

    __slots__ = (
        "_context",
        "_slots",
        "_selector",
        "_selector_pid",
        "_selector_closed",
        "_refcount",
    )

    def __init__(
        self,
        context: Union[None, str, BaseContext] = None,
        slots: Optional[int] = None,
    ) -> None:
        """
        Wrap some multiprocessing context and allow some number of slots.

        When not provided, context defaults to multiprocessing.get_context().
        When not provided, slots defaults to len(os.sched_getaffinity(0))
        which reports the number of usable CPUs for the current process.
        """
        # Validate arguments before allocating OS resources so that
        # invalid inputs cannot leave pipes or semaphores behind.
        if slots is None:
            slots = sched_getaffinity0()
        if isinstance(slots, bool) or not isinstance(slots, int):
            raise TypeError(f"slots: int, got {type(slots).__name__}")
        if slots < 1:
            raise ValueError(f"slots must be >= 1, got {slots!r}")
        # All tokens are written in one atomic put(), which FixedBytesQueue
        # bounds at pipe_buf() bytes; one byte per slot caps slots there too.
        if slots > pipe_buf():
            raise ValueError(f"slots must be <= {pipe_buf()}, got {slots!r}")

        # Obtain some multiprocessing Context and the slot-tracking queue
        self._context = resolve_context(context)
        self._slots = FixedBytesQueue(self._context, fixedlen=1)

        # One single-byte token per slot, written in one atomic put().  The
        # byte value is opaque, so "J" (for Jobserver) is as good as any.
        self._slots.put(b"J" * slots)

        # Tracks outstanding Futures via their process sentinels.  Built
        # lazily by lazy_selector(); None means "not yet built" and
        # _selector_pid stamps the building process so forks rebuild it.
        self._selector: Optional[DefaultSelector] = None
        self._selector_pid: Optional[int] = None
        self._selector_closed = False

        # Number of open with-blocks across handles sharing these resources.
        self._refcount = 0

    def __getstate__(self) -> tuple:
        """
        Get Resources state without exposing in-flight Futures.

        Only capture the context and slots so an instance can travel to a
        sub-Process; Futures can be neither copied nor pickled.
        """
        return (self._context, self._slots)

    def __setstate__(self, state: tuple) -> None:
        """
        Set Resources state, deferring selector construction to first use.
        """
        assert isinstance(state, tuple) and len(state) == 2
        self._context, self._slots = state
        self._selector = None
        self._selector_pid = None
        self._selector_closed = False
        self._refcount = 0

    def tracked(self) -> int:
        """
        Futures in the selector, excluding any slots entry.

        Tolerates partial construction (returns 0 when __init__ raised
        before _selector was assigned) and a closed selector (get_map()
        returns None once close() has run).
        """
        selector = getattr(self, "_selector", None)
        if selector is None:
            return 0
        sm = selector.get_map()  # None once the selector is closed
        if not sm:
            return 0
        # Each Future registers two fds (sentinel and connection) beyond
        # the lone self._slots.waitable() entry.  Approximate while a
        # completing Future is transiently half-unregistered.
        return (len(sm) - 1) // 2

    def __repr__(self) -> str:
        method = self._context.get_start_method()
        return f"Resources({method!r}, tracked={self.tracked()})"

    def __del__(self) -> None:
        """
        Emit ResourceWarning if any Futures are still running.

        With no undone Futures these resources are implicitly closed upon
        finalization; with running Futures a ResourceWarning is emitted.
        """
        # tracked() tolerates partial construction; hasattr guards
        # the remaining resource accesses.
        if (tracked := self.tracked()) > 0:
            warnings.warn(
                f"Finalizing {self!r} with {tracked} running Futures",
                ResourceWarning,
                stacklevel=2,
                source=self,
            )
        if hasattr(self, "_slots"):
            self._slots.close_put()
            self._slots.close_get()
        # Matches the cleanup in _teardown(), tolerating partial construction.
        self._selector_close()

    def _selector_close(self) -> None:
        # Record closure on _selector_closed because a closed selector
        # cannot be detected after the fact: its get_map() can spuriously
        # report open in a forked child under garbage collection.  Forks
        # inherit the flag, so lazy_selector checks it before its fork
        # rebuild and a closed fork raises a clean RuntimeError rather than
        # tripping self._slots.waitable().
        selector = getattr(self, "_selector", None)
        if selector is not None:
            selector.close()
            self._selector_closed = True

    def __enter__(self) -> "Resources":
        # Preparing the selector confirms these resources are not closed.
        # Counting the entry lets a nested with-block on a sibling handle exit
        # without closing resources the outer block still needs.
        self.lazy_selector()
        self._refcount += 1
        return self

    def __exit__(self, *exc: Any) -> None:
        # Balance one __enter__(); tear down only once the last scope exits.
        self._refcount -= 1
        if self._refcount <= 0:
            self._teardown()

    def _teardown(self) -> None:
        """
        Clean up slots and drain ready callbacks.

        Never raises CallbackRaised.  Calls reclaim() repeatedly until
        every ready callback has been attempted.  Each suppressed
        CallbackRaised emits a RuntimeWarning.
        """
        # Idempotent: once closed, reclaim() below would raise.
        if self._selector_closed:
            return
        # Each call drains at most one CallbackRaised per future
        while True:
            try:
                self.reclaim()
                break
            except CallbackRaised as e:
                warnings.warn(
                    "Jobserver teardown suppressed"
                    f" CallbackRaised with cause: {e.__cause__!r}",
                    RuntimeWarning,
                    stacklevel=2,
                    source=self,
                )
        # Finally, stop any further manipulation of slots
        self._slots.close_put()
        self._slots.close_get()
        self._selector_close()

    def lazy_selector(self) -> DefaultSelector:
        """
        Create-or-return the selector for the current process.

        Built on first use (None), rebuilt after a fork (pid change): a
        child reusing an ancestor's selector would join()/is_alive() the
        ancestor's Futures and share its epoll set, so it is discarded.
        """
        # Closed via _teardown()/__del__ here or in an ancestor before fork.
        if self._selector_closed:
            raise RuntimeError("Jobserver is closed")
        # Build on first use, or rebuild after a fork (pid change) to discard
        # an ancestor's selector: reusing it would join()/is_alive() the
        # ancestor's Futures and share its epoll set.
        if self._selector is None or self._selector_pid != os.getpid():
            # Pre-register the slots waitable so the obtain-token loop and
            # reclaim share one persistent interest set (a noop done() keeps
            # it indistinguishable from a Future entry during select()).
            self._selector = DefaultSelector()
            self._selector.register(
                self._slots.waitable(),
                EVENT_READ,
                data=SlotsSentinel(),
            )
            self._selector_pid = os.getpid()
        return self._selector

    def reclaim(self) -> None:
        """
        Reclaim resources for completed submissions and issue callbacks.

        May raise CallbackRaised from at most one registered callback.
        See CallbackRaised documentation for callback error semantics.
        """
        # O(k) where k = newly completed futures.  The persistent
        # selector maintains the interest set incrementally so no
        # O(N) rebuild occurs on each call.  select(timeout=0) is
        # a non-blocking poll returning only the k ready entries.
        # done() triggers callbacks mutating the selector.
        #
        # A Future's two fds (connection and sentinel) share one data and
        # are often ready together; the set collapses them so done() runs
        # once, and it materializes before any done() mutates the selector.
        ready = self.lazy_selector().select(timeout=0)
        for data in {key.data for key, _ in ready}:
            assert hasattr(data, "done"), type(data)
            data.done()

    @property
    def context(self) -> BaseContext:
        return self._context


# Serves as the default preexec and sleep so Jobserver need not special-case
def noop(*args, **kwargs) -> None:
    """A "do nothing" function conforming to (the rejected) PEP-559."""
    return None


@final
class Jobserver:
    """
    A Jobserver exposing a Future interface built atop multiprocessing.

    A slot is one unit of process concurrency, roughly one running worker.
    Slots default to the number of CPUs available to the current process.

    A Jobserver is a thin handle over a shared set of slots.  The env,
    preexec, and sleep submission controls are adjusted via modify_env(...),
    replace_preexec(...), and replace_sleep(...), each returning a new
    Jobserver sharing this one's slots.

    Concurrent submit() / reclaim_resources() calls on a Jobserver are not
    thread-safe.  In contrast, returned Futures are thread-safe.  Sibling
    handles are not independent.  They reach the same slots and selector, so
    the same constraint spans every one.

    Workers are non-daemon so they can spawn children; consequently a hard
    parent crash orphans running workers rather than terminating them.
    OS features, like PR_SET_PDEATHSIG on Linux, can force termination.

    Do not provide untrusted arguments to submit().  Under spawn/forkserver,
    everything sent to a child is pickled: fn, args/kwargs, env values, and
    preexec.  Lambdas and local closures are unpicklable and so work only
    under fork.
    """

    __slots__ = ("_resources", "_env", "_preexec", "_sleep")

    _resources: Resources
    _env: dict[str, Optional[str]]
    _preexec: Callable
    _sleep: Callable

    def __init__(
        self,
        context: Union[None, str, BaseContext] = None,
        slots: Optional[int] = None,
    ) -> None:
        """
        Wrap some multiprocessing context and allow some number of slots.

        When not provided, context defaults to multiprocessing.get_context().
        When not provided, slots defaults to len(os.sched_getaffinity(0))
        which reports the number of usable CPUs for the current process.

        Submissions default to an empty env and no-op preexec and sleep; use
        modify_env(...), replace_preexec(...), and replace_sleep(...) to
        derive a handle with different submission controls.
        """
        self._resources = Resources(context, slots)
        self._env = {}
        self._preexec = noop
        self._sleep = noop

    def __getstate__(self) -> tuple:
        """
        Get instance state without exposing in-flight Futures.

        Only capture the shared Resources and controls to allow nesting.
        """
        # Required because Futures can be neither copied nor pickled
        # Without custom handling of Futures, submit(...) would fail
        # whenever an instance is part of an argument to a sub-Process
        return (self._resources, self._env, self._preexec, self._sleep)

    def __setstate__(self, state: tuple) -> None:
        """Set instance state."""
        assert isinstance(state, tuple) and len(state) == 4
        self._resources, self._env, self._preexec, self._sleep = state

    # Use typing.Self once Python 3.11 is the minimum version
    def __copy__(self) -> "Jobserver":
        """Return a sibling handle sharing this Jobserver's slots."""
        # A copy is another handle onto the same Resources with identical
        # submission controls; modify_env() and replace_*() build on this.
        other = Jobserver.__new__(Jobserver)
        other._resources = self._resources
        other._env = self._env
        other._preexec = self._preexec
        other._sleep = self._sleep
        return other

    def __deepcopy__(self, _: Any) -> "Jobserver":
        """Return a sibling handle sharing this Jobserver's slots."""
        # The shared Resources cannot be duplicated, so a deep copy shares
        # it exactly as a shallow copy does.
        return self.__copy__()

    def modify_env(
        self,
        additions: Union[
            Mapping[str, Optional[str]],
            Iterable[tuple[str, Optional[str]]],
        ],
        /,
    ) -> "Jobserver":
        """
        Return a Jobserver whose child also applies additions to os.environ.

        Additions is a Mapping of names to values or an iterable of such
        pairs.  They merge onto any env already set on this Jobserver,
        overriding entries for the same key; a None value unsets the name,
        including one set by an earlier modify_env(...).  Shares this
        Jobserver's slots.
        """
        items = (
            additions.items() if isinstance(additions, Mapping) else additions
        )
        env = dict(self._env)  # Merge onto a copy of the prior env
        for key, value in items:
            if not isinstance(key, str):
                raise TypeError(
                    f"env key {key!r}: must be str, got {type(key).__name__}"
                )
            if value is not None and not isinstance(value, str):
                raise TypeError(
                    f"env key {key!r}: value must be str"
                    f" or None, got {type(value).__name__}"
                )
            env[key] = value

        other = self.__copy__()
        other._env = env
        return other

    def replace_preexec(
        self,
        replacement: Callable[[], Union[None, AbstractContextManager]],
        /,
    ) -> "Jobserver":
        """
        Return a Jobserver invoking replacement in the child just before fn.

        replacement() may return None (a plain pre-execution hook) or a
        context manager that wraps fn execution with entry/exit semantics, so
        fn runs inside it.  A context manager's __exit__ may suppress
        exceptions from fn, in which case the result is None.  Shares this
        Jobserver's slots.
        """
        if callable(replacement):
            other = self.__copy__()
            other._preexec = replacement
            return other
        raise TypeError(
            f"preexec must be callable, got {type(replacement).__name__}"
        )

    def replace_sleep(
        self,
        replacement: Callable[[], Optional[float]],
        /,
    ) -> "Jobserver":
        """
        Return a Jobserver gating work acceptance via replacement.

        replacement() returns None when work is acceptable or a non-negative
        number of seconds for which this process should sleep before retrying.
        Shares this Jobserver's slots.
        """
        if callable(replacement):
            other = self.__copy__()
            other._sleep = replacement
            return other
        raise TypeError(
            f"sleep must be callable, got {type(replacement).__name__}"
        )

    def __repr__(self) -> str:
        method = self._resources.context.get_start_method()
        tracked = self._resources.tracked()
        return f"Jobserver({method!r}, tracked={tracked})"

    def __enter__(self) -> "Jobserver":
        # Entering the shared Resources confirms it is open, counts the scope.
        self._resources.__enter__()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._resources.__exit__(*exc)

    def __call__(self, fn: Callable[..., T], *args, **kwargs) -> Future[T]:
        """
        Submit running fn(*args, **kwargs) to this Jobserver.

        Shorthand for calling submit(fn=fn, args=*args, kwargs=**kwargs),
        with all submission semantics per that method's default arguments.
        """
        return self.submit(fn=fn, args=args, kwargs=kwargs)

    @property
    def context(self) -> BaseContext:
        """Return the multiprocessing context used by this Jobserver."""
        return self._resources.context

    def reclaim_resources(self) -> None:
        """
        Reclaim resources for any completed submissions and issue callbacks.

        Method exposed for when explicit resource reclamation is desired.
        For example, when work requires locking more than just a slot and
        the paired unlock is accomplished via Future-registered callbacks.

        May raise CallbackRaised from at most one registered callback.
        See CallbackRaised documentation for callback error semantics.
        """
        self._resources.reclaim()

    def submit(
        self,
        fn: Callable[..., T],
        *,
        args: Iterable = (),
        kwargs: Mapping[str, Any] = types.MappingProxyType({}),
        consume: int = 1,
        timeout: Optional[float] = None,
    ) -> Future[T]:
        """
        Submit running fn(*args, **kwargs) to this Jobserver.

        Raises Blocked when insufficient resources available to accept work.
        Timeout is given in seconds with None meaning block indefinitely.

        When consume == 0, no job slot is consumed by the submission.
        Only consume == 0 or consume == 1 is permitted by the implementation.

        The env, preexec, and sleep submission controls are taken from this
        Jobserver; see modify_env(), replace_preexec(), and replace_sleep().

        May raise CallbackRaised from at most one registered callback
        due to a prior Future's completion.  The caller may resubmit.
        See CallbackRaised documentation for callback error semantics.
        """
        # First, check any arguments not for _maybe_obtain_token(...)
        if not callable(fn):
            raise TypeError(f"fn must be callable, got {type(fn).__name__}")
        if not isinstance(args, Iterable):
            raise TypeError(f"args: Iterable, got {type(args).__name__}")
        if not isinstance(kwargs, Mapping):
            raise TypeError(f"kwargs: Mapping, got {type(kwargs).__name__}")

        # Eagerly convert args before acquiring tokens so that malformed
        # inputs fail fast without consuming resources.  The env, preexec,
        # and sleep controls were validated when this handle was derived.
        args = tuple(args)
        # Some Mapping subclasses are not picklable so coerce if needed.
        kwargs = kwargs if isinstance(kwargs, dict) else dict(kwargs)

        # Next, either obtain requested tokens or else raise Blocked
        #
        # Choosing "reclaim=resources.reclaim"
        # may cause submit(...) to raise due to Future callbacks that raise.
        # Design-wise, these other alternatives were CONSIDERED and REJECTED:
        #
        #   (A) Silently ignore the CallbackRaised?
        #       No, client code must know about Exceptions it causes.
        #   (B) Issue callback priorities <= _PRIORITY_BEFORE?
        #       No, client used a callback because client wanted immediacy.
        #       Otherwise, the client could do its own work after done()
        #       and would not have employed callbacks in the first place.
        #   (C) Issue callbacks until the first CallbackRaised is observed
        #       then delay raising it until done() or wait() or result()?
        #       No, that would incur very messy semantics inside Future
        #       which would be (a) rare and (b) hard to reason about.
        #       If the client wants to avoid CallbackRaised the client
        #       is free to never let Exception escape from a callback.
        #
        # ASIDE: If another design is desired, instead of reclaim
        # any other method could be injected below.  Notice that the
        # callback priority mechanism does permit issuing callback subsets.
        resources = self._resources
        selector = resources.lazy_selector()
        token = _maybe_obtain_token(
            consume=consume,
            deadline=timeout_to_deadline(timeout),
            reclaim=resources.reclaim,
            selector=selector,
            sleep=self._sleep,
            slots=resources._slots,
        )

        # Then, with required slots consumed, begin consuming resources:
        recv = send = process = None
        try:
            # Grab resources for processing the submitted work
            # Why use a Pipe instead of a Queue?  Pipes can detect EOFError!
            recv, send = resources.context.Pipe(duplex=False)
            process = resources.context.Process(  # type: ignore
                target=_worker_entrypoint,
                args=(send, self._env, self._preexec, fn, args, kwargs),
                daemon=False,
                name="Jobserver-worker",
            )
            future: Future[T] = Future(process, recv)
            # TODO: better report pickling problems (e.g. preexec)
            process.start()
            send.close()

            # Register both the connection and the sentinel for O(k) polling.
            # Observing the connection reclaims a Future whose result is ready
            # while its child stays blocked mid-send() and thus unexited.
            selector.register(recv, EVENT_READ, data=future)
            selector.register(process.sentinel, EVENT_READ, data=future)
        except Exception:
            # Undo any registration that succeeded before the failure.
            # The connection registers first, so cleanup it first too.
            try:
                if recv is not None:
                    selector.unregister(recv)
                if process is not None:
                    selector.unregister(process.sentinel)
            except (KeyError, ValueError):
                pass  # Never registered or never started
            # Close pipe fds to avoid leaking until garbage collection
            if send is not None:
                send.close()
            if recv is not None:
                recv.close()
            # Unwind any consumed slot on unexpected errors
            if token is not None:
                resources._slots.put(token)
            raise

        # Unregister and close the result connection before user callbacks
        # so a raising user callback cannot leak the connection fd.
        # Holding recv keeps its fd open until unregister.
        future._when_done(
            fn=_deregister_connection,
            args=(selector, recv),
            priority=_PRIORITY_BEFORE,
        )

        # As above process.start() succeeded, now Future must restore token
        future._when_done(
            fn=_restore_token,
            args=(resources._slots, token),
            priority=_PRIORITY_BEFORE,
        )

        # The sentinel stays registered so the Future remains discoverable
        # if a raising user callback aborts the drain.  Prevent premature
        # GC of process (closing the sentinel fd) by holding a reference.
        #
        # Because _PRIORITY_AFTER is best-effort, unregistering the
        # sentinel additionally happens implicitly in __exit__ and
        # __del__ when the selector is closed.
        future._when_done(
            fn=_deregister_sentinel,
            args=(selector, process.sentinel, process),
            priority=_PRIORITY_AFTER,
        )

        # Finally, return a viable Future to the caller
        return future

    def map(
        self,
        fn: Callable[..., T],
        argses: Optional[Iterable[Iterable]] = None,
        kwargses: Optional[Iterable[Mapping[str, Any]]] = None,
        *,
        timeout: Optional[float] = None,
        chunksize: int = 1,
        buffersize: Optional[int] = None,
    ) -> Iterator[T]:
        """
        Map fn over argses and kwargses, yielding results in order.

        Each element of argses provides positional arguments and each
        element of kwargses provides keyword arguments for one call
        to fn.  Non-None argses and kwargses must have equal length.

        Input argses and kwargses are collected immediately unless
        buffersize limits the number of outstanding submissions, in
        which case they are consumed lazily as results are yielded.
        Individual args and kwargs are always collected eagerly.

        Timeout is given in seconds from this call; if a result is
        not available by the deadline, the iterator raises Blocked.
        Function calls are sent to workers in groups of chunksize.
        Consistent with concurrent.futures, all chunksize results are
        materialized in memory at once.

        Stopping iteration early neither cancels nor waits for running work.
        Slots are retained until next reclaim_resources() or __exit__.

        The env, preexec, and sleep submission controls are taken from this
        Jobserver; see modify_env(), replace_preexec(), and replace_sleep().
        """
        if chunksize < 1:
            raise ValueError("chunksize must be >= 1")
        if buffersize is not None and buffersize < 1:
            raise ValueError("buffersize must be >= 1")

        if isinstance(argses, Mapping):
            raise TypeError(
                "argses: expected an Iterable of positional-argument"
                " Iterables (e.g. a list of tuples), got"
                f" {type(argses).__name__}; iterating it would silently"
                " yield its keys"
            )
        if isinstance(kwargses, Mapping):
            raise TypeError(
                "kwargses: expected an Iterable of Mappings (e.g. a list"
                f" of dicts), got a single {type(kwargses).__name__};"
                " iterating it would silently yield its keys"
            )

        deadline = timeout_to_deadline(timeout)

        # Build a (possibly lazy) iterator of (args, kwargs) pairs
        pairs: Iterable[tuple]
        if argses is not None and kwargses is not None:
            pairs = _strict_zip(argses, kwargses)
        elif kwargses is not None:
            pairs = (((), kw) for kw in kwargses)
        else:
            pairs = ((args, {}) for args in (argses or ()))

        # Eagerly validate argses elements when collecting
        if buffersize is None:
            collected = [
                _validate_args_kwargs(n, args, kwargs)
                for n, (args, kwargs) in enumerate(pairs)
            ]
        else:
            collected = None
        return _map_generate(
            submit=self.submit,
            reclaim=self.reclaim_resources,
            fn=fn,
            pairs=pairs if collected is None else iter(collected),
            chunksize=chunksize,
            buffersize=(
                len(collected)  # type: ignore[arg-type]
                if buffersize is None
                else buffersize
            ),
            deadline=deadline,
        )


def _worker_entrypoint(send, env, preexec, fn, args, kwargs) -> None:
    """
    Entry point for workers to run fn(...) due to some submit(...).

    The env is a dict of str to Optional[str] already validated by
    modify_env; a None value unsets the name in os.environ.
    """
    ignore_sigpipe()

    # Enforce close-on-exec so exec()'d grandchildren don't inherit the pipe.
    os.set_inheritable(send.fileno(), False)

    # Wrapper usage tracks whether a value was returned or raised
    # in degenerate case where client code returns an Exception
    result: Optional[Wrapper[Any]] = None
    try:
        # None invalid in os.environ so interpret as sentinel for popping
        for key, value in env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        # preexec() may return a context manager wrapping fn execution.
        # If __exit__ suppresses an exception from fn, raw keeps its
        # pre-call value of None so the result becomes ResultWrapper(None).
        raw = None
        with ExitStack() as stack:
            cm = preexec()
            if cm is not None:
                stack.enter_context(cm)
            raw = fn(*args, **kwargs)
        result = ResultWrapper(raw)
    except Exception as exception:
        result = ExceptionWrapper(exception)
    except BaseException as base_exception:
        # When possible, send cause of death back to the parent to aid
        # users in debugging.  Raising LostResult() from the escaped
        # BaseException gives it a live traceback whose chained __cause__
        # names the SystemExit/KeyboardInterrupt for the parent to render.
        # LostResult() without __cause__ indicates this send failed,
        # e.g. due to a SIGKILL.
        try:
            raise LostResult() from base_exception
        except LostResult as lost:
            result = ExceptionWrapper(lost)
        raise  # Proceed with any BaseException-specific teardown
    finally:
        try:
            # None means result assignment never ran (e.g. an interpreter
            # teardown between except and finally); let the pipe close so
            # the parent sees EOFError -> LostResult.
            if result is not None:
                try:
                    payload = ForkingPickler.dumps(result)
                except PICKLE_DUMP_ERRORS as pe:
                    payload = ForkingPickler.dumps(
                        ExceptionWrapper(
                            RuntimeError(
                                f"{result.describe()} not picklable: {pe!r}"
                            ),
                            cause=result,
                        )
                    )
                send.send_bytes(payload)
        except OSError:
            # Broken or closed result fd (BrokenPipeError, EBADF, ...):
            # close quietly and let the parent see EOFError instead.
            pass
        try:
            send.close()
        except OSError:
            pass


_RESOLUTION = 1.0e-2


def _maybe_obtain_token(
    consume: int,
    deadline: float,
    reclaim: Callable[[], Any],
    selector: DefaultSelector,
    sleep: Callable[[], Optional[float]],
    slots: FixedBytesQueue,
) -> Optional[bytes]:
    """
    Either retrieve a requested token or raise Blocked while trying.

    Returns the token when consume == 1, or None when consume == 0.
    May raise CallbackRaised via reclaim so raising.
    """
    # Defensively check arguments
    if type(consume) is not int or consume not in (0, 1):
        raise ValueError(f"consume must be 0 or 1, got {consume!r}")
    # Acquire the requested token or raise Blocked when impossible
    token: Optional[bytes] = None
    # Ready fds the previous pass could not reclaim
    # If they recur, back off to avoid busy spinning
    stall_fds: set[int] = set()
    while True:
        # (1) Eagerly clean up any completed work to avoid deadlocks
        reclaim()

        # (2) Exit once a consume == 1 submission already holds its slot.
        if token is not None:
            break

        # (3) When sleep() vetoes new work proceed to sleep.
        # Bound the sleep by the remaining deadline so that
        # sleep() duration is properly accounted for.
        seconds = sleep()
        if seconds is not None:
            if (
                isinstance(seconds, bool)
                or not isinstance(seconds, (int, float))
                or not seconds >= 0.0
            ):
                raise ValueError(
                    "sleep must return None or non-negative "
                    f"seconds, got {seconds!r}"
                )
            remaining = deadline_to_timeout(deadline)
            time.sleep(max(_RESOLUTION, min(seconds, remaining)))
            if time.monotonic() >= deadline:
                raise Blocked()
            continue

        # (4) Gate passed; a consume == 0 submission needs no slot.
        if consume == 0:
            break

        try:
            # (5) Grab any immediately available token
            token = slots.get(timeout=0)
        except queue.Empty:
            # (6) Otherwise, possibly throw in the towel...
            monotonic = time.monotonic()
            if monotonic >= deadline:
                raise Blocked() from None

            # (7) ...then block until some interesting event.
            # O(k) via the persistent selector: lazy_selector() places
            # slots.waitable() once so only one epoll_wait is needed here.
            # No rebuilding of the interest set.
            keys_events = selector.select(timeout=deadline - monotonic)

            # (8) A sentinel wakeup normally means a child exited and the next
            # reclaim() pass will restore its slot, so loop at once.
            # But if its pipe stays open or its lock is held, reclaim cannot
            # complete it and its sentinel stays ready, so re-select()ing would
            # peg a CPU.  Back off only when a sentinel-only wakeup repeats
            # fds the prior pass failed to reclaim, bounded by the deadline.
            waitable = slots.waitable()
            ready_fds = {
                key.fd for key, _ in keys_events if key.fileobj is not waitable
            }
            only_sentinels = len(ready_fds) == len(keys_events)
            if ready_fds and only_sentinels and ready_fds.issubset(stall_fds):
                time.sleep(min(_RESOLUTION, deadline_to_timeout(deadline)))
            stall_fds = ready_fds if only_sentinels else set()

    assert token is None or consume == 1, "Postcondition"
    return token


# Removable once Python 3.10 is the oldest tested version (zip(strict=True)).
def _strict_zip(a: Iterable, b: Iterable) -> Iterator[tuple]:
    """Zip raising ValueError when the two iterables differ in length."""
    # Eagerly check lengths when both support len() so that
    # mismatches surface at call time, not mid-iteration.
    try:
        m = len(a)  # type: ignore[arg-type]
        n = len(b)  # type: ignore[arg-type]
        if m != n:
            raise ValueError(
                f"Length of argses ({m}) must match"
                f" length of kwargses ({n})"
            )
    except TypeError:
        pass
    a_it, b_it = iter(a), iter(b)
    sentinel = object()
    for a_val in a_it:
        b_val = next(b_it, sentinel)
        if b_val is sentinel:
            raise ValueError("argses and kwargses must have equal length")
        yield (a_val, b_val)
    if next(b_it, sentinel) is not sentinel:
        raise ValueError("argses and kwargses must have equal length")


def _validate_args_kwargs(n: int, args: Any, kwargs: Any) -> tuple:
    """Return (tuple(args), dict(kwargs)), raising TypeError with n."""
    try:
        args = tuple(args)
    except TypeError:
        raise TypeError(
            f"argses[{n}]: expected iterable of positional"
            f" arguments (e.g. a tuple), got"
            f" {type(args).__name__}: {args!r}"
        ) from None
    # Some Mapping subclasses are not picklable so coerce if needed.
    try:
        kwargs = kwargs if isinstance(kwargs, dict) else dict(kwargs)
    except (TypeError, ValueError):
        raise TypeError(
            f"kwargses[{n}]: expected a mapping, got"
            f" {type(kwargs).__name__}: {kwargs!r}"
        ) from None
    return (args, kwargs)


def _map_chunk(fn: Callable, chunk: tuple) -> list:
    """Execute fn(*args, **kwargs) for each (args, kwargs) in chunk."""
    # Eager list required; result must be picklable across process boundary.
    return [fn(*args, **kwargs) for args, kwargs in chunk]


def _map_generate(
    submit: Callable[..., Future[T]],
    reclaim: Callable[[], None],
    fn: Callable[..., T],
    pairs: Iterator,
    chunksize: int,
    buffersize: int,
    deadline: float,
) -> Iterator[T]:
    """Generator backing Jobserver.map() which yields results in order."""
    futures: deque[Future] = deque()  # Future[list[T]] in practice

    def _futures_append_submit(chunk: tuple) -> None:
        futures.append(
            submit(
                fn=_map_chunk,
                args=(fn, chunk),
                timeout=deadline_to_timeout(deadline),
            )
        )

    try:
        # Initial fill up to buffersize
        while len(futures) < buffersize and (
            chunk := tuple(islice(pairs, chunksize))
        ):
            _futures_append_submit(chunk)

        # Yield results, submitting replacements
        while futures:
            if chunk := tuple(islice(pairs, chunksize)):
                _futures_append_submit(chunk)
            yield from futures.popleft().result(
                timeout=deadline_to_timeout(deadline)
            )
    finally:
        # On teardown explicitly clear still-held Futures, then reclaim
        # finished workers' slots.  Loop since reclaim aborts on the first
        # CallbackRaised; a closed Jobserver (RuntimeError) or a closed
        # selector/queue (ValueError or OSError) ends the loop.
        futures.clear()
        chunk = ()
        while True:
            try:
                reclaim()
                break
            except CallbackRaised:
                continue
            except (RuntimeError, ValueError, OSError):
                break
