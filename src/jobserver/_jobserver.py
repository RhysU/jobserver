# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Implementation of the Jobserver and related classes."""

import abc
import concurrent.futures
import functools
import heapq
import os
import queue
import signal
import threading
import time
import types
import warnings
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import AbstractContextManager, ExitStack
from itertools import islice

# Implementation depends upon an explicit subset of multiprocessing
from multiprocessing.connection import Connection, wait
from multiprocessing.context import BaseContext
from multiprocessing.process import BaseProcess
from selectors import EVENT_READ, DefaultSelector, SelectorKey
from typing import Any, Generic, NoReturn, Optional, TypeVar, Union

from ._compat import ignore_sigpipe, sched_getaffinity0
from ._queue import (
    MinimalQueue,
    deadline_to_timeout,
    resolve_context,
    timeout_to_deadline,
)

__all__ = (
    "Blocked",
    "CallbackRaised",
    "Future",
    "Jobserver",
    "SubmissionDied",
)

T = TypeVar("T")

# preexec_fn may return None (plain setup) or a context manager
# that wraps fn execution, providing entry/exit semantics.
PreexecFn = Callable[[], Union[None, AbstractContextManager]]
# sleep_fn returns None when work is acceptable or a non-negative
# number of seconds for which the process should sleep before retrying.
SleepFn = Callable[[], Optional[float]]


class Blocked(Exception):
    """Reports that Jobserver.submit(...) or Future.result(...) is blocked."""

    pass


class CallbackRaised(Exception):
    """
    Reports an Exception raised from callbacks registered with a Future.

    Instances of this type must have non-None __cause__ members (see PEP 3134).
    The __cause__ member will be the Exception raised by client code.

    When raised by some method, e.g. by Future.done(...), Future.wait(...),
    or Future.result(...), the caller MAY choose to re-invoke that same
    method immediately to continue processing any additional callbacks.
    If the caller requires that all callbacks are attempted, the caller
    MUST re-invoke the same method until no CallbackRaised occurs.
    These MAY/MUST semantics allow the caller to decide how much additional
    processing to perform after seeing the 1st, 2nd, or Nth error.
    """

    pass


class SubmissionDied(Exception):
    """
    Reports a submission died for unknowable reasons, e.g. being killed.

    Work that is killed, terminated, interrupted, etc. raises this exception.
    Exactly what has transpired is not reported.  Do not attempt to recover.
    """

    pass


class Wrapper(abc.ABC, Generic[T]):
    """Allows Futures to track whether a value was raised or returned."""

    __slots__ = ()

    @abc.abstractmethod
    def unwrap(self) -> T:
        """Raise any wrapped Exception otherwise return some result."""
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


class ExceptionWrapper(Wrapper[Any]):
    """Specialization of Wrapper for when an Exception has been raised."""

    __slots__ = ("_raised",)

    def __init__(self, raised: Exception) -> None:
        """Wrap the provided Exception for use during unwrap()."""
        assert isinstance(raised, Exception), type(raised)
        self._raised = raised

    def unwrap(self) -> NoReturn:
        raise self._raised


# Callback priorities for the Future heapq-based callback queue.
# Token restoration fires first, user callbacks in the middle,
# and sentinel cleanup fires last.
_PRIORITY_TOKEN = 0
_PRIORITY_USER = 1
_PRIORITY_CLEANUP = 2


def _callback_wrapper(fn, *args, **kwargs) -> None:
    """Call fn(*args, **kwargs) wrapping any Exception in CallbackRaised."""
    try:
        fn(*args, **kwargs)
    except Exception as e:
        # A re-entrant when_done() on an already-completed future
        # triggers a nested _issue_callbacks(); without this
        # guard the caller sees CallbackRaised wrapping another
        # CallbackRaised instead of one layer around the cause.
        if isinstance(e, CallbackRaised):
            raise
        raise CallbackRaised() from e


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
        "_callback_seqno",
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

        # Becomes non-None after result is obtained
        self._wrapper: Optional[Wrapper[T]] = None

        # Populated by calls to when_done(...).  A heapq ordered by
        # (priority, callback_seqno, ...) so token restoration fires before
        # user callbacks, which fire before sentinel cleanup.
        self._callbacks: list[tuple] = []
        self._callback_seqno = 0  # Monotonic needed due to reentrancy

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

    def when_done(self, fn: Callable, *args: Any, **kwargs: Any) -> None:
        """
        Register fn(*args, **kwargs) for execution after Future.done(...).

        When already done(...) the requested function is immediately invoked.
        The Future is not automatically passed; to receive it, bind it
        explicitly via args, e.g. future.when_done(cb, future).
        May raise CallbackRaised from at most this new callback.
        """
        self._when_done(
            fn=functools.partial(_callback_wrapper, fn),
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
    ) -> None:
        """Internal method for registering a callback with some priority."""
        with self._rlock:
            heapq.heappush(
                self._callbacks,
                (
                    priority,
                    self._callback_seqno,
                    fn,
                    args,
                    {} if kwargs is None else kwargs,
                ),
            )
            self._callback_seqno += 1
            if self._connection is None:
                self._issue_callbacks()

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
        Second, returns True if process completion confirmed by the timeout.
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

            # Wait for either pipe data or process death
            assert self._process is not None
            ready = wait(
                [self._connection, self._process.sentinel],
                timeout=deadline_to_timeout(deadline),
            )
            # Sentinel ready implies process exited; trust it over is_alive()
            # because Linux closes fds before marking the process as a zombie
            if not ready and self._process.is_alive():
                return False

            # Attempt to read the result Wrapper from the Connection
            # EOFError is an unexpected hang up from the other end
            try:
                self._wrapper = self._connection.recv()
                assert isinstance(self._wrapper, Wrapper), type(self._wrapper)
            except EOFError:
                self._wrapper = ExceptionWrapper(SubmissionDied())

            # Now join() and set to None reclaiming OS/Python resources
            assert self._process is not None
            self._process.join()
            self._process = None

            # Should close() throw notice it can never be retried
            connection, self._connection = self._connection, None
            connection.close()
            self._issue_callbacks()
            return True
        finally:
            self._rlock.release()

    def _issue_callbacks(self):
        assert self._connection is None and self._process is None, "Invariant"
        while self._callbacks:
            _, _, fn, args, kwargs = heapq.heappop(self._callbacks)
            fn(*args, **kwargs)

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


# Appears as a default argument in Jobserver to simplify some logic therein
def noop(*args, **kwargs) -> None:
    """A "do nothing" function conforming to (the rejected) PEP-559."""
    return None


def _restore_token(slots: MinimalQueue, token: Optional[int]) -> None:
    """Restore token to slots, tolerating a closed queue."""
    if token is not None:
        try:
            slots.put(token)
        except ValueError:
            pass  # Queue closed; Jobserver is shutting down


def _unregister_sentinel(
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


def _initialize_selector(
    slots: MinimalQueue,
) -> tuple[DefaultSelector, Mapping[Any, SelectorKey]]:
    """Create a selector with slots pre-registered."""
    selector = DefaultSelector()
    # The SimpleNamespace provides the done() that
    # reclaim_resources() calls on all selector keys.
    selector.register(
        slots.waitable(),
        EVENT_READ,
        data=types.SimpleNamespace(done=noop),
    )
    return selector, selector.get_map()


class Jobserver:
    """A Jobserver exposing a Future interface built atop multiprocessing."""

    __slots__ = (
        "_context",
        "_slots",
        "_selector",
        "_selector_map",
        "_env",
        "_preexec_fn",
        "_sleep_fn",
    )

    def __init__(
        self,
        context: Union[None, str, BaseContext] = None,
        slots: Optional[int] = None,
        *,
        env: Union[
            Mapping[str, Optional[str]],
            Iterable[tuple[str, Optional[str]]],
        ] = (),
        preexec_fn: PreexecFn = noop,
        sleep_fn: SleepFn = noop,
    ) -> None:
        """
        Wrap some multiprocessing context and allow some number of slots.

        When not provided, context defaults to multiprocessing.get_context().
        When not provided, slots defaults to len(os.sched_getaffinity(0))
        which reports the number of usable CPUs for the current process.

        The env, preexec_fn, and sleep_fn parameters set instance-level
        defaults for submit(...).  See submit() for preexec_fn semantics.
        """
        # Validate arguments before allocating OS resources so that
        # invalid inputs cannot leave pipes or semaphores behind.
        if slots is None:
            slots = sched_getaffinity0()
        if not isinstance(slots, int):
            raise TypeError(f"slots: int, got {type(slots).__name__}")
        if slots < 1:
            raise ValueError(f"slots must be >= 1, got {slots!r}")

        # Obtain some multiprocessing Context and the slot-tracking queue
        self._context = resolve_context(context)
        self._slots: MinimalQueue[int] = MinimalQueue(self._context)

        # Issue one token for each requested slot
        self._slots.put(*range(slots))

        # Tracks outstanding Futures via their process sentinels.
        # _selector_map is the live view from get_map(), captured once
        # so that post-close() access returns an empty view rather
        # than the None that get_map() itself returns after close().
        self._selector, self._selector_map = _initialize_selector(self._slots)

        # Instance-level defaults for submit(...)
        # Defensive copy: consume any one-shot iterable and guard against
        # mutation of the caller's container after __init__ returns.
        # Mappings expose .items(); plain iterables are already pairs.
        items = env.items() if isinstance(env, Mapping) else env
        self._env = tuple(items)
        self._preexec_fn = preexec_fn
        self._sleep_fn = sleep_fn

    def _tracked(self) -> int:
        """Futures in the selector, excluding any slots entry.

        Safe to call on a partially-constructed instance: returns 0 when
        __init__ raised before _selector_map was assigned.
        """
        if sm := getattr(self, "_selector_map", None):
            # Omit ever-present self._slots.waitable()
            return len(sm) - 1
        return 0

    def __repr__(self) -> str:
        method = self._context.get_start_method()
        tracked = self._tracked()
        return f"Jobserver({method!r}, tracked={tracked})"

    def __del__(self) -> None:
        """Emit ResourceWarning if any Futures are still running.

        A Jobserver with no undone Futures is implicitly closed upon
        finalization; one with running Futures emits a ResourceWarning.
        """
        # _tracked() tolerates partial construction; hasattr guards
        # the remaining resource accesses.
        if (tracked := self._tracked()) > 0:
            warnings.warn(
                f"Finalizing {self!r} with {tracked} running Futures",
                ResourceWarning,
                stacklevel=2,
                source=self,
            )
        if hasattr(self, "_slots"):
            self._slots.close_put()
            self._slots.close_get()
        # Matches the cleanup in __exit__.
        if hasattr(self, "_selector"):
            self._selector.close()

    def __enter__(self) -> "Jobserver":
        return self

    def __exit__(self, *exc: Any) -> None:
        """Clean up slots and drain all callbacks.

        Never raises CallbackRaised.  Calls reclaim_resources()
        repeatedly until every ready callback has been attempted.
        Each suppressed CallbackRaised emits a RuntimeWarning.
        """
        # Each call drains at most one CallbackRaised per future
        while True:
            try:
                self.reclaim_resources()
                break
            except CallbackRaised as e:
                warnings.warn(
                    "Jobserver.__exit__ suppressed"
                    f" CallbackRaised with cause: {e.__cause__!r}",
                    RuntimeWarning,
                    stacklevel=2,
                    source=self,
                )
        # Finally, stop any further manipulation of slots
        self._slots.close_put()
        self._slots.close_get()
        # Release the selector's underlying fd and all registrations.
        # _selector_map survives close() as an empty live view.
        self._selector.close()

    def __getstate__(self) -> tuple:
        """Get instance state without exposing in-flight Futures."""
        # Required because Futures can be neither copied nor pickled
        # Without custom handling of Futures, submit(...) would fail
        # whenever an instance is part of an argument to a sub-Process
        return (
            self._context,
            self._slots,
            self._env,
            self._preexec_fn,
            self._sleep_fn,
        )

    def __setstate__(self, state: tuple) -> None:
        """Set instance state."""
        assert isinstance(state, tuple) and len(state) == 5
        (
            self._context,
            self._slots,
            self._env,
            self._preexec_fn,
            self._sleep_fn,
        ) = state
        self._selector, self._selector_map = _initialize_selector(self._slots)

    # Use typing.Self once Python 3.11 is the minimum version
    def __copy__(self) -> "Jobserver":
        """Shallow copies return the original Jobserver unchanged."""
        # Because any "copy" should and can only mutate same slots/sentinels
        return self

    def __deepcopy__(self, _: Any) -> "Jobserver":
        """Deep copies return the original Jobserver unchanged."""
        # Because any "copy" should and can only mutate same slots/sentinels
        return self

    def __call__(self, fn: Callable[..., T], *args, **kwargs) -> Future[T]:
        """Submit running fn(*args, **kwargs) to this Jobserver.

        Shorthand for calling submit(fn=fn, args=*args, kwargs=**kwargs),
        with all submission semantics per that method's default arguments.
        """
        return self.submit(fn=fn, args=args, kwargs=kwargs)

    @property
    def context(self) -> BaseContext:
        """Return the multiprocessing context used by this Jobserver."""
        return self._context

    def reclaim_resources(self) -> None:
        """
        Reclaim resources for any completed submissions and issue callbacks.

        Method exposed for when explicit resource reclamation is desired.
        For example, when work requires locking more than just a slot and
        the paired unlock is accomplished via Future-registered callbacks.

        May raise CallbackRaised from at most one registered callback.
        See CallbackRaised documentation for callback error semantics.
        """
        # O(k) where k = newly completed futures.  The persistent
        # selector maintains the interest set incrementally so no
        # O(N) rebuild occurs on each call.  select(timeout=0) is
        # a non-blocking poll returning only the k ready entries.
        # done() triggers callbacks mutating the selector.
        for key, _ in self._selector.select(timeout=0):
            assert hasattr(key.data, "done"), type(key.data)
            key.data.done()

    def submit(
        self,
        fn: Callable[..., T],
        *,
        args: Iterable = (),
        kwargs: Mapping[str, Any] = types.MappingProxyType({}),
        consume: int = 1,
        env: Union[
            None,
            Mapping[str, Optional[str]],
            Iterable[tuple[str, Optional[str]]],
        ] = None,
        preexec_fn: Optional[PreexecFn] = None,  # None: use default
        sleep_fn: Optional[SleepFn] = None,  # None: use default
        timeout: Optional[float] = None,
    ) -> Future[T]:
        """Submit running fn(*args, **kwargs) to this Jobserver.

        Raises Blocked when insufficient resources available to accept work.
        Timeout is given in seconds with None meaning block indefinitely.

        When consume == 0, no job slot is consumed by the submission.
        Only consume == 0 or consume == 1 is permitted by the implementation.
        When env provided, child updates os.environ unsetting None-valued keys.

        The preexec_fn callable is invoked in the child just before fn.
        If preexec_fn() returns a context manager, fn runs inside it;
        if it returns None, it acts as a plain pre-execution hook.
        A context manager's __exit__ may suppress exceptions from fn,
        in which case the result is None.

        Optional sleep_fn() permits injecting additional logic as
        to when a slot may be consumed.  For example, one can accept work
        only when sufficient RAM is available.  Function sleep_fn()
        should either return None when work is acceptable or return the
        non-negative number of seconds for which this process should sleep.

        For env, preexec_fn, and sleep_fn non-None values override any
        instance defaults.

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

        # Resolve None to the instance-level default for each optional param
        env = self._env if env is None else env
        preexec_fn = self._preexec_fn if preexec_fn is None else preexec_fn
        sleep_fn = self._sleep_fn if sleep_fn is None else sleep_fn

        assert preexec_fn is not None

        # Eagerly convert env and args before acquiring tokens so that
        # malformed inputs fail fast without consuming resources.
        env = _env_coerce(env)
        args = tuple(args)

        # Next, either obtain requested tokens or else raise Blocked
        #
        # Choosing "reclaim_tokens_fn=self.reclaim_resources"
        # may cause submit(...) to raise due to Future callbacks that raise.
        # Design-wise, these other alternatives were CONSIDERED and REJECTED:
        #
        #   (A) Silently ignore the CallbackRaised?
        #       No, client code must know about Exceptions it causes.
        #   (B) Issue callback priorities <= _PRIORITY_TOKEN?
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
        # ASIDE: If another design is desired, instead of reclaim_resources
        # any other method could be injected below.  Notice that the
        # callback priority mechanism does permit issuing callback subsets.
        token = _maybe_obtain_token(
            consume=consume,
            deadline=timeout_to_deadline(timeout),
            reclaim_tokens_fn=self.reclaim_resources,
            selector=self._selector,
            sleep_fn=sleep_fn,
            slots=self._slots,
        )

        # Then, with required slots consumed, begin consuming resources:
        recv = send = None
        try:
            # Grab resources for processing the submitted work
            # Why use a Pipe instead of a Queue?  Pipes can detect EOFError!
            recv, send = self._context.Pipe(duplex=False)
            process = self._context.Process(  # type: ignore
                target=_worker_entrypoint,
                args=((send, env, preexec_fn, fn) + args),
                kwargs=kwargs,
                daemon=False,
                name="Jobserver-worker",
            )
            future: Future[T] = Future(process, recv)
            process.start()
            send.close()

            # Register sentinel for O(k) polling in reclaim_resources
            self._selector.register(
                process.sentinel,
                EVENT_READ,
                data=future,
            )
        except Exception:
            # Close pipe fds to avoid leaking until garbage collection
            if send is not None:
                send.close()
            if recv is not None:
                recv.close()
            # Unwind any consumed slot on unexpected errors
            if token is not None:
                self._slots.put(token)
            raise

        # As above process.start() succeeded, now Future must restore token
        future._when_done(
            fn=_restore_token,
            args=(self._slots, token),
            priority=_PRIORITY_TOKEN,
        )

        # After token restoration, stop tracking this Future's sentinel.
        # Prevent premature garbage collection of process (which closes
        # the sentinel fd and silently removes it from epoll) by
        # holding a reference until this last-priority callback fires.
        future._when_done(
            fn=_unregister_sentinel,
            args=(self._selector, process.sentinel, process),
            priority=_PRIORITY_CLEANUP,
        )

        # Finally, return a viable Future to the caller
        return future

    def map(
        self,
        fn: Callable[..., T],
        argses: Optional[Iterable] = None,
        kwargses: Optional[Iterable] = None,
        *,
        env: Union[
            None,
            Mapping[str, Optional[str]],
            Iterable[tuple[str, Optional[str]]],
        ] = None,
        preexec_fn: Optional[PreexecFn] = None,  # None: use default
        sleep_fn: Optional[SleepFn] = None,  # None: use default
        timeout: Optional[float] = None,
        chunksize: int = 1,
        buffersize: Optional[int] = None,
    ) -> Iterator[T]:
        """Map fn over argses and kwargses, yielding results in order.

        Each element of argses provides positional arguments and each
        element of kwargses provides keyword arguments for one call
        to fn.  Non-None argses and kwargses must have equal length.

        Inputs are collected immediately unless buffersize limits
        the number of outstanding submissions, in which case inputs
        are consumed lazily as results are yielded.

        Timeout is given in seconds from this call; if a result is
        not available by the deadline, the iterator raises
        concurrent.futures.TimeoutError.  Function calls are sent to
        workers in groups of chunksize.

        When env provided, child updates os.environ unsetting None-valued keys.
        See submit() for preexec_fn semantics.

        Optional sleep_fn() permits injecting additional logic as
        to when a slot may be consumed.

        For env, preexec_fn, and sleep_fn non-None values override any
        instance defaults.
        """
        if chunksize < 1:
            raise ValueError("chunksize must be >= 1")
        if buffersize is not None and buffersize < 1:
            raise ValueError("buffersize must be >= 1")

        deadline = timeout_to_deadline(timeout)

        # Build a (possibly lazy) iterator of (args, kwargs) pairs
        pairs: Iterable[tuple]
        if argses is not None and kwargses is not None:
            pairs = _strict_zip(argses, kwargses)
        elif kwargses is not None:
            pairs = (((), kw) for kw in kwargses)
        else:
            pairs = ((args, {}) for args in (argses or ()))

        collected = list(pairs) if buffersize is None else None
        return _map_generate(
            submit=self.submit,
            fn=fn,
            pairs=pairs if collected is None else iter(collected),
            chunksize=chunksize,
            buffersize=(
                buffersize
                if buffersize is not None
                else len(collected)  # type: ignore[arg-type]
            ),
            deadline=deadline,
            env=env,
            preexec_fn=preexec_fn,
            sleep_fn=sleep_fn,
        )


def _worker_entrypoint(send, env, preexec_fn, fn, *args, **kwargs) -> None:
    """Entry point for workers to run fn(...) due to some submit(...)."""
    ignore_sigpipe()

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
        # preexec_fn() may return a context manager wrapping fn execution.
        # If __exit__ suppresses an exception from fn, raw keeps its
        # pre-call value of None so the result becomes ResultWrapper(None).
        raw = None
        with ExitStack() as stack:
            cm = preexec_fn()
            if cm is not None:
                stack.enter_context(cm)
            raw = fn(*args, **kwargs)
        result = ResultWrapper(raw)
    except Exception as exception:
        result = ExceptionWrapper(exception)
    finally:
        # Ignore broken pipes which naturally occur when the destination
        # terminates (or otherwise hangs up) before the result is ready
        try:
            # None means a BaseException (not Exception) escaped fn; let
            # the pipe close so the parent sees EOFError -> SubmissionDied.
            if result is not None:
                send.send(result)  # ValueError => object too large
        except BrokenPipeError:
            pass
        try:
            send.close()
        except BrokenPipeError:
            pass


def _env_coerce(
    env: Union[
        Mapping[str, Optional[str]],
        Iterable[tuple[str, Optional[str]]],
    ],
) -> dict[str, Optional[str]]:
    """Convert env to a dict, validating key/value types."""
    result = dict(env.items() if isinstance(env, Mapping) else env)
    for key, value in result.items():
        if not isinstance(key, str):
            raise TypeError(
                f"env key {key!r}: must be str," f" got {type(key).__name__}"
            )
        if value is not None and not isinstance(value, str):
            raise TypeError(
                f"env key {key!r}: value must be str"
                f" or None, got {type(value).__name__}"
            )
    return result


_RESOLUTION = 1.0e-2


def _maybe_obtain_token(
    consume: int,
    deadline: float,
    reclaim_tokens_fn: Callable[[], Any],
    selector: DefaultSelector,
    sleep_fn: SleepFn,
    slots: MinimalQueue[int],
) -> Optional[int]:
    """
    Either retrieve a requested token or raise Blocked while trying.

    Returns the token when consume == 1, or None when consume == 0.
    May raise CallbackRaised via reclaim_tokens_fn so raising.
    """
    # Defensively check arguments
    if consume != 0 and consume != 1:
        raise ValueError(f"consume must be 0 or 1, got {consume!r}")
    # Acquire the requested token or raise Blocked when impossible
    token: Optional[int] = None
    while True:
        # (1) Eagerly clean up any completed work to avoid deadlocks
        reclaim_tokens_fn()

        # (2) Exit loop if all requested resources have been acquired
        if consume == 0 or token is not None:
            break

        # (3) When sleep_fn() vetoes new work proceed to sleep
        sleep = sleep_fn()
        monotonic = time.monotonic()
        if sleep is not None:
            assert sleep >= 0.0
            time.sleep(max(_RESOLUTION, min(sleep, deadline - monotonic)))
            if time.monotonic() >= deadline:
                raise Blocked()
            continue

        try:
            # (4) Grab any immediately available token
            token = slots.get(timeout=0)
        except queue.Empty:
            # (5) Otherwise, possibly throw in the towel...
            monotonic = time.monotonic()
            if monotonic >= deadline:
                raise Blocked() from None

            # (6) ...then block until some interesting event.
            # O(k) via the persistent selector:
            # _initialize_selector() places slots.waitable() once
            # so only one epoll_wait
            # is needed here.  No rebuilding of the interest set.
            selector.select(timeout=deadline - monotonic)

    assert token is None or consume == 1, "Postcondition"
    return token


# Removable once Python 3.10 is the oldest tested version (zip(strict=True)).
def _strict_zip(a: Iterable, b: Iterable) -> Iterator[tuple]:
    """Zip raising ValueError when the two iterables differ in length."""
    a_it, b_it = iter(a), iter(b)
    sentinel = object()
    for a_val in a_it:
        b_val = next(b_it, sentinel)
        if b_val is sentinel:
            raise ValueError("argses and kwargses must have equal length")
        yield (a_val, b_val)
    if next(b_it, sentinel) is not sentinel:
        raise ValueError("argses and kwargses must have equal length")


def _map_chunk(fn: Callable, chunk: tuple) -> list:
    """Execute fn(*args, **kwargs) for each (args, kwargs) in chunk."""
    # Eager list required; result must be picklable across process boundary.
    return [fn(*args, **kwargs) for args, kwargs in chunk]


def _map_generate(
    submit: Callable[..., Future[T]],
    fn: Callable[..., T],
    pairs: Iterator,
    chunksize: int,
    buffersize: int,
    deadline: float,
    env: Union[
        None,
        Mapping[str, Optional[str]],
        Iterable[tuple[str, Optional[str]]],
    ] = None,
    preexec_fn: Optional[PreexecFn] = None,
    sleep_fn: Optional[SleepFn] = None,
) -> Iterator[T]:
    """Generator backing Jobserver.map() which yields results in order."""
    futures: deque[Future] = deque()  # Future[list[T]] in practice

    def _futures_append_submit(chunk: tuple) -> None:
        futures.append(
            submit(
                fn=_map_chunk,
                args=(fn, chunk),
                env=env,
                preexec_fn=preexec_fn,
                sleep_fn=sleep_fn,
                timeout=deadline - time.monotonic(),
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
                timeout=deadline - time.monotonic()
            )
    except Blocked:
        # concurrent.futures.TimeoutError (not builtin TimeoutError) so that
        # callers catching either type see it on Python < 3.11.
        raise concurrent.futures.TimeoutError() from None
