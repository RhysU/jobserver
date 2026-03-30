# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Implementation of the Jobserver and related classes."""

import abc
import concurrent.futures
import heapq
import os
import queue
import signal
import threading
import time
import types
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping
from itertools import islice, zip_longest

# Implementation depends upon an explicit subset of multiprocessing
from multiprocessing.connection import Connection, wait
from multiprocessing.context import BaseContext
from multiprocessing.process import BaseProcess
from typing import Any, Generic, NoReturn, Optional, TypeVar, Union

from ._compat import ignore_sigpipe, sched_getaffinity0
from ._queue import (
    MinimalQueue,
    absolute_deadline,
    relative_timeout,
    resolve_context,
)

__all__ = (
    "Blocked",
    "CallbackRaised",
    "Future",
    "Jobserver",
    "MinimalQueue",
    "SubmissionDied",
)

T = TypeVar("T")


class Blocked(Exception):
    """Reports that Jobserver.submit(...) or Future.result(...) is blocked."""

    pass


class CallbackRaised(Exception):
    """
    Reports an Exception raised from callbacks registered with a Future.

    Instances of this type must have non-None __cause__ members (see PEP 3154).
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
        raise NotImplementedError()


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


class ExceptionWrapper(Wrapper[T]):
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
        p = self._process
        return (
            f"Future({'done' if self._connection is None else 'running'}"
            f", callbacks={len(self._callbacks)}"
            f", pid={None if p is None else p.pid})"
        )

    def __copy__(self) -> NoReturn:
        """Disallow copying as duplicates cannot sensibly share resources."""
        # In particular, which copy would call self._process.join()?
        raise NotImplementedError("Futures cannot be copied.")

    def __reduce__(self) -> NoReturn:
        """Disallow pickling as duplicates cannot sensibly share resources."""
        # In particular, because pickles create copies
        raise NotImplementedError("Futures cannot be pickled.")

    def when_done(
        self,
        fn: Callable,
        *args,
        __internal: bool = False,
        __priority: int = _PRIORITY_USER,
        **kwargs,
    ) -> None:
        """
        Register a function for execution sometime after Future.done(...).

        When already done(...) the requested function is immediately invoked.
        Registered callback functions can accept a Future as an argument.
        May raise CallbackRaised from at most this new callback.
        """
        with self._rlock:
            heapq.heappush(
                self._callbacks,
                (
                    __priority,
                    self._callback_seqno,
                    __internal,
                    fn,
                    args,
                    kwargs,
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
        A signal need not force termination, e.g. SIGUSR1 / SIGSTOP / SIGCONT.

        May raise CallbackRaised from at most one registered callback.
        See CallbackRaised documentation for callback error semantics.
        """
        # Deadline computed before lock so acquisition time is deducted
        deadline = absolute_deadline(timeout)

        # Acquire the lock, respecting the caller's timeout budget
        if not self._rlock.acquire(timeout=relative_timeout(deadline)):
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
                timeout=relative_timeout(deadline),
            )
            # Sentinel ready implies process exited; trust it over is_alive()
            # because Linux closes fds before marking the process as a zombie.
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

            # Should close() throw notice it will never be retried
            connection, self._connection = self._connection, None
            connection.close()
            self._issue_callbacks()
            return True
        finally:
            self._rlock.release()

    def _issue_callbacks(self):
        # Only a non-internal callback may cause CallbackRaised
        # Otherwise, we might obfuscate bugs within this module's logic
        assert self._connection is None and self._process is None, "Invariant"
        while self._callbacks:
            _, _, internal, fn, args, kwargs = heapq.heappop(self._callbacks)
            if internal:
                fn(*args, **kwargs)
            else:
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


def _restore_tokens(slots: MinimalQueue, tokens: list) -> None:
    """Return slot tokens to the queue, tolerating a closed queue."""
    try:
        slots.put(*tokens)
    except ValueError:
        pass  # Queue closed; Jobserver is shutting down


class Jobserver:
    """A Jobserver exposing a Future interface built atop multiprocessing."""

    __slots__ = (
        "_context",
        "_slots",
        "_future_sentinels",
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
        preexec_fn: Callable[[], None] = noop,
        sleep_fn: Callable[[], Optional[float]] = noop,
    ) -> None:
        """
        Wrap some multiprocessing context and allow some number of slots.

        When not provided, context defaults to multiprocessing.get_context().
        When not provided, slots defaults to len(os.sched_getaffinity(0))
        which reports the number of usable CPUs for the current process.

        The env, preexec_fn, and sleep_fn parameters set instance-level
        defaults for submit(...).
        """
        # Obtain some multiprocessing Context and the slot-tracking queue
        self._context = resolve_context(context)
        self._slots: MinimalQueue[int] = MinimalQueue(self._context)

        # Issue one token for each requested slot
        if slots is None:
            slots = sched_getaffinity0()
        assert isinstance(slots, int) and slots >= 1, type(slots)
        self._slots.put(*range(slots))

        # Tracks outstanding Futures (and wait-able sentinels)
        self._future_sentinels: dict[Future, int] = {}

        # Instance-level defaults for submit(...)
        # Defensive copy: consume any one-shot iterable and guard against
        # mutation of the caller's container after __init__ returns.
        # Mappings expose .items(); plain iterables are already pairs.
        items = env.items() if isinstance(env, Mapping) else env
        self._env = tuple(items)
        self._preexec_fn = preexec_fn
        self._sleep_fn = sleep_fn

    def __repr__(self) -> str:
        method = self._context.get_start_method()
        n = len(self._future_sentinels)
        return f"Jobserver({method!r}, tracked={n})"

    def __enter__(self) -> "Jobserver":
        return self

    def __exit__(self, *exc: Any) -> None:
        """Clean up slots and drain all callbacks.

        Never raises CallbackRaised.  Calls reclaim_resources()
        repeatedly until every registered callback has been attempted.
        """
        # Each call drains at most one CallbackRaised per future
        while True:
            try:
                self.reclaim_resources()
            except CallbackRaised:
                continue
            break
        # Allow any still-incomplete futures to be garbage collected
        self._future_sentinels.clear()
        self._slots.close_put()
        self._slots.close_get()

    def __getstate__(self) -> tuple:
        """Get instance state without exposing in-flight Futures."""
        # Required because Futures can be neither copied nor pickled
        # Without custom handling of Futures, submit(...) would fail
        # whenever an instance is part of an argument to a sub-Process
        return (
            self._context,
            self._slots,
            {},
            self._env,
            self._preexec_fn,
            self._sleep_fn,
        )

    def __setstate__(self, state: tuple) -> None:
        """Set instance state."""
        assert isinstance(state, tuple) and len(state) == 6
        (
            self._context,
            self._slots,
            self._future_sentinels,
            self._env,
            self._preexec_fn,
            self._sleep_fn,
        ) = state

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
        # Copy of keys() required to prevent concurrent modification
        for future in tuple(self._future_sentinels.keys()):
            future.done()

    def submit(
        self,
        fn: Callable[..., T],
        *,
        args: Iterable = (),
        kwargs: Mapping[str, Any] = types.MappingProxyType({}),
        callbacks: bool = True,
        consume: int = 1,
        env: Union[
            None,
            Mapping[str, Optional[str]],
            Iterable[tuple[str, Optional[str]]],
        ] = None,
        preexec_fn: Optional[Callable[[], None]] = None,  # None: use default
        sleep_fn: Optional[  # None uses instance default
            Callable[[], Optional[float]]
        ] = None,
        timeout: Optional[float] = None,
    ) -> Future[T]:
        """Submit running fn(*args, **kwargs) to this Jobserver.

        Raises Blocked when insufficient resources available to accept work.
        This method issues callbacks on completed work when callbacks is True.
        Timeout is given in seconds with None meaning block indefinitely.

        When consume == 0, no job slot is consumed by the submission.
        Only consume == 0 or consume == 1 is permitted by the implementation.
        When env provided, child updates os.environ unsetting None-valued keys.
        When preexec_fn provided, child calls it just before fn(...).

        Optional sleep_fn() permits injecting additional logic as
        to when a slot may be consumed.  For example, one can accept work
        only when sufficient RAM is available.  Function sleep_fn()
        should either return None when work is acceptable or return the
        non-negative number of seconds for which this process should sleep.

        For env, preexec_fn, and sleep_fn non-None values override any
        instance defaults.
        """
        # First, check any arguments not for _obtain_tokens(...)
        assert fn is not None
        assert isinstance(args, Iterable), type(args)
        assert isinstance(kwargs, Mapping), type(kwargs)
        assert isinstance(callbacks, bool), type(callbacks)

        # Resolve None to the instance-level default for each optional param
        env = self._env if env is None else env
        preexec_fn = self._preexec_fn if preexec_fn is None else preexec_fn
        sleep_fn = self._sleep_fn if sleep_fn is None else sleep_fn

        assert isinstance(env, Iterable), type(env)
        assert preexec_fn is not None

        # Next, either obtain requested tokens or else raise Blocked
        # Work submission only reclaims tokens when callbacks are enabled
        reclaim_tokens_fn = self.reclaim_resources if callbacks else noop
        tokens = _obtain_tokens(
            consume=consume,
            deadline=absolute_deadline(timeout),
            reclaim_tokens_fn=reclaim_tokens_fn,
            sentinels_fn=self._future_sentinels.values,
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
                args=((send, dict(env), preexec_fn, fn) + tuple(args)),
                kwargs=kwargs,
                daemon=False,
                name="Jobserver-worker",
            )
            future: Future[T] = Future(process, recv)
            process.start()
            send.close()

            # Prepare to track the Future and the wait(...)-able sentinel
            self._future_sentinels[future] = process.sentinel
        except Exception:
            # Close pipe fds to avoid leaking until GC
            if send is not None:
                send.close()
            if recv is not None:
                recv.close()
            # Unwinding any consumed slots on unexpected errors
            while tokens:
                self._slots.put(tokens.pop())
            raise

        # As above process.start() succeeded, now Future must restore tokens
        # After any restoration, no longer track this Future within Jobserver
        if tokens:
            future.when_done(
                _restore_tokens,
                self._slots,
                tokens,
                _Future__internal=True,
                _Future__priority=_PRIORITY_TOKEN,
            )
        future.when_done(
            self._future_sentinels.pop,
            future,
            None,
            _Future__internal=True,
            _Future__priority=_PRIORITY_CLEANUP,
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
        preexec_fn: Optional[Callable[[], None]] = None,  # None: use default
        sleep_fn: Optional[  # None uses instance default
            Callable[[], Optional[float]]
        ] = None,
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
        When preexec_fn provided, child calls it just before fn(...).

        Optional sleep_fn() permits injecting additional logic as
        to when a slot may be consumed.

        For env, preexec_fn, and sleep_fn non-None values override any
        instance defaults.
        """
        if chunksize < 1:
            raise ValueError("chunksize must be >= 1")
        if buffersize is not None and buffersize < 1:
            raise ValueError("buffersize must be >= 1")

        deadline = absolute_deadline(timeout)

        # Build a (possibly lazy) iterator of (args, kwargs) pairs
        pairs: Iterable[tuple]
        if argses is not None and kwargses is not None:
            pairs = _strict_zip(argses, kwargses)
        elif kwargses is not None:
            pairs = zip_longest((), kwargses, fillvalue=())
        else:
            pairs = zip_longest(argses or (), (), fillvalue={})

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
    """Entry point for workers to fun fn(...) due to some  submit(...)."""
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
        preexec_fn()
        result = ResultWrapper(fn(*args, **kwargs))
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
        send.close()


def _obtain_tokens(
    consume: int,
    deadline: float,
    reclaim_tokens_fn: Callable[[], Any],
    sentinels_fn: Callable[[], Iterable[int]],
    sleep_fn: Callable[[], Optional[float]],
    slots: MinimalQueue[int],
    *,
    resolution: float = 1.0e-2,
) -> list[int]:
    """Either retrieve requested tokens or raise Blocked while trying."""
    # Defensively check arguments
    assert consume == 0 or consume == 1, "Invalid or deadlock possible"
    assert deadline > 0.0

    # Acquire the requested retval or raise Blocked when impossible
    retval: list[int] = []
    while True:

        # (1) Eagerly clean up any completed work to avoid deadlocks
        reclaim_tokens_fn()

        # (2) Exit loop if all requested resources have been acquired
        if len(retval) >= consume:
            break

        # (3) When sleep_fn() vetoes new work proceed to sleep
        sleep = sleep_fn()
        monotonic = time.monotonic()
        if sleep is not None:
            assert sleep >= 0.0
            time.sleep(max(resolution, min(sleep, deadline - monotonic)))
            if monotonic >= deadline:
                raise Blocked()
            continue

        try:
            # (4) Grab any immediately available token
            retval.append(slots.get(timeout=0))
        except queue.Empty:
            # (5) Otherwise, possibly throw in the towel...
            monotonic = time.monotonic()
            if monotonic >= deadline:
                raise Blocked() from None

            # (6) ...then block until some interesting event.
            wait(
                tuple(sentinels_fn())  # "Child" result
                + (slots.waitable(),),  # "Grandchild" restores token
                timeout=deadline - monotonic,
            )

    assert len(retval) == consume, "Postcondition"
    return retval


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
    preexec_fn: Optional[Callable[[], None]] = None,
    sleep_fn: Optional[Callable[[], Optional[float]]] = None,
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
