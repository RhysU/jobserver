# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Implementation of the Jobserver and related classes."""

import abc
from collections.abc import Callable, Iterable, Mapping
import os
import queue
import threading
import time
import types
from typing import Any, Generic, NoReturn, Optional, TypeVar, Union

# Implementation depends upon an explicit subset of multiprocessing
from multiprocessing.connection import Connection, wait
from multiprocessing.context import BaseContext
from multiprocessing.process import BaseProcess

from ._queue import MinimalQueue, absolute_deadline, resolve_context

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

    When raised by some method, e.g. by Future.done(...) or by
    Future.result(...), the caller MAY choose to re-invoke that same
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

        # Populated by calls to when_done(...)
        self._callbacks: list[tuple] = []

    def __copy__(self) -> NoReturn:
        """Disallow copying as duplicates cannot sensibly share resources."""
        # In particular, which copy would call self._process.join()?
        raise NotImplementedError("Futures cannot be copied.")

    def __reduce__(self) -> NoReturn:
        """Disallow pickling as duplicates cannot sensibly share resources."""
        # In particular, because pickles create copies
        raise NotImplementedError("Futures cannot be pickled.")

    def when_done(
        self, fn: Callable, *args, __internal: bool = False, **kwargs
    ) -> None:
        """
        Register a function for execution sometime after Future.done(...).

        When already done(...), will immediately invoke the requested function.
        Registered callback functions can accept a Future as an argument.
        May raise CallbackRaised from at most this new callback.
        """
        with self._rlock:
            self._callbacks.append((__internal, fn, args, kwargs))
            if self._connection is None:
                self._issue_callbacks()

    def done(self, timeout: Optional[float] = None) -> bool:
        """
        Is result ready?  Never raises Blocked instead returning False.

        Returns whether completion can be confirmed within the timeout.
        Timeout is given in seconds with None meaning to block indefinitely.
        May raise CallbackRaised from at most one registered callback.
        See CallbackRaised documentation for callback error semantics.
        """
        # Deadline computed before lock so acquisition time is deducted
        deadline = absolute_deadline(relative_timeout=timeout)

        # Acquire the lock, respecting the caller's timeout budget
        remaining = max(0, deadline - time.monotonic())
        if not self._rlock.acquire(timeout=remaining):
            return False

        try:
            # Multiple calls may be required to issue all callbacks
            if self._connection is None:
                self._issue_callbacks()
                return True

            # Possibly wait until a result is available for reading
            remaining = max(0, deadline - time.monotonic())
            if not self._connection.poll(remaining):
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
            internal, fn, args, kwargs = self._callbacks.pop(0)
            if internal:
                fn(*args, **kwargs)
            else:
                try:
                    fn(*args, **kwargs)
                except Exception as e:
                    raise CallbackRaised() from e

    def result(self, timeout: Optional[float] = None) -> T:
        """
        Obtain result when ready.  Raises Blocked if result unavailable.

        Timeout is given in seconds with None meaning to block indefinitely.
        May raise CallbackRaised from at most one registered callback.
        See CallbackRaised documentation for callback error semantics.
        """
        if not self.done(timeout):
            raise Blocked()

        assert self._wrapper is not None
        return self._wrapper.unwrap()


# Appears as a default argument in Jobserver to simplify some logic therein
def noop(*args, **kwargs) -> None:
    """A "do nothing" function conforming to (the rejected) PEP-559."""
    return None


class Jobserver:
    """A Jobserver exposing a Future interface built atop multiprocessing."""

    __slots__ = ("_context", "_slots", "_future_sentinels")

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
        # Obtain some multiprocessing Context and the slot-tracking queue
        self._context = resolve_context(context)
        self._slots: MinimalQueue[int] = MinimalQueue(self._context)

        # Issue one token for each requested slot
        if slots is None:
            slots = len(os.sched_getaffinity(0))  # Not context.cpu_count()!
        assert isinstance(slots, int) and slots >= 1, type(slots)
        self._slots.put(*range(slots))

        # Tracks outstanding Futures (and wait-able sentinels)
        self._future_sentinels: dict[Future, int] = {}

    def __getstate__(self) -> tuple:
        """Get instance state without exposing in-flight Futures."""
        # Required because Futures can be neither copied nor pickled
        # Without custom handling of Futures, submit(...) would fail
        # whenever an instance is part of an argument to a sub-Process
        return self._context, self._slots, {}

    def __setstate__(self, state: tuple) -> None:
        """Set instance state."""
        assert isinstance(state, tuple) and len(state) == 3
        self._context, self._slots, self._future_sentinels = state

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

    def submit(
        self,
        fn: Callable[..., T],
        *,
        args: Iterable = (),
        kwargs: Mapping[str, Any] = types.MappingProxyType({}),
        callbacks: bool = True,
        consume: int = 1,
        env: Iterable = (),  # Iterable[Tuple[str,str]] breaks!
        preexec_fn: Callable[[], None] = noop,
        sleep_fn: Callable[[], Optional[float]] = noop,
        timeout: Optional[float] = None
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
        """
        # First, check any arguments not for _obtain_tokens(...) just below.
        assert fn is not None
        assert isinstance(args, Iterable), type(args)
        assert isinstance(kwargs, Mapping), type(kwargs)
        assert isinstance(callbacks, bool), type(callbacks)
        assert isinstance(env, Iterable), type(env)
        assert preexec_fn is not None

        # Next, either obtain requested tokens or else raise Blocked
        # Work submission only reclaims tokens when callbacks are enabled
        reclaim_tokens_fn = self.reclaim_resources if callbacks else noop
        tokens = self._obtain_tokens(
            consume=consume,
            deadline=absolute_deadline(relative_timeout=timeout),
            reclaim_tokens_fn=reclaim_tokens_fn,  # type: ignore
            sentinels_fn=self._future_sentinels.values,
            sleep_fn=sleep_fn,
            slots=self._slots,
        )

        # Then, with required slots consumed, begin consuming resources:
        try:
            # Grab resources for processing the submitted work
            # Why use a Pipe instead of a Queue?  Pipes can detect EOFError!
            recv, send = self._context.Pipe(duplex=False)
            process = self._context.Process(  # type: ignore
                target=self._worker_entrypoint,
                args=((send, dict(env), preexec_fn, fn) + tuple(args)),
                kwargs=kwargs,
                daemon=False,
            )
            future: Future[T] = Future(process, recv)
            process.start()

            # Prepare to track the Future and the wait(...)-able sentinel
            self._future_sentinels[future] = process.sentinel
        except Exception:
            # Unwinding any consumed slots on unexpected errors
            while tokens:
                self._slots.put(tokens.pop(0))
            raise

        # As above process.start() succeeded, now Future must restore tokens
        # After any restoration, no longer track this Future within Jobserver
        if tokens:
            future.when_done(self._slots.put, *tokens, _Future__internal=True)
        future.when_done(
            self._future_sentinels.pop, future, _Future__internal=True
        )

        # Finally, return a viable Future to the caller
        return future

    def reclaim_resources(self) -> None:
        """
        Reclaim resources for any completed submissions and issue callbacks.

        Method exposed for when explicit resource reclamation is desired.
        For example, when work requires locking more than just a slot and
        the paired unlock is accomplished via Future-registered callbacks.
        """
        # Copy of keys() required to prevent concurrent modification
        for future in tuple(self._future_sentinels.keys()):
            future.done(timeout=0)

    @staticmethod
    def _worker_entrypoint(send, env, preexec_fn, fn, *args, **kwargs) -> None:
        """Entry point for workers to fun fn(...) due to some  submit(...)."""
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

    # Static because who can worry about one's self at a time like this?!
    @staticmethod
    def _obtain_tokens(
        consume: int,
        deadline: float,
        reclaim_tokens_fn: Callable[[], Any],
        sentinels_fn: Callable[[], Iterable[int]],
        sleep_fn: Callable[[], Optional[float]],
        slots: MinimalQueue[int],
        *,
        resolution: float = 1.0e-2
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
                    raise Blocked()

                # (6) ...then block until some interesting event.
                wait(
                    tuple(sentinels_fn())  # "Child" result
                    + (slots.waitable(),),  # "Grandchild" restores token
                    timeout=deadline - monotonic,
                )

        assert len(retval) == consume, "Postcondition"
        return retval
