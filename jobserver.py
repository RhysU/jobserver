# Copyright (C) 2019-2020 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
A nestable, multiprocessing Jobserver having Futures which permit callbacks.

This Jobserver is similar in spirit to multiprocessing.Pool
or concurrent.futures.Executor with a few differences:

    * First, the implementation choices are based upon
      https://www.gnu.org/software/make/manual/html_node/POSIX-Jobserver.html.
    * Second, as a result, the Jobserver is "nestable" meaning that resource
      constraints will be shared with work submitted by other work.
    * Third, no background threads are spun up to handle any backing
      queues consequently permitting the implementation to play well with
      more 3rd party libraries.
    * Fourth, Futures are eagerly scanned to quickly reclaim resources.
    * Fifth, Futures can detect when a child process died unexpectedly.
    * Sixth, the user can specify additional work acceptance criteria.
      For example, not launching work unless some amount of RAM is available.
    * Lastly, the API communicates when Exceptions occur within a callback.

For usage, see JobserverTest located within the same file as Jobserver.
Implementation is intended to work on CPython 3.5, 3.6, 3.7, and 3.8.
Implementation is both PEP 8 (per flake8) and type-hinting clean (per mypy).
Refer to https://github.com/RhysU/jobserver for the upstream project.
"""
import abc
import collections.abc
import copy
import itertools
import os
import pickle
import queue
import signal
import tempfile
import time
import types
import typing
import unittest

# Implementation depends upon an explicit subset of multiprocessing.
from multiprocessing import get_all_start_methods, get_context
from multiprocessing.connection import Connection, wait
from multiprocessing.context import BaseContext
from multiprocessing.process import BaseProcess
from multiprocessing.reduction import ForkingPickler  # type: ignore

__all__ = [
    "Blocked",
    "CallbackRaised",
    "Future",
    "Jobserver",
    "SubmissionDied",
]

T = typing.TypeVar("T")


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


class Wrapper(abc.ABC, typing.Generic[T]):
    """Allows Futures to track whether a value was raised or returned."""

    __slots__ = ()

    @abc.abstractmethod
    def unwrap(self) -> T:
        """Raise any wrapped Exception otherwise return some result."""
        raise NotImplementedError()


# Down the road, ResultWrapper might be extended with "big object"
# support that chooses to place data in shared memory or on disk.
# Likely only necessary if/when sending results via pipe breaks down.
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

    def unwrap(self) -> typing.NoReturn:
        raise self._raised


class Future(typing.Generic[T]):
    """
    Future instances are obtained by submitting work to a Jobserver.

    Futures report if a submission is done(), its result(), and may
    additionally be used to register callbacks issued at completion.
    Futures can be neither copied nor pickled.
    """

    __slots__ = ("_process", "_connection", "_wrapper", "_callbacks")

    def __init__(self, process: BaseProcess, connection: Connection) -> None:
        """
        An instance expecting a Process to send(...) a result to a Connection.
        """
        assert process is not None  # Becomes None after BaseProcess.join()
        self._process = process  # type: typing.Optional[BaseProcess]

        assert connection is not None  # Becomes None after Connection.close()
        self._connection = connection  # type: typing.Optional[Connection]

        # Becomes non-None after result is obtained
        self._wrapper = None  # type: typing.Optional[Wrapper[T]]

        # Populated by calls to when_done(...)
        self._callbacks = []  # type: typing.List[typing.Tuple]

    def __copy__(self) -> typing.NoReturn:
        """Disallow copying as duplicates cannot sensibly share resources."""
        # In particular, which copy would call self._process.join()?
        raise NotImplementedError("Futures cannot be copied.")

    def __reduce__(self) -> typing.NoReturn:
        """Disallow pickling as duplicates cannot sensibly share resources."""
        # In particular, because pickles create copies.
        raise NotImplementedError("Futures cannot be pickled.")

    def when_done(
        self, fn: typing.Callable, *args, __internal: bool = False, **kwargs
    ) -> None:
        """
        Register a function for execution sometime after Future.done(...).

        When already done(...), will immediately invoke the requested function.
        Registered callback functions can accept a Future as an argument.
        May raise CallbackRaised from at most this new callback.
        """
        self._callbacks.append((__internal, fn, args, kwargs))
        if self._connection is None:
            self._issue_callbacks()

    def done(self, timeout: typing.Optional[float] = None) -> bool:
        """
        Is result ready?  Never raises Blocked instead returning False.

        Timeout is given in seconds with None meaning to block indefinitely.
        May raise CallbackRaised from at most one registered callback.
        See CallbackRaised documentation for callback error semantics.
        """
        # Multiple calls to done() may be required to issue all callbacks.
        if self._connection is None:
            self._issue_callbacks()
            return True

        # Possibly wait until a result is available for reading.
        if not self._connection.poll(timeout):
            return False

        # Attempt to read the result Wrapper from the underlying Connection
        # Any EOFError is treated as an unexpected hang up from the other end.
        try:
            self._wrapper = self._connection.recv()
            assert isinstance(self._wrapper, Wrapper), type(self._wrapper)
        except EOFError:
            self._wrapper = ExceptionWrapper(SubmissionDied())

        # Now join(...) and set to None thus reclaiming OS/Python resources.
        assert self._process is not None
        self._process.join()
        self._process = None

        # Should close() throw just below notice it will never be retried.
        connection, self._connection = self._connection, None
        connection.close()
        self._issue_callbacks()
        return True

    def _issue_callbacks(self):
        # Only a non-internal callback may cause CallbackRaised.
        # Otherwise, we might obfuscate bugs within this module's logic.
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

    def result(self, timeout: typing.Optional[float] = None) -> T:
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


class MinimalQueue(typing.Generic[T]):
    """
    An unbounded SimpleQueue-variant providing minimal semantics for Jobserver.

    Vanilla multiprocessing.SimpleQueue lacks timeout on get(...).
    Vanilla multiprocessing.Queue has wildly undesired threading machinery.
    """

    __slots__ = ("_reader", "_writer", "_read_lock", "_write_lock")

    def __init__(self, context: BaseContext) -> None:
        self._reader, self._writer = context.Pipe(duplex=False)
        self._read_lock = context.Lock()
        self._write_lock = context.Lock()

    def waitable(self) -> int:
        """The object on which to wait(...) to get(...) new data."""
        return self._reader.fileno()

    def get(self, timeout: typing.Optional[float] = None) -> T:
        """
        Get one object from the queue raising queue.Empty if unavailable.

        Raises EOFError on exhausted queue whenever sending half has hung up.
        """
        with self._read_lock:
            if self._reader.poll(timeout):
                recv = self._reader.recv_bytes()
            else:
                raise queue.Empty
        return ForkingPickler.loads(recv)

    def put(self, *args: T) -> None:
        """
        Put zero or more objects into the queue, contiguously.

        Raises BrokenPipeError if the receiving half has hung up.
        """
        if args:
            send = [ForkingPickler.dumps(arg) for arg in args]
            with self._write_lock:
                while send:
                    self._writer.send_bytes(send.pop(0))


# Appears as a default argument in Jobserver to simplify some logic therein.
def noop(*args, **kwargs) -> None:
    """A "do nothing" function conforming to (the rejected) PEP-559."""
    return None


class Jobserver:
    """A Jobserver exposing a Future interface built atop multiprocessing."""

    __slots__ = ("_context", "_slots", "_future_sentinels")

    def __init__(
        self,
        context: typing.Union[None, str, BaseContext] = None,
        slots: typing.Optional[int] = None,
    ) -> None:
        """
        Wrap some multiprocessing context and allow some number of slots.

        When not provided, context defaults to multiprocessing.get_context().
        As shorthand, multiprocessing.get_context(string) is used for a string.

        When not provided, slots defaults to len(os.sched_getaffinity(0))
        which reports the number of usable CPUs for the current process.
        """
        # Obtain some multiprocessing Context
        self._context = (
            get_context(context)
            if context is None or isinstance(context, str)
            else context
        )
        self._slots = MinimalQueue(self._context)  # type: MinimalQueue[int]

        # Issue one token for each requested slot
        if slots is None:
            slots = len(os.sched_getaffinity(0))  # Not context.cpu_count()!
        assert isinstance(slots, int) and slots >= 1, type(slots)
        self._slots.put(*range(slots))

        # Tracks outstanding Futures (and wait-able sentinels)
        self._future_sentinels = {}  # type: typing.Dict[Future, int]

    def __getstate__(self) -> typing.Tuple:
        """Get instance state without exposing in-flight Futures."""
        return self._context, self._slots, {}

    def __setstate__(self, state: typing.Tuple) -> None:
        """Set instance state."""
        self._context, self._slots, self._future_sentinels = state

    def __copy__(self) -> "Jobserver":
        """Shallow copies return the original Jobserver unchanged."""
        # Because any "copy" should and can only mutate same slots/sentinels
        return self

    def __deepcopy__(self, _: typing.Any) -> "Jobserver":
        """Deep copies return the original Jobserver unchanged."""
        # Because any "copy" should and can only mutate same slots/sentinels
        return self

    def __call__(
        self, fn: typing.Callable[..., T], *args, **kwargs
    ) -> Future[T]:
        """Submit running fn(*args, **kwargs) to this Jobserver.

        Shorthand for calling submit(fn=fn, args=*args, kwargs=**kwargs),
        with all submission semantics per that method's default arguments.
        """
        return self.submit(fn=fn, args=args, kwargs=kwargs)

    def submit(
        self,
        fn: typing.Callable[..., T],
        *,
        args: typing.Iterable = (),
        kwargs: typing.Mapping[str, typing.Any] = types.MappingProxyType({}),
        callbacks: bool = True,
        consume: int = 1,
        env: typing.Iterable = (),  # Iterable[Tuple[str,str]] breaks!
        preexec_fn: typing.Callable[[], None] = noop,
        sleep_fn: typing.Callable[[], typing.Optional[float]] = noop,
        timeout: typing.Optional[float] = None
    ) -> Future[T]:
        """Submit running fn(*args, **kwargs) to this Jobserver.

        Raises Blocked when insufficient resources available to accept work.
        This method issues callbacks on completed work when callbacks is True.
        Timeout is given in seconds with None meaning block indefinitely.

        When consume == 0, no job slot is consumed by the submission.
        Only consume == 0 or consume == 1 is permitted by the implementation.
        When provided, child first calls os.environ.update(env) just before fn.
        When provided, child next calls preexec_fn() just before fn(...).

        Optional sleep_fn() permits injecting additional logic as
        to when a slot may be consumed.  For example, one can accept work
        only when sufficient RAM is available.  Function sleep_fn()
        should either return None when work is acceptable or return the
        non-negative number of seconds for which this process should sleep.
        """
        # First, check any arguments not for to _obtain_tokens(...) just below.
        assert fn is not None
        assert isinstance(args, collections.abc.Iterable), type(args)
        assert isinstance(kwargs, collections.abc.Mapping), type(kwargs)
        assert isinstance(env, collections.abc.Iterable), type(env)
        assert preexec_fn is not None

        # Next, either obtain requested tokens or else raise Blocked
        tokens = self._obtain_tokens(
            callbacks=callbacks,
            consume=consume,
            deadline=self._absolute_deadline(relative_timeout=timeout),
            future_sentinels=self._future_sentinels,
            sleep_fn=sleep_fn,
            slots=self._slots,
        )

        # Then, with required slots consumed, begin consuming resources:
        try:
            # Temporarily mutate members to clear known Futures for new worker
            # to accommodate the possibility of an os.fork() under the covers
            registered, self._future_sentinels = self._future_sentinels, {}

            # Grab resources for processing the submitted work
            # Why use a Pipe instead of a Queue?  Pipes can detect EOFError!
            recv, send = self._context.Pipe(duplex=False)
            process = self._context.Process(  # type: ignore
                target=self._worker_entrypoint,
                args=((send, env, preexec_fn, fn) + tuple(args)),
                kwargs=kwargs,
                daemon=False,
            )
            future = Future(process, recv)  # type: Future[T]
            process.start()

            # Prepare to track the Future and the wait(...)-able sentinel
            registered[future] = process.sentinel
        except Exception:
            # Unwinding any consumed slots on unexpected errors
            while tokens:
                self._slots.put(tokens.pop(0))
            raise
        finally:
            # Re-mutate members to restore known-Future tracking
            self._future_sentinels = registered

        # As above process.start() succeeded, now Future must restore tokens.
        # After any restoration, no longer track this Future within Jobserver.
        if tokens:
            future.when_done(self._slots.put, *tokens, _Future__internal=True)
        future.when_done(
            self._future_sentinels.pop, future, _Future__internal=True
        )

        # Finally, return a viable Future to the caller.
        return future

    @staticmethod
    def _worker_entrypoint(send, env, preexec_fn, fn, *args, **kwargs) -> None:
        """Entry point for workers to fun fn(...) due to some  submit(...)."""
        # Wrapper usage tracks whether a value was returned or raised
        # in degenerate case where client code returns an Exception.
        result = None  # type: typing.Optional[Wrapper[typing.Any]]
        try:
            os.environ.update(env)
            preexec_fn()
            result = ResultWrapper(fn(*args, **kwargs))
        except Exception as exception:
            result = ExceptionWrapper(exception)
        finally:
            # Ignore broken pipes which naturally occur when the destination
            # terminates (or otherwise hangs up) before the result is ready.
            try:
                send.send(result)  # On ValueError suspect object too large!
            except BrokenPipeError:
                pass
            send.close()

    # Static because who can worry about one's self at a time like this?!
    @staticmethod
    def _obtain_tokens(
        callbacks: bool,
        consume: int,
        deadline: float,
        future_sentinels: typing.Dict[Future, int],
        sleep_fn: typing.Callable[[], typing.Optional[float]],
        slots: MinimalQueue[int],
        *,
        resolution: float = 1.0e-2
    ) -> typing.List[int]:
        """Either retrieve requested tokens or raise Blocked while trying."""
        # Defensively check arguments
        assert isinstance(callbacks, bool), type(callbacks)
        assert consume == 0 or consume == 1, "Invalid or deadlock possible"
        assert sleep_fn is not None
        assert deadline > 0.0

        # Acquire the requested retval or raise Blocked when impossible
        retval = []  # type: typing.List[int]
        while True:

            # (1) Eagerly clean up any completed work (including callbacks)
            if callbacks:
                for future in tuple(future_sentinels.keys()):
                    future.done(timeout=0)

            # (2) Exit loop if all requested resources have been acquired
            if len(retval) >= consume:
                break

            # (3) When sleep_fn() vetoes new work proceed to sleep.
            sleep = sleep_fn()
            monotonic = time.monotonic()
            if sleep is not None:
                assert sleep >= 0.0
                time.sleep(max(resolution, min(sleep, deadline - monotonic)))
                if monotonic >= deadline:
                    raise Blocked()
                continue

            try:
                # (4) Grab any immediately available token.
                retval.append(slots.get(timeout=0))
            except queue.Empty:
                # (5) Otherwise, possibly throw in the towel...
                monotonic = time.monotonic()
                if monotonic >= deadline:
                    raise Blocked()

                # (6) ...then block until some interesting event.
                wait(
                    tuple(future_sentinels.values())  # "Child" result
                    + (slots.waitable(),),  # "Grandchild" restores token
                    timeout=deadline - monotonic,
                )

        assert len(retval) == consume, "Postcondition"
        return retval

    @staticmethod
    def _absolute_deadline(relative_timeout: typing.Optional[float]) -> float:
        """Convert relative timeout in seconds into an absolute deadline."""
        # Cannot be Inf nor sys.float_info.max nor sys.maxsize / 1000
        # nor _PyTime_t per https://stackoverflow.com/questions/45704243!
        return time.monotonic() + (
            (60 * 60 * 24 * 7)  # 604.8k seconds ought to be enough for anyone
            if relative_timeout is None
            else relative_timeout
        )


###########################################################################
# TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS 3
###########################################################################


class JobserverTest(unittest.TestCase):
    """Unit tests (doubling as examples) for Jobserver/Future/Wrapper."""

    def test_defaults(self) -> None:
        """Default construction and __call__ shorthand ok?"""
        js = Jobserver()
        f = js(len, (1, 2, 3))
        g = js(str, object=2)
        h = js(lambda x: len(x), (1, 2, 3, 4))
        self.assertEqual(4, h.result())
        self.assertEqual("2", g.result())
        self.assertEqual(3, f.result())

    @staticmethod
    def helper_callback(lizt: typing.List, index: int, increment: int) -> None:
        """Helper permitting tests to observe callbacks firing."""
        lizt[index] += increment

    def test_basic(self) -> None:
        """Basic submission up to slot limit along with callbacks firing?"""
        for method, check_done in itertools.product(
            get_all_start_methods(), (True, False)
        ):
            with self.subTest(method=method, check_done=check_done):
                # Prepare how callbacks will be observed
                mutable = [0, 0, 0]

                # Prepare work filling all slots
                context = get_context(method)
                js = Jobserver(context=context, slots=3)
                f = js.submit(
                    fn=len,
                    args=((1, 2, 3),),
                    callbacks=False,
                    consume=1,
                    timeout=None,
                )
                f.when_done(self.helper_callback, mutable, 0, 1)
                g = js.submit(
                    fn=str,
                    kwargs=dict(object=2),
                    callbacks=False,
                    consume=1,
                    timeout=None,
                )
                g.when_done(self.helper_callback, mutable, 1, 2)
                g.when_done(self.helper_callback, mutable, 1, 3)
                h = js.submit(
                    fn=len,
                    args=((1,),),
                    callbacks=False,
                    consume=1,
                    timeout=None,
                )
                h.when_done(
                    self.helper_callback, lizt=mutable, index=2, increment=7
                )

                # Try too much work given fixed slot count
                with self.assertRaises(Blocked):
                    js.submit(
                        fn=len,
                        args=((),),
                        callbacks=False,
                        consume=1,
                        timeout=0,
                    )

                # Confirm zero-consumption requests accepted immediately
                i = js.submit(
                    fn=len,
                    args=((1, 2, 3, 4),),
                    callbacks=False,
                    consume=0,
                    timeout=0,
                )

                # Again, try too much work given fixed slot count
                with self.assertRaises(Blocked):
                    js.submit(
                        fn=len,
                        args=((),),
                        callbacks=False,
                        consume=1,
                        timeout=0,
                    )

                # Confirm results in something other than submission order
                self.assertEqual("2", g.result())
                self.assertEqual(mutable[1], 5, "Two callbacks observed")
                if check_done:
                    self.assertTrue(f.done())
                self.assertTrue(h.done())  # No check_done guard!
                self.assertEqual(mutable[2], 7)
                self.assertEqual(1, h.result())
                self.assertEqual(1, h.result(), "Multiple calls OK")
                h.when_done(
                    self.helper_callback, lizt=mutable, index=2, increment=11
                )
                self.assertEqual(mutable[2], 18, "Callback after done")
                self.assertEqual(1, h.result())
                self.assertTrue(h.done())
                self.assertEqual(mutable[2], 18, "Callbacks idempotent")
                self.assertEqual(4, i.result(), "Zero-consumption request")
                if check_done:
                    self.assertTrue(g.done())
                    self.assertTrue(g.done(), "Multiple calls OK")
                self.assertEqual(3, f.result())
                self.assertEqual(mutable[0], 1, "One callback observed")
                self.assertEqual(4, i.result(), "Zero-consumption repeat")

    def helper_check_semantics(self, f: Future[None]) -> None:
        """Helper checking Future semantics *inside* a callback as expected."""
        # Prepare how callbacks will be observed
        mutable = [0]

        # Confirm that inside a callback the Future reports done()
        self.assertTrue(f.done(timeout=0))

        # Confirm that inside a callback additional work can be registered
        f.when_done(self.helper_callback, mutable, 0, 1)
        f.when_done(self.helper_callback, mutable, 0, 2)

        # Confirm that inside a callback above work was immediately performed
        self.assertEqual(mutable[0], 3, "Two callbacks observed")

    def test_callback_semantics(self) -> None:
        """Inside a Future's callback the Future reports it is done."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=3)
                f = js.submit(fn=len, args=((1, 2, 3),))
                f.when_done(self.helper_check_semantics, f)
                self.assertEqual(3, f.result())

    def test_duplication_futures(self) -> None:
        """Copying and pickling of Futures is explicitly disallowed."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=3)
                f = js.submit(fn=len, args=((1, 2, 3),))
                # Cannot copy a Future
                with self.assertRaises(NotImplementedError):
                    copy.copy(f)
                with self.assertRaises(NotImplementedError):
                    copy.deepcopy(f)
                # Cannot pickle a Future
                with self.assertRaises(NotImplementedError):
                    pickle.dumps(f)
                # Cannot submit a Future as part of additional work
                # (as a consequence of the above when pickling required)
                if method != "fork":
                    with self.assertRaises(NotImplementedError):
                        js.submit(fn=type, args=(f,))

    # No behavioral assertions made around pickling, however.
    def test_duplication_jobserver(self) -> None:
        """Copying of Jobservers is explicitly allowed."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js1 = Jobserver(context=method, slots=3)
                js2 = copy.copy(js1)
                js3 = copy.deepcopy(js1)
                f = js1.submit(fn=len, args=((1, 2, 3),))
                g = js2.submit(fn=len, args=((1, 2, 3, 4),))
                h = js3.submit(fn=len, args=((1, 2, 3, 4, 5),))
                self.assertEqual(5, h.result())
                self.assertEqual(4, g.result())
                self.assertEqual(3, f.result())
                # Though copying is allowed, it is degenerate in that
                # copy.copy(...) and copy.deepcopy(...) return the original.
                self.assertIs(js1, js2)
                self.assertIs(js1, js3)

    # Motivated by multiprocessing.Connection mentioning a possible 32MB limit
    def test_large_objects(self) -> None:
        """Confirm increasingly large objects can be processed."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
                for size in (2 ** i for i in range(22, 28)):  # 2**27 is 128 MB
                    with self.subTest(size=size):
                        f = js.submit(fn=bytearray, args=(size,))
                        x = f.result()
                        self.assertEqual(len(x), size)

    @staticmethod
    def helper_block(path: str, timeout: float) -> float:
        """Helper blocking until given path no longer exists."""
        slept = 0.0
        while os.path.exists(path):
            time.sleep(timeout)
            slept += timeout
        return slept

    def test_nonblocking(self) -> None:
        """Ensure non-blocking done() and submit() semantics clean."""
        for method, check_done in itertools.product(
            get_all_start_methods(), (True, False)
        ):
            with self.subTest(method=method, check_done=check_done):
                js = Jobserver(context=method, slots=1)
                timeout = 0.1
                with tempfile.NamedTemporaryFile() as t:
                    # Future f cannot complete until file t is removed
                    f = js.submit(fn=self.helper_block, args=(t.name, timeout))
                    # Because Future f is stalled, new work not accepted
                    with self.assertRaises(Blocked):
                        js.submit(fn=len, args=("abc",), timeout=0)
                    with self.assertRaises(Blocked):
                        js.submit(fn=len, args=("abc",), timeout=1.5 * timeout)
                    # Future f reports not done() blocking or otherwise
                    if check_done:
                        self.assertFalse(f.done(timeout=0))
                        self.assertFalse(f.done(timeout=1.5 * timeout))
                    # Future f reports no result() blocking or otherwise
                    with self.assertRaises(Blocked):
                        f.result(timeout=0)
                    with self.assertRaises(Blocked):
                        f.result(timeout=1.5 * timeout)
                # With file t removed, done() will report True
                if check_done:
                    self.assertTrue(f.done(timeout=None))
                # With file t removed, result() now gives a result,
                # which is compared with the minimum stalled time.
                self.assertGreater(f.result(timeout=None), timeout)
                self.assertGreater(f.result(timeout=0), timeout)

    # Uses "SENTINEL", not None, because None handling is tested elsewhere
    @staticmethod
    def helper_envget(key: str) -> str:
        """Retrieve os.environ.get(key, "SENTINEL")."""
        return os.environ.get(key, "SENTINEL")

    @staticmethod
    def helper_envset(key: str, value: str) -> None:
        """Sets os.environ[key] = value."""
        os.environ[key] = value

    @staticmethod
    def helper_preexec_fn() -> None:
        """Mutates os.environ so that the change can be observed."""
        os.environ["JOBSERVER_TEST_ENVIRON"] = "PREEXEC_FN"

    def test_environ(self) -> None:
        """Confirm sub-process environment is modifiable via submit(...)."""
        # Precondition: key must not be in environment
        key = "JOBSERVER_TEST_ENVIRON"
        self.assertIsNone(os.environ.get(key, None))

        # Test observability of changes to the environment
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
                # Notice f sets, g confirms unset, and h re-sets they key.
                # Notice that i then uses preexec_fn, not env, to set the key.
                # Then j uses both to confirm env updated before preexec_fn.
                f = js.submit(
                    fn=self.helper_envget, args=(key,), env={key: "5678"}
                )
                g = js.submit(fn=self.helper_envget, args=(key,), env={})
                h = js.submit(
                    fn=self.helper_envget, args=(key,), env={key: "1234"}
                )
                i = js.submit(
                    fn=self.helper_envget,
                    args=(key,),
                    preexec_fn=self.helper_preexec_fn,
                )
                j = js.submit(
                    fn=self.helper_envget,
                    args=(key,),
                    preexec_fn=self.helper_preexec_fn,
                    env={key: "OVERWRITTEN"},
                )
                self.assertEqual("PREEXEC_FN", j.result())
                self.assertEqual("PREEXEC_FN", i.result())
                self.assertEqual("1234", h.result())
                self.assertEqual("SENTINEL", g.result())
                self.assertEqual("5678", f.result())

    def test_heavyusage(self) -> None:
        """Workload saturating the configured slots does not deadlock?"""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                # Prepare workload based on number of available slots
                context = get_context(method)
                slots = 2
                js = Jobserver(context=context, slots=slots)

                # Alternate between submissions with and without timeouts
                kwargs = [
                    dict(callbacks=True, timeout=None),
                    dict(callbacks=True, timeout=1000),
                ]  # type: typing.List[typing.Dict[str, typing.Any]]
                fs = [
                    js.submit(fn=len, args=("x" * i,), **(kwargs[i % 2]))
                    for i in range(10 * slots)
                ]

                # Confirm all work completed
                for i, f in enumerate(fs):
                    self.assertEqual(i, f.result(timeout=None))

    # Explicitly tested because of handling woes observed in other designs
    def test_returns_none(self) -> None:
        """None can be returned from a Future?"""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=3)
                f = js.submit(fn=noop, args=(), timeout=None)
                self.assertIsNone(f.result())

    @staticmethod
    def helper_return(arg: T) -> T:
        """Helper returning its lone argument."""
        return arg

    # Explicitly tested because of handling woes observed in other designs
    def test_returns_not_raises_exception(self) -> None:
        """An Exception can be returned, not raised, from a Future?"""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=3)
                e = Exception("Returned by method {}".format(method))
                f = js.submit(fn=self.helper_return, args=(e,), timeout=None)
                self.assertEqual(type(e), type(f.result()))
                self.assertEqual(e.args, f.result().args)  # type: ignore

    @staticmethod
    def helper_raise(klass: type, *args) -> typing.NoReturn:
        """Helper raising the requested Exception class."""
        raise klass(*args)

    def test_raises(self) -> None:
        """Future.result() raises Exceptions thrown while processing work?"""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                # Prepare how callbacks will be observed
                mutable = [0]

                # Prepare work interleaving exceptions and success cases
                js = Jobserver(context=method, slots=3)

                # Confirm exception is raised repeatedly
                f = js.submit(
                    fn=self.helper_raise,
                    args=(ArithmeticError, "message123"),
                    timeout=None,
                )
                f.when_done(self.helper_callback, mutable, 0, 1)
                with self.assertRaises(ArithmeticError):
                    f.result()
                self.assertEqual(mutable[0], 1, "One callback observed")
                f.when_done(self.helper_callback, mutable, 0, 2)
                self.assertEqual(mutable[0], 3, "Callback after done")
                with self.assertRaises(ArithmeticError):
                    f.result()
                self.assertTrue(f.done())
                self.assertEqual(mutable[0], 3, "Callback idempotent")

                # Confirm other work processed without issue
                g = js.submit(fn=str, kwargs=dict(object=2), timeout=None)
                self.assertEqual("2", g.result())

    def test_done_callback_raises(self) -> None:
        """Future.done() raises Exceptions thrown while processing work?"""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=3)

                # Calling done() repeatedly correctly reports multiple errors
                f = js.submit(fn=len, args=(("hello",)), timeout=None)
                f.when_done(self.helper_raise, ArithmeticError, "123")
                f.when_done(self.helper_raise, ZeroDivisionError, "45")
                with self.assertRaises(CallbackRaised) as c:
                    f.done(timeout=None)
                self.assertIsInstance(c.exception.__cause__, ArithmeticError)
                with self.assertRaises(CallbackRaised) as c:
                    f.done(timeout=None)
                self.assertIsInstance(c.exception.__cause__, ZeroDivisionError)
                self.assertTrue(f.done(timeout=None))
                self.assertTrue(f.done(timeout=0))

                # After callbacks have completed, the result is available.
                self.assertEqual(f.result(), 5)
                self.assertEqual(f.result(), 5)

                # Now that work is complete, adding callback raises immediately
                with self.assertRaises(CallbackRaised) as c:
                    f.when_done(self.helper_raise, UnicodeError, "67")
                self.assertIsInstance(c.exception.__cause__, UnicodeError)
                self.assertTrue(f.done(timeout=0.0))

                # After callbacks have completed, result is still available.
                self.assertEqual(f.result(), 5)
                self.assertEqual(f.result(), 5)

    @staticmethod
    def helper_signal(sig: signal.Signals) -> typing.NoReturn:
        """Helper sending the given signal to the current process."""
        os.kill(os.getpid(), sig)
        assert False, "Unreachable"

    def test_submission_died(self) -> None:
        """Signal receipt by worker can be detected via Future?"""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                # Permit observing callback side-effects
                mutable = [0, 0, 0, 0, 0]

                # Prepare jobs with workers possibly receiving signals
                context = get_context(method)
                js = Jobserver(context=context, slots=2)
                f = js.submit(fn=self.helper_signal, args=(signal.SIGKILL,))
                f.when_done(self.helper_callback, mutable, 0, 2)
                g = js.submit(fn=self.helper_signal, args=(signal.SIGTERM,))
                g.when_done(self.helper_callback, mutable, 1, 3)
                h = js.submit(fn=self.helper_signal, args=(signal.SIGUSR1,))
                h.when_done(self.helper_callback, mutable, 2, 5)
                i = js.submit(fn=len, args=(("The jig is up!",)))
                i.when_done(self.helper_callback, mutable, 3, 7)
                j = js.submit(fn=self.helper_signal, args=(signal.SIGUSR2,))
                j.when_done(self.helper_callback, mutable, 4, 11)

                # Confirm done/callbacks correct even when submissions die
                self.assertTrue(f.done())
                self.assertTrue(g.done())
                self.assertTrue(h.done())
                self.assertTrue(i.done())
                self.assertTrue(j.done())
                self.assertEqual([2, 3, 5, 7, 11], mutable)

                # Confirm either results or signals available in reverse order
                with self.assertRaises(SubmissionDied):
                    j.result()
                self.assertEqual(14, i.result())
                with self.assertRaises(SubmissionDied):
                    h.result()
                with self.assertRaises(SubmissionDied):
                    g.result()
                with self.assertRaises(SubmissionDied):
                    f.result()

    def test_sleep_fn(self) -> None:
        """Confirm sleep_fn(...) invoked and handled per documentation."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)

                # Confirm negative sleep is detectable with fn never called
                with self.assertRaises(AssertionError):
                    js.submit(fn=len, sleep_fn=lambda: -1.0)

                # Confirm sleep_fn(...) returning zero can proceed
                zs = iter(itertools.cycle((0, None)))
                f = js.submit(fn=len, args=((1,),), sleep_fn=lambda: next(zs))

                # Confirm sleep_fn(...) returning finite sleep can proceed
                gs = iter(itertools.cycle((0.1, 0.05, None)))
                g = js.submit(fn=len, args=((),), sleep_fn=lambda: next(gs))

                # Confirm repeated sleeping can cause a timeout to occur.
                # Note fn is never called as sleep_fn vetoes the invocation.
                hs = iter(itertools.cycle((0.1,)))
                with self.assertRaises(Blocked):
                    js.submit(fn=len, sleep_fn=lambda: next(hs), timeout=0.35)

                # Confirm as expected.  Importantly, results not previously
                # retrieved implying above submissions finalized results.
                self.assertEqual(1, f.result())
                self.assertEqual(0, g.result())

    @classmethod
    def helper_recurse(cls, js: Jobserver, max_depth: int) -> int:
        """Helper submitting work until either Blocked or max_depth reached."""
        if max_depth < 1:
            return 0
        try:
            f = js.submit(
                fn=cls.helper_recurse, args=(js, max_depth - 1), timeout=0
            )
        except Blocked:
            return 0
        return 1 + f.result(timeout=None)

    def test_submission_nested(self) -> None:
        """Jobserver resource limits honored during nested submissions."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                context = get_context(method)
                self.assertEqual(
                    0,
                    self.helper_recurse(
                        js=Jobserver(context=context, slots=3), max_depth=0
                    ),
                    msg="Recursive base case must terminate recursion",
                )
                self.assertEqual(
                    1,
                    self.helper_recurse(
                        js=Jobserver(context=context, slots=3), max_depth=1
                    ),
                    msg="One inductive step must be possible",
                )
                self.assertEqual(
                    4,
                    self.helper_recurse(
                        js=Jobserver(context=context, slots=4), max_depth=6
                    ),
                    msg="Recursion is limited by number of available slots",
                )
