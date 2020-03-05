# Copyright (C) 2019-2020 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
A nestable, multiprocessing Jobserver providing Futures which permit callbacks.

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
    * Lastly, the API communicates when Exceptions occur within a callback.

For usage, see JobserverTest located within the same file as Jobserver.
Implementation is intended to work on CPython 3.5, 3.6, 3.7, and 3.8.
Implementation is both PEP8 (per flake8) and type-hinting clean (per mypy).
"""
import collections.abc
import copy
import itertools
import multiprocessing
import multiprocessing.connection
import multiprocessing.reduction  # type: ignore
import os
import os.path
import pickle
import queue
import signal
import tempfile
import time
import typing
import unittest

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
    processing to perform after seeing the 1st, 2nd, or N-th error.
    """

    pass


class SubmissionDied(Exception):
    """
    Reports a submission died for unknowable reasons, e.g. being killed.

    Work that is killed, terminated, interrupted, etc. raises this exception.
    Exactly what has transpired is not reported.  Do not attempt to recover.
    """

    pass


# Down the road, Wrapper might be extended with "big object"
# support that chooses to place data in shared memory or on disk.
# Likely only necessary if/when sending results via pipe breaks down.
class Wrapper(typing.Generic[T]):
    """Allows Futures to track whether a value was raised or returned."""

    __slots__ = ("_result", "_raised")

    def __init__(
        self,
        *,
        result: typing.Optional[T] = None,
        raised: typing.Optional[Exception] = None
    ) -> None:
        """Provide just one of result or raised."""
        assert raised is None or result is None
        self._result = result
        self._raised = raised

    def unwrap(self) -> typing.Optional[T]:
        """Raise any wrapped Exception otherwise return the result."""
        if self._raised is not None:
            raise self._raised
        return self._result

    def __repr__(self) -> str:
        return "Wrapper(result={!r}, raised={!r})".format(
            self._result, self._raised
        )


class Future(typing.Generic[T]):
    """
    Future instances are obtained by submitting work to a Jobserver.

    Futures report if a submission is done(), its result(), and may
    additionally be used to register callbacks issued at completion.
    Futures can be neither copied nor pickled.
    Throughout API, arguments block/timeout follow queue.Queue semantics.
    """

    __slots__ = ("_process", "_connection", "_wrapper", "_callbacks")

    def __init__(
        self,
        process: multiprocessing.Process,
        connection: multiprocessing.connection.Connection,
    ) -> None:
        """
        An instance expecting a Process to send(...) a result to a Connection.
        """
        # Becomes None after Process.join(...)
        assert process is not None
        self._process = (
            process
        )  # type: typing.Optional[multiprocessing.Process]

        # Becomes None after recv/Connection.close(...)
        assert connection is not None
        self._connection = (
            connection
        )  # type: typing.Optional[multiprocessing.connection.Connection]

        # Becomes non-None after result is obtained
        self._wrapper = None  # type: typing.Optional[Wrapper]

        # Populated by calls to when_done(...)
        self._callbacks = []  # type: typing.List[tuple]

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

    def done(
        self, block: bool = True, timeout: typing.Optional[float] = None
    ) -> bool:
        """
        Is result ready?  Never raises Blocked instead returning False.

        Argument timeout can only be specified for blocking operations.
        When specified, timeout is given in seconds and must be non-negative.
        May raise CallbackRaised from at most one registered callback.
        See CallbackRaised documentation for callback error semantics.
        """
        # Sanity check requested block/timeout
        if timeout is not None:
            assert block, "Non-blocking operation cannot specify a timeout"
            assert timeout >= 0, "Blocking allows only a non-negative timeout"

        # Multiple calls to done() may be required to issue all callbacks.
        if self._connection is None:
            self._issue_callbacks()
            return True

        # Attempt to read the result Wrapper from the underlying Connection
        # (possibly converting Queue's timeout semantic to Connection.poll).
        # Any EOFError is treated as an unexpected hang up from the other end.
        # Confusingly, as a Wrapper is anticipated, None news is bad news.
        assert self._connection is not None
        if self._connection.poll(timeout=timeout if block else 0):
            try:
                self._wrapper = self._connection.recv()
                if self._wrapper is None:
                    self._wrapper = Wrapper(raised=SubmissionDied())
            except EOFError:
                self._wrapper = Wrapper(raised=SubmissionDied())
            assert self._wrapper is not None, "Confirm logic invariant"
            assert self._process is not None, "Confirm type hinting invariant"
            self._process.join()  # Always join(...) to reclaim OS resources
            self._process = None  # Allow reclaiming via garbage collection
        else:
            return False

        # Callbacks must observe "self.connection is None" otherwise
        # they might observe different state when done vs not-done.
        # (Notice should close() throw, close() will never be re-tried).
        connection, self._connection = self._connection, None
        connection.close()
        self._issue_callbacks()

        return True

    def _issue_callbacks(self):
        # Only a non-internal callback may cause CallbackRaised.
        # Otherwise, we might obfuscate bugs within this module's logic.
        while self._callbacks:
            internal, fn, args, kwargs = self._callbacks.pop(0)
            if internal:
                fn(*args, **kwargs)
            else:
                try:
                    fn(*args, **kwargs)
                except Exception as e:
                    raise CallbackRaised() from e

    def result(
        self, block: bool = True, timeout: typing.Optional[float] = None
    ) -> T:
        """
        Obtain result when ready.  Raises Blocked if result unavailable.

        May raise CallbackRaised from at most one registered callback.
        See CallbackRaised documentation for callback error semantics.
        """
        if not self.done(block=block, timeout=timeout):
            raise Blocked()

        assert isinstance(self._wrapper, Wrapper)
        return typing.cast(T, self._wrapper.unwrap())

    def __copy__(self) -> typing.NoReturn:
        """Disallow copying as duplicates cannot sensibly share resources."""
        raise NotImplementedError("Futures cannot be copied.")

    def __reduce__(self) -> typing.NoReturn:
        """Disallow pickling as duplicates cannot sensibly share resources."""
        raise NotImplementedError("Futures cannot be pickled.")


class JobserverQueue:
    """
    A SimpleQueue-variant providing the minimal semantics Jobserver requires.

    Vanilla multiprocessing.SimpleQueue lacks block/timeout on get(...).
    Vanilla multiprocessing.Queue has wildly undesired threading machinery.
    """

    __slots__ = ("_reader", "_writer", "_read_lock", "_write_lock")

    def __init__(self, context: multiprocessing.context.BaseContext) -> None:
        self._reader, self._writer = context.Pipe(duplex=False)
        self._read_lock = context.Lock()
        self._write_lock = context.Lock()  # Some platforms may not need

    def waitable(self) -> int:
        """The object on which to wait(...) to get(...) new data."""
        return self._reader.fileno()

    def get(
        self, block: bool = True, timeout: typing.Optional[float] = None
    ) -> typing.Any:
        """
        Get an object from the queue raising queue.Empty if unavailable.

        Raises EOFError on exhausted queue whenever sending half has hung up.
        """
        assert block or timeout is None, "block == False cannot have timeout"
        with self._read_lock:
            if self._reader.poll(timeout=timeout if block else 0.0):
                recv = self._reader.recv_bytes()
            else:
                raise queue.Empty
        return multiprocessing.reduction.ForkingPickler.loads(recv)

    def put_nowait(self, obj) -> None:
        """
        Put an object into the queue.

        Raises BrokenPipeError if the receiving half has hung up.
        """
        send = multiprocessing.reduction.ForkingPickler.dumps(obj)
        with self._write_lock:
            self._writer.send_bytes(send)


# Appears as a default argument in Jobserver thus simplifying some logic.
def noop(*args, **kwargs) -> None:
    """A "no nothing" function conformant to PEP-559."""
    return None


# Throughout, put_nowait(...) denotes places where blocking should not happen.
class Jobserver:
    """
    A Jobserver exposing a Future interface built atop multiprocessing.

    Throughout API, arguments block/timeout follow queue.Queue semantics.
    """

    __slots__ = ("_context", "_slots", "_future_sentinels")

    def __init__(
        self,
        context: typing.Union[
            None, str, multiprocessing.context.BaseContext
        ] = None,
        slots: typing.Optional[int] = None,
    ) -> None:
        """
        Wrap some multiprocessing context and allow some number of slots.

        When not provided, context defaults to multiprocessing.get_context().
        As shorthand, multiprocessing.get_context(string) is used for a string.

        When not provided, slots defaults to len(os.sched_getaffinity(0))
        which reports the number of usable CPUs for the current process.
        """
        # Prepare required resources
        if context is None or isinstance(context, str):
            context = multiprocessing.get_context(method=context)
        self._context = context
        if slots is None:
            slots = len(os.sched_getaffinity(0))  # Not context.cpu_count()!
        assert isinstance(slots, int) and slots >= 1
        self._slots = JobserverQueue(context=self._context)
        self._future_sentinels = {}  # type: typing.Dict[Future, int]

        # Issue one token for each requested slot
        for i in range(slots):
            self._slots.put_nowait(i)

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
        args: typing.Sequence = (),
        kwargs: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        block: bool = True,
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
        Timeout can only be specified for blocking operations.
        When specified, timeout is given in seconds and must be non-negative.

        When consume == 0, no job slot is consumed by the submission.
        Only consume == 0 or consume == 1 is permitted by the implementation.
        When specified, the child calls os.environ.update(env) just before fn.
        When specified, the child calls preexec_fn() just before fn(...).
        When both are specified, os.environ is updated before preexec_fn()

        Optional sleep_fn() permits injecting additional logic as
        to when a slot may be consumed.  For example, one can accept work
        only when sufficient RAM is available.  Function sleep_fn()
        should either return None when work is acceptable or return the
        non-negative number of seconds for which this process should sleep.
        """
        # Argument check, especially args/kwargs as misuse is easy and deadly
        assert fn is not None
        assert isinstance(args, collections.abc.Sequence)
        assert kwargs is None or isinstance(kwargs, collections.abc.Mapping)
        assert isinstance(block, bool)
        assert isinstance(callbacks, bool)
        assert isinstance(consume, int)
        assert isinstance(env, collections.abc.Iterable)
        assert preexec_fn is not None
        assert sleep_fn is not None

        # Convert timeout into concrete deadline then defensively drop timeout
        if timeout is None:
            # Cannot be Inf nor sys.float_info.max nor sys.maxsize / 1000
            # nor _PyTime_t per https://stackoverflow.com/questions/45704243!
            deadline = time.monotonic() + (60 * 60 * 24 * 7)  # One week
        else:
            assert block, "Non-blocking operation cannot specify a timeout"
            assert timeout >= 0, "Blocking allows only a non-negative timeout"
            deadline = time.monotonic() + float(timeout)
        del timeout

        # Acquire the requested tokens or raise Blocked when impossible
        tokens = []  # type: typing.List[int]
        assert consume == 0 or consume == 1, "Invalid or deadlock possible"
        while True:
            # (1) Eagerly clean up any completed work hence issuing callbacks
            if callbacks:
                for future in list(self._future_sentinels.keys()):
                    future.done(block=False, timeout=None)

            # (2) Exit loop if all requested resources have been acquired
            if len(tokens) >= consume:
                break

            # (3) When sleep_fn() vetoes new work sleep then GOTO (1).
            # Unless sleeping would push past a blocking timeout threshold!
            seconds = sleep_fn()
            if seconds is not None:
                seconds = min(seconds, 1.0e-2)  # 10 millisecond resolution
                if not block or time.monotonic() + seconds > deadline:
                    raise Blocked()
                time.sleep(seconds)
                continue
            del seconds

            try:
                # (4) If any slot immediately available grab then GOTO (1)
                tokens.append(self._slots.get(block=False, timeout=None))
                continue
            except queue.Empty:
                # (5) Only report failure in (3) non-blocking or deadline hit
                if not block or time.monotonic() > deadline:
                    raise Blocked()

            # (6) Block until either some work completes or deadline hit.
            # (Work completion might be due to some grandchild restoring slots
            # which this process cannot observe via self._future_sentinels!)
            # Beware that completed work will require callbacks at step (1)
            assert block, "Sanity check control flow"
            multiprocessing.connection.wait(
                (self._slots.waitable(),)
                + tuple(self._future_sentinels.values()),
                timeout=deadline - time.monotonic(),
            )

        # Neither block nor deadline are relevant below
        del block, deadline

        # Now, with required slots consumed, begin consuming resources:
        assert len(tokens) >= consume, "Sanity check slot acquisition"
        try:
            # Temporarily mutate members to clear known Futures for new worker
            registered, self._future_sentinels = self._future_sentinels, {}
            # Grab resources for processing the submitted work
            # Why use a Pipe instead of a Queue?  Pipes can detect EOFError!
            recv, send = self._context.Pipe(duplex=False)
            process = self._context.Process(  # type: ignore
                target=self._worker_entrypoint,
                args=((send, env, preexec_fn, fn) + tuple(args)),
                kwargs=kwargs if kwargs else {},
                daemon=False,
            )
            future = Future(process, recv)
            process.start()
            # Prepare to track the Future and the wait(...)-able sentinel
            registered[future] = process.sentinel
        except Exception:
            # Unwinding any consumed slots on unexpected errors
            while tokens:
                self._slots.put_nowait(tokens.pop(0))
            raise
        finally:
            # Re-mutate members to restore known-Future tracking
            self._future_sentinels = registered
            del registered

        # As the above process.start() succeeded, now Future restores tokens.
        # This choice causes token release only after future.process.join()
        # from within Future.done().  It keeps _worker_entrypoint() simple.
        # Restoring tokens MUST occur before Future unregistered (just below).
        while tokens:
            future.when_done(
                self._slots.put_nowait, tokens.pop(0), _Future__internal=True
            )

        # When a Future has completed, no longer track it within Jobserver.
        # Remove, versus discard, chosen to confirm removals previously known.
        future.when_done(
            self._future_sentinels.pop, future, _Future__internal=True
        )

        return future

    @staticmethod
    def _worker_entrypoint(send, env, preexec_fn, fn, *args, **kwargs) -> None:
        # Absent an always well-defined result the following finally
        # block may complain that "result" is unbound on, e.g, SIGKILL.
        # That is, the need for 'result = None' below is wildly unintuitive.
        result = None

        # Wrapper usage tracks whether a value was returned or raised
        # in degenerate case where client code returns an Exception.
        try:
            os.environ.update(env)
            preexec_fn()
            result = Wrapper(result=fn(*args, **kwargs))
        except Exception as exception:
            result = Wrapper(raised=exception)
        finally:
            # Ignore broken pipes which naturally occur when the destination
            # terminates (or otherwise hangs up) before the result is ready.
            try:
                send.send(result)  # On ValueError suspect object too large!
            except BrokenPipeError:
                pass
            send.close()


###########################################################################
# TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS 3
###########################################################################
# TODO Unit tests should, but do not, pass on pypy3.  Signal-related woes?!


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
        for method in multiprocessing.get_all_start_methods():
            for check_done in (True, False):
                with self.subTest(method=method, check_done=check_done):
                    # Prepare how callbacks will be observed
                    mutable = [0, 0, 0]

                    # Prepare work filling all slots
                    context = multiprocessing.get_context(method)
                    js = Jobserver(context=context, slots=3)
                    f = js.submit(
                        fn=len,
                        args=((1, 2, 3),),
                        block=True,
                        callbacks=False,
                        consume=1,
                    )
                    f.when_done(self.helper_callback, mutable, 0, 1)
                    g = js.submit(
                        fn=str,
                        kwargs=dict(object=2),
                        block=True,
                        callbacks=False,
                        consume=1,
                    )
                    g.when_done(self.helper_callback, mutable, 1, 2)
                    g.when_done(self.helper_callback, mutable, 1, 3)
                    h = js.submit(
                        fn=len,
                        args=((1,),),
                        block=True,
                        callbacks=False,
                        consume=1,
                    )
                    h.when_done(
                        self.helper_callback,
                        lizt=mutable,
                        index=2,
                        increment=7,
                    )

                    # Try too much work given fixed slot count
                    with self.assertRaises(Blocked):
                        js.submit(
                            fn=len,
                            args=((),),
                            block=False,
                            callbacks=False,
                            consume=1,
                        )

                    # Confirm zero-consumption requests accepted immediately
                    i = js.submit(
                        fn=len,
                        args=((1, 2, 3, 4),),
                        block=False,
                        callbacks=False,
                        consume=0,
                    )

                    # Again, try too much work given fixed slot count
                    with self.assertRaises(Blocked):
                        js.submit(
                            fn=len,
                            args=((),),
                            block=False,
                            callbacks=False,
                            consume=1,
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
                        self.helper_callback,
                        lizt=mutable,
                        index=2,
                        increment=11,
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
        self.assertTrue(f.done(block=False))

        # Confirm that inside a callback additional work can be registered
        f.when_done(self.helper_callback, mutable, 0, 1)
        f.when_done(self.helper_callback, mutable, 0, 2)

        # Confirm that inside a callback above work was immediately performed
        self.assertEqual(mutable[0], 3, "Two callbacks observed")

    def test_callback_semantics(self) -> None:
        """Inside a Future's callback the Future reports it is done."""
        for method in multiprocessing.get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=3)
                f = js.submit(fn=len, args=((1, 2, 3),))
                f.when_done(self.helper_check_semantics, f)
                self.assertEqual(3, f.result())

    def test_duplication(self) -> None:
        """Copying and pickling of Futures is explicitly disallowed."""
        for method in multiprocessing.get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=3)
                f = js.submit(fn=len, args=((1, 2, 3),))
                # Cannot copy
                with self.assertRaises(NotImplementedError):
                    copy.copy(f)
                with self.assertRaises(NotImplementedError):
                    copy.deepcopy(f)
                # Cannot pickle a Future directly
                with self.assertRaises(NotImplementedError):
                    pickle.dumps(f)
                # Cannot submit a Future as part of additional work
                # (as a consequence of the above when pickling required)
                if method != "fork":
                    with self.assertRaises(NotImplementedError):
                        js.submit(fn=type, args=(f,))

    # Motivated by multiprocessing.Connection mentioning a possible 32MB limit
    def test_large_objects(self) -> None:
        """Confirm increasingly large objects can be processed."""
        for method in multiprocessing.get_all_start_methods():
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
        for method in multiprocessing.get_all_start_methods():
            for check_done in (True, False):
                with self.subTest(method=method, check_done=check_done):
                    js = Jobserver(context=method, slots=1)
                    timeout = 0.1
                    # Future f cannot complete until file t is removed
                    with tempfile.NamedTemporaryFile() as t:
                        f = js.submit(
                            fn=self.helper_block, args=(t.name, timeout)
                        )
                        # Because Future f is stalled, new work not accepted
                        with self.assertRaises(Blocked):
                            js.submit(fn=len, args=("abc",), block=False)
                        with self.assertRaises(Blocked):
                            js.submit(
                                fn=len,
                                args=("abc",),
                                block=True,
                                timeout=1.5 * timeout,
                            )
                        # Future f reports not done() blocking or otherwise
                        if check_done:
                            self.assertFalse(f.done(block=False))
                            self.assertFalse(
                                f.done(block=True, timeout=1.5 * timeout)
                            )
                        # Future f reports no result() blocking or otherwise
                        with self.assertRaises(Blocked):
                            f.result(block=False)
                        with self.assertRaises(Blocked):
                            f.result(block=True, timeout=1.5 * timeout)
                    # With file t removed, done() will report True
                    if check_done:
                        self.assertTrue(f.done(block=True))
                    # With file t removed, result() now gives a result,
                    # which is compared with the minimum stalled time.
                    self.assertGreater(f.result(block=True), timeout)
                    self.assertGreater(f.result(block=False), timeout)

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
        for method in multiprocessing.get_all_start_methods():
            js = Jobserver(context=method, slots=1)
            with self.subTest(method=method):
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
        for method in multiprocessing.get_all_start_methods():
            with self.subTest(method=method):
                # Prepare workload based on number of available slots
                context = multiprocessing.get_context(method)
                slots = 2
                js = Jobserver(context=context, slots=slots)

                # Alternate between submissions with and without timeouts
                kwargs = [
                    dict(block=True, callbacks=True, timeout=None),
                    dict(block=True, callbacks=True, timeout=1000),
                ]  # type: typing.List[typing.Dict[str, typing.Any]]
                fs = [
                    js.submit(fn=len, args=("x" * i,), **(kwargs[i % 2]))
                    for i in range(10 * slots)
                ]

                # Confirm all work completed
                for i, f in enumerate(fs):
                    self.assertEqual(i, f.result(block=True))

    @staticmethod
    def helper_none() -> None:
        """Nullary helper returning None."""
        return None

    # Explicitly tested because of handling woes observed in other designs
    def test_returns_none(self) -> None:
        """None can be returned from a Future?"""
        for method in multiprocessing.get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=3)
                f = js.submit(fn=self.helper_none, args=(), block=True)
                self.assertIsNone(f.result())

    @staticmethod
    def helper_return(arg: T) -> T:
        """Helper returning its lone argument."""
        return arg

    # Explicitly tested because of handling woes observed in other designs
    def test_returns_not_raises_exception(self) -> None:
        """An Exception can be returned, not raised, from a Future?"""
        for method in multiprocessing.get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=3)
                e = Exception("Returned by method {}".format(method))
                f = js.submit(fn=self.helper_return, args=(e,), block=True)
                self.assertEqual(type(e), type(f.result()))
                self.assertEqual(e.args, f.result().args)  # type: ignore

    @staticmethod
    def helper_raise(klass: type, *args) -> typing.NoReturn:
        """Helper raising the requested Exception class."""
        raise klass(*args)

    def test_raises(self) -> None:
        """Future.result() raises Exceptions thrown while processing work?"""
        for method in multiprocessing.get_all_start_methods():
            with self.subTest(method=method):
                # Prepare how callbacks will be observed
                mutable = [0]

                # Prepare work interleaving exceptions and success cases
                js = Jobserver(context=method, slots=3)

                # Confirm exception is raised repeatedly
                f = js.submit(
                    fn=self.helper_raise,
                    args=(ArithmeticError, "message123"),
                    block=True,
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
                g = js.submit(fn=str, kwargs=dict(object=2), block=True)
                self.assertEqual("2", g.result())

    def test_done_callback_raises(self) -> None:
        """Future.done() raises Exceptions thrown while processing work?"""
        for method in multiprocessing.get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=3)

                # Calling done() repeatedly correctly reports multiple errors
                f = js.submit(fn=len, args=(("hello",)), block=True)
                f.when_done(self.helper_raise, ArithmeticError, "123")
                f.when_done(self.helper_raise, ZeroDivisionError, "45")
                with self.assertRaises(CallbackRaised) as c:
                    f.done(block=True)
                self.assertIsInstance(c.exception.__cause__, ArithmeticError)
                with self.assertRaises(CallbackRaised) as c:
                    f.done(block=True)
                self.assertIsInstance(c.exception.__cause__, ZeroDivisionError)
                self.assertTrue(f.done(block=True))
                self.assertTrue(f.done(block=False))

                # After callbacks have completed, the result is available.
                self.assertEqual(f.result(), 5)
                self.assertEqual(f.result(), 5)

                # Now that work is complete, adding callback raises immediately
                with self.assertRaises(CallbackRaised) as c:
                    f.when_done(self.helper_raise, UnicodeError, "67")
                self.assertIsInstance(c.exception.__cause__, UnicodeError)
                self.assertTrue(f.done(block=False))

                # After callbacks have completed, result is still available.
                self.assertEqual(f.result(), 5)
                self.assertEqual(f.result(), 5)

    @staticmethod
    def helper_signal(sig: signal.Signals) -> typing.NoReturn:
        """Helper sending signal sig to the current process."""
        os.kill(os.getpid(), sig)
        assert False, "Unreachable"

    def test_submission_died(self) -> None:
        """Signal receipt by worker can be detected via Future?"""
        for method in multiprocessing.get_all_start_methods():
            with self.subTest(method=method):
                # Permit observing callback side-effects
                mutable = [0, 0, 0, 0, 0]

                # Prepare jobs with workers possibly receiving signals
                context = multiprocessing.get_context(method)
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
        for method in multiprocessing.get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)

                # Confirm negative sleep is detectable with fn never called
                with self.assertRaises(ValueError):
                    js.submit(fn=len, sleep_fn=lambda: -1.0)

                # Confirm sleep_fn(...) returning zero can proceed
                zs = iter(itertools.cycle((0, None)))
                f = js.submit(fn=len, args=((1,),), sleep_fn=lambda: next(zs))

                # Confirm sleep_fn(...) returning finite sleep can proceed
                gs = iter(itertools.cycle((0.1, 0.05, None)))
                g = js.submit(fn=len, args=((),), sleep_fn=lambda: next(gs))

                # Confirm repeated sleeping can cause a timeout to occur.
                # Again, fn is never called as sleep_fn vetoes the invocation.
                hs = iter(itertools.cycle((0.1,)))
                with self.assertRaises(Blocked):
                    js.submit(fn=len, sleep_fn=lambda: next(hs), timeout=0.35)

                # Confirm as expected.  Importantly, results not previously
                # retrieved implying above submissions finalized results.
                self.assertEqual(1, f.result())
                self.assertEqual(0, g.result())

    @classmethod
    def helper_recurse(cls, js: Jobserver, maxdepth: int) -> int:
        """Helper submitting work until either Blocked or maxdepth reached."""
        if maxdepth < 1:
            return 0
        try:
            f = js.submit(
                fn=cls.helper_recurse, args=(js, maxdepth - 1), block=False
            )
        except Blocked:
            return 0
        return 1 + f.result(block=True)

    def test_submission_nested(self) -> None:
        """Jobserver resource limits honored during nested submissions."""
        for method in multiprocessing.get_all_start_methods():
            with self.subTest(method=method):
                context = multiprocessing.get_context(method)
                self.assertEqual(
                    0,
                    self.helper_recurse(
                        js=Jobserver(context=context, slots=3), maxdepth=0
                    ),
                    msg="Recursive base case must terminate recursion",
                )
                self.assertEqual(
                    1,
                    self.helper_recurse(
                        js=Jobserver(context=context, slots=3), maxdepth=1
                    ),
                    msg="One inductive step must be possible",
                )
                self.assertEqual(
                    4,
                    self.helper_recurse(
                        js=Jobserver(context=context, slots=4), maxdepth=6
                    ),
                    msg="Recursion is limited by number of available slots",
                )
