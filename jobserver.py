#!/usr/bin/env python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""A Jobserver exposing a Future interface built atop multiprocessing."""
import atexit
import collections.abc
import multiprocessing
import multiprocessing.connection
import os
import signal
import time
import typing
import unittest

from queue import Empty

T = typing.TypeVar("T")

__all__ = [
    "CallbackRaised",
    "SubmissionDied",
    "Empty",
    "Future",
    "Jobserver",
    "Wrapper",
]


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


# TODO This name is pretty awful.  Matches some built-in Exception?
class SubmissionDied(Exception):
    """
    Reports a submission died for unknowable reasons, e.g. being killed.

    Work this is killed, terminated, interrupted, etc. raises this exception.
    Exactly what has transpired is not reported.  Do not attempt to recover.
    """

    pass


class Wrapper(typing.Generic[T]):
    """Allows Futures to track whether a value was raised or returned."""

    __slots__ = ("_result", "_raised")

    def __init__(
        self,
        *,
        result: typing.Optional[T] = None,
        raised: typing.Optional[Exception] = None
    ) -> None:
        assert raised is None or result is None, "Both cannot be None"
        self._result = result
        self._raised = raised

    def raised(self) -> bool:
        """Will unwrap(...) raise an Exception?"""
        return self._raised is not None

    def unwrap(self) -> T:
        """Raise any wrapped Exception otherwise return the result."""
        if self.raised():
            raise self._raised
        return self._result

    def __repr__(self):
        return "Wrapper(result={!r}, raised={!r})".format(
            self._result, self._raised
        )


class Future(typing.Generic[T]):
    """
    A Future expecting a Process to send(...) a result to the given Connection.
    Throughout this API, arguments block/timeout follow queue.Queue semantics.
    """

    __slots__ = ("_process", "_connection", "_wrapper", "_callbacks")

    def __init__(
        self,
        process: multiprocessing.Process,
        connection: multiprocessing.connection.Connection,
    ) -> None:
        assert process is not None  # None after Process.join(...)
        assert connection is not None  # None after recv/Connection.close(...)
        self._process = process
        self._connection = connection
        self._wrapper = None
        self._callbacks = []  # Tuples per add_done_callback, _issue_callbacks

    def add_done_callback(
        self, fn: typing.Callable, *args, __internal: bool = False, **kwargs
    ) -> None:
        """
        Register a function for execution sometime after Future.done(...).

        When already done(...), will immediately invoke the requested function.
        May raise CallbackRaised from at most this new callback.
        """
        self._callbacks.append((__internal, fn, args, kwargs))
        if self._connection is None:
            self._issue_callbacks()

    def done(
        self, block: bool = True, timeout: typing.Optional[float] = None
    ) -> bool:
        """
        Is result ready?

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

        # Attempt to read the result Wrapper from the underlying Connection.
        # Any EOFError is treated as an unexpected hang up from the other end.
        assert self._connection is not None
        if block or self._connection.poll(timeout=timeout):
            try:
                self._wrapper = self._connection.recv()
            except EOFError:
                self._wrapper = Wrapper(raised=SubmissionDied())
            assert isinstance(self._wrapper, Wrapper), "Confirm invariant"
            self._process.join()
            self._process = None  # Allow reclaiming via garbage collection
        else:
            return False

        # Callback must observe "self.connection is None" otherwise
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

    def result(self, block: bool = True, timeout: float = None) -> T:
        """
        Obtain result when ready.

        May raise CallbackRaised from at most one registered callback.
        See CallbackRaised documentation for callback error semantics.
        """
        if not self.done(block=block, timeout=timeout):
            raise Empty()

        return self._wrapper.unwrap()


# Throughout, put_nowait(...) denotes places where blocking should not happen.
# If debugging, consider any related queue.Full exceptions to be logic errors.
class Jobserver:
    """
    A Jobserver exposing a Future interface built atop multiprocessing.
    Throughout API, arguments block/timeout follow queue.Queue semantics.
    """

    __slots__ = ("_context", "_slots", "_future_sentinels")

    def __init__(
        self,
        context: typing.Optional[multiprocessing.context.BaseContext] = None,
        slots: typing.Optional[int] = None,
    ) -> None:
        """
        Wrap some multiprocessing context and allow some number of slots.

        When not provided, context defaults to multiprocessing.get_context().
        When not provided, slots defaults to context.cpu_count().
        """
        # Prepare required resources ensuring their LIFO-ordered tear down
        if context is None:
            context = multiprocessing.get_context()
        self._context = context
        if slots is None:
            slots = self._context.cpu_count()
        assert isinstance(slots, int) and slots >= 1
        self._slots = self._context.Queue(maxsize=slots)
        atexit.register(self._slots.join_thread)
        atexit.register(self._slots.close)
        self._future_sentinels = {}  # type: typing.Dict[Future, int]

        # Issue one token for each requested slot
        for i in range(slots):
            self._slots.put_nowait(i)

    def __call__(
        self,
        fn: typing.Callable[..., T],
        *args: typing.Any,
        **kwargs: typing.Any
    ) -> Future[T]:
        """Submit running fn(*args, **kwargs) to this Jobserver.

        Shorthand for calling submit(fn=fn, args=*args, kwargs=**kwargs),
        with all submission semantics per that methods default arguments.
        """
        return self.submit(fn=fn, args=args, kwargs=kwargs)

    def submit(
        self,
        fn: typing.Callable[..., T],
        *,
        args: typing.Sequence = None,
        kwargs: typing.Dict[str, typing.Any] = None,
        block: bool = True,
        callbacks: bool = True,
        consume: int = 1,
        timeout: typing.Optional[float] = None
    ) -> Future[T]:
        """Submit running fn(*args, **kwargs) to this Jobserver.

        Non-blocking usage per block/timeout possibly raises queue.Empty.
        When consume == 0, no job slot is consumed by the submission.
        This method issues callbacks on completed work when callbacks is True.
        Timeout can only be specified for blocking operations.
        When specified, timeout is given in seconds and must be non-negative.
        """
        # Argument check, especially args/kwargs as misusage is easy and deadly
        assert fn is not None
        assert args is None or isinstance(args, collections.abc.Sequence)
        assert kwargs is None or isinstance(kwargs, collections.abc.Mapping)
        assert isinstance(block, bool)
        assert isinstance(callbacks, bool)
        assert isinstance(consume, int)

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

        # Acquire the requested tokens or raise queue.Empty when impossible
        tokens = []
        assert consume == 0 or consume == 1, "Invalid or deadlock possible"
        while True:
            # (1) Eagerly clean up any completed work hence issuing callbacks
            if callbacks:
                for future in list(self._future_sentinels.keys()):
                    future.done(block=False, timeout=None)

            # (2) Exit loop if all requested resources have been acquired
            if len(tokens) >= consume:
                break

            try:
                # (3) If any slot immediately available grab then GOTO (1)
                tokens.append(self._slots.get(block=False, timeout=None))
                continue
            except Empty:
                # (4) Only report failure in (3) non-blocking or deadline hit
                if not block or time.monotonic() >= deadline:
                    raise

            # (5) Block until either some work completes or deadline hit
            # Beware that completed work will requires callbacks from (1)
            assert block, "Sanity check control flow"
            if self._future_sentinels:
                multiprocessing.connection.wait(
                    list(self._future_sentinels.values()),
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
            args = tuple(args) if args else ()
            process = self._context.Process(
                target=Jobserver._worker_entrypoint,
                args=((send, fn) + args),
                kwargs=kwargs if kwargs else {},
                daemon=False,
            )
            future = Future(process, recv)
            process.start()
            # Prepare to track the Future and the wait(...)-able sentinel
            registered[future] = process.sentinel
        except:  # noqa:E722
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
            future.add_done_callback(
                self._slots.put_nowait, tokens.pop(0), _Future__internal=True
            )

        # When a Future has completed, no longer track it within Jobserver.
        # Remove, versus discard, chosen to confirm removals previously known.
        future.add_done_callback(
            self._future_sentinels.pop, future, _Future__internal=True
        )

        return future

    @staticmethod
    def _worker_entrypoint(send, fn, *args, **kwargs) -> None:
        # Wrapper usage tracks whether a value was returned or raised
        # in degenerate case where client code returns an Exception.
        try:
            result = Wrapper(result=fn(*args, **kwargs))
        except Exception as exception:
            result = Wrapper(raised=exception)
        finally:
            send.send(result)
            send.close()


###########################################################################
# TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS 3
###########################################################################
# TODO Test non-blocking as expected
# TODO Test processes inside processes
# TODO Hide queue.Empty from the user?
# TODO Usage examples within the module docstring
# TODO Apply black formatter
# TODO Satisfy flake8
# TODO Lambdas as work?  Think pickling woes prevent it...


class JobserverTest(unittest.TestCase):
    """Unit tests (doubling as examples) for Jobserver/Future/Wrapper."""

    def test_defaults(self):
        """Default construction and __call__ shorthand ok?"""
        js = Jobserver()
        f = js(len, (1, 2, 3))
        g = js(str, object=2)
        self.assertEqual("2", g.result())
        self.assertEqual(3, f.result())

    @staticmethod
    def helper_callback(lizt, index, increment):
        """Helper permitting tests to observe callbacks firing."""
        lizt[index] += increment

    def test_basic(self):
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
                    f.add_done_callback(self.helper_callback, mutable, 0, 1)
                    g = js.submit(
                        fn=str,
                        kwargs=dict(object=2),
                        block=True,
                        callbacks=False,
                        consume=1,
                    )
                    g.add_done_callback(self.helper_callback, mutable, 1, 2)
                    g.add_done_callback(self.helper_callback, mutable, 1, 3)
                    h = js.submit(
                        fn=len,
                        args=((1,),),
                        block=True,
                        callbacks=False,
                        consume=1,
                    )
                    h.add_done_callback(
                        self.helper_callback,
                        lizt=mutable,
                        index=2,
                        increment=7,
                    )

                    # Try too much work given fixed slot count
                    with self.assertRaises(Empty):
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
                    with self.assertRaises(Empty):
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
                    h.add_done_callback(
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

    def test_heavyusage(self):
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
                ]
                fs = [
                    js.submit(fn=len, args=("x" * i,), **(kwargs[i % 2]))
                    for i in range(10 * slots)
                ]

                # Confirm all work completed
                for i, f in enumerate(fs):
                    self.assertEqual(i, f.result(block=True))

    @staticmethod
    def helper_none():
        """Nullary helper returning None."""
        return None

    # Explicitly tested because of handling woes observed in other designs
    def test_returns_none(self):
        """None can be returned from a Future?"""
        for method in multiprocessing.get_all_start_methods():
            with self.subTest(method=method):
                context = multiprocessing.get_context(method)
                js = Jobserver(context=context, slots=3)
                f = js.submit(fn=self.helper_none, args=(), block=True)
                self.assertIsNone(f.result())

    @staticmethod
    def helper_return(arg):
        """Helper returning its lone argument."""
        return arg

    # Explicitly tested because of handling woes observed in other designs
    def test_returns_not_raises_exception(self):
        """An Exception can be returned, not raised, from a Future?"""
        for method in multiprocessing.get_all_start_methods():
            with self.subTest(method=method):
                context = multiprocessing.get_context(method)
                js = Jobserver(context=context, slots=3)
                e = Exception("Returned by method {}".format(method))
                f = js.submit(fn=self.helper_return, args=(e,), block=True)
                self.assertEqual(type(e), type(f.result()))
                self.assertEqual(e.args, f.result().args)

    @staticmethod
    def helper_raise(klass, *args):
        """Helper raising the requested Exception class."""
        raise klass(*args)

    def test_raises(self):
        """Future.result() raises Exceptions thrown while processing work?"""
        for method in multiprocessing.get_all_start_methods():
            with self.subTest(method=method):
                # Prepare how callbacks will be observed
                mutable = [0]

                # Prepare work interleaving exceptions and success cases
                context = multiprocessing.get_context(method)
                js = Jobserver(context=context, slots=3)

                # Confirm exception is raised repeatedly
                f = js.submit(
                    fn=self.helper_raise,
                    args=(ArithmeticError, "message123"),
                    block=True,
                )
                f.add_done_callback(self.helper_callback, mutable, 0, 1)
                with self.assertRaises(ArithmeticError):
                    f.result()
                self.assertEqual(mutable[0], 1, "One callback observed")
                f.add_done_callback(self.helper_callback, mutable, 0, 2)
                self.assertEqual(mutable[0], 3, "Callback after done")
                with self.assertRaises(ArithmeticError):
                    f.result()
                self.assertTrue(f.done())
                self.assertEqual(mutable[0], 3, "Callback idempotent")

                # Confirm other work processed without issue
                g = js.submit(fn=str, kwargs=dict(object=2), block=True)
                self.assertEqual("2", g.result())

    def test_done_callback_raises(self):
        """Future.done() raises Exceptions thrown while processing work?"""
        for method in multiprocessing.get_all_start_methods():
            with self.subTest(method=method):
                context = multiprocessing.get_context(method)
                js = Jobserver(context=context, slots=3)

                # Calling done() repeatedly correctly reports multiple errors
                f = js.submit(fn=len, args=(("hello",)), block=True)
                f.add_done_callback(self.helper_raise, ArithmeticError, "123")
                f.add_done_callback(self.helper_raise, ZeroDivisionError, "45")
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
                    f.add_done_callback(self.helper_raise, UnicodeError, "67")
                self.assertIsInstance(c.exception.__cause__, UnicodeError)
                self.assertTrue(f.done(block=False))

                # After callbacks have completed, result is still available.
                self.assertEqual(f.result(), 5)
                self.assertEqual(f.result(), 5)

    @staticmethod
    def helper_signal(sig):
        """Helper sending signal sig to the current process."""
        os.kill(os.getpid(), sig)
        assert False, "Unreachable"

    # TODO Permit sequence of children all killed by signals (i.e. uncomment)
    # TODO Behavior of done() in these scenarios, both w/ and w/o blocking?
    # TODO Behavior of result() in these scenarios, both w/ and w/o blocking?
    # TODO Confirm callbacks delivered as expected
    def test_signalled(self):
        """Signal receipt by worker can be detected via Future?"""
        for method in multiprocessing.get_all_start_methods():
            with self.subTest(method=method):
                # Prepare jobs with workers possibly receiving signals
                context = multiprocessing.get_context(method)
                js = Jobserver(context=context, slots=1)
                f = js.submit(fn=self.helper_signal, args=(signal.SIGKILL,))
                # g = js.submit(fn=self.helper_signal, args=(signal.SIGINT,))
                # h = js.submit(fn=self.helper_signal, args=(signal.SIGTERM, ))
                # i = js.submit(fn=len, args=(("The jig is up!",)))
                # j = js.submit(fn=self.helper_signal, args=(signal.SIGUSR1, ))

                # Confirm either results or signals available in reverse order
                # with self.assertRaises(SubmissionDied):
                #     j.result()
                # self.assertEqual(14, i.result())
                # with self.assertRaises(SubmissionDied):
                #     h.result()
                # with self.assertRaises(SubmissionDied):
                #     g.result()
                with self.assertRaises(SubmissionDied):
                    f.result()


if __name__ == "__main__":
    unittest.main()
