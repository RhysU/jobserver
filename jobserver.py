#!/usr/bin/env python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
A Jobserver exposing a Future interface built atop multiprocessing.
"""
import atexit
import collections.abc
import multiprocessing
import time
import typing
import unittest

from queue import Empty, Full

T = typing.TypeVar("T")


class CallbackRaisedException(Exception):
    """
    Reports an Exception raised from callbacks registered with a Future.

    Instances of this type must have non-None __cause__ members (see PEP 3154).
    The __cause__ member will be the Exception raised by client code.

    When raised by some method, e.g. by Future.done(...) or by
    Future.result(...), the caller MAY choose to re-invoke that same
    method immediately to continue processing any additional callbacks.
    If the caller requires that all callbacks are attempted, the caller
    MUST re-invoke the same method until no CallbackRaisedException occurs.
    These MAY/MUST semantics allow the caller to decide how much additional
    processing to perform after seeing the 1st, 2nd, or N-th error.
    """
    pass


class Wrapper(typing.Generic[T]):
    """Allows Futures to track whether a value was raised or returned."""
    __slots__ = ("result", "raised")

    def __init__(
        self,
        *,
        result: typing.Optional[T]=None,
        raised: typing.Optional[Exception]=None
    ) -> None:
        assert raised is None or result is None, "Both disallowed"
        self.result = result
        self.raised = raised

    def unwrap(self) -> T:
        """Raise any wrapped Exception otherwise return the result."""
        if self.raised is not None:
            raise self.raised
        return self.result

    def __repr__(self):
        return ("Wrapper(result={!r}, raised={!r})"
                .format(self.result, self.raised))


class Future(typing.Generic[T]):
    """
    A Future anticipating one result in a Queue emitted by some Process.
    Throughout API, arguments block/timeout follow queue.Queue semantics.
    """
    def __init__(
        self, process: multiprocessing.Process, queue: multiprocessing.Queue
    ) -> None:
        assert process is not None  # None after Process.join(...)
        assert queue is not None  # None after result read and Queue.close(...)
        self.process = process
        self.queue = queue
        self.wrapper = None
        self.callbacks = []  # Tuples per add_done_callback, _issue_callbacks

    def add_done_callback(
            self, fn: typing.Callable, *args, __internal: bool=False, **kwargs
        ) -> None:
        """
        Register a function for execution sometime after Future.done(...).

        When already done(...), will immediately invoke the requested function.
        May raise CallbackRaisedException from at most this new callback.
        """
        self.callbacks.append((__internal, fn, args, kwargs))
        if self.queue is None:
            self._issue_callbacks()

    def done(
        self,
        block: bool=True,
        timeout: typing.Optional[float]=None
    ) -> bool:
        """
        Is result ready?

        Argument timeout can only be specified for blocking operations.
        When specified, timeout is given in seconds and must be non-negative.
        May raise CallbackRaisedException from at most one registered callback.
        See CallbackRaisedException documentation for callback error semantics.
        """
        # Sanity check requested block/timeout
        if timeout is not None:
            assert block, "Non-blocking operation cannot specify a timeout"
            assert timeout >= 0, "Blocking allows only a non-negative timeout"

        # Multiple calls to done() may be required to issue all callbacks.
        if self.queue is None:
            self._issue_callbacks()
            return True

        # Attempt to read the result Wrapper from the underlying queue
        assert self.queue is not None
        try:
            self.wrapper = self.queue.get(block=block, timeout=timeout)
            assert isinstance(self.wrapper, Wrapper), "Confirm invariant"
            self.process.join()
            self.process = None  # Allow reclaiming via garbage collection
        except Empty:
            return False

        # Empirically, closing self.queue after callbacks (in particular,
        # those registered by Jobserver.submit(...) restoring tokens to
        # resource-tracking slots) *reduces* sporadic BrokenPipeErrors
        # (SIGPIPEs) which otherwise occur.  Unsatisfying but pragmatic.
        #
        # Callback must observe "self.queue is None" (supposing someone
        # registers some callback using this Future) otherwise our grubby
        # empiricism around avoiding SIGPIPE "leaks" in treatment below.
        queue, self.queue = self.queue, None
        try:
            self._issue_callbacks()
        finally:
            queue.close()
            queue.join_thread()

        return True

    def _issue_callbacks(self):
        # Only a non-internal callback may cause CallbackRaisedException.
        # Otherwise, we might obfuscate bugs within this module"s logic.
        while self.callbacks:
            internal, fn, args, kwargs = self.callbacks.pop(0)
            if internal:
                fn(*args, **kwargs)
            else:
                try:
                    fn(*args, **kwargs)
                except Exception as e:
                    raise CallbackRaisedException() from e

    def result(self, block: bool=True, timeout: float=None) -> T:
        """
        Obtain result when ready.

        May raise CallbackRaisedException from at most one registered callback.
        See CallbackRaisedException documentation for callback error semantics.
        """
        if not self.done(block=block, timeout=timeout):
            raise Empty()

        return self.wrapper.unwrap()


# Throughout, put_nowait(...) denotes places where blocking should not happen.
# If debugging, consider any related queue.Full exceptions to be logic errors.
class Jobserver:
    """
    A Jobserver exposing a Future interface built atop multiprocessing.
    Throughout methods, arguments block/timeout follow queue.Queue semantics.
    """

    def __init__(
        self,
        context: typing.Optional[multiprocessing.context.BaseContext] = None,
        slots: typing.Optional[int] = None
    ) -> None:
        """
        Wrap some multiprocessing context and allow some number of slots.

        When not provided, context defaults to multiprocessing.get_context().
        When not provided, slots defaults to (multiprocessing.cpu_count() + 1).
        """
        # Prepare required resources ensuring their LIFO-ordered tear down
        if context is None:
            context = multiprocessing.get_context()
        self.context = context
        if slots is None:
            slots = self.context.cpu_count() + 1
        assert isinstance(slots, int) and slots >= 1
        self.slots = self.context.Queue(maxsize=slots)
        atexit.register(self.slots.join_thread)
        atexit.register(self.slots.close)
        self.future_sentinels = {}  # type: typing.Dict[Future, int]

        # Issue one token for each requested slot
        for i in range(slots):
            self.slots.put_nowait(i)

    def __call__(
        self,
        fn: typing.Callable[..., T],
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> Future[T]:
        """Submit running submitting fn(*args, **kwargs) to this Jobserver.

        Shorthand for calling submit(fn=fn, args=*args, kwargs=**kwargs),
        with all submission semantics per that methods default arguments.
        """
        return self.submit(fn=fn, args=args, kwargs=kwargs)

    def submit(
        self,
        fn: typing.Callable[..., T],
        *,
        args: typing.Sequence=None,
        kwargs: typing.Dict[str, typing.Any]=None,
        block: bool=True,
        callbacks: bool=True,
        consume: int=1,
        timeout: typing.Optional[float]=None
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
                for future in list(self.future_sentinels.keys()):
                    future.done(block=False, timeout=None)

            # (2) Exit loop if all requested resources have been acquired
            if len(tokens) >= consume:
                break

            try:
                # (3) If any slot immediately available grab then GOTO (1)
                tokens.append(self.slots.get(block=False, timeout=None))
                continue
            except Empty:
                # (4) Only report failure in (3) non-blocking or deadline hit
                if not block or time.monotonic() >= deadline:
                    raise

            # (5) Block until either some work completes or deadline hit
            # Beware that completed work will requires callbacks from (1)
            assert block, "Sanity check control flow"
            if self.future_sentinels:
                multiprocessing.connection.wait(
                        list(self.future_sentinels.values()),
                        timeout=deadline - time.monotonic())

        # Neither block nor deadline are relevant below
        del block, deadline

        # Now, with required slots consumed, begin consuming resources:
        assert len(tokens) >= consume, "Sanity check slot acquisition"
        try:
            # Temporarily mutate members to clear known Futures for new worker
            registered, self.future_sentinels = self.future_sentinels, {}
            # Grab resources for processing the submitted work
            queue = self.context.Queue(maxsize=1)
            args = tuple(args) if args else ()
            process = self.context.Process(target=Jobserver._worker_entrypoint,
                                           args=((queue, fn) + args),
                                           kwargs=kwargs if kwargs else {},
                                           daemon=False)
            future = Future(process, queue)
            process.start()
            # Prepare to track the Future and the wait(...)-able sentinel
            registered[future] = process.sentinel
        except:
            # Unwinding any consumed slots on unexpected errors
            while tokens:
                self.slots.put_nowait(tokens.pop(0))
            raise
        finally:
            # Re-mutate members to restore known-Future tracking
            self.future_sentinels = registered
            del registered

        # As the above process.start() succeeded, now Future restores tokens.
        # This choice causes token release only after future.process.join()
        # from within Future.done().  It keeps _worker_entrypoint() simple.
        # Restoring tokens MUST occur before Future unregistered (just below).
        while tokens:
            future.add_done_callback(self.slots.put_nowait, tokens.pop(0),
                                     _Future__internal=True)

        # When a Future has completed, no longer track it within Jobserver.
        # Remove, versus discard, chosen to confirm removals previously known.
        future.add_done_callback(self.future_sentinels.pop, future,
                                 _Future__internal=True)

        return future

    @staticmethod
    def _worker_entrypoint(queue, fn, *args, **kwargs) -> None:
        # Wrapper usage tracks whether a value was returned or raised
        # in degenerate case where client code returns an Exception.
        try:
            result = Wrapper(result=fn(*args, **kwargs))
        except Exception as exception:
            result = Wrapper(raised=exception)
        finally:
            queue.put_nowait(result)
            queue.close()
            queue.join_thread()


###########################################################################
### TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS
###########################################################################
# TODO Test non-blocking as expected
# TODO Test processes inside processes
# TODO Hide queue.Empty() and queue.Full() from the user?
# TODO Craft __all__ hiding all uninteresting details, especially tests.
# TODO Consider using __slots__ to better document/lock-down allowed members
# TODO Usage examples within the module docstring
# TODO Apply black formatter
# TODO Satisfy flake8
# TODO What if child process receives SIGTERM or SIGKILL?
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
                    f = js.submit(fn=len, args=((1, 2, 3), ),
                                  block=True, callbacks=False, consume=1)
                    f.add_done_callback(self.helper_callback, mutable, 0, 1)
                    g = js.submit(fn=str, kwargs=dict(object=2),
                                  block=True, callbacks=False, consume=1)
                    g.add_done_callback(self.helper_callback, mutable, 1, 2)
                    g.add_done_callback(self.helper_callback, mutable, 1, 3)
                    h = js.submit(fn=len, args=((1, ), ),
                                  block=True, callbacks=False, consume=1)
                    h.add_done_callback(self.helper_callback,
                                        lizt=mutable, index=2, increment=7)

                    # Try too much work given fixed slot count
                    with self.assertRaises(Empty):
                        js.submit(fn=len, args=((), ),
                                  block=False, callbacks=False, consume=1)

                    # Confirm zero-consumption requests accepted immediately
                    i = js.submit(fn=len, args=((1, 2, 3, 4), ),
                                  block=False, callbacks=False, consume=0)

                    # Again, try too much work given fixed slot count
                    with self.assertRaises(Empty):
                        js.submit(fn=len, args=((), ),
                                  block=False, callbacks=False, consume=1)

                    # Confirm results in something other than submission order
                    self.assertEqual("2", g.result())
                    self.assertEqual(mutable[1], 5, "Two callbacks observed")
                    if check_done:
                        self.assertTrue(f.done())
                    self.assertTrue(h.done())  # No check_done guard!
                    self.assertEqual(mutable[2], 7)
                    self.assertEqual(1, h.result())
                    self.assertEqual(1, h.result(), "Multiple calls OK")
                    h.add_done_callback(self.helper_callback,
                                        lizt=mutable, index=2, increment=11)
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
                kwargs = [dict(block=True, callbacks=True, timeout=None),
                          dict(block=True, callbacks=True, timeout=1000)]
                fs = [js.submit(fn=len, args=("x" * i, ), **(kwargs[i % 2]))
                      for i in range(10 * slots)]

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
                f = js.submit(fn=self.helper_raise,
                              args=(ArithmeticError, "message123"),
                              block=True)
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
                f = js.submit(fn=len, args=(("hello", )), block=True)
                f.add_done_callback(self.helper_raise, ArithmeticError, "123")
                f.add_done_callback(self.helper_raise, ZeroDivisionError, "45")
                with self.assertRaises(CallbackRaisedException) as c:
                    f.done(block=True)
                self.assertIsInstance(c.exception.__cause__, ArithmeticError)
                with self.assertRaises(CallbackRaisedException) as c:
                    f.done(block=True)
                self.assertIsInstance(c.exception.__cause__, ZeroDivisionError)
                self.assertTrue(f.done(block=True))
                self.assertTrue(f.done(block=False))

                # After callbacks have completed, the result is available.
                self.assertEqual(f.result(), 5)
                self.assertEqual(f.result(), 5)

                # Now that work is complete, adding callback raises immediately
                with self.assertRaises(CallbackRaisedException) as c:
                    f.add_done_callback(self.helper_raise, UnicodeError, "67")
                self.assertIsInstance(c.exception.__cause__, UnicodeError)
                self.assertTrue(f.done(block=False))

                # After callbacks have completed, result is still available.
                self.assertEqual(f.result(), 5)
                self.assertEqual(f.result(), 5)

if __name__ == "__main__":
    unittest.main()
