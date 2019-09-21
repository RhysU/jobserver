"""
A Jobserver exposing a Future interface built atop multiprocessing.
"""
import atexit
import collections
import multiprocessing
import sys
import time
import typing
import unittest

from queue import Empty, Full

T = typing.TypeVar('T')


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
        self.value = None
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

    # TODO Enforce (not block) => (timeout is None)?
    def done(self, block: bool=True, timeout: float=None) -> bool:
        """
        Is result ready?

        May raise CallbackRaisedException from at most one registered callback.
        See CallbackRaisedException documentation for callback error semantics.
        """
        # Multiple calls to done() may be required to issue all callbacks.
        if self.queue is None:
            self._issue_callbacks()
            return True

        # Attempt to read the result from the underlying queue
        assert self.queue is not None
        try:
            self.value = self.queue.get(block=block, timeout=timeout)
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
        # Otherwise, we might obfuscate bugs within this module's logic.
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

        if isinstance(self.value, Exception):
            raise self.value

        return self.value


# Throughout, put_nowait(...) denotes places where blocking should not happen.
# If debugging, consider any related queue.Full exceptions to be logic errors.
class Jobserver:
    def __init__(self, context, slots: int):
        """
        Wrap some multiprocessing context and allow some number of slots.
        Throughout API, arguments block/timeout follow queue.Queue semantics.
        """
        # Prepare required resources ensuring their LIFO-ordered tear down
        assert context is not None
        self.context = context
        assert isinstance(slots, int) and slots >= 1
        self.slots = self.context.Queue(maxsize=slots)
        atexit.register(self.slots.join_thread)
        atexit.register(self.slots.close)
        self.future_sentinels = {}  # type: typing.Dict[Future, int]

        # Issue one token for each requested slot
        for i in range(slots):
            self.slots.put_nowait(i)

    # TODO Simpler?  Maybe a helper like simpler(fn, *args, **kwargs)?
    # TODO Enforce (not block) => (timeout is None)?
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
        """
        # Argument check, especially args/kwargs as misusage is easy and deadly
        assert fn is not None
        assert args is None or isinstance(args, collections.Sequence)
        assert kwargs is None or isinstance(kwargs, collections.Mapping)
        assert isinstance(block, bool)
        assert isinstance(callbacks, bool)
        assert isinstance(consume, int)

        # Convert timeout into concrete deadline then defensively drop timeout
        if timeout is None:
            # Breakage can occur on Python 3.5 for Inf or sys.float_info.max
            deadline = sys.maxsize / 1000
        else:
            assert isinstance(timeout, float) and timeout >= 0
            deadline = time.monotonic() + timeout
        del timeout

        # Acquire the requested tokens or raise queue.Empty when impossible
        tokens = []
        assert consume == 0 or consume == 1, 'Invalid or deadlock possible'
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
            assert block, 'Sanity check control flow'
            if self.future_sentinels:
                multiprocessing.connection.wait(
                        list(self.future_sentinels.values()),
                        timeout=deadline - time.monotonic())

        # Neither block nor deadline are relevant below
        del block, deadline

        # Now, with required slots consumed, begin consuming resources:
        assert len(tokens) >= consume, 'Sanity check slot acquisition'
        try:
            # Temporarily mutate members to clear known Futures for new worker
            registered, self.future_sentinels = self.future_sentinels, {}
            # Grab resources for processing the submitted work
            queue = self.context.Queue(maxsize=1)
            args = tuple(args) if args else ()
            process = self.context.Process(target=Jobserver._worker_entrypoint,
                                           args=(queue, fn) + args,
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

    # TODO Employ PR_SET_PDEATHSIG so child dies should the parent die
    # TODO Permit running logic prior to beginning requested work?
    # TODO Consider permitting a context manager to be "installed"
    @staticmethod
    def _worker_entrypoint(queue, fn, *args, **kwargs) -> None:
        try:
            result = fn(*args, **kwargs)
        except Exception as exception:
            result = exception
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
# TODO Distinguish between returning an Exception and raising one!


class JobserverTest(unittest.TestCase):
    METHODS = ('forkserver', 'fork', 'spawn')

    @staticmethod
    def helper_callback(lizt, index, increment):
        lizt[index] += increment

    def test_basic(self):
        for method in self.METHODS:
            for check_done in (True, False):
                with self.subTest(method=method, check_done=check_done):
                    # Prepare how callbacks will be observed
                    mutable = [0, 0, 0]

                    # Prepare work filling all slots
                    context = multiprocessing.get_context(method)
                    js = Jobserver(context=context, slots=3)
                    f = js.submit(fn=len, args=((1, 2, 3), ),
                                  block=True, callbacks=False)
                    f.add_done_callback(self.helper_callback, mutable, 0, 1)
                    g = js.submit(fn=str, kwargs=dict(object=2),
                                  block=True, callbacks=False)
                    g.add_done_callback(self.helper_callback, mutable, 1, 2)
                    g.add_done_callback(self.helper_callback, mutable, 1, 3)
                    h = js.submit(fn=len, args=((1, ), ),
                                  block=True, callbacks=False)
                    h.add_done_callback(self.helper_callback,
                                        lizt=mutable, index=2, increment=7)

                    # Try too much work given fixed slot count
                    with self.assertRaises(Empty):
                        js.submit(fn=len, args=((), ),
                                  block=False, callbacks=False)

                    # Confirm results in something other than submission order
                    self.assertEqual('2', g.result())
                    self.assertEqual(mutable[1], 5, 'Two callbacks observed')
                    if check_done:
                        self.assertTrue(f.done())
                    self.assertTrue(h.done())  # No check_done guard!
                    self.assertEqual(mutable[2], 7)
                    self.assertEqual(1, h.result())
                    self.assertEqual(1, h.result(), 'Multiple calls OK')
                    h.add_done_callback(self.helper_callback,
                                        lizt=mutable, index=2, increment=11)
                    self.assertEqual(mutable[2], 18, 'Callback after done')
                    self.assertEqual(1, h.result())
                    self.assertTrue(h.done())
                    self.assertEqual(mutable[2], 18, 'Callbacks idempotent')
                    if check_done:
                        self.assertTrue(g.done())
                        self.assertTrue(g.done(), 'Multiple calls OK')
                    self.assertEqual(3, f.result())
                    self.assertEqual(mutable[0], 1, 'One callback observed')

    # TODO Increase workload beyond available slot count
    def test_heavyusage(self):
        for method in self.METHODS:
            with self.subTest(method=method):
                # Prepare workload based on number of available slots
                context = multiprocessing.get_context(method)
                slots = 2
                js = Jobserver(context=context, slots=slots)
                fs = [js.submit(fn=len, args=('x' * i, ), block=True)
                      for i in range(slots)]

                # Confirm all work completed
                for i, f in enumerate(fs):
                    self.assertEqual(i, f.result(block=True))

    @staticmethod
    def helper_none():
        return None

    # Explicitly tested because of handling woes observed in other designs
    def test_returns_none(self):
        for method in self.METHODS:
            with self.subTest(method=method):
                context = multiprocessing.get_context(method)
                js = Jobserver(context=context, slots=3)
                f = js.submit(fn=self.helper_none, args=(), block=True)
                self.assertIsNone(f.result())

    @staticmethod
    def helper_raise(klass, *args):
        raise klass(*args)

    def test_raises(self):
        for method in self.METHODS:
            with self.subTest(method=method):
                # Prepare how callbacks will be observed
                mutable = [0]

                # Prepare work interleaving exceptions and success cases
                context = multiprocessing.get_context(method)
                js = Jobserver(context=context, slots=3)

                # Confirm exception is raised repeatedly
                f = js.submit(fn=self.helper_raise,
                              args=(ArithmeticError, 'message123'),
                              block=True)
                f.add_done_callback(self.helper_callback, mutable, 0, 1)
                with self.assertRaises(ArithmeticError):
                    f.result()
                self.assertEqual(mutable[0], 1, 'One callback observed')
                f.add_done_callback(self.helper_callback, mutable, 0, 2)
                self.assertEqual(mutable[0], 3, 'Callback after done')
                with self.assertRaises(ArithmeticError):
                    f.result()
                self.assertTrue(f.done())
                self.assertEqual(mutable[0], 3, 'Callback idempotent')

                # Confirm other work processed without issue
                g = js.submit(fn=str, kwargs=dict(object=2), block=True)
                self.assertEqual('2', g.result())

    def test_done_callback_raises(self):
        for method in self.METHODS:
            with self.subTest(method=method):
                context = multiprocessing.get_context(method)
                js = Jobserver(context=context, slots=3)

                # Calling done() repeatedly correctly reports multiple errors
                f = js.submit(fn=self.helper_none, args=(), block=True)
                f.add_done_callback(self.helper_raise, ArithmeticError, '123')
                f.add_done_callback(self.helper_raise, ZeroDivisionError, '45')
                with self.assertRaises(CallbackRaisedException) as c:
                    f.done(block=True)
                self.assertIsInstance(c.exception.__cause__, ArithmeticError)
                with self.assertRaises(CallbackRaisedException) as c:
                    f.done(block=True)
                self.assertIsInstance(c.exception.__cause__, ZeroDivisionError)
                self.assertTrue(f.done(block=True))
                self.assertTrue(f.done(block=False))

                # Now that work is complete, adding callback raises immediately
                with self.assertRaises(CallbackRaisedException) as c:
                    f.add_done_callback(self.helper_raise, UnicodeError, '67')
                self.assertIsInstance(c.exception.__cause__, UnicodeError)
                self.assertTrue(f.done(block=False))
                # TODO What is the contract from result()?
                # TODO What is the contract if result() called multiple times?


if __name__ == '__main__':
    unittest.main()
