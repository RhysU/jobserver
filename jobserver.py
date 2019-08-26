"""
A Jobserver exposing a Future interface built atop multiprocessing.
"""
import atexit
import collections
import multiprocessing
import queue
import typing
import unittest


T = typing.TypeVar('T')


class CallbackRaisedException(Exception):
    """
    Reports an Exception raised during callback processing for some Future.
    Instances of this exception have non-None __cause__ members per PEP 3154.
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
        self.callbacks = []

    def add_done_callback(self, fn: typing.Callable, *args, **kwargs) -> None:
        """
        Register a function fn for execution after result is ready.

        May raise CallbackRaisedException from at most this new callback.
        """
        self.callbacks.append((fn, args, kwargs))
        if self.queue is None:
            self._issue_callbacks()

    # TODO Test the multiple-callback-raising semantics
    def done(self, block: bool=True, timeout: float=None) -> bool:
        """
        Is result ready?

        May raise CallbackRaisedException from at most one registered callback.
        When raised, done() should be called until it returns True if one
        needs to confirm that all registered callbacks have been issued.
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
        except queue.Empty:
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
        try:
            while self.callbacks:
                fn, args, kwargs = self.callbacks.pop(0)
                fn(*args, **kwargs)
        except Exception as e:
            raise CallbackRaisedException() from e

    def result(self, block: bool=True, timeout: float=None) -> T:
        """
        Obtain result when ready.

        Raises CallbackRaisedException once for each callback that raised.
        """
        if not self.done(block=block, timeout=timeout):
            raise queue.Empty()

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
        assert slots >= 0
        self.context = context
        self.slots = self.context.Queue(maxsize=slots)
        atexit.register(self.slots.join_thread)
        atexit.register(self.slots.close)

        # Issue one token for each requested slot
        for i in range(slots):
            self.slots.put_nowait(i)

    # TODO Prior to consuming tokens, scan for any previously done Future.
    # TODO Simpler?  Maybe a helper like simpler(fn, *args, **kwargs)?
    def submit(
        self,
        fn: typing.Callable[..., T],
        args: typing.Sequence = None,
        kwargs: typing.Dict[str, typing.Any] = None,
        block: bool=True,
        timeout: float=None,
        consume: int=1,
    ) -> Future[T]:
        """Submit running fn(*args, **kwargs) to this Jobserver.

        Non-blocking usage per block/timeout possibly raises Queue.Empty.
        When consume == 0, no job slot is consumed by the submission.
        """
        # Sanity check args and kwargs as misusage is easy and deadly
        assert args is None or isinstance(args, collections.Sequence)
        assert kwargs is None or isinstance(kwargs, collections.Mapping)

        # Possibly consume one slot prior to acquiring any resources
        tokens = []
        assert consume is 0 or consume is 1, 'Invalid or deadlock possible'
        # TODO Check if any prior work has completed thus freeing slots.
        # TODO Somewhat subtle/fun to account for blocking/timeout.
        # TODO May change the semantics around test_basic
        for _ in range(consume):
            tokens.append(self.slots.get(block=block, timeout=timeout))
        try:
            # Now, with a slot consumed, begin consuming resources...
            queue = self.context.Queue(maxsize=1)
            args = tuple(args) if args else ()
            process = self.context.Process(target=Jobserver._worker_entrypoint,
                                           args=(queue, fn) + args,
                                           kwargs=kwargs if kwargs else {},
                                           daemon=False)
            future = Future(process, queue)
            process.start()
        except:
            # ...unwinding the consumed slot on unexpected errors.
            while tokens:
                self.slots.put_nowait(tokens.pop(0))
            raise

        # As the above process.start() succeeded, now Future restores tokens.
        # This choice causes token release only after future.process.join()
        # from within Future.done().  It keeps _worker_entrypoint() simple.
        while tokens:
            future.add_done_callback(self.slots.put_nowait, tokens.pop(0))

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
                    f = js.submit(fn=len, args=((1, 2, 3), ), block=True)
                    f.add_done_callback(self.helper_callback, mutable, 0, 1)
                    g = js.submit(fn=str, kwargs=dict(object=2), block=True)
                    g.add_done_callback(self.helper_callback, mutable, 1, 2)
                    g.add_done_callback(self.helper_callback, mutable, 1, 3)
                    h = js.submit(fn=len, args=((1, ), ), block=True)
                    h.add_done_callback(self.helper_callback,
                                        lizt=mutable, index=2, increment=7)

                    # Try too much work given fixed slot count
                    with self.assertRaises(queue.Empty):
                        js.submit(fn=len, args=((), ), block=False)

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

    def test_callback_raises(self):
        for method in self.METHODS:
            with self.subTest(method=method):
                context = multiprocessing.get_context(method)
                js = Jobserver(context=context, slots=3)
                f = js.submit(fn=self.helper_none, args=(), block=True)
                f.add_done_callback(self.helper_raise, ArithmeticError, '123')
                with self.assertRaises(CallbackRaisedException) as cm:
                    f.done(block=True)
                self.assertIsInstance(cm.exception.__cause__, ArithmeticError)
                # TODO What is the contract from result()?
                # TODO What is the contract if result() called multiple times?
                # TODO What is the contract if done() called multiple times?
                # TODO What happens to later callbacks after the first fails?


if __name__ == '__main__':
    unittest.main()
