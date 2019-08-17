"""
A Jobserver exposing a Future interface built atop multiprocessing.
"""
import atexit
import collections
import multiprocessing
import queue
import typing


T = typing.TypeVar('T')


class Future(typing.Generic[T]):
    """
    A Future anticipating one result in a Queue emitted by some Process.
    Throughout API, arguments block/timeout follow queue.Queue semantics.
    """
    def __init__(
        self, process: multiprocessing.Process, queue:multiprocessing.Queue
    ) -> None:
        assert process is not None  # None after Process.join(...)
        assert queue is not None  # None after result read and Queue.close(...)
        self.process = process
        self.queue = queue
        self.value = None
        self.callbacks = []

    def add_done_callback(self, fn: typing.Callable, *args, **kwargs) -> None:
        """Register a function fn for execution after result is ready."""
        if self.queue is None:
            fn(*args, **kwargs)
        else:
            self.callbacks.append((fn, args, kwargs))

    def done(self, block: bool=True, timeout: float=None) -> bool:
        """Is result ready?"""
        if self.queue is not None:
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
                while self.callbacks:
                    fn, args, kwargs = self.callbacks.pop(0)
                    fn(*args, **kwargs)
            finally:
                queue.close()
                queue.join_thread()

        return True

    def result(self, block: bool=True, timeout: float=None) -> T:
        """Obtain result when ready."""
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

import unittest

class JobserverTest(unittest.TestCase):
    METHODS = ('forkserver', 'fork', 'spawn')

    def test_basic(self):
        for method in self.METHODS:
            for check_done in (True, False):
                with self.subTest(method=method, check_done=check_done):
                    # Prepare work filling all slots
                    context = multiprocessing.get_context(method)
                    js = Jobserver(context=context, slots=3)
                    f = js.submit(fn=len, args=((1, 2, 3), ), block=True)
                    g = js.submit(fn=str, kwargs=dict(object=2), block=True)
                    h = js.submit(fn=len, args=((1, ), ), block=True)

                    # Try too much work given fixed slot count
                    self.assertRaises(queue.Empty, js.submit,
                                      fn=len, args=((), ), block=False)

                    # Confirm results in something other than submission order
                    self.assertEqual('2', g.result())
                    if check_done:
                        self.assertTrue(f.done())
                    if check_done:
                        self.assertTrue(h.done())
                    self.assertEqual(1, h.result())
                    if check_done:
                        self.assertTrue(g.done())
                    self.assertEqual(3, f.result())


# TODO Test submitting more work after first batch completes
# TODO Test raising inside work raises outside work
# TODO Test non-blocking as expected
# TODO Test processes inside processes


if __name__ == '__main__':
    unittest.main()
