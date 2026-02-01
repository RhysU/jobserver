# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Tests for the Jobserver and related classes."""
import contextlib
import copy
import itertools
import os
import pickle
import signal
import time
import typing
import unittest

from multiprocessing import get_all_start_methods, get_context

from .impl import (
    Blocked,
    CallbackRaised,
    Future,
    Jobserver,
    MinimalQueue,
    SubmissionDied,
    noop,
)

T = typing.TypeVar("T")


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

    # TODO Can the "method != fork" clause be relaxed?
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

    # No behavioral assertions made around pickling, however.
    # Indeed, disabling pickling MinimalQueues empirically breaks many tests.
    def test_duplication_minimalqueue(self) -> None:
        """Copying of MinimalQueue is explicitly allowed."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                mq1 = MinimalQueue(context=method)  # type: MinimalQueue[int]
                mq2 = copy.copy(mq1)
                mq3 = copy.deepcopy(mq1)
                mq1.put(1)
                mq2.put(2)
                mq3.put(3)
                self.assertEqual(1, mq3.get())
                self.assertEqual(2, mq2.get())
                self.assertEqual(3, mq1.get())
                # Though copying is allowed, it is degenerate in that
                # copy.copy(...) and copy.deepcopy(...) return the original.
                self.assertIs(mq1, mq2)
                self.assertIs(mq1, mq3)

    # Motivated by multiprocessing.Connection mentioning a possible 32MB limit
    def test_large_objects(self) -> None:
        """Confirm increasingly large objects can be processed."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
                for size in (1 << i for i in range(22, 28)):  # 2**27 is 128 MB
                    with self.subTest(size=size):
                        f = js.submit(fn=bytearray, args=(size,))
                        x = f.result()
                        self.assertEqual(len(x), size)

    @contextlib.contextmanager
    def assert_elapsed(self, minimum: float):  # type: ignore
        """Asserts a 'with' block required at least minimum seconds to run."""
        start = time.monotonic()
        yield
        elapsed = time.monotonic() - start
        self.assertGreaterEqual(elapsed, minimum, "Not enough seconds elapsed")

    @staticmethod
    def helper_nonblocking(mq: MinimalQueue[str]) -> str:
        """Helper for test_nonblocking receiving and returning a handshake."""
        # Timeout allows heavy OS load while also detecting complete breakage
        return mq.get(timeout=60.0)

    def test_nonblocking(self) -> None:
        """Ensure non-blocking done() and submit() logic honors timeouts."""
        for method, check_done in itertools.product(
            get_all_start_methods(), (True, False)
        ):
            with self.subTest(method=method, check_done=check_done):
                context = get_context(method)
                mq = MinimalQueue(context=context)  # type: MinimalQueue[str]
                js = Jobserver(context=context, slots=1)
                delay = 0.1  # Impacts test runtime on the success path

                # Future f stalls until it receives the handshake below
                f = js.submit(fn=self.helper_nonblocking, args=(mq,))

                # Because Future f is stalled, new work not accepted
                with self.assert_elapsed(0), self.assertRaises(Blocked):
                    js.submit(fn=len, args=("abc",), timeout=0)
                with self.assert_elapsed(delay), self.assertRaises(Blocked):
                    js.submit(fn=len, args=("abc",), timeout=delay)

                # Future f reports not done() and adheres to timeouts
                if check_done:
                    with self.assert_elapsed(0):
                        self.assertFalse(f.done(timeout=0))
                    with self.assert_elapsed(delay):
                        self.assertFalse(f.done(timeout=delay))

                # Future f reports no result() and adheres to timeouts
                with self.assert_elapsed(0), self.assertRaises(Blocked):
                    f.result(timeout=0)
                with self.assert_elapsed(delay), self.assertRaises(Blocked):
                    f.result(timeout=delay)

                # Future f has a result() after it receives this handshake
                mq.put("handshake")
                if check_done:
                    self.assertTrue(f.done(timeout=None))
                self.assertEqual(f.result(timeout=None), "handshake")
                self.assertEqual(f.result(timeout=0), "handshake")

    def test_jobserver_as_submit_argument(self) -> None:
        """Ensure instances with in-flight Futures passable as arguments."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                # Submit work so an in-flight Future is being tracked
                js = Jobserver(context=method, slots=1)
                f = js.submit(fn=len, args=((1, 2),))
                # Submit work passing the Jobserver with a live Future
                ks = Jobserver(context=method, slots=1)
                g = ks.submit(fn=len, args=((1, js, js),))
                # Confirm results as expected from each Jobserver
                self.assertEqual(g.result(timeout=None), 3)
                self.assertEqual(f.result(timeout=None), 2)

    # Uses "SENTINEL", not None, because None handling is tested elsewhere
    @staticmethod
    def helper_envget(key: str) -> str:
        """Retrieve os.environ.get(key, "SENTINEL")."""
        return os.environ.get(key, "SENTINEL")

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
                f = js.submit(
                    fn=self.helper_envget, args=(key,), env={key: "5678"}
                )
                g = js.submit(fn=self.helper_envget, args=(key,), env={})
                h = js.submit(
                    fn=self.helper_envget, args=(key,), env={key: "1234"}
                )
                # Notice that i then uses preexec_fn, not env, to set the key.
                i = js.submit(
                    fn=self.helper_envget,
                    args=(key,),
                    preexec_fn=self.helper_preexec_fn,
                )
                # Then j uses both to confirm env updated before preexec_fn.
                j = js.submit(
                    fn=self.helper_envget,
                    args=(key,),
                    preexec_fn=self.helper_preexec_fn,
                    env={key: "OVERWRITTEN"},
                )
                # Next, k sets then unsets to check unsetting and Iterables.
                k = js.submit(
                    fn=self.helper_envget,
                    args=(key,),
                    env=((key, "OVERWRITTEN"), (key, None)),
                )
                # Variable l is skipped because flake8 complains otherwise
                # Lastly, m confirms removal of a possibly pre-existing key
                m = js.submit(
                    fn=self.helper_envget,
                    args=(key,),
                    env=((key, None),),
                )
                # Checking the various results in an arbitrary order
                self.assertEqual("PREEXEC_FN", j.result())
                self.assertEqual("SENTINEL", m.result())
                self.assertEqual("PREEXEC_FN", i.result())
                self.assertEqual("1234", h.result())
                self.assertEqual("SENTINEL", g.result())
                self.assertEqual("5678", f.result())
                self.assertEqual("SENTINEL", k.result())

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
