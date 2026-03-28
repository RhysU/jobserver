# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Core Jobserver submit/result behavior.

Exercises the Jobserver API directly, without the JobserverExecutor
wrapper, to verify the fundamental submit-then-result contract under
normal operation, slot exhaustion, timeouts, and edge-case payloads.
"""
import contextlib
import copy
import itertools
import pickle
import sys
import time
import typing
import unittest
from multiprocessing import get_all_start_methods, get_context

from jobserver import (
    Blocked,
    Jobserver,
    MinimalQueue,
)

from .helpers import (
    helper_callback,
    helper_nonblocking,
    helper_recurse,
    helper_return,
)


class TestJobserverBasic(unittest.TestCase):
    """Core Jobserver submit/result behavior."""

    def test_defaults(self) -> None:
        """Default construction and __call__ shorthand ok?"""
        js = Jobserver()
        f = js(len, (1, 2, 3))
        g = js(str, object=2)
        # Python 3.14+ defaults to "forkserver", which requires picklable
        # callables.  Lambdas are only usable with the "fork" start method.
        fn = len if sys.version_info >= (3, 14) else lambda x: len(x)
        h = js(fn, (1, 2, 3, 4))
        self.assertEqual(4, h.result())
        self.assertEqual("2", g.result())
        self.assertEqual(3, f.result())

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
                f.when_done(helper_callback, mutable, 0, 1)
                g = js.submit(
                    fn=str,
                    kwargs=dict(object=2),
                    callbacks=False,
                    consume=1,
                    timeout=None,
                )
                g.when_done(helper_callback, mutable, 1, 2)
                g.when_done(helper_callback, mutable, 1, 3)
                h = js.submit(
                    fn=len,
                    args=((1,),),
                    callbacks=False,
                    consume=1,
                    timeout=None,
                )
                h.when_done(
                    helper_callback, lizt=mutable, index=2, increment=7
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
                    helper_callback, lizt=mutable, index=2, increment=11
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

    # Explicitly tested because of handling woes observed in other designs
    def test_returns_none(self) -> None:
        """None can be returned from a Future?"""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=3)
                f = js.submit(
                    fn=min, args=((),), kwargs=dict(default=None), timeout=None
                )
                self.assertIsNone(f.result())

    # Explicitly tested because of handling woes observed in other designs
    def test_returns_not_raises_exception(self) -> None:
        """An Exception can be returned, not raised, from a Future?"""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=3)
                e = Exception("Returned by method {}".format(method))
                f = js.submit(fn=helper_return, args=(e,), timeout=None)
                self.assertEqual(type(e), type(f.result()))
                self.assertEqual(e.args, f.result().args)

    def test_raises(self) -> None:
        """Future.result() raises Exceptions thrown while processing work?"""
        from .helpers import helper_raise

        for method in get_all_start_methods():
            with self.subTest(method=method):
                # Prepare how callbacks will be observed
                mutable = [0]

                # Prepare work interleaving exceptions and success cases
                js = Jobserver(context=method, slots=3)

                # Confirm exception is raised repeatedly
                f = js.submit(
                    fn=helper_raise,
                    args=(ArithmeticError, "message123"),
                    timeout=None,
                )
                f.when_done(helper_callback, mutable, 0, 1)
                with self.assertRaises(ArithmeticError):
                    f.result()
                self.assertEqual(mutable[0], 1, "One callback observed")
                f.when_done(helper_callback, mutable, 0, 2)
                self.assertEqual(mutable[0], 3, "Callback after done")
                with self.assertRaises(ArithmeticError):
                    f.result()
                self.assertTrue(f.done())
                self.assertEqual(mutable[0], 3, "Callback idempotent")

                # Confirm other work processed without issue
                g = js.submit(fn=str, kwargs=dict(object=2), timeout=None)
                self.assertEqual("2", g.result())

    @contextlib.contextmanager
    def assert_elapsed(self, minimum: float):
        """Asserts a 'with' block required at least minimum seconds to run."""
        start = time.monotonic()
        yield
        elapsed = time.monotonic() - start
        self.assertGreaterEqual(elapsed, minimum, "Not enough seconds elapsed")

    def test_nonblocking(self) -> None:
        """Ensure non-blocking done() and submit() logic honors timeouts."""
        for method, check_done in itertools.product(
            get_all_start_methods(), (True, False)
        ):
            with self.subTest(method=method, check_done=check_done):
                context = get_context(method)
                mq: MinimalQueue[str] = MinimalQueue(context=context)
                js = Jobserver(context=context, slots=1)
                delay = 0.02  # Impacts test runtime on the success path

                # Future f stalls until it receives the handshake below
                f = js.submit(fn=helper_nonblocking, args=(mq,))

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

    def test_heavyusage(self) -> None:
        """Workload saturating the configured slots does not deadlock?"""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                # Prepare workload based on number of available slots
                context = get_context(method)
                slots = 2
                js = Jobserver(context=context, slots=slots)

                # Alternate between submissions with and without timeouts
                kwargs: list[dict[str, typing.Any]] = [
                    dict(callbacks=True, timeout=None),
                    dict(callbacks=True, timeout=1000),
                ]
                fs = [
                    js.submit(fn=len, args=("x" * i,), **(kwargs[i % 2]))
                    for i in range(10 * slots)
                ]

                # Confirm all work completed
                for i, f in enumerate(fs):
                    self.assertEqual(i, f.result(timeout=None))

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
                # Round-trip through __getstate__/__setstate__
                # (bare pickle.dumps fails because Semaphores only
                # allow pickling during process spawning)
                js4 = Jobserver.__new__(Jobserver)
                js4.__setstate__(js1.__getstate__())
                i = js4.submit(fn=len, args=((1, 2),))
                self.assertEqual(2, i.result())

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

    def test_submission_nested(self) -> None:
        """Jobserver resource limits honored during nested submissions."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                context = get_context(method)
                self.assertEqual(
                    0,
                    helper_recurse(
                        js=Jobserver(context=context, slots=3), max_depth=0
                    ),
                    msg="Recursive base case must terminate recursion",
                )
                self.assertEqual(
                    1,
                    helper_recurse(
                        js=Jobserver(context=context, slots=3), max_depth=1
                    ),
                    msg="One inductive step must be possible",
                )
                self.assertEqual(
                    4,
                    helper_recurse(
                        js=Jobserver(context=context, slots=4), max_depth=6
                    ),
                    msg="Recursion is limited by number of available slots",
                )
