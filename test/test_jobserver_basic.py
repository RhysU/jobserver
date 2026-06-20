# Copyright (C) 2019-2026 Rhys Ulerich
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
import os
import pickle
import sys
import tempfile
import time
import typing
import unittest
from collections import ChainMap

from jobserver import (
    Blocked,
    Jobserver,
)
from jobserver._queue import SPSCQueue

from .helpers import (
    FAST,
    barrier_wait,
    helper_callback,
    helper_nonblocking,
    helper_noop,
    helper_recurse,
    helper_return,
    helper_return_kwargs,
    start_methods,
)


class UnpickleableMapping(ChainMap):  # type: ignore[type-arg]
    """A Mapping subclass that cannot be pickled."""

    def __reduce__(self) -> typing.NoReturn:
        raise pickle.PicklingError("deliberately unpickleable")


class TestJobserverBasic(unittest.TestCase):
    """Core Jobserver submit/result behavior."""

    def test_defaults(self) -> None:
        """Default construction and the __call__ shorthand work."""
        with Jobserver() as js:
            f = js(len, (1, 2, 3))
            g = js(str, object=2)
            # Python 3.14+ defaults to "forkserver", which requires picklable
            # callables.  Lambdas are only usable with the "fork" start method.
            fn = len if sys.version_info >= (3, 14) else lambda x: len(x)
            h = js(fn, (1, 2, 3, 4))
            self.assertEqual(4, h.result())
            self.assertEqual("2", g.result())
            self.assertEqual(3, f.result())

    def test_call_kwarg_named_fn(self) -> None:
        """__call__ forwards a kwarg named 'fn' without collision."""
        with Jobserver() as js:
            f = js(helper_return_kwargs, fn="sentinel")
            self.assertEqual({"fn": "sentinel"}, f.result())

    def test_basic(self) -> None:
        """Basic submission up to the slot limit fires callbacks."""
        for method, check_done in itertools.product(
            start_methods(), (True, False)
        ):
            with self.subTest(method=method, check_done=check_done):
                # Prepare how callbacks will be observed
                mutable = [0, 0, 0]

                # Use a barrier file to hold worker slots open during
                # Blocked tests; workers block until the file appears.
                with tempfile.TemporaryDirectory() as tmpdir:
                    barrier_path = os.path.join(tmpdir, "go")

                    # Prepare work filling all slots
                    with Jobserver(context=method, slots=3) as js:
                        f = js.submit(
                            fn=barrier_wait,
                            args=(barrier_path,),
                            consume=1,
                            timeout=None,
                        )
                        f.when_done(helper_callback, mutable, 0, 1)
                        g = js.submit(
                            fn=barrier_wait,
                            args=(barrier_path,),
                            consume=1,
                            timeout=None,
                        )
                        g.when_done(helper_callback, mutable, 1, 2)
                        g.when_done(helper_callback, mutable, 1, 3)
                        h = js.submit(
                            fn=barrier_wait,
                            args=(barrier_path,),
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
                                consume=1,
                                timeout=0,
                            )

                        # Confirm zero-consumption requests accepted
                        # immediately
                        i = js.submit(
                            fn=len,
                            args=((1, 2, 3, 4),),
                            consume=0,
                            timeout=0,
                        )

                        # Again, try too much work given fixed slot count
                        with self.assertRaises(Blocked):
                            js.submit(
                                fn=len,
                                args=((),),
                                consume=1,
                                timeout=0,
                            )

                        # Release all workers by creating the barrier file
                        open(barrier_path, "w").close()

                        # Confirm results and callbacks
                        self.assertEqual("released", g.result())
                        self.assertEqual(
                            mutable[1], 5, "Two callbacks observed"
                        )
                        if check_done:
                            self.assertTrue(f.wait())
                        self.assertTrue(h.wait())  # No check_done guard!
                        self.assertEqual(mutable[2], 7)
                        self.assertEqual("released", h.result())
                        self.assertEqual(
                            "released", h.result(), "Multiple calls OK"
                        )
                        h.when_done(
                            helper_callback,
                            lizt=mutable,
                            index=2,
                            increment=11,
                        )
                        self.assertEqual(
                            mutable[2], 18, "Callback after completion"
                        )
                        self.assertEqual("released", h.result())
                        self.assertTrue(h.wait())
                        self.assertEqual(
                            mutable[2], 18, "Callbacks idempotent"
                        )
                        self.assertEqual(
                            4, i.result(), "Zero-consumption request"
                        )
                        if check_done:
                            self.assertTrue(g.wait())
                            self.assertTrue(g.wait(), "Multiple calls OK")
                        self.assertEqual("released", f.result())
                        self.assertEqual(
                            mutable[0], 1, "One callback observed"
                        )
                        self.assertEqual(
                            4, i.result(), "Zero-consumption repeat"
                        )

    # Explicitly tested because of handling woes observed in other designs
    def test_returns_none(self) -> None:
        """None can be returned from a Future."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=3) as js:
                    f = js.submit(
                        fn=min,
                        args=((),),
                        kwargs=dict(default=None),
                        timeout=None,
                    )
                    self.assertIsNone(f.result())

    def test_non_dict_mapping_kwargs(self) -> None:
        """Non-dict Mapping kwargs are coerced to dict for pickling."""
        for method in start_methods():
            with self.subTest(method=method):
                m = UnpickleableMapping({"object": 42})
                self.assertNotIsInstance(m, dict)
                with self.assertRaises(pickle.PicklingError):
                    pickle.dumps(m)
                with Jobserver(context=method, slots=1) as js:
                    f = js.submit(fn=str, kwargs=m, timeout=None)
                    self.assertEqual("42", f.result())

    def test_unpicklable_fn_wrapped(self) -> None:
        """Pickling failures from process.start() raise a clear TypeError."""
        for method in start_methods():
            if method == "fork":
                continue  # fork pickles nothing, so lambdas run fine
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    # A lambda is unpicklable under spawn/forkserver.
                    with self.assertRaises(TypeError) as cm:
                        js.submit(fn=lambda: 42, timeout=None)
                    message = str(cm.exception)
                    self.assertIn("not picklable", message)
                    self.assertIn(method, message)
                    # The slot token was restored, so new work still runs.
                    f = js.submit(fn=str, kwargs=dict(object=7), timeout=None)
                    self.assertEqual("7", f.result())

    # Explicitly tested because of handling woes observed in other designs
    def test_returns_not_raises_exception(self) -> None:
        """An Exception can be returned, not raised, from a Future."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=3) as js:
                    e = Exception(f"Returned by method {method}")
                    f = js.submit(fn=helper_return, args=(e,), timeout=None)
                    self.assertEqual(type(e), type(f.result()))
                    self.assertEqual(e.args, f.result().args)

    def test_raises(self) -> None:
        """Future.result() raises exceptions thrown while processing work."""
        from .helpers import helper_raise

        for method in start_methods():
            with self.subTest(method=method):
                # Prepare how callbacks will be observed
                mutable = [0]

                # Prepare work interleaving exceptions and success cases
                with Jobserver(context=method, slots=3) as js:
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
                    self.assertEqual(
                        mutable[0], 3, "Callback after completion"
                    )
                    with self.assertRaises(ArithmeticError):
                        f.result()
                    self.assertTrue(f.wait())
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
        """Non-blocking wait() and submit() logic honors timeouts."""
        for method, check_done in itertools.product(
            start_methods(), (True, False)
        ):
            with self.subTest(method=method, check_done=check_done):
                with (
                    SPSCQueue(context=method) as mq,
                    Jobserver(context=method, slots=1) as js,
                ):
                    delay = 0.02  # Impacts test runtime on success path

                    # Future f stalls until it receives the handshake
                    f = js.submit(fn=helper_nonblocking, args=(mq,))

                    # Because Future f is stalled, new work not accepted
                    with self.assert_elapsed(0), self.assertRaises(Blocked):
                        js.submit(fn=len, args=("abc",), timeout=0)
                    with (
                        self.assert_elapsed(delay),
                        self.assertRaises(Blocked),
                    ):
                        js.submit(fn=len, args=("abc",), timeout=delay)

                    # Future f reports not done() and adheres to timeouts
                    if check_done:
                        with self.assert_elapsed(0):
                            self.assertFalse(f.done())
                        with self.assert_elapsed(delay):
                            self.assertFalse(f.wait(timeout=delay))

                    # Future f reports no result() and adheres to timeouts
                    with self.assert_elapsed(0), self.assertRaises(Blocked):
                        f.result(timeout=0)
                    with (
                        self.assert_elapsed(delay),
                        self.assertRaises(Blocked),
                    ):
                        f.result(timeout=delay)

                    # Future f has a result() after this handshake
                    mq.put("handshake")
                    if check_done:
                        self.assertTrue(f.wait(timeout=None))
                    self.assertEqual(f.result(timeout=None), "handshake")
                    self.assertEqual(f.result(timeout=0), "handshake")

    def test_reclaim_frees_slots_for_large_unread_results(self) -> None:
        """reclaim_resources() completes children blocked mid-send.

        A child whose pickled result exceeds the pipe buffer (~64KB)
        blocks in send() and never exits, so its process sentinel never
        fires.  Observing the result connection lets reclaim_resources()
        drain the result, unblock the child, restore its slot, and thus
        admit further work without any explicit result()/wait() read.
        Regression test for issue #269.
        """
        for method in start_methods():
            with self.subTest(method=method):
                # One megabyte far exceeds the pipe buffer so each child
                # blocks mid-send() on its result and does not exit.
                payload = b"X" * 1_000_000
                with Jobserver(context=method, slots=2) as js:
                    f1 = js.submit(fn=helper_return, args=(payload,))
                    f2 = js.submit(fn=helper_return, args=(payload,))

                    # Both slots are consumed by children blocked mid-send.
                    # Polling reclaim_resources() alone must complete them
                    # purely off connection readiness, restoring both slots.
                    deadline = time.monotonic() + 30.0
                    while time.monotonic() < deadline:
                        js.reclaim_resources()
                        if "tracked=0" in repr(js):
                            break
                        time.sleep(0.01)
                    self.assertIn("tracked=0", repr(js))

                    # With slots restored a further submission succeeds.
                    f3 = js.submit(fn=helper_return, args=(1,), timeout=10)
                    self.assertEqual(f3.result(timeout=10), 1)

                    # Drained results remain available via the Futures.
                    self.assertEqual(f1.result(timeout=0), payload)
                    self.assertEqual(f2.result(timeout=0), payload)

    def test_heavy_usage(self) -> None:
        """A workload saturating the configured slots does not deadlock."""
        for method in start_methods():
            with self.subTest(method=method):
                # Prepare workload based on number of available slots
                slots = 2
                with Jobserver(context=method, slots=slots) as js:
                    # Alternate between submissions with/without timeouts
                    kwargs: list[dict[str, typing.Any]] = [
                        dict(timeout=None),
                        dict(timeout=1000),
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
        """Increasingly large objects can be processed."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    for size in (
                        1 << i for i in range(22, 26)
                    ):  # 2**25 is 32 MB
                        with self.subTest(size=size):
                            f = js.submit(fn=bytearray, args=(size,))
                            x = f.result()
                            self.assertEqual(len(x), size)

    # TODO Can the "method != fork" clause be relaxed?
    def test_duplication_futures(self) -> None:
        """Copying and pickling of Futures is explicitly disallowed."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=3) as js:
                    f = js.submit(fn=len, args=((1, 2, 3),))
                    # Cannot copy a Future
                    with self.assertRaises(TypeError):
                        copy.copy(f)
                    with self.assertRaises(TypeError):
                        copy.deepcopy(f)
                    # Cannot pickle a Future
                    with self.assertRaises(TypeError):
                        pickle.dumps(f)
                    # Cannot submit a Future as part of additional work
                    # (as a consequence of the above when pickling
                    # required)
                    if method != "fork":
                        with self.assertRaises(TypeError):
                            js.submit(fn=type, args=(f,))
                    if not f.done(timeout=10):
                        self.fail("Future not done within timeout")

    # No behavioral assertions made around pickling, however.
    def test_duplication_jobserver(self) -> None:
        """Copying of Jobservers is explicitly allowed."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=3) as js1:
                    js2 = copy.copy(js1)
                    js3 = copy.deepcopy(js1)
                    f = js1.submit(fn=len, args=((1, 2, 3),))
                    g = js2.submit(fn=len, args=((1, 2, 3, 4),))
                    h = js3.submit(fn=len, args=((1, 2, 3, 4, 5),))
                    self.assertEqual(5, h.result())
                    self.assertEqual(4, g.result())
                    self.assertEqual(3, f.result())
                    # Copies are sibling handles onto the same slot pool, so
                    # they are distinct objects sharing one set of resources.
                    self.assertIsNot(js1, js2)
                    self.assertIsNot(js1, js3)
                    self.assertIs(js1._resources, js2._resources)
                    self.assertIs(js1._resources, js3._resources)
                    # Round-trip through __getstate__/__setstate__
                    # (bare pickle.dumps fails because Semaphores only
                    # allow pickling during process spawning)
                    js4 = Jobserver.__new__(Jobserver)
                    js4.__setstate__(js1.__getstate__())
                    i = js4.submit(fn=len, args=((1, 2),))
                    self.assertEqual(2, i.result())

    def test_nested_sibling_with_keeps_pool_open(self) -> None:
        """An inner with on a sibling handle must not close the shared pool.

        The shared Resources reference-counts context-manager entries, so the
        inner block exiting only decrements; teardown waits for the last open
        with.  A regression that closed on any exit would fail the final
        submit() against the still-open outer handle.
        """
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=2) as js:
                    sibling = js.revise_env({})  # shares js's slots
                    with sibling:
                        self.assertEqual(
                            2, sibling.submit(fn=len, args=((1, 2),)).result()
                        )
                    # sibling.__exit__ must not have torn down the pool.
                    self.assertEqual(
                        3, js.submit(fn=len, args=((1, 2, 3),)).result()
                    )

    def test_jobserver_as_submit_argument(self) -> None:
        """Instances with in-flight Futures are passable as arguments."""
        for method in start_methods():
            with self.subTest(method=method):
                # Submit work so an in-flight Future is being tracked
                with (
                    Jobserver(context=method, slots=1) as js,
                    Jobserver(context=method, slots=1) as ks,
                ):
                    f = js.submit(fn=len, args=((1, 2),))
                    # Submit work passing the Jobserver with a live
                    # Future
                    g = ks.submit(fn=len, args=((1, js, js),))
                    # Confirm results as expected from each Jobserver
                    self.assertEqual(g.result(timeout=None), 3)
                    self.assertEqual(f.result(timeout=None), 2)

    def test_submission_nested(self) -> None:
        """Jobserver resource limits honored during nested submissions."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=3) as js:
                    self.assertEqual(
                        0,
                        helper_recurse(js=js, max_depth=0),
                        msg="Recursive base case must terminate recursion",
                    )
                with Jobserver(context=method, slots=3) as js:
                    self.assertEqual(
                        1,
                        helper_recurse(js=js, max_depth=1),
                        msg="One inductive step must be possible",
                    )
                with Jobserver(context=method, slots=4) as js:
                    self.assertEqual(
                        4,
                        helper_recurse(js=js, max_depth=6),
                        msg="Recursion is limited by number of "
                        "available slots",
                    )

    def test_consume_rejects_non_int(self) -> None:
        """consume accepts only the ints 0/1, not bools/floats (#304)."""
        with Jobserver(slots=2) as js:
            for good in (0, 1):
                self.assertEqual(
                    1,
                    js.submit(
                        fn=helper_return, args=(1,), consume=good
                    ).result(timeout=5),
                )
            for bad in (2, -1, 99, True, False, 1.0, 0.0):
                with self.assertRaises(ValueError):
                    js.submit(fn=helper_return, args=(1,), consume=bad)

    def test_timeout_non_numeric_raises_typeerror(self) -> None:
        """submit()/result() reject a non-numeric timeout clearly (#342)."""
        with Jobserver(slots=2) as js:
            for bad in ("5", object()):
                with self.assertRaises(TypeError):
                    js.submit(fn=helper_return, args=(1,), timeout=bad)
            f = js.submit(fn=helper_return, args=(7,))
            with self.assertRaises(TypeError):
                f.result(timeout="soon")
            self.assertEqual(7, f.result(timeout=5))

    def test_context_manager(self) -> None:
        """Context manager closes slots; submit raises after exit."""
        with Jobserver(slots=2) as js:
            f = js.submit(fn=helper_return, args=(42,))
            self.assertEqual(42, f.result())
        with self.assertRaisesRegex(RuntimeError, "Jobserver is closed"):
            js.submit(fn=helper_return, args=(1,))
        # Re-entering a closed Jobserver raises the same defined error
        with self.assertRaisesRegex(RuntimeError, "Jobserver is closed"):
            with js:
                pass

    def test_exit_is_idempotent(self) -> None:
        """A second __exit__ is a no-op, not RuntimeError (#331)."""
        js = Jobserver(slots=2)
        js.__enter__()
        self.assertEqual(7, js.submit(fn=abs, args=(-7,)).result(timeout=5))
        js.__exit__(None, None, None)
        # Second (and third) close must not raise "Jobserver is closed".
        js.__exit__(None, None, None)
        js.__exit__(None, None, None)

    def test_nested_with_same_instance(self) -> None:
        """Nested with on one instance: outer __exit__ must no-op (#346)."""
        js = Jobserver(slots=2)
        with js:
            with js:  # inner __exit__ closes the shared instance
                self.assertEqual(
                    1, js.submit(fn=helper_return, args=(1,)).result(timeout=5)
                )
            # Outer __exit__ now runs on an already-closed instance and must
            # be a harmless no-op rather than raising.

    def test_context_manager_lingering_future(self) -> None:
        """Lingering Future completes and fires callbacks after exit."""
        results: list[typing.Any] = []
        with Jobserver(slots=2) as js:
            f = js.submit(fn=helper_return, args=(99,))
        # Future is still live after exit; its result and callbacks must work
        f.when_done(lambda: results.append("fired"))
        self.assertEqual(99, f.result())
        self.assertEqual(["fired"], results)

    def test_cleanup_callback_after_selector_close(self) -> None:
        """Cleanup callback tolerates a closed selector after __exit__."""
        # Submit a slow task so the future is still running at __exit__
        with Jobserver(slots=2) as js:
            f = js.submit(fn=time.sleep, args=(0.5,), timeout=5)
        # __exit__ closed the selector; result() fires all callbacks
        # including the _deregister_sentinel cleanup callback
        self.assertIsNone(f.result(timeout=5))

    def test_token_restored_after_unpicklable_submit(self) -> None:
        """Slot token is restored when submit() fails with TypeError."""
        for method in start_methods():
            if method == "fork":
                continue
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    with self.assertRaises(TypeError):
                        js.submit(fn=lambda: 42, timeout=5)
                    f = js.submit(fn=helper_return, args=(7,), timeout=5)
                    self.assertEqual(7, f.result(timeout=5))

    def test_orphan_future_recovered_by_reclaim(self) -> None:
        """A Future dropped without collecting is recovered by reclaim."""
        with Jobserver(context=FAST, slots=1) as js:
            js.submit(fn=helper_return, args=(1,), timeout=5)
            # Future ref dropped; reclaim must recover the slot.
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                js.reclaim_resources()
                if "tracked=0" in repr(js):
                    break
                time.sleep(0.01)
            self.assertIn("tracked=0", repr(js))
            f = js.submit(fn=helper_return, args=(2,), timeout=5)
            self.assertEqual(2, f.result(timeout=5))

    def test_sleep_inf_bounded_by_deadline(self) -> None:
        """sleep() returning inf is clamped by the submission deadline."""
        with Jobserver(context=FAST, slots=1) as js:
            f = js.submit(fn=helper_noop, timeout=5)
            f.result(timeout=5)
            with self.assertRaises(Blocked):
                js.replace_sleep(lambda: float("inf")).submit(
                    fn=helper_noop, timeout=0.1
                )
