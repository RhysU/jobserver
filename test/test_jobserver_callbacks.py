# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Jobserver Future callback machinery.

Verifies the when_done() contract on Jobserver Futures: registration
ordering, error propagation via CallbackRaised, and correct behavior
when callbacks are registered after the Future has already completed.
"""

import time
import unittest

from jobserver import (
    CallbackRaised,
    Future,
    Jobserver,
)

from .helpers import (
    FAST,
    helper_callback,
    helper_raise,
    helper_return,
    wait_until,
)


class TestJobserverCallbacks(unittest.TestCase):
    """Jobserver Future callback machinery."""

    def await_exit(self, *futures: Future) -> None:
        """Block until each child exits so results are ready to reclaim.

        A fixed sleep is unreliable under forkserver (the Python 3.14
        default), whose slower startup can leave a child unfinished.
        Polling is_alive() waits without firing callbacks early.
        """
        for f in futures:
            if not wait_until(
                lambda f=f: f._process is None or not f._process.is_alive(),
                timeout=5,
            ):
                self.fail("child did not exit")

    def helper_check_semantics(
        self, f: Future[None], mutable: list[int]
    ) -> None:
        """Helper checking Future semantics *inside* a callback as expected."""
        # Confirm that inside a callback the Future reports done()
        self.assertTrue(f.done())
        self.assertTrue(f.wait(timeout=0))
        self.assertTrue(f.wait(timeout=None))

        # Confirm that inside a callback additional work can be registered
        f.when_done(helper_callback, mutable, 0, 1)
        f.when_done(helper_callback, mutable, 0, 2)

        # Confirm that *inside* this callback above callbacks haven't happened
        # Those helper_callback(...) calls occur *after* helper_check_semantics
        self.assertEqual(mutable[0], 0, "Zero callbacks observed")

    def test_callback_semantics(self) -> None:
        """Inside a Future's callback the Future reports done() is True."""
        # Prepare how callbacks will be observed
        mutable_a = [0]
        mutable_b = [0]

        with Jobserver(context=FAST, slots=3) as js:
            # First, test registering callbacks before waiting for result()
            f = js.submit(fn=len, args=((1, 2, 3),))
            f.when_done(self.helper_check_semantics, f, mutable_a)
            self.assertEqual(3, f.result())
            self.assertEqual(mutable_a[0], 3, "Two callbacks observed")

            # Second, test registering callbacks after waiting for result()
            g = js.submit(fn=len, args=((1, 2, 3, 4),))
            self.assertEqual(4, g.result())
            g.when_done(self.helper_check_semantics, g, mutable_b)
            self.assertEqual(mutable_b[0], 3, "Two callbacks observed")

    def test_done_callback_raises(self) -> None:
        """Future.wait() raises CallbackRaised from processing callbacks."""
        with Jobserver(context=FAST, slots=3) as js:
            # Calling wait() repeatedly reports multiple errors
            f = js.submit(fn=len, args=(("hello",)), timeout=None)
            f.when_done(helper_raise, ArithmeticError, "123")
            f.when_done(helper_raise, ZeroDivisionError, "45")
            with self.assertRaises(CallbackRaised) as c:
                f.wait(timeout=None)
            self.assertIsInstance(c.exception.__cause__, ArithmeticError)
            with self.assertRaises(CallbackRaised) as c:
                f.wait(timeout=None)
            self.assertIsInstance(c.exception.__cause__, ZeroDivisionError)
            self.assertTrue(f.wait(timeout=None))
            self.assertTrue(f.done())

            # After callbacks completed, result is available.
            self.assertEqual(f.result(), 5)
            self.assertEqual(f.result(), 5)

            # Now that work is complete, adding callback raises
            with self.assertRaises(CallbackRaised) as c:
                f.when_done(helper_raise, UnicodeError, "67")
            self.assertIsInstance(c.exception.__cause__, UnicodeError)
            self.assertTrue(f.wait(timeout=0.0))

            # After callbacks completed, result still available.
            self.assertEqual(f.result(), 5)
            self.assertEqual(f.result(), 5)

    def test_100_callbacks_in_order(self) -> None:
        """100 callbacks fire in registration order."""
        with Jobserver(context=FAST, slots=1) as js:
            f = js.submit(fn=len, args=((1,),), timeout=5)
            order: list[int] = []
            for i in range(100):
                f.when_done(lambda idx=i, o=order: o.append(idx))
            f.wait(timeout=5)
            self.assertEqual(order, list(range(100)))

    def test_five_raising_callbacks_drain(self) -> None:
        """Five raising callbacks each require a done() call to drain."""
        with Jobserver(context=FAST, slots=1) as js:
            f = js.submit(fn=len, args=((1,),), timeout=5)
            for i in range(5):
                f.when_done(helper_raise, ValueError, f"cb-{i}")
            for i in range(5):
                with self.assertRaises(CallbackRaised) as c:
                    f.wait(timeout=5)
                self.assertIsInstance(c.exception.__cause__, ValueError)
                self.assertIn(f"cb-{i}", str(c.exception.__cause__))
            self.assertTrue(f.done())
            self.assertEqual(f.result(), 1)

    def test_when_done_returns_monotonic_seqno(self) -> None:
        """when_done() returns 0, 1, 2, ... for successive user callbacks."""
        with Jobserver(context=FAST, slots=1) as js:
            f = js.submit(fn=len, args=((1,),), timeout=5)
            seqnos = [
                f.when_done(helper_callback, [0], 0, 1) for _ in range(5)
            ]
            # The first user callback sees 0, then 1, 2, ... regardless of
            # the internal callbacks submit() registers beforehand.
            self.assertEqual(seqnos, list(range(5)))
            f.wait(timeout=5)

    def test_callback_raised_carries_seqno(self) -> None:
        """CallbackRaised.seqno identifies the offending registration."""
        with Jobserver(context=FAST, slots=1) as js:
            f = js.submit(fn=len, args=((1,),), timeout=5)
            s0 = f.when_done(helper_raise, ArithmeticError, "0")
            s1 = f.when_done(helper_raise, ZeroDivisionError, "1")
            self.assertEqual([s0, s1], [0, 1])

            with self.assertRaises(CallbackRaised) as c:
                f.wait(timeout=5)
            self.assertIsInstance(c.exception.__cause__, ArithmeticError)
            self.assertEqual(c.exception.seqno, 0)
            self.assertEqual(repr(c.exception), "CallbackRaised(seqno=0)")
            self.assertEqual(str(c.exception), "CallbackRaised(seqno=0)")

            with self.assertRaises(CallbackRaised) as c:
                f.wait(timeout=5)
            self.assertIsInstance(c.exception.__cause__, ZeroDivisionError)
            self.assertEqual(c.exception.seqno, 1)
            self.assertEqual(repr(c.exception), "CallbackRaised(seqno=1)")
            self.assertEqual(str(c.exception), "CallbackRaised(seqno=1)")

            self.assertTrue(f.wait(timeout=5))

    def test_callback_raised_repr_str_and_seqno_type(self) -> None:
        """CallbackRaised repr/str show the seqno; seqno must be an int."""
        # repr and str are identical and expose the seqno.
        e = CallbackRaised(7)
        self.assertEqual(repr(e), "CallbackRaised(seqno=7)")
        self.assertEqual(str(e), repr(e))
        # seqno is required and must be an int.
        with self.assertRaises(TypeError):
            CallbackRaised()  # type: ignore[call-arg]
        with self.assertRaises(TypeError):
            CallbackRaised("7")  # type: ignore[arg-type]

    def test_already_done_callback_raised_carries_seqno(self) -> None:
        """A raising callback registered post-completion reports its seqno."""
        with Jobserver(context=FAST, slots=1) as js:
            f = js.submit(fn=len, args=((1,),), timeout=5)
            f.wait(timeout=5)
            # Registering on an already-done Future fires immediately and
            # raises out of when_done() itself before it can return a seqno;
            # the CallbackRaised still carries the registration's seqno.
            # The first user callback always sees seqno 0.
            with self.assertRaises(CallbackRaised) as c:
                f.when_done(helper_raise, UnicodeError, "now")
            self.assertIsInstance(c.exception.__cause__, UnicodeError)
            self.assertEqual(c.exception.seqno, 0)
            # The next registration sees seqno 1.
            with self.assertRaises(CallbackRaised) as c:
                f.when_done(helper_raise, UnicodeError, "later")
            self.assertEqual(c.exception.seqno, 1)

    def test_reentrant_when_done_nests_issue_callbacks(self) -> None:
        """Callbacks calling when_done() nest _issue_callbacks correctly.

        A callback that registers another callback via when_done()
        triggers a nested _issue_callbacks() invocation.  All callbacks
        must fire in registration order, exactly once.

        NB: The CPython GIL may prevent this test from failing even if
        Future had no explicit locking.
        """
        with Jobserver(slots=1) as js:
            f = js.submit(fn=len, args=((1, 2),), timeout=5)
            order: list[str] = []

            def first_cb() -> None:
                order.append("first")
                # Register two more callbacks from inside a callback
                f.when_done(lambda: order.append("second"))
                f.when_done(lambda: order.append("third"))

            f.when_done(first_cb)
            f.wait(timeout=5)
            self.assertEqual(
                order,
                ["first", "second", "third"],
                "Nested when_done() must fire in order via nested"
                " _issue_callbacks()",
            )
            self.assertEqual(f.result(), 2)

    def test_reentrant_callback_raised_no_double_wrap(self) -> None:
        """Re-entrant raising inner callback must not double-wrap.

        If a callback calls when_done() on an already-completed future
        and that inner callback raises, the caller must see
        CallbackRaised(cause=<original>), not
        CallbackRaised(cause=CallbackRaised(cause=<original>)).
        """
        with Jobserver(slots=1) as js:
            f = js.submit(fn=len, args=((1,),), timeout=5)
            f.wait(timeout=5)

        def outer() -> None:
            f.when_done(lambda: 1 / 0)

        with self.assertRaises(CallbackRaised) as c:
            f.when_done(outer)
        self.assertIsInstance(c.exception.__cause__, ZeroDivisionError)
        self.assertNotIsInstance(c.exception.__cause__, CallbackRaised)

    def test_reclaim_resources_raises_callback_raised(self) -> None:
        """reclaim_resources() surfaces CallbackRaised one at a time."""
        with Jobserver(slots=4) as js:
            sleep = (0.3,)
            a = js.submit(fn=time.sleep, args=sleep, timeout=5)
            b = js.submit(fn=time.sleep, args=sleep, timeout=5)
            c = js.submit(fn=time.sleep, args=sleep, timeout=5)
            d = js.submit(fn=time.sleep, args=sleep, timeout=5)
            a.when_done(helper_raise, ValueError, "a0")
            a.when_done(helper_raise, TypeError, "a1")
            b.when_done(helper_raise, ArithmeticError, "b")
            c.when_done(helper_raise, LookupError, "c")
            order: list[str] = []
            d.when_done(order.append, "ok")
            self.await_exit(a, b, c, d)

            # Collect all CallbackRaised causes; ordering across
            # futures is kernel-determined and not guaranteed.
            causes: list[type] = []
            while True:
                try:
                    js.reclaim_resources()
                except CallbackRaised as e:
                    causes.append(type(e.__cause__))
                    continue
                break

            # All four raising callbacks were surfaced
            self.assertCountEqual(
                causes,
                [ValueError, TypeError, ArithmeticError, LookupError],
            )
            # Intra-future ordering is guaranteed by heapq:
            # a's ValueError always fires before a's TypeError.
            self.assertLess(causes.index(ValueError), causes.index(TypeError))
            # d's non-raising callback fired
            self.assertEqual(order, ["ok"])

    def test_submit_issues_callback_raised_when_blocking(self) -> None:
        """submit() does not raise CallbackRaised while blocking for a slot.

        When submit() blocks waiting for a slot and a previous future
        completes with a raising callback, submit() raise CallbackRaised.
        """
        with Jobserver(context=FAST, slots=1) as js:
            # Submit work A completing quickly; attach a raising cb
            a = js.submit(fn=time.sleep, args=(0.2,), timeout=5)
            a.when_done(helper_raise, ValueError, "from-cb")

            # Submit work B: the single slot taken, so _obtain_tokens
            # blocks until "a" is done then fires its callbacks.
            with self.assertRaises(CallbackRaised) as ctx:
                b = js.submit(fn=len, args=((1, 2),), timeout=5)
                self.assertIsInstance(ctx.exception.__cause__, ValueError)
                self.assertIn("from-cb", str(ctx.exception.__cause__))

            # The caller MAY re-submit and should see "b" complete
            b = js.submit(fn=len, args=((1, 2),), timeout=5)
            self.assertEqual(b.result(timeout=5), 2)

    def test_exit_drains_callback_raised(self) -> None:
        """__exit__ drains all CallbackRaised without propagating."""
        order: list[str] = []
        with self.assertWarns(RuntimeWarning), Jobserver(slots=4) as js:
            sleep = (0.3,)
            a = js.submit(fn=time.sleep, args=sleep, timeout=5)
            b = js.submit(fn=time.sleep, args=sleep, timeout=5)
            c = js.submit(fn=time.sleep, args=sleep, timeout=5)
            d = js.submit(fn=time.sleep, args=sleep, timeout=5)
            a.when_done(helper_raise, ValueError, "a0")
            a.when_done(helper_raise, TypeError, "a1")
            b.when_done(helper_raise, ArithmeticError, "b")
            c.when_done(helper_raise, LookupError, "c")
            d.when_done(order.append, "ok")
            self.await_exit(a, b, c, d)
        # __exit__ ran without raising; all callbacks were drained
        self.assertEqual(order, ["ok"])

    def test_callback_calls_result_reentrantly(self) -> None:
        """A callback calling result() on the same future works via RLock."""
        with Jobserver(context=FAST, slots=1) as js:
            f = js.submit(fn=helper_return, args=(99,), timeout=5)
            inner_result = []
            f.when_done(lambda: inner_result.append(f.result()))
            self.assertEqual(99, f.result(timeout=5))
            self.assertEqual([99], inner_result)

    def test_callback_base_exception_propagates_raw(self) -> None:
        """BaseException from a callback propagates unwrapped."""
        for exc_type in (SystemExit, KeyboardInterrupt):
            with self.subTest(exc_type=exc_type.__name__):
                with Jobserver(context=FAST, slots=1) as js:
                    f = js.submit(fn=helper_return, args=(1,), timeout=5)
                    f.when_done(helper_raise, exc_type)
                    with self.assertRaises(exc_type):
                        f.result(timeout=5)

    def test_callback_submits_new_work(self) -> None:
        """A callback can submit new work to the same Jobserver."""
        with Jobserver(context=FAST, slots=1) as js:
            results = []

            def on_done():
                g = js.submit(fn=helper_return, args=(42,), timeout=5)
                results.append(g.result(timeout=5))

            f = js.submit(fn=helper_return, args=(1,), timeout=5)
            f.when_done(on_done)
            self.assertEqual(1, f.result(timeout=5))
            self.assertEqual([42], results)
