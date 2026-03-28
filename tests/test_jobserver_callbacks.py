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

import unittest
from multiprocessing import get_all_start_methods

from jobserver import (
    CallbackRaised,
    Future,
    Jobserver,
)

from .helpers import (
    helper_callback,
    helper_raise,
)


class TestJobserverCallbacks(unittest.TestCase):
    """Jobserver Future callback machinery."""

    def helper_check_semantics(self, f: Future[None]) -> None:
        """Helper checking Future semantics *inside* a callback as expected."""
        # Prepare how callbacks will be observed
        mutable = [0]

        # Confirm that inside a callback the Future reports done()
        self.assertTrue(f.done(timeout=0))

        # Confirm that inside a callback additional work can be registered
        f.when_done(helper_callback, mutable, 0, 1)
        f.when_done(helper_callback, mutable, 0, 2)

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

    def test_done_callback_raises(self) -> None:
        """Future.done() raises Exceptions thrown while processing work?"""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=3)

                # Calling done() repeatedly correctly reports multiple errors
                f = js.submit(fn=len, args=(("hello",)), timeout=None)
                f.when_done(helper_raise, ArithmeticError, "123")
                f.when_done(helper_raise, ZeroDivisionError, "45")
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
                    f.when_done(helper_raise, UnicodeError, "67")
                self.assertIsInstance(c.exception.__cause__, UnicodeError)
                self.assertTrue(f.done(timeout=0.0))

                # After callbacks have completed, result is still available.
                self.assertEqual(f.result(), 5)
                self.assertEqual(f.result(), 5)

    def test_100_callbacks_in_order(self) -> None:
        """100 callbacks fire in registration order."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
                f = js.submit(fn=len, args=((1,),), timeout=5)
                order: list[int] = []
                for i in range(100):
                    f.when_done(lambda idx=i, o=order: o.append(idx))
                f.done(timeout=5)
                self.assertEqual(order, list(range(100)))

    def test_five_raising_callbacks_drain(self) -> None:
        """Five raising callbacks each require a done() call to drain."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
                f = js.submit(fn=len, args=((1,),), timeout=5)
                for i in range(5):
                    f.when_done(helper_raise, ValueError, f"cb-{i}")
                for i in range(5):
                    with self.assertRaises(CallbackRaised) as c:
                        f.done(timeout=5)
                    self.assertIsInstance(c.exception.__cause__, ValueError)
                    self.assertIn(f"cb-{i}", str(c.exception.__cause__))
                self.assertTrue(f.done(timeout=0))
                self.assertEqual(f.result(), 1)

    def test_reentrant_when_done_nests_issue_callbacks(self) -> None:
        """Callbacks calling when_done() nest _issue_callbacks correctly.

        A callback that registers another callback via when_done()
        triggers a nested _issue_callbacks() invocation.  All callbacks
        must fire in registration order, exactly once.

        NB: The CPython GIL may prevent this test from failing even if
        Future had no explicit locking.
        """
        js = Jobserver(slots=1)
        f = js.submit(fn=len, args=((1, 2),), timeout=5)
        order: list[str] = []

        def first_cb() -> None:
            order.append("first")
            # Register two more callbacks from inside a callback
            f.when_done(lambda: order.append("second"))
            f.when_done(lambda: order.append("third"))

        f.when_done(first_cb)
        f.done(timeout=5)
        self.assertEqual(
            order,
            ["first", "second", "third"],
            "Nested when_done() must fire in order via nested"
            " _issue_callbacks()",
        )
        self.assertEqual(f.result(), 2)

    def test_reentrant_callback_raised_no_double_wrap(self) -> None:
        """Re-entrant raising inner callback must not double-wrap.

        If a callback calls when_done() on an already-done future
        and that inner callback raises, the caller must see
        CallbackRaised(cause=<original>), not
        CallbackRaised(cause=CallbackRaised(cause=<original>)).
        """
        js = Jobserver(slots=1)
        f = js.submit(fn=len, args=((1,),), timeout=5)
        f.done(timeout=5)

        def outer() -> None:
            f.when_done(lambda: 1 / 0)  # raises ZeroDivisionError immediately

        with self.assertRaises(CallbackRaised) as c:
            f.when_done(outer)
        self.assertIsInstance(c.exception.__cause__, ZeroDivisionError)
        self.assertNotIsInstance(c.exception.__cause__, CallbackRaised)
