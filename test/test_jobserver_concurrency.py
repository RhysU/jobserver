# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Thread-safety of Jobserver Futures.

Verifies that concurrent access to a single Future from multiple
threads is safe, that timeout budgets are respected under lock
contention, and that reclaim_resources() tolerates contested locks.
"""

import threading
import time
import unittest

from jobserver import (
    CallbackRaised,
    Future,
    Jobserver,
)

from .helpers import helper_raise


class TestJobserverConcurrency(unittest.TestCase):
    """Threading races and concurrent Future access."""

    def test_concurrent_done_no_crash(self) -> None:
        """Concurrent done() on the same Future must not crash.

        Two threads calling done() on the same Future concurrently must not
        cause an AttributeError or AssertionError.  This naturally happens
        when one thread calls reclaim_resources() while another is inside
        submit() with callbacks=True.
        """
        js = Jobserver(slots=4)
        errors: list[Exception] = []

        def call_done(future: Future, barrier: threading.Barrier) -> None:
            """Wait at the barrier then count exceptions from done()."""
            barrier.wait()
            try:
                future.done(timeout=5)
            except Exception as e:
                errors.append(e)

        # Repeat to increase chance of hitting the race window
        for _ in range(10):
            f = js.submit(fn=time.sleep, args=(0.05,), timeout=5)
            barrier = threading.Barrier(2)
            t = threading.Thread(target=call_done, args=(f, barrier))
            t.start()
            call_done(f, barrier)  # Main thread also races into done()
            t.join(timeout=5)

        # Drain futures left incomplete when the race caused an exception
        for future in list(js._future_sentinels.keys()):
            future.done(timeout=10)
        self.assertEqual(errors, [], f"Concurrent done() crashed: {errors}")

    def test_concurrent_done_both_threads_see_true(self) -> None:
        """Both threads calling done() concurrently must see True.

        The losing thread must take the fast-path (_connection is None)
        and return True, never silently swallowing the result.
        """
        js = Jobserver(slots=4)

        for _ in range(20):
            f = js.submit(fn=time.sleep, args=(0.02,), timeout=5)
            results: list = [None, None]

            def call_done(
                idx: int, barrier: threading.Barrier, f=f, results=results
            ) -> None:
                barrier.wait()
                results[idx] = f.done(timeout=5)

            barrier = threading.Barrier(2)
            t = threading.Thread(target=call_done, args=(1, barrier))
            t.start()
            call_done(0, barrier)
            t.join(timeout=5)
            self.assertTrue(results[0], "Main thread must see done() == True")
            self.assertTrue(
                results[1], "Background thread must see done() == True"
            )

    def test_concurrent_done_timeout_budget(self) -> None:
        """Lock acquisition time is deducted from the timeout budget.

        A done(timeout=T) call must not block for longer than
        approximately T seconds, even if the lock is contested.
        """
        js = Jobserver(slots=1)
        f = js.submit(fn=time.sleep, args=(0.5,), timeout=5)

        # Hold the lock from another thread to force contention
        acquired = threading.Event()
        release = threading.Event()

        def hold_lock() -> None:
            with f._rlock:
                acquired.set()
                release.wait(timeout=10)

        t = threading.Thread(target=hold_lock)
        t.start()
        acquired.wait(timeout=5)

        # done(timeout=0.1) must return within ~0.2s, not hang
        start = time.monotonic()
        result = f.done(timeout=0.1)
        elapsed = time.monotonic() - start

        release.set()
        t.join(timeout=5)

        self.assertFalse(result, "Should return False when lock held")
        self.assertLess(
            elapsed, 2.0, "Must respect timeout despite lock contention"
        )

        # Clean up: let the future actually complete
        f.done(timeout=10)

    def test_concurrent_when_done_with_done(self) -> None:
        """when_done() from one thread while done() transitions in another.

        The callback registered by when_done() must fire exactly once,
        regardless of the timing relative to the done() transition.

        NB: The CPython GIL may prevent this test from failing even if
        Future had no explicit locking.
        """
        js = Jobserver(slots=4)
        fired: list[str] = []

        for trial in range(20):
            fired.clear()
            f = js.submit(fn=time.sleep, args=(0.02,), timeout=5)
            barrier = threading.Barrier(2)

            def register_callback(
                barrier: threading.Barrier, f=f, fired=fired
            ) -> None:
                barrier.wait()
                try:
                    f.when_done(lambda fired=fired: fired.append("cb"))
                except CallbackRaised:
                    pass  # Callback fired and raised; still counts

            t = threading.Thread(target=register_callback, args=(barrier,))
            t.start()

            # Main thread races done() against when_done()
            barrier.wait()
            f.done(timeout=5)
            t.join(timeout=5)

            # Drain any remaining callbacks
            while True:
                try:
                    f.done(timeout=0)
                    break
                except CallbackRaised:
                    pass

            self.assertEqual(
                fired.count("cb"),
                1,
                f"Trial {trial}: callback must fire exactly once,"
                f" got {fired.count('cb')}",
            )

    def test_concurrent_callback_raised_delivery(self) -> None:
        """CallbackRaised is delivered to exactly one thread.

        When two threads race into done(), only the winner executes
        callbacks.  The loser sees an already-done Future with no
        pending callbacks and does not raise CallbackRaised.
        """
        js = Jobserver(slots=4)

        for _ in range(20):
            f = js.submit(fn=time.sleep, args=(0.02,), timeout=5)
            f.when_done(helper_raise, ValueError, "raised")

            raised_in: list[str] = []

            def call_done(
                name: str, barrier: threading.Barrier, f=f, raised_in=raised_in
            ) -> None:
                barrier.wait()
                try:
                    f.done(timeout=5)
                except CallbackRaised:
                    raised_in.append(name)

            barrier = threading.Barrier(2)
            t = threading.Thread(target=call_done, args=("bg", barrier))
            t.start()
            call_done("main", barrier)
            t.join(timeout=5)

            self.assertEqual(
                len(raised_in),
                1,
                f"CallbackRaised must be delivered to exactly"
                f" one thread, got: {raised_in}",
            )

    def test_reclaim_resources_with_contested_lock(self) -> None:
        """reclaim_resources() tolerates a contested Future lock.

        When one thread holds a Future's lock (inside done()), another
        thread calling reclaim_resources() must not crash.  The
        done(timeout=0) inside reclaim_resources returns False for the
        contested Future and moves on.
        """
        js = Jobserver(slots=2)
        f = js.submit(fn=time.sleep, args=(0.05,), timeout=5)

        # Hold the Future's lock from a background thread
        acquired = threading.Event()
        release = threading.Event()

        def hold_lock() -> None:
            with f._rlock:
                acquired.set()
                release.wait(timeout=10)

        t = threading.Thread(target=hold_lock)
        t.start()
        acquired.wait(timeout=5)

        # reclaim_resources uses done(timeout=0) which should fail
        # gracefully when the lock is contested
        js.reclaim_resources()  # Must not crash

        release.set()
        t.join(timeout=5)

        # Now the future can complete normally
        f.done(timeout=10)
        self.assertIsNone(f.result())
