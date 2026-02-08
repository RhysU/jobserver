# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Tests for JobserverExecutor -- a concurrent.futures.Executor adapter."""
import concurrent.futures
import os
import signal
import threading
import time
import unittest

from multiprocessing import get_all_start_methods, get_context

from .executor import JobserverExecutor
from .impl import Jobserver


class JobserverExecutorTest(unittest.TestCase):
    """Tests verifying the concurrent.futures.Executor contract."""

    # ------------------------------------------------------------------
    # Basic submit / result
    # ------------------------------------------------------------------

    def test_submit_returns_cf_future(self) -> None:
        """submit() must return a concurrent.futures.Future."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                with JobserverExecutor(js) as exe:
                    f = exe.submit(len, (1, 2, 3))
                    self.assertIsInstance(f, concurrent.futures.Future)
                    self.assertEqual(3, f.result(timeout=10))

    def test_submit_result_success(self) -> None:
        """Successful calls produce correct results."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                with JobserverExecutor(js) as exe:
                    f = exe.submit(len, (1, 2, 3))
                    g = exe.submit(str, 42)
                    self.assertEqual("42", g.result(timeout=10))
                    self.assertEqual(3, f.result(timeout=10))

    def test_submit_with_kwargs(self) -> None:
        """submit() passes keyword arguments correctly."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                with JobserverExecutor(js) as exe:
                    f = exe.submit(int, "ff", base=16)
                    self.assertEqual(255, f.result(timeout=10))

    def test_submit_returns_none(self) -> None:
        """None can be returned as a result."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
                with JobserverExecutor(js) as exe:
                    f = exe.submit(min, (), default=None)
                    self.assertIsNone(f.result(timeout=10))

    # ------------------------------------------------------------------
    # Exception propagation
    # ------------------------------------------------------------------

    @staticmethod
    def _raise(klass, *args):
        raise klass(*args)

    def test_exception_propagation(self) -> None:
        """Exceptions raised in the callable propagate via result()."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                with JobserverExecutor(js) as exe:
                    f = exe.submit(self._raise, ArithmeticError, "boom")
                    with self.assertRaises(ArithmeticError):
                        f.result(timeout=10)

    def test_exception_method(self) -> None:
        """Future.exception() returns the raised exception."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                with JobserverExecutor(js) as exe:
                    f = exe.submit(self._raise, ValueError, "oops")
                    exc = f.exception(timeout=10)
                    self.assertIsInstance(exc, ValueError)
                    self.assertEqual(("oops",), exc.args)

    def test_exception_none_on_success(self) -> None:
        """Future.exception() returns None when callable succeeds."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                with JobserverExecutor(js) as exe:
                    f = exe.submit(len, (1, 2))
                    self.assertIsNone(f.exception(timeout=10))

    # ------------------------------------------------------------------
    # PENDING state is observable
    # ------------------------------------------------------------------

    @staticmethod
    def _wait_on_event(event_path):
        """Block until a file appears, to stall a worker."""
        while not os.path.exists(event_path):
            time.sleep(0.01)
        return "released"

    def test_future_starts_pending(self) -> None:
        """Futures begin in the PENDING state."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
                exe = JobserverExecutor(js)
                try:
                    # Fill the single slot so the next submit stays PENDING
                    blocker = exe.submit(time.sleep, 10)
                    time.sleep(0.1)  # Let the dispatcher dispatch blocker
                    f = exe.submit(len, (1, 2, 3))
                    # f should be PENDING because the slot is occupied
                    time.sleep(0.05)
                    self.assertFalse(f.done())
                    self.assertFalse(f.running())
                    self.assertFalse(f.cancelled())
                finally:
                    exe.shutdown(wait=False, cancel_futures=True)

    # ------------------------------------------------------------------
    # Cancel
    # ------------------------------------------------------------------

    def test_cancel_pending_future(self) -> None:
        """A PENDING future can be cancelled."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
                exe = JobserverExecutor(js)
                try:
                    # Fill the single slot
                    blocker = exe.submit(time.sleep, 10)
                    time.sleep(0.1)
                    f = exe.submit(len, (1, 2, 3))
                    time.sleep(0.05)
                    self.assertTrue(f.cancel())
                    self.assertTrue(f.cancelled())
                    self.assertTrue(f.done())
                    with self.assertRaises(concurrent.futures.CancelledError):
                        f.result(timeout=0)
                finally:
                    exe.shutdown(wait=False, cancel_futures=True)

    def test_cancel_running_future_returns_false(self) -> None:
        """A RUNNING future cannot be cancelled."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                with JobserverExecutor(js) as exe:
                    f = exe.submit(time.sleep, 10)
                    # Wait for it to become RUNNING
                    deadline = time.monotonic() + 5
                    while not f.running() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertFalse(f.cancel())
                    self.assertFalse(f.cancelled())

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def test_shutdown_rejects_new_work(self) -> None:
        """submit() raises RuntimeError after shutdown()."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                exe = JobserverExecutor(js)
                exe.shutdown(wait=True)
                with self.assertRaises(RuntimeError):
                    exe.submit(len, (1, 2))

    def test_shutdown_wait_true(self) -> None:
        """shutdown(wait=True) blocks until all futures complete."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                exe = JobserverExecutor(js)
                f = exe.submit(len, (1, 2, 3))
                g = exe.submit(str, 99)
                exe.shutdown(wait=True)
                self.assertTrue(f.done())
                self.assertTrue(g.done())
                self.assertEqual(3, f.result(timeout=0))
                self.assertEqual("99", g.result(timeout=0))

    def test_shutdown_wait_false(self) -> None:
        """shutdown(wait=False) returns immediately."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                exe = JobserverExecutor(js)
                f = exe.submit(len, (1, 2, 3))
                exe.shutdown(wait=False)
                # The result should still become available eventually
                self.assertEqual(3, f.result(timeout=10))

    def test_shutdown_cancel_futures(self) -> None:
        """shutdown(cancel_futures=True) cancels pending futures."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
                exe = JobserverExecutor(js)
                # Fill the slot
                blocker = exe.submit(time.sleep, 5)
                time.sleep(0.15)
                # These should be pending
                futures = [exe.submit(len, (i,)) for i in range(5)]
                exe.shutdown(wait=True, cancel_futures=True)
                # At least some of the queued futures should be cancelled
                cancelled = [f for f in futures if f.cancelled()]
                self.assertGreater(len(cancelled), 0)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def test_context_manager(self) -> None:
        """The with-statement calls shutdown(wait=True)."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                with JobserverExecutor(js) as exe:
                    f = exe.submit(len, "hello")
                # After exiting the with block, everything is done
                self.assertEqual(5, f.result(timeout=0))
                with self.assertRaises(RuntimeError):
                    exe.submit(len, "x")

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------

    def test_add_done_callback(self) -> None:
        """add_done_callback fires when the future completes."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                results = []
                event = threading.Event()
                with JobserverExecutor(js) as exe:
                    f = exe.submit(len, (1, 2, 3))

                    def cb(fut):
                        results.append(fut.result())
                        event.set()

                    f.add_done_callback(cb)
                    event.wait(timeout=10)
                self.assertEqual([3], results)

    def test_add_done_callback_already_done(self) -> None:
        """add_done_callback fires immediately if already done."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                with JobserverExecutor(js) as exe:
                    f = exe.submit(len, (1, 2))
                    f.result(timeout=10)  # Wait for completion
                    results = []
                    f.add_done_callback(lambda fut: results.append(42))
                    self.assertEqual([42], results)

    # ------------------------------------------------------------------
    # map()
    # ------------------------------------------------------------------

    def test_map_basic(self) -> None:
        """Executor.map() produces correct results."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                with JobserverExecutor(js) as exe:
                    results = list(exe.map(str, range(5)))
                self.assertEqual(["0", "1", "2", "3", "4"], results)

    def test_map_exception(self) -> None:
        """map() propagates exceptions from callables."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                with JobserverExecutor(js) as exe:
                    it = exe.map(self._raise, [ValueError], ["boom"])
                    with self.assertRaises(ValueError):
                        next(it)

    def test_map_timeout(self) -> None:
        """map() raises TimeoutError when results take too long."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
                with JobserverExecutor(js) as exe:
                    it = exe.map(time.sleep, [10], timeout=0.1)
                    with self.assertRaises(TimeoutError):
                        next(it)

    # ------------------------------------------------------------------
    # wait() and as_completed()
    # ------------------------------------------------------------------

    def test_wait_all_completed(self) -> None:
        """concurrent.futures.wait() works with our futures."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                with JobserverExecutor(js) as exe:
                    futures = [exe.submit(len, (i,) * i) for i in range(1, 4)]
                    done, not_done = concurrent.futures.wait(
                        futures, timeout=10
                    )
                self.assertEqual(3, len(done))
                self.assertEqual(0, len(not_done))
                results = sorted(f.result() for f in done)
                self.assertEqual([1, 2, 3], results)

    def test_as_completed(self) -> None:
        """concurrent.futures.as_completed() works with our futures."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                with JobserverExecutor(js) as exe:
                    futures = {exe.submit(len, "x" * i): i for i in range(5)}
                    results = set()
                    for f in concurrent.futures.as_completed(
                        futures, timeout=10
                    ):
                        results.add(f.result())
                self.assertEqual({0, 1, 2, 3, 4}, results)

    # ------------------------------------------------------------------
    # Heavy usage / saturation
    # ------------------------------------------------------------------

    def test_heavy_usage(self) -> None:
        """Many submissions exceeding slot count complete correctly."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                with JobserverExecutor(js) as exe:
                    n = 20
                    futures = [exe.submit(len, "x" * i) for i in range(n)]
                    results = [f.result(timeout=30) for f in futures]
                self.assertEqual(list(range(n)), results)

    # ------------------------------------------------------------------
    # Worker death detection
    # ------------------------------------------------------------------

    @staticmethod
    def _self_signal(sig):
        os.kill(os.getpid(), sig)

    def test_worker_death(self) -> None:
        """Worker death surfaces as an exception on the future."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                with JobserverExecutor(js) as exe:
                    f = exe.submit(self._self_signal, signal.SIGKILL)
                    with self.assertRaises(Exception):
                        f.result(timeout=10)
                    # A normal submission still works afterward
                    g = exe.submit(len, (1, 2))
                    self.assertEqual(2, g.result(timeout=10))

    # ------------------------------------------------------------------
    # Multiple shutdown calls are safe
    # ------------------------------------------------------------------

    def test_double_shutdown(self) -> None:
        """Calling shutdown() twice does not raise."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                exe = JobserverExecutor(js)
                exe.shutdown(wait=True)
                exe.shutdown(wait=True)  # Should not raise


if __name__ == "__main__":
    unittest.main()
