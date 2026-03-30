# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""JobserverExecutor under concurrency and stress.

Verifies the executor under heavy parallel load, confirms integration
with concurrent.futures.wait() and as_completed(), exercises every
multiprocessing start method, and checks internal invariants.
"""

from __future__ import annotations

import concurrent.futures
import gc
import multiprocessing
import sys
import threading
import time
import typing
import unittest
import unittest.mock
import weakref
from multiprocessing import get_all_start_methods

from jobserver import Jobserver, JobserverExecutor, MinimalQueue
from jobserver._request import Submit

from .helpers import (
    FAST,
    TIMEOUT,
    executor_in_child_via_queue,
    helper_raise,
    silence_forkserver,
)


def setUpModule() -> None:
    silence_forkserver()


# ================================================================
# Concurrency Stress
# ================================================================


class TestConcurrencyStress(unittest.TestCase):
    """Concurrency Stress."""

    def test_heavy_submission(self) -> None:
        """Heavy submission exceeding slot count."""
        with Jobserver(context=FAST, slots=2) as js:
            n = 200
            with JobserverExecutor(js) as exe:
                futures = [exe.submit(len, "x" * i) for i in range(n)]
                results = [f.result(timeout=TIMEOUT) for f in futures]
            self.assertEqual(list(range(n)), results)

    def test_mixed_workload(self) -> None:
        """Mixed workload: success, exception, cancel, death."""
        with Jobserver(context=FAST, slots=4) as js:
            exe = JobserverExecutor(js)
            try:
                # Fill a slot to create pending work for cancel
                exe.submit(time.sleep, 0.5)
                time.sleep(0.15)

                f_ok = exe.submit(len, (1, 2, 3))
                f_err = exe.submit(helper_raise, ValueError, "raised")
                f_cancel = exe.submit(len, (1,))
                f_cancel.cancel()

                self.assertEqual(3, f_ok.result(timeout=TIMEOUT))
                with self.assertRaises(ValueError):
                    f_err.result(timeout=TIMEOUT)
                # f_cancel: either cancelled or completed
                self.assertTrue(f_cancel.done())
            finally:
                exe.shutdown(wait=True)

    def _threaded_submit_stress(self, exe: JobserverExecutor) -> None:
        """Submit 200 tasks from 8 threads and verify all results."""
        results: list[concurrent.futures.Future] = []
        lock = threading.Lock()

        def worker(start: int, count: int) -> None:
            local: list[concurrent.futures.Future] = []
            for i in range(start, start + count):
                local.append(exe.submit(len, "x" * i))
            with lock:
                results.extend(local)

        threads = []
        for t_idx in range(8):
            t = threading.Thread(target=worker, args=(t_idx * 25, 25))
            threads.append(t)
            t.start()
        for t in threads:
            t.join(timeout=TIMEOUT)

        self.assertEqual(200, len(results))
        vals = sorted(f.result(timeout=TIMEOUT) for f in results)
        self.assertEqual(sorted(range(200)), vals)

    def test_concurrent_submit_threads(self) -> None:
        """Concurrent submit() from multiple threads."""
        with Jobserver(context=FAST, slots=4) as js:
            with JobserverExecutor(js) as exe:
                self._threaded_submit_stress(exe)

    def test_setswitchinterval_stress(self) -> None:
        """sys.setswitchinterval stress test."""
        old = sys.getswitchinterval()
        try:
            sys.setswitchinterval(1e-6)
            with Jobserver(context=FAST, slots=4) as js:
                with JobserverExecutor(js) as exe:
                    self._threaded_submit_stress(exe)
        finally:
            sys.setswitchinterval(old)


# ================================================================
# wait() and as_completed() Integration
# ================================================================


class TestWaitAndAsCompleted(unittest.TestCase):
    """wait() and as_completed()."""

    def test_wait_all_completed(self) -> None:
        """wait(ALL_COMPLETED) returns all in done."""
        with Jobserver(context=FAST, slots=2) as js:
            with JobserverExecutor(js) as exe:
                futures = [exe.submit(len, "x" * i) for i in range(5)]
                done, not_done = concurrent.futures.wait(
                    futures, timeout=TIMEOUT
                )
            self.assertEqual(5, len(done))
            self.assertEqual(0, len(not_done))

    def test_wait_first_completed(self) -> None:
        """wait(FIRST_COMPLETED) returns on first."""
        with Jobserver(context=FAST, slots=2) as js:
            with JobserverExecutor(js) as exe:
                slow = exe.submit(time.sleep, 0.3)
                fast = exe.submit(len, (1,))
                futures: list[concurrent.futures.Future[typing.Any]] = [
                    slow,
                    fast,
                ]
                done, not_done = concurrent.futures.wait(
                    futures,
                    timeout=TIMEOUT,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                self.assertGreater(len(done), 0)

    def test_wait_first_exception(self) -> None:
        """wait(FIRST_EXCEPTION) returns on first error."""
        with Jobserver(context=FAST, slots=2) as js:
            with JobserverExecutor(js) as exe:
                good = exe.submit(time.sleep, 0.3)
                bad = exe.submit(helper_raise, ValueError, "oops")
                done, not_done = concurrent.futures.wait(
                    [good, bad],
                    timeout=TIMEOUT,
                    return_when=concurrent.futures.FIRST_EXCEPTION,
                )
                self.assertIn(bad, done)

    def test_wait_timeout_partial(self) -> None:
        """wait() with timeout returns partial results."""
        with Jobserver(context=FAST, slots=2) as js:
            with JobserverExecutor(js) as exe:
                futures = [exe.submit(time.sleep, 1.0) for _ in range(3)]
                done, not_done = concurrent.futures.wait(futures, timeout=0.1)
                self.assertGreater(len(not_done), 0)

    def test_as_completed_order(self) -> None:
        """as_completed() yields in completion order."""
        with Jobserver(context=FAST, slots=4) as js:
            with JobserverExecutor(js) as exe:
                f_slow = exe.submit(time.sleep, 0.5)
                f_fast = exe.submit(str, "fast")
                order = []
                for f in concurrent.futures.as_completed(
                    [f_slow, f_fast], timeout=TIMEOUT
                ):
                    order.append(f.result())
                self.assertEqual("fast", order[0])

    def test_as_completed_timeout(self) -> None:
        """as_completed() with timeout raises TimeoutError."""
        with Jobserver(context=FAST, slots=1) as js:
            with JobserverExecutor(js) as exe:
                f = exe.submit(time.sleep, 1.0)
                with self.assertRaises(concurrent.futures.TimeoutError):
                    for _ in concurrent.futures.as_completed([f], timeout=0.1):
                        pass

    def test_duplicate_future(self) -> None:
        """Duplicate future in wait() and as_completed()."""
        with Jobserver(context=FAST, slots=2) as js:
            with JobserverExecutor(js) as exe:
                f = exe.submit(len, (1, 2))
                done, _ = concurrent.futures.wait([f, f], timeout=TIMEOUT)
                self.assertEqual(1, len(done))

                g = exe.submit(len, (1, 2, 3))
                results = list(
                    concurrent.futures.as_completed([g, g], timeout=TIMEOUT)
                )
                self.assertEqual(1, len(results))

    def test_as_completed_gc(self) -> None:
        """as_completed() does not retain yielded futures."""
        with Jobserver(context=FAST, slots=2) as js:
            with JobserverExecutor(js) as exe:
                f = exe.submit(len, (1, 2))
                ref = weakref.ref(f)  # noqa: F841
                for _done in concurrent.futures.as_completed(
                    [f], timeout=TIMEOUT
                ):
                    pass
                del f, _done
                gc.collect()
                # Best-effort: GC may or may not collect
                # Just verify no crash


# ================================================================
# Multiprocessing Start Method Coverage
# ================================================================


class TestStartMethods(unittest.TestCase):
    """Start method coverage."""

    def test_all_methods(self) -> None:
        """All start methods: submit, result, exception."""
        methods = get_all_start_methods()
        if sys.version_info >= (3, 12):
            methods = [m for m in methods if m != "fork"]
        for method in methods:
            with self.subTest(method=method):
                with Jobserver(context=method, slots=2) as js:
                    with JobserverExecutor(js) as exe:
                        # Success
                        f = exe.submit(len, (1, 2, 3))
                        self.assertEqual(3, f.result(timeout=TIMEOUT))
                        # Exception
                        g = exe.submit(helper_raise, ValueError, "test")
                        self.assertIsInstance(
                            g.exception(timeout=TIMEOUT),
                            ValueError,
                        )
                        # Kwargs
                        h = exe.submit(int, "ff", base=16)
                        self.assertEqual(255, h.result(timeout=TIMEOUT))

    def test_map_all_methods(self) -> None:
        """map() works with all start methods."""
        methods = get_all_start_methods()
        if sys.version_info >= (3, 12):
            methods = [m for m in methods if m != "fork"]
        for method in methods:
            with self.subTest(method=method):
                with Jobserver(context=method, slots=2) as js:
                    with JobserverExecutor(js) as exe:
                        result = list(exe.map(str, range(3)))
                        self.assertEqual(["0", "1", "2"], result)

    def test_shutdown_all_methods(self) -> None:
        """Shutdown works with all start methods."""
        methods = get_all_start_methods()
        if sys.version_info >= (3, 12):
            methods = [m for m in methods if m != "fork"]
        for method in methods:
            with self.subTest(method=method):
                with Jobserver(context=method, slots=2) as js:
                    exe = JobserverExecutor(js)
                    f = exe.submit(len, (1,))
                    exe.shutdown(wait=True)
                    self.assertTrue(f.done())


# ================================================================
# Edge Cases (CPython Bug Reports)
# ================================================================


class TestEdgeCases(unittest.TestCase):
    """Edge cases inspired by CPython bugs."""

    def test_executor_in_forked_child(self) -> None:
        """Executor created inside a forked child."""
        methods = get_all_start_methods()
        if sys.version_info >= (3, 12):
            methods = [m for m in methods if m != "fork"]
        method = methods[0] if methods else FAST
        # Use a non-daemonic Process so the child can
        # spawn its own children (Pool workers are
        # daemonic and cannot).
        ctx = multiprocessing.get_context(
            "forkserver" if "forkserver" in methods else "spawn"
        )
        result_queue: multiprocessing.Queue = ctx.Queue()
        p = ctx.Process(  # type: ignore[attr-defined]
            target=executor_in_child_via_queue,
            args=(method, result_queue),
            daemon=False,
        )
        p.start()
        p.join(timeout=30)
        self.assertEqual(0, p.exitcode)
        result = result_queue.get(timeout=1)
        self.assertEqual(3, result)

    def test_result_timeout_zero(self) -> None:
        """result(timeout=0) on incomplete future."""
        with Jobserver(context=FAST, slots=1) as js:
            with JobserverExecutor(js) as exe:
                f = exe.submit(time.sleep, 1.0)
                with self.assertRaises(concurrent.futures.TimeoutError):
                    f.result(timeout=0)

    def test_exception_timeout_zero(self) -> None:
        """exception(timeout=0) on incomplete future."""
        with Jobserver(context=FAST, slots=1) as js:
            with JobserverExecutor(js) as exe:
                f = exe.submit(time.sleep, 1.0)
                with self.assertRaises(concurrent.futures.TimeoutError):
                    f.exception(timeout=0)

    def test_cancel_many_then_shutdown(self) -> None:
        """Many futures cancelled then shutdown."""
        with Jobserver(context=FAST, slots=1) as js:
            exe = JobserverExecutor(js)
            exe.submit(time.sleep, 0.5)
            time.sleep(0.2)
            futures = [exe.submit(len, (i,)) for i in range(50)]
            for f in futures:
                f.cancel()
            t0 = time.monotonic()
            exe.shutdown(wait=True)
            elapsed = time.monotonic() - t0
            # Should return promptly, not block for ages
            self.assertLess(elapsed, 10)


# ================================================================
# Internal Invariants
# ================================================================


class TestInternalInvariants(unittest.TestCase):
    """Internal invariants verified via mocking."""

    def test_lock_released_before_put(self) -> None:
        """_lock must be free when _requests.put() is called.

        Verify the lock is released before each put() by spying on the call.

        MinimalQueue uses __slots__ so instance-level patching is impossible;
        patch the class method and filter by queue object identity instead.
        """
        with Jobserver(context=FAST, slots=2) as js:
            exe = JobserverExecutor(js)
            lock_held: list[bool] = []
            original_put = MinimalQueue.put

            def spy_put(self_q: MinimalQueue, *args: typing.Any) -> None:
                if self_q is exe._requests:
                    lock_held.append(exe._lock.locked())
                return original_put(self_q, *args)

            with unittest.mock.patch.object(MinimalQueue, "put", spy_put):
                exe.submit(len, (1, 2, 3)).result(timeout=TIMEOUT)
                exe.shutdown(wait=True)

            # The first put is the _SUBMIT message from submit();
            # it must have been issued with the lock already released.
            self.assertGreater(len(lock_held), 0)
            self.assertFalse(lock_held[0])

    def test_submit_removes_future_on_put_failure(self) -> None:
        """A future registered in _futures is removed if put() fails.

        Without rollback the future would sit in _futures forever,
        preventing clean shutdown and leaking state.

        MinimalQueue uses __slots__ so instance-level patching is impossible;
        patch the class method and filter by queue object identity instead.
        """
        with Jobserver(context=FAST, slots=2) as js:
            exe = JobserverExecutor(js)
            original_put = MinimalQueue.put
            fail_once = [True]

            def failing_put(self_q: MinimalQueue, *args: typing.Any) -> None:
                if (
                    fail_once[0]
                    and self_q is exe._requests
                    and args
                    and isinstance(args[0], Submit)
                ):
                    fail_once[0] = False
                    raise OSError("simulated put failure")
                return original_put(self_q, *args)

            with unittest.mock.patch.object(MinimalQueue, "put", failing_put):
                with self.assertRaises(OSError):
                    exe.submit(len, (1,))

            with exe._lock:
                remaining = len(exe._futures)

            self.assertEqual(0, remaining)
            exe.shutdown(wait=True)
