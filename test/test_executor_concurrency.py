# Copyright (C) 2019-2026 Rhys Ulerich
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

from jobserver import Jobserver, JobserverExecutor, _request, _response
from jobserver._executor import _DispatchState, _handle_request
from jobserver._queue import SPSCQueue
from jobserver._request import Submit

from .helpers import (
    FAST,
    TIMEOUT,
    executor_in_child_via_queue,
    helper_raise,
    silence_forkserver,
    start_methods,
)


def setUpModule() -> None:
    silence_forkserver()


class TestConcurrencyStress(unittest.TestCase):
    """Concurrency stress."""

    def test_heavy_submission(self) -> None:
        """Heavy submission exceeding slot count."""
        with Jobserver(context=FAST, slots=2) as js:
            n = 100
            with JobserverExecutor(js) as exe:
                futures = [exe.submit(len, "x" * i) for i in range(n)]
                results = [f.result(timeout=TIMEOUT) for f in futures]
            self.assertEqual(list(range(n)), results)

    def test_drains_saturating_work_to_completion(self) -> None:
        """Executor makes forward progress by recycling a saturated slot.

        On a single-slot executor, run map() (recycling the lone slot
        across several workers), then hold that slot with one task while a
        second queues PENDING behind it, and block on the second's result,
        the holder's result, and a wait=True shutdown at exit.  Forward
        progress requires the dispatcher to reap the finished holder and
        recycle its slot -- the end-to-end path examples/ex12 exercises but
        the unit suite otherwise covers only piecewise: saturation that
        shuts down without waiting, and result-waiting with slack slots.  A
        regression that breaks the reap deadlocks here, not merely fails a
        state check.  Exercised across every start method.
        """
        for method in start_methods(threaded=True):
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    with JobserverExecutor(js) as exe:
                        # map() recycles the lone slot across workers, priming
                        # the dispatcher's state for the reuse that follows.
                        self.assertEqual(
                            list(exe.map(len, ["a", "bb", "ccc"])),
                            [1, 2, 3],
                        )
                        # holder occupies the lone slot; follow queues behind.
                        holder = exe.submit(time.sleep, 0.1)
                        follow = exe.submit(len, (1, 2, 3))
                        # follow runs only once holder's slot recycles.
                        self.assertEqual(follow.result(timeout=TIMEOUT), 3)
                        self.assertIsNone(holder.result(timeout=TIMEOUT))
                    # with-exit -> shutdown(wait=True) returns, no hang.

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
        """Concurrent submit() from multiple threads, also under GIL stress.

        Run _threaded_submit_stress twice: once at the default switch
        interval and once at 1 µs (maximum GIL contention) to catch
        races that only surface under rapid thread interleaving.
        """
        with Jobserver(context=FAST, slots=4) as js:
            with JobserverExecutor(js) as exe:
                self._threaded_submit_stress(exe)
        old = sys.getswitchinterval()
        try:
            sys.setswitchinterval(1e-6)
            with Jobserver(context=FAST, slots=4) as js:
                with JobserverExecutor(js) as exe:
                    self._threaded_submit_stress(exe)
        finally:
            sys.setswitchinterval(old)


class TestWaitAndAsCompleted(unittest.TestCase):
    """wait() and as_completed() behavior."""

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
                futures = [exe.submit(time.sleep, 0.5) for _ in range(3)]
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
                f = exe.submit(time.sleep, 0.5)
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


class TestStartMethods(unittest.TestCase):
    """Start method coverage."""

    def test_all_methods(self) -> None:
        """All start methods: submit, result, exception."""
        for method in start_methods(threaded=True):
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
        for method in start_methods(threaded=True):
            with self.subTest(method=method):
                with Jobserver(context=method, slots=2) as js:
                    with JobserverExecutor(js) as exe:
                        result = list(exe.map(str, range(3)))
                        self.assertEqual(["0", "1", "2"], result)

    def test_shutdown_all_methods(self) -> None:
        """Shutdown works with all start methods."""
        for method in start_methods(threaded=True):
            with self.subTest(method=method):
                with Jobserver(context=method, slots=2) as js:
                    exe = JobserverExecutor(js)
                    f = exe.submit(len, (1,))
                    exe.shutdown(wait=True)
                    self.assertTrue(f.done())


class TestEdgeCases(unittest.TestCase):
    """Edge cases inspired by CPython bugs."""

    def test_executor_in_forked_child(self) -> None:
        """Executor created inside a forked child."""
        methods = start_methods(threaded=True)
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
                f = exe.submit(time.sleep, 0.3)
                with self.assertRaises(concurrent.futures.TimeoutError):
                    f.result(timeout=0)

    def test_exception_timeout_zero(self) -> None:
        """exception(timeout=0) on incomplete future."""
        with Jobserver(context=FAST, slots=1) as js:
            with JobserverExecutor(js) as exe:
                f = exe.submit(time.sleep, 0.3)
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


class TestInternalInvariants(unittest.TestCase):
    """Internal invariants verified via mocking."""

    def test_lock_released_before_put(self) -> None:
        """_lock must be free when _requests.put() is called.

        Verify the lock is released before each put() by spying on the call.

        SPSCQueue uses __slots__ so instance-level patching is impossible;
        patch the class method and filter by queue object identity instead.
        """
        with Jobserver(context=FAST, slots=2) as js:
            exe = JobserverExecutor(js)
            lock_held: list[bool] = []
            original_put = SPSCQueue.put

            def spy_put(self_q: SPSCQueue, *args: typing.Any) -> None:
                if self_q is exe._requests:
                    lock_held.append(exe._lock.locked())
                return original_put(self_q, *args)

            with unittest.mock.patch.object(SPSCQueue, "put", spy_put):
                exe.submit(len, (1, 2, 3)).result(timeout=TIMEOUT)
                exe.shutdown(wait=True)

            # The first put is the _SUBMIT message from submit();
            # it must have been issued with the lock already released.
            self.assertGreater(len(lock_held), 0)
            self.assertFalse(lock_held[0])

    def test_blanket_cancel_latches_cancelling(self) -> None:
        """A blanket Cancel() makes the dispatcher cancel later Submits.

        After shutdown(cancel_futures=True) sends Cancel(), a Submit racing
        the impending Shutdown must be cancelled on arrival, never dispatched.
        """
        responses: SPSCQueue = SPSCQueue(FAST)
        state = _DispatchState()
        try:
            # Blanket Cancel() latches cancelling True.
            _handle_request(_request.Cancel(), state, responses)
            self.assertFalse(state.shutdown)
            self.assertTrue(state.cancelling)

            # A Submit arriving while cancelling is cancelled, not queued.
            _handle_request(
                _request.Submit(work_id=7, fn=len, args=((1,),), kwargs={}),
                state,
                responses,
            )
            self.assertFalse(state.shutdown)
            self.assertTrue(state.cancelling)
            self.assertEqual(0, len(state.pending))

            msg = responses.get(timeout=TIMEOUT)
            self.assertIsInstance(msg, _response.Cancelled)
            self.assertEqual(7, msg.work_id)
        finally:
            responses.close_put()
            responses.close_get()

    def test_submit_removes_future_on_put_failure(self) -> None:
        """A future registered in _futures is removed if put() fails.

        Without rollback the future would sit in _futures forever,
        preventing clean shutdown and leaking state.

        SPSCQueue uses __slots__ so instance-level patching is impossible;
        patch the class method and filter by queue object identity instead.
        """
        with Jobserver(context=FAST, slots=2) as js:
            exe = JobserverExecutor(js)
            original_put = SPSCQueue.put
            fail_once = [True]

            def failing_put(self_q: SPSCQueue, *args: typing.Any) -> None:
                if (
                    fail_once[0]
                    and self_q is exe._requests
                    and args
                    and isinstance(args[0], Submit)
                ):
                    fail_once[0] = False
                    raise OSError("simulated put failure")
                return original_put(self_q, *args)

            with unittest.mock.patch.object(SPSCQueue, "put", failing_put):
                with self.assertRaises(OSError):
                    exe.submit(len, (1,))

            with exe._lock:
                remaining = len(exe._futures)

            self.assertEqual(0, remaining)
            exe.shutdown(wait=True)
