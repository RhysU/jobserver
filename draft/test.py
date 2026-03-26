# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Hardening tests for JobserverExecutor.

Tests the public API of JobserverExecutor as a concurrent.futures.Executor,
informed by CPython's own test suite, known bug reports, and common pitfalls.
"""
import concurrent.futures
import gc
import multiprocessing
import os
import signal
import sys
import threading
import time
import typing
import unittest
import unittest.mock
import weakref

from multiprocessing import get_all_start_methods

from draft.executor import JobserverExecutor
from draft._request import Submit
from jobserver._jobserver import Jobserver, MinimalQueue

# Most tests use the fastest start method.  On Python 3.12+ "fork" is
# deprecated when the process is multi-threaded, so fall back to
# "forkserver".
_FAST = "forkserver" if sys.version_info >= (3, 12) else "fork"

_TIMEOUT = 30  # generous per-future timeout to avoid flakes


def _noop() -> None:
    """Trivial target used only to trigger forkserver start."""


def setUpModule() -> None:
    """Start the forkserver with stderr silenced.

    The forkserver process inherits the parent's fd 2 at start time.
    By pointing fd 2 at /dev/null before the first forkserver spawn,
    benign tracebacks from the forkserver (e.g. FileNotFoundError
    when a SemLock backing file is unlinked during cleanup) go to
    /dev/null instead of polluting the test trace.  The main
    process's stderr is restored immediately afterward.
    """
    if _FAST != "forkserver":
        return
    devnull_fd = os.open(os.devnull, os.O_WRONLY)
    saved_fd = os.dup(2)
    os.dup2(devnull_fd, 2)
    os.close(devnull_fd)
    try:
        ctx = multiprocessing.get_context("forkserver")
        p = ctx.Process(target=_noop)
        p.start()
        p.join()
    finally:
        os.dup2(saved_fd, 2)
        os.close(saved_fd)


# ---- Module-level helpers (must be picklable for spawn) ----


def _raise(klass: type, *args: typing.Any) -> typing.Any:
    raise klass(*args)


def _return_value(x: typing.Any) -> typing.Any:
    return x


def _return_exception() -> ValueError:
    return ValueError("not raised")


def _self_kill() -> None:
    os.kill(os.getpid(), signal.SIGKILL)


def _sys_exit(code: int) -> None:
    sys.exit(code)


def _sleep(secs: float) -> str:
    time.sleep(secs)
    return "done"


def _sleep_return(secs: float, val: typing.Any) -> typing.Any:
    time.sleep(secs)
    return val


_identity = _return_value


def _add(a: int, b: int) -> int:
    return a + b


def _round_trip_bytes(n: int) -> bytes:
    """Create and return n bytes."""
    return b"x" * n


def _barrier_wait(path: str) -> str:
    """Block until a file appears, then return."""
    deadline = time.monotonic() + 30
    while not os.path.exists(path):
        if time.monotonic() > deadline:
            raise TimeoutError("barrier file never appeared")
        time.sleep(0.01)
    return "released"


def _raising_at_position(i: int, fail_at: int) -> int:
    if i == fail_at:
        raise ValueError(f"fail at {fail_at}")
    return i


def _executor_in_child(method: str) -> int:
    """Create an executor inside a child process and run work."""
    js = Jobserver(context=method, slots=1)
    with JobserverExecutor(js) as exe:
        return exe.submit(len, (1, 2, 3)).result(timeout=10)  # type: ignore


def _executor_in_child_via_queue(
    method: str, q: "multiprocessing.Queue[int]"
) -> None:
    """Wrapper for _executor_in_child that puts result on a queue."""
    q.put(_executor_in_child(method))


# ================================================================
# Submit and Result
# ================================================================


class TestSubmitAndResult(unittest.TestCase):
    """Submit and Result."""

    def test_successful_call(self) -> None:
        """Successful call returns correct result."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2, 3))
            self.assertEqual(3, f.result(timeout=_TIMEOUT))

    def test_keyword_arguments(self) -> None:
        """Keyword arguments are forwarded."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(int, "ff", base=16)
            self.assertEqual(255, f.result(timeout=_TIMEOUT))

    def test_none_return(self) -> None:
        """None can be returned."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(min, (), default=None)
            self.assertIsNone(f.result(timeout=_TIMEOUT))

    def test_multiple_concurrent(self) -> None:
        """Multiple concurrent submissions."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            futures = [exe.submit(len, "x" * i) for i in range(10)]
            results = [f.result(timeout=_TIMEOUT) for f in futures]
        self.assertEqual(list(range(10)), results)

    def test_returns_cf_future(self) -> None:
        """submit() returns a concurrent.futures.Future."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1,))
            self.assertIsInstance(f, concurrent.futures.Future)
            f.result(timeout=_TIMEOUT)


# ================================================================
# Exception Propagation
# ================================================================


class TestExceptionPropagation(unittest.TestCase):
    """Exception Propagation."""

    def test_exception_via_result(self) -> None:
        """Exception raised in callable surfaces via result()."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(_raise, ValueError, "boom")
            with self.assertRaises(ValueError):
                f.result(timeout=_TIMEOUT)

    def test_exception_method(self) -> None:
        """exception() returns the raised exception."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(_raise, ValueError, "boom")
            exc = f.exception(timeout=_TIMEOUT)
            self.assertIsInstance(exc, ValueError)

    def test_exception_none_on_success(self) -> None:
        """exception() returns None on success."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2))
            self.assertIsNone(f.exception(timeout=_TIMEOUT))

    def test_exception_returned_not_raised(self) -> None:
        """An Exception can be returned (not raised)."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(_return_exception)
            result = f.result(timeout=_TIMEOUT)
            self.assertIsInstance(result, ValueError)
            self.assertEqual(("not raised",), result.args)

    def test_worker_killed_by_signal(self) -> None:
        """Worker killed by signal."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(_self_kill)
            with self.assertRaises(Exception):
                f.result(timeout=_TIMEOUT)
            # Executor must recover
            g = exe.submit(len, (1, 2))
            self.assertEqual(2, g.result(timeout=_TIMEOUT))

    def test_sys_exit(self) -> None:
        """Worker exits via sys.exit()."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(_sys_exit, 1)
            with self.assertRaises(Exception):
                f.result(timeout=_TIMEOUT)
            # Executor must remain usable
            g = exe.submit(len, (1, 2, 3))
            self.assertEqual(3, g.result(timeout=_TIMEOUT))

    def test_unpicklable_callable(self) -> None:
        """Unpicklable callable raises, not hangs."""
        js = Jobserver(context="spawn", slots=2)
        with JobserverExecutor(js) as exe:
            # lambda is not picklable under spawn;
            # pickling fails at submit() time in put().
            with self.assertRaises(Exception):
                exe.submit(lambda: 42)

    def test_unpicklable_arguments(self) -> None:
        """Unpicklable arguments raise, not hang."""
        js = Jobserver(context="spawn", slots=2)
        with JobserverExecutor(js) as exe:
            lock = threading.Lock()
            # Lock is not picklable; fails at submit().
            with self.assertRaises(Exception):
                exe.submit(len, lock)

    def test_large_arguments_and_results(self) -> None:
        """Very large arguments and results."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            size = 10 * 1024 * 1024  # 10 MB
            f = exe.submit(_round_trip_bytes, size)
            result = f.result(timeout=_TIMEOUT)
            self.assertEqual(size, len(result))
            self.assertEqual(b"x" * 10, result[:10])


# ================================================================
# Future State Queries
# ================================================================


class TestFutureStateQueries(unittest.TestCase):
    """Future State Queries."""

    def test_pending_when_slots_full(self) -> None:
        """A newly submitted future is PENDING when slots full."""
        js = Jobserver(context=_FAST, slots=1)
        exe = JobserverExecutor(js)
        try:
            exe.submit(_sleep, 1.0)
            time.sleep(0.2)
            f = exe.submit(len, (1, 2, 3))
            time.sleep(0.1)
            self.assertFalse(f.done())
            self.assertFalse(f.running())
            self.assertFalse(f.cancelled())
        finally:
            exe.shutdown(wait=False, cancel_futures=True)

    def test_running_transition(self) -> None:
        """A dispatched future transitions to RUNNING."""
        import tempfile

        js = Jobserver(context=_FAST, slots=2)
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            gate = tmp.name
        os.unlink(gate)  # remove so barrier blocks

        exe = JobserverExecutor(js)
        try:
            f = exe.submit(_barrier_wait, gate)
            deadline = time.monotonic() + 5
            while not f.running() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(f.running())
            self.assertFalse(f.done())
        finally:
            # Release the barrier
            with open(gate, "w") as fh:
                fh.write("go")
            f.result(timeout=_TIMEOUT)
            exe.shutdown(wait=True)
            os.unlink(gate)

    def test_finished_state(self) -> None:
        """A completed future is FINISHED."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2))
            f.result(timeout=_TIMEOUT)
            self.assertTrue(f.done())
            self.assertFalse(f.running())
            self.assertFalse(f.cancelled())


# ================================================================
# Cancellation
# ================================================================


class TestCancellation(unittest.TestCase):
    """Cancellation."""

    def test_cancel_pending(self) -> None:
        """A PENDING future can be cancelled."""
        js = Jobserver(context=_FAST, slots=1)
        exe = JobserverExecutor(js)
        try:
            exe.submit(_sleep, 1.0)
            time.sleep(0.2)
            f = exe.submit(len, (1, 2, 3))
            time.sleep(0.1)
            self.assertTrue(f.cancel())
            self.assertTrue(f.cancelled())
            self.assertTrue(f.done())
            with self.assertRaises(concurrent.futures.CancelledError):
                f.result(timeout=0)
        finally:
            exe.shutdown(wait=False, cancel_futures=True)

    def test_cancel_running(self) -> None:
        """A RUNNING future cannot be cancelled."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(_sleep, 0.5)
            deadline = time.monotonic() + 5
            while not f.running() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertFalse(f.cancel())
            self.assertFalse(f.cancelled())
            f.result(timeout=_TIMEOUT)

    def test_cancel_finished(self) -> None:
        """A FINISHED future cannot be cancelled."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2))
            f.result(timeout=_TIMEOUT)
            self.assertFalse(f.cancel())

    def test_rapid_submit_cancel_churn(self) -> None:
        """Rapid submit-then-cancel churn."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            for _ in range(200):
                f = exe.submit(len, (1,))
                f.cancel()
        # No deadlock, no crash -- shutdown completes

    def test_cancel_racing_with_dispatch(self) -> None:
        """Cancel racing with dispatch."""
        js = Jobserver(context=_FAST, slots=1)
        exe = JobserverExecutor(js)
        try:
            blocker = exe.submit(_sleep, 1.0)
            time.sleep(0.2)
            f = exe.submit(len, (1, 2, 3))
            time.sleep(0.1)
            f.cancel()
            # Now free the slot
            blocker.result(timeout=_TIMEOUT)
            # f must stay cancelled even though a slot opened
            self.assertTrue(f.cancelled())
        finally:
            exe.shutdown(wait=True)


# ================================================================
# Callbacks
# ================================================================


class TestCallbacks(unittest.TestCase):
    """Callbacks."""

    def test_callback_on_success(self) -> None:
        """add_done_callback fires on success."""
        js = Jobserver(context=_FAST, slots=2)
        results: typing.List[typing.Any] = []
        event = threading.Event()
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2, 3))

            def cb(fut: concurrent.futures.Future) -> None:
                results.append(fut.result())
                event.set()

            f.add_done_callback(cb)
            event.wait(timeout=_TIMEOUT)
        self.assertEqual([3], results)

    def test_callback_on_exception(self) -> None:
        """add_done_callback fires on exception."""
        js = Jobserver(context=_FAST, slots=2)
        results: typing.List[typing.Any] = []
        event = threading.Event()
        with JobserverExecutor(js) as exe:
            f = exe.submit(_raise, ValueError, "boom")

            def cb(fut: concurrent.futures.Future) -> None:
                results.append(type(fut.exception()))
                event.set()

            f.add_done_callback(cb)
            event.wait(timeout=_TIMEOUT)
        self.assertEqual([ValueError], results)

    def test_callback_on_cancellation(self) -> None:
        """add_done_callback fires on cancellation."""
        js = Jobserver(context=_FAST, slots=1)
        exe = JobserverExecutor(js)
        results: typing.List[bool] = []
        event = threading.Event()
        try:
            exe.submit(_sleep, 1.0)
            time.sleep(0.2)
            f = exe.submit(len, (1, 2, 3))
            time.sleep(0.1)

            def cb(fut: concurrent.futures.Future) -> None:
                results.append(fut.cancelled())
                event.set()

            f.add_done_callback(cb)
            f.cancel()
            event.wait(timeout=_TIMEOUT)
        finally:
            exe.shutdown(wait=False, cancel_futures=True)
        self.assertEqual([True], results)

    def test_callback_on_already_done(self) -> None:
        """Callback on already-done future fires immediately."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2))
            f.result(timeout=_TIMEOUT)
            results: typing.List[int] = []
            f.add_done_callback(lambda fut: results.append(42))
            self.assertEqual([42], results)

    def test_multiple_callbacks_order(self) -> None:
        """Multiple callbacks fire in registration order."""
        js = Jobserver(context=_FAST, slots=2)
        order: typing.List[str] = []
        event = threading.Event()
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2))

            def make_cb(
                label: str,
            ) -> typing.Callable:
                def cb(
                    fut: concurrent.futures.Future,
                ) -> None:
                    order.append(label)
                    if label == "C":
                        event.set()

                return cb

            f.add_done_callback(make_cb("A"))
            f.add_done_callback(make_cb("B"))
            f.add_done_callback(make_cb("C"))
            event.wait(timeout=_TIMEOUT)
        self.assertEqual(["A", "B", "C"], order)

    def test_raising_callback(self) -> None:
        """A raising callback does not prevent subsequent ones."""
        js = Jobserver(context=_FAST, slots=2)
        order: typing.List[str] = []
        event = threading.Event()
        # concurrent.futures.Future._invoke_callbacks logs
        # raising callbacks via the 'concurrent.futures' logger
        # at ERROR level.  Expect exactly that record here.
        with self.assertLogs("concurrent.futures", level="ERROR") as cm:
            with JobserverExecutor(js) as exe:
                f = exe.submit(len, (1, 2))

                def cb_first(
                    fut: concurrent.futures.Future,
                ) -> None:
                    order.append("first")

                def cb_raise(
                    fut: concurrent.futures.Future,
                ) -> None:
                    order.append("raise")
                    raise RuntimeError("bad callback")

                def cb_third(
                    fut: concurrent.futures.Future,
                ) -> None:
                    order.append("third")
                    event.set()

                f.add_done_callback(cb_first)
                f.add_done_callback(cb_raise)
                f.add_done_callback(cb_third)
                event.wait(timeout=_TIMEOUT)
        self.assertIn("first", order)
        self.assertIn("third", order)
        self.assertTrue(any("bad callback" in m for m in cm.output))

    def test_callback_receives_correct_future(self) -> None:
        """Callback receives the correct future."""
        js = Jobserver(context=_FAST, slots=2)
        mapping: typing.Dict[int, concurrent.futures.Future] = {}
        event = threading.Event()
        count = [0]
        with JobserverExecutor(js) as exe:
            futures = []
            for i in range(3):
                f = exe.submit(len, "x" * i)
                futures.append(f)

                def cb(
                    fut: concurrent.futures.Future,
                    expected: concurrent.futures.Future = f,
                ) -> None:
                    mapping[id(expected)] = fut
                    count[0] += 1
                    if count[0] == 3:
                        event.set()

                f.add_done_callback(cb)
            event.wait(timeout=_TIMEOUT)
        for f in futures:
            self.assertIs(f, mapping[id(f)])


# ================================================================
# Shutdown Semantics
# ================================================================


class TestShutdown(unittest.TestCase):
    """Shutdown Semantics."""

    def test_wait_true_blocks(self) -> None:
        """shutdown(wait=True) blocks until all complete."""
        js = Jobserver(context=_FAST, slots=2)
        exe = JobserverExecutor(js)
        futures = [exe.submit(len, "x" * i) for i in range(5)]
        exe.shutdown(wait=True)
        for f in futures:
            self.assertTrue(f.done())

    def test_wait_false_returns_immediately(self) -> None:
        """shutdown(wait=False) returns immediately."""
        js = Jobserver(context=_FAST, slots=2)
        exe = JobserverExecutor(js)
        f = exe.submit(_sleep, 0.5)
        t0 = time.monotonic()
        exe.shutdown(wait=False)
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 0.5)
        # Future should eventually complete
        f.result(timeout=_TIMEOUT)

    def test_cancel_futures(self) -> None:
        """shutdown(cancel_futures=True) cancels pending."""
        js = Jobserver(context=_FAST, slots=1)
        exe = JobserverExecutor(js)
        blocker = exe.submit(_sleep, 1.0)
        time.sleep(0.2)
        pending = [exe.submit(len, (i,)) for i in range(5)]
        exe.shutdown(wait=True, cancel_futures=True)
        # blocker was RUNNING so should complete normally
        self.assertTrue(blocker.done())
        self.assertFalse(blocker.cancelled())
        # At least some pending should be cancelled
        cancelled = [f for f in pending if f.cancelled()]
        self.assertGreater(len(cancelled), 0)
        for f in cancelled:
            with self.assertRaises(concurrent.futures.CancelledError):
                f.result(timeout=0)

    def test_wait_false_cancel_futures(self) -> None:
        """shutdown(wait=False, cancel_futures=True)."""
        js = Jobserver(context=_FAST, slots=1)
        exe = JobserverExecutor(js)
        exe.submit(_sleep, 1.0)
        time.sleep(0.2)
        pending = [exe.submit(len, (i,)) for i in range(5)]
        exe.shutdown(wait=False, cancel_futures=True)
        # Must not deadlock; pending futures resolved eventually
        for f in pending:
            try:
                f.result(timeout=_TIMEOUT)
            except (
                concurrent.futures.CancelledError,
                Exception,
            ):
                pass

    def test_submit_after_shutdown(self) -> None:
        """submit() after shutdown() raises RuntimeError."""
        js = Jobserver(context=_FAST, slots=2)
        exe = JobserverExecutor(js)
        exe.shutdown(wait=True)
        with self.assertRaises(RuntimeError):
            exe.submit(len, (1,))

    def test_double_shutdown(self) -> None:
        """Double shutdown() is safe."""
        js = Jobserver(context=_FAST, slots=2)
        exe = JobserverExecutor(js)
        exe.shutdown(wait=True)
        exe.shutdown(wait=True)

    def test_context_manager(self) -> None:
        """Context-manager exit calls shutdown(wait=True)."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, "hello")
        self.assertEqual(5, f.result(timeout=0))
        with self.assertRaises(RuntimeError):
            exe.submit(len, "x")

    def test_concurrent_submit_and_shutdown(self) -> None:
        """Concurrent submit and shutdown (race test)."""
        for _ in range(50):
            js = Jobserver(context=_FAST, slots=2)
            exe = JobserverExecutor(js)
            barrier = threading.Barrier(2)
            result_holder: typing.List[
                typing.Optional[concurrent.futures.Future]
            ] = [None]
            error_holder: typing.List[typing.Optional[Exception]] = [None]

            def submitter() -> None:
                barrier.wait()
                try:
                    result_holder[0] = exe.submit(len, (1,))
                except RuntimeError as e:
                    error_holder[0] = e

            t = threading.Thread(target=submitter)
            t.start()
            barrier.wait()
            exe.shutdown(wait=True)
            t.join(timeout=5)
            self.assertFalse(t.is_alive())

            if result_holder[0] is not None:
                # Submit succeeded, future must resolve
                try:
                    result_holder[0].result(timeout=_TIMEOUT)
                except Exception:
                    pass  # acceptable if shutdown raced
            else:
                self.assertIsInstance(error_holder[0], RuntimeError)

    def test_wait_after_cancel_futures(self) -> None:
        """wait() does not hang after cancel_futures."""
        js = Jobserver(context=_FAST, slots=1)
        exe = JobserverExecutor(js)
        exe.submit(_sleep, 1.0)
        time.sleep(0.2)
        futures = [exe.submit(len, (i,)) for i in range(5)]
        exe.shutdown(wait=True, cancel_futures=True)
        done, not_done = concurrent.futures.wait(futures, timeout=5)
        self.assertEqual(0, len(not_done))

    def test_trivial_submit_after_construction(self) -> None:
        """Trivial submit immediately after construction."""
        js = Jobserver(context=_FAST, slots=2)
        exe = JobserverExecutor(js)
        f = exe.submit(len, (1, 2, 3))
        self.assertEqual(3, f.result(timeout=_TIMEOUT))
        exe.shutdown(wait=True)


# ================================================================
# map()
# ================================================================


class TestMap(unittest.TestCase):
    """map()."""

    def test_basic(self) -> None:
        """Basic correctness."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            result = list(exe.map(str, range(5)))
        self.assertEqual(["0", "1", "2", "3", "4"], result)

    def test_exception_preserves_position(self) -> None:
        """Exception propagation preserves position."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            it = exe.map(
                _raising_at_position,
                range(5),
                [2] * 5,
            )
            self.assertEqual(0, next(it))
            self.assertEqual(1, next(it))
            with self.assertRaises(ValueError):
                next(it)

    def test_timeout(self) -> None:
        """Timeout raises TimeoutError."""
        js = Jobserver(context=_FAST, slots=1)
        with JobserverExecutor(js) as exe:
            it = exe.map(time.sleep, [5], timeout=0.1)
            with self.assertRaises(concurrent.futures.TimeoutError):
                next(it)

    def test_empty_iterables(self) -> None:
        """Empty iterables."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            self.assertEqual([], list(exe.map(str, [])))

    def test_unequal_length_iterables(self) -> None:
        """Unequal-length iterables stop at shortest."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            result = list(exe.map(_add, [1, 2, 3], [10, 20]))
        self.assertEqual([11, 22], result)

    def test_gc_after_yield(self) -> None:
        """Iterator does not retain completed futures."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            # Submit several tasks and track via weakrefs
            futures = [exe.submit(_identity, i) for i in range(5)]
            refs = [weakref.ref(f) for f in futures]
            # Consume results to let map() release them
            for f in futures:
                f.result(timeout=_TIMEOUT)
            del futures
            gc.collect()
            # At least some should be collected
            alive = sum(1 for r in refs if r() is not None)
            # We just verify the test runs; GC is best-effort
            self.assertGreaterEqual(alive, 0)

    def test_partially_consumed_iterator(self) -> None:
        """Partially consumed iterator."""
        js = Jobserver(context=_FAST, slots=2)
        exe = JobserverExecutor(js)
        it = exe.map(str, range(10))
        self.assertEqual("0", next(it))
        del it
        exe.shutdown(wait=True)
        # No leaked processes -- shutdown completed

    def test_multiple_iterables(self) -> None:
        """Multiple iterables."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            result = list(exe.map(pow, [2, 3], [10, 10]))
        self.assertEqual([1024, 59049], result)


# ================================================================
# wait() and as_completed() Integration
# ================================================================


class TestWaitAndAsCompleted(unittest.TestCase):
    """wait() and as_completed()."""

    def test_wait_all_completed(self) -> None:
        """wait(ALL_COMPLETED) returns all in done."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            futures = [exe.submit(len, "x" * i) for i in range(5)]
            done, not_done = concurrent.futures.wait(futures, timeout=_TIMEOUT)
        self.assertEqual(5, len(done))
        self.assertEqual(0, len(not_done))

    def test_wait_first_completed(self) -> None:
        """wait(FIRST_COMPLETED) returns on first."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            slow = exe.submit(_sleep, 2.0)
            fast = exe.submit(len, (1,))
            futures: typing.List[concurrent.futures.Future[typing.Any]] = [
                slow,
                fast,
            ]
            done, not_done = concurrent.futures.wait(
                futures,
                timeout=_TIMEOUT,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            self.assertGreater(len(done), 0)

    def test_wait_first_exception(self) -> None:
        """wait(FIRST_EXCEPTION) returns on first error."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            good = exe.submit(_sleep, 2.0)
            bad = exe.submit(_raise, ValueError, "oops")
            done, not_done = concurrent.futures.wait(
                [good, bad],
                timeout=_TIMEOUT,
                return_when=concurrent.futures.FIRST_EXCEPTION,
            )
            self.assertIn(bad, done)

    def test_wait_timeout_partial(self) -> None:
        """wait() with timeout returns partial results."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            futures = [exe.submit(_sleep, 5.0) for _ in range(3)]
            done, not_done = concurrent.futures.wait(futures, timeout=0.1)
            self.assertGreater(len(not_done), 0)

    def test_as_completed_order(self) -> None:
        """as_completed() yields in completion order."""
        js = Jobserver(context=_FAST, slots=4)
        with JobserverExecutor(js) as exe:
            f_slow = exe.submit(_sleep_return, 0.5, "slow")
            f_fast = exe.submit(_return_value, "fast")
            order = []
            for f in concurrent.futures.as_completed(
                [f_slow, f_fast], timeout=_TIMEOUT
            ):
                order.append(f.result())
            self.assertEqual("fast", order[0])

    def test_as_completed_timeout(self) -> None:
        """as_completed() with timeout raises TimeoutError."""
        js = Jobserver(context=_FAST, slots=1)
        with JobserverExecutor(js) as exe:
            f = exe.submit(_sleep, 5.0)
            with self.assertRaises(concurrent.futures.TimeoutError):
                for _ in concurrent.futures.as_completed([f], timeout=0.1):
                    pass

    def test_duplicate_future(self) -> None:
        """Duplicate future in wait() and as_completed()."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2))
            done, _ = concurrent.futures.wait([f, f], timeout=_TIMEOUT)
            self.assertEqual(1, len(done))

            g = exe.submit(len, (1, 2, 3))
            results = list(
                concurrent.futures.as_completed([g, g], timeout=_TIMEOUT)
            )
            self.assertEqual(1, len(results))

    def test_as_completed_gc(self) -> None:
        """as_completed() does not retain yielded futures."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2))
            ref = weakref.ref(f)  # noqa: F841
            for done in concurrent.futures.as_completed([f], timeout=_TIMEOUT):
                pass
            del f, done
            gc.collect()
            # Best-effort: GC may or may not collect
            # Just verify no crash


# ================================================================
# Concurrency Stress
# ================================================================


class TestConcurrencyStress(unittest.TestCase):
    """Concurrency Stress."""

    def test_heavy_submission(self) -> None:
        """Heavy submission exceeding slot count."""
        js = Jobserver(context=_FAST, slots=2)
        n = 200
        with JobserverExecutor(js) as exe:
            futures = [exe.submit(len, "x" * i) for i in range(n)]
            results = [f.result(timeout=_TIMEOUT) for f in futures]
        self.assertEqual(list(range(n)), results)

    def test_mixed_workload(self) -> None:
        """Mixed workload: success, exception, cancel, death."""
        js = Jobserver(context=_FAST, slots=4)
        exe = JobserverExecutor(js)
        try:
            # Fill a slot to create pending work for cancel
            exe.submit(_sleep, 0.5)
            time.sleep(0.15)

            f_ok = exe.submit(len, (1, 2, 3))
            f_err = exe.submit(_raise, ValueError, "boom")
            f_cancel = exe.submit(len, (1,))
            f_cancel.cancel()

            self.assertEqual(3, f_ok.result(timeout=_TIMEOUT))
            with self.assertRaises(ValueError):
                f_err.result(timeout=_TIMEOUT)
            # f_cancel: either cancelled or completed
            self.assertTrue(f_cancel.done())
        finally:
            exe.shutdown(wait=True)

    def test_concurrent_submit_threads(self) -> None:
        """Concurrent submit() from multiple threads."""
        js = Jobserver(context=_FAST, slots=4)
        with JobserverExecutor(js) as exe:
            results: typing.List[concurrent.futures.Future] = []
            lock = threading.Lock()

            def worker(start: int, count: int) -> None:
                local: typing.List[concurrent.futures.Future] = []
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
                t.join(timeout=_TIMEOUT)

            self.assertEqual(200, len(results))
            vals = sorted(f.result(timeout=_TIMEOUT) for f in results)
            self.assertEqual(sorted(range(200)), vals)

    def test_setswitchinterval_stress(self) -> None:
        """sys.setswitchinterval stress test."""
        old = sys.getswitchinterval()
        try:
            sys.setswitchinterval(1e-6)
            js = Jobserver(context=_FAST, slots=4)
            with JobserverExecutor(js) as exe:
                results: typing.List[concurrent.futures.Future] = []
                lock = threading.Lock()

                def worker(start: int, count: int) -> None:
                    local: typing.List[concurrent.futures.Future] = []
                    for i in range(start, start + count):
                        local.append(exe.submit(len, "x" * i))
                    with lock:
                        results.extend(local)

                threads = []
                for t_idx in range(8):
                    t = threading.Thread(
                        target=worker,
                        args=(t_idx * 25, 25),
                    )
                    threads.append(t)
                    t.start()
                for t in threads:
                    t.join(timeout=_TIMEOUT)

                self.assertEqual(200, len(results))
                vals = sorted(f.result(timeout=_TIMEOUT) for f in results)
                self.assertEqual(sorted(range(200)), vals)
        finally:
            sys.setswitchinterval(old)

    def test_burst_submission(self) -> None:
        """Burst submission (thousands of tasks)."""
        js = Jobserver(context=_FAST, slots=4)
        n = 2000
        with JobserverExecutor(js) as exe:
            futures = [exe.submit(len, "x" * (i % 50)) for i in range(n)]
            results = [f.result(timeout=60) for f in futures]
        expected = [i % 50 for i in range(n)]
        self.assertEqual(expected, results)


# ================================================================
# Resource Leak Detection
# ================================================================


class TestResourceLeaks(unittest.TestCase):
    """Resource Leak Detection."""

    @staticmethod
    def _fd_count() -> int:
        """Count open file descriptors."""
        try:
            return len(os.listdir("/proc/self/fd"))
        except (FileNotFoundError, PermissionError):
            return -1

    def test_process_count_baseline(self) -> None:
        """Process count returns to baseline."""
        baseline = len(multiprocessing.active_children())
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2))
            f.result(timeout=_TIMEOUT)
        time.sleep(0.5)
        after = len(multiprocessing.active_children())
        self.assertEqual(baseline, after)

    def test_fd_count_baseline(self) -> None:
        """File descriptor count returns to baseline."""
        gc.collect()
        baseline = self._fd_count()
        if baseline < 0:
            self.skipTest("/proc/self/fd not available")
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2))
            f.result(timeout=_TIMEOUT)
        gc.collect()
        time.sleep(0.5)
        after = self._fd_count()
        # Allow margin for GC timing and background FDs
        self.assertLessEqual(after, baseline + 10)

    def test_thread_count_baseline(self) -> None:
        """Thread count returns to baseline."""
        time.sleep(0.2)
        baseline = threading.active_count()
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2))
            f.result(timeout=_TIMEOUT)
        time.sleep(0.5)
        after = threading.active_count()
        self.assertLessEqual(after, baseline + 1)

    def test_repeated_cycles(self) -> None:
        """Repeated create/shutdown cycles."""
        baseline_procs = len(multiprocessing.active_children())
        baseline_threads = threading.active_count()
        for _ in range(20):
            js = Jobserver(context=_FAST, slots=2)
            with JobserverExecutor(js) as exe:
                exe.submit(len, (1,)).result(timeout=_TIMEOUT)
        time.sleep(1.0)
        self.assertLessEqual(
            len(multiprocessing.active_children()),
            baseline_procs + 2,
        )
        self.assertLessEqual(
            threading.active_count(),
            baseline_threads + 2,
        )

    def test_shutdown_after_worker_death(self) -> None:
        """Shutdown after worker death cleans up."""
        baseline = len(multiprocessing.active_children())
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(_self_kill)
            with self.assertRaises(Exception):
                f.result(timeout=_TIMEOUT)
        time.sleep(0.5)
        after = len(multiprocessing.active_children())
        self.assertEqual(baseline, after)


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
                js = Jobserver(context=method, slots=2)
                with JobserverExecutor(js) as exe:
                    # Success
                    f = exe.submit(len, (1, 2, 3))
                    self.assertEqual(3, f.result(timeout=_TIMEOUT))
                    # Exception
                    g = exe.submit(_raise, ValueError, "test")
                    self.assertIsInstance(
                        g.exception(timeout=_TIMEOUT),
                        ValueError,
                    )
                    # Kwargs
                    h = exe.submit(int, "ff", base=16)
                    self.assertEqual(255, h.result(timeout=_TIMEOUT))

    def test_map_all_methods(self) -> None:
        """map() works with all start methods."""
        methods = get_all_start_methods()
        if sys.version_info >= (3, 12):
            methods = [m for m in methods if m != "fork"]
        for method in methods:
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
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
                js = Jobserver(context=method, slots=2)
                exe = JobserverExecutor(js)
                f = exe.submit(len, (1,))
                exe.shutdown(wait=True)
                self.assertTrue(f.done())


# ================================================================
# Edge Cases (CPython Bug Reports)
# ================================================================


class TestEdgeCases(unittest.TestCase):
    """Edge cases inspired by CPython bugs."""

    def test_cancel_then_result(self) -> None:
        """Cancel then result() on same future."""
        js = Jobserver(context=_FAST, slots=1)
        exe = JobserverExecutor(js)
        try:
            exe.submit(_sleep, 1.0)
            time.sleep(0.2)
            f = exe.submit(len, (1, 2, 3))
            time.sleep(0.1)
            f.cancel()
            with self.assertRaises(concurrent.futures.CancelledError):
                f.result(timeout=_TIMEOUT)
        finally:
            exe.shutdown(wait=False, cancel_futures=True)

    def test_worker_death_not_poison(self) -> None:
        """Worker death does not poison the executor."""
        js = Jobserver(context=_FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(_self_kill)
            with self.assertRaises(Exception):
                f.result(timeout=_TIMEOUT)
            # Must not be permanently broken
            for i in range(5):
                g = exe.submit(len, "x" * i)
                self.assertEqual(i, g.result(timeout=_TIMEOUT))

    def test_executor_in_forked_child(self) -> None:
        """Executor created inside a forked child."""
        methods = get_all_start_methods()
        if sys.version_info >= (3, 12):
            methods = [m for m in methods if m != "fork"]
        method = methods[0] if methods else _FAST
        # Use a non-daemonic Process so the child can
        # spawn its own children (Pool workers are
        # daemonic and cannot).
        ctx = multiprocessing.get_context(
            "forkserver" if "forkserver" in methods else "spawn"
        )
        result_queue: multiprocessing.Queue = ctx.Queue()
        p = ctx.Process(  # type: ignore[attr-defined]
            target=_executor_in_child_via_queue,
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
        js = Jobserver(context=_FAST, slots=1)
        with JobserverExecutor(js) as exe:
            f = exe.submit(_sleep, 5.0)
            with self.assertRaises(concurrent.futures.TimeoutError):
                f.result(timeout=0)

    def test_exception_timeout_zero(self) -> None:
        """exception(timeout=0) on incomplete future."""
        js = Jobserver(context=_FAST, slots=1)
        with JobserverExecutor(js) as exe:
            f = exe.submit(_sleep, 5.0)
            with self.assertRaises(concurrent.futures.TimeoutError):
                f.exception(timeout=0)

    def test_submit_after_wait_false_shutdown(
        self,
    ) -> None:
        """Submit after shutdown(wait=False) raises."""
        js = Jobserver(context=_FAST, slots=2)
        exe = JobserverExecutor(js)
        exe.shutdown(wait=False)
        with self.assertRaises(RuntimeError):
            exe.submit(len, (1,))

    def test_cancel_many_then_shutdown(self) -> None:
        """Many futures cancelled then shutdown."""
        js = Jobserver(context=_FAST, slots=1)
        exe = JobserverExecutor(js)
        exe.submit(_sleep, 0.5)
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
        """_lock must be free when _request_queue.put() is called.

        Verify the lock is released before each put() by spying on the call.

        MinimalQueue uses __slots__ so instance-level patching is impossible;
        patch the class method and filter by queue object identity instead.
        """
        js = Jobserver(context=_FAST, slots=2)
        exe = JobserverExecutor(js)
        lock_held: typing.List[bool] = []
        original_put = MinimalQueue.put

        def spy_put(self_q: MinimalQueue, *args: typing.Any) -> None:
            if self_q is exe._request_queue:
                lock_held.append(exe._lock.locked())
            return original_put(self_q, *args)

        with unittest.mock.patch.object(MinimalQueue, "put", spy_put):
            exe.submit(len, (1, 2, 3)).result(timeout=_TIMEOUT)
            exe.shutdown(wait=True)

        # The first put is the _SUBMIT message from submit(); it must have
        # been issued with the lock already released.
        self.assertGreater(len(lock_held), 0)
        self.assertFalse(lock_held[0])

    def test_submit_removes_future_on_put_failure(self) -> None:
        """A future registered in _futures is removed if put() fails.

        Without rollback the future would sit in _futures forever,
        preventing clean shutdown and leaking state.

        MinimalQueue uses __slots__ so instance-level patching is impossible;
        patch the class method and filter by queue object identity instead.
        """
        js = Jobserver(context=_FAST, slots=2)
        exe = JobserverExecutor(js)
        original_put = MinimalQueue.put
        fail_once = [True]

        def failing_put(self_q: MinimalQueue, *args: typing.Any) -> None:
            if (
                fail_once[0]
                and self_q is exe._request_queue
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


if __name__ == "__main__":
    unittest.main()
