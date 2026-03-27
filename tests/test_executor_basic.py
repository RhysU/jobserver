# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Core Future API: submit, result, exceptions, state queries, callbacks, map.

Tests the fundamental data-flow and single-future behaviour of
JobserverExecutor as a concurrent.futures.Executor.
"""
import concurrent.futures
import threading
import time
import typing
import unittest

from jobserver import JobserverExecutor, Jobserver

from .helpers import (
    FAST,
    TIMEOUT,
    add,
    barrier_wait,
    raise_,
    raising_at_position,
    return_exception,
    return_value,
    round_trip_bytes,
    self_kill,
    silence_forkserver,
    sleep,
    sleep_return,
    sys_exit,
)


def setUpModule() -> None:
    silence_forkserver()


# ================================================================
# Submit and Result
# ================================================================


class TestSubmitAndResult(unittest.TestCase):
    """Submit and Result."""

    def test_successful_call(self) -> None:
        """Successful call returns correct result."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2, 3))
            self.assertEqual(3, f.result(timeout=TIMEOUT))

    def test_keyword_arguments(self) -> None:
        """Keyword arguments are forwarded."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(int, "ff", base=16)
            self.assertEqual(255, f.result(timeout=TIMEOUT))

    def test_none_return(self) -> None:
        """None can be returned."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(min, (), default=None)
            self.assertIsNone(f.result(timeout=TIMEOUT))

    def test_multiple_concurrent(self) -> None:
        """Multiple concurrent submissions."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            futures = [exe.submit(len, "x" * i) for i in range(10)]
            results = [f.result(timeout=TIMEOUT) for f in futures]
        self.assertEqual(list(range(10)), results)

    def test_returns_cf_future(self) -> None:
        """submit() returns a concurrent.futures.Future."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1,))
            self.assertIsInstance(f, concurrent.futures.Future)
            f.result(timeout=TIMEOUT)


# ================================================================
# Exception Propagation
# ================================================================


class TestExceptionPropagation(unittest.TestCase):
    """Exception Propagation."""

    def test_exception_via_result(self) -> None:
        """Exception raised in callable surfaces via result()."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(raise_, ValueError, "raised")
            with self.assertRaises(ValueError):
                f.result(timeout=TIMEOUT)

    def test_exception_method(self) -> None:
        """exception() returns the raised exception."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(raise_, ValueError, "raised")
            exc = f.exception(timeout=TIMEOUT)
            self.assertIsInstance(exc, ValueError)

    def test_exception_none_on_success(self) -> None:
        """exception() returns None on success."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2))
            self.assertIsNone(f.exception(timeout=TIMEOUT))

    def test_exception_returned_not_raised(self) -> None:
        """An Exception can be returned (not raised)."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(return_exception)
            result = f.result(timeout=TIMEOUT)
            self.assertIsInstance(result, ValueError)
            self.assertEqual(("not raised",), result.args)

    def test_worker_killed_by_signal(self) -> None:
        """Worker killed by signal does not poison the executor."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(self_kill)
            with self.assertRaises(Exception):
                f.result(timeout=TIMEOUT)
            # Executor must recover for multiple subsequent tasks
            for i in range(5):
                g = exe.submit(len, "x" * i)
                self.assertEqual(i, g.result(timeout=TIMEOUT))

    def test_sys_exit(self) -> None:
        """Worker exits via sys.exit()."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(sys_exit, 1)
            with self.assertRaises(Exception):
                f.result(timeout=TIMEOUT)
            # Executor must remain usable
            g = exe.submit(len, (1, 2, 3))
            self.assertEqual(3, g.result(timeout=TIMEOUT))

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
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            size = 10 * 1024 * 1024  # 10 MB
            f = exe.submit(round_trip_bytes, size)
            result = f.result(timeout=TIMEOUT)
            self.assertEqual(size, len(result))
            self.assertEqual(b"x" * 10, result[:10])


# ================================================================
# Future State Queries
# ================================================================


class TestFutureStateQueries(unittest.TestCase):
    """Future State Queries."""

    def test_pending_when_slots_full(self) -> None:
        """A newly submitted future is PENDING when slots full."""
        js = Jobserver(context=FAST, slots=1)
        exe = JobserverExecutor(js)
        try:
            exe.submit(sleep, 0.4)
            time.sleep(0.1)
            f = exe.submit(len, (1, 2, 3))
            time.sleep(0.05)
            self.assertFalse(f.done())
            self.assertFalse(f.running())
            self.assertFalse(f.cancelled())
        finally:
            exe.shutdown(wait=False, cancel_futures=True)

    def test_running_transition(self) -> None:
        """A dispatched future transitions to RUNNING."""
        import os
        import tempfile

        js = Jobserver(context=FAST, slots=2)
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            gate = tmp.name
        os.unlink(gate)  # remove so barrier blocks

        exe = JobserverExecutor(js)
        try:
            f = exe.submit(barrier_wait, gate)
            deadline = time.monotonic() + 5
            while not f.running() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(f.running())
            self.assertFalse(f.done())
        finally:
            # Release the barrier
            with open(gate, "w") as fh:
                fh.write("go")
            f.result(timeout=TIMEOUT)
            exe.shutdown(wait=True)
            os.unlink(gate)

    def test_finished_state(self) -> None:
        """A completed future is FINISHED."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2))
            f.result(timeout=TIMEOUT)
            self.assertTrue(f.done())
            self.assertFalse(f.running())
            self.assertFalse(f.cancelled())


# ================================================================
# Callbacks
# ================================================================


class TestCallbacks(unittest.TestCase):
    """Callbacks."""

    def test_callback_on_success(self) -> None:
        """add_done_callback fires on success."""
        js = Jobserver(context=FAST, slots=2)
        results: typing.List[typing.Any] = []
        event = threading.Event()
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2, 3))

            def cb(fut: concurrent.futures.Future) -> None:
                results.append(fut.result())
                event.set()

            f.add_done_callback(cb)
            event.wait(timeout=TIMEOUT)
        self.assertEqual([3], results)

    def test_callback_on_exception(self) -> None:
        """add_done_callback fires on exception."""
        js = Jobserver(context=FAST, slots=2)
        results: typing.List[typing.Any] = []
        event = threading.Event()
        with JobserverExecutor(js) as exe:
            f = exe.submit(raise_, ValueError, "raised")

            def cb(fut: concurrent.futures.Future) -> None:
                results.append(type(fut.exception()))
                event.set()

            f.add_done_callback(cb)
            event.wait(timeout=TIMEOUT)
        self.assertEqual([ValueError], results)

    def test_callback_on_cancellation(self) -> None:
        """add_done_callback fires on cancellation."""
        js = Jobserver(context=FAST, slots=1)
        exe = JobserverExecutor(js)
        results: typing.List[bool] = []
        event = threading.Event()
        try:
            exe.submit(sleep, 0.4)
            time.sleep(0.1)
            f = exe.submit(len, (1, 2, 3))
            time.sleep(0.05)

            def cb(fut: concurrent.futures.Future) -> None:
                results.append(fut.cancelled())
                event.set()

            f.add_done_callback(cb)
            f.cancel()
            event.wait(timeout=TIMEOUT)
        finally:
            exe.shutdown(wait=False, cancel_futures=True)
        self.assertEqual([True], results)

    def test_callback_on_already_done(self) -> None:
        """Callback on already-done future fires immediately."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2))
            f.result(timeout=TIMEOUT)
            results: typing.List[int] = []
            f.add_done_callback(lambda fut: results.append(42))
            self.assertEqual([42], results)

    def test_multiple_callbacks_order(self) -> None:
        """Multiple callbacks fire in registration order."""
        js = Jobserver(context=FAST, slots=2)
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
            event.wait(timeout=TIMEOUT)
        self.assertEqual(["A", "B", "C"], order)

    def test_raising_callback(self) -> None:
        """A raising callback does not prevent subsequent ones."""
        js = Jobserver(context=FAST, slots=2)
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
                event.wait(timeout=TIMEOUT)
        self.assertIn("first", order)
        self.assertIn("third", order)
        self.assertTrue(any("bad callback" in m for m in cm.output))

    def test_callback_receives_correct_future(self) -> None:
        """Callback receives the correct future."""
        js = Jobserver(context=FAST, slots=2)
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
            event.wait(timeout=TIMEOUT)
        for f in futures:
            self.assertIs(f, mapping[id(f)])


# ================================================================
# map()
# ================================================================


class TestMap(unittest.TestCase):
    """map()."""

    def test_basic(self) -> None:
        """Basic correctness."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            result = list(exe.map(str, range(5)))
        self.assertEqual(["0", "1", "2", "3", "4"], result)

    def test_exception_preserves_position(self) -> None:
        """Exception propagation preserves position."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            it = exe.map(
                raising_at_position,
                range(5),
                [2] * 5,
            )
            self.assertEqual(0, next(it))
            self.assertEqual(1, next(it))
            with self.assertRaises(ValueError):
                next(it)

    def test_timeout(self) -> None:
        """Timeout raises TimeoutError."""
        js = Jobserver(context=FAST, slots=1)
        with JobserverExecutor(js) as exe:
            it = exe.map(time.sleep, [1.0], timeout=0.1)
            with self.assertRaises(concurrent.futures.TimeoutError):
                next(it)

    def test_empty_iterables(self) -> None:
        """Empty iterables."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            self.assertEqual([], list(exe.map(str, [])))

    def test_unequal_length_iterables(self) -> None:
        """Unequal-length iterables stop at shortest."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            result = list(exe.map(add, [1, 2, 3], [10, 20]))
        self.assertEqual([11, 22], result)

    def test_partially_consumed_iterator(self) -> None:
        """Partially consumed iterator."""
        js = Jobserver(context=FAST, slots=2)
        exe = JobserverExecutor(js)
        it = exe.map(str, range(10))
        self.assertEqual("0", next(it))
        del it
        exe.shutdown(wait=True)
        # No leaked processes -- shutdown completed

    def test_multiple_iterables(self) -> None:
        """Multiple iterables."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            result = list(exe.map(pow, [2, 3], [10, 10]))
        self.assertEqual([1024, 59049], result)

