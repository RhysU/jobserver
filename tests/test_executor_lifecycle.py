# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""JobserverExecutor lifecycle management.

Covers the executor's start-to-finish lifecycle: cancellation of pending
and running futures, shutdown semantics (wait, cancel_futures), and
verification that processes, threads, and file descriptors are released.
"""
import concurrent.futures
import gc
import multiprocessing
import os
import signal
import threading
import time
import typing
import unittest

from jobserver import JobserverExecutor, Jobserver

from .helpers import (
    FAST,
    TIMEOUT,
    silence_forkserver,
)


def setUpModule() -> None:
    silence_forkserver()


# ================================================================
# Cancellation
# ================================================================


class TestCancellation(unittest.TestCase):
    """Cancellation."""

    def test_cancel_pending(self) -> None:
        """A PENDING future can be cancelled."""
        js = Jobserver(context=FAST, slots=1)
        exe = JobserverExecutor(js)
        try:
            exe.submit(time.sleep, 0.4)
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

    def test_cancel_running(self) -> None:
        """A RUNNING future cannot be cancelled."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(time.sleep, 0.3)
            deadline = time.monotonic() + 5
            while not f.running() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertFalse(f.cancel())
            self.assertFalse(f.cancelled())
            f.result(timeout=TIMEOUT)

    def test_cancel_finished(self) -> None:
        """A FINISHED future cannot be cancelled."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2))
            f.result(timeout=TIMEOUT)
            self.assertFalse(f.cancel())

    def test_rapid_submit_cancel_churn(self) -> None:
        """Rapid submit-then-cancel churn."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            for _ in range(200):
                f = exe.submit(len, (1,))
                f.cancel()
        # No deadlock, no crash -- shutdown completes

    def test_cancel_racing_with_dispatch(self) -> None:
        """Cancel racing with dispatch."""
        js = Jobserver(context=FAST, slots=1)
        exe = JobserverExecutor(js)
        try:
            blocker = exe.submit(time.sleep, 1.0)
            time.sleep(0.1)
            f = exe.submit(len, (1, 2, 3))
            time.sleep(0.05)
            f.cancel()
            # Now free the slot
            blocker.result(timeout=TIMEOUT)
            # f must stay cancelled even though a slot opened
            self.assertTrue(f.cancelled())
        finally:
            exe.shutdown(wait=True)


# ================================================================
# Shutdown Semantics
# ================================================================


class TestShutdown(unittest.TestCase):
    """Shutdown Semantics."""

    def test_wait_true_blocks(self) -> None:
        """shutdown(wait=True) blocks until all complete."""
        js = Jobserver(context=FAST, slots=2)
        exe = JobserverExecutor(js)
        futures = [exe.submit(len, "x" * i) for i in range(5)]
        exe.shutdown(wait=True)
        for f in futures:
            self.assertTrue(f.done())

    def test_wait_false_returns_immediately(self) -> None:
        """shutdown(wait=False) returns immediately."""
        js = Jobserver(context=FAST, slots=2)
        exe = JobserverExecutor(js)
        f = exe.submit(time.sleep, 1.0)
        t0 = time.monotonic()
        exe.shutdown(wait=False)
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 0.5)
        # Future should eventually complete
        f.result(timeout=TIMEOUT)

    def test_cancel_futures(self) -> None:
        """shutdown(cancel_futures=True) cancels pending."""
        js = Jobserver(context=FAST, slots=1)
        exe = JobserverExecutor(js)
        blocker = exe.submit(time.sleep, 1.0)
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
        js = Jobserver(context=FAST, slots=1)
        exe = JobserverExecutor(js)
        exe.submit(time.sleep, 0.3)
        time.sleep(0.1)
        pending = [exe.submit(len, (i,)) for i in range(5)]
        exe.shutdown(wait=False, cancel_futures=True)
        # Must not deadlock; pending futures resolved eventually
        for f in pending:
            try:
                f.result(timeout=TIMEOUT)
            except (
                concurrent.futures.CancelledError,
                Exception,
            ):
                pass

    def test_submit_after_shutdown(self) -> None:
        """submit() after shutdown() raises RuntimeError."""
        for wait in (True, False):
            with self.subTest(wait=wait):
                js = Jobserver(context=FAST, slots=2)
                exe = JobserverExecutor(js)
                exe.shutdown(wait=wait)
                with self.assertRaises(RuntimeError):
                    exe.submit(len, (1,))

    def test_double_shutdown(self) -> None:
        """Double shutdown() is safe."""
        js = Jobserver(context=FAST, slots=2)
        exe = JobserverExecutor(js)
        exe.shutdown(wait=True)
        exe.shutdown(wait=True)

    def test_context_manager(self) -> None:
        """Context-manager exit calls shutdown(wait=True)."""
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, "hello")
        self.assertEqual(5, f.result(timeout=0))
        with self.assertRaises(RuntimeError):
            exe.submit(len, "x")

    def test_concurrent_submit_and_shutdown(self) -> None:
        """Concurrent submit and shutdown (race test)."""
        for _ in range(50):
            js = Jobserver(context=FAST, slots=2)
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
                    result_holder[0].result(timeout=TIMEOUT)
                except Exception:
                    pass  # acceptable if shutdown raced
            else:
                self.assertIsInstance(error_holder[0], RuntimeError)

    def test_wait_after_cancel_futures(self) -> None:
        """wait() does not hang after cancel_futures."""
        js = Jobserver(context=FAST, slots=1)
        exe = JobserverExecutor(js)
        exe.submit(time.sleep, 0.3)
        time.sleep(0.1)
        futures = [exe.submit(len, (i,)) for i in range(5)]
        exe.shutdown(wait=True, cancel_futures=True)
        done, not_done = concurrent.futures.wait(futures, timeout=5)
        self.assertEqual(0, len(not_done))

    def test_trivial_submit_after_construction(self) -> None:
        """Trivial submit immediately after construction."""
        js = Jobserver(context=FAST, slots=2)
        exe = JobserverExecutor(js)
        f = exe.submit(len, (1, 2, 3))
        self.assertEqual(3, f.result(timeout=TIMEOUT))
        exe.shutdown(wait=True)


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
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2))
            f.result(timeout=TIMEOUT)
        time.sleep(0.5)
        after = len(multiprocessing.active_children())
        self.assertEqual(baseline, after)

    def test_fd_count_baseline(self) -> None:
        """File descriptor count returns to baseline."""
        gc.collect()
        baseline = self._fd_count()
        if baseline < 0:
            self.skipTest("/proc/self/fd not available")
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2))
            f.result(timeout=TIMEOUT)
        gc.collect()
        time.sleep(0.2)
        after = self._fd_count()
        # Allow margin for GC timing and background FDs
        self.assertLessEqual(after, baseline + 10)

    def test_thread_count_baseline(self) -> None:
        """Thread count returns to baseline."""
        time.sleep(0.2)
        baseline = threading.active_count()
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(len, (1, 2))
            f.result(timeout=TIMEOUT)
        time.sleep(0.2)
        after = threading.active_count()
        self.assertLessEqual(after, baseline + 1)

    def test_repeated_cycles(self) -> None:
        """Repeated create/shutdown cycles."""
        baseline_procs = len(multiprocessing.active_children())
        baseline_threads = threading.active_count()
        for _ in range(20):
            js = Jobserver(context=FAST, slots=2)
            with JobserverExecutor(js) as exe:
                exe.submit(len, (1,)).result(timeout=TIMEOUT)
        time.sleep(0.5)
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
        js = Jobserver(context=FAST, slots=2)
        with JobserverExecutor(js) as exe:
            f = exe.submit(signal.raise_signal, signal.SIGKILL)
            with self.assertRaises(Exception):
                f.result(timeout=TIMEOUT)
        time.sleep(0.5)
        after = len(multiprocessing.active_children())
        self.assertEqual(baseline, after)

    def test_dispatcher_death_orphans_futures(self) -> None:
        """Killing the dispatcher fails outstanding futures."""
        js = Jobserver(context=FAST, slots=1)
        exe = JobserverExecutor(js)
        try:
            # Submit slow work so a future is outstanding
            f = exe.submit(time.sleep, 10)
            # Wait for the dispatcher to be alive
            deadline = time.monotonic() + 5
            while (
                not exe._dispatcher.is_alive() and time.monotonic() < deadline
            ):
                time.sleep(0.01)
            self.assertTrue(exe._dispatcher.is_alive())
            # Kill the dispatcher
            os.kill(exe._dispatcher.pid, signal.SIGKILL)
            # The outstanding future must surface an error
            with self.assertRaises(Exception):
                f.result(timeout=TIMEOUT)
        finally:
            exe.shutdown(wait=True)

    def test_submit_after_dispatcher_death(self) -> None:
        """submit() after dispatcher death raises RuntimeError."""
        js = Jobserver(context=FAST, slots=2)
        exe = JobserverExecutor(js)
        try:
            f = exe.submit(len, (1, 2))
            f.result(timeout=TIMEOUT)
            # Kill the dispatcher
            os.kill(exe._dispatcher.pid, signal.SIGKILL)
            exe._dispatcher.join(timeout=5)
            # Give the receiver thread time to notice the EOF
            time.sleep(0.2)
            # submit() should now raise RuntimeError because the
            # request pipe is broken
            with self.assertRaises(RuntimeError):
                exe.submit(len, (1,))
        finally:
            exe.shutdown(wait=True)

    def test_shutdown_after_dispatcher_death(self) -> None:
        """shutdown() tolerates an already-dead dispatcher."""
        js = Jobserver(context=FAST, slots=2)
        exe = JobserverExecutor(js)
        f = exe.submit(len, (1, 2))
        f.result(timeout=TIMEOUT)
        # Kill the dispatcher
        os.kill(exe._dispatcher.pid, signal.SIGKILL)
        exe._dispatcher.join(timeout=5)
        time.sleep(0.2)
        # shutdown() must not raise despite BrokenPipeError
        exe.shutdown(wait=True)
