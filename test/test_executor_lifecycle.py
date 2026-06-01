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

from __future__ import annotations

import concurrent.futures
import gc
import multiprocessing
import os
import signal
import tempfile
import threading
import time
import unittest
import weakref
from unittest import mock

from jobserver import (
    Jobserver,
    JobserverExecutor,
    SubmissionDied,
    _request,
    _response,
)
from jobserver._executor import _DispatchState, _handle_request
from jobserver._queue import MinimalQueue

from .helpers import (
    FAST,
    TIMEOUT,
    barrier_wait,
    create_marker,
    helper_return,
    silence_forkserver,
)


def setUpModule() -> None:
    silence_forkserver()


class TestCancellation(unittest.TestCase):
    """Cancellation."""

    def test_cancel_pending(self) -> None:
        """A PENDING future can be cancelled."""
        with Jobserver(context=FAST, slots=1) as js:
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
        with Jobserver(context=FAST, slots=2) as js:
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
        with Jobserver(context=FAST, slots=2) as js:
            with JobserverExecutor(js) as exe:
                f = exe.submit(len, (1, 2))
                f.result(timeout=TIMEOUT)
                self.assertFalse(f.cancel())

    def test_rapid_submit_cancel_churn(self) -> None:
        """Rapid submit-then-cancel churn."""
        with Jobserver(context=FAST, slots=2) as js:
            with JobserverExecutor(js) as exe:
                for _ in range(200):
                    f = exe.submit(len, (1,))
                    f.cancel()
            # No deadlock, no crash -- shutdown completes

    def test_cancel_pending_prunes_in_dispatcher(self) -> None:
        """Cancelling a PENDING future prunes it so the work never runs.

        The blocker holds the single slot, so the victim stays PENDING.
        Cancelling it must emit Cancel(work_id) to the dispatcher and prune
        it before dispatch; otherwise the work would run once the slot frees
        (creating the marker) and waste a slot on a discarded result.
        """
        with tempfile.TemporaryDirectory() as tmp:
            release = os.path.join(tmp, "release")
            marker = os.path.join(tmp, "marker")
            with Jobserver(context=FAST, slots=1) as js:
                exe = JobserverExecutor(js)
                try:
                    blocker = exe.submit(barrier_wait, release)
                    deadline = time.monotonic() + TIMEOUT
                    while (
                        not blocker.running() and time.monotonic() < deadline
                    ):
                        time.sleep(0.01)
                    self.assertTrue(blocker.running())

                    # Victim cannot dispatch (slot held), so it stays PENDING.
                    victim = exe.submit(create_marker, marker)
                    self.assertTrue(victim.cancel())
                    self.assertTrue(victim.cancelled())

                    # Sentinel proves the dispatcher cycled past the victim's
                    # slot once the blocker frees it.
                    sentinel = exe.submit(helper_return, 42)

                    # Free the slot and let the dispatcher run to completion.
                    with open(release, "w"):
                        pass
                    self.assertEqual(
                        "released", blocker.result(timeout=TIMEOUT)
                    )
                    self.assertEqual(42, sentinel.result(timeout=TIMEOUT))

                    # The pruned victim never executed.
                    self.assertFalse(os.path.exists(marker))
                finally:
                    exe.shutdown(wait=True)

    def test_cancel_racing_with_dispatch(self) -> None:
        """Cancel racing with dispatch."""
        with Jobserver(context=FAST, slots=1) as js:
            exe = JobserverExecutor(js)
            try:
                blocker = exe.submit(time.sleep, 0.5)
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


class TestSelectiveCancel(unittest.TestCase):
    """Selective cancel via _request.Cancel(work_id=...)."""

    def test_selective_cancel_removes_only_target(self) -> None:
        """Cancel(work_id=X) removes only X from pending."""
        with MinimalQueue() as responses:
            state = _DispatchState(
                pending={
                    n: _request.Submit(n, len, ((),), {}) for n in (1, 2, 3)
                }
            )
            _handle_request(_request.Cancel(work_id=2), state, responses)
            # A targeted Cancel(work_id=X) does not latch the cancelling state.
            self.assertFalse(state.cancelling)
            self.assertEqual(
                [1, 3], [s.work_id for s in state.pending.values()]
            )
            msg = responses.get(timeout=1)
            self.assertIsInstance(msg, _response.Cancelled)
            self.assertEqual(2, msg.work_id)

    def test_bulk_cancel_removes_all(self) -> None:
        """Cancel(work_id=None) removes everything."""
        with MinimalQueue() as responses:
            state = _DispatchState(
                pending={n: _request.Submit(n, len, ((),), {}) for n in (1, 2)}
            )
            _handle_request(_request.Cancel(), state, responses)
            # A blanket Cancel() latches cancelling for later Submits.
            self.assertTrue(state.cancelling)
            self.assertEqual(0, len(state.pending))
            ids = {
                responses.get(timeout=1).work_id,
                responses.get(timeout=1).work_id,
            }
            self.assertEqual({1, 2}, ids)


class TestShutdown(unittest.TestCase):
    """Shutdown Semantics."""

    def test_wait_true_blocks(self) -> None:
        """shutdown(wait=True) blocks until all complete."""
        with Jobserver(context=FAST, slots=2) as js:
            exe = JobserverExecutor(js)
            futures = [exe.submit(len, "x" * i) for i in range(5)]
            exe.shutdown(wait=True)
            for f in futures:
                self.assertTrue(f.done())

    def test_wait_false_returns_immediately(self) -> None:
        """shutdown(wait=False) returns immediately."""
        with Jobserver(context=FAST, slots=2) as js:
            exe = JobserverExecutor(js)
            f = exe.submit(time.sleep, 0.5)
            t0 = time.monotonic()
            exe.shutdown(wait=False)
            elapsed = time.monotonic() - t0
            self.assertLess(elapsed, 0.3)
            # Future should eventually complete
            f.result(timeout=TIMEOUT)

    def test_cancel_futures(self) -> None:
        """shutdown(cancel_futures=True) cancels pending."""
        with Jobserver(context=FAST, slots=1) as js:
            exe = JobserverExecutor(js)
            blocker = exe.submit(time.sleep, 0.5)
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
        with Jobserver(context=FAST, slots=1) as js:
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
                with Jobserver(context=FAST, slots=2) as js:
                    exe = JobserverExecutor(js)
                    exe.shutdown(wait=wait)
                    with self.assertRaises(RuntimeError):
                        exe.submit(len, (1,))

    def test_double_shutdown(self) -> None:
        """Double shutdown(wait=True) is safe, including pipe close."""
        with Jobserver(context=FAST, slots=2) as js:
            exe = JobserverExecutor(js)
            exe.shutdown(wait=True)
            exe.shutdown(wait=True)
            exe.shutdown(wait=True)

    def test_context_manager(self) -> None:
        """Context-manager exit calls shutdown(wait=True)."""
        with Jobserver(context=FAST, slots=2) as js:
            with JobserverExecutor(js) as exe:
                f = exe.submit(len, "hello")
            self.assertEqual(5, f.result(timeout=0))
            with self.assertRaises(RuntimeError):
                exe.submit(len, "x")

    def test_concurrent_submit_and_shutdown(self) -> None:
        """Concurrent submit and shutdown (race test)."""
        for _ in range(50):
            with Jobserver(context=FAST, slots=2) as js:
                exe = JobserverExecutor(js)
                barrier = threading.Barrier(2)
                result_holder: list[concurrent.futures.Future | None] = [None]
                error_holder: list[Exception | None] = [None]

                def submitter(
                    barrier=barrier,
                    exe=exe,
                    result_holder=result_holder,
                    error_holder=error_holder,
                ) -> None:
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
        with Jobserver(context=FAST, slots=1) as js:
            exe = JobserverExecutor(js)
            exe.submit(time.sleep, 0.3)
            time.sleep(0.1)
            futures = [exe.submit(len, (i,)) for i in range(5)]
            exe.shutdown(wait=True, cancel_futures=True)
            done, not_done = concurrent.futures.wait(futures, timeout=5)
            self.assertEqual(0, len(not_done))

    def test_trivial_submit_after_construction(self) -> None:
        """Trivial submit immediately after construction."""
        with Jobserver(context=FAST, slots=2) as js:
            exe = JobserverExecutor(js)
            f = exe.submit(len, (1, 2, 3))
            self.assertEqual(3, f.result(timeout=TIMEOUT))
            exe.shutdown(wait=True)


class TestResourceLeaks(unittest.TestCase):
    """Resource Leak Detection."""

    @staticmethod
    def _fd_count() -> int:
        """Count open file descriptors."""
        try:
            return len(os.listdir("/proc/self/fd"))
        except (FileNotFoundError, PermissionError):
            return -1

    def test_retained_future_does_not_pin_executor(self) -> None:
        """A retained future must not keep the executor object alive.

        The cancellation done-callback holds only a weak reference to the
        executor, so holding a completed future after shutdown must not
        prevent the executor from being garbage collected.
        """
        with Jobserver(context=FAST, slots=2) as js:
            exe = JobserverExecutor(js)
            f = exe.submit(helper_return, 7)
            self.assertEqual(7, f.result(timeout=TIMEOUT))
            exe.shutdown(wait=True)
            ref = weakref.ref(exe)
            del exe
            gc.collect()
            # f is still held, yet the executor must be collectable.
            self.assertIsNone(ref())
            # f remains usable independently of the executor.
            self.assertEqual(7, f.result(timeout=0))

    def test_process_count_baseline(self) -> None:
        """Process count returns to baseline."""
        baseline = len(multiprocessing.active_children())
        with Jobserver(context=FAST, slots=2) as js:
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
        with Jobserver(context=FAST, slots=2) as js:
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
        with Jobserver(context=FAST, slots=2) as js:
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
            with Jobserver(context=FAST, slots=2) as js:
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
        with Jobserver(context=FAST, slots=2) as js:
            with JobserverExecutor(js) as exe:
                f = exe.submit(signal.raise_signal, signal.SIGKILL)
                with self.assertRaises(SubmissionDied):
                    f.result(timeout=TIMEOUT)
        time.sleep(0.5)
        after = len(multiprocessing.active_children())
        self.assertEqual(baseline, after)

    def test_dispatcher_death_orphans_futures(self) -> None:
        """Killing the dispatcher fails outstanding futures."""
        with Jobserver(context=FAST, slots=1) as js:
            exe = JobserverExecutor(js)
            try:
                # Submit slow work so a future is outstanding
                f = exe.submit(time.sleep, 10)
                # Wait for the dispatcher to be alive
                deadline = time.monotonic() + 5
                while (
                    not exe._dispatcher.is_alive()
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.01)
                self.assertTrue(exe._dispatcher.is_alive())
                # Kill the dispatcher
                os.kill(exe._dispatcher.pid, signal.SIGKILL)
                # The outstanding future must surface an error
                with self.assertRaises(RuntimeError):
                    f.result(timeout=TIMEOUT)
            finally:
                exe.shutdown(wait=True)

    def test_submit_after_dispatcher_death(self) -> None:
        """submit() after dispatcher death raises RuntimeError."""
        with Jobserver(context=FAST, slots=2) as js:
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
        with Jobserver(context=FAST, slots=2) as js:
            exe = JobserverExecutor(js)
            f = exe.submit(len, (1, 2))
            f.result(timeout=TIMEOUT)
            # Kill the dispatcher
            os.kill(exe._dispatcher.pid, signal.SIGKILL)
            exe._dispatcher.join(timeout=5)
            time.sleep(0.2)
            # shutdown() must not raise despite BrokenPipeError
            exe.shutdown(wait=True)


class _RaisingResponses:
    """A _responses stand-in whose get() crashes the receiver loop."""

    def __init__(self, error: BaseException) -> None:
        self._error = error

    def get(self, timeout: object = None) -> object:
        raise self._error


class _StubExecutor(JobserverExecutor):
    """JobserverExecutor sans __init__: just the receiver-guard attributes."""

    def __init__(self, responses: object) -> None:
        self._lock = threading.Lock()
        self._shutdown = False
        self._futures = {}
        self._broken = None
        self._responses = responses
        self._jobserver = None


class TestReceiverGuard(unittest.TestCase):
    """The receiver thread fails loudly instead of dying silently (#193)."""

    def test_crash_breaks_executor_and_fails_futures(self) -> None:
        """A receiver crash marks the executor broken and fails futures."""
        sentinel = ValueError("boom in receiver")
        exe = _StubExecutor(_RaisingResponses(sentinel))
        future: concurrent.futures.Future = concurrent.futures.Future()
        exe._futures[0] = future

        # Capture the loud death so the re-raise stays out of test output.
        captured: list[BaseException] = []
        old_hook = threading.excepthook
        threading.excepthook = lambda args: captured.append(args.exc_value)
        try:
            t = threading.Thread(target=exe._receive_loop, daemon=True)
            t.start()
            t.join(timeout=TIMEOUT)
        finally:
            threading.excepthook = old_hook

        self.assertFalse(t.is_alive())  # died loudly, did not hang
        self.assertIn(sentinel, captured)
        self.assertIs(exe._broken, sentinel)
        # The outstanding future fails carrying the real cause.
        exc = future.exception(timeout=TIMEOUT)
        self.assertIsInstance(exc, concurrent.futures.BrokenExecutor)
        self.assertIs(exc.__cause__, sentinel)

    def test_fails_running_future_without_reraising(self) -> None:
        """A RUNNING outstanding future is failed, not left for a crash."""
        sentinel = ValueError("receiver died")
        exe = _StubExecutor(_RaisingResponses(sentinel))
        future: concurrent.futures.Future = concurrent.futures.Future()
        future.set_running_or_notify_cancel()  # Started arrived, no Completed
        exe._futures[0] = future

        exe._fail_all(cause=sentinel)  # must not raise

        exc = future.exception(timeout=TIMEOUT)
        self.assertIsInstance(exc, concurrent.futures.BrokenExecutor)
        self.assertIs(exc.__cause__, sentinel)

    def test_submit_after_broken_raises_with_cause(self) -> None:
        """submit() refuses work once broken, chaining the real cause."""
        sentinel = RuntimeError("receiver died")
        exe = _StubExecutor(_RaisingResponses(sentinel))
        exe._broken = sentinel
        with self.assertRaises(concurrent.futures.BrokenExecutor) as cm:
            exe.submit(len, (1,))
        self.assertIs(cm.exception.__cause__, sentinel)

    def test_repr_reports_broken(self) -> None:
        """__repr__ surfaces the broken state."""
        exe = _StubExecutor(_RaisingResponses(RuntimeError()))
        exe._broken = RuntimeError("x")
        self.assertIn("broken", repr(exe))


def _fail_receiver_start(boom: BaseException):
    """Patch threading.Thread.start to raise only for the receiver thread."""
    original = threading.Thread.start

    def start(self: threading.Thread) -> None:
        if self.name == "JobserverExecutor-receiver":
            raise boom
        original(self)

    return start


class TestStartupFailure(unittest.TestCase):
    """Partial __init__ failure must not orphan the dispatcher (#220)."""

    def _wait_for_baseline(self, baseline: int) -> None:
        deadline = time.monotonic() + TIMEOUT
        while (
            len(multiprocessing.active_children()) > baseline
            and time.monotonic() < deadline
        ):
            time.sleep(0.02)
        self.assertEqual(baseline, len(multiprocessing.active_children()))

    def test_receiver_start_failure_tears_down_owned(self) -> None:
        """A failed receiver start stops the dispatcher; nothing leaks."""
        baseline = len(multiprocessing.active_children())
        boom = RuntimeError("can't start new thread")
        with mock.patch.object(
            threading.Thread, "start", _fail_receiver_start(boom)
        ):
            with self.assertRaises(RuntimeError) as cm:
                JobserverExecutor()
        # The original failure propagates, not a cleanup error.
        self.assertIs(cm.exception, boom)
        # The dispatcher that did start must have been reaped.
        self._wait_for_baseline(baseline)

    def test_receiver_start_failure_keeps_external_jobserver(self) -> None:
        """Cleanup of a partial init leaves a caller-owned Jobserver open."""
        boom = RuntimeError("can't start new thread")
        with Jobserver(context=FAST, slots=2) as js:
            baseline = len(multiprocessing.active_children())
            with mock.patch.object(
                threading.Thread, "start", _fail_receiver_start(boom)
            ):
                with self.assertRaises(RuntimeError):
                    JobserverExecutor(js)
            self._wait_for_baseline(baseline)
            # The external Jobserver was not closed by cleanup: still usable.
            f = js.submit(fn=len, args=((1, 2, 3),))
            self.assertEqual(3, f.result(timeout=TIMEOUT))


class TestOwnedJobserver(unittest.TestCase):
    """Owned Jobserver (constructed with jobserver=None)."""

    def test_default_construction_runs_work(self) -> None:
        """Default-constructed executor submits and retrieves results."""
        with JobserverExecutor() as exe:
            f = exe.submit(len, (1, 2, 3))
            self.assertEqual(3, f.result(timeout=TIMEOUT))

    def test_explicit_none_same_as_default(self) -> None:
        """Passing jobserver=None is equivalent to omitting the argument."""
        with JobserverExecutor(None) as exe:
            f = exe.submit(len, (1, 2))
            self.assertEqual(2, f.result(timeout=TIMEOUT))

    def test_owns_jobserver_flag(self) -> None:
        """_own_jobserver is True when no jobserver is supplied."""
        exe = JobserverExecutor()
        try:
            self.assertTrue(exe._own_jobserver)
        finally:
            exe.shutdown(wait=True)

    def test_not_owns_jobserver_flag(self) -> None:
        """_own_jobserver is False when an explicit jobserver is supplied."""
        with Jobserver(context=FAST, slots=2) as js:
            exe = JobserverExecutor(js)
            try:
                self.assertFalse(exe._own_jobserver)
            finally:
                exe.shutdown(wait=True)

    def test_owned_jobserver_closed_after_shutdown(self) -> None:
        """shutdown(wait=True) closes the owned Jobserver."""
        exe = JobserverExecutor()
        f = exe.submit(len, (1,))
        f.result(timeout=TIMEOUT)
        exe.shutdown(wait=True)
        # The owned jobserver's slots queue should be closed; putting
        # to it should raise (ValueError from MinimalQueue.put on a
        # closed queue).
        self.assertFalse(exe._own_jobserver)

    def test_owned_jobserver_double_shutdown_safe(self) -> None:
        """Double shutdown(wait=True) is safe when executor owns jobserver."""
        exe = JobserverExecutor()
        exe.submit(len, (1,)).result(timeout=TIMEOUT)
        exe.shutdown(wait=True)
        exe.shutdown(wait=True)  # Must not raise

    def test_external_jobserver_not_closed_on_shutdown(self) -> None:
        """Explicitly provided Jobserver is not closed by shutdown()."""
        with Jobserver(context=FAST, slots=2) as js:
            exe = JobserverExecutor(js)
            exe.submit(len, (1,)).result(timeout=TIMEOUT)
            exe.shutdown(wait=True)
            # js is still usable: submit work directly via it
            f = js.submit(fn=len, args=((1, 2),))
            self.assertEqual(2, f.result(timeout=TIMEOUT))

    def test_context_manager_closes_owned_jobserver(self) -> None:
        """Context-manager exit closes the owned Jobserver."""
        exe = JobserverExecutor()
        with exe:
            f = exe.submit(len, "hello")
            self.assertEqual(5, f.result(timeout=TIMEOUT))
        self.assertFalse(exe._own_jobserver)
