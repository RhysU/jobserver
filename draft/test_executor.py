# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Tests for JobserverExecutor -- a concurrent.futures.Executor adapter."""
import sys
import typing
import unittest
import unittest.mock

from draft._request import Submit
from draft.executor import JobserverExecutor
from jobserver.impl import Jobserver, MinimalQueue

# Most tests use the fastest start method and the Executor logic is
# independent of the multiprocessing context.  On Python 3.12+ "fork" is
# deprecated when the process is multi-threaded (as it is here because
# JobserverExecutor has a dispatcher thread), so fall back to "forkserver".
_FAST = "forkserver" if sys.version_info >= (3, 12) else "fork"


class JobserverExecutorTest(unittest.TestCase):
    """Tests verifying internal implementation details via mocking."""

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
            exe.submit(len, (1, 2, 3)).result(timeout=10)
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
