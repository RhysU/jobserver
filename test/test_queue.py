# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""MinimalQueue and low-level utility tests.

MinimalQueue receives heavy indirect coverage through the Jobserver and
JobserverExecutor suites, so this file only covers its own API surface
and the resolve_context / absolute_deadline helpers.
"""

import copy
import time
import unittest
from multiprocessing import get_all_start_methods, get_context
from multiprocessing.context import BaseContext

from jobserver import MinimalQueue, absolute_deadline, resolve_context


class MinimalQueueTest(unittest.TestCase):
    """Unit tests for MinimalQueue."""

    def test_duplication_minimalqueue(self) -> None:
        """Copying of MinimalQueue is explicitly allowed."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                with MinimalQueue(context=method) as mq1:
                    mq2 = copy.copy(mq1)
                    mq3 = copy.deepcopy(mq1)
                    mq1.put(1)
                    mq2.put(2)
                    mq3.put(3)
                    self.assertEqual(1, mq3.get())
                    self.assertEqual(2, mq2.get())
                    self.assertEqual(3, mq1.get())
                    # Copying is allowed but degenerate: copy.copy(...)
                    # and copy.deepcopy(...) return the original.
                    self.assertIs(mq1, mq2)
                    self.assertIs(mq1, mq3)

    def test_close_get_and_close_put_are_idempotent(self) -> None:
        """close_get() and close_put() are safe to call more than once."""
        with MinimalQueue() as mq:
            pass
        # Both ends already closed by __exit__; repeat must not raise
        mq.close_get()
        mq.close_put()

    def test_context_manager(self) -> None:
        """Context manager closes both ends; put/get raise after exit."""
        with MinimalQueue() as mq:
            mq.put(42)
            self.assertEqual(42, mq.get(timeout=1))
        with self.assertRaises(ValueError):
            mq.put(99)
        with self.assertRaises(ValueError):
            mq.get(timeout=0)


class ResolveContextTest(unittest.TestCase):
    """Unit tests for resolve_context."""

    def test_none_returns_default_context(self) -> None:
        """None resolves to the default multiprocessing context."""
        ctx = resolve_context(None)
        self.assertIsInstance(ctx, BaseContext)

    def test_string_returns_named_context(self) -> None:
        """A start-method string resolves to the named context."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                ctx = resolve_context(method)
                self.assertIsInstance(ctx, BaseContext)

    def test_context_passes_through(self) -> None:
        """An existing BaseContext is returned unchanged."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                original = get_context(method)
                self.assertIs(resolve_context(original), original)


class AbsoluteDeadlineTest(unittest.TestCase):
    """Unit tests for absolute_deadline."""

    def test_none_yields_large_deadline(self) -> None:
        """None timeout produces a deadline far in the future."""
        before = time.monotonic()
        deadline = absolute_deadline(None)
        self.assertGreater(deadline, before + 86400)

    def test_zero_yields_near_now(self) -> None:
        """Zero timeout produces a deadline near the current time."""
        before = time.monotonic()
        deadline = absolute_deadline(0)
        after = time.monotonic()
        self.assertGreaterEqual(deadline, before)
        self.assertLessEqual(deadline, after + 0.01)

    def test_positive_offset(self) -> None:
        """A positive timeout offsets from the current monotonic time."""
        before = time.monotonic()
        deadline = absolute_deadline(5.0)
        after = time.monotonic()
        self.assertGreaterEqual(deadline, before + 5.0)
        self.assertLessEqual(deadline, after + 5.0 + 0.01)
