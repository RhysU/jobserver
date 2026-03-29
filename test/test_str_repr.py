# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Tests for __repr__ across all public symbols."""

import unittest

from jobserver import (
    Blocked,
    CallbackRaised,
    Future,
    Jobserver,
    JobserverExecutor,
    MinimalQueue,
    SubmissionDied,
)

from .helpers import helper_return


class TestExceptionRepr(unittest.TestCase):
    """Exception classes have sensible default messages."""

    def test_blocked_default(self) -> None:
        e = Blocked()
        self.assertIn("blocked", str(e))
        self.assertIn(str(e), repr(e))

    def test_blocked_custom(self) -> None:
        e = Blocked("custom")
        self.assertEqual(str(e), "custom")
        self.assertIn("custom", repr(e))

    def test_callback_raised_default(self) -> None:
        e = CallbackRaised()
        self.assertIn("callback", str(e))
        self.assertIn(str(e), repr(e))

    def test_submission_died_default(self) -> None:
        e = SubmissionDied()
        self.assertIn("died", str(e))
        self.assertIn(str(e), repr(e))


class TestFutureRepr(unittest.TestCase):
    """Future __repr__ reports state."""

    def test_pending_future(self) -> None:
        js = Jobserver(slots=1)
        f = js.submit(fn=helper_return, args=(42,))
        r = repr(f)
        self.assertIn("running", r)
        self.assertIn("pid=", r)
        self.assertEqual(str(f), repr(f))
        f.result()

    def test_done_future(self) -> None:
        js = Jobserver(slots=1)
        f = js.submit(fn=helper_return, args=(42,))
        f.result()
        r = repr(f)
        self.assertIn("done", r)
        self.assertNotIn("pid=", r)
        self.assertEqual(str(f), repr(f))


class TestJobserverRepr(unittest.TestCase):
    """Jobserver __repr__ reports state."""

    def test_idle(self) -> None:
        js = Jobserver(slots=2)
        r = repr(js)
        self.assertTrue(r.startswith("Jobserver('"))
        self.assertIn("tracked=0", r)
        self.assertEqual(str(js), repr(js))

    def test_with_tracked(self) -> None:
        js = Jobserver(slots=2)
        f = js.submit(fn=helper_return, args=(1,))
        self.assertIn("tracked=", repr(js))
        f.result()


class TestJobserverExecutorRepr(unittest.TestCase):
    """JobserverExecutor __repr__ reports state."""

    def test_active(self) -> None:
        js = Jobserver(slots=1)
        ex = JobserverExecutor(js)
        r = repr(ex)
        self.assertIn("active", r)
        self.assertIn("pending=", r)
        self.assertIn("jobserver=", r)
        self.assertIn("Jobserver(", r)
        self.assertEqual(str(ex), repr(ex))
        ex.shutdown(wait=True)

    def test_shutdown(self) -> None:
        js = Jobserver(slots=1)
        ex = JobserverExecutor(js)
        ex.shutdown(wait=True)
        r = repr(ex)
        self.assertIn("shutdown", r)
        self.assertIn("jobserver=", r)
        self.assertEqual(str(ex), repr(ex))


class TestMinimalQueueRepr(unittest.TestCase):
    """MinimalQueue __repr__ reports pipe state."""

    def test_open(self) -> None:
        mq: MinimalQueue[int] = MinimalQueue()
        r = repr(mq)
        self.assertIn("reader=open", r)
        self.assertIn("writer=open", r)
        self.assertIn("fd=", r)
        self.assertEqual(str(mq), repr(mq))

    def test_closed_reader(self) -> None:
        mq: MinimalQueue[int] = MinimalQueue()
        mq.close_get()
        self.assertIn("reader=closed", repr(mq))
        self.assertIn("writer=open", repr(mq))

    def test_closed_writer(self) -> None:
        mq: MinimalQueue[int] = MinimalQueue()
        mq.close_put()
        self.assertIn("reader=open", repr(mq))
        self.assertIn("writer=closed", repr(mq))
