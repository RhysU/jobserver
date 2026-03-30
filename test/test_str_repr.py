# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Tests for __repr__ across all public symbols."""

import unittest

from jobserver import (
    Jobserver,
    JobserverExecutor,
    MinimalQueue,
)

from .helpers import helper_return


class TestFutureRepr(unittest.TestCase):
    """Future __repr__ reports state."""

    def test_pending_future(self) -> None:
        with Jobserver(slots=1) as js:
            f = js.submit(fn=helper_return, args=(42,))
            r = repr(f)
            self.assertIn("running", r)
            self.assertIn("pid=", r)
            self.assertEqual(str(f), repr(f))
            f.result()

    def test_done_future(self) -> None:
        with Jobserver(slots=1) as js:
            f = js.submit(fn=helper_return, args=(42,))
            f.result()
            r = repr(f)
            self.assertIn("done", r)
            self.assertIn("pid=None", r)
            self.assertEqual(str(f), repr(f))


class TestJobserverRepr(unittest.TestCase):
    """Jobserver __repr__ reports state."""

    def test_idle(self) -> None:
        with Jobserver(slots=2) as js:
            r = repr(js)
            self.assertTrue(r.startswith("Jobserver('"))
            self.assertIn("tracked=0", r)
            self.assertEqual(str(js), repr(js))

    def test_with_tracked(self) -> None:
        with Jobserver(slots=2) as js:
            f = js.submit(fn=helper_return, args=(1,))
            self.assertIn("tracked=", repr(js))
            f.result()


class TestJobserverExecutorRepr(unittest.TestCase):
    """JobserverExecutor __repr__ reports state."""

    def test_active(self) -> None:
        with Jobserver(slots=1) as js:
            ex = JobserverExecutor(js)
            r = repr(ex)
            self.assertIn("active", r)
            self.assertIn("pending=", r)
            self.assertIn("jobserver=", r)
            self.assertIn("Jobserver(", r)
            self.assertEqual(str(ex), repr(ex))
            ex.shutdown(wait=True)

    def test_shutdown(self) -> None:
        with Jobserver(slots=1) as js:
            ex = JobserverExecutor(js)
            ex.shutdown(wait=True)
            r = repr(ex)
            self.assertIn("shutdown", r)
            self.assertIn("jobserver=", r)
            self.assertEqual(str(ex), repr(ex))


class TestMinimalQueueRepr(unittest.TestCase):
    """MinimalQueue __repr__ reports pipe state."""

    def test_open(self) -> None:
        with MinimalQueue() as mq:
            r = repr(mq)
            self.assertIn("reader=open", r)
            self.assertIn("writer=open", r)
            self.assertIn("fd=", r)
            self.assertEqual(str(mq), repr(mq))

    def test_closed_reader(self) -> None:
        with MinimalQueue() as mq:
            mq.close_get()
            self.assertIn("reader=closed", repr(mq))
            self.assertIn("writer=open", repr(mq))
            self.assertEqual(str(mq), repr(mq))

    def test_closed_writer(self) -> None:
        with MinimalQueue() as mq:
            mq.close_put()
            self.assertIn("reader=open", repr(mq))
            self.assertIn("writer=closed", repr(mq))
            self.assertEqual(str(mq), repr(mq))
