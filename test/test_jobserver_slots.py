# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""slots is bounded so construction cannot hang (issue #302).

All tokens are written in one atomic FixedBytesQueue.put(), which is
capped at pipe_buf() bytes; one single-byte token per slot caps slots
there too.  Construction therefore rejects slots beyond pipe_buf() with
an informative ValueError, and the largest allowed value still fits.
"""

import threading
import time
import unittest

from jobserver import Jobserver
from jobserver._compat import pipe_buf

from .helpers import FAST, TIMEOUT


class TestLargeSlots(unittest.TestCase):
    """slots above pipe_buf() is rejected; the boundary still works."""

    def test_slots_above_max_raises_informative_valueerror(self) -> None:
        """Jobserver(slots=10**9) fails fast rather than hanging."""
        with self.assertRaises(ValueError) as cm:
            Jobserver(context=FAST, slots=10**9)
        message = str(cm.exception)
        self.assertIn(str(pipe_buf()), message)
        self.assertIn(repr(10**9), message)

    def test_max_slots_constructs_quickly_and_runs(self) -> None:
        """The largest allowed slots fills the pipe without deadlocking."""
        done = threading.Event()

        def build() -> None:
            # Construct and immediately tear down; both must be fast.
            with Jobserver(context=FAST, slots=pipe_buf()):
                pass
            done.set()

        # A daemon thread so a regression (a blocked pipe put()) cannot
        # wedge the whole suite; it is reaped at interpreter exit.
        thread = threading.Thread(target=build, daemon=True)
        start = time.monotonic()
        thread.start()
        thread.join(timeout=TIMEOUT)
        self.assertTrue(
            done.is_set(),
            "construction hung: still running after"
            f" {time.monotonic() - start:.1f}s",
        )

    def test_max_slots_still_runs_work(self) -> None:
        """A max-slots Jobserver still accepts and completes work."""
        with Jobserver(context=FAST, slots=pipe_buf()) as js:
            future = js.submit(fn=abs, args=(-7,))
            self.assertEqual(future.result(timeout=TIMEOUT), 7)

    def test_slots_rejects_bool(self) -> None:
        """slots=True/False is a TypeError, not a silent slots=1 (#336)."""
        for bad in (True, False):
            with self.assertRaises(TypeError) as cm:
                Jobserver(context=FAST, slots=bad)
            self.assertIn("bool", str(cm.exception))

    def test_slots_rejects_non_int(self) -> None:
        """slots must be an int; str/float are TypeErrors (#336)."""
        for bad in ("2", 2.0):
            with self.assertRaises(TypeError):
                Jobserver(context=FAST, slots=bad)


if __name__ == "__main__":
    unittest.main()
