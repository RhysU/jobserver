# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Recursive submission deadlocks without consume=0 (GitHub issue #17).

GNU Make's jobserver protocol gives every recursive invocation an
implicit "free" token -- one job can always run without acquiring from
the pipe.  In this library, submit(..., consume=0) is the equivalent.

The scenario
~~~~~~~~~~~~

A Jobserver(slots=2) starts with tokens {0, 1} in its pipe:

    Parent --> P1 --> P2 --> ...
      |          |       |
      | token 0  | token 1  needs a token
      | waits P1 | waits P2  but none exist

P2 cannot start.  P1 cannot finish.  The parent cannot finish.
With consume=0, P2 runs without acquiring a token and the recursion
bottoms out normally.

"""

import signal
import time
import unittest
from multiprocessing import get_all_start_methods

from jobserver import Blocked, Jobserver


def _recurse(js: Jobserver, depth: int, consume: int = 1) -> int:
    """Recursively submit to depth, returning the depth reached."""
    if depth <= 0:
        return 0
    future = js.submit(
        fn=_recurse,
        args=(js, depth - 1, consume),
        consume=consume,
        timeout=0,
    )
    return 1 + future.result(timeout=5)


class TestDesignConsume(unittest.TestCase):
    """Recursive fork-join requires consume=0 to avoid deadlock."""

    SLOTS = 2

    def test_consume_1_deadlocks_beyond_slot_count(self) -> None:
        """consume=1 (default) deadlocks when recursion depth > slots."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=self.SLOTS) as js:
                    # depth=1 succeeds: parent + child = 2 tokens = 2 slots
                    f = js.submit(fn=_recurse, args=(js, 1), timeout=5)
                    self.assertEqual(f.result(timeout=10), 1)

                    # depth=2 deadlocks: 3 tokens needed, only 2 exist
                    f = js.submit(fn=_recurse, args=(js, 2), timeout=5)
                    with self.assertRaises(Blocked):
                        f.result(timeout=10)

    def test_consume_0_permits_arbitrary_depth(self) -> None:
        """consume=0 is the implicit free token, avoiding deadlock."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=self.SLOTS) as js:
                    f = js.submit(fn=_recurse, args=(js, 10, 0), timeout=5)
                    self.assertEqual(f.result(timeout=30), 10)

    def test_top_level_backpressure_preserved(self) -> None:
        """consume=0 is for interiors; the top level still obeys slots."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=self.SLOTS) as js:
                    held = []
                    for _ in range(self.SLOTS):
                        held.append(
                            js.submit(fn=time.sleep, args=(10.0,), timeout=5)
                        )

                    with self.assertRaises(Blocked):
                        js.submit(fn=time.sleep, args=(0,), timeout=0)

                    for f in held:
                        f.wait(signal=signal.SIGKILL)
