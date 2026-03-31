# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Recursive submission deadlocks without consume=0 (GitHub issue #17).

Every bounded worker pool shares this limitation: when a task submits a
subtask to the same pool and blocks on the result, the parent holds a
slot while doing no work.  If every slot is held by a blocked ancestor,
no descendant can run.  This is "thread-starvation deadlock" (SEI CERT
TPS01-J) and affects Java thread pools, .NET ThreadPool, and any other
bounded pool that permits nested blocking submission.

GNU Make's jobserver protocol avoids this by giving every recursive make
invocation an implicit "free" token -- one job can always run without
acquiring a token from the pipe.  In this library, the equivalent is
submit(..., consume=0): the child runs using the slot its parent already
holds, rather than acquiring a new one.

The scenario
~~~~~~~~~~~~

A Jobserver(slots=2) starts with tokens {0, 1} in its pipe.  A
recursive function submits itself to the same Jobserver and blocks on
the result:

    Parent --submit(consume=1)--> P1 --submit(consume=1)--> P2 ---> ...
      |                            |                          |
      |  holds token 0             |  holds token 1           |  needs a token
      |  waits for P1              |  waits for P2            |  but none exist

P2 cannot start because both tokens are held by ancestors that are
blocked waiting for their children.  P1 cannot finish because P2 cannot
run.  The parent cannot finish because P1 cannot finish.

With consume=0, P2 runs without acquiring a token.  The recursion
bottoms out, results propagate back, and tokens are released.

"""

import signal
import time
import unittest

from jobserver import Blocked, Jobserver


def _recursive_sum(js: Jobserver, head: int, *rest: int) -> int:
    """Recursively submit additions, blocking on each child's result."""
    if not rest:
        return head
    future = js.submit(
        fn=_recursive_sum, args=(js, *rest), consume=0, timeout=5
    )
    return head + future.result(timeout=30)


class TestDesignConsume(unittest.TestCase):
    """Recursive fork-join requires consume=0 to avoid deadlock."""

    SLOTS = 2

    def test_consume_1_deadlocks(self) -> None:
        """Default consume=1 deadlocks when recursion depth > slots."""
        with Jobserver(context="fork", slots=self.SLOTS) as js:

            def _recursive_consume_1(js_: Jobserver, depth: int) -> int:
                if depth <= 0:
                    return 0
                future = js_.submit(
                    fn=_recursive_consume_1,
                    args=(js_, depth - 1),
                    # consume=1 is the default
                    timeout=0,
                )
                return 1 + future.result(timeout=5)

            # Recursion to depth 1 succeeds (uses 2 of 2 tokens)
            f = js.submit(
                fn=_recursive_consume_1, args=(js, 1), timeout=5
            )
            self.assertEqual(f.result(timeout=10), 1)

            # Recursion to depth 2 fails: the 3rd level can't get a token
            f = js.submit(
                fn=_recursive_consume_1, args=(js, 2), timeout=5
            )
            with self.assertRaises(Blocked):
                f.result(timeout=10)

    def test_consume_0_permits_arbitrary_depth(self) -> None:
        """consume=0 acts as the implicit free token, avoiding deadlock."""
        with Jobserver(context="fork", slots=self.SLOTS) as js:
            future = js.submit(
                fn=_recursive_sum,
                args=(js, 1, 2, 3, 4),
                timeout=5,
            )
            self.assertEqual(future.result(timeout=30), 10)

    def test_consume_0_preserves_backpressure_at_top_level(self) -> None:
        """consume=0 is for recursive interiors; the top level still obeys slots."""
        with Jobserver(context="fork", slots=self.SLOTS) as js:
            # Fill all slots with long-running work
            held = []
            for _ in range(self.SLOTS):
                held.append(
                    js.submit(fn=time.sleep, args=(10.0,), timeout=5)
                )

            # A new consume=1 submission is blocked -- slots are full
            with self.assertRaises(Blocked):
                js.submit(fn=time.sleep, args=(0,), timeout=0)

            # Clean up
            for f in held:
                f.wait(signal=signal.SIGKILL)
                try:
                    f.result()
                except Exception:
                    pass
