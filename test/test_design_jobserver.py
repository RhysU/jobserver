# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Killing a nested intermediate process leaks tokens.

Every pipe-based jobserver shares this shortcoming: GNU Make, Rust's
jobserver crate (Cargo/rustc), and Ninja among them.  A pipe provides
no mechanism for a dead process to return tokens it consumed.

The scenario
~~~~~~~~~~~~

A Jobserver(slots=3) starts with tokens {0, 1, 2} in its pipe:

    Parent --submit--> Child --submit--> Grandchild
      |                  |                   |
      |  owns Child      |  owns Grandchild  |  does work
      |  future          |  future           |
      |  holds token 0   |  holds token 1    |

When the Child is killed:

  * The Parent detects Child death via EOFError on the result pipe,
    wraps it as SubmissionDied, and fires the Child future's internal
    callback restoring token 0.

  * But the Grandchild's token-restoring callback existed only in the
    Child's address space.  The Child is dead; the callback is gone.
    No surviving process knows token 1 must be returned.  Token 1 is
    permanently leaked.

"""

import signal
import time
import unittest
from multiprocessing import get_all_start_methods

from jobserver import Blocked, Jobserver, SubmissionDied


def _child_work(js: Jobserver, grandchild_duration: float) -> str:
    """Submit a grandchild, then sleep long enough to be killed."""
    js.submit(fn=_grandchild_work, args=(grandchild_duration,), timeout=5)
    time.sleep(60)  # Killed before this returns
    return "unreachable"


def _grandchild_work(duration: float) -> str:
    """Do brief work and return."""
    time.sleep(duration)
    return "done"


class TestDesignShortcoming(unittest.TestCase):
    """Killing a nested intermediate process leaks exactly 1 token."""

    SLOTS = 3

    def test_intermediate_death_leaks_one_token(self) -> None:
        """Parent -> Child -> Grandchild; kill Child; 1 token is leaked."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=self.SLOTS) as js:
                    # All slots are available: submit and immediately collect
                    for _ in range(self.SLOTS):
                        js.submit(fn=time.sleep, args=(0,), timeout=0).result(
                            timeout=5
                        )

                    # Parent -> Child -> Grandchild
                    future = js.submit(
                        fn=_child_work, args=(js, 0.5), timeout=5
                    )
                    time.sleep(0.5)  # Let Child start and submit Grandchild

                    # Kill the intermediate Child and collect it
                    future.wait(signal=signal.SIGKILL, timeout=5)
                    with self.assertRaises(SubmissionDied):
                        future.result()

                    # Grandchild finishes but nobody reclaims its token
                    time.sleep(1.0)

                    # One fewer slot is usable now: the last is leaked.
                    # Hold workers alive so their tokens stay consumed.
                    futures = []
                    for i in range(self.SLOTS):
                        try:
                            futures.append(
                                js.submit(
                                    fn=time.sleep, args=(10.0,), timeout=0
                                )
                            )
                        except Blocked:
                            self.assertEqual(
                                i, self.SLOTS - 1, "leak appeared too early"
                            )
                            break
                    else:
                        self.fail(
                            "expected Blocked on the 3rd submit,"
                            " but all succeeded"
                        )

                    # Clean up the submissions that did succeed
                    for f in futures:
                        f.wait(signal=signal.SIGKILL)
                        with self.assertRaises(SubmissionDied):
                            f.result()
