# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Design-level invariants and shortcomings of the jobserver protocol.

Unlike the API tests, each TestCase here pins down a property of the
pipe-based jobserver design itself.  The three scenarios are documented
in detail above their respective classes below.
"""

import os
import signal
import time
import unittest

from jobserver import Blocked, Jobserver, LostResult

from .helpers import start_methods

# ---------------------------------------------------------------------------
# Recursive submission deadlocks without consume=0 (GitHub issue #17).
#
# GNU Make's jobserver protocol gives every recursive invocation an
# implicit "free" token -- one job can always run without acquiring from
# the pipe.  In this library, submit(..., consume=0) is the equivalent.
#
# The scenario
# ~~~~~~~~~~~~
#
# A Jobserver(slots=2) starts with tokens {0, 1} in its pipe:
#
#     Parent --> P1 --> P2 --> ...
#       |          |       |
#       | token 0  | token 1  needs a token
#       | waits P1 | waits P2  but none exist
#
# P2 cannot start.  P1 cannot finish.  The parent cannot finish.
# With consume=0, P2 runs without acquiring a token and the recursion
# bottoms out normally.
# ---------------------------------------------------------------------------


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
        for method in start_methods():
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
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=self.SLOTS) as js:
                    # Depth far exceeds the 2 slots, exercising the
                    # implicit free token at every interior level.
                    f = js.submit(fn=_recurse, args=(js, 6, 0), timeout=5)
                    self.assertEqual(f.result(timeout=30), 6)

    def test_consume_zero_succeeds_when_slots_exhausted(self) -> None:
        """consume=0 submit succeeds even with all slots held."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=self.SLOTS) as js:
                    held = []
                    for _ in range(self.SLOTS):
                        held.append(
                            js.submit(fn=time.sleep, args=(10.0,), timeout=5)
                        )
                    f = js.submit(fn=len, args=((1, 2),), consume=0, timeout=0)
                    self.assertEqual(2, f.result(timeout=10))
                    for h in held:
                        h.wait(signal=signal.SIGKILL)

    def test_alternating_consume_zero_and_one(self) -> None:
        """Alternating consume=0 and consume=1 submits all complete."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=self.SLOTS) as js:
                    futures = []
                    for i in range(6):
                        consume = 0 if i % 2 == 0 else 1
                        futures.append(
                            js.submit(
                                fn=len,
                                args=((1,) * (i + 1),),
                                consume=consume,
                                timeout=10,
                            )
                        )
                    results = [f.result(timeout=10) for f in futures]
                    self.assertEqual(results, [1, 2, 3, 4, 5, 6])

    def test_top_level_backpressure_preserved(self) -> None:
        """consume=0 is for interiors; the top level still obeys slots."""
        for method in start_methods():
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


# ---------------------------------------------------------------------------
# Forked children must not inherit the parent's selector (issue #296).
#
# Under spawn/forkserver a Jobserver is pickled into each child, so
# __setstate__ rebuilds a fresh per-process selector.  Under fork no
# pickling occurs: a worker inherits the parent's Jobserver as a memory
# copy, so without intervention it keeps the parent's selector, including
# sibling Futures whose Process objects belong to an ancestor.
#
# A worker performing nested fan-out (submitting a second child while a
# first sibling is still registered) then calls reclaim_resources() on
# entry to submit().  If an inherited sibling's sentinel fd is ready, the
# worker would join()/is_alive() a process it never created, raising
# "can only join a child process".  The Jobserver instead rebuilds a
# clean selector when it first observes a pid change.
# ---------------------------------------------------------------------------


def _sibling() -> int:
    """Still running when the nested child forks, then exits."""
    time.sleep(0.1)
    return 0


def _child_reclaims(js: Jobserver) -> int:
    """Reclaim after the inherited sibling has exited (fd ready).

    Under fork the inherited selector still tracks _sibling's Future;
    by the time this sleep elapses that process has exited, so its
    sentinel fd is ready.  reclaim_resources() must rebuild a clean
    selector rather than act on the ancestor-owned process.  The sleep
    only needs to outlast the 0.1s sibling with a margin to spare.
    """
    time.sleep(0.3)
    js.reclaim_resources()
    return 0


def _parent_fanout(js: Jobserver) -> int:
    """Submit a sibling, then a nested child that reclaims while it runs."""
    sibling = js.submit(fn=_sibling)
    nested = js.submit(fn=_child_reclaims, args=(js,), consume=0)
    value = nested.result(timeout=30)
    sibling.result(timeout=30)
    return value


class TestDesignForkSelector(unittest.TestCase):
    """Nested fan-out under fork must not reclaim an ancestor's Future."""

    def test_nested_fanout_rebuilds_inherited_selector(self) -> None:
        """A forked worker reclaims cleanly despite inherited siblings."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=8) as js:
                    f = js.submit(fn=_parent_fanout, args=(js,), consume=0)
                    self.assertEqual(f.result(timeout=60), 0)

    @unittest.skipUnless(hasattr(os, "fork"), "requires os.fork")
    def test_fork_of_closed_parent_raises_closed(self) -> None:
        """A child forked from a closed Jobserver reports it as closed.

        The child inherits the parent's _selector_closed flag, so its
        _lazy_selector raises a clean RuntimeError before the fork rebuild
        reaches self._slots.waitable() on the inherited closed slots queue.
        Selector closure cannot be probed after the fact: a closed selector's
        get_map() can spuriously report open in a forked child under GC.
        """
        js = Jobserver(context="fork", slots=2)
        with js:
            js.submit(fn=len, args=("warm",)).result(timeout=30)
        # js is now closed; fork a child that tries to reuse it.
        reader, writer = os.pipe()
        pid = os.fork()
        if pid == 0:  # pragma: no cover - exercised in the forked child
            os.close(reader)
            try:
                js.submit(fn=len, args=("child",))
                name = "NoError"
            except BaseException as exc:  # noqa: BLE001
                name = type(exc).__name__
            os.write(writer, name.encode())
            os.close(writer)
            os._exit(0)
        os.close(writer)
        self.assertEqual(os.waitpid(pid, 0)[1], 0)
        self.assertEqual(os.read(reader, 64).decode(), "RuntimeError")
        os.close(reader)


# ---------------------------------------------------------------------------
# Killing a nested intermediate process leaks tokens.
#
# Every pipe-based jobserver shares this shortcoming: GNU Make, Rust's
# jobserver crate (Cargo/rustc), and Ninja among them.  A pipe provides
# no mechanism for a dead process to return tokens it consumed.
#
# The scenario
# ~~~~~~~~~~~~
#
# A Jobserver(slots=3) starts with tokens {0, 1, 2} in its pipe:
#
#     Parent --submit--> Child --submit--> Grandchild
#       |                  |                   |
#       |  owns Child      |  owns Grandchild  |  does work
#       |  future          |  future           |
#       |  holds token 0   |  holds token 1    |
#
# When the Child is killed:
#
#   * The Parent detects Child death via EOFError on the result pipe,
#     wraps it as LostResult, and fires the Child future's internal
#     callback restoring token 0.
#
#   * But the Grandchild's token-restoring callback existed only in the
#     Child's address space.  The Child is dead; the callback is gone.
#     No surviving process knows token 1 must be returned.  Token 1 is
#     permanently leaked.
# ---------------------------------------------------------------------------


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
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=self.SLOTS) as js:
                    # All slots are available: submit and immediately collect
                    for _ in range(self.SLOTS):
                        js.submit(fn=time.sleep, args=(0,), timeout=0).result(
                            timeout=5
                        )

                    # Parent -> Child -> Grandchild
                    future = js.submit(
                        fn=_child_work, args=(js, 0.2), timeout=5
                    )
                    time.sleep(0.5)  # Let Child start and submit Grandchild

                    # Kill the intermediate Child and collect it
                    future.wait(signal=signal.SIGKILL, timeout=5)
                    with self.assertRaises(LostResult):
                        future.result()

                    # Grandchild finishes but nobody reclaims its token.
                    # Its token was consumed before the kill, so the leak
                    # is independent of when the brief Grandchild returns;
                    # this wait only need exceed the Grandchild duration.
                    time.sleep(0.5)

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
                        with self.assertRaises(LostResult):
                            f.result()
