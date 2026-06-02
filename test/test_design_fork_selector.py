# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Forked children must not inherit the parent's selector (issue #296).

Under spawn/forkserver a Jobserver is pickled into each child, so
__setstate__ rebuilds a fresh per-process selector.  Under fork no
pickling occurs: a worker inherits the parent's Jobserver as a memory
copy, so without intervention it keeps the parent's selector, including
sibling Futures whose Process objects belong to an ancestor.

A worker performing nested fan-out (submitting a second child while a
first sibling is still registered) then calls reclaim_resources() on
entry to submit().  If an inherited sibling's sentinel fd is ready, the
worker would join()/is_alive() a process it never created, raising
"can only join a child process".  The Jobserver instead rebuilds a
clean selector when it first observes a pid change.
"""

import os
import time
import unittest

from jobserver import Jobserver

from .helpers import start_methods


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
