# Copyright (C) 2019-2026 Rhys Ulerich
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""SPSCQueue, MPMCQueue, FixedBytesQueue, and low-level utility tests.

SPSCQueue receives heavy indirect coverage through the Jobserver and
JobserverExecutor suites, so this file only covers its own API surface
and the resolve_context / timeout_to_deadline helpers.
"""

import copy
import os
import queue
import tempfile
import threading
import time
import unittest
from multiprocessing import get_context
from multiprocessing.context import BaseContext
from multiprocessing.synchronize import Lock as IPCLock

from jobserver._compat import pipe_buf
from jobserver._queue import (
    FixedBytesQueue,
    MPMCQueue,
    SPSCQueue,
    from_fds,
    from_fifo,
    resolve_context,
    timeout_to_deadline,
)

from .helpers import start_methods


def _mpmc_echo_double(inq: MPMCQueue, outq: MPMCQueue) -> None:
    """Child: receive one item on inq and send back twice its value."""
    outq.put(inq.get(timeout=30) * 2)


def _fbq_echo(inq: FixedBytesQueue, outq: FixedBytesQueue) -> None:
    """Child: receive one token on inq and send it back unchanged on outq."""
    outq.put(inq.get(timeout=30))


class TestSPSCQueue(unittest.TestCase):
    """SPSCQueue behavior."""

    def test_duplication_spscqueue(self) -> None:
        """Copying of SPSCQueue is explicitly allowed."""
        for method in start_methods():
            with self.subTest(method=method):
                with SPSCQueue(method) as mq1:
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
        with SPSCQueue() as mq:
            pass
        # Both ends already closed by __exit__; repeat must not raise
        mq.close_get()
        mq.close_put()

    def test_close_tolerates_already_closed_fd(self) -> None:
        """close_*() swallows OSError/EBADF from a stale fd (#335).

        Mirrors a pickled in-process clone sharing the same fd: closing
        the underlying handle after it has already gone must not raise.
        """
        mq: SPSCQueue = SPSCQueue()
        # Close the raw fds out from under the queue, then close_*() must
        # tolerate the resulting EBADF rather than propagating it.
        os.close(mq._writer.fileno())
        os.close(mq._reader.fileno())
        mq.close_put()  # would raise OSError(EBADF) without the guard
        mq.close_get()

    def test_context_manager(self) -> None:
        """Context manager closes both ends; put/get raise after exit."""
        with SPSCQueue() as mq:
            mq.put(42)
            self.assertEqual(42, mq.get(timeout=1))
        with self.assertRaises(ValueError):
            mq.put(99)
        with self.assertRaises(ValueError):
            mq.get(timeout=0)


class TestMPMCQueue(unittest.TestCase):
    """MPMCQueue behavior."""

    def test_context_manager_roundtrip(self) -> None:
        """Context manager closes both ends; put/get raise after exit."""
        with MPMCQueue() as mq:
            mq.put(42)
            self.assertEqual(42, mq.get(timeout=1))
        with self.assertRaises(ValueError):
            mq.put(99)
        with self.assertRaises(ValueError):
            mq.get(timeout=0)

    def test_guards_are_interprocess_locks(self) -> None:
        """The read/write guards are multiprocessing IPC locks."""
        with MPMCQueue() as mq:
            self.assertIsInstance(mq._read_lock, IPCLock)
            self.assertIsInstance(mq._write_lock, IPCLock)

    def test_getstate_preserves_locks(self) -> None:
        """_getstate_locks preserves the live locks; setstate reuses them."""
        with MPMCQueue() as mq:
            read_lock, write_lock = mq._read_lock, mq._write_lock
            state = mq.__getstate__()
            # Unlike SPSCQueue (which emits None), the lock blob is present.
            self.assertEqual((read_lock, write_lock), state[2])
            mq.__setstate__(state)
            self.assertIs(read_lock, mq._read_lock)
            self.assertIs(write_lock, mq._write_lock)

    def test_cross_process_roundtrip(self) -> None:
        """Locks survive pickling into a child that echoes via the queue."""
        for method in start_methods():
            with self.subTest(method=method):
                ctx = get_context(method)
                with (
                    MPMCQueue(ctx) as inq,
                    MPMCQueue(ctx) as outq,
                ):
                    proc = ctx.Process(
                        target=_mpmc_echo_double, args=(inq, outq)
                    )
                    proc.start()
                    inq.put(21)
                    self.assertEqual(42, outq.get(timeout=30))
                    proc.join(30)
                    self.assertEqual(0, proc.exitcode)

    def test_concurrent_puts_from_threads(self) -> None:
        """Multiple threads putting concurrently produce no lost items."""
        n_threads = 4
        per_thread = 50
        with MPMCQueue() as mq:

            def writer(start: int) -> None:
                for i in range(start, start + per_thread):
                    mq.put(i)

            threads = [
                threading.Thread(target=writer, args=(t * per_thread,))
                for t in range(n_threads)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            got = set()
            for _ in range(n_threads * per_thread):
                got.add(mq.get(timeout=5))
            self.assertEqual(got, set(range(n_threads * per_thread)))

    def test_concurrent_gets_from_threads(self) -> None:
        """Multiple threads getting concurrently collect all items."""
        n_threads = 4
        total = 200
        with MPMCQueue() as mq:
            for i in range(total):
                mq.put(i)
            results: list[list[int]] = [[] for _ in range(n_threads)]

            def reader(idx: int) -> None:
                while True:
                    try:
                        results[idx].append(mq.get(timeout=0.1))
                    except queue.Empty:
                        return

            threads = [
                threading.Thread(target=reader, args=(i,))
                for i in range(n_threads)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            combined = sorted(v for r in results for v in r)
            self.assertEqual(combined, list(range(total)))


class TestFixedBytesQueue(unittest.TestCase):
    """FixedBytesQueue behavior.

    The headline property is that, restricted to fixedlen-byte payloads,
    FixedBytesQueue is observationally indistinguishable from MPMCQueue
    used on bytes: same FIFO values, same Empty/EOF/closed semantics.
    """

    @staticmethod
    def _byte_queues():
        """Factories for a FixedBytesQueue and an MPMCQueue using 4-byte
        tokens, the regime in which the two must be indistinguishable."""
        return (
            ("FixedBytesQueue", lambda: FixedBytesQueue(fixedlen=4)),
            ("MPMCQueue", lambda: MPMCQueue()),
        )

    def test_roundtrip_matches_mpmc(self) -> None:
        """Both queues return the same bytes in the same FIFO order."""
        tokens = [b"AAAA", b"BBBB", b"CCCC", b"DDDD"]
        results = {}
        for name, factory in self._byte_queues():
            with factory() as q:
                for token in tokens:
                    q.put(token)
                results[name] = [q.get(timeout=1) for _ in tokens]
        self.assertEqual(tokens, results["FixedBytesQueue"])
        self.assertEqual(results["FixedBytesQueue"], results["MPMCQueue"])

    def test_empty_raises_queue_empty_like_mpmc(self) -> None:
        """get() on an empty queue raises queue.Empty for both."""
        for name, factory in self._byte_queues():
            with self.subTest(queue=name), factory() as q:
                with self.assertRaises(queue.Empty):
                    q.get(timeout=0)

    def test_eof_after_close_put_like_mpmc(self) -> None:
        """Draining then reading a hung-up queue raises EOFError for both."""
        for name, factory in self._byte_queues():
            with self.subTest(queue=name):
                q = factory()
                try:
                    q.put(b"AAAA")
                    q.close_put()
                    self.assertEqual(b"AAAA", q.get(timeout=1))
                    with self.assertRaises(EOFError):
                        q.get(timeout=1)
                finally:
                    q.close_get()

    def test_after_close_raises_valueerror_like_mpmc(self) -> None:
        """put()/get() after the context manager exits raise for both."""
        for name, factory in self._byte_queues():
            with self.subTest(queue=name):
                with factory() as q:
                    q.put(b"AAAA")
                    self.assertEqual(b"AAAA", q.get(timeout=1))
                with self.assertRaises(ValueError):
                    q.put(b"AAAA")
                with self.assertRaises(ValueError):
                    q.get(timeout=0)

    def test_init_rejects_out_of_range_fixedlen(self) -> None:
        """fixedlen must satisfy 1 <= fixedlen < pipe_buf()."""
        for bad in (0, -1, pipe_buf(), pipe_buf() + 1):
            with self.subTest(fixedlen=bad), self.assertRaises(ValueError):
                FixedBytesQueue(fixedlen=bad)

    def test_init_rejects_non_int_fixedlen(self) -> None:
        """A non-int fixedlen raises TypeError."""
        with self.assertRaises(TypeError):
            FixedBytesQueue(fixedlen=4.0)

    def test_put_rejects_non_multiple_length(self) -> None:
        """put() rejects empty and non-multiple-of-fixedlen payloads."""
        with FixedBytesQueue(fixedlen=4) as q:
            for bad in (b"", b"abc", b"abcde"):
                with self.subTest(payload=bad), self.assertRaises(ValueError):
                    q.put(bad)

    def test_put_rejects_over_pipe_buf(self) -> None:
        """put() rejects a whole-token payload exceeding pipe_buf()."""
        with FixedBytesQueue(fixedlen=4) as q:
            over = b"a" * (((pipe_buf() // 4) + 1) * 4)
            with self.assertRaises(ValueError):
                q.put(over)

    def test_multi_token_put_reads_individually(self) -> None:
        """A multi-token put() is read back one token at a time."""
        with FixedBytesQueue(fixedlen=4) as q:
            q.put(b"AAAABBBBCCCC")
            self.assertEqual(
                [b"AAAA", b"BBBB", b"CCCC"],
                [q.get(timeout=1) for _ in range(3)],
            )

    def test_cross_process_roundtrip(self) -> None:
        """Tokens survive being shared with a child across start methods."""
        for method in start_methods():
            with self.subTest(method=method):
                ctx = get_context(method)
                with (
                    FixedBytesQueue(ctx, fixedlen=4) as inq,
                    FixedBytesQueue(ctx, fixedlen=4) as outq,
                ):
                    proc = ctx.Process(target=_fbq_echo, args=(inq, outq))
                    proc.start()
                    inq.put(b"PING")
                    self.assertEqual(b"PING", outq.get(timeout=30))
                    proc.join(30)
                    self.assertEqual(0, proc.exitcode)


class TestEndpointSources(unittest.TestCase):
    """from_fds / from_fifo as the pipe source for any queue type."""

    def test_from_fds_roundtrips_all_queues(self) -> None:
        """Every queue type works over an inherited fd pair, surviving a
        getstate/setstate round-trip before put/get."""
        cases = (
            (lambda s: FixedBytesQueue(s, fixedlen=1), b"J"),
            (lambda s: SPSCQueue(s), "hi"),
            (lambda s: MPMCQueue(s), ("tuple", 1)),
        )
        for factory, item in cases:
            with self.subTest(item=item):
                r, w = os.pipe()
                q = factory(from_fds(r, w))
                os.close(r)  # the queue holds its own dups
                os.close(w)
                clone = type(q).__new__(type(q))
                clone.__setstate__(q.__getstate__())
                with clone:
                    clone.put(item)
                    self.assertEqual(item, clone.get(timeout=1))

    def test_from_fds_keeps_caller_fds(self) -> None:
        """from_fds dups, so the caller's fds stay usable afterward."""
        r, w = os.pipe()
        try:
            with FixedBytesQueue(from_fds(r, w), fixedlen=1) as q:
                q.put(b"J")
                self.assertEqual(b"J", q.get(timeout=1))
            os.write(w, b"Z")  # queue closed its dups, not the originals
            self.assertEqual(b"Z", os.read(r, 1))
        finally:
            os.close(r)
            os.close(w)

    def test_from_fds_preserves_token_values(self) -> None:
        """Opaque byte values survive verbatim (the GNU Make guarantee)."""
        r, w = os.pipe()
        os.write(w, b"\x00\x7f\xff")
        q = FixedBytesQueue(from_fds(r, w), fixedlen=1)
        os.close(r)  # the queue holds its own dups
        os.close(w)
        with q:
            got = sorted(q.get(timeout=1) for _ in range(3))
        self.assertEqual([b"\x00", b"\x7f", b"\xff"], got)

    def test_from_fifo_roundtrip(self) -> None:
        """A named FIFO backs a queue end to end."""
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "fifo")
            os.mkfifo(path)
            with FixedBytesQueue(from_fifo(path), fixedlen=1) as q:
                q.put(b"J")
                self.assertEqual(b"J", q.get(timeout=1))


class TestResolveContext(unittest.TestCase):
    """resolve_context behavior."""

    def test_none_returns_default_context(self) -> None:
        """None resolves to the default multiprocessing context."""
        ctx = resolve_context(None)
        self.assertIsInstance(ctx, BaseContext)

    def test_string_returns_named_context(self) -> None:
        """A start-method string resolves to the named context."""
        for method in start_methods():
            with self.subTest(method=method):
                ctx = resolve_context(method)
                self.assertIsInstance(ctx, BaseContext)

    def test_context_passes_through(self) -> None:
        """An existing BaseContext is returned unchanged."""
        for method in start_methods():
            with self.subTest(method=method):
                original = get_context(method)
                self.assertIs(resolve_context(original), original)


class TestTimeoutToDeadline(unittest.TestCase):
    """timeout_to_deadline behavior."""

    def test_none_yields_large_deadline(self) -> None:
        """None timeout produces a deadline far in the future."""
        before = time.monotonic()
        deadline = timeout_to_deadline(None)
        self.assertGreater(deadline, before + 86400)

    def test_zero_yields_near_now(self) -> None:
        """Zero timeout produces a deadline near the current time."""
        before = time.monotonic()
        deadline = timeout_to_deadline(0)
        after = time.monotonic()
        self.assertGreaterEqual(deadline, before)
        self.assertLessEqual(deadline, after + 0.01)

    def test_positive_offset(self) -> None:
        """A positive timeout offsets from the current monotonic time."""
        before = time.monotonic()
        deadline = timeout_to_deadline(5.0)
        after = time.monotonic()
        self.assertGreaterEqual(deadline, before + 5.0)
        self.assertLessEqual(deadline, after + 5.0 + 0.01)

    def test_non_numeric_raises_typeerror(self) -> None:
        """A non-numeric timeout is a clear TypeError, not raw arithmetic
        failure (#342); bool is rejected as an int subclass."""
        for bad in ("5", object(), [], True, False):
            with self.assertRaises(TypeError) as cm:
                timeout_to_deadline(bad)
            self.assertIn("timeout", str(cm.exception))

    def test_negative_raises_valueerror(self) -> None:
        """A negative timeout is rejected so caller bugs surface as
        ValueError instead of an immediately-expired deadline (#383)."""
        for bad in (-1, -0.001, -5.0):
            with self.assertRaises(ValueError) as cm:
                timeout_to_deadline(bad)
            self.assertIn("non-negative", str(cm.exception))

    def test_non_finite_raises_valueerror(self) -> None:
        """NaN/Inf timeouts are rejected: NaN silently disables every
        deadline check and busy-spins, while Inf is an undocumented alias
        for None.  Use None to block indefinitely (#389)."""
        for bad in (float("nan"), float("inf"), float("-inf")):
            with self.assertRaises(ValueError) as cm:
                timeout_to_deadline(bad)
            self.assertIn("finite", str(cm.exception))
