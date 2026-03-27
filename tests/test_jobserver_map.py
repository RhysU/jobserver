# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Jobserver.map() behavior.

Exercises the map API covering argument handling, chunking, buffering,
timeout semantics, and error propagation.
"""
import time
import unittest

from multiprocessing import get_all_start_methods

from jobserver import Jobserver

from .helpers import (
    FAST,
    TIMEOUT,
    helper_return,
    raising_at_position,
    silence_forkserver,
)


def _kw_sum(a, b=0, c=0):
    """Return a + b + c (picklable helper for kwargs tests)."""
    return a + b + c


def _kw_only(*, x=0, y=0):
    """Return x + y (keyword-only picklable helper)."""
    return x + y


def _slow_identity(x, delay=0.0):
    """Return x after sleeping for delay seconds."""
    time.sleep(delay)
    return x


class TestJobserverMap(unittest.TestCase):
    """Jobserver.map() functionality."""

    @classmethod
    def setUpClass(cls):
        silence_forkserver()

    # ---- Basic functionality ----

    def test_none_argses(self) -> None:
        """map() with argses=None yields nothing."""
        js = Jobserver(context=FAST, slots=2)
        results = list(js.map(fn=len))
        self.assertEqual(results, [])

    def test_none_both(self) -> None:
        """map() with both None yields nothing."""
        js = Jobserver(context=FAST, slots=2)
        results = list(js.map(fn=len, argses=None, kwargses=None))
        self.assertEqual(results, [])

    def test_empty_argses(self) -> None:
        """map() over empty argses yields nothing."""
        js = Jobserver(context=FAST, slots=2)
        results = list(js.map(fn=len, argses=[]))
        self.assertEqual(results, [])

    def test_empty_both(self) -> None:
        """map() with empty argses and kwargses yields nothing."""
        js = Jobserver(context=FAST, slots=2)
        results = list(js.map(fn=len, argses=[], kwargses=[]))
        self.assertEqual(results, [])

    def test_none_argses_with_kwargses(self) -> None:
        """map() with argses=None and kwargses provided uses empty args."""
        js = Jobserver(context=FAST, slots=2)
        kwargses = [{"x": 1, "y": 2}, {"x": 10, "y": 20}]
        results = list(
            js.map(
                fn=_kw_only,
                argses=None,
                kwargses=kwargses,
                timeout=TIMEOUT,
            )
        )
        self.assertEqual(results, [3, 30])

    def test_basic_argses_only(self) -> None:
        """map() with argses only works like builtin map."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                argses = [((1, 2, 3),), ((4, 5),), ((),)]
                results = list(js.map(fn=len, argses=argses, timeout=TIMEOUT))
                self.assertEqual(results, [3, 2, 0])

    def test_argses_and_kwargses(self) -> None:
        """map() with both argses and kwargses passes both."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                argses = [(1,), (10,), (100,)]
                kwargses = [
                    {"b": 2, "c": 3},
                    {"b": 20, "c": 30},
                    {"b": 200, "c": 300},
                ]
                results = list(
                    js.map(
                        fn=_kw_sum,
                        argses=argses,
                        kwargses=kwargses,
                        timeout=TIMEOUT,
                    )
                )
                self.assertEqual(results, [6, 60, 600])

    def test_kwargses_shorter_than_argses_raises(self) -> None:
        """Mismatched lengths raise ValueError (kwargses shorter)."""
        js = Jobserver(context=FAST, slots=2)
        with self.assertRaises(ValueError):
            js.map(
                fn=_kw_sum,
                argses=[(1,), (2,), (3,)],
                kwargses=[{"b": 10}],
            )

    def test_argses_shorter_than_kwargses_raises(self) -> None:
        """Mismatched lengths raise ValueError (argses shorter)."""
        js = Jobserver(context=FAST, slots=2)
        with self.assertRaises(ValueError):
            js.map(
                fn=_kw_sum,
                argses=[(1,)],
                kwargses=[{"b": 10}, {"b": 20}, {"b": 30}],
            )

    def test_preserves_order(self) -> None:
        """Results are yielded in submission order, not completion."""
        js = Jobserver(context=FAST, slots=4)
        argses = [(i,) for i in range(20)]
        results = list(
            js.map(fn=helper_return, argses=argses, timeout=TIMEOUT)
        )
        self.assertEqual(results, list(range(20)))

    def test_single_element(self) -> None:
        """map() over a single element works."""
        js = Jobserver(context=FAST, slots=1)
        results = list(js.map(fn=len, argses=[((1, 2),)], timeout=TIMEOUT))
        self.assertEqual(results, [2])

    # ---- Iterator semantics ----

    def test_returns_iterator(self) -> None:
        """map() returns an iterator, not a list."""
        js = Jobserver(context=FAST, slots=2)
        result = js.map(fn=len, argses=[((1,),)], timeout=TIMEOUT)
        self.assertTrue(hasattr(result, "__next__"))
        self.assertEqual(next(result), 1)

    # ---- Chunksize ----

    def test_chunksize_basic(self) -> None:
        """map() with chunksize > 1 groups calls correctly."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                argses = [((i,),) for i in range(7)]
                results = list(
                    js.map(
                        fn=len,
                        argses=argses,
                        chunksize=3,
                        timeout=TIMEOUT,
                    )
                )
                self.assertEqual(results, [1] * 7)

    def test_chunksize_with_kwargs(self) -> None:
        """map() chunksize > 1 works with kwargs too."""
        js = Jobserver(context=FAST, slots=2)
        argses = [(i,) for i in range(6)]
        kwargses = [{"b": 100}] * 6
        results = list(
            js.map(
                fn=_kw_sum,
                argses=argses,
                kwargses=kwargses,
                chunksize=2,
                timeout=TIMEOUT,
            )
        )
        self.assertEqual(results, [100, 101, 102, 103, 104, 105])

    def test_chunksize_exact_multiple(self) -> None:
        """chunksize evenly divides work count."""
        js = Jobserver(context=FAST, slots=2)
        argses = [(i,) for i in range(6)]
        results = list(
            js.map(
                fn=helper_return,
                argses=argses,
                chunksize=3,
                timeout=TIMEOUT,
            )
        )
        self.assertEqual(results, list(range(6)))

    def test_chunksize_not_exact_multiple(self) -> None:
        """chunksize does not evenly divide work count."""
        js = Jobserver(context=FAST, slots=2)
        argses = [(i,) for i in range(7)]
        results = list(
            js.map(
                fn=helper_return,
                argses=argses,
                chunksize=3,
                timeout=TIMEOUT,
            )
        )
        self.assertEqual(results, list(range(7)))

    def test_chunksize_larger_than_work(self) -> None:
        """chunksize larger than total work produces single chunk."""
        js = Jobserver(context=FAST, slots=2)
        argses = [(i,) for i in range(3)]
        results = list(
            js.map(
                fn=helper_return,
                argses=argses,
                chunksize=100,
                timeout=TIMEOUT,
            )
        )
        self.assertEqual(results, [0, 1, 2])

    def test_chunksize_one(self) -> None:
        """chunksize=1 is the default single-call behavior."""
        js = Jobserver(context=FAST, slots=2)
        argses = [(i,) for i in range(5)]
        results = list(
            js.map(
                fn=helper_return,
                argses=argses,
                chunksize=1,
                timeout=TIMEOUT,
            )
        )
        self.assertEqual(results, list(range(5)))

    # ---- Chunksize edge cases ----

    def test_chunksize_zero_raises(self) -> None:
        """chunksize=0 raises ValueError."""
        js = Jobserver(context=FAST, slots=2)
        with self.assertRaises(ValueError):
            js.map(fn=len, argses=[], chunksize=0)

    def test_chunksize_negative_raises(self) -> None:
        """Negative chunksize raises ValueError."""
        js = Jobserver(context=FAST, slots=2)
        with self.assertRaises(ValueError):
            js.map(fn=len, argses=[], chunksize=-1)

    # ---- Buffersize ----

    def test_buffersize_basic(self) -> None:
        """map() with buffersize limits in-flight submissions."""
        js = Jobserver(context=FAST, slots=4)
        argses = [(i,) for i in range(10)]
        results = list(
            js.map(
                fn=helper_return,
                argses=argses,
                buffersize=2,
                timeout=TIMEOUT,
            )
        )
        self.assertEqual(results, list(range(10)))

    def test_buffersize_one(self) -> None:
        """buffersize=1 serializes submission and consumption."""
        js = Jobserver(context=FAST, slots=2)
        argses = [(i,) for i in range(5)]
        results = list(
            js.map(
                fn=helper_return,
                argses=argses,
                buffersize=1,
                timeout=TIMEOUT,
            )
        )
        self.assertEqual(results, list(range(5)))

    def test_buffersize_larger_than_work(self) -> None:
        """buffersize larger than work count submits everything."""
        js = Jobserver(context=FAST, slots=2)
        argses = [(i,) for i in range(3)]
        results = list(
            js.map(
                fn=helper_return,
                argses=argses,
                buffersize=100,
                timeout=TIMEOUT,
            )
        )
        self.assertEqual(results, [0, 1, 2])

    def test_buffersize_with_chunksize(self) -> None:
        """buffersize and chunksize work together."""
        js = Jobserver(context=FAST, slots=4)
        argses = [(i,) for i in range(12)]
        results = list(
            js.map(
                fn=helper_return,
                argses=argses,
                chunksize=3,
                buffersize=2,
                timeout=TIMEOUT,
            )
        )
        self.assertEqual(results, list(range(12)))

    # ---- Buffersize edge cases ----

    def test_buffersize_zero_raises(self) -> None:
        """buffersize=0 raises ValueError."""
        js = Jobserver(context=FAST, slots=2)
        with self.assertRaises(ValueError):
            js.map(fn=len, argses=[], buffersize=0)

    def test_buffersize_negative_raises(self) -> None:
        """Negative buffersize raises ValueError."""
        js = Jobserver(context=FAST, slots=2)
        with self.assertRaises(ValueError):
            js.map(fn=len, argses=[], buffersize=-1)

    # ---- Timeout semantics ----

    def test_timeout_none_blocks(self) -> None:
        """timeout=None waits indefinitely for results."""
        js = Jobserver(context=FAST, slots=2)
        argses = [(i,) for i in range(4)]
        results = list(js.map(fn=helper_return, argses=argses, timeout=None))
        self.assertEqual(results, list(range(4)))

    def test_timeout_sufficient(self) -> None:
        """Generous timeout does not interfere."""
        js = Jobserver(context=FAST, slots=2)
        argses = [(i,) for i in range(4)]
        results = list(
            js.map(
                fn=helper_return,
                argses=argses,
                timeout=TIMEOUT,
            )
        )
        self.assertEqual(results, list(range(4)))

    def test_timeout_expired_raises(self) -> None:
        """Expired deadline raises TimeoutError on next()."""
        js = Jobserver(context=FAST, slots=1)
        argses = [(i, 0.5) for i in range(10)]
        it = js.map(fn=_slow_identity, argses=argses, timeout=0.2)
        with self.assertRaises(TimeoutError):
            list(it)

    def test_timeout_zero_raises(self) -> None:
        """timeout=0 raises TimeoutError unless results are instant."""
        js = Jobserver(context=FAST, slots=1)
        argses = [(i, 0.5) for i in range(3)]
        it = js.map(fn=_slow_identity, argses=argses, timeout=0)
        with self.assertRaises(TimeoutError):
            list(it)

    def test_timeout_is_from_map_call(self) -> None:
        """Timeout counts from the map() call, not from __next__."""
        js = Jobserver(context=FAST, slots=2)
        # Submit work that takes ~0.3s each, with 0.5s timeout
        argses = [(i, 0.3) for i in range(5)]
        it = js.map(fn=_slow_identity, argses=argses, timeout=0.5)
        # First result may succeed, but eventually we hit the
        # deadline
        with self.assertRaises(TimeoutError):
            list(it)

    # ---- Error propagation ----

    def test_exception_propagates(self) -> None:
        """Exception raised by fn surfaces from __next__."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                argses = [(0, 5), (1, 5), (2, 5), (3, 5), (4, 5)]
                it = js.map(
                    fn=raising_at_position,
                    argses=argses,
                    timeout=TIMEOUT,
                )
                # Items before the failure succeed
                self.assertEqual(next(it), 0)
                self.assertEqual(next(it), 1)
                self.assertEqual(next(it), 2)
                self.assertEqual(next(it), 3)
                self.assertEqual(next(it), 4)

    def test_exception_at_first(self) -> None:
        """Exception on the very first call propagates."""
        js = Jobserver(context=FAST, slots=2)
        argses = [(0, 0), (1, 0), (2, 0)]
        it = js.map(
            fn=raising_at_position,
            argses=argses,
            timeout=TIMEOUT,
        )
        with self.assertRaises(ValueError):
            next(it)

    def test_exception_midstream(self) -> None:
        """Exception mid-stream propagates at the right position."""
        js = Jobserver(context=FAST, slots=2)
        argses = [(0, 2), (1, 2), (2, 2), (3, 2), (4, 2)]
        it = js.map(
            fn=raising_at_position,
            argses=argses,
            timeout=TIMEOUT,
        )
        self.assertEqual(next(it), 0)
        self.assertEqual(next(it), 1)
        with self.assertRaises(ValueError):
            next(it)

    def test_exception_with_chunksize(self) -> None:
        """Exception within a chunk fails the entire chunk."""
        js = Jobserver(context=FAST, slots=2)
        # Items 0,1,2 in first chunk; item 2 raises.
        # The whole first chunk fails.
        argses = [(0, 2), (1, 2), (2, 2), (3, 2)]
        it = js.map(
            fn=raising_at_position,
            argses=argses,
            chunksize=3,
            timeout=TIMEOUT,
        )
        with self.assertRaises(ValueError):
            next(it)

    # ---- Saturation ----

    def test_many_items_few_slots(self) -> None:
        """Many work items with few slots completes without deadlock."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=2)
                argses = [(i,) for i in range(30)]
                results = list(
                    js.map(
                        fn=helper_return,
                        argses=argses,
                        timeout=TIMEOUT,
                    )
                )
                self.assertEqual(results, list(range(30)))

    def test_many_items_chunksize_few_slots(self) -> None:
        """Chunked work with few slots completes correctly."""
        js = Jobserver(context=FAST, slots=2)
        argses = [(i,) for i in range(30)]
        results = list(
            js.map(
                fn=helper_return,
                argses=argses,
                chunksize=5,
                timeout=TIMEOUT,
            )
        )
        self.assertEqual(results, list(range(30)))
