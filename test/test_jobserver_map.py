# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Jobserver.map() behavior.

Exercises the map API covering argument handling, chunking, buffering,
timeout semantics, and error propagation.
"""

import concurrent.futures
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

    # ---- Empty / no-op inputs ----

    def test_empty_inputs(self) -> None:
        """map() with empty or None inputs yields nothing."""
        with Jobserver(context=FAST, slots=2) as js:
            for argses, kwargses in [
                (None, None),
                ([], None),
                (None, []),
                ([], []),
            ]:
                with self.subTest(argses=argses, kwargses=kwargses):
                    results = list(
                        js.map(
                            fn=len,
                            argses=argses,
                            kwargses=kwargses,
                            buffersize=None,
                        )
                    )
                    self.assertEqual(results, [])

    # ---- Basic functionality ----

    def test_argses_only(self) -> None:
        """map() with argses only works like builtin map."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=2) as js:
                    argses = [((1, 2, 3),), ((4, 5),), ((),)]
                    results = list(
                        js.map(fn=len, argses=argses, timeout=TIMEOUT)
                    )
                    self.assertEqual(results, [3, 2, 0])

    def test_kwargses_only(self) -> None:
        """map() with argses=None and kwargses provided uses empty args."""
        with Jobserver(context=FAST, slots=2) as js:
            kwargses = [{"x": 1, "y": 2}, {"x": 10, "y": 20}]
            results = list(
                js.map(fn=_kw_only, kwargses=kwargses, timeout=TIMEOUT)
            )
            self.assertEqual(results, [3, 30])

    def test_argses_and_kwargses(self) -> None:
        """map() with both argses and kwargses passes both."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=2) as js:
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

    def test_single_element(self) -> None:
        """map() over a single element works."""
        with Jobserver(context=FAST, slots=1) as js:
            results = list(
                js.map(
                    fn=len,
                    argses=[((1, 2),)],
                    buffersize=None,
                    timeout=TIMEOUT,
                )
            )
            self.assertEqual(results, [2])

    def test_length_mismatch_raises(self) -> None:
        """Mismatched argses/kwargses lengths raise ValueError."""
        with Jobserver(context=FAST, slots=2) as js:
            for argses, kwargses in [
                ([(1,), (2,), (3,)], [{"b": 10}]),
                ([(1,)], [{"b": 10}, {"b": 20}, {"b": 30}]),
            ]:
                with self.subTest(argses=argses, kwargses=kwargses):
                    with self.assertRaises(ValueError):
                        js.map(fn=_kw_sum, argses=argses, kwargses=kwargses)

    def test_preserves_order(self) -> None:
        """Results are yielded in submission order, not completion."""
        with Jobserver(context=FAST, slots=4) as js:
            argses = [(i,) for i in range(20)]
            results = list(
                js.map(fn=helper_return, argses=argses, timeout=TIMEOUT)
            )
            self.assertEqual(results, list(range(20)))

    def test_returns_iterator(self) -> None:
        """map() returns an iterator, not a list."""
        with Jobserver(context=FAST, slots=2) as js:
            result = js.map(fn=len, argses=[((1,),)], timeout=TIMEOUT)
            self.assertTrue(hasattr(result, "__next__"))
            self.assertEqual(next(result), 1)

    # ---- Chunksize ----

    def test_chunksize(self) -> None:
        """Various chunksize values produce correct ordered results."""
        with Jobserver(context=FAST, slots=2) as js:
            for n, cs in [(7, 3), (6, 3), (7, 3), (3, 100), (5, 1)]:
                with self.subTest(n=n, chunksize=cs):
                    argses = [(i,) for i in range(n)]
                    results = list(
                        js.map(
                            fn=helper_return,
                            argses=argses,
                            chunksize=cs,
                            timeout=TIMEOUT,
                        )
                    )
                    self.assertEqual(results, list(range(n)))

    def test_chunksize_with_kwargs(self) -> None:
        """map() chunksize > 1 works with kwargs too."""
        with Jobserver(context=FAST, slots=2) as js:
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

    def test_chunksize_invalid_raises(self) -> None:
        """chunksize <= 0 raises ValueError."""
        with Jobserver(context=FAST, slots=2) as js:
            for cs in [0, -1]:
                with self.subTest(chunksize=cs):
                    with self.assertRaises(ValueError):
                        js.map(fn=len, argses=[], chunksize=cs)

    # ---- Buffersize ----

    def test_buffersize(self) -> None:
        """Various buffersize values produce correct ordered results."""
        with Jobserver(context=FAST, slots=4) as js:
            for bs in [1, 2, 100]:
                with self.subTest(buffersize=bs):
                    argses = [(i,) for i in range(10)]
                    results = list(
                        js.map(
                            fn=helper_return,
                            argses=argses,
                            buffersize=bs,
                            timeout=TIMEOUT,
                        )
                    )
                    self.assertEqual(results, list(range(10)))

    def test_buffersize_with_chunksize(self) -> None:
        """buffersize and chunksize work together."""
        with Jobserver(context=FAST, slots=4) as js:
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

    def test_buffersize_invalid_raises(self) -> None:
        """buffersize <= 0 raises ValueError."""
        with Jobserver(context=FAST, slots=2) as js:
            for bs in [0, -1]:
                with self.subTest(buffersize=bs):
                    with self.assertRaises(ValueError):
                        js.map(fn=len, argses=[], buffersize=bs)

    # ---- Timeout semantics ----

    def test_timeout_sufficient(self) -> None:
        """timeout=None and generous timeout both work."""
        with Jobserver(context=FAST, slots=2) as js:
            argses = [(i,) for i in range(4)]
            for timeout in [None, TIMEOUT]:
                with self.subTest(timeout=timeout):
                    results = list(
                        js.map(
                            fn=helper_return,
                            argses=argses,
                            timeout=timeout,
                        )
                    )
                    self.assertEqual(results, list(range(4)))

    # concurrent.futures.TimeoutError differs from builtin TimeoutError
    # before Python 3.11.
    def test_timeout_expired_raises(self) -> None:
        """Expired deadline raises TimeoutError."""
        with Jobserver(context=FAST, slots=1) as js:
            for timeout in [0, 0.2]:
                with self.subTest(timeout=timeout):
                    argses = [(i, 0.5) for i in range(10)]
                    it = js.map(
                        fn=_slow_identity, argses=argses, timeout=timeout
                    )
                    with self.assertRaises(concurrent.futures.TimeoutError):
                        list(it)

    # concurrent.futures.TimeoutError differs from builtin TimeoutError
    # before Python 3.11.
    def test_timeout_is_from_map_call(self) -> None:
        """Timeout counts from the map() call, not from __next__."""
        with Jobserver(context=FAST, slots=2) as js:
            argses = [(i, 0.3) for i in range(5)]
            it = js.map(fn=_slow_identity, argses=argses, timeout=0.5)
            with self.assertRaises(concurrent.futures.TimeoutError):
                list(it)

    # ---- Error propagation ----

    def test_exception_propagates(self) -> None:
        """Exception raised by fn surfaces from __next__, all start methods."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=2) as js:
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

    def test_exception_midstream(self) -> None:
        """Exception propagates at the right position (first or mid-stream)."""
        cases = [
            # (fail_at, argses, values_expected_before_raise)
            (0, [(0, 0), (1, 0), (2, 0)], []),
            (2, [(0, 2), (1, 2), (2, 2), (3, 2), (4, 2)], [0, 1]),
        ]
        with Jobserver(context=FAST, slots=2) as js:
            for fail_at, argses, before in cases:
                with self.subTest(fail_at=fail_at):
                    it = js.map(
                        fn=raising_at_position,
                        argses=argses,
                        timeout=TIMEOUT,
                    )
                    for expected in before:
                        self.assertEqual(next(it), expected)
                    with self.assertRaises(ValueError):
                        next(it)

    def test_exception_with_chunksize(self) -> None:
        """Exception within a chunk fails the entire chunk."""
        with Jobserver(context=FAST, slots=2) as js:
            it = js.map(
                fn=raising_at_position,
                argses=[(0, 2), (1, 2), (2, 2), (3, 2)],
                chunksize=3,
                timeout=TIMEOUT,
            )
            with self.assertRaises(ValueError):
                next(it)

    # ---- Argument validation ----

    def test_non_iterable_argses_element_raises(self) -> None:
        """map() raises TypeError for non-iterable argses elements."""
        with Jobserver(context=FAST, slots=2) as js:
            with self.assertRaises(TypeError) as cm:
                list(js.map(fn=len, argses=[(1,), 42, (3,)]))
            self.assertIn("argses[1]", str(cm.exception))

    def test_non_mapping_kwargses_element_raises(self) -> None:
        """map() raises TypeError for non-mapping kwargses elements."""
        with Jobserver(context=FAST, slots=2) as js:
            with self.assertRaises(TypeError) as cm:
                list(
                    js.map(
                        fn=_kw_sum,
                        argses=[(1,), (2,)],
                        kwargses=[{"b": 2}, "bad"],
                    )
                )
            self.assertIn("kwargses[1]", str(cm.exception))

    # ---- Length mismatch ----

    def test_mismatched_lengths_raises(self) -> None:
        """map() raises ValueError eagerly for mismatched-length inputs."""
        with Jobserver(context=FAST, slots=1) as js:
            with self.assertRaises(ValueError) as cm:
                js.map(
                    fn=_kw_sum,
                    argses=[(1,), (2,), (3,)],
                    kwargses=[{"b": 10}],
                )
            self.assertIn("3", str(cm.exception))
            self.assertIn("1", str(cm.exception))

    # ---- Saturation ----

    def test_many_items_few_slots(self) -> None:
        """Many work items with few slots completes without deadlock."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=2) as js:
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
