# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Child-process traceback propagation across Jobserver Future.result().

Pickle does not carry __traceback__ across the pipe, so the parent's
result() must surface the child's traceback some other way (see #206).
"""

import pickle
import traceback
import typing
import unittest
from multiprocessing import get_all_start_methods, get_context

from jobserver import Jobserver, SubmissionDied
from jobserver._jobserver import ExceptionWrapper, ResultWrapper

from .helpers import helper_raise


class CustomChildError(Exception):
    """Module-level (hence picklable) custom exception subclass."""


def helper_raise_local() -> typing.NoReturn:
    """Raise an exception whose type is defined locally and is unpicklable."""

    class LocallyDefinedError(Exception):
        pass

    raise LocallyDefinedError("local boom")


def helper_raise_picklable_chain() -> typing.NoReturn:
    """Raise RuntimeError chained from a picklable ValueError cause."""
    try:
        helper_raise(ValueError, "inner-picklable")
    except ValueError as e:
        raise RuntimeError("outer-picklable") from e


def helper_raise_unpicklable_cause() -> typing.NoReturn:
    """Raise picklable RuntimeError chained from a locally-defined cause."""

    class LocalCause(Exception):
        pass

    try:
        raise LocalCause("inner-local")
    except LocalCause as e:
        raise RuntimeError("outer-picklable") from e


def helper_raise_unpicklable_outer() -> typing.NoReturn:
    """Raise a locally-defined exception chained from a picklable cause."""

    class LocalOuter(Exception):
        pass

    try:
        helper_raise(ValueError, "inner-picklable")
    except ValueError as e:
        raise LocalOuter("outer-local") from e


def _wrap_live_exception() -> ExceptionWrapper:
    """Return an ExceptionWrapper around an exception with a live tb."""
    try:
        helper_raise(ZeroDivisionError, "boom")
    except Exception as e:
        return ExceptionWrapper(e)
    raise AssertionError("unreachable")


class TestJobserverTraceback(unittest.TestCase):
    """Future.result() preserves the child's traceback (see #206)."""

    def test_traceback_includes_child_frames(self) -> None:
        """Rendered traceback names the child's failing frame."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                with Jobserver(context=get_context(method), slots=1) as js:
                    f = js.submit(
                        fn=helper_raise,
                        args=(ZeroDivisionError, "division by zero"),
                        timeout=None,
                    )
                    with self.assertRaises(ZeroDivisionError) as ctx:
                        f.result()
                    rendered = "".join(
                        traceback.format_exception(
                            type(ctx.exception),
                            ctx.exception,
                            ctx.exception.__traceback__,
                        )
                    )
                    self.assertIn("helper_raise", rendered)
                    self.assertIn("helpers.py", rendered)
                    self.assertIn("division by zero", rendered)

    def test_traceback_custom_picklable_type(self) -> None:
        """A module-level custom exception type round-trips with its trace."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                with Jobserver(context=get_context(method), slots=1) as js:
                    f = js.submit(
                        fn=helper_raise,
                        args=(CustomChildError, "custom boom"),
                        timeout=None,
                    )
                    with self.assertRaises(CustomChildError) as ctx:
                        f.result()
                    rendered = "".join(
                        traceback.format_exception(
                            type(ctx.exception),
                            ctx.exception,
                            ctx.exception.__traceback__,
                        )
                    )
                    self.assertIn("helper_raise", rendered)
                    self.assertIn("custom boom", rendered)

    def test_traceback_unpicklable_exception_type(self) -> None:
        """An unpicklable child exception still surfaces a useful traceback."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                with Jobserver(context=get_context(method), slots=1) as js:
                    f = js.submit(fn=helper_raise_local, timeout=None)
                    with self.assertRaises(Exception) as ctx:
                        f.result()
                    rendered = "".join(
                        traceback.format_exception(
                            type(ctx.exception),
                            ctx.exception,
                            ctx.exception.__traceback__,
                        )
                    )
                    self.assertIn("LocallyDefinedError", rendered)
                    self.assertIn("local boom", rendered)
                    self.assertIn("helper_raise_local", rendered)

    def test_traceback_picklable_chain(self) -> None:
        """Both layers of a picklable cause chain render in the parent."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                with Jobserver(context=get_context(method), slots=1) as js:
                    f = js.submit(
                        fn=helper_raise_picklable_chain, timeout=None
                    )
                    with self.assertRaises(RuntimeError) as ctx:
                        f.result()
                    rendered = "".join(
                        traceback.format_exception(
                            type(ctx.exception),
                            ctx.exception,
                            ctx.exception.__traceback__,
                        )
                    )
                    self.assertIn("ValueError", rendered)
                    self.assertIn("inner-picklable", rendered)
                    self.assertIn("outer-picklable", rendered)
                    self.assertIn("direct cause", rendered)

    def test_traceback_unpicklable_cause(self) -> None:
        """A locally-defined cause still renders despite being unpicklable."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                with Jobserver(context=get_context(method), slots=1) as js:
                    f = js.submit(
                        fn=helper_raise_unpicklable_cause, timeout=None
                    )
                    with self.assertRaises(RuntimeError) as ctx:
                        f.result()
                    rendered = "".join(
                        traceback.format_exception(
                            type(ctx.exception),
                            ctx.exception,
                            ctx.exception.__traceback__,
                        )
                    )
                    self.assertIn("LocalCause", rendered)
                    self.assertIn("inner-local", rendered)
                    self.assertIn("outer-picklable", rendered)
                    self.assertIn("direct cause", rendered)

    def test_traceback_unpicklable_outer_picklable_cause(self) -> None:
        """A locally-defined outer's chain renders through the fallback."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                with Jobserver(context=get_context(method), slots=1) as js:
                    f = js.submit(
                        fn=helper_raise_unpicklable_outer, timeout=None
                    )
                    with self.assertRaises(RuntimeError) as ctx:
                        f.result()
                    rendered = "".join(
                        traceback.format_exception(
                            type(ctx.exception),
                            ctx.exception,
                            ctx.exception.__traceback__,
                        )
                    )
                    self.assertIn("ValueError", rendered)
                    self.assertIn("inner-picklable", rendered)
                    self.assertIn("LocalOuter", rendered)
                    self.assertIn("outer-local", rendered)
                    self.assertIn("direct cause", rendered)
                    # Fallback wraps the unpicklable outer as RuntimeError.
                    self.assertIn("not picklable", rendered)


class TestExceptionWrapperPickle(unittest.TestCase):
    """ExceptionWrapper pickle round-trips are idempotent (see #206)."""

    def _round_trip(self, w: ExceptionWrapper) -> ExceptionWrapper:
        return pickle.loads(pickle.dumps(w))

    def _assert_idempotent(self, w: ExceptionWrapper) -> None:
        """Two consecutive round-trips preserve _raised_tb and unwrap()."""
        once = self._round_trip(w)
        twice = self._round_trip(once)
        self.assertEqual(w._raised_tb, once._raised_tb)
        self.assertEqual(once._raised_tb, twice._raised_tb)
        self.assertEqual(type(w._raised), type(twice._raised))
        self.assertEqual(w._raised.args, twice._raised.args)
        with self.assertRaises(type(w._raised)) as ctx_once:
            once.unwrap()
        with self.assertRaises(type(w._raised)) as ctx_twice:
            twice.unwrap()
        if w._raised_tb:
            self.assertIsNotNone(ctx_once.exception.__cause__)
            self.assertEqual(
                str(ctx_once.exception.__cause__),
                str(ctx_twice.exception.__cause__),
            )
            self.assertEqual(str(ctx_once.exception.__cause__), w._raised_tb)
        else:
            self.assertIsNone(ctx_twice.exception.__cause__)

    def test_round_trip_with_live_traceback(self) -> None:
        """Wrapping a freshly-raised exception captures and survives."""
        w = _wrap_live_exception()
        self.assertIn("helper_raise", w._raised_tb)
        self._assert_idempotent(w)

    def test_round_trip_without_traceback(self) -> None:
        """Parent-built wrappers have no tb; round-trips stay empty."""
        w = ExceptionWrapper(SubmissionDied())
        self.assertEqual(w._raised_tb, "")
        self._assert_idempotent(w)

    def test_round_trip_cause_result_wrapper(self) -> None:
        """A ResultWrapper cause contributes no tb; round-trips stay empty."""
        w = ExceptionWrapper(RuntimeError("fallback"), cause=ResultWrapper(42))
        self.assertEqual(w._raised_tb, "")
        self._assert_idempotent(w)

    def test_round_trip_cause_exception_wrapper(self) -> None:
        """An ExceptionWrapper cause donates its tb; round-trips preserve."""
        inner = _wrap_live_exception()
        w = ExceptionWrapper(RuntimeError("fallback"), cause=inner)
        self.assertEqual(w._raised_tb, inner._raised_tb)
        self.assertIn("helper_raise", w._raised_tb)
        self._assert_idempotent(w)

    def test_round_trip_cause_exception_wrapper_after_pickle(self) -> None:
        """Cause donation works after the cause has already round-tripped."""
        inner = self._round_trip(_wrap_live_exception())
        w = ExceptionWrapper(RuntimeError("fallback"), cause=inner)
        self.assertEqual(w._raised_tb, inner._raised_tb)
        self._assert_idempotent(w)
