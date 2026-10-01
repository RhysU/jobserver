# Copyright (C) 2019-2026 Rhys Ulerich
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Child-process traceback propagation across Jobserver Future.result().

Pickle does not carry __traceback__ across the pipe, so the parent's
result() must surface the child's traceback some other way (see #206).
"""

import pickle
import sys
import traceback
import typing
import unittest

from jobserver import Jobserver, LostResult
from jobserver._jobserver import (
    _ExceptionWrapper,
    _RemoteTraceback,
    _ResultWrapper,
)

from .helpers import (
    FAST,
    helper_raise,
    helper_raise_custom_init,
    start_methods,
)


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


def _wrap_live_exception() -> _ExceptionWrapper:
    """Return an _ExceptionWrapper around an exception with a live tb."""
    try:
        helper_raise(ZeroDivisionError, "boom")
    except Exception as e:
        return _ExceptionWrapper(e)
    raise AssertionError("unreachable")


def _wrap_live_base_exception(klass: type, *args) -> _ExceptionWrapper:
    """Mirror _worker_entrypoint's BaseException path: raise LostResult
    from a freshly-raised control-flow BaseException, then wrap it (#167).
    """
    try:
        raise klass(*args)
    except BaseException as e:
        try:
            raise LostResult() from e
        except LostResult as died:
            return _ExceptionWrapper(died)
    raise AssertionError("unreachable")


class TestJobserverTraceback(unittest.TestCase):
    """Future.result() preserves the child's traceback (see #206)."""

    def test_traceback_includes_child_frames(self) -> None:
        """Rendered traceback names the child's failing frame."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
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
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
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
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
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
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
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
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
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
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
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
    """_ExceptionWrapper pickle round-trips are idempotent (see #206)."""

    def _round_trip(self, w: _ExceptionWrapper) -> _ExceptionWrapper:
        return pickle.loads(pickle.dumps(w))

    def _assert_idempotent(self, w: _ExceptionWrapper) -> None:
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
        w = _ExceptionWrapper(LostResult())
        self.assertEqual(w._raised_tb, "")
        self._assert_idempotent(w)

    def test_round_trip_cause_result_wrapper(self) -> None:
        """A _ResultWrapper cause contributes no tb; round-trips stay empty."""
        w = _ExceptionWrapper(
            RuntimeError("fallback"), cause=_ResultWrapper(42)
        )
        self.assertEqual(w._raised_tb, "")
        self._assert_idempotent(w)

    def test_round_trip_cause_exception_wrapper(self) -> None:
        """An _ExceptionWrapper cause donates its tb; round-trips preserve."""
        inner = _wrap_live_exception()
        w = _ExceptionWrapper(RuntimeError("fallback"), cause=inner)
        self.assertEqual(w._raised_tb, inner._raised_tb)
        self.assertIn("helper_raise", w._raised_tb)
        self._assert_idempotent(w)

    def test_round_trip_cause_exception_wrapper_after_pickle(self) -> None:
        """Cause donation works after the cause has already round-tripped."""
        inner = self._round_trip(_wrap_live_exception())
        w = _ExceptionWrapper(RuntimeError("fallback"), cause=inner)
        self.assertEqual(w._raised_tb, inner._raised_tb)
        self._assert_idempotent(w)

    def test_submission_died_chained_from_keyboard_interrupt(self) -> None:
        """LostResult raised-from KeyboardInterrupt captures both."""
        w = _wrap_live_base_exception(KeyboardInterrupt)
        self.assertIsInstance(w._raised, LostResult)
        self.assertIn("LostResult", w._raised_tb)
        self.assertIn("KeyboardInterrupt", w._raised_tb)
        self.assertIn("_wrap_live_base_exception", w._raised_tb)
        self._assert_idempotent(w)

    def test_submission_died_chained_from_system_exit(self) -> None:
        """The chained SystemExit type renders so the parent can debug."""
        w = _wrap_live_base_exception(SystemExit, 2)
        self.assertIsInstance(w._raised, LostResult)
        self.assertIn("SystemExit", w._raised_tb)
        self._assert_idempotent(w)

    def test_chained_base_exception_round_trips(self) -> None:
        """unwrap() after pickle raises LostResult whose _RemoteTraceback
        renders the chained control-flow cause."""
        w = self._round_trip(_wrap_live_base_exception(KeyboardInterrupt))
        with self.assertRaises(LostResult) as ctx:
            w.unwrap()
        self.assertIsInstance(ctx.exception.__cause__, _RemoteTraceback)
        self.assertIn("KeyboardInterrupt", str(ctx.exception.__cause__))

    def test_unwrap_traceback_grows(self) -> None:
        """Each unwrap() re-raises one instance, growing its tb."""
        w = self._round_trip(_wrap_live_exception())
        lengths = []
        for _ in range(3):
            try:
                w.unwrap()
            except ZeroDivisionError as e:
                lengths.append(len(traceback.extract_tb(e.__traceback__)))
                self.assertIsInstance(e.__cause__, _RemoteTraceback)
                self.assertEqual(w._raised_tb, str(e.__cause__))
        # Undesired: tracebacks accumulate across calls.
        self.assertEqual([2, 4, 6], lengths)

    @unittest.skipIf(sys.version_info < (3, 11), "requires add_note")
    def test_unwrap_notes_accumulate(self) -> None:
        """Each unwrap() re-raises one instance, leaking notes."""
        try:
            helper_raise(ZeroDivisionError, "boom")
        except ZeroDivisionError as e:
            e.add_note("worker")
            w = self._round_trip(_ExceptionWrapper(e))
        notes = []
        for i in range(3):
            try:
                w.unwrap()
            except ZeroDivisionError as e:
                notes.append(list(getattr(e, "__notes__", ())))
                e.add_note(f"caller {i}")
                self.assertIsInstance(e.__cause__, _RemoteTraceback)
                self.assertEqual(w._raised_tb, str(e.__cause__))
        # Undesired: caller notes leak into later calls.
        self.assertEqual([[], ["caller 0"], ["caller 0", "caller 1"]], notes)

    def test_custom_init_exception_reconstruct_failure(self) -> None:
        """An exception whose __init__ has a non-standard signature
        still surfaces usefully after pickle round-trip."""
        with Jobserver(context=FAST, slots=1) as js:
            f = js.submit(fn=helper_raise_custom_init, timeout=5)
            with self.assertRaises(Exception) as ctx:
                f.result(timeout=5)
            msg = str(ctx.exception).lower()
            self.assertIn("not reconstructable", msg)
            self.assertIn("__init__", msg)
