# Copyright (C) 2019-2026 Rhys Ulerich
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Exception notes raised in a worker as seen through Future.result().

The worker renders its traceback, notes included, into text before
pickling.  Notes on exceptions that also cross the pipe render again.
"""

import errno
import functools
import os
import sys
import traceback
import typing
import unittest

from jobserver import Jobserver, LostResult
from jobserver._jobserver import (
    _RAISED_NOTE,
    _ExceptionWrapper,
    _ResultWrapper,
)

from .helpers import (
    FAST,
    helper_raise_made,
    helper_return_raise_on_unpickle,
)

# Note texts, distinct so that render counts cannot overlap.
_CALLER_NOTE = "caller note"
_CAUSE_GROUP_NOTE = "cause group note"
_CAUSE_NOTE = "cause note"
_CLASS_NOTE = "class note"
_CONTEXT_NOTE = "context note"
_GROUP_NOTE = "group note"
_HAND_NOTE = "hand note"
_HELD_NOTE = "held note"
_LEAF_NOTE = "leaf note"
_MEMBER_NOTE = "member note"
_NESTED_NOTE = "nested note"
_OUTER_GROUP_NOTE = "outer group note"
_OUTER_NOTE = "outer note"
_PROPERTY_NOTE = "property note"
_RETURNED_NOTE = "returned note"
_WORKER_NOTE = "worker note"


class _PropertyNotesError(Exception):
    """Defines __notes__ as a property, which add_note(...) refuses."""

    @property
    def __notes__(self) -> tuple[str, ...]:
        return (_PROPERTY_NOTE,)


class _ClassNotesError(Exception):
    """Defines __notes__ as a tuple, which add_note(...) refuses."""

    __notes__ = (_CLASS_NOTE,)


class _RaisingAddNoteError(Exception):
    """Overrides add_note(...) to raise."""

    def add_note(self, note: str) -> None:
        raise RuntimeError(note)


class _MissingAddNoteError(Exception):
    """Hides add_note(...) entirely."""

    add_note = None


class _StrictStateError(Exception):
    """Rejects unpickled state carrying any attribute."""

    def __setstate__(self, state: dict) -> None:
        if state:
            raise ValueError(f"unexpected {sorted(state)}")


def _noted(e: BaseException, note: str) -> BaseException:
    e.add_note(note)
    return e


def _make_noted(klass: type, args: tuple) -> BaseException:
    """Build klass(*args) carrying the worker note."""
    return _noted(klass(*args), _WORKER_NOTE)


def _counts(e: BaseException, *notes: str) -> tuple[int, ...]:
    """How often each note renders in e's formatted traceback."""
    text = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    return tuple(text.count(note) for note in notes)


def _raised(
    fn: typing.Callable[..., typing.Any], *args: typing.Any
) -> BaseException:
    """Return what result() raises for fn(*args) run in a worker."""
    with Jobserver(context=FAST, slots=1) as js:
        f = js.submit(fn=fn, args=args, timeout=5)
        try:
            f.result(timeout=5)
        except Exception as e:
            return e
    raise AssertionError("result() returned")


@unittest.skipIf(sys.version_info < (3, 11), "requires add_note")
class TestNotesRendered(unittest.TestCase):
    """Worker notes render in the parent."""

    def test_single_note(self) -> None:
        """A worker note on a lone exception."""
        e = _raised(
            helper_raise_made, functools.partial(_make_noted, ValueError, ())
        )
        self.assertEqual(
            (
                (1,),
                False,
            ),
            (_counts(e, _WORKER_NOTE), hasattr(e, "__notes__")),
        )

    def test_builtin_notes(self) -> None:
        """Built-ins with and without C fields keep notes and fields."""
        # Built-in exceptions without and with C-level fields beyond args.
        for klass, args in (
            (ValueError, ("plain",)),
            (KeyError, ("key",)),
            (OSError, (errno.ENOENT, "missing", "/path")),
            (UnicodeDecodeError, ("utf-8", b"\xff", 0, 1, "bad")),
            (StopIteration, (7,)),
            (SyntaxError, ("bad", ("f.py", 3, 4, "x y"))),
        ):
            expected = klass(*args)
            with self.subTest(type=klass.__name__):
                e = _raised(
                    helper_raise_made,
                    functools.partial(_make_noted, klass, args),
                )
                self.assertEqual(
                    (
                        type(expected),
                        expected.args,
                        str(expected),
                        (1,),
                        False,
                    ),
                    (
                        type(e),
                        e.args,
                        str(e),
                        _counts(e, _WORKER_NOTE),
                        hasattr(e, "__notes__"),
                    ),
                )

    @staticmethod
    def _raise_explicit_chain() -> typing.NoReturn:
        try:
            raise _noted(KeyError("inner"), _CAUSE_NOTE)
        except KeyError as k:
            raise _noted(ValueError("outer"), _OUTER_NOTE) from k

    def test_explicit_chain_notes(self) -> None:
        """Notes on an outer exception and its explicit cause."""
        e = _raised(TestNotesRendered._raise_explicit_chain)
        self.assertEqual(
            (
                (1, 1),
                False,
            ),
            (
                _counts(e, _CAUSE_NOTE, _OUTER_NOTE),
                hasattr(e, "__notes__"),
            ),
        )

    @staticmethod
    def _raise_implicit_chain() -> typing.NoReturn:
        try:
            raise _noted(KeyError("inner"), _CONTEXT_NOTE)
        except KeyError:
            raise _noted(ValueError("outer"), _OUTER_NOTE)  # noqa: B904

    def test_implicit_chain_notes(self) -> None:
        """Notes on an outer exception and its implicit context."""
        e = _raised(TestNotesRendered._raise_implicit_chain)
        self.assertEqual(
            (
                (1, 1),
                False,
            ),
            (
                _counts(e, _CONTEXT_NOTE, _OUTER_NOTE),
                hasattr(e, "__notes__"),
            ),
        )

    @staticmethod
    def _make_group() -> BaseException:
        inner = ExceptionGroup(  # noqa: F821
            "inner",
            [_noted(TypeError("nested"), _NESTED_NOTE), OSError("plain")],
        )
        group = ExceptionGroup(  # noqa: F821
            "outer",
            [
                _noted(KeyError("member"), _MEMBER_NOTE),
                RuntimeError("plain"),
                inner,
            ],
        )
        return _noted(group, _GROUP_NOTE)

    def test_group_notes(self) -> None:
        """Noted and plain members of a group and a nested group."""
        e = _raised(helper_raise_made, TestNotesRendered._make_group)
        member, plain, inner = e.exceptions
        nested, plain_nested = inner.exceptions
        self.assertEqual(
            (
                (1, 1, 1),
                False,
                False,
                False,
                False,
            ),
            (
                _counts(e, _GROUP_NOTE, _MEMBER_NOTE, _NESTED_NOTE),
                hasattr(member, "__notes__"),
                hasattr(nested, "__notes__"),
                hasattr(plain, "__notes__"),
                hasattr(plain_nested, "__notes__"),
            ),
        )

    @staticmethod
    def _raise_group_with_group_cause() -> typing.NoReturn:
        cause = ExceptionGroup(  # noqa: F821
            "cause",
            [_noted(KeyError("leaf"), _LEAF_NOTE), OSError("plain leaf")],
        )
        try:
            try:
                raise _noted(cause, _CAUSE_GROUP_NOTE)
            except ExceptionGroup as c:  # noqa: F821
                raise _noted(ValueError("member"), _MEMBER_NOTE) from c
        except ValueError as m:
            group = ExceptionGroup(  # noqa: F821
                "outer", [m, RuntimeError("plain")]
            )
            raise _noted(group, _OUTER_GROUP_NOTE) from None

    def test_group_member_with_group_cause_notes(self) -> None:
        """Notes on a group, its member, and the member's group cause."""
        e = _raised(TestNotesRendered._raise_group_with_group_cause)
        member, plain = e.exceptions
        self.assertEqual(
            (
                (1, 1, 1, 1),
                None,
                False,
                False,
            ),
            (
                _counts(
                    e,
                    _CAUSE_GROUP_NOTE,
                    _LEAF_NOTE,
                    _OUTER_GROUP_NOTE,
                    _MEMBER_NOTE,
                ),
                member.__cause__,
                hasattr(member, "__notes__"),
                hasattr(plain, "__notes__"),
            ),
        )

    # Pathological: only "raise e from e" or direct assignment chains an
    # exception to itself.  Implicit chaining never sets such a context.
    @staticmethod
    def _raise_from_self() -> typing.NoReturn:
        e = _noted(ValueError("from self"), _WORKER_NOTE)
        raise e from e

    @staticmethod
    def _raise_own_cause() -> typing.NoReturn:
        e = _noted(ValueError("own cause"), _WORKER_NOTE)
        e.__cause__ = e
        raise e

    @staticmethod
    def _raise_own_context() -> typing.NoReturn:
        e = _noted(ValueError("own context"), _WORKER_NOTE)
        e.__context__ = e
        raise e

    def test_self_chained_notes(self) -> None:
        """An exception chained to itself neither hangs nor loses notes."""
        for fn in (
            TestNotesRendered._raise_from_self,
            TestNotesRendered._raise_own_cause,
            TestNotesRendered._raise_own_context,
        ):
            with self.subTest(fn=fn.__name__):
                e = _raised(fn)
                self.assertEqual(
                    (
                        (1,),
                        False,
                    ),
                    (_counts(e, _WORKER_NOTE), hasattr(e, "__notes__")),
                )

    @staticmethod
    def _raise_member_caused_by_group() -> typing.NoReturn:
        member = _noted(KeyError("member"), _MEMBER_NOTE)
        group = ExceptionGroup("outer", [member])  # noqa: F821
        member.__cause__ = group
        raise _noted(group, _GROUP_NOTE)

    def test_member_caused_by_its_group_notes(self) -> None:
        """A member whose cause is its own group neither hangs nor loops."""
        e = _raised(TestNotesRendered._raise_member_caused_by_group)
        (member,) = e.exceptions
        self.assertEqual(
            (
                (1, 1),
                None,
                False,
            ),
            (
                _counts(e, _GROUP_NOTE, _MEMBER_NOTE),
                member.__cause__,
                hasattr(member, "__notes__"),
            ),
        )

    @staticmethod
    def _make_holder() -> BaseException:
        held = _noted(KeyError("held"), _HELD_NOTE)
        holder = ValueError(held)
        holder.held = held
        return holder

    def test_unrendered_notes_kept(self) -> None:
        """Notes on exceptions held in args or attributes survive."""
        e = _raised(helper_raise_made, TestNotesRendered._make_holder)
        self.assertEqual(
            ([_HELD_NOTE], [_HELD_NOTE]),
            (e.args[0].__notes__, e.held.__notes__),
        )

    def test_caller_note_renders_once(self) -> None:
        """A note the parent adds renders once."""
        e = _raised(helper_raise_made, ValueError)
        self.assertEqual((1,), _counts(_noted(e, _CALLER_NOTE), _CALLER_NOTE))

    @staticmethod
    def _raise_noted_local() -> typing.NoReturn:
        class LocallyDefinedError(Exception):
            pass

        raise _noted(LocallyDefinedError("local boom"), _WORKER_NOTE)

    def test_fallback_note_renders_once(self) -> None:
        """An unpicklable noted exception arrives as RuntimeError."""
        e = _raised(TestNotesRendered._raise_noted_local)
        self.assertEqual(
            (RuntimeError, (1,), False),
            (type(e), _counts(e, _WORKER_NOTE), hasattr(e, "__notes__")),
        )

    @staticmethod
    def _make_returned() -> BaseException:
        return _noted(ValueError("returned"), _RETURNED_NOTE)

    def test_returned_exception_keeps_notes(self) -> None:
        """An exception returned as a value keeps its notes."""
        with Jobserver(context=FAST, slots=1) as js:
            f = js.submit(fn=TestNotesRendered._make_returned, timeout=5)
            self.assertEqual([_RETURNED_NOTE], f.result(timeout=5).__notes__)


@unittest.skipIf(sys.version_info < (3, 11), "requires add_note")
class TestNotesHint(unittest.TestCase):
    """One note in the worker traceback says a worker raised it."""

    def test_wrapper_notes(self) -> None:
        """Given notes render once, only when the wrapper formats a tb."""
        try:
            raise ValueError("inner")
        except ValueError as e:
            inner = _ExceptionWrapper(e)
        try:
            raise ValueError("live")
        except ValueError as e:
            live = _ExceptionWrapper(e, _CALLER_NOTE, _HAND_NOTE)
        reused = _ExceptionWrapper(
            RuntimeError("reused"), _CALLER_NOTE, cause=inner
        )
        valued = _ExceptionWrapper(
            RuntimeError("valued"), _CALLER_NOTE, cause=_ResultWrapper(0)
        )
        self.assertEqual(
            ((1, 1, 0), inner._raised_tb, "", False),
            (
                tuple(
                    live._raised_tb.count(n)
                    for n in (_CALLER_NOTE, _HAND_NOTE, _RAISED_NOTE)
                ),
                reused._raised_tb,
                valued._raised_tb,
                any(
                    hasattr(w._raised, "__notes__")
                    for w in (live, reused, valued)
                ),
            ),
        )

    @staticmethod
    def _raise_nested(depth: int) -> typing.NoReturn:
        if depth == 0:
            raise ValueError("innermost")
        with Jobserver(context=FAST, slots=1) as js:
            f = js.submit(
                fn=TestNotesHint._raise_nested, args=(depth - 1,), timeout=5
            )
            f.result(timeout=5)
        raise AssertionError("result() returned")

    def test_nested(self) -> None:
        """Each worker boundary renders one hint, which stays behind."""
        for depth in (0, 1, 2):
            with self.subTest(depth=depth):
                e = _raised(TestNotesHint._raise_nested, depth)
                self.assertEqual(
                    ((depth + 1,), False),
                    (_counts(e, _RAISED_NOTE), hasattr(e, "__notes__")),
                )

    def test_group(self) -> None:
        """A group renders the hint after its notes, before its members."""
        e = _raised(helper_raise_made, TestNotesRendered._make_group)
        text = str(e.__cause__)
        self.assertEqual(
            ((1,), True),
            (
                _counts(e, _RAISED_NOTE),
                text.index(_GROUP_NOTE)
                < text.index(_RAISED_NOTE)
                < text.index(_MEMBER_NOTE),
            ),
        )

    def test_refused(self) -> None:
        """A class refusing add_note(...) arrives intact without the hint."""
        for klass, counts in (
            (_RaisingAddNoteError, (0, 0, 0)),
            (_MissingAddNoteError, (0, 0, 0)),
            # Undesired: own notes render in the worker and again.
            (_PropertyNotesError, (0, 2, 0)),
            (_ClassNotesError, (0, 0, 2)),
        ):
            with self.subTest(type=klass.__name__):
                e = _raised(helper_raise_made, klass)
                self.assertEqual(
                    (klass, counts),
                    (
                        type(e),
                        _counts(e, _RAISED_NOTE, _PROPERTY_NOTE, _CLASS_NOTE),
                    ),
                )

    def test_strict_state(self) -> None:
        """The hint adds no attribute to the pickled state."""
        e = _raised(helper_raise_made, _StrictStateError)
        self.assertEqual(
            (_StrictStateError, (1,)),
            (type(e), _counts(e, _RAISED_NOTE)),
        )

    @staticmethod
    def _return_local() -> typing.Any:
        class LocallyDefined:
            pass

        return LocallyDefined()

    def test_other_outcomes(self) -> None:
        """Only outcomes carrying a worker traceback render the hint."""
        for fn, args, klass, count in (
            (TestNotesRendered._raise_noted_local, (), RuntimeError, 1),
            (sys.exit, (3,), LostResult, 1),
            (TestNotesHint._return_local, (), RuntimeError, 0),
            (os._exit, (0,), LostResult, 0),
            (helper_return_raise_on_unpickle, (ValueError,), RuntimeError, 0),
        ):
            with self.subTest(fn=fn.__name__):
                e = _raised(fn, *args)
                self.assertEqual(
                    (klass, (count,)),
                    (type(e), _counts(e, _RAISED_NOTE)),
                )


@unittest.skipIf(sys.version_info >= (3, 11), "notes render from 3.11")
class TestNotesUnrendered(unittest.TestCase):
    """Before 3.11 __notes__ is an ordinary attribute and must survive."""

    @staticmethod
    def _make_hand_noted() -> BaseException:
        e = ValueError("hand")
        e.__notes__ = [_HAND_NOTE]
        return e

    def test_wrapper_notes_ignored(self) -> None:
        """Without add_note(...) the wrapper renders no given notes."""
        try:
            raise ValueError("wrapped")
        except ValueError as e:
            w = _ExceptionWrapper(e, _CALLER_NOTE)
        self.assertEqual(
            (0, False),
            (
                w._raised_tb.count(_CALLER_NOTE),
                hasattr(w._raised, "__notes__"),
            ),
        )

    def test_hand_set_notes_survive(self) -> None:
        """A hand-set __notes__ crosses the pipe intact, with no hint."""
        e = _raised(helper_raise_made, TestNotesUnrendered._make_hand_noted)
        self.assertEqual(
            ([_HAND_NOTE], (0,)),
            (e.__notes__, _counts(e, _RAISED_NOTE)),
        )
