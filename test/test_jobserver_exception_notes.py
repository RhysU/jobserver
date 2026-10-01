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
import sys
import traceback
import typing
import unittest

from jobserver import Jobserver

from .helpers import FAST, helper_raise_made

# Note texts, distinct so that render counts cannot overlap.
_CALLER_NOTE = "caller note"
_CAUSE_GROUP_NOTE = "cause group note"
_CAUSE_NOTE = "cause note"
_CONTEXT_NOTE = "context note"
_GROUP_NOTE = "group note"
_HAND_NOTE = "hand note"
_HELD_NOTE = "held note"
_LEAF_NOTE = "leaf note"
_MEMBER_NOTE = "member note"
_NESTED_NOTE = "nested note"
_OUTER_GROUP_NOTE = "outer group note"
_OUTER_NOTE = "outer note"
_RETURNED_NOTE = "returned note"
_WORKER_NOTE = "worker note"


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


@unittest.skipIf(sys.version_info >= (3, 11), "notes render from 3.11")
class TestNotesUnrendered(unittest.TestCase):
    """Before 3.11 __notes__ is an ordinary attribute and must survive."""

    @staticmethod
    def _make_hand_noted() -> BaseException:
        e = ValueError("hand")
        e.__notes__ = [_HAND_NOTE]
        return e

    def test_hand_set_notes_survive(self) -> None:
        """A hand-set __notes__ crosses the pipe intact."""
        e = _raised(helper_raise_made, TestNotesUnrendered._make_hand_noted)
        self.assertEqual([_HAND_NOTE], e.__notes__)
