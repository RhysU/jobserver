# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Jobserver worker process behavior.

Covers the per-submission child process: abnormal exits and signal
delivery, environment customization via env and preexec_fn, sleep_fn
scheduling control, and propagation of __init__ defaults to submit().
"""

import functools
import itertools
import os
import signal
import sys
import time
import typing
import unittest
from multiprocessing import get_context
from multiprocessing.reduction import ForkingPickler

from jobserver import (
    Blocked,
    Jobserver,
    SubmissionDied,
)
from jobserver._jobserver import (
    ExceptionWrapper,
    ResultWrapper,
    _worker_entrypoint,
)
from jobserver._queue import SPSCQueue

from .helpers import (
    helper_callback,
    helper_current_process_name,
    helper_nonblocking,
    helper_noop,
    helper_preexec_cm,
    helper_preexec_fn,
    helper_preexec_suppressing_cm,
    helper_raise,
    helper_return,
    helper_return_connection,
    helper_return_kwargs,
    start_methods,
)


class TestJobserverWorker(unittest.TestCase):
    """Jobserver worker process behavior."""

    def test_submission_died(self) -> None:
        """Signal receipt by worker can be detected via Future?"""
        for method in start_methods():
            with self.subTest(method=method):
                # Permit observing callback side-effects
                mutable = [0, 0, 0, 0, 0]

                # Prepare jobs with workers possibly receiving signals
                context = get_context(method)
                with Jobserver(context=context, slots=2) as js:
                    rsig = signal.raise_signal
                    f = js.submit(fn=rsig, args=(signal.SIGKILL,))
                    f.when_done(helper_callback, mutable, 0, 2)
                    g = js.submit(fn=rsig, args=(signal.SIGTERM,))
                    g.when_done(helper_callback, mutable, 1, 3)
                    h = js.submit(fn=rsig, args=(signal.SIGUSR1,))
                    h.when_done(helper_callback, mutable, 2, 5)
                    i = js.submit(fn=len, args=(("The jig is up!",)))
                    i.when_done(helper_callback, mutable, 3, 7)
                    j = js.submit(fn=rsig, args=(signal.SIGUSR2,))
                    j.when_done(helper_callback, mutable, 4, 11)

                    # Confirm completion/callbacks correct even when
                    # submissions die
                    self.assertTrue(f.wait())
                    self.assertTrue(g.wait())
                    self.assertTrue(h.wait())
                    self.assertTrue(i.wait())
                    self.assertTrue(j.wait())
                    self.assertEqual([2, 3, 5, 7, 11], mutable)

                    # Confirm results or signals in reverse order
                    with self.assertRaises(SubmissionDied):
                        j.result()
                    self.assertEqual(14, i.result())
                    with self.assertRaises(SubmissionDied):
                        h.result()
                    with self.assertRaises(SubmissionDied):
                        g.result()
                    with self.assertRaises(SubmissionDied):
                        f.result()

    def test_base_exception_surfaces_as_submission_died(self) -> None:
        """BaseException subclasses escaping fn surface as SubmissionDied.

        sys.exit(1) raises SystemExit and helper_raise(KeyboardInterrupt)
        raises KeyboardInterrupt – both are BaseException subclasses that
        fall outside the ``except Exception:`` guard in _worker_entrypoint().
        The worker catches them via ``except BaseException``, sends a
        SubmissionDied carrying the cause's traceback, then re-raises for
        normal teardown.  The parent sees SubmissionDied whose __cause__ is a
        RemoteTraceback naming the originating type (see #167).
        """
        # fn, args, substring expected in the propagated child traceback.
        base_exceptions: list[tuple] = [
            (sys.exit, (1,), "SystemExit"),
            (helper_raise, (KeyboardInterrupt,), "KeyboardInterrupt"),
        ]
        for method in start_methods():
            for fn, args, expected in base_exceptions:
                with self.subTest(method=method, fn=fn.__name__):
                    with Jobserver(context=method, slots=1) as js:
                        f = js.submit(fn=fn, args=args, timeout=5)
                        with self.assertRaises(SubmissionDied) as ctx:
                            f.result(timeout=5)
                    # The enriched path attaches the child's traceback as a
                    # cause; a silent death would leave __cause__ as None.
                    cause = ctx.exception.__cause__
                    self.assertIsNotNone(cause)
                    self.assertIn(expected, str(cause))

    def test_os_exit_becomes_submission_died(self) -> None:
        """os._exit() in worker produces a bare, causeless SubmissionDied.

        Unlike SystemExit/KeyboardInterrupt, os._exit() raises nothing the
        worker can catch, so no cause is sent and the parent falls back to
        EOFError -> SubmissionDied with __cause__ None (the silent-death
        path that distinguishes it from the enriched #167 case).
        """
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    f = js.submit(fn=os._exit, args=(1,), timeout=5)
                    with self.assertRaises(SubmissionDied) as ctx:
                        f.result(timeout=5)
                self.assertIsNone(ctx.exception.__cause__)

    def test_done_signal_terminates(self) -> None:
        """Future.wait(..., signal=...) accepts Signals enum and int forms."""
        for method, sig in itertools.product(
            start_methods(), (signal.SIGTERM, int(signal.SIGTERM))
        ):
            with self.subTest(method=method, sig=sig):
                with Jobserver(context=get_context(method), slots=1) as js:
                    f = js.submit(fn=time.sleep, args=(60,))
                    self.assertTrue(f.wait(timeout=None, signal=sig))
                    with self.assertRaises(SubmissionDied):
                        f.result()

    def test_done_signal_invalid_raises(self) -> None:
        """Future.wait(..., signal=invalid) raises OSError from os.kill."""
        invalid_sig = max(signal.valid_signals()) + 1
        for method in start_methods():
            with self.subTest(method=method):
                context = get_context(method)
                with SPSCQueue(context=context) as mq:
                    with Jobserver(context=context, slots=1) as js:
                        f = js.submit(fn=helper_nonblocking, args=(mq,))
                        try:
                            with self.assertRaises(OSError):
                                f.wait(timeout=0, signal=invalid_sig)
                        finally:
                            mq.put("cleanup")
                            f.result(timeout=10)

    def test_done_signal_after_done_is_safe(self) -> None:
        """wait(..., signal=...) on an already-completed Future is safe."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    f = js.submit(fn=len, args=((1, 2, 3),))
                    self.assertEqual(f.result(timeout=None), 3)
                    self.assertTrue(
                        f.wait(timeout=None, signal=signal.SIGTERM)
                    )
                    self.assertTrue(
                        f.wait(timeout=0, signal=int(signal.SIGTERM))
                    )

    def test_done_signal_very_short_timeout(self) -> None:
        """Future.wait(timeout=tiny, signal=...) still delivers the signal."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=get_context(method), slots=1) as js:
                    f = js.submit(fn=time.sleep, args=(60,))
                    f.wait(timeout=1e-9, signal=signal.SIGTERM)
                    self.assertTrue(f.wait(timeout=5))
                    with self.assertRaises(SubmissionDied):
                        f.result()

    def test_environ(self) -> None:
        """Confirm sub-process environment is modifiable via submit(...)."""
        # Precondition: key must not be in environment
        key = "JOBSERVER_TEST_ENVIRON"
        self.assertIsNone(os.environ.get(key, None))

        # Test observability of changes to the environment
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    # Notice f sets, g confirms unset, and h re-sets they key.
                    f = js.submit(
                        fn=os.getenv, args=(key, "SENTINEL"), env={key: "5678"}
                    )
                    g = js.submit(fn=os.getenv, args=(key, "SENTINEL"), env={})
                    h = js.submit(
                        fn=os.getenv, args=(key, "SENTINEL"), env={key: "1234"}
                    )
                    # i uses preexec_fn, not env, to set the key.
                    i = js.submit(
                        fn=os.getenv,
                        args=(key, "SENTINEL"),
                        preexec_fn=helper_preexec_fn,
                    )
                    # j uses both to confirm env updated before preexec_fn.
                    j = js.submit(
                        fn=os.getenv,
                        args=(key, "SENTINEL"),
                        preexec_fn=helper_preexec_fn,
                        env={key: "OVERWRITTEN"},
                    )
                    # k sets then unsets to check unsetting and Iterables.
                    k = js.submit(
                        fn=os.getenv,
                        args=(key, "SENTINEL"),
                        env=((key, "OVERWRITTEN"), (key, None)),
                    )
                    # Variable l is skipped because flake8 complains otherwise
                    # Lastly, m confirms removal of a possibly pre-existing key
                    m = js.submit(
                        fn=os.getenv,
                        args=(key, "SENTINEL"),
                        env=((key, None),),
                    )
                    # Checking the various results in an arbitrary order
                    self.assertEqual("PREEXEC_FN", j.result())
                    self.assertEqual("SENTINEL", m.result())
                    self.assertEqual("PREEXEC_FN", i.result())
                    self.assertEqual("1234", h.result())
                    self.assertEqual("SENTINEL", g.result())
                    self.assertEqual("5678", f.result())
                    self.assertEqual("SENTINEL", k.result())

    def test_worker_process_name(self) -> None:
        """Worker process name is 'Jobserver-worker'."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    f = js.submit(fn=helper_current_process_name)
                    self.assertEqual("Jobserver-worker", f.result())

    def test_preexec_fn_exception(self) -> None:
        """Exception in preexec_fn propagates through result()."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    f = js.submit(
                        fn=helper_return,
                        args=(1,),
                        preexec_fn=functools.partial(
                            helper_raise, RuntimeError, "preexec failed"
                        ),
                        timeout=5,
                    )
                    with self.assertRaises(RuntimeError):
                        f.result(timeout=5)

    def test_sleep_fn(self) -> None:
        """Confirm sleep_fn(...) invoked and handled per documentation."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    # Confirm negative/nan sleep raises ValueError, fn uncalled
                    with self.assertRaises(ValueError) as c:
                        js.submit(fn=len, sleep_fn=lambda: -1.0)
                    self.assertIn("-1.0", str(c.exception))
                    with self.assertRaises(ValueError):
                        js.submit(fn=len, sleep_fn=lambda: float("nan"))

                    # Confirm sleep_fn(...) returning zero can proceed
                    zs = iter(itertools.cycle((0, None)))

                    def sfn_zs(zs=zs):
                        return next(zs)

                    f = js.submit(fn=len, args=((1,),), sleep_fn=sfn_zs)

                    # Confirm sleep_fn(...) returning finite sleep can proceed
                    gs = iter(itertools.cycle((0.05, 0.02, None)))

                    def sfn_gs(gs=gs):
                        return next(gs)

                    g = js.submit(fn=len, args=((),), sleep_fn=sfn_gs)

                    # Confirm repeated sleeping can cause a timeout to occur.
                    # fn is never called as sleep_fn vetoes the invocation.
                    hs = iter(itertools.cycle((0.05,)))

                    def sfn_hs(hs=hs):
                        return next(hs)

                    with self.assertRaises(Blocked):
                        js.submit(fn=len, sleep_fn=sfn_hs, timeout=0.1)

                    # Confirm as expected.  Importantly, results not previously
                    # retrieved implying above submissions finalized results.
                    self.assertEqual(1, f.result())
                    self.assertEqual(0, g.result())

    def test_sleep_fn_timeout_not_overshot(self) -> None:
        """sleep_fn veto must raise Blocked once the deadline passes.

        _obtain_tokens must re-sample time.monotonic() after sleeping so the
        post-sleep deadline check is current.  A stale pre-sleep sample always
        compared less-than the deadline, allowing an extra loop iteration; if
        sleep_fn returned None on that call the free token was grabbed and work
        was submitted past the caller's timeout.
        """
        # sleep_fn vetoes once (the clamped sleep consumes the timeout budget)
        # then allows work.  With a stale check the extra iteration calls
        # sleep_fn again, gets None, grabs the free token, and submit()
        # succeeds instead of raising Blocked.
        call_count = [0]

        def sleep_fn() -> typing.Optional[float]:
            call_count[0] += 1
            if call_count[0] == 1:
                return 2.0  # veto: clamped to remaining budget
            return None  # allow: only reached with the bug

        timeout = 0.5
        with Jobserver(context="fork", slots=1) as js:
            # Free the slot before the second submit so the token is
            # available; sleep_fn alone should prevent the submission.
            f = js.submit(fn=time.sleep, args=(0.01,))
            f.result(timeout=5)

            with self.assertRaises(Blocked):
                js.submit(
                    fn=len,
                    args=((),),
                    sleep_fn=sleep_fn,
                    timeout=timeout,
                )

        self.assertEqual(
            1,
            call_count[0],
            "sleep_fn called more than once: stale monotonic allowed an "
            "extra loop iteration past the deadline",
        )

    def test_sleep_fn_raises_propagates(self) -> None:
        """Exception from sleep_fn propagates out of submit()."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    # First submission fills the slot
                    f = js.submit(fn=time.sleep, args=(0.1,), timeout=5)

                    def raises_error() -> float:
                        raise RuntimeError("sleep_fn failed")

                    with self.assertRaises(RuntimeError) as c:
                        js.submit(
                            fn=len,
                            args=((),),
                            sleep_fn=raises_error,
                            timeout=5,
                        )
                    self.assertIn("sleep_fn failed", str(c.exception))
                    f.wait(timeout=5)

    def test_init_defaults_used_by_submit(self) -> None:
        """Defaults set in __init__ apply in submit."""
        key = "JOBSERVER_TEST_ENVIRON"
        self.assertIsNone(os.environ.get(key, None))
        for method in start_methods():
            with self.subTest(method=method):
                # env: child sees key without submit() specifying it
                with Jobserver(
                    context=method, slots=1, env={key: "FROM_INIT"}
                ) as js:
                    f = js.submit(fn=os.getenv, args=(key, "SENTINEL"))
                    self.assertEqual("FROM_INIT", f.result())

                # preexec_fn: helper sets key; no repeat needed
                with Jobserver(
                    context=method,
                    slots=1,
                    preexec_fn=helper_preexec_fn,
                ) as js:
                    g = js.submit(fn=os.getenv, args=(key, "SENTINEL"))
                    self.assertEqual("PREEXEC_FN", g.result())

                # sleep_fn: a vetoing fn blocks every submit()
                with Jobserver(
                    context=method, slots=1, sleep_fn=lambda: 99.0
                ) as js:
                    with self.assertRaises(Blocked):
                        js.submit(fn=len, timeout=0.0)

    def test_submit_overrides_init_defaults(self) -> None:
        """submit() kwargs override the instance defaults set in __init__."""
        key = "JOBSERVER_TEST_ENVIRON"
        self.assertIsNone(os.environ.get(key, None))
        for method in start_methods():
            with self.subTest(method=method):
                # env override: submit-level value replaces the init default
                with Jobserver(
                    context=method, slots=1, env={key: "FROM_INIT"}
                ) as js:
                    f = js.submit(
                        fn=os.getenv,
                        args=(key, "SENTINEL"),
                        env={key: "FROM_SUBMIT"},
                    )
                    self.assertEqual("FROM_SUBMIT", f.result())

                # preexec_fn override: submit-level noop suppresses the helper
                with Jobserver(
                    context=method,
                    slots=1,
                    preexec_fn=helper_preexec_fn,
                ) as js:
                    g = js.submit(
                        fn=os.getenv,
                        args=(key, "SENTINEL"),
                        preexec_fn=helper_noop,
                    )
                    self.assertEqual("SENTINEL", g.result())

                # sleep_fn override: submit-level permissive fn unblocks work
                with Jobserver(
                    context=method, slots=1, sleep_fn=lambda: 99.0
                ) as js:
                    h = js.submit(fn=len, args=((1,),), sleep_fn=lambda: None)
                    self.assertEqual(1, h.result())

    def test_preexec_fn_context_manager(self) -> None:
        """preexec_fn returning a context manager wraps fn execution."""
        key = "JOBSERVER_TEST_CM"
        self.assertIsNone(os.environ.get(key, None))
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    # fn reads the env var set by __enter__
                    f = js.submit(
                        fn=os.getenv,
                        args=(key, "SENTINEL"),
                        preexec_fn=helper_preexec_cm,
                        timeout=5,
                    )
                    self.assertEqual("ENTERED", f.result(timeout=5))

    def test_preexec_fn_context_manager_on_exception(self) -> None:
        """Context manager __exit__ runs even when fn raises."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    f = js.submit(
                        fn=helper_raise,
                        args=(RuntimeError, "boom"),
                        preexec_fn=helper_preexec_cm,
                        timeout=5,
                    )
                    with self.assertRaises(RuntimeError):
                        f.result(timeout=5)

    def test_preexec_fn_context_manager_suppresses(self) -> None:
        """Context manager __exit__ may suppress exceptions; result is None."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    f = js.submit(
                        fn=helper_raise,
                        args=(RuntimeError, "suppressed"),
                        preexec_fn=helper_preexec_suppressing_cm,
                        timeout=5,
                    )
                    self.assertIsNone(f.result(timeout=5))

    def test_preexec_fn_cm_as_init_default(self) -> None:
        """Context manager factory as __init__ default applies to all."""
        key = "JOBSERVER_TEST_CM"
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(
                    context=method, slots=1, preexec_fn=helper_preexec_cm
                ) as js:
                    f = js.submit(
                        fn=os.getenv, args=(key, "SENTINEL"), timeout=5
                    )
                    self.assertEqual("ENTERED", f.result(timeout=5))

    def test_preexec_fn_cm_override_at_submit(self) -> None:
        """submit-level preexec_fn overrides init-level context manager."""
        key = "JOBSERVER_TEST_CM"
        for method in start_methods():
            with self.subTest(method=method):
                # Init sets context manager; submit overrides with callable
                with Jobserver(
                    context=method, slots=1, preexec_fn=helper_preexec_cm
                ) as js:
                    f = js.submit(
                        fn=os.getenv,
                        args=(key, "SENTINEL"),
                        preexec_fn=helper_noop,
                        timeout=5,
                    )
                    self.assertEqual("SENTINEL", f.result(timeout=5))


def _make_unpicklable_result() -> object:
    """Return a locally-defined class instance that cannot be pickled."""

    class Local:
        pass

    return Local()


class _RecordingSend:
    """Stand-in for a worker's pipe end recording send_bytes payloads.

    When fail_with is provided, send_bytes raises it to emulate a
    misbehaving Connection substitute rather than a pickle failure.
    """

    def __init__(
        self, fail_with: typing.Optional[BaseException] = None
    ) -> None:
        self.payloads: list[bytes] = []
        self.closed = False
        self._fail_with = fail_with

    def send_bytes(self, payload: typing.Any) -> None:
        if self._fail_with is not None:
            raise self._fail_with
        # ForkingPickler.dumps returns a reusable buffer view; copy it.
        self.payloads.append(bytes(payload))

    def close(self) -> None:
        self.closed = True


class TestWorkerEntrypointPickleFallback(unittest.TestCase):
    """_worker_entrypoint pre-flights the pickle so only genuine
    serialization failures hit the not-picklable fallback (see #284)."""

    def test_send_error_propagates_not_rewrapped(self) -> None:
        """An error raised by send_bytes itself surfaces unchanged.

        Before #284 the broad except caught the AttributeError from a
        misbehaving Connection and misattributed it as "not picklable".
        The pre-flight now confines the catch to serialization, so the
        genuine error propagates and no fallback re-send is attempted.
        """
        boom = AttributeError("misbehaving connection")
        send = _RecordingSend(fail_with=boom)
        with self.assertRaises(AttributeError) as ctx:
            _worker_entrypoint(send, {}, lambda: None, len, ((1, 2, 3),), {})
        self.assertIs(boom, ctx.exception)
        # No "not picklable" rewrap was sent in place of the real error.
        self.assertEqual([], send.payloads)

    def test_picklable_result_sent_unchanged(self) -> None:
        """A picklable result rides the happy path to a single send."""
        send = _RecordingSend()
        _worker_entrypoint(send, {}, lambda: None, len, ((1, 2, 3),), {})
        self.assertEqual(1, len(send.payloads))
        wrapper = ForkingPickler.loads(send.payloads[0])
        self.assertIsInstance(wrapper, ResultWrapper)
        self.assertEqual(3, wrapper.unwrap())
        self.assertTrue(send.closed)

    def test_unpicklable_result_uses_fallback(self) -> None:
        """A genuinely unpicklable result still degrades to the fallback."""
        send = _RecordingSend()
        _worker_entrypoint(
            send, {}, lambda: None, _make_unpicklable_result, (), {}
        )
        self.assertEqual(1, len(send.payloads))
        wrapper = ForkingPickler.loads(send.payloads[0])
        self.assertIsInstance(wrapper, ExceptionWrapper)
        with self.assertRaises(RuntimeError) as ctx:
            wrapper.unwrap()
        self.assertIn("not picklable", str(ctx.exception))
        self.assertTrue(send.closed)


class TestKwargsNameCollisions(unittest.TestCase):
    """A target's kwargs must forward verbatim for any key name, including
    those shared with the internal worker entrypoint parameters (#322)."""

    def test_entrypoint_parameter_names_forward(self) -> None:
        """fn/env/preexec_fn/send no longer collide with the entrypoint."""
        names = (
            "send",
            "env",
            "preexec_fn",
            "fn",
            "args",
            "kwargs",
            "timeout",
            "consume",
            "sleep_fn",
        )
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=2) as js:
                    for name in names:
                        with self.subTest(name=name):
                            future = js.submit(
                                fn=helper_return_kwargs,
                                kwargs={name: 1},
                            )
                            self.assertEqual({name: 1}, future.result())


class TestResultNotReconstructable(unittest.TestCase):
    """A value that pickles in the child but cannot be rebuilt in the
    parent must surface a clean library error, not a raw OS error (#303)."""

    @unittest.skipUnless(
        "fork" in start_methods(), "requires fork start method"
    )
    def test_returned_connection_does_not_leak_filenotfounderror(self) -> None:
        """Returning a live Connection pickles in the child but fails to
        rebuild in the parent; report it via RuntimeError rather than
        leaking FileNotFoundError from recv()."""
        context = get_context("fork")
        with Jobserver(context=context, slots=2) as js:
            future = js.submit(fn=helper_return_connection, args=(context,))
            with self.assertRaises(RuntimeError) as ctx:
                future.result(timeout=10)
        self.assertIn("not reconstructable", str(ctx.exception))
