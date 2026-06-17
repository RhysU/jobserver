# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Jobserver worker process behavior.

Covers the per-submission child process: abnormal exits and signal
delivery, environment customization via revise_env and replace_preexec,
replace_sleep scheduling control, and how derived handles apply in submit().
"""

import errno
import functools
import itertools
import os
import signal
import sys
import threading
import time
import typing
import unittest
from multiprocessing import get_context
from multiprocessing.reduction import ForkingPickler

from jobserver import (
    Blocked,
    Jobserver,
    LostResult,
)
from jobserver._jobserver import (
    ExceptionWrapper,
    ResultWrapper,
    _worker_entrypoint,
)
from jobserver._queue import SPSCQueue

from .helpers import (
    TIMEOUT,
    helper_callback,
    helper_close_own_pipe,
    helper_current_process_name,
    helper_exec_grandchild_then_die,
    helper_fork_orphan_then_die,
    helper_make_circular,
    helper_nonblocking,
    helper_noop,
    helper_preexec_cm,
    helper_preexec_fn,
    helper_preexec_suppressing_cm,
    helper_raise,
    helper_return,
    helper_return_connection,
    helper_return_kwargs,
    helper_return_raise_on_unpickle,
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
                with Jobserver(context=method, slots=2) as js:
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
                    with self.assertRaises(LostResult):
                        j.result()
                    self.assertEqual(14, i.result())
                    with self.assertRaises(LostResult):
                        h.result()
                    with self.assertRaises(LostResult):
                        g.result()
                    with self.assertRaises(LostResult):
                        f.result()

    def test_base_exception_surfaces_as_submission_died(self) -> None:
        """BaseException subclasses escaping fn surface as LostResult.

        sys.exit(1) raises SystemExit and helper_raise(KeyboardInterrupt)
        raises KeyboardInterrupt – both are BaseException subclasses that
        fall outside the ``except Exception:`` guard in _worker_entrypoint().
        The worker catches them via ``except BaseException``, sends a
        LostResult carrying the cause's traceback, then re-raises for
        normal teardown.  The parent sees LostResult whose __cause__ is a
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
                        with self.assertRaises(LostResult) as ctx:
                            f.result(timeout=5)
                    # The enriched path attaches the child's traceback as a
                    # cause; a silent death would leave __cause__ as None.
                    cause = ctx.exception.__cause__
                    self.assertIsNotNone(cause)
                    self.assertIn(expected, str(cause))

    def test_os_exit_becomes_submission_died(self) -> None:
        """os._exit() in worker produces a bare, causeless LostResult.

        Unlike SystemExit/KeyboardInterrupt, os._exit() raises nothing the
        worker can catch, so no cause is sent and the parent falls back to
        EOFError -> LostResult with __cause__ None (the silent-death
        path that distinguishes it from the enriched #167 case).
        """
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    f = js.submit(fn=os._exit, args=(1,), timeout=5)
                    with self.assertRaises(LostResult) as ctx:
                        f.result(timeout=5)
                self.assertIsNone(ctx.exception.__cause__)

    def test_open_result_pipe_keeps_future_undetermined(self) -> None:
        """An open result pipe leaves a Future undetermined, not dead (#328).

        A worker forks a grandchild that inherits the result-pipe write end,
        then exits without sending a result.  Per the pipe-as-contract model
        the result is merely *undetermined* while that write end stays open:
        the worker's exit must NOT manufacture a death, so wait(timeout)
        returns False within its deadline (rather than hanging in recv() or
        reporting LostResult).  Only once the orphan is reaped does the
        pipe reach EOF and the death legitimately surface.

        wait() runs on a daemon thread so a regression to an unbounded recv()
        shows up as the thread failing to finish rather than wedging the suite.
        """
        for method in start_methods():
            with self.subTest(method=method):
                # The grandchild reports its pid here so the test can reap it.
                with SPSCQueue(context=method) as q:
                    with Jobserver(context=method, slots=1) as js:
                        f = js.submit(
                            fn=helper_fork_orphan_then_die, args=(q,)
                        )
                        # Block until the orphan exists and holds the pipe.
                        orphan_pid = q.get(timeout=TIMEOUT)
                        returned = threading.Event()
                        out: list = []

                        # Bind loop vars as defaults so the closure captures
                        # this iteration's objects (ruff B023).
                        def _waiter(f=f, ev=returned, sink=out) -> None:
                            sink.append(f.wait(timeout=0.5))
                            ev.set()

                        t = threading.Thread(target=_waiter, daemon=True)
                        t.start()
                        try:
                            self.assertTrue(
                                returned.wait(TIMEOUT),
                                "wait() ignored its timeout",
                            )
                            # Undetermined: a finite wait returns False, with
                            # no death synthesized from the worker's exit.
                            self.assertEqual(out, [False])
                        finally:
                            # Reaping the orphan closes the last write end so
                            # the pipe reaches EOF (also unwedges any hang).
                            try:
                                os.kill(orphan_pid, signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                        # Pipe now EOFs: the death legitimately surfaces.
                        with self.assertRaises(LostResult):
                            f.result(timeout=TIMEOUT)

    def test_exec_grandchild_does_not_pin_result_pipe(self) -> None:
        """An exec'd grandchild does not hold the result pipe open (#368).

        A worker spawns a grandchild via subprocess.Popen (which exec()s) and
        exits without sending a result.  With FD_CLOEXEC set on the write end,
        exec() closes the fd in the grandchild, the pipe reaches EOF when the
        worker exits, and the Future completes promptly -- in contrast to the
        bare-fork case in test_open_result_pipe_keeps_future_undetermined.
        """
        for method in start_methods():
            with self.subTest(method=method):
                with SPSCQueue(context=method) as q:
                    with Jobserver(context=method, slots=1) as js:
                        f = js.submit(
                            fn=helper_exec_grandchild_then_die, args=(q,)
                        )
                        grandchild_pid = q.get(timeout=TIMEOUT)
                        try:
                            # The pipe must reach EOF promptly; the exec'd
                            # grandchild must not hold the write end open.
                            with self.assertRaises(LostResult):
                                f.result(timeout=TIMEOUT)
                        finally:
                            try:
                                os.kill(grandchild_pid, signal.SIGKILL)
                                os.waitpid(grandchild_pid, 0)
                            except (ProcessLookupError, ChildProcessError):
                                pass

    def test_done_signal_terminates(self) -> None:
        """Future.wait(..., signal=...) accepts Signals enum and int forms."""
        for method, sig in itertools.product(
            start_methods(), (signal.SIGTERM, int(signal.SIGTERM))
        ):
            with self.subTest(method=method, sig=sig):
                with Jobserver(context=method, slots=1) as js:
                    f = js.submit(fn=time.sleep, args=(60,))
                    self.assertTrue(f.wait(timeout=None, signal=sig))
                    with self.assertRaises(LostResult):
                        f.result()

    def test_done_signal_invalid_raises(self) -> None:
        """Future.wait(..., signal=invalid) raises OSError from os.kill."""
        invalid_sig = max(signal.valid_signals()) + 1
        for method in start_methods():
            with self.subTest(method=method):
                with SPSCQueue(context=method) as mq:
                    with Jobserver(context=method, slots=1) as js:
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
                with Jobserver(context=method, slots=1) as js:
                    f = js.submit(fn=time.sleep, args=(60,))
                    f.wait(timeout=1e-9, signal=signal.SIGTERM)
                    self.assertTrue(f.wait(timeout=5))
                    with self.assertRaises(LostResult):
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
                    f = js.revise_env({key: "5678"}).submit(
                        fn=os.getenv, args=(key, "SENTINEL")
                    )
                    g = js.revise_env({}).submit(
                        fn=os.getenv, args=(key, "SENTINEL")
                    )
                    h = js.revise_env({key: "1234"}).submit(
                        fn=os.getenv, args=(key, "SENTINEL")
                    )
                    # i uses replace_preexec, not env, to set the key.
                    i = js.replace_preexec(helper_preexec_fn).submit(
                        fn=os.getenv,
                        args=(key, "SENTINEL"),
                    )
                    # j uses both to confirm env updated before preexec.
                    j = (
                        js.revise_env({key: "OVERWRITTEN"})
                        .replace_preexec(helper_preexec_fn)
                        .submit(
                            fn=os.getenv,
                            args=(key, "SENTINEL"),
                        )
                    )
                    # k sets then unsets to check unsetting and Iterables.
                    k = js.revise_env(
                        ((key, "OVERWRITTEN"), (key, None))
                    ).submit(
                        fn=os.getenv,
                        args=(key, "SENTINEL"),
                    )
                    # Variable l is skipped because flake8 complains otherwise
                    # Lastly, m confirms removal of a possibly pre-existing key
                    m = js.revise_env(((key, None),)).submit(
                        fn=os.getenv,
                        args=(key, "SENTINEL"),
                    )
                    # Checking the various results in an arbitrary order
                    self.assertEqual("PREEXEC_FN", j.result())
                    self.assertEqual("SENTINEL", m.result())
                    self.assertEqual("PREEXEC_FN", i.result())
                    self.assertEqual("1234", h.result())
                    self.assertEqual("SENTINEL", g.result())
                    self.assertEqual("5678", f.result())
                    self.assertEqual("SENTINEL", k.result())

    def test_revise_env_stacks(self) -> None:
        """Successive revise_env(...) calls accumulate; None unsets a prior."""
        a = "JOBSERVER_TEST_ENVIRON_A"
        b = "JOBSERVER_TEST_ENVIRON_B"
        self.assertIsNone(os.environ.get(a, None))
        self.assertIsNone(os.environ.get(b, None))
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    # Two stacked revise_env calls both take effect.
                    both = js.revise_env({a: "1"}).revise_env({b: "2"})
                    f = both.submit(fn=os.getenv, args=(a, "SENTINEL"))
                    g = both.submit(fn=os.getenv, args=(b, "SENTINEL"))
                    # A later None unsets an entry set by an earlier call.
                    undo = js.revise_env({a: "1"}).revise_env({a: None})
                    h = undo.submit(fn=os.getenv, args=(a, "SENTINEL"))
                    self.assertEqual("1", f.result())
                    self.assertEqual("2", g.result())
                    self.assertEqual("SENTINEL", h.result())
                    # Stored env is canonical: one entry per key, None kept.
                    self.assertEqual({a: "1", b: "2"}, both._envdiff)
                    self.assertEqual({a: None}, undo._envdiff)

    def test_revise_env_validates_content(self) -> None:
        """revise_env(...) rejects empty, '=', and null keys/values (#391)."""
        with Jobserver(slots=1) as js:
            # Keys and values must be str or None, respectively.
            with self.assertRaises(TypeError):
                js.revise_env({1: "value"})  # type: ignore[dict-item]
            with self.assertRaises(TypeError):
                js.revise_env({"KEY": 1})  # type: ignore[dict-item]
            # Reject exactly the content the worker's os.environ assignment
            # rejects: confirm each pair fails there before requiring it of
            # revise_env(), so we promise no behavior the worker would not.
            for key, value in (
                ("", "value"),
                ("A=B", "value"),
                ("A\0B", "value"),
                ("KEY", "va\0lue"),
            ):
                with self.assertRaises((OSError, ValueError)):
                    os.environ[key] = value  # what the worker would attempt
                with self.assertRaises(ValueError):
                    js.revise_env({key: value})
            # A None value sidesteps the value check entirely.
            self.assertEqual(
                {"KEY": None}, js.revise_env({"KEY": None})._envdiff
            )
            # An ordinary key/value pair is accepted unchanged.
            self.assertEqual(
                {"KEY": "value"}, js.revise_env({"KEY": "value"})._envdiff
            )

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
                    f = js.replace_preexec(
                        functools.partial(
                            helper_raise, RuntimeError, "preexec failed"
                        )
                    ).submit(
                        fn=helper_return,
                        args=(1,),
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
                        js.replace_sleep(lambda: -1.0).submit(fn=len)
                    self.assertIn("-1.0", str(c.exception))
                    with self.assertRaises(ValueError):
                        js.replace_sleep(lambda: float("nan")).submit(fn=len)
                    # A non-numeric return is a clean ValueError, not a raw
                    # TypeError from the `>= 0.0` comparison (#333).
                    with self.assertRaises(ValueError) as c:
                        js.replace_sleep(lambda: "soon").submit(fn=len)
                    self.assertIn("'soon'", str(c.exception))
                    # bool is ruled out even though it is an int subclass.
                    with self.assertRaises(ValueError):
                        js.replace_sleep(lambda: True).submit(fn=len)

                    # Confirm sleep_fn(...) returning zero can proceed
                    zs = iter(itertools.cycle((0, None)))

                    def sfn_zs(zs=zs):
                        return next(zs)

                    f = js.replace_sleep(sfn_zs).submit(fn=len, args=((1,),))

                    # Confirm sleep_fn(...) returning finite sleep can proceed
                    gs = iter(itertools.cycle((0.05, 0.02, None)))

                    def sfn_gs(gs=gs):
                        return next(gs)

                    g = js.replace_sleep(sfn_gs).submit(fn=len, args=((),))

                    # Confirm repeated sleeping can cause a timeout to occur.
                    # fn is never called as sleep_fn vetoes the invocation.
                    hs = iter(itertools.cycle((0.05,)))

                    def sfn_hs(hs=hs):
                        return next(hs)

                    with self.assertRaises(Blocked):
                        js.replace_sleep(sfn_hs).submit(fn=len, timeout=0.1)

                    # Confirm as expected.  Importantly, results not previously
                    # retrieved implying above submissions finalized results.
                    self.assertEqual(1, f.result())
                    self.assertEqual(0, g.result())

    def test_sleep_fn_consume_zero(self) -> None:
        """sleep_fn gates consume == 0 work too, uniformly (#340).

        A consume == 0 submission consumes no slot but still spawns real
        work, so the admission gate must apply.
        """
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    # A vetoing gate must block consume == 0 work to timeout
                    # rather than letting it slip through ungated; fn never
                    # runs because the gate is never satisfied.
                    veto_calls = [0]

                    def veto(veto_calls=veto_calls) -> float:
                        veto_calls[0] += 1
                        return 0.05

                    with self.assertRaises(Blocked):
                        js.replace_sleep(veto).submit(
                            fn=len,
                            args=((),),
                            consume=0,
                            timeout=0.1,
                        )
                    self.assertGreater(
                        veto_calls[0],
                        0,
                        "sleep_fn never consulted for consume == 0 work",
                    )

                    # A permissive gate admits consume == 0 work and is
                    # consulted at least once on the way in.
                    allow_calls = [0]

                    def allow(allow_calls=allow_calls):
                        allow_calls[0] += 1
                        return None

                    f = js.replace_sleep(allow).submit(
                        fn=len, args=((1, 2),), consume=0
                    )
                    self.assertEqual(2, f.result(timeout=5))
                    self.assertGreater(
                        allow_calls[0],
                        0,
                        "sleep_fn never consulted for admitted consume == 0",
                    )

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
                js.replace_sleep(sleep_fn).submit(
                    fn=len,
                    args=((),),
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
                        js.replace_sleep(raises_error).submit(
                            fn=len,
                            args=((),),
                            timeout=5,
                        )
                    self.assertIn("sleep_fn failed", str(c.exception))
                    f.wait(timeout=5)

    def test_init_defaults_used_by_submit(self) -> None:
        """Controls set via replace_*() apply in submit."""
        key = "JOBSERVER_TEST_ENVIRON"
        self.assertIsNone(os.environ.get(key, None))
        for method in start_methods():
            with self.subTest(method=method):
                # env: child sees key without submit() specifying it
                with Jobserver(context=method, slots=1).revise_env(
                    {key: "FROM_INIT"}
                ) as js:
                    f = js.submit(fn=os.getenv, args=(key, "SENTINEL"))
                    self.assertEqual("FROM_INIT", f.result())

                # preexec: helper sets key; no repeat needed
                with Jobserver(context=method, slots=1).replace_preexec(
                    helper_preexec_fn
                ) as js:
                    g = js.submit(fn=os.getenv, args=(key, "SENTINEL"))
                    self.assertEqual("PREEXEC_FN", g.result())

                # sleep: a vetoing fn blocks every submit()
                with Jobserver(context=method, slots=1).replace_sleep(
                    lambda: 99.0
                ) as js:
                    with self.assertRaises(Blocked):
                        js.submit(fn=len, timeout=0.0)

    def test_submit_overrides_init_defaults(self) -> None:
        """A later replace_*() overrides an earlier one, sharing the pool."""
        key = "JOBSERVER_TEST_ENVIRON"
        self.assertIsNone(os.environ.get(key, None))
        for method in start_methods():
            with self.subTest(method=method):
                # env override: later value replaces the base handle's
                with Jobserver(context=method, slots=1).revise_env(
                    {key: "FROM_INIT"}
                ) as js:
                    f = js.revise_env({key: "FROM_SUBMIT"}).submit(
                        fn=os.getenv, args=(key, "SENTINEL")
                    )
                    self.assertEqual("FROM_SUBMIT", f.result())

                # preexec override: later noop suppresses the helper
                with Jobserver(context=method, slots=1).replace_preexec(
                    helper_preexec_fn
                ) as js:
                    g = js.replace_preexec(helper_noop).submit(
                        fn=os.getenv, args=(key, "SENTINEL")
                    )
                    self.assertEqual("SENTINEL", g.result())

                # sleep override: later permissive fn unblocks work
                with Jobserver(context=method, slots=1).replace_sleep(
                    lambda: 99.0
                ) as js:
                    h = js.replace_sleep(lambda: None).submit(
                        fn=len, args=((1,),)
                    )
                    self.assertEqual(1, h.result())

    def test_preexec_fn_context_manager(self) -> None:
        """preexec_fn returning a context manager wraps fn execution."""
        key = "JOBSERVER_TEST_CM"
        self.assertIsNone(os.environ.get(key, None))
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    # fn reads the env var set by __enter__
                    f = js.replace_preexec(helper_preexec_cm).submit(
                        fn=os.getenv,
                        args=(key, "SENTINEL"),
                        timeout=5,
                    )
                    self.assertEqual("ENTERED", f.result(timeout=5))

    def test_preexec_fn_context_manager_on_exception(self) -> None:
        """Context manager __exit__ runs even when fn raises."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    f = js.replace_preexec(helper_preexec_cm).submit(
                        fn=helper_raise,
                        args=(RuntimeError, "boom"),
                        timeout=5,
                    )
                    with self.assertRaises(RuntimeError):
                        f.result(timeout=5)

    def test_preexec_fn_context_manager_suppresses(self) -> None:
        """Context manager __exit__ may suppress exceptions; result is None."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    f = js.replace_preexec(
                        helper_preexec_suppressing_cm
                    ).submit(
                        fn=helper_raise,
                        args=(RuntimeError, "suppressed"),
                        timeout=5,
                    )
                    self.assertIsNone(f.result(timeout=5))

    def test_preexec_fn_cm_as_init_default(self) -> None:
        """Context manager factory as __init__ default applies to all."""
        key = "JOBSERVER_TEST_CM"
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1).replace_preexec(
                    helper_preexec_cm
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
                with Jobserver(context=method, slots=1).replace_preexec(
                    helper_preexec_cm
                ) as js:
                    f = js.replace_preexec(helper_noop).submit(
                        fn=os.getenv,
                        args=(key, "SENTINEL"),
                        timeout=5,
                    )
                    self.assertEqual("SENTINEL", f.result(timeout=5))


def _make_unpicklable_result() -> object:
    """Return a locally-defined class instance that cannot be pickled."""

    class Local:
        pass

    return Local()


def _make_deep_result() -> object:
    """Return a picklable list nested far deeper than the recursive pickler
    can serialize, provoking a RecursionError from ForkingPickler.dumps."""
    x: object = []
    for _ in range(sys.getrecursionlimit() * 100):
        x = [x]
    return x


class _ReduceRaisesRuntimeError(Exception):
    """An exception whose __reduce__ raises RuntimeError when pickled."""

    def __reduce__(self) -> typing.Any:
        raise RuntimeError("__reduce__ refuses to pickle")


def _raise_reduce_runtimeerror() -> object:
    """Raise an exception whose __reduce__ raises RuntimeError."""
    raise _ReduceRaisesRuntimeError()


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
        self._fd = os.open(os.devnull, os.O_WRONLY)

    def fileno(self) -> int:
        return self._fd

    def send_bytes(self, payload: typing.Any) -> None:
        if self._fail_with is not None:
            raise self._fail_with
        # ForkingPickler.dumps returns a reusable buffer view; copy it.
        self.payloads.append(bytes(payload))

    def close(self) -> None:
        self.closed = True
        os.close(self._fd)


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

    def test_deeply_nested_result_uses_fallback(self) -> None:
        """A picklable-but-too-deep result overflows the pickler with a
        RecursionError that now degrades to the descriptive fallback rather
        than crashing the worker into a misleading LostResult (#343)."""
        send = _RecordingSend()
        _worker_entrypoint(send, {}, lambda: None, _make_deep_result, (), {})
        self.assertEqual(1, len(send.payloads))
        wrapper = ForkingPickler.loads(send.payloads[0])
        self.assertIsInstance(wrapper, ExceptionWrapper)
        with self.assertRaises(RuntimeError) as ctx:
            wrapper.unwrap()
        self.assertIn("not picklable", str(ctx.exception))
        self.assertTrue(send.closed)

    def test_reduce_raising_runtimeerror_uses_fallback(self) -> None:
        """A __reduce__ raising RuntimeError uses the fallback (#390)."""
        send = _RecordingSend()
        _worker_entrypoint(
            send, {}, lambda: None, _raise_reduce_runtimeerror, (), {}
        )
        self.assertEqual(1, len(send.payloads))
        wrapper = ForkingPickler.loads(send.payloads[0])
        self.assertIsInstance(wrapper, ExceptionWrapper)
        with self.assertRaises(RuntimeError) as ctx:
            wrapper.unwrap()
        self.assertIn("not picklable", str(ctx.exception))
        self.assertTrue(send.closed)


class TestWorkerEntrypointSendBytesErrors(unittest.TestCase):
    """A broken or closed result fd is swallowed during the result-send so
    the worker closes quietly rather than escaping with a traceback and
    degrading to a misleading LostResult (#348)."""

    def test_broken_result_fd_is_swallowed(self) -> None:
        """An OSError/EBADF from a closed result fd closes quietly instead
        of crashing the worker; the parent still observes EOF and reports
        LostResult, but without a noisy child traceback."""
        boom = OSError(errno.EBADF, "Bad file descriptor")
        send = _RecordingSend(fail_with=boom)
        _worker_entrypoint(send, {}, lambda: None, len, ((1, 2, 3),), {})
        self.assertEqual([], send.payloads)
        self.assertTrue(send.closed)

    def test_broken_pipe_still_swallowed(self) -> None:
        """The original BrokenPipeError handling is preserved."""
        send = _RecordingSend(fail_with=BrokenPipeError())
        _worker_entrypoint(send, {}, lambda: None, len, ((1, 2, 3),), {})
        self.assertEqual([], send.payloads)
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
        # Keep the context object: it is handed to the worker to build a
        # Connection, so it cannot collapse to a bare start-method string.
        context = get_context("fork")
        with Jobserver(context=context, slots=2) as js:
            future = js.submit(fn=helper_return_connection, args=(context,))
            with self.assertRaises(RuntimeError) as ctx:
                future.result(timeout=10)
        self.assertIn("not reconstructable", str(ctx.exception))

    def test_worker_closes_send_pipe_yields_lost_result(self) -> None:
        """A worker that sabotages its result pipe produces LostResult."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    f = js.submit(fn=helper_close_own_pipe, timeout=5)
                    with self.assertRaises(LostResult):
                        f.result(timeout=5)

    def test_circular_reference_round_trips(self) -> None:
        """A result with circular references round-trips via pickle."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    f = js.submit(fn=helper_make_circular, timeout=5)
                    result = f.result(timeout=TIMEOUT)
                    self.assertEqual(result[0], 1)
                    self.assertEqual(result[1], 2)
                    self.assertIs(result[2], result)


class TestUnpickleInterruptSelfHeals(unittest.TestCase):
    """A KeyboardInterrupt raised by a result's __setstate__ still escapes
    Future.wait() by design: the #396 fix re-raises KeyboardInterrupt to
    preserve interactive Ctrl-C, so it cannot tell a real Ctrl-C apart from
    one smuggled in by a malicious __setstate__.  The escape orphans the
    Future and leaks its slot, but the leak must be transient: the next
    submit() reclaims the slot, leaving the Jobserver fully usable."""

    def test_slot_reclaimed_after_unpickle_keyboardinterrupt(self) -> None:
        """The single leaked slot is recovered by the following submit()."""
        for method in start_methods():
            with self.subTest(method=method):
                with Jobserver(context=method, slots=1) as js:
                    # The lone slot's worker returns a value whose
                    # __setstate__ raises KeyboardInterrupt in the parent.
                    evil = js.submit(
                        fn=helper_return_raise_on_unpickle,
                        args=(KeyboardInterrupt,),
                    )
                    with self.assertRaises(KeyboardInterrupt):
                        evil.result(timeout=TIMEOUT)

                    # The escape consumed the slot without restoring it and
                    # left evil orphaned.  Acquiring the lone slot for fresh
                    # work is only possible if submit()'s reclaim() pass
                    # self-heals the orphan (EOFError -> LostResult), firing
                    # its token-restore callback.
                    healed = js.submit(
                        fn=helper_return, args=(42,), timeout=TIMEOUT
                    )
                    self.assertEqual(42, healed.result(timeout=TIMEOUT))

                    # The orphan converges to LostResult once its drained
                    # pipe reports EOF; it never re-raises KeyboardInterrupt.
                    self.assertTrue(evil.done(timeout=TIMEOUT))
                    with self.assertRaises(LostResult):
                        evil.result(timeout=0)
