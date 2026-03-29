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
import unittest
from multiprocessing import get_all_start_methods, get_context

from jobserver import (
    Blocked,
    Jobserver,
    MinimalQueue,
    SubmissionDied,
)

from .helpers import (
    helper_callback,
    helper_current_process_name,
    helper_nonblocking,
    helper_noop,
    helper_preexec_fn,
    helper_raise,
    helper_return,
)


class TestJobserverWorker(unittest.TestCase):
    """Jobserver worker process behavior."""

    def test_submission_died(self) -> None:
        """Signal receipt by worker can be detected via Future?"""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                # Permit observing callback side-effects
                mutable = [0, 0, 0, 0, 0]

                # Prepare jobs with workers possibly receiving signals
                context = get_context(method)
                js = Jobserver(context=context, slots=2)
                f = js.submit(fn=signal.raise_signal, args=(signal.SIGKILL,))
                f.when_done(helper_callback, mutable, 0, 2)
                g = js.submit(fn=signal.raise_signal, args=(signal.SIGTERM,))
                g.when_done(helper_callback, mutable, 1, 3)
                h = js.submit(fn=signal.raise_signal, args=(signal.SIGUSR1,))
                h.when_done(helper_callback, mutable, 2, 5)
                i = js.submit(fn=len, args=(("The jig is up!",)))
                i.when_done(helper_callback, mutable, 3, 7)
                j = js.submit(fn=signal.raise_signal, args=(signal.SIGUSR2,))
                j.when_done(helper_callback, mutable, 4, 11)

                # Confirm done/callbacks correct even when submissions die
                self.assertTrue(f.wait())
                self.assertTrue(g.wait())
                self.assertTrue(h.wait())
                self.assertTrue(i.wait())
                self.assertTrue(j.wait())
                self.assertEqual([2, 3, 5, 7, 11], mutable)

                # Confirm either results or signals available in reverse order
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
        """SystemExit in fn must surface as SubmissionDied."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
                f = js.submit(fn=sys.exit, args=(1,))
                with self.assertRaises(SubmissionDied):
                    f.result(timeout=5)

    def test_keyboard_interrupt_becomes_submission_died(self) -> None:
        """KeyboardInterrupt (BaseException) surfaces as SubmissionDied."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
                f = js.submit(
                    fn=helper_raise,
                    args=(KeyboardInterrupt,),
                    timeout=5,
                )
                with self.assertRaises(SubmissionDied):
                    f.result(timeout=5)

    def test_os_exit_becomes_submission_died(self) -> None:
        """os._exit() in worker produces SubmissionDied."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
                f = js.submit(fn=os._exit, args=(1,), timeout=5)
                with self.assertRaises(SubmissionDied):
                    f.result(timeout=5)

    def test_done_signal_terminates(self) -> None:
        """Future.wait(..., signal=...) accepts Signals enum and int forms."""
        for method, sig in itertools.product(
            get_all_start_methods(), (signal.SIGTERM, int(signal.SIGTERM))
        ):
            with self.subTest(method=method, sig=sig):
                js = Jobserver(context=get_context(method), slots=1)
                f = js.submit(fn=time.sleep, args=(60,))
                self.assertTrue(f.wait(timeout=None, signal=sig))
                with self.assertRaises(SubmissionDied):
                    f.result()

    def test_done_signal_invalid_raises(self) -> None:
        """Future.wait(..., signal=invalid) raises OSError from os.kill."""
        invalid_sig = max(signal.valid_signals()) + 1
        for method in get_all_start_methods():
            with self.subTest(method=method):
                context = get_context(method)
                mq: MinimalQueue[str] = MinimalQueue(context=context)
                js = Jobserver(context=context, slots=1)
                f = js.submit(fn=helper_nonblocking, args=(mq,))
                try:
                    with self.assertRaises(OSError):
                        f.wait(timeout=0, signal=invalid_sig)
                finally:
                    mq.put("cleanup")
                    f.result(timeout=10)

    def test_done_signal_after_done_is_safe(self) -> None:
        """Future.wait(..., signal=...) on an already-done Future is safe."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
                f = js.submit(fn=len, args=((1, 2, 3),))
                self.assertEqual(f.result(timeout=None), 3)
                self.assertTrue(f.wait(timeout=None, signal=signal.SIGTERM))
                self.assertTrue(f.wait(timeout=0, signal=int(signal.SIGTERM)))

    def test_done_signal_very_short_timeout(self) -> None:
        """Future.wait(timeout=tiny, signal=...) still delivers the signal."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=get_context(method), slots=1)
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
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
                # Notice f sets, g confirms unset, and h re-sets they key.
                f = js.submit(
                    fn=os.getenv, args=(key, "SENTINEL"), env={key: "5678"}
                )
                g = js.submit(fn=os.getenv, args=(key, "SENTINEL"), env={})
                h = js.submit(
                    fn=os.getenv, args=(key, "SENTINEL"), env={key: "1234"}
                )
                # Notice that i then uses preexec_fn, not env, to set the key.
                i = js.submit(
                    fn=os.getenv,
                    args=(key, "SENTINEL"),
                    preexec_fn=helper_preexec_fn,
                )
                # Then j uses both to confirm env updated before preexec_fn.
                j = js.submit(
                    fn=os.getenv,
                    args=(key, "SENTINEL"),
                    preexec_fn=helper_preexec_fn,
                    env={key: "OVERWRITTEN"},
                )
                # Next, k sets then unsets to check unsetting and Iterables.
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
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
                f = js.submit(fn=helper_current_process_name)
                self.assertEqual("Jobserver-worker", f.result())

    def test_preexec_fn_exception(self) -> None:
        """Exception in preexec_fn propagates through result()."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
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
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)

                # Confirm negative sleep is detectable with fn never called
                with self.assertRaises(AssertionError):
                    js.submit(fn=len, sleep_fn=lambda: -1.0)

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
                # Note fn is never called as sleep_fn vetoes the invocation.
                hs = iter(itertools.cycle((0.05,)))

                def sfn_hs(hs=hs):
                    return next(hs)

                with self.assertRaises(Blocked):
                    js.submit(fn=len, sleep_fn=sfn_hs, timeout=0.1)

                # Confirm as expected.  Importantly, results not previously
                # retrieved implying above submissions finalized results.
                self.assertEqual(1, f.result())
                self.assertEqual(0, g.result())

    def test_sleep_fn_raises_propagates(self) -> None:
        """Exception from sleep_fn propagates out of submit()."""
        for method in get_all_start_methods():
            with self.subTest(method=method):
                js = Jobserver(context=method, slots=1)
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
        for method in get_all_start_methods():
            with self.subTest(method=method):
                # env: child sees key without submit() specifying it
                js = Jobserver(context=method, slots=1, env={key: "FROM_INIT"})
                f = js.submit(fn=os.getenv, args=(key, "SENTINEL"))
                self.assertEqual("FROM_INIT", f.result())

                # preexec_fn: helper sets key; no repeat needed
                js = Jobserver(
                    context=method,
                    slots=1,
                    preexec_fn=helper_preexec_fn,
                )
                g = js.submit(fn=os.getenv, args=(key, "SENTINEL"))
                self.assertEqual("PREEXEC_FN", g.result())

                # sleep_fn: a vetoing fn blocks every submit()
                js = Jobserver(context=method, slots=1, sleep_fn=lambda: 99.0)
                with self.assertRaises(Blocked):
                    js.submit(fn=len, timeout=0.0)

    def test_submit_overrides_init_defaults(self) -> None:
        """submit() kwargs override the instance defaults set in __init__."""
        key = "JOBSERVER_TEST_ENVIRON"
        self.assertIsNone(os.environ.get(key, None))
        for method in get_all_start_methods():
            with self.subTest(method=method):
                # env override: submit-level value replaces the init default
                js = Jobserver(context=method, slots=1, env={key: "FROM_INIT"})
                f = js.submit(
                    fn=os.getenv,
                    args=(key, "SENTINEL"),
                    env={key: "FROM_SUBMIT"},
                )
                self.assertEqual("FROM_SUBMIT", f.result())

                # preexec_fn override: submit-level noop suppresses the helper
                js = Jobserver(
                    context=method,
                    slots=1,
                    preexec_fn=helper_preexec_fn,
                )
                g = js.submit(
                    fn=os.getenv,
                    args=(key, "SENTINEL"),
                    preexec_fn=helper_noop,
                )
                self.assertEqual("SENTINEL", g.result())

                # sleep_fn override: submit-level permissive fn unblocks work
                js = Jobserver(context=method, slots=1, sleep_fn=lambda: 99.0)
                h = js.submit(fn=len, args=((1,),), sleep_fn=lambda: None)
                self.assertEqual(1, h.result())
