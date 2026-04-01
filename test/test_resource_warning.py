# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Verify ResourceWarning from Jobserver.__del__ (see issue #124)."""

import gc
import time
import unittest
import warnings

from jobserver import Jobserver

from .helpers import (
    FAST,
    TIMEOUT,
    helper_raise,
    helper_return,
    silence_forkserver,
)


def setUpModule() -> None:
    silence_forkserver()


class TestResourceWarning(unittest.TestCase):
    """Jobserver emits ResourceWarning when finalized with running Futures."""

    def _assert_no_resource_warning(self, js: Jobserver) -> None:
        """Assert that finalizing *js* emits no ResourceWarning."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            del js
            gc.collect()
        self.assertEqual(
            [w for w in caught if issubclass(w.category, ResourceWarning)],
            [],
        )

    def test_warning_emitted_with_running_future(self) -> None:
        """__del__ warns when outstanding Futures remain."""
        js = Jobserver(context=FAST, slots=1)
        # One submit is enough: _selector_map retains the entry until
        # reclaim_resources() or __exit__, so the subprocess finishing
        # before del/gc does not create a race.
        _f = js.submit(fn=helper_return, args=(42,))
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            del js
            gc.collect()
        rw = [w for w in caught if issubclass(w.category, ResourceWarning)]
        self.assertEqual(len(rw), 1)
        self.assertRegex(str(rw[0].message), r"Finalizing .* running Future")
        self.assertIsNotNone(rw[0].source)

    def test_no_warning_when_properly_closed(self) -> None:
        """__del__ is silent after the context manager cleans up."""
        with Jobserver(context=FAST, slots=1) as js:
            f = js.submit(fn=helper_return, args=(42,))
            f.result(timeout=TIMEOUT)
        self._assert_no_resource_warning(js)

    def test_no_warning_when_no_futures_submitted(self) -> None:
        """__del__ is silent when no Futures were ever submitted."""
        self._assert_no_resource_warning(Jobserver(context=FAST, slots=1))


class TestExitRuntimeWarning(unittest.TestCase):
    """Jobserver.__exit__ emits RuntimeWarning for suppressed callbacks."""

    def test_runtime_warning_on_callback_raised(self) -> None:
        """__exit__ emits RuntimeWarning for each suppressed CallbackRaised."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with Jobserver(context=FAST, slots=2) as js:
                a = js.submit(fn=time.sleep, args=(0.1,), timeout=TIMEOUT)
                b = js.submit(fn=time.sleep, args=(0.1,), timeout=TIMEOUT)
                # Two failing callbacks on a, one on b
                a.when_done(helper_raise, ValueError, "a0")
                a.when_done(helper_raise, TypeError, "a1")
                b.when_done(helper_raise, ArithmeticError, "b")
                time.sleep(0.5)
        rw = [w for w in caught if issubclass(w.category, RuntimeWarning)]
        self.assertEqual(len(rw), 3)
        messages = [str(w.message) for w in rw]
        for m in messages:
            self.assertIn("suppressed CallbackRaised", m)
        combined = "\n".join(messages)
        self.assertIn("ValueError('a0')", combined)
        self.assertIn("TypeError('a1')", combined)
        self.assertIn("ArithmeticError('b')", combined)
