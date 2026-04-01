# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Verify ResourceWarning from Jobserver.__del__ (see issue #124)."""

import gc
import unittest
import warnings

from jobserver import Jobserver

from .helpers import FAST, TIMEOUT, helper_return, silence_forkserver


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
