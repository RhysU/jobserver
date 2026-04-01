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

    def test_warning_emitted_with_running_future(self) -> None:
        """__del__ warns when outstanding Futures remain."""
        js = Jobserver(context=FAST, slots=1)
        f = js.submit(fn=helper_return, args=(42,))
        f.result(timeout=TIMEOUT)
        # Re-submit so a Future is outstanding at finalization time.
        f2 = js.submit(fn=helper_return, args=(99,))
        # Drop the Jobserver without closing it.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            del js
            gc.collect()
        resource_warnings = [
            w for w in caught if issubclass(w.category, ResourceWarning)
        ]
        self.assertEqual(len(resource_warnings), 1)
        self.assertIn("running Future(s)", str(resource_warnings[0].message))
        # Clean up the orphaned future (best-effort).
        try:
            f2.result(timeout=TIMEOUT)
        except Exception:
            pass

    def test_no_warning_when_properly_closed(self) -> None:
        """__del__ is silent after the context manager cleans up."""
        with Jobserver(context=FAST, slots=1) as js:
            f = js.submit(fn=helper_return, args=(42,))
            f.result(timeout=TIMEOUT)
        # js.__exit__ already ran; dropping the last reference is safe.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            del js
            gc.collect()
        resource_warnings = [
            w for w in caught if issubclass(w.category, ResourceWarning)
        ]
        self.assertEqual(len(resource_warnings), 0)

    def test_no_warning_when_no_futures_submitted(self) -> None:
        """__del__ is silent when no Futures were ever submitted."""
        js = Jobserver(context=FAST, slots=1)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            del js
            gc.collect()
        resource_warnings = [
            w for w in caught if issubclass(w.category, ResourceWarning)
        ]
        self.assertEqual(len(resource_warnings), 0)
