# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""The package exposes a usable __version__ string."""

import unittest
from importlib.metadata import version

import jobserver


class TestVersion(unittest.TestCase):
    """jobserver.__version__ is present and matches distribution metadata."""

    def test_version_is_nonempty_str(self) -> None:
        """__version__ must be a non-empty string."""
        # Deliberately format-agnostic: a development checkout reports
        # something like '0.1.dev50+g025f09e26', so asserting an X.Y.Z
        # release shape here would fail on untagged builds.
        self.assertIsInstance(jobserver.__version__, str)
        self.assertTrue(jobserver.__version__)

    def test_version_matches_metadata(self) -> None:
        """__version__ must agree with importlib.metadata for the install."""
        # The test suite runs against an installed (editable) jobserver, so
        # the distribution metadata is queryable; the uninstalled-tree
        # fallback in __init__ is not exercised here.
        self.assertEqual(jobserver.__version__, version("jobserver"))
