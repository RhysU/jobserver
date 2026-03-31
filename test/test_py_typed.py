# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""PEP 561 compliance: py.typed marker must exist in the package."""

import importlib.resources
import unittest


class TestPyTyped(unittest.TestCase):
    """The py.typed marker file is required for PEP 561 compliance."""

    def test_py_typed_exists(self) -> None:
        """py.typed marker file must exist in the installed package."""
        ref = importlib.resources.files("jobserver").joinpath("py.typed")
        self.assertTrue(ref.is_file(), "src/jobserver/py.typed is missing")
