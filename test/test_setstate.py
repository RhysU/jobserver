# Copyright (C) 2019-2026 Rhys Ulerich
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""__setstate__ rejects malformed pickled state (survives python -O)."""

import unittest

from jobserver import Jobserver
from jobserver._jobserver import Resources


class TestSetStateValidation(unittest.TestCase):
    """Resources/Jobserver __setstate__ validate shape with real raises.

    These guards replaced asserts so that a malformed payload is still
    rejected when the interpreter is run with -O (which strips asserts).
    Instances are built via __new__ to invoke __setstate__ directly,
    bypassing the resource-allocating __init__.
    """

    def test_resources_setstate_rejects_non_tuple(self) -> None:
        """Resources.__setstate__ raises TypeError on a non-tuple."""
        obj = Resources.__new__(Resources)
        with self.assertRaises(TypeError):
            obj.__setstate__(None)

    def test_resources_setstate_rejects_wrong_length(self) -> None:
        """Resources.__setstate__ raises ValueError on a mis-sized tuple."""
        obj = Resources.__new__(Resources)
        with self.assertRaises(ValueError):
            obj.__setstate__(("only-one-element",))

    def test_jobserver_setstate_rejects_non_tuple(self) -> None:
        """Jobserver.__setstate__ raises TypeError on a non-tuple."""
        obj = Jobserver.__new__(Jobserver)
        with self.assertRaises(TypeError):
            obj.__setstate__(None)

    def test_jobserver_setstate_rejects_wrong_length(self) -> None:
        """Jobserver.__setstate__ raises ValueError on a mis-sized tuple."""
        obj = Jobserver.__new__(Jobserver)
        with self.assertRaises(ValueError):
            obj.__setstate__((1, 2, 3))
