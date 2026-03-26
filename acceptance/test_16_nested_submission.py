"""Acceptance 5.3: Nested Submission (Recursive Work).

Acceptance Criteria
-------------------
- Child can submit grandchild work when slots >= 2.
- Three-level nesting works when slots >= 3.
- slots=1 with nested submit(timeout=0) raises Blocked (no deadlock).
- slots=1 with nested submit(timeout=2) raises Blocked after timeout.
"""

import unittest

from jobserver import Blocked, Jobserver

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import (
    nested_submit,
    nested_submit_two,
    nested_three_levels,
    return_value,
)


def setUpModule():
    bootstrap_forkserver()


class TestNestedSubmission(unittest.TestCase):
    """Verify nested (recursive) submissions across process boundaries."""

    def test_child_submits_grandchild(self):
        """5.3.1: Child submits one grandchild with slots >= 2."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(
            fn=nested_submit,
            args=(js, return_value, (42,)),
            timeout=TIMEOUT,
        )
        self.assertEqual(42, f.result(timeout=TIMEOUT))

    def test_three_levels_deep(self):
        """5.3.2: Parent -> child -> grandchild (3 levels) with slots >= 3."""
        js = Jobserver(context=FAST_METHOD, slots=3)
        f = js.submit(fn=nested_three_levels, args=(js,), timeout=TIMEOUT)
        self.assertEqual(42, f.result(timeout=TIMEOUT))

    def test_two_grandchildren_slots_2(self):
        """5.3.3: slots=2, child submits 2 grandchildren with timeout."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        # Child holds 1 slot, tries to submit 2 grandchildren.
        # Only 1 slot free, so second grandchild blocks until first completes.
        f = js.submit(
            fn=nested_submit_two,
            args=(js, return_value, (10,), (20,)),
            kwargs={"timeout": TIMEOUT},
            timeout=TIMEOUT,
        )
        result = f.result(timeout=TIMEOUT)
        self.assertEqual(result, (10, 20))

    def test_single_slot_nested_timeout_zero(self):
        """5.3.4: slots=1, child submits grandchild timeout=0 -> Blocked."""
        js = Jobserver(context=FAST_METHOD, slots=1)
        f = js.submit(
            fn=nested_submit,
            args=(js, return_value, (1,)),
            kwargs={"timeout": 0},
            timeout=TIMEOUT,
        )
        with self.assertRaises(Blocked):
            f.result(timeout=TIMEOUT)

    def test_single_slot_nested_timeout_short(self):
        """5.3.5: slots=1, child submits grandchild timeout=1 -> Blocked."""
        js = Jobserver(context=FAST_METHOD, slots=1)
        f = js.submit(
            fn=nested_submit,
            args=(js, return_value, (1,)),
            kwargs={"timeout": 1},
            timeout=TIMEOUT,
        )
        with self.assertRaises(Blocked):
            f.result(timeout=TIMEOUT)


if __name__ == "__main__":
    unittest.main()
