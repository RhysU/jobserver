"""Acceptance 5.6: Environment and preexec_fn Edge Cases.

Acceptance Criteria
-------------------
- env sets variables visible in child.
- env with None values unsets variables in child.
- Set-then-unset (last write wins) works correctly.
- preexec_fn modifies child state without affecting parent.
- preexec_fn that raises propagates to result().
"""

import os
import unittest

from jobserver import Jobserver

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import (
    get_cwd,
    get_env_var,
    preexec_chdir_tmp,
    preexec_raise,
    return_value,
)


def setUpModule():
    bootstrap_forkserver()


class TestEnvPreexec(unittest.TestCase):
    """Verify env and preexec_fn edge cases."""

    def test_env_sets_variable(self):
        """5.6.1: env=[("FOO", "bar")] visible in child."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(
            fn=get_env_var,
            args=("ACCEPTANCE_TEST_FOO",),
            env=[("ACCEPTANCE_TEST_FOO", "bar")],
            timeout=TIMEOUT,
        )
        self.assertEqual("bar", f.result(timeout=TIMEOUT))

    def test_env_unsets_variable(self):
        """5.6.2: env with None value unsets variable in child."""
        # Set a variable in parent so we can verify it's unset in child
        os.environ["ACCEPTANCE_TEST_UNSET"] = "present"
        try:
            js = Jobserver(context=FAST_METHOD, slots=2)
            f = js.submit(
                fn=get_env_var,
                args=("ACCEPTANCE_TEST_UNSET",),
                env=[("ACCEPTANCE_TEST_UNSET", None)],
                timeout=TIMEOUT,
            )
            self.assertIsNone(f.result(timeout=TIMEOUT))
        finally:
            os.environ.pop("ACCEPTANCE_TEST_UNSET", None)

    def test_env_set_then_unset(self):
        """5.6.3: Set then unset -> absent (last write wins)."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(
            fn=get_env_var,
            args=("ACCEPTANCE_TEST_SETUNSET",),
            env=[
                ("ACCEPTANCE_TEST_SETUNSET", "val"),
                ("ACCEPTANCE_TEST_SETUNSET", None),
            ],
            timeout=TIMEOUT,
        )
        self.assertIsNone(f.result(timeout=TIMEOUT))

    def test_preexec_fn_modifies_child_state(self):
        """5.6.4: preexec_fn changes cwd in child, parent unaffected."""
        parent_cwd = os.getcwd()
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(
            fn=get_cwd,
            preexec_fn=preexec_chdir_tmp,
            timeout=TIMEOUT,
        )
        child_cwd = f.result(timeout=TIMEOUT)
        # Child should see /tmp (or its realpath)
        self.assertIn("tmp", child_cwd.lower())
        # Parent should be unaffected
        self.assertEqual(parent_cwd, os.getcwd())

    def test_preexec_fn_raises(self):
        """5.6.5: preexec_fn that raises propagates to result()."""
        js = Jobserver(context=FAST_METHOD, slots=2)
        f = js.submit(
            fn=return_value,
            args=(1,),
            preexec_fn=preexec_raise,
            timeout=TIMEOUT,
        )
        with self.assertRaises(RuntimeError) as ctx:
            f.result(timeout=TIMEOUT)
        self.assertIn("preexec failed", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
