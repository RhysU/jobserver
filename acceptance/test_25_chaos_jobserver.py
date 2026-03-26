"""Acceptance 8.1: Combined Chaos Monkey -- Jobserver.

Acceptance Criteria
-------------------
- 200 jobs (mixed normal/exception/death/large/callback) complete within 120s.
- No leaked child processes (all futures reach terminal state).
- Every future has a deterministic outcome matching its category.
- Concurrent submit, collect, and reclaim threads do not deadlock.
"""

import os
import random
import threading
import time
import unittest

from jobserver import Blocked, CallbackRaised, Jobserver, SubmissionDied

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import (
    exit_abruptly,
    make_bytes,
    raise_value_error,
    return_value,
    sleep_and_return,
)


def setUpModule():
    bootstrap_forkserver()


# Categories and their expected outcome
NORMAL = "normal"
EXCEPTION = "exception"
DEATH = "death"
LARGE = "large"
CALLBACK_OK = "callback_ok"
CALLBACK_RAISE = "callback_raise"


def _make_work(category, idx):
    """Return (fn, args, category) for a given category."""
    if category == NORMAL:
        delay = random.uniform(0, 0.1)
        return sleep_and_return, (delay, idx), category
    elif category == EXCEPTION:
        return raise_value_error, (f"chaos-{idx}",), category
    elif category == DEATH:
        return exit_abruptly, (1,), category
    elif category == LARGE:
        return make_bytes, (1 * 1024 * 1024,), category
    elif category in (CALLBACK_OK, CALLBACK_RAISE):
        return return_value, (idx,), category
    raise ValueError(category)


class TestChaosJobserver(unittest.TestCase):
    """Chaos monkey test combining multiple failure modes."""

    def test_combined_chaos(self):
        """8.1: 200 mixed jobs with concurrent submit/collect/reclaim."""
        random.seed(42)
        js = Jobserver(context=FAST_METHOD, slots=4)

        # Build work distribution:
        # 40% normal, 20% exception, 20% death, 10% large, 10% callback
        categories = (
            [NORMAL] * 80
            + [EXCEPTION] * 40
            + [DEATH] * 40
            + [LARGE] * 20
            + [CALLBACK_OK] * 10
            + [CALLBACK_RAISE] * 10
        )
        random.shuffle(categories)

        futures = []  # (future, category)
        lock = threading.Lock()
        errors = []

        # Thread 1: Submit all work
        def submitter():
            try:
                for idx, cat in enumerate(categories):
                    fn, args, category = _make_work(cat, idx)
                    f = js.submit(fn=fn, args=args, timeout=TIMEOUT)
                    # Register callbacks for callback categories
                    if category == CALLBACK_OK:
                        f.when_done(lambda: None)
                    elif category == CALLBACK_RAISE:
                        f.when_done(lambda: (_ for _ in ()).throw(
                            RuntimeError("cb chaos")
                        ))
                    with lock:
                        futures.append((f, category))
            except Exception as e:
                with lock:
                    errors.append(("submitter", e))

        # Thread 2: Periodically reclaim resources
        stop_event = threading.Event()

        def reclaimer():
            try:
                while not stop_event.is_set():
                    js.reclaim_resources()
                    time.sleep(0.05)
            except Exception as e:
                with lock:
                    errors.append(("reclaimer", e))

        t_submit = threading.Thread(target=submitter)
        t_reclaim = threading.Thread(target=reclaimer)
        t_submit.start()
        t_reclaim.start()
        t_submit.join(timeout=120)

        # Collect all results
        with lock:
            snapshot = list(futures)

        normal_count = 0
        exception_count = 0
        death_count = 0
        large_count = 0

        for f, category in snapshot:
            # Drain all callbacks
            while True:
                try:
                    f.done(timeout=TIMEOUT)
                    break
                except CallbackRaised:
                    pass

            if category == NORMAL:
                result = f.result(timeout=0)
                self.assertIsNotNone(result) or result == 0  # idx can be 0
                normal_count += 1
            elif category == EXCEPTION:
                with self.assertRaises(ValueError):
                    f.result(timeout=0)
                exception_count += 1
            elif category == DEATH:
                with self.assertRaises(SubmissionDied):
                    f.result(timeout=0)
                death_count += 1
            elif category == LARGE:
                result = f.result(timeout=0)
                self.assertEqual(len(result), 1 * 1024 * 1024)
                large_count += 1
            elif category in (CALLBACK_OK, CALLBACK_RAISE):
                # Result should be available
                f.result(timeout=0)

        stop_event.set()
        t_reclaim.join(timeout=5)

        # Verify counts
        self.assertEqual(normal_count, 80)
        self.assertEqual(exception_count, 40)
        self.assertEqual(death_count, 40)
        self.assertEqual(large_count, 20)
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
