"""Acceptance 6.3: Executor Throughput.

Acceptance Criteria
-------------------
- 5,000 trivial jobs all resolve via as_completed.
- 4 threads each submitting 250 jobs concurrently: all 1,000 resolve.
- Mixed fast/slow workload: all complete; no starvation.
"""

import concurrent.futures
import os
import threading
import unittest

from jobserver import Jobserver, JobserverExecutor

from .conftest import FAST_METHOD, TIMEOUT, bootstrap_forkserver
from .helpers import return_value, sleep_and_return


def setUpModule():
    bootstrap_forkserver()


class TestExecutorThroughput(unittest.TestCase):
    """Stress test JobserverExecutor throughput."""

    def test_5000_trivial_as_completed(self):
        """6.3.1: 5,000 trivial jobs collected via as_completed."""
        js = Jobserver(context=FAST_METHOD, slots=os.cpu_count() or 4)
        with JobserverExecutor(js) as ex:
            futures = {ex.submit(return_value, i): i for i in range(5000)}
            results = set()
            for f in concurrent.futures.as_completed(
                futures.keys(), timeout=120
            ):
                results.add(f.result())
            self.assertEqual(results, set(range(5000)))

    def test_4_threads_250_each(self):
        """6.3.2: 4 threads each submit 250 jobs concurrently."""
        js = Jobserver(context=FAST_METHOD, slots=os.cpu_count() or 4)
        ex = JobserverExecutor(js)
        all_futures = []
        lock = threading.Lock()
        errors = []

        def submitter(start):
            try:
                local_futures = []
                for i in range(250):
                    f = ex.submit(return_value, start + i)
                    local_futures.append((start + i, f))
                with lock:
                    all_futures.extend(local_futures)
            except Exception as e:
                with lock:
                    errors.append(e)

        threads = [
            threading.Thread(target=submitter, args=(i * 250,))
            for i in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=TIMEOUT)

        ex.shutdown(wait=True)
        self.assertEqual(errors, [])
        self.assertEqual(len(all_futures), 1000)
        results = {f.result(timeout=TIMEOUT) for _, f in all_futures}
        self.assertEqual(results, set(range(1000)))

    def test_mixed_fast_slow(self):
        """6.3.3: 50% fast (0ms), 50% slow (100ms): all complete."""
        js = Jobserver(context=FAST_METHOD, slots=os.cpu_count() or 4)
        with JobserverExecutor(js) as ex:
            futures = []
            for i in range(100):
                if i % 2 == 0:
                    futures.append(ex.submit(return_value, i))
                else:
                    futures.append(ex.submit(sleep_and_return, 0.1, i))
            results = [f.result(timeout=TIMEOUT) for f in futures]
            self.assertEqual(results, list(range(100)))


if __name__ == "__main__":
    unittest.main()
