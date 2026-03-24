# Hardening Findings

Bugs discovered while writing `test_harden.py` against the specification
in `HARDEN.md`.  Each section describes the bug, its root cause in
`impl.py`, the user-visible consequence, and a minimal reproducing test.

---

## 1. `_worker_entrypoint` sends `None` when `fn` raises a `BaseException`

**HARDEN.md reference:** 6.8

### Description

When the callable passed to `submit()` raises a `BaseException` subclass
that is *not* an `Exception` — e.g. `SystemExit`, `KeyboardInterrupt` —
the worker process sends `None` over the pipe instead of letting the pipe
close unread.  The parent then fails an internal assertion rather than
surfacing the expected `SubmissionDied`.

### Root cause

`_worker_entrypoint` (line 476) initialises `result = None` and wraps it
in a `try / except Exception / finally` block:

```python
result = None
try:
    ...
    result = ResultWrapper(fn(*args, **kwargs))
except Exception as exception:
    result = ExceptionWrapper(exception)
finally:
    try:
        send.send(result)          # <-- sends None
    except BrokenPipeError:
        pass
    send.close()
```

A `BaseException` that is not an `Exception` (such as `SystemExit`)
bypasses the `except Exception` clause, so `result` stays `None`.
Control still reaches the `finally` block, where `send.send(None)`
succeeds.  The parent's `Future.done()` then executes:

```python
self._wrapper = self._connection.recv()   # receives None
assert isinstance(self._wrapper, Wrapper), type(self._wrapper)  # BOOM
```

The assertion fails with `AssertionError: <class 'NoneType'>`.

### Expected behaviour

Per HARDEN.md §6.8, a `BaseException` inside `fn` should cause the
worker to exit *without* sending a result.  The parent should see
`EOFError` on the pipe and convert it to `SubmissionDied`, which is the
existing fallback at line 176.

### Consequence

Any user code that raises `SystemExit` (including `sys.exit()`) or
`KeyboardInterrupt` inside a submitted callable triggers an
`AssertionError` traceback from Jobserver internals instead of the
documented `SubmissionDied` exception.

### Minimal reproducing test

```python
import sys
import unittest
from jobserver import Jobserver, SubmissionDied

def _raise_system_exit():
    raise SystemExit(1)

class TestBaseExceptionNotCaught(unittest.TestCase):
    """SystemExit in fn must surface as SubmissionDied, not AssertionError."""

    def test_system_exit_surfaces_as_submission_died(self) -> None:
        js = Jobserver(slots=1)
        f = js.submit(fn=_raise_system_exit)
        with self.assertRaises(SubmissionDied):
            f.result(timeout=10)
```

---

## 2. `Future.done()` is not safe under concurrent calls

**HARDEN.md reference:** 8.4

### Description

When two threads call `Future.done()` on the same `Future`
concurrently — which naturally happens when one thread calls
`reclaim_resources()` while another is inside `submit()` with
`callbacks=True` — the second caller crashes with
`AttributeError: 'NoneType' object has no attribute 'close'`.

### Root cause

`Future.done()` (line 153) has a check-then-act (TOCTOU) race on
`self._connection`:

```
Thread A                            Thread B
────────                            ────────
if self._connection is None: ...    # not None → continue
                                    if self._connection is None: ...
                                    # not None → continue
self._connection.poll(timeout)
self._wrapper = self._connection.recv()
                                    self._connection.poll(timeout)
                                    # poll may return True (data already read)
                                    # or race into recv()
self._process.join()
self._process = None
connection, self._connection = self._connection, None
connection.close()
                                    # self._connection is now None
                                    assert self._process is not None  # BOOM
                                    # or later:
                                    connection, self._connection = self._connection, None
                                    connection.close()  # close() on None → AttributeError
```

Both threads pass the `self._connection is None` guard, then race
through the `recv → join → close` sequence.  Whichever thread finishes
second operates on `None` references.

### Expected behaviour

Per HARDEN.md §8.4, `reclaim_resources` (and therefore `done()`) must
be safe to call while another thread is submitting work.  Since
`submit()` with `callbacks=True` calls `reclaim_resources()` internally,
concurrent `done()` calls on the same `Future` are a realistic scenario.

### Consequence

Under concurrent load, `reclaim_resources()` or `submit()` can crash
with `AttributeError` or `AssertionError` from within `Future.done()`.
The crash is intermittent and depends on thread scheduling, making it
difficult to reproduce outside of stress tests.

### Minimal reproducing test

```python
import threading
import time
import unittest
from jobserver import Jobserver, Blocked

def _sleep_briefly():
    time.sleep(0.05)

class TestDoneConcurrentCalls(unittest.TestCase):
    """Concurrent done() on the same Future must not crash."""

    def test_concurrent_done_no_crash(self) -> None:
        js = Jobserver(slots=4)
        errors = []

        def submitter(stop):
            while not stop.is_set():
                try:
                    js.submit(fn=_sleep_briefly, timeout=0.1)
                except Blocked:
                    pass
                except Exception as e:
                    errors.append(e)

        stop = threading.Event()
        t = threading.Thread(target=submitter, args=(stop,))
        t.start()
        time.sleep(0.3)
        # Main thread also reclaims — races with submitter's reclaim
        for _ in range(20):
            try:
                js.reclaim_resources()
            except Exception as e:
                errors.append(e)
        stop.set()
        t.join(timeout=10)
        # Drain remaining futures
        for future in list(js._future_sentinels.keys()):
            future.done(timeout=10)
        self.assertEqual(errors, [], f"Concurrent done() crashed: {errors}")
```
