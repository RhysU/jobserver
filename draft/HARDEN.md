# Hardening Plan for `JobserverExecutor`

A specification for testing the **public API** of `JobserverExecutor`
as a `concurrent.futures.Executor`.  Informed by CPython's own
`Lib/test/test_concurrent_futures/` suite, known CPython bug reports,
and common pitfalls in process-pool executor implementations.

Every test interacts only through the public surface:

- `JobserverExecutor(jobserver)` -- construction
- `executor.submit(fn, /, *args, **kwargs)` -- submission
- `executor.map(fn, *iterables, timeout=)` -- bulk submission
- `executor.shutdown(wait=, cancel_futures=)` -- lifecycle
- `with JobserverExecutor(js) as exe:` -- context manager
- The returned `concurrent.futures.Future` -- `result()`,
  `exception()`, `done()`, `running()`, `cancelled()`, `cancel()`,
  `add_done_callback()`
- `concurrent.futures.wait()` and `concurrent.futures.as_completed()`

No test should reach into private attributes, mock internal queues,
or test the underlying `Jobserver` in isolation.

---

## 1. Submit and Result

### 1.1 Successful call returns correct result

```python
with JobserverExecutor(js) as exe:
    assert exe.submit(len, (1, 2, 3)).result(timeout=10) == 3
```

### 1.2 Keyword arguments are forwarded

```python
with JobserverExecutor(js) as exe:
    assert exe.submit(int, "ff", base=16).result(timeout=10) == 255
```

### 1.3 `None` can be returned

```python
with JobserverExecutor(js) as exe:
    assert exe.submit(min, (), default=None).result(timeout=10) is None
```

### 1.4 Multiple concurrent submissions

Submit several tasks and collect results in submission order.  All
results must be correct regardless of completion order.

### 1.5 `submit()` returns a `concurrent.futures.Future`

```python
assert isinstance(exe.submit(len, (1,)), concurrent.futures.Future)
```

---

## 2. Exception Propagation

### 2.1 Exception raised in callable surfaces via `result()`

```python
with JobserverExecutor(js) as exe:
    f = exe.submit(raises, ValueError, "boom")
    with self.assertRaises(ValueError):
        f.result(timeout=10)
```

### 2.2 `exception()` returns the raised exception

```python
exc = f.exception(timeout=10)
assert isinstance(exc, ValueError)
```

### 2.3 `exception()` returns `None` on success

```python
f = exe.submit(len, (1, 2))
assert f.exception(timeout=10) is None
```

### 2.4 An Exception can be *returned* (not raised)

Submit a callable that returns `ValueError("not raised")` as a value.
`result()` must return it, not raise it.

### 2.5 Worker killed by signal

Submit a task that sends `SIGKILL` to its own process.  `result()`
must raise an exception (not hang).  A subsequent submission must
succeed, proving the executor recovered.

### 2.6 Worker exits via `sys.exit()`

Submit `sys.exit(1)`.  `result()` must raise.  The executor must
remain usable for further submissions.

### 2.7 Unpicklable callable

Submit a lambda (unpicklable under `spawn` context).  `result()` must
raise an exception, not hang.

### 2.8 Unpicklable arguments

Submit `len` with a `threading.Lock()` argument.  `result()` must
raise, not hang.

### 2.9 Very large arguments and results

Submit work that passes and returns large byte strings (e.g. 10 MB).
Verify correct round-trip and no hang.

---

## 3. Future State Queries

### 3.1 A newly submitted future is PENDING when slots are full

Fill all slots with slow tasks, then submit another.  The new future
must report `done() == False`, `running() == False`,
`cancelled() == False`.

### 3.2 A dispatched future transitions to RUNNING

Submit a task that blocks on a barrier.  Poll `running()` until it
becomes `True` (with a timeout).  Verify `done() == False`.

### 3.3 A completed future is FINISHED

After `result()` returns, `done() == True`, `running() == False`,
`cancelled() == False`.

---

## 4. Cancellation

### 4.1 A PENDING future can be cancelled

Fill all slots, submit another, call `cancel()`.  Assert:
- `cancel()` returns `True`
- `cancelled()` returns `True`
- `done()` returns `True`
- `result()` raises `CancelledError`

### 4.2 A RUNNING future cannot be cancelled

Submit a slow task, wait until `running()` is `True`, then call
`cancel()`.  Assert `cancel()` returns `False` and the future
completes normally.

### 4.3 A FINISHED future cannot be cancelled

Wait for `result()`, then call `cancel()`.  Returns `False`.

### 4.4 Rapid submit-then-cancel churn

In a tight loop, submit and immediately cancel N=200 futures.
Verify no deadlock and no leaked processes (process count returns
to baseline after `shutdown()`).

### 4.5 Cancel racing with dispatch

Fill slots, submit extra work, cancel the pending future, then
free a slot.  The cancelled future must stay cancelled even though
a slot became available.

---

## 5. Callbacks

### 5.1 `add_done_callback` fires on success

Register a callback before the future completes.  Verify it fires
and receives the future as its argument.

### 5.2 `add_done_callback` fires on exception

Same, but the callable raises.  Callback still fires.

### 5.3 `add_done_callback` fires on cancellation

Cancel a PENDING future, verify the callback fires.

### 5.4 Callback on already-done future fires immediately

Wait for `result()`, then register a callback.  It must fire
synchronously before `add_done_callback` returns.

### 5.5 Multiple callbacks fire in registration order

Register callbacks A, B, C.  After completion, verify they fired
in order A, B, C.

### 5.6 A raising callback does not prevent subsequent callbacks

Register three callbacks where the middle one raises.  The first and
third must still fire.

### 5.7 Callback receives the correct future

When multiple futures each have callbacks, each callback's argument
must be the future on which it was registered.

---

## 6. Shutdown Semantics

### 6.1 `shutdown(wait=True)` blocks until all futures complete

Submit several slow tasks, call `shutdown(wait=True)`, assert all
futures report `done() == True` before the call returns.

### 6.2 `shutdown(wait=False)` returns immediately

Submit a slow task, measure wall-clock time of
`shutdown(wait=False)`.  Assert < 0.5 s.  Assert the future
eventually completes.

### 6.3 `shutdown(cancel_futures=True)` cancels pending work

Fill slots with slow tasks, submit additional work, then
`shutdown(wait=True, cancel_futures=True)`.  Assert:
- Futures that were RUNNING complete normally.
- Futures that were PENDING are cancelled.
- `result()` on cancelled futures raises `CancelledError`.

### 6.4 `shutdown(wait=False, cancel_futures=True)` combined

Must not deadlock.  Pending futures should be cancelled.

### 6.5 `submit()` after `shutdown()` raises `RuntimeError`

### 6.6 Double `shutdown()` is safe

Call `shutdown()` twice; no exception.

### 6.7 Context-manager exit calls `shutdown(wait=True)`

```python
with JobserverExecutor(js) as exe:
    f = exe.submit(len, "hello")
assert f.result(timeout=0) == 5
with self.assertRaises(RuntimeError):
    exe.submit(len, "x")
```

### 6.8 Concurrent submit and shutdown (race test)

Use a `threading.Barrier` so one thread calls `submit()` while
another calls `shutdown()` at the same instant.  The submit must
either succeed (future eventually completes) or raise
`RuntimeError`.  No hang, no crash.  Repeat N=100 times.

### 6.9 `wait()` does not hang after `shutdown(cancel_futures=True)`

After `shutdown(cancel_futures=True)`, call
`concurrent.futures.wait(futures, timeout=5)`.  Must return
promptly, not hang.  (CPython `#109934`.)

### 6.10 Trivial submit immediately after construction

Submit a single trivial task right after creating the executor.
Must not hang.  (CPython `#56665`.)

---

## 7. `map()`

### 7.1 Basic correctness

```python
with JobserverExecutor(js) as exe:
    assert list(exe.map(str, range(5))) == ["0","1","2","3","4"]
```

### 7.2 Exception propagation preserves position

When the k-th invocation raises, iterating the result raises that
exception at position k.

### 7.3 Timeout raises `TimeoutError`

```python
it = exe.map(time.sleep, [5], timeout=0.1)
with self.assertRaises(concurrent.futures.TimeoutError):
    next(it)
```

### 7.4 Empty iterables

```python
assert list(exe.map(str, [])) == []
```

### 7.5 Unequal-length iterables

`map()` stops at the shortest iterable, matching `builtins.map`.

### 7.6 Iterator does not retain completed futures

After yielding a result from `map()`, the corresponding future
should be eligible for garbage collection.  Confirm with
`weakref.ref`.

### 7.7 Partially consumed iterator

Create an iterator via `map()`, consume only the first element,
then drop the iterator.  Verify no leaked processes after
`shutdown()`.

### 7.8 Multiple iterables

```python
list(exe.map(pow, [2,3], [10,10])) == [1024, 59049]
```

---

## 8. `wait()` and `as_completed()` Integration

### 8.1 `wait(ALL_COMPLETED)` returns all futures in `done`

### 8.2 `wait(FIRST_COMPLETED)` returns on first completion

Submit slow and fast tasks.  `wait(FIRST_COMPLETED)` must return
as soon as any one task finishes.

### 8.3 `wait(FIRST_EXCEPTION)` returns on first exception

Submit a mix of successful and failing tasks.  Must return as soon
as any task raises.

### 8.4 `wait()` with timeout returns partial results

Submit slow tasks, call `wait(timeout=0.1)`.  Some futures must be
in `not_done`.

### 8.5 `as_completed()` yields in completion order

Submit tasks with varying durations.  Verify `as_completed()` yields
faster tasks first.

### 8.6 `as_completed()` with timeout

Submit a slow task.  `as_completed(timeout=0.1)` must raise
`TimeoutError`.

### 8.7 Duplicate future in `wait()` and `as_completed()`

Pass the same future twice.  Verify it appears only once in the
result.  (CPython `test_20369`.)

### 8.8 `as_completed()` does not retain references to yielded futures

Use `weakref.ref` to confirm futures are GC-eligible after being
yielded.  (CPython `test_free_reference_yielded_future`.)

---

## 9. Concurrency Stress

### 9.1 Heavy submission exceeding slot count

Submit N=200 tasks with 2 slots.  All N results must be correct.

### 9.2 Mixed workload

Submit a mix of: successful tasks, tasks that raise, tasks that
are immediately cancelled, and tasks whose workers die.  Run all
concurrently and verify every future resolves to the expected
outcome.

### 9.3 Concurrent `submit()` from multiple threads

Spawn T=8 threads each submitting M=25 tasks.  Verify all T*M=200
results are correct.

### 9.4 `sys.setswitchinterval(1e-6)` stress

Re-run the concurrent submit (9.3) and the submit/shutdown race
(6.8) with the GIL switch interval set to 1 microsecond to
maximize context switching and expose races.

### 9.5 Burst submission (thousands of tasks)

Submit N=2000 tasks in rapid succession.  All must complete
correctly with no deadlock.  Exercises queue capacity under burst
load.

---

## 10. Resource Leak Detection

### 10.1 Process count returns to baseline

Count child processes before and after a full executor lifecycle
(create, submit work, shutdown).  Must return to baseline.

### 10.2 File descriptor count returns to baseline

Use `/proc/self/fd` (Linux) to count open FDs before and after.
No leak.

### 10.3 Thread count returns to baseline

`threading.active_count()` before and after.  The receiver thread
must exit after `shutdown(wait=True)`.

### 10.4 Repeated create/shutdown cycles

Create and shut down the executor 50 times in a loop.  Monitor for
monotonically increasing resource consumption (processes, FDs,
threads, RSS).

### 10.5 Shutdown after worker death cleans up

After a worker dies (SIGKILL), verify `shutdown(wait=True)`
completes and all resources are reclaimed.

---

## 11. Multiprocessing Start Method Coverage

All tests in sections 1--10 should be parameterized over every
available start method: `fork`, `forkserver`, `spawn`.

Rationale:

- `spawn` requires full picklability of callables and arguments,
  catching issues that `fork` hides by inheriting process state.
- `forkserver` has a different process-tree topology and different
  EOF/broken-pipe propagation characteristics.
- `fork` can inherit locks in inconsistent states from threaded
  parents.

Use `subTest(method=method)` to run each test under each method.

---

## 12. Timing and Deadlock Detection

### 12.1 Global test timeout

Every test must complete within a hard timeout (e.g. 60 s).  Use
a `signal.alarm`-based decorator or `unittest` timeout support.
A test that exceeds the timeout is treated as a deadlock failure.

### 12.2 Targeted short timeouts

Tests expected to be fast (e.g. `shutdown(wait=False)`, trivial
submit) should assert wall-clock time < 1 s.

### 12.3 CI deadlock canary

Run the full test suite under a 5-minute wall-clock limit in CI.
If any test hangs, the CI job is killed and reported as a failure.

---

## 13. Edge Cases Inspired by CPython Bug Reports

Each of these is tested purely through the public API.

### 13.1 Cancel then `result()` on same future

Cancel a PENDING future, then call `result()`.  Must raise
`CancelledError`, not hang or raise `InvalidStateError`.

### 13.2 Worker death does not poison the executor

After a worker dies (SIGKILL), subsequent submissions must succeed.
The executor must not enter a permanently broken state.  (Unlike
CPython's `BrokenProcessPool`, which is terminal.)

### 13.3 Executor created inside a forked child

Create the executor in a `fork()`ed child process.  Must not
inherit stale lock state from the parent.  (CPython `#89184`.)

### 13.4 `result(timeout=0)` on incomplete future raises `TimeoutError`

```python
f = exe.submit(time.sleep, 5)
with self.assertRaises(concurrent.futures.TimeoutError):
    f.result(timeout=0)
```

### 13.5 `exception(timeout=0)` on incomplete future raises `TimeoutError`

Same as above but via `exception()`.

### 13.6 Submitting after `shutdown(wait=False)` raises `RuntimeError`

Even though `wait=False` returns immediately, `submit()` must
still be rejected.

### 13.7 Many futures cancelled then `shutdown(wait=True)`

Submit many futures, cancel them all, then `shutdown(wait=True)`.
Must return promptly, not block.
