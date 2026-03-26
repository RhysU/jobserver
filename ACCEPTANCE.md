# Acceptance Testing Plan: Jobserver, Future, and JobserverExecutor

## Objective

Prove that `Jobserver`, `Future`, and `JobserverExecutor` are production-ready
by exercising every public API under realistic, adversarial, and pathological
conditions.  All tests use only public interfaces.  The plan is organized into
seven areas, each with concrete test scenarios.

---

## 1. Baseline Functional Correctness

Verify that the happy path works before trying to break anything.

### 1.1 Jobserver Round-Trip

| # | Scenario | Method | Expected |
|---|----------|--------|----------|
| 1 | Submit `lambda: 42`, collect `result()` | `submit()` / `result()` | Returns `42` |
| 2 | Submit via shorthand `js(fn, arg)` | `__call__` | Same semantics as `submit()` |
| 3 | Submit with `args=(1,2)` and `kwargs={"c":3}`, verify all received | `submit()` | `fn(1,2,c=3)` executes correctly |
| 4 | Submit with `consume=0` (fire-and-forget, no slot consumed) | `submit(consume=0)` | Returns immediately; result available later |
| 5 | Return `None` explicitly | `result()` | Returns `None`, not `SubmissionDied` |
| 6 | Return an `Exception` object (not raised) | `result()` | Returns the Exception object, does not raise |
| 7 | Verify `context` property returns a usable `BaseContext` | `.context` | Is a `BaseContext` instance |

### 1.2 JobserverExecutor Round-Trip

| # | Scenario | Expected |
|---|----------|----------|
| 1 | `executor.submit(fn, *args, **kwargs)` returns `concurrent.futures.Future` | Future resolves to correct value |
| 2 | `executor.map(fn, iterable)` yields results in order | All values correct, order preserved |
| 3 | Use as context manager (`with JobserverExecutor(js) as ex:`) | `shutdown(wait=True)` called on exit |
| 4 | `as_completed()` / `wait()` interop from `concurrent.futures` | Standard idioms work without modification |

---

## 2. Concurrency and Load Testing

### 2.1 Slot Saturation

| # | Scenario | Slots | Work Items | Expected |
|---|----------|-------|------------|----------|
| 1 | Exactly fill all slots | N | N | All complete, no blocking |
| 2 | Submit 10x slots | N | 10N | All complete; at most N concurrent |
| 3 | `slots=1`, submit 100 items sequentially drained | 1 | 100 | Serialized execution, all complete |
| 4 | `slots=1`, rapid-fire submit with `timeout=None` | 1 | 50 | All eventually complete (no deadlock) |

**How to verify concurrency limit:** Each work function records
`(start_time, end_time)`.  Post-hoc analysis confirms that at no point
are more than `slots` functions executing simultaneously.

### 2.2 High-Throughput Stress

| # | Scenario | Details | Expected |
|---|----------|---------|----------|
| 1 | 1,000 trivial jobs (`lambda: None`) | `slots=cpu_count` | All complete within reasonable wall time; no resource leaks |
| 2 | 10,000 trivial jobs via `JobserverExecutor` | `slots=cpu_count` | Executor handles backpressure; all futures resolve |
| 3 | Sustained submission loop for 60 seconds | Continuous submit/collect cycle | No monotonic growth in RSS, FD count, or process table |

### 2.3 Large Payloads

| # | Scenario | Expected |
|---|----------|----------|
| 1 | Return a 1 MB `bytes` object | Result received correctly |
| 2 | Return a 50 MB `bytes` object | Result received correctly (under pipe limits) |
| 3 | Return a 128 MB+ object (exceeds pipe capacity) | Child catches `ValueError`; parent sees exception, not hang |
| 4 | Pass a 10 MB argument into `fn` | Argument received correctly in child |

---

## 3. Error Handling and Exception Propagation

### 3.1 Work Exceptions

| # | Scenario | Expected |
|---|----------|----------|
| 1 | `fn` raises `ValueError("boom")` | `result()` raises `ValueError("boom")` |
| 2 | `fn` raises a custom `Exception` subclass | Same exception type and message re-raised |
| 3 | `fn` raises `SystemExit(1)` (a `BaseException`, not `Exception`) | `result()` raises `SubmissionDied` |
| 4 | `fn` raises `KeyboardInterrupt` | `result()` raises `SubmissionDied` |
| 5 | Exception in `preexec_fn` | `result()` raises the `preexec_fn` exception |
| 6 | Exception raised after partial stdout/stderr output | Exception still propagated; output doesn't corrupt pipe |

### 3.2 Process Death

| # | Scenario | Expected |
|---|----------|----------|
| 1 | `fn` calls `os._exit(1)` | `result()` raises `SubmissionDied` |
| 2 | `fn` calls `os.kill(os.getpid(), signal.SIGKILL)` | `result()` raises `SubmissionDied` |
| 3 | `fn` calls `os.kill(os.getpid(), signal.SIGTERM)` | `result()` raises `SubmissionDied` |
| 4 | `fn` calls `os.kill(os.getpid(), signal.SIGSEGV)` | `result()` raises `SubmissionDied` |
| 5 | `fn` enters infinite loop, parent calls `done(timeout=2)` | `done()` returns `False`; process remains running (not killed by library) |

### 3.3 Blocked / Timeout Behavior

| # | Scenario | Expected |
|---|----------|----------|
| 1 | `submit(timeout=0)` when all slots busy | Raises `Blocked` immediately |
| 2 | `submit(timeout=0.5)` when all slots busy for >0.5s | Raises `Blocked` after ~0.5s |
| 3 | `result(timeout=0)` on incomplete future | Raises `Blocked` |
| 4 | `done(timeout=0)` on incomplete future | Returns `False` (never raises `Blocked`) |
| 5 | `done(timeout=None)` on future that eventually completes | Returns `True` after work finishes |
| 6 | `result(timeout=None)` blocks until completion | Returns correct result |

### 3.4 JobserverExecutor Exception Propagation

| # | Scenario | Expected |
|---|----------|----------|
| 1 | `fn` raises `ValueError` | `future.result()` raises `ValueError` |
| 2 | Submit after `shutdown()` | Raises `RuntimeError` |
| 3 | Submit unpicklable `fn` (e.g. a lambda in spawn mode) | `future.result()` raises pickling error |

---

## 4. Callback Chaos

### 4.1 Normal Callback Operation

| # | Scenario | Expected |
|---|----------|----------|
| 1 | Register one callback, verify it fires after `done()` | Callback invoked exactly once |
| 2 | Register callback on already-complete future | Callback invoked immediately within `when_done()` |
| 3 | Register 100 callbacks on one future | All 100 invoked in registration order |
| 4 | Callback receives arguments passed to `when_done(fn, *args, **kwargs)` | All args/kwargs forwarded correctly |

### 4.2 Callback Error Draining

| # | Scenario | Expected |
|---|----------|----------|
| 1 | Single callback raises `RuntimeError` | `done()` raises `CallbackRaised` with `__cause__` set |
| 2 | Three callbacks: OK, RAISE, OK | First `done()` runs first OK, then raises `CallbackRaised`; second `done()` runs third OK |
| 3 | All 5 callbacks raise different exceptions | Five `done()` calls needed, each raises `CallbackRaised` |
| 4 | Callback raises, then `result()` called | `result()` may raise `CallbackRaised` first; once drained, returns result |

### 4.3 Re-Entrant Callbacks

| # | Scenario | Expected |
|---|----------|----------|
| 1 | Callback A registers callback B inside `when_done` | Both A and B fire |
| 2 | Callback A registers callback B which registers callback C | All three fire |
| 3 | Re-entrant callback raises | `CallbackRaised` propagated correctly |

### 4.4 Callback + Thread Safety

| # | Scenario | Expected |
|---|----------|----------|
| 1 | Thread T1 calls `done()`, Thread T2 calls `when_done()` concurrently | No deadlock; callback fires exactly once |
| 2 | 10 threads all call `done()` on the same future | Exactly one thread processes result; all see `True` |
| 3 | 10 threads register callbacks while future completes | All callbacks fire exactly once |

---

## 5. Pathological and Adversarial Scenarios

### 5.1 Resource Exhaustion Attacks

| # | Scenario | Expected |
|---|----------|----------|
| 1 | `slots=1`, submit 500 jobs, never call `done()` on any | With `callbacks=True`, each `submit()` reclaims completed work; all eventually submitted |
| 2 | `slots=1`, submit 500 jobs with `callbacks=False` | Only 1 runs at a time; caller must manually `reclaim_resources()` or `done()` to free slots |
| 3 | Deliberately leak futures (let them be GC'd without calling `done()`) | No permanent deadlock of the Jobserver; slots may be temporarily lost until GC |
| 4 | `slots=1`, submit job A (fills slot), submit job B with `timeout=0` | B raises `Blocked`; slot is not corrupted; A completes normally |

### 5.2 Timing Attacks

| # | Scenario | Expected |
|---|----------|----------|
| 1 | `done(timeout=0.001)` in a tight loop until complete | Eventually returns `True`; no spurious exceptions |
| 2 | `submit(timeout=0.001)` when slots transiently available | Either succeeds or raises `Blocked`; no half-submitted state |
| 3 | Call `result(timeout=1e-9)` (near-zero timeout) | Either returns result (if done) or raises `Blocked`; no corruption |
| 4 | Rapid alternation: `submit()`, `done(timeout=0)`, `submit()`, `done(timeout=0)` | All state transitions are consistent |

### 5.3 Nested Submission (Recursive Work)

| # | Scenario | Slots | Expected |
|---|----------|-------|----------|
| 1 | Child submits one grandchild job | N >= 2 | Both complete |
| 2 | Child submits work that submits work (3 levels deep) | N >= 3 | All levels complete |
| 3 | `slots=2`, child tries to submit 2 grandchildren with `timeout=5` | 2 | One grandchild runs; second may block then succeed after first completes |
| 4 | `slots=1`, child tries to submit grandchild with `timeout=0` | 1 | Grandchild raises `Blocked` (parent holds the only slot) |
| 5 | `slots=1`, child tries to submit grandchild with `timeout=2` | 1 | Grandchild raises `Blocked` after timeout (deadlock detected by timeout) |

### 5.4 sleep_fn Adversarial Behavior

| # | Scenario | Expected |
|---|----------|----------|
| 1 | `sleep_fn` always returns `0.01` (always vetoes) with `timeout=1` | `submit()` raises `Blocked` after ~1s |
| 2 | `sleep_fn` returns `0.01` five times then `None` | Submission succeeds after ~50ms delay |
| 3 | `sleep_fn` returns a very large value (e.g., `1000`) with `timeout=1` | `submit()` raises `Blocked` after ~1s, not after 1000s |
| 4 | `sleep_fn` raises an exception | Exception propagates from `submit()` |
| 5 | `sleep_fn` is stateful (uses external condition like file existence) | Works as gating mechanism |

### 5.5 Pickling Edge Cases

| # | Scenario | Expected |
|---|----------|----------|
| 1 | `fn` returns an unpicklable object (e.g., open file handle) | `result()` raises pickling-related exception |
| 2 | `fn` argument is unpicklable | `submit()` raises pickling error |
| 3 | `copy.copy(jobserver)` returns same instance | Identity preserved |
| 4 | `copy.deepcopy(jobserver)` returns same instance | Identity preserved |
| 5 | `pickle.dumps(jobserver)` / `pickle.loads(...)` round-trips | Works; in-flight futures excluded from state |
| 6 | `copy.copy(future)` raises `NotImplementedError` | Cannot copy futures |
| 7 | `pickle.dumps(future)` raises `NotImplementedError` | Cannot pickle futures |

### 5.6 Environment and preexec_fn Edge Cases

| # | Scenario | Expected |
|---|----------|----------|
| 1 | Set `env=[("FOO", "bar")]`, verify `os.environ["FOO"] == "bar"` in child | Correct |
| 2 | Unset variable: `env=[("PATH", None)]` | `PATH` absent in child |
| 3 | Set then unset: `env=[("X", "1"), ("X", None)]` | `X` absent (last write wins) |
| 4 | `preexec_fn` modifies global state (e.g., `os.chdir("/tmp")`) | `fn` sees modified state; parent unaffected |
| 5 | `preexec_fn` raises `RuntimeError` | `result()` raises `RuntimeError` |

---

## 6. JobserverExecutor-Specific Stress Tests

### 6.1 Cancellation Matrix

| # | Scenario | Expected |
|---|----------|----------|
| 1 | `future.cancel()` before dispatcher picks it up | Returns `True`; `future.cancelled()` is `True` |
| 2 | `future.cancel()` after dispatcher started it | Returns `False`; result eventually available |
| 3 | `shutdown(cancel_futures=True)` with 50 pending, 5 running | 50 cancelled, 5 complete normally |
| 4 | `shutdown(cancel_futures=False)` with 50 pending, 5 running | All 55 eventually complete |
| 5 | Cancel, then check `future.result()` | Raises `CancelledError` |

### 6.2 Shutdown Robustness

| # | Scenario | Expected |
|---|----------|----------|
| 1 | `shutdown(wait=True)` blocks until all done | Returns only after all work finished |
| 2 | `shutdown(wait=False)` returns immediately | Returns promptly; work continues in background |
| 3 | Double `shutdown()` | Idempotent; no error |
| 4 | Triple `shutdown()` from 3 threads simultaneously | No crash, no deadlock |
| 5 | `shutdown()` with no submitted work | Clean exit |
| 6 | Submit 1000 jobs, immediately `shutdown(wait=True, cancel_futures=True)` | Some run, rest cancelled; no leaked processes |

### 6.3 Executor Throughput

| # | Scenario | Expected |
|---|----------|----------|
| 1 | Submit 5,000 `lambda: None` jobs, collect all via `as_completed` | All 5,000 futures resolve |
| 2 | 4 threads each submit 250 jobs concurrently | All 1,000 futures resolve; no corruption |
| 3 | Mixed workload: 50% fast (1ms), 50% slow (500ms) | All complete; fast jobs not starved behind slow |

### 6.4 Dispatcher Crash Recovery

| # | Scenario | Expected |
|---|----------|----------|
| 1 | Kill the dispatcher process via `os.kill(pid, SIGKILL)` externally | Outstanding futures get `RuntimeError`; executor becomes unusable; `submit()` raises `RuntimeError` |
| 2 | Dispatcher crashes from unpicklable submission | That future fails; other futures unaffected |

---

## 7. Multiprocessing Context Compatibility

All of the above tests should pass under each supported start method.

| Context | Notes |
|---------|-------|
| `"fork"` | Default on Linux. Fast but unsafe with threads. |
| `"forkserver"` | Safer fork. Extra startup cost. |
| `"spawn"` | Default on macOS/Windows. Requires picklable everything. |

### 7.1 Context-Specific Edge Cases

| # | Scenario | Context | Expected |
|---|----------|---------|----------|
| 1 | Submit a module-level function | `spawn` | Works (picklable) |
| 2 | Submit a closure over local variable | `spawn` | Fails with pickling error (expected) |
| 3 | Submit a `staticmethod` | all | Works |
| 4 | Nested jobserver shared via pickle | `spawn` | Child receives working jobserver sharing same token pool |
| 5 | `Jobserver(context="spawn", slots=2)` explicit construction | `spawn` | Works correctly |

---

## 8. Chaos Monkey Protocol

A scripted chaos run combining multiple stressors simultaneously.

### 8.1 Combined Stress Scenario

```
1. Create Jobserver(slots=4)
2. Submit 200 jobs:
   - 40% return normally after random 0-100ms sleep
   - 20% raise random exceptions
   - 20% call os._exit(1)
   - 10% return large (1 MB) payloads
   - 10% have callbacks registered (half of which raise)
3. Collect all results in a background thread using done(timeout=0.1) polling
4. Simultaneously, a second thread submits 100 more jobs
5. A third thread periodically calls reclaim_resources()
6. After all submitted, call done()/result() on every future
7. Verify:
   - No deadlocks (completes within 120s wall time)
   - No leaked child processes (check /proc or psutil)
   - No leaked file descriptors
   - Every future is in a terminal state
   - Exception types match expected categories
```

### 8.2 Executor Chaos Scenario

```
1. Create JobserverExecutor(Jobserver(slots=4))
2. From 4 threads, each submit 100 jobs:
   - 50% return normally
   - 25% raise exceptions
   - 25% call os._exit(1)
3. A 5th thread randomly calls future.cancel() on pending futures
4. After 2 seconds, call shutdown(wait=True, cancel_futures=True)
5. Verify:
   - All futures are in a terminal state (done, cancelled, or exception)
   - No zombie processes
   - No hanging threads (only main thread remains)
   - Executor rejects new submissions after shutdown
```

### 8.3 Long-Running Soak Test

```
1. Create Jobserver(slots=cpu_count)
2. Run for 10 minutes:
   - Continuously submit work (mix of fast and slow)
   - Continuously drain results
   - Every 30 seconds, snapshot: RSS, open FDs, process count, slot queue depth
3. Verify:
   - All resource metrics remain bounded (no monotonic growth)
   - Slot count returns to full after all work drained
   - No OOM, no FD exhaustion
```

---

## 9. Acceptance Criteria

The system is production-ready when:

1. **100% of baseline tests pass** (Section 1) across all three multiprocessing
   contexts.
2. **All concurrency tests pass** (Section 2) with no deadlocks or data races
   detected under ThreadSanitizer-equivalent analysis.
3. **All error-handling tests pass** (Section 3) with correct exception types
   and no swallowed errors.
4. **Callback semantics are correct** (Section 4) including re-entrant and
   multi-threaded scenarios.
5. **No pathological scenario causes undefined behavior** (Section 5) --
   everything either succeeds or raises a documented exception.
6. **JobserverExecutor passes all cancellation and shutdown tests** (Section 6)
   with no leaked resources.
7. **All tests pass under `fork`, `forkserver`, and `spawn`** (Section 7).
8. **Chaos monkey runs complete** (Section 8) within time bounds with zero
   resource leaks.
9. **Soak test shows bounded resource usage** over 10 minutes of sustained
   load.

---

## 10. Recommended Load Levels

| Level | Slots | Jobs | Duration | Purpose |
|-------|-------|------|----------|---------|
| Smoke | 2 | 10 | <5s | Quick sanity check |
| Light | cpu_count | 100 | <30s | Basic correctness |
| Medium | cpu_count | 1,000 | <2min | Throughput validation |
| Heavy | cpu_count | 10,000 | <10min | Stress and backpressure |
| Soak | cpu_count | continuous | 10min+ | Resource leak detection |
| Extreme | 1 | 10,000 | <30min | Serialized pathological case |

---

## 11. Out of Scope

These tests deliberately avoid:

- Private/internal APIs (only public methods of `Jobserver`, `Future`,
  `JobserverExecutor`)
- Modifying source code or monkey-patching internals
- Platform-specific behavior beyond multiprocessing context differences
- Network or distributed scenarios (this is a local-machine jobserver)
