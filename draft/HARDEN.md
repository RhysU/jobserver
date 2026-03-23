# Hardening Plan for `JobserverExecutor`

A specification for testing and hardening the `concurrent.futures.Executor`
implementation in `draft/`.  Informed by CPython's own
`Lib/test/test_concurrent_futures/` suite, known CPython bug reports, and
common pitfalls in process-pool executor implementations.

---

## 1. Future State-Machine Correctness

The `concurrent.futures.Future` state machine has five states
(`PENDING`, `RUNNING`, `CANCELLED`, `CANCELLED_AND_NOTIFIED`, `FINISHED`)
and a small set of valid transitions.  Every invalid transition must raise
`InvalidStateError`.

### 1.1 Deterministic state-transition tests

Create futures in known states (without a live executor) and verify:

| Start state | Operation | Expected result |
|---|---|---|
| PENDING | `cancel()` | returns `True`; state becomes CANCELLED |
| PENDING | `set_running_or_notify_cancel()` | state becomes RUNNING |
| RUNNING | `cancel()` | returns `False`; state unchanged |
| RUNNING | `set_result(v)` | state becomes FINISHED; `result()` returns `v` |
| RUNNING | `set_exception(e)` | state becomes FINISHED; `exception()` returns `e` |
| FINISHED | `set_result(v)` | raises `InvalidStateError` |
| FINISHED | `set_exception(e)` | raises `InvalidStateError` |
| CANCELLED | `set_running_or_notify_cancel()` | fires cancel callbacks |
| CANCELLED | `result()` | raises `CancelledError` |
| FINISHED | `cancel()` | returns `False` |

These tests should run without spawning any processes; they exercise the
c.f.Future object directly and confirm `JobserverExecutor._receive_loop`
can rely on the documented state machine.

### 1.2 Double-set guards

Verify that calling `set_result()` or `set_exception()` a second time on
the same future raises `InvalidStateError`.  This protects against the
receiver accidentally completing a future twice (e.g. a `Completed` message
arriving after a `Failed` for the same `work_id`).

### 1.3 State queries at each stage

For a live executor submission, sample:
- `f.done()`, `f.running()`, `f.cancelled()` while PENDING
- The same while RUNNING (use a barrier/event to hold the worker)
- The same after completion

---

## 2. Shutdown Semantics

CPython's `test_shutdown.py` is the richest source of edge cases.
Each test below should be run for every available `multiprocessing`
start method (`fork`, `forkserver`, `spawn`).

### 2.1 `shutdown(wait=True)` blocks until all futures complete

Submit several slow tasks, call `shutdown(wait=True)`, and assert all
futures are done before the call returns.

### 2.2 `shutdown(wait=False)` returns immediately

Submit a slow task, call `shutdown(wait=False)`, record the wall-clock
time of the call.  Assert the call returns in < 0.5 s.  Assert the
future eventually completes.

### 2.3 `shutdown(cancel_futures=True)` cancels pending work

Fill slots with slow tasks, submit additional work, then
`shutdown(cancel_futures=True)`.  Assert:
- Running futures are **not** cancelled (cancel returns False).
- Pending futures **are** cancelled.
- `result()` on cancelled futures raises `CancelledError`.

### 2.4 Submit-after-shutdown raises `RuntimeError`

Call `shutdown()`, then `submit()`.  Expect `RuntimeError`.

### 2.5 Double shutdown is safe

Call `shutdown()` twice in succession; no exception should be raised.

### 2.6 Concurrent submit and shutdown

Use a `threading.Barrier` so that one thread calls `submit()` while
another calls `shutdown()` at the same instant.  The submit must either
succeed (and the future eventually completes) or raise `RuntimeError`.
No hang, no crash.  Repeat N=100 times.

### 2.7 Context-manager exit calls `shutdown(wait=True)`

Verify `__exit__` path, and that subsequent `submit()` raises
`RuntimeError`.

### 2.8 `shutdown(wait=False)` with `cancel_futures=True` combined

The combination must not deadlock.  All pending futures should be
cancelled; running futures should complete normally.

---

## 3. Callback Semantics

### 3.1 `add_done_callback` fires on success, failure, cancellation

Register a callback before the future completes and verify it fires in
each terminal state.

### 3.2 Callback on already-done future fires immediately

Wait for a future to finish, then add a callback; it must fire
synchronously in the caller's thread.

### 3.3 A raising callback does not prevent subsequent callbacks

Register three callbacks where the second raises.  Verify the first and
third still fire.  (Note: `concurrent.futures.Future` logs exceptions
from callbacks but does not suppress them for other callbacks.)

### 3.4 Callback ordering

Callbacks must fire in registration order per the `concurrent.futures`
contract.

### 3.5 Callback receives the correct future

Each callback's argument must be the future on which it was registered.

---

## 4. Pickling / Serialization Boundary

The dispatcher process boundary requires pickling of `_request.Submit`
(including `fn`, `args`, `kwargs`) and `_response.*` messages.  Each
serialization failure point must be tested.

### 4.1 Unpicklable callable

Submit a lambda or a closure that cannot be pickled (with `spawn`
context where globals are not inherited).  The resulting future must
surface an exception, not hang.

### 4.2 Unpicklable arguments

Submit `len` with an argument containing an unpicklable object
(e.g. a lock).  Same expectation.

### 4.3 Unpicklable result

Submit a callable that returns an unpicklable object (e.g. a generator).
The future must surface an exception.

### 4.4 Very large objects

Submit work that passes and/or returns large objects (e.g. 10 MB, 100 MB
byte strings).  Verify correct results and no hang.  This exercises the
pipe capacity limits -- `Connection.send()` can block or raise
`ValueError` for very large objects.

### 4.5 Pickle failure during shutdown

Inject a pickling failure (via `unittest.mock.patch` on
`MinimalQueue.put`) during `shutdown()`.  The `BrokenPipeError` handler
must not hang.

---

## 5. Worker / Dispatcher Death

### 5.1 Worker killed by signal

Submit a task that sends `SIGKILL` to itself.  The future must raise an
exception (not hang).  A subsequent submission must succeed, proving the
executor recovered.

### 5.2 Worker exits via `sys.exit()`

Submit `sys.exit(1)`.  The future must raise.  Executor must remain
usable.

### 5.3 Dispatcher process killed externally

While futures are in flight, send `SIGKILL` to `_dispatcher.pid`.
All outstanding futures must eventually raise
`RuntimeError("dispatcher process terminated")`.
No deadlock in `shutdown()`.

### 5.4 Dispatcher crash during large data transfer

Submit many large-payload tasks, then kill the dispatcher mid-stream.
All futures must resolve (with errors) and shutdown must complete.

### 5.5 Parent process dies (orphan detection)

The dispatcher closes unused pipe ends so that EOF propagates on parent
death.  Verify: if the parent process is killed, the dispatcher's
`request_queue.get()` sees `EOFError` and exits.
(Test by forking a child that creates the executor, kills itself, and
the test harness verifies no leaked dispatcher/worker processes.)

---

## 6. Concurrency Stress Tests

### 6.1 Heavy submission exceeding slot count

Submit N >> slots tasks.  Verify all N results are correct and arrive
in bounded time.  Current test uses N=20; increase to N=200 or N=1000.

### 6.2 Rapid submit/cancel churn

In a tight loop, submit and immediately cancel futures.  Verify no
deadlock and no resource leak (process count returns to baseline).
This targets the `PENDING -> cancel() -> Cancelled response` path
racing with `Started` responses.

### 6.3 Mixed success/failure/cancel/death workload

Submit a mix of: successful tasks, tasks that raise, tasks that are
cancelled, tasks whose workers die.  Run all concurrently and verify
every future resolves correctly.

### 6.4 Concurrent `submit()` from multiple threads

Spawn T threads each submitting M tasks.  Verify all T*M results and
that `_work_ids` produces unique IDs under contention.

### 6.5 `sys.setswitchinterval(1e-6)` stress

Run the submit/shutdown race test (2.6) and the multi-threaded submit
test (6.4) with the GIL switch interval set to 1 microsecond to
maximize thread-switching and expose races.

### 6.6 Slot exhaustion with nested submissions

If the underlying `Jobserver` is shared and the executor's dispatcher
submits work that itself uses the same `Jobserver`, slots can be
exhausted.  Verify this either deadlocks predictably (documented) or
is handled gracefully.

---

## 7. `map()` and Iteration

### 7.1 Basic correctness

`list(executor.map(str, range(100)))` == `[str(i) for i in range(100)]`.

### 7.2 Exception propagation

When the k-th invocation raises, iterating the result raises that
exception at position k.

### 7.3 Timeout

`executor.map(time.sleep, [5], timeout=0.1)` raises
`concurrent.futures.TimeoutError`.

### 7.4 Empty iterables

`list(executor.map(str, []))` == `[]`.

### 7.5 Unequal-length iterables

Verify `map()` stops at the shortest iterable, matching `builtins.map`.

### 7.6 Memory: iterator does not retain completed futures

After yielding a result from `map()`, the corresponding future should
be eligible for garbage collection.  Use `weakref.ref` to confirm.

### 7.7 Generator not fully consumed

Create an iterator via `map()`, consume only the first element, then
let the iterator be garbage collected.  Verify no leaked processes.

---

## 8. `wait()` and `as_completed()` Integration

### 8.1 `wait(FIRST_COMPLETED)` returns on first done future

Submit slow and fast tasks; `wait(FIRST_COMPLETED)` should return as
soon as the fast task finishes.

### 8.2 `wait(FIRST_EXCEPTION)` returns on first exception

Submit a mix of successful and failing tasks.  `wait(FIRST_EXCEPTION)`
should return as soon as any task raises.

### 8.3 `wait(ALL_COMPLETED)` waits for everything

All futures should be in `done` set.

### 8.4 `wait()` with timeout

Submit slow tasks, call `wait(timeout=0.1)`.  Some futures should be
in `not_done`.

### 8.5 `as_completed()` yields in completion order

Submit tasks with varying durations; verify that `as_completed()` yields
faster tasks first.

### 8.6 Duplicate future in `wait()` / `as_completed()`

Pass the same future twice.  Verify it appears only once in the result
(CPython `test_20369`).

### 8.7 `as_completed()` does not retain references

Use `weakref.ref` to confirm futures are GC-eligible after being
yielded (CPython `test_free_reference_yielded_future`).

---

## 9. Resource Leak Detection

### 9.1 Process count returns to baseline after shutdown

Before creating the executor, count processes.  After `shutdown()`,
assert the count returns to the original value (within a timeout).

### 9.2 File descriptor count returns to baseline

Use `/proc/self/fd` (Linux) to count open file descriptors before and
after an executor lifecycle.  No leak.

### 9.3 Thread count returns to baseline

`threading.active_count()` before and after.  The receiver thread must
exit after `shutdown(wait=True)`.

### 9.4 `_futures` dict is empty after shutdown

After `shutdown(wait=True)`, assert `len(exe._futures) == 0`.

### 9.5 Repeated create/shutdown cycles

Create and shut down the executor 50 times in a loop.  Monitor for
monotonically increasing resource consumption (processes, FDs, memory).

---

## 10. Multiprocessing Start Method Coverage

All tests in sections 1--9 should be parameterized over every available
start method: `fork`, `forkserver`, `spawn`.  The current test suite
defaults to `fork` for speed; a hardening pass must cover all methods
because:

- `spawn` requires full picklability (catches issues `fork` hides).
- `forkserver` has different process-tree topology and different
  EOF/broken-pipe propagation characteristics.
- `fork` can inherit locks in inconsistent states from threaded parents.

Use `@parameterized` or `subTest` to run each test under each method.

---

## 11. Timing and Deadlock Detection

### 11.1 Global test timeout

Every test must complete within a hard timeout (e.g. 60 s).  Use
`unittest`'s `timeout` support or a `signal.alarm`-based decorator.
A test that exceeds the timeout is treated as a deadlock.

### 11.2 Targeted short timeouts

For tests that are expected to be fast (e.g. `shutdown(wait=False)`),
assert wall-clock time < 1 s.

### 11.3 CI deadlock canary

Add a CI step that runs the full test suite under a 5-minute wall-clock
limit.  If any test hangs, the CI job is killed and reported as failed.

---

## 12. Lock Discipline and Internal Invariants

### 12.1 `_lock` is not held during `put()`

Already tested (`test_lock_released_before_put`).  Extend to cover the
`shutdown()` path: verify `_lock` is released before `_request_queue.put`
of `Cancel` and `Shutdown` messages.

### 12.2 `_lock` is not held during `_response_queue.get()`

The receiver thread must never hold `_lock` while blocking on the
response queue.  Instrument with a spy to verify.

### 12.3 `_futures` cleanup on `put()` failure

Already tested (`test_submit_removes_future_on_put_failure`).  Extend
to cover `BrokenPipeError` specifically (the path that translates to
`RuntimeError("cannot submit after shutdown")`).

### 12.4 No lock ordering violations

Document the lock ordering: `_lock` is always acquired before any queue
operation, and released before the queue operation.  A test or assertion
should verify this invariant is never violated.  Consider a debug-mode
wrapper around `_lock` that records acquire/release and checks ordering.

---

## 13. Edge Cases from CPython Bug Reports

### 13.1 `#105829` -- Wakeup pipe full

Submit many tasks in rapid succession (thousands).  The internal pipe
must not fill up and deadlock.  If the implementation uses a bounded
pipe, verify backpressure or overflow handling.

### 13.2 `#76757` / `#65208` -- GC during queue operations

Force garbage collection (`gc.collect()`) while the executor is actively
processing submissions.  No deadlock.

### 13.3 `#107219` -- `InvalidStateError` during crash cleanup

Kill a worker while the receiver is processing its `Started` message.
The `Failed` message for the same `work_id` must not attempt
`set_exception()` on an already-done future.

### 13.4 `#109934` -- `wait()` hangs after `cancel_futures`

After `shutdown(cancel_futures=True)`, call
`concurrent.futures.wait(futures, timeout=5)`.  Must not hang.

### 13.5 `#56665` -- Early hangs

Submit a single trivial task immediately after construction.
Must not hang.

### 13.6 `#89184` -- Race in forked children

Create the executor in a forked child process.  Must not inherit stale
lock state from the parent.  (Relevant when start method is `fork`.)

---

## 14. `_dispatch_loop` Internal Testing

The dispatcher runs in a separate process, making it hard to test in
isolation.  Consider:

### 14.1 Unit-test `_drain_requests` in-process

Construct `MinimalQueue` pairs, enqueue known `_request` messages, and
call `_drain_requests` directly.  Verify `pending` list and return value.

### 14.2 Unit-test `_dispatch_pending` with a mock Jobserver

Inject a `Jobserver` that raises `Blocked` after N submissions.  Verify
`still_pending` and `in_flight` are correct.

### 14.3 Unit-test `_poll_in_flight` with mock Futures

Create `JobserverFuture` mocks that report `done()` and verify
`_bridge_result` produces correct `_response.Completed` / `Failed`.

### 14.4 Unit-test `_handle_shutdown`

Verify pending items are cancelled, in-flight items are drained, and
`_response.Shutdown` is sent.

### 14.5 `_poll_requests_briefly` timeout behavior

Verify the 5 ms timeout does not cause busy-spinning (measure CPU usage
over 1 s with no pending work).

---

## 15. Implementation Checklist

The following items are not tests but rather code-review items to verify
or fix before declaring the implementation hardened:

- [ ] `daemon=False` on dispatcher -- correct, prevents orphan workers.
      Verify workers spawned by the dispatcher also use `daemon=False`.
- [ ] Pipe-end closure -- verify `_reader`/`_writer` closes are symmetric
      between parent and dispatcher.
- [ ] `_receive_loop` handles every `_response` variant -- no unhandled
      message type silently dropped.
- [ ] `_dispatch_loop` handles every `_request` variant -- same.
- [ ] `_bridge_result` catches `BaseException` or only `Exception`?
      If a worker raises `SystemExit` or `KeyboardInterrupt`, does it
      propagate correctly?
- [ ] The `Cancel` message only cancels `pending` items in the dispatcher.
      In-flight items continue.  Verify this matches the `cancel_futures`
      contract (running tasks must not be cancelled).
- [ ] `_poll_requests_briefly` returns `False` on `EOFError` -- should
      this be `True` (treat parent death as shutdown)?
- [ ] Thread safety of `_futures` dict -- all accesses are under `_lock`,
      including in `_receive_loop`.  Verify no unguarded path.
