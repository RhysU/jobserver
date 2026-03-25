# Thread-Safety for `Future`: Design Notes

Adding a `threading.RLock` to `Future` to protect its mutable state
(`_process`, `_connection`, `_wrapper`, `_callbacks`) from concurrent
access by multiple threads.  The `absolute_deadline` helper is used
throughout to convert relative timeouts into absolute deadlines,
following the same convention already established by `MinimalQueue.get`.

## Why `RLock`?

Callbacks registered via `when_done` are executed inside
`_issue_callbacks`, which is called under the lock.  Existing tests
(specifically `helper_check_semantics`) demonstrate that callbacks
re-enter the same `Future`: a callback calls `f.done(timeout=0)` and
`f.when_done(...)` on the `Future` currently firing its callbacks.  A
plain `Lock` would deadlock on these re-entrant calls.  An `RLock`
permits the same thread to re-acquire, preserving the existing
single-threaded callback semantics:

- Callbacks execute serially in registration order.
- Only one thread runs `_issue_callbacks` at a time; the losing
  thread's `done()` takes the `_connection is None` fast-path and
  finds no callbacks left to process.
- The `CallbackRaised` partial-processing contract is preserved
  exactly: remaining callbacks are truly "remaining" because no other
  thread is concurrently draining them.

The cost is that a slow or blocking callback holds the `RLock`,
preventing all other threads from entering `done`, `when_done`, or
`result` for the duration.  This is acceptable because callbacks are
expected to be lightweight and non-blocking.

## Subtlety: Thundering herd on `poll()` (Issue #38)

This is the motivating bug.  Without locking, two threads calling
`done()` concurrently both enter `self._connection.poll(timeout)`.
When data arrives, both threads wake up.  Only one can successfully
`recv()`.  The other sees an `EOFError` (pipe already consumed) or
hits the `assert self._process is not None` after the winning thread
set `_process = None`.

The `test_concurrent_done_no_crash` test reproduces this: its FIXME'd
assertion (`self.assertEqual(errors, [], ...)`) must be uncommented
once locking is in place.

The fix: the lock serializes `poll()` + `recv()` as an atomic critical
section.  Only one thread polls at a time.  The losing thread blocks
on lock acquisition, and when it eventually enters, it sees
`_connection is None` and takes the fast-path.

## Subtlety: Timeout budget consumed by lock acquisition

When the lock serializes entry into `done()`, a thread waiting for the
lock burns through its caller's timeout budget.  The implementation
must convert `timeout` to an absolute deadline *before* attempting lock
acquisition, then subtract elapsed time before calling `poll()`:

```python
deadline = absolute_deadline(relative_timeout=timeout)
remaining = max(0, deadline - time.monotonic())
if not self._lock.acquire(timeout=remaining):
    return False
# ...
remaining = max(0, deadline - time.monotonic())
if not self._connection.poll(remaining):
    return False
```

This is the same pattern already used in `MinimalQueue.get` (lines
285-289 of `impl.py`), which provides a convenient template.

## Subtlety: `poll()` with non-positive remaining time

After acquiring the lock, `deadline - time.monotonic()` can be zero or
negative if the thread waited a long time for the lock.  The `max(0,
...)` clamp ensures `poll` receives a non-negative timeout, producing
a non-blocking check that correctly returns `False` when the budget is
exhausted.

## Subtlety: `timeout=0` and a contested lock

A non-blocking `done(timeout=0)` computes a deadline approximately
equal to `time.monotonic()`.  If another thread holds the lock, then
`self._lock.acquire(timeout=remaining)` where `remaining` is zero (or
near-zero) will fail immediately, causing `done` to return `False`
even if the `Future` is already done.

This is semantically correct: "I could not confirm completion within
zero seconds."  But it is a behavioral change from the lock-free
version, where `done(timeout=0)` always checked `_connection is None`
without contention.  Callers that depend on `done(timeout=0)` as a
truly instantaneous check should be aware of this.

## Subtlety: `when_done()` race with the "done" transition

Without locking, this interleaving is possible:

1. Thread A (in `done()`) sets `self._connection = None`.
2. Thread B (in `when_done()`) checks `self._connection is None` and
   sees `True`, but has not yet appended the callback.
3. Thread A calls `_issue_callbacks()`, finds an empty list, returns.
4. Thread B appends the callback.
5. Nobody fires Thread B's callback.

The lock must make the append and `_connection is None` check in
`when_done` atomic:

```python
def when_done(self, fn, *args, __internal=False, **kwargs):
    with self._lock:
        self._callbacks.append(...)
        if self._connection is None:
            self._issue_callbacks()
```

Because callbacks re-enter `when_done` (see `helper_check_semantics`),
this requires `RLock`.

## Subtlety: Re-entrant `when_done` nests `_issue_callbacks`

A callback calling `done()` re-enters and acquires the `RLock`
recursively -- fine.  But a callback calling `when_done()` is more
subtle: `when_done` appends a new callback, sees `_connection is
None`, and calls `_issue_callbacks()` *inside* the outer
`_issue_callbacks()` loop.  That inner invocation drains callbacks
(including the just-appended one) before the outer loop gets back to
checking `while self._callbacks`.  The outer loop then finds the list
empty sooner than expected and terminates.

This is correct -- every registered callback fires exactly once, in
registration order -- but it is surprising nested behavior.
`helper_check_semantics` exercises this exact pattern: inside a
callback it calls `f.when_done(...)` twice, and both fire immediately
via the nested `_issue_callbacks` invocation before control returns to
the outer loop.

## Subtlety: `_callbacks` list mutation from multiple threads

`when_done` appends to `_callbacks`; `_issue_callbacks` pops from it.
Without synchronization this is a data race on the list.  Both methods
must execute under the lock.  With `RLock`, `_issue_callbacks` holds
the lock for the entire drain loop, and any `when_done` call from
within a callback re-acquires the same `RLock` to append and
immediately fire.

## Subtlety: `process.join()` under the lock

`process.join()` waits for the child process to fully terminate.
Since it is only called *after* `recv()` succeeds (the child has
already `send()`-ed its result and `close()`-d its pipe end), it
should return nearly instantly.  Holding the lock during `join()`
briefly delays other threads but keeps the entire not-done-to-done
transition atomic.  Moving `join()` outside the lock would require
additional machinery to ensure exactly one thread calls it.

## Subtlety: `result()` reads `_wrapper` after `done()` returns

`result()` calls `self.done(timeout)` then reads
`self._wrapper.unwrap()`.  The lock release inside `done()` provides a
happens-before edge (Python's `RLock.release` is a memory barrier), so
`_wrapper` is safely visible.  Additionally, once `_wrapper` is set it
is never mutated again, so no lock is needed for the read in `result`.

## Subtlety: Internal callbacks access `Jobserver` state

The internal callbacks registered by `Jobserver.submit` at lines
454-458 are `self._slots.put(*tokens)` and
`self._future_sentinels.pop(future)`.  These execute under the
`Future`'s `RLock` (inside `_issue_callbacks`).  `MinimalQueue.put()`
has its own `_write_lock` so it is safe.
`self._future_sentinels.pop()` accesses the `Jobserver`'s dict.  If
`Jobserver` methods are also called from multiple threads, that dict
needs its own protection, but that is a separate concern from
`Future`'s locking.

## Subtlety: `reclaim_resources()` and `submit()` interaction

`Jobserver.submit(callbacks=True)` calls `reclaim_resources()`, which
iterates `tuple(self._future_sentinels.keys())` and calls
`future.done(timeout=0)` on each.  If another thread is already
inside `done()` on one of those Futures (holding the `RLock`), the
`reclaim_resources` thread's `done(timeout=0)` will fail to acquire
the lock and return `False`.  This is correct: that Future is being
finalized by the other thread and its resources will be reclaimed
momentarily.

## Subtlety: The `_issue_callbacks` invariant assert

The assert `self._connection is None and self._process is None` at the
top of `_issue_callbacks` reads mutable state.  Under the `RLock`,
this assert is checked while the lock is held, ensuring a consistent
view.  The assert also serves as documentation that `_issue_callbacks`
is only reachable after the done-transition has completed.

## Subtlety: `CallbackRaised` propagation across threads

When a non-internal callback raises an exception, `_issue_callbacks`
wraps it in `CallbackRaised` and propagates it to the caller of
`done()`.  With `RLock`, only the thread that "won" the done-
transition (or the thread calling `when_done` on an already-done
Future) will ever execute callbacks.  The losing thread sees
`_connection is None`, enters `_issue_callbacks`, finds an empty
callback list, and returns `True` without raising.  This means
`CallbackRaised` is always delivered to exactly one thread, preserving
the existing contract.

## Subtlety: `__copy__` and `__reduce__`

These methods always raise `NotImplementedError` and do not access
mutable state in a way that requires locking.  No changes needed.

## Summary of changes required

1. Add `import threading` to `impl.py`.
2. Add `_lock` to `Future.__slots__`.
3. Initialize `self._lock = threading.RLock()` in `__init__`.
4. In `done()`: compute deadline via `absolute_deadline()` before
   acquiring the lock; acquire with `timeout=max(0, deadline -
   time.monotonic())`; under the lock, check fast-path, poll with
   adjusted remaining time, read/join/close, and issue callbacks.
5. In `when_done()`: acquire the lock around the append and the
   `_connection is None` check plus `_issue_callbacks` call.
6. `_issue_callbacks()` is unchanged in structure; it runs under the
   lock because its callers (`done` and `when_done`) hold it.
7. In `test.py`: uncomment the FIXME assertion in
   `test_concurrent_done_no_crash`.
