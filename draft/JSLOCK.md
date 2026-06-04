# Thread-Safety for Jobserver.submit() / reclaim_resources()

Analysis and implementation plan for
[issue #326](https://github.com/RhysU/jobserver/issues/326).

## Status Quo

The `Jobserver` docstring documents:

> Concurrent submit() / reclaim_resources() calls on a Jobserver are not
> thread-safe.  In contrast, returned Futures are thread-safe.

The escape hatch today is `JobserverExecutor`, which guards its own
`submit()` with a `threading.Lock` and funnels work through a
background dispatcher process plus a receiver thread.

## Goal

Make concurrent `submit()` and `reclaim_resources()` on a single
`Jobserver` instance thread-safe, keeping the returned `Future`
thread-safety guarantee intact.

## Shared Mutable State

All thread-safety concerns flow from one shared resource:

| State | Accessed by |
|---|---|
| `_selector` (`DefaultSelector`) | `submit()`, `reclaim_resources()`, `__enter__`, `__exit__`, `__del__` |
| `_selector_pid` | `_lazy_selector()` |
| `_selector_closed` | `_lazy_selector()`, `_selector_close()`, `__exit__` |

`DefaultSelector` wraps `EpollSelector` on Linux.  While `epoll_wait`
and `epoll_ctl` are kernel-safe to call concurrently, Python's
selector maintains an unsynchronized `_fd_to_key` dict.  Therefore
concurrent `select()`, `register()`, and `unregister()` on the same
`DefaultSelector` are **not safe** without external synchronization.

`_slots` (`FixedBytesQueue`) is process-safe by construction (pipe-backed),
and token get/put are already atomic at the OS level.

## Approach: Coarse-Grained `threading.RLock`

### Why RLock

`submit()` → `_maybe_obtain_token()` → `reclaim_tokens_fn()` which is
bound to `self.reclaim_resources`.  This is a re-entrant call path.
`__exit__` also calls `reclaim_resources()` in a loop.  A plain `Lock`
would deadlock on the first nested call; `RLock` permits same-thread
re-acquisition.

### New Instance State

Add two slots:

```python
"_lock",      # threading.RLock
"_lock_pid",  # int — pid that created the lock
```

And a helper mirroring the `_lazy_selector` pattern:

```python
def _ensure_lock(self) -> threading.RLock:
    """Return the lock, creating a fresh one after fork."""
    if self._lock_pid != os.getpid():
        self._lock = threading.RLock()
        self._lock_pid = os.getpid()
    return self._lock
```

After `os.fork()` only the forking thread survives.  If another thread
held the lock at fork time, the inherited `RLock` is permanently stuck.
The pid check detects this and creates a fresh lock.  Because only one
thread exists in the child, the unsynchronized check-and-replace is safe.

### Lock Acquisition Points

#### `reclaim_resources()`

```python
def reclaim_resources(self) -> None:
    with self._ensure_lock():
        ready = self._lazy_selector().select(timeout=0)
        for data in {key.data for key, _ in ready}:
            assert hasattr(data, "done"), type(data)
            data.done()
```

Body unchanged.  `select(timeout=0)` is a non-blocking poll, so the
critical section is microseconds times the number of newly-ready fds.

#### `submit()`

```python
def submit(self, fn, *, ..., timeout=None) -> Future[T]:
    # --- validation (no lock needed) ---
    if not callable(fn): ...
    env = _env_coerce(env)
    args = tuple(args)
    ...

    # --- compute deadline BEFORE acquiring the lock ---
    deadline = timeout_to_deadline(timeout)

    # --- acquire lock, deducting wait from the deadline ---
    lock = self._ensure_lock()
    remaining = deadline_to_timeout(deadline)
    if remaining == float("inf"):
        lock.acquire()
    elif not lock.acquire(timeout=remaining):
        raise Blocked()
    try:
        selector = self._lazy_selector()
        token = _maybe_obtain_token(
            consume=consume,
            deadline=deadline,   # time already ticking
            ...
        )
        # ... process creation, selector.register(), callback setup ...
        return future
    finally:
        lock.release()
```

Key detail: the deadline is computed **before** `lock.acquire()`.
The time spent waiting for the lock is therefore automatically deducted
from `_maybe_obtain_token`'s budget.  If the lock wait alone exhausts
the deadline, `lock.acquire(timeout=remaining)` returns `False` and
we raise `Blocked` without entering the critical section.

Move of the `timeout_to_deadline(timeout)` call earlier in `submit()`
(currently at line 948, inside the `_maybe_obtain_token` call site)
to before the lock acquisition.

#### `__enter__`

```python
def __enter__(self) -> "Jobserver":
    with self._ensure_lock():
        self._lazy_selector()
    return self
```

#### `__exit__`

```python
def __exit__(self, *exc) -> None:
    with self._ensure_lock():
        if self._selector_closed:
            return
        while True:
            try:
                self.reclaim_resources()  # re-entrant via RLock
                break
            except CallbackRaised as e:
                warnings.warn(...)
        self._slots.close_put()
        self._slots.close_get()
        self._selector_close()
```

Holding the lock for the entire drain-and-close sequence prevents a
concurrent `submit()` from racing against shutdown.  A `submit()` that
arrives after `__exit__` releases the lock will see `_selector_closed`
and get `RuntimeError("Jobserver is closed")` from `_lazy_selector()`.

#### `__del__`

```python
def __del__(self) -> None:
    if (tracked := self._tracked()) > 0:
        warnings.warn(...)
    lock = getattr(self, "_lock", None)
    if lock is not None and not lock.acquire(blocking=False):
        return  # another thread is active; it will clean up
    try:
        if hasattr(self, "_slots"):
            self._slots.close_put()
            self._slots.close_get()
        self._selector_close()
    finally:
        if lock is not None:
            lock.release()
```

Non-blocking acquire avoids hanging the finalizer thread.  If the lock
is held, another thread is actively using the instance and cleanup can
wait.  `_tracked()` already tolerates partial construction and a closed
selector, so reading it without the lock is acceptable for a warning.

#### Methods That Do NOT Acquire

| Method | Rationale |
|---|---|
| `map()` | Delegates to `submit()` / `reclaim_resources()` which lock internally. |
| `__repr__` / `_tracked()` | Diagnostic only; already defensive with `getattr` and `None` checks. |
| `__copy__` / `__deepcopy__` | Return `self`; no state mutation. |
| `__call__` | Delegates to `submit()`. |
| `context` property | Returns immutable `_context`. |

### Pickle: `__getstate__` / `__setstate__`

`__getstate__` already excludes `_selector`, `_selector_pid`, and
`_selector_closed`.  Likewise exclude `_lock` and `_lock_pid` — they
are process-local state.  The tuple format grows from 5 to 5 (no
change to the serialized form).

`__setstate__` creates a fresh `RLock` and sets `_lock_pid = os.getpid()`,
exactly paralleling the fresh `_selector = None` / `_selector_pid = None`
it already does.

No change to the pickle wire format.

## Blocking Behavior Under Contention

### Scenario: Two Threads Submitting

```
Thread A: submit(timeout=10)  →  acquires lock  →  _maybe_obtain_token  →  select(timeout=...)
Thread B: submit(timeout=0.5) →  lock.acquire(timeout=0.5)  →  ...
```

Thread B blocks on the lock.  Three outcomes:

1. **A's `select()` returns before B's 0.5 s expires.**  A finishes
   submit, releases lock, B proceeds normally.
2. **B's lock wait expires.**  `lock.acquire()` returns `False`, B
   raises `Blocked`.  B's timeout contract is honored.
3. **A's deadline expires first.**  A raises `Blocked` from inside
   `_maybe_obtain_token`, exits the `finally` block releasing the lock,
   B proceeds.

In all cases, each thread's timeout budget is respected.

### Scenario: submit() Blocking While reclaim_resources() Wanted

Thread A holds the lock inside `_maybe_obtain_token`'s blocking
`select()`.  Thread B wants `reclaim_resources()` to fire a user
callback promptly.  B is blocked until A's `select()` returns.

This is a latency cost, not a correctness issue.  `select()` returns as
soon as any child exits, so the delay is bounded by child execution time
(not A's full timeout).  If this latency matters in practice, the lock
can later be released around the blocking `select()` — see
[Future Optimization](#future-optimization-release-lock-around-blocking-select).

## Lock Ordering and Deadlock Analysis

Two locks participate: `Jobserver._lock` and `Future._rlock`.

### Normal order: `Jobserver._lock` → `Future._rlock`

```
reclaim_resources()          submit()
  acquire Jobserver._lock      acquire Jobserver._lock
    selector.select()            _maybe_obtain_token
    future.done()                  reclaim_resources  [re-entrant]
      Future._rlock.acquire          future.done()
                                       Future._rlock.acquire
```

### Reverse direction: does it occur?

The reverse would be: hold `Future._rlock`, then acquire `Jobserver._lock`.

```
Thread A:  future.wait()
             acquires Future._rlock
             _issue_callbacks()
               user callback calls js.submit()
                 tries to acquire Jobserver._lock      ← blocked if B holds it
Thread B:  js.reclaim_resources()
             acquires Jobserver._lock
             future.done(timeout=0)
               Future._rlock.acquire(timeout=0)        ← non-blocking, returns False
             releases Jobserver._lock
Thread A:  acquires Jobserver._lock                    ← unblocked
```

**No deadlock.**  The critical property is that `Future.done()` (and
`Future.wait()`) acquires `_rlock` with a **timeout** (defaulting to 0
for `done()`).  When the lock is contested, `done()` returns `False`
and the holder moves on.  The contested future gets reclaimed on a
later pass.  This is already tested by
`test_reclaim_resources_with_contested_lock`.

A user callback calling back into `js.submit()` or `js.reclaim_resources()`
from inside `future.wait()` acquires `Jobserver._lock` while holding
`Future._rlock`.  If another thread holds `Jobserver._lock` and tries
`future.done()` on the same future, it gets `False` from the
non-blocking `_rlock` acquisition and releases `Jobserver._lock`,
allowing the first thread to proceed.  No cycle forms.

### Self-deadlock via RLock re-entrance

`submit()` → `_maybe_obtain_token` → `reclaim_resources()` → `done()`:
all on the same thread.  `Jobserver._lock` is an RLock, so the nested
acquisitions succeed.  `Future._rlock` is also an RLock, so if a
callback registers another callback on the same future (acquiring
`_rlock` from `_when_done` while `_issue_callbacks` holds it), that
also succeeds.

## Fork Safety

### spawn / forkserver start methods

The `Jobserver` is pickled via `__getstate__` / `__setstate__`.  A fresh
lock is created in `__setstate__`.  No concern.

### fork start method

The child inherits the parent's memory, including any `RLock` state.
If another thread held the lock at `fork()` time, the child's copy is
permanently stuck — a well-known Python limitation
([CPython issue](https://bugs.python.org/issue6721)).

`_ensure_lock()` detects the pid change and creates a fresh lock before
any acquisition.  Because only the forking thread survives, the
unsynchronized check is safe.  This mirrors the existing `_lazy_selector`
fork-rebuild pattern.

**Constraint that already exists:** Python's own documentation warns
against mixing `os.fork()` with threads.  The `Jobserver` fork-rebuild
is a best-effort recovery, not a guarantee for arbitrary thread+fork
combinations.

## Edge Cases

### Partially Constructed Instance

If `__init__` raises after `_slots` is created but before `_lock` is
assigned, `__del__` uses `getattr(self, "_lock", None)` to handle the
missing attribute gracefully.  Same pattern as the existing
`_tracked()` / `hasattr(self, "_slots")` guards.

### Callbacks That Raise

No change to semantics.  `CallbackRaised` propagates out of
`reclaim_resources()` or `submit()`.  The lock is released via the
`finally` block before the exception reaches the caller.

### `__exit__` Racing With `submit()`

`__exit__` holds the lock for the entire drain-close sequence.  A
concurrent `submit()` that loses the lock race will block, then see
`_selector_closed = True` from `_lazy_selector()` and raise
`RuntimeError("Jobserver is closed")`.

### GC / Finalizer Thread

`__del__` uses non-blocking `lock.acquire(blocking=False)`.  If the
lock is held, cleanup is skipped — the active thread will handle it.

### `timeout=None` (Block Indefinitely)

`deadline_to_timeout(deadline)` returns `float("inf")`.  We use
unconditional `lock.acquire()` in this case (no timeout argument),
matching the existing "block forever" semantics.

## Future Optimization: Release Lock Around Blocking select()

If profiling shows that the coarse lock's serialization of `submit()`
calls harms throughput, the lock can be released around the blocking
`selector.select()` in `_maybe_obtain_token`:

```python
lock.release()
try:
    keys_events = selector.select(timeout=remaining)
finally:
    lock.acquire()
```

On Linux, `epoll_wait` and `epoll_ctl` are kernel-synchronized, so
concurrent `select()` + `register()`/`unregister()` is safe at the
syscall level.  However, Python's `DefaultSelector._fd_to_key` dict
is **not** thread-safe, so `register()` and `unregister()` must still
be serialized under the lock.  The optimization is: hold the lock for
`register`/`unregister`, release it for `select`.

This is safe because:
- `register()` and `unregister()` only happen inside `submit()` and
  inside callbacks fired by `done()` — both under the lock.
- `select()` only reads the interest set; it does not mutate `_fd_to_key`.

Deferred until the coarse lock proves insufficient.

## Docstring Update

Replace:

> Concurrent submit() / reclaim_resources() calls on a Jobserver are not
> thread-safe.  In contrast, returned Futures are thread-safe.

With:

> Concurrent submit() / reclaim_resources() calls on a Jobserver are
> thread-safe.  Returned Futures are also thread-safe.

## Tests To Add

All in `test/test_jobserver_concurrency.py`:

1. **`test_concurrent_submit_from_multiple_threads`** — N threads
   (e.g. 4) submit through a Jobserver with limited slots (e.g. 2);
   all futures complete, no crashes or assertion errors.

2. **`test_concurrent_submit_and_reclaim`** — One thread submitting in
   a loop, another calling `reclaim_resources()` in a loop; no crashes.

3. **`test_concurrent_submit_timeout_respected`** — With the lock held
   by a thread blocking in `submit(timeout=long)`, another thread's
   `submit(timeout=short)` raises `Blocked` within a reasonable
   wall-clock bound.

4. **`test_concurrent_exit_blocks_submit`** — After `__exit__`, a
   concurrent `submit()` raises `RuntimeError`.

## Alternatives Considered but Rejected

### A. Keep Not-Thread-Safe, Document Better

Leave `submit()` / `reclaim_resources()` unsynchronized.  Update the
docstring to more prominently direct users to `JobserverExecutor` for
multi-threaded use.

**Rejected because:** the issue explicitly asks to revisit the decision,
and the locking cost is modest for a class whose hot path is "spawn a
child process."  The RLock acquisition overhead (tens of nanoseconds)
is negligible next to `Process.start()` (milliseconds).  Providing
thread-safety at the `Jobserver` level makes the simpler API safe by
default rather than requiring users to discover the executor wrapper.

### B. Per-Method `threading.Lock` (Non-Reentrant)

Use a plain `Lock` instead of `RLock`.  Restructure `submit()` so it
does not call `reclaim_resources()` under the lock — e.g., release the
lock before entering `_maybe_obtain_token`, or have `_maybe_obtain_token`
call a lock-free internal reclaim helper.

**Rejected because:** the re-entrant call path (`submit` →
`_maybe_obtain_token` → `reclaim_tokens_fn` → `reclaim_resources`) is
deeply embedded in the token-acquisition loop.  Splitting the loop
into locked and unlocked phases would complicate the logic, introduce
subtle gaps where the selector is unsynchronized, and make the code
harder to reason about — all to avoid `RLock`, whose only cost is one
extra owner-thread check per acquisition.

### C. Fine-Grained Locking (Separate Locks per Resource)

Use one lock for `_selector` access and another for `_slots` access,
reducing contention by allowing concurrent slot operations and selector
mutations.

**Rejected because:** `_slots` (`FixedBytesQueue`) is already
process-safe (pipe-backed, OS-atomic reads and writes).  It needs no
Python-level lock.  The only resource that needs synchronization is the
`DefaultSelector` and its associated bookkeeping (`_selector_pid`,
`_selector_closed`).  A single lock for that resource is the
minimum — splitting it further adds complexity with no benefit.

### D. Replace DefaultSelector With a Thread-Safe Wrapper

Wrap `DefaultSelector` in a class that synchronizes `select()`,
`register()`, and `unregister()` internally, keeping `Jobserver`
methods lock-free.

**Rejected because:** `select()` can block, so the wrapper would need
the same timeout-aware acquisition and blocking-select tradeoffs that
the coarse lock addresses.  It pushes the same problem into a different
class without simplifying it.  Additionally, the selector is an
implementation detail of `Jobserver`; encapsulating its locking there
keeps the synchronization policy in one place.

### E. Lock-Free Design With `queue.SimpleQueue` Dispatch

Replace the selector-based architecture with a `SimpleQueue` that
worker threads pull from, similar to `concurrent.futures.ThreadPoolExecutor`.
Each `submit()` enqueues work; a dispatcher thread handles process
creation and completion.

**Rejected because:** this is `JobserverExecutor`.  It already exists.
The goal of issue #326 is to make the simpler, lower-level `Jobserver`
API safe for concurrent use, not to duplicate the executor's architecture.

### F. Release Lock Around Blocking `select()` From Day One

Hold the lock for `register()` / `unregister()` but release it around
`selector.select()` in `_maybe_obtain_token`, relying on the kernel's
thread-safety for `epoll_wait`.

**Rejected as the initial approach** (but preserved as a future
optimization path in the plan) because:

1. Python's `DefaultSelector._fd_to_key` dict is read by `select()` to
   build the return value.  A concurrent `register()` or `unregister()`
   mutating that dict during `select()`'s iteration would be a race at
   the Python level, even though the underlying `epoll_wait` is safe.
2. The release-reacquire pattern introduces a window where another
   thread can mutate the selector, requiring `_maybe_obtain_token` to
   re-validate state after reacquiring.  This adds complexity for a
   marginal gain — the coarse lock serializes `submit()` calls, but
   each `select()` wakes promptly when a child exits, so the
   serialization delay is bounded by child execution time, not by
   the full timeout.
3. If profiling later shows the coarse lock is a bottleneck, this
   optimization can be added incrementally without changing the
   external API or test contracts.

## Implementation Checklist

- [ ] Add `"_lock"` and `"_lock_pid"` to `__slots__`
- [ ] Initialize both in `__init__`
- [ ] Add `_ensure_lock()` method
- [ ] Wrap `submit()` with lock acquisition (deadline-aware)
- [ ] Wrap `reclaim_resources()` with lock acquisition
- [ ] Wrap `__enter__` with lock acquisition
- [ ] Wrap `__exit__` with lock acquisition
- [ ] Update `__del__` with non-blocking lock acquisition
- [ ] Update `__getstate__` (no change to tuple — lock excluded)
- [ ] Update `__setstate__` to create fresh lock
- [ ] Update class docstring
- [ ] Add concurrency tests
- [ ] Run full test suite
