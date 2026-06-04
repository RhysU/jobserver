# Thread-Safety for Jobserver.submit() / reclaim_resources()

Analysis and implementation plan for
[issue #326](https://github.com/RhysU/jobserver/issues/326),
using a **self-synchronizing selector wrapper** (the design previously
catalogued as "Alternative D").

## Status Quo

The `Jobserver` docstring documents:

> Concurrent submit() / reclaim_resources() calls on a Jobserver are not
> thread-safe.  In contrast, returned Futures are thread-safe.

The escape hatch today is `JobserverExecutor`.  Crucially, the executor's
own `submit()` is thread-safe only because it **never calls
`jobserver.submit()` from multiple threads**: it enqueues a request on an
`SPSCQueue`, and a single background **dispatcher process** (`_dispatch_loop`)
is the lone caller of `jobserver.submit()`.  A daemon receiver thread only
ever touches `concurrent.futures.Future` objects.  So no existing code path
exercises concurrent `jobserver.submit()` — and, by extension, no existing
code performs concurrent `multiprocessing` `Process.start()` through a
single `Jobserver`.  Any plan that makes `Jobserver` itself thread-safe is
breaking genuinely new ground on **two** fronts, not one (see below).

## Goal

Make concurrent `submit()` and `reclaim_resources()` on a single
`Jobserver` instance thread-safe, keeping the returned `Future`
thread-safety guarantee intact — including the common pattern where one
thread submits while another thread independently drives a returned
Future's completion via `future.result()` / `wait()` / `done()`.

## Why a wrapper, not a coarse `Jobserver._lock`

A coarse `threading.RLock` wrapping `submit()` / `reclaim_resources()` /
`__enter__` / `__exit__` looks simpler, but it has a **correctness hole**
that this plan exists to close.

The shared resource is the `DefaultSelector` and its unsynchronized
`_fd_to_key` dict.  `submit()` mutates it via `selector.register()`
(`_jobserver.py:975-976`).  But it is **also** mutated by the Future
completion callbacks `_unregister_sentinel` / `_unregister_connection`
(`_jobserver.py:542-572`), which call `selector.unregister()`.  Those
callbacks fire from `Future._issue_callbacks()`, reached by **any** call to
`Future.wait()` / `done()` / `result()` — including a user calling
`future.result()` on its own thread, with no `Jobserver` lock held.

A lock that lives on the `Jobserver` and is acquired only by `Jobserver`
methods therefore **cannot** cover the callback-driven `unregister()`: the
callbacks hold a reference to the `selector`, not to the `Jobserver`.  A
coarse `Jobserver._lock` serializes `submit() ‖ reclaim_resources()`, but
leaves `submit() ‖ future.result()` racing on `_fd_to_key` — the exact
corruption it set out to prevent, and the most natural multi-threaded
pattern once "submit is thread-safe" is advertised.

The fix is to **put the lock where the resource is**: wrap the selector so
that every `register()` / `unregister()` / `select()` — from `submit()`,
from `reclaim_resources()`, and from the Future cleanup callbacks alike —
is serialized by one lock that travels with the selector object the
callbacks already hold.

## Shared Mutable State

| State | Mutated by | Covered by |
|---|---|---|
| `DefaultSelector._fd_to_key` | `submit()` (`register`), `reclaim_resources()` (`select`→`done`→cleanup callbacks), **direct `future.result()`** (cleanup callbacks) | `_LockedSelector._lock` |
| `_selector` / `_selector_pid` / `_selector_closed` | `_lazy_selector()`, `_selector_close()`, `__exit__`, `__del__` | eager construction + fork rebuild (see below) |
| `multiprocessing` global state (`_children` set, resource tracker, process-name counter) | `submit()` → `Process(...).start()` | `_spawn_lock` |
| `_slots` (`FixedBytesQueue`) | `submit()` (`get`), `_restore_token` callback (`put`) | **nothing needed** — lockless by construction |

Note the third row.  `FixedBytesQueue` is genuinely lockless
(`_queue.py:340-433`): each `get()` is one indivisible `os.read()` and each
`put()` one atomic `<= PIPE_BUF` `os.write()` on a non-blocking pipe.
Concurrent producers (`_restore_token`) and consumers (`submit()`) across
threads are safe at the OS level, with the post-poll race already handled
by the `BlockingIOError`/retry loop.  So slot traffic needs **no** Python
lock — a real simplification over guarding `submit()` wholesale.

## Two locks, two jobs

This design uses two narrowly-scoped `threading.Lock`s, each created
per-process (see Fork Safety):

1. **`_LockedSelector._lock`** — serializes all selector bookkeeping.  This
   is the correctness lock; it is the object passed into Future cleanup
   callbacks, so callback-driven `unregister()` is synchronized against
   `submit()`'s `register()`.
2. **`Jobserver._spawn_lock`** — serializes the process-creation tail of
   `submit()` (`Pipe()`, `Process(...)`, `start()`).  CPython's
   `multiprocessing` does not promise thread-safe concurrent
   `Process.start()` (shared module-level `_children` set scanned by
   `_cleanup()`, the resource tracker, the `_process_counter`).  The
   coarse-RLock design gets this serialization "for free" by holding one
   lock across all of `submit()`; here we must serialize the spawn
   explicitly because we deliberately let token-waiting and reclaiming run
   concurrently.

Token acquisition — including the potentially long blocking wait — happens
**before** `_spawn_lock` and outside it, so N submitters wait for slots and
reclaim completed work concurrently; only the brief (millisecond) spawn is
serialized.

## The `_LockedSelector` wrapper

### Responsibilities and interface

```python
from selectors import DefaultSelector, EVENT_READ

class _LockedSelector:
    """Thread-safe facade over DefaultSelector.

    All _fd_to_key bookkeeping is serialized by self._lock.  The blocking
    kernel wait inside select() runs OUTSIDE the lock so a thread parked
    waiting for a slot cannot starve another thread registering a freshly
    spawned worker.  Instances are passed as the `selector` argument to the
    Future cleanup callbacks, so callback-driven unregister() shares this
    lock with submit()-driven register().
    """

    __slots__ = ("_sel", "_lock", "_closed", "_epoll")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._closed = False
        self._sel = DefaultSelector()
        # Backend used to release the lock around the blocking wait; None
        # when the platform selector is not epoll-based (then fall back to
        # holding the lock across select(), see "Blocking wait").
        self._epoll = getattr(self._sel, "_epoll", None)

    def register(self, fileobj, events, data) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("Jobserver is closed")
            self._sel.register(fileobj, events, data)

    def unregister(self, fileobj) -> None:
        with self._lock:
            if self._closed:
                return            # callbacks already tolerate a closed selector
            self._sel.unregister(fileobj)   # may raise KeyError; callers guard

    def get_map(self):
        with self._lock:
            if self._closed:
                return None       # mirrors a closed DefaultSelector
            return dict(self._sel.get_map())   # point-in-time snapshot

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._closed = True
                self._sel.close()

    def select(self, timeout):
        ...   # see below
```

The interface is exactly the subset of `DefaultSelector` that
`_jobserver.py` uses today (`register`, `unregister`, `select`, `get_map`,
`close`), so call sites are unchanged apart from the type.

### Blocking wait: the lock MUST be released around the kernel poll

This is the subtle heart of the design.  If `select()` held `_lock` for the
entire blocking wait, the wrapper would **deadlock** in a way the coarse
RLock does not:

> slots=2, both consumed by running futures F1 (finite) and F2 (infinite).
> Thread A `submit()` grabs a freed token from F1's completion, then needs
> `_lock` to `register()` its new worker's fds.  Thread B `submit()` is
> parked in `select(timeout=None)` holding `_lock`, waiting for a token.
> A's fds are never registered, so B's `select()` never sees A's worker
> exit; B waits forever for a token that only A's (un-registerable) future
> could free.  → deadlock.

The coarse RLock avoids this only because it serializes *whole* submits:
by the time a thread parks in `select()`, every other submit has already
finished registering.  The finer-grained wrapper reintroduces the
interleaving, so it **must** let `register()`/`unregister()` proceed while
another thread is parked in the kernel wait.  On Linux this is exactly what
`epoll` allows — `epoll_wait` and `epoll_ctl` are kernel-synchronized;
only Python's `_fd_to_key` dict needs protection, and that is touched only
*before* and *after* the syscall:

```python
def select(self, timeout):
    # Non-blocking poll (reclaim's hot path) stays fully under the lock.
    # Same for platforms without an epoll backend: correctness first,
    # accepting coarser blocking there (see Alternatives).
    if timeout == 0 or self._epoll is None:
        with self._lock:
            if self._closed:
                raise RuntimeError("Jobserver is closed")
            return self._sel.select(timeout)

    # Blocking wait: hold the lock only for the _fd_to_key bookkeeping,
    # release it around epoll_wait.
    with self._lock:
        if self._closed:
            raise RuntimeError("Jobserver is closed")
        max_ev = max(len(self._sel._fd_to_key), 1)
        epoll = self._epoll
    try:
        fd_event_list = epoll.poll(timeout, max_ev)   # GIL + lock released
    except InterruptedError:
        return []
    with self._lock:
        if self._closed:
            return []
        ready = []
        for fd, _ev in fd_event_list:
            key = self._sel._fd_to_key.get(fd)
            if key is not None:
                # Only EVENT_READ is ever registered by this library.
                ready.append((key, EVENT_READ))
        return ready
```

This is `selectors.EpollSelector.select()` re-expressed with explicit lock
discipline.  Every consumer only reads `key.data`, `key.fd`, and
`key.fileobj` (`_jobserver.py:853, 1282-1287`), so returning
`(key, EVENT_READ)` is faithful.

Properties this preserves from the existing design:
- **O(k), not O(N):** one persistent selector, no per-call interest-set
  rebuild.  (Routing the blocking wait through `multiprocessing.connection.
  wait()` instead would rebuild an O(N) wait set every call — rejected.)
- **Prompt wakeups:** a parked `epoll_wait` wakes the instant any registered
  fd (a child's sentinel/connection, or `slots.waitable()`) becomes ready,
  even if another thread registered it mid-wait.
- **No busy-spin:** `_maybe_obtain_token`'s existing `stall_fds` backoff
  (`_jobserver.py:1280-1289`) is unchanged and still bounds the
  contested-sentinel case.

### Dependence on a private attribute

`self._sel._epoll` and `self._sel._fd_to_key` are CPython implementation
details of `selectors.EpollSelector`.  Mitigations:
- Feature-detect once in `__init__` (`getattr(self._sel, "_epoll", None)`);
  when absent, fall back to holding `_lock` across the whole `select()`.
  That fallback is correct everywhere and only reintroduces the
  blocking-wait serialization (and its deadlock risk) on **non-epoll**
  platforms.  Per `_compat.py`, this library already targets Linux for its
  hot path (epoll, `PR_SET_PDEATHSIG`, `PIPE_BUF`) while degrading on
  others; the fallback keeps non-Linux correct, just coarser.
- A guard test asserts the fast path is actually taken on Linux
  (`isinstance(js._selector._sel, selectors.EpollSelector)` and
  `_epoll is not None`), so a CPython change that renames the attribute
  fails loudly rather than silently degrading.

See [Alternatives Within This Design](#alternatives-within-this-design) for
two ways to avoid the private attribute entirely (time-sliced blocking, or
building directly on `select.epoll`).

## The `_spawn_lock`

```python
def submit(self, fn, *, ..., timeout=None) -> Future[T]:
    # --- validation + coercion (unchanged, lock-free) ---
    ...
    selector = self._lazy_selector()        # raises if closed; fork-rebuilds

    # --- token acquisition: NO spawn lock; selector self-synchronizes ---
    token = _maybe_obtain_token(
        consume=consume,
        deadline=timeout_to_deadline(timeout),
        reclaim_tokens_fn=self.reclaim_resources,
        selector=selector,
        sleep_fn=sleep_fn,
        slots=self._slots,
    )

    # --- spawn tail: serialize multiprocessing process creation ---
    recv = send = process = None
    try:
        with self._spawn_lock:
            # Re-check closed under the lock so __exit__ cannot let a worker
            # start after shutdown (see "__exit__ racing submit").
            if selector._closed:
                raise RuntimeError("Jobserver is closed")
            recv, send = self._context.Pipe(duplex=False)
            process = self._context.Process(target=_worker_entrypoint,
                args=(send, env, preexec_fn, fn, args, kwargs),
                daemon=False, name="Jobserver-worker")
            future: Future[T] = Future(process, recv)
            process.start()
            send.close()
            selector.register(recv, EVENT_READ, data=future)
            selector.register(process.sentinel, EVENT_READ, data=future)
    except Exception:
        # Existing cleanup, unchanged: unregister (tolerating KeyError/
        # ValueError), close pipe fds, and put the token back.
        ...
        raise

    # --- callback wiring: outside spawn lock (Future is self-synchronizing) ---
    future._when_done(fn=_restore_token, args=(self._slots, token),
                      priority=_PRIORITY_TOKEN)
    future._when_done(fn=_unregister_sentinel,
                      args=(selector, process.sentinel, process),
                      priority=_PRIORITY_CLEANUP)
    future._when_done(fn=_unregister_connection, args=(selector, recv),
                      priority=_PRIORITY_CLEANUP)
    return future
```

Why the callback wiring is safe outside `_spawn_lock`: by the time these
run, the fds are registered.  If a concurrent `reclaim_resources()` already
completed the future (its child exited between `start()` and here),
`_when_done` observes `_connection is None` and issues the callback
immediately (`_jobserver.py:385-386`); the cleanup `unregister()` then goes
through `_LockedSelector._lock` like any other.  `Future` is independently
thread-safe via its own `_rlock`, so concurrent `_when_done` (this thread)
and `done()` (the reclaiming thread) on the same future are already
supported.

`reclaim_resources()` needs **no** Jobserver-level lock at all — every line
it touches is either `selector.select()`/cleanup (covered by
`_LockedSelector._lock`) or `Future.done()` (covered by `Future._rlock`):

```python
def reclaim_resources(self) -> None:
    ready = self._lazy_selector().select(timeout=0)
    for data in {key.data for key, _ in ready}:
        assert hasattr(data, "done"), type(data)
        data.done()
```

Body unchanged.  `select(0)` and each cleanup `unregister()` serialize on
the wrapper lock; `done(timeout=0)` keeps the existing non-blocking
`Future._rlock` semantics that make a contested future skip-and-retry
(exercised by `test_reclaim_resources_with_contested_lock`).

## Changes to lifecycle methods

### `__init__` / `__setstate__`: eager construction

Build the wrapper and spawn lock eagerly rather than lazily, to remove a
concurrent-first-use race: two parent threads both seeing `_selector is
None` would each build a wrapper, register `slots.waitable()` twice, and
race the assignment — leaving one thread's futures registered on a
discarded selector, invisible to `reclaim_resources()`.

```python
# __init__ (after _slots is built) and __setstate__:
self._spawn_lock = threading.Lock()
self._selector = self._build_selector()    # _LockedSelector + slots.waitable()
self._selector_pid = os.getpid()
self._selector_closed = False
```

`_build_selector()` factors the wrapper construction plus the one
`register(slots.waitable(), EVENT_READ, SlotsSentinel())` call.  Tradeoff:
every `Jobserver` (including one unpickled into a child that never submits)
now opens an epoll fd up front.  This is one fd; acceptable, and it makes
the concurrency story far simpler than a guarded lazy build.

### `_lazy_selector()`: fork rebuild only

With eager construction, `_lazy_selector()` no longer builds on first use;
it only (a) refuses a closed instance and (b) rebuilds after a fork, where
the child is single-threaded so the unsynchronized swap is safe.  It
rebuilds **both** per-process locks together, keyed on the existing
`_selector_pid`, so no extra `_lock_pid` bookkeeping is needed:

```python
def _lazy_selector(self) -> _LockedSelector:
    if self._selector_closed:
        raise RuntimeError("Jobserver is closed")
    if self._selector is None or self._selector_pid != os.getpid():
        self._selector = self._build_selector()
        self._spawn_lock = threading.Lock()   # fresh, un-stuck after fork
        self._selector_pid = os.getpid()
    return self._selector
```

### `__enter__`

```python
def __enter__(self) -> "Jobserver":
    self._lazy_selector()    # confirms not closed; fork-rebuilds if needed
    return self
```

No lock needed — eager construction already happened; this only validates.

### `__exit__`

```python
def __exit__(self, *exc) -> None:
    if self._selector_closed:
        return
    while True:
        try:
            self.reclaim_resources()      # drain; selector self-synchronizes
            break
        except CallbackRaised as e:
            warnings.warn(...)
    with self._spawn_lock:                 # serialize against submit's spawn
        self._slots.close_put()
        self._slots.close_get()
        self._selector_close()             # sets _selector_closed; wrapper.close()
```

Holding `_spawn_lock` across the close means a concurrent `submit()` either
(a) has not yet entered its `with self._spawn_lock` block — it will then see
`selector._closed` and raise before spawning a worker — or (b) is mid-spawn,
in which case `__exit__` waits for it, then closes; that worker's future is
fully registered and will be drained by the *next* reclaim or orphaned
cleanly on `__del__`.  Either way no worker is started after the selector
closes.  The drain loop runs **before** taking `_spawn_lock` so it cannot
deadlock against an in-progress spawn.

### `__del__`

```python
def __del__(self) -> None:
    if (tracked := self._tracked()) > 0:
        warnings.warn(...)
    if hasattr(self, "_slots"):
        self._slots.close_put()
        self._slots.close_get()
    self._selector_close()    # wrapper.close() is idempotent and self-locked
```

Simpler than the coarse-RLock plan's non-blocking-acquire dance: `__del__`
runs only when the instance is unreferenced, so **no live `Jobserver`
method can be executing concurrently** (any such method holds `self`).  A
Future cleanup callback may still reference the *wrapper* after the
`Jobserver` is gone; `wrapper.close()` is idempotent and a later
callback `unregister()` no-ops on the closed wrapper.  `_tracked()` reads
`get_map()` through the wrapper lock and tolerates a closed selector
(returns 0).

### `_selector_close()`

```python
def _selector_close(self) -> None:
    selector = getattr(self, "_selector", None)
    if selector is not None:
        selector.close()              # _LockedSelector.close(): idempotent
        self._selector_closed = True
```

`_selector_closed` (a plain `bool`) is retained on the `Jobserver` for the
same reason as today: it must survive a fork so a child of a closed
instance raises in `_lazy_selector()` before rebuilding.  The wrapper's own
`_closed` flag is the authoritative *runtime* guard (checked under its
lock); the `Jobserver` bool is the fork-visible/advisory one.  Both are set
on close; reading the bool racily is harmless because the wrapper makes the
actual close-vs-use decision atomic.

### Pickle

`__getstate__` is unchanged — it already excludes `_selector*`, and the new
`_spawn_lock` is likewise process-local and excluded.  The serialized tuple
stays length 5.  `__setstate__` performs the same eager construction as
`__init__` (fresh wrapper, fresh `_spawn_lock`, `_selector_closed = False`).
No change to the pickle wire format.

## Lock Ordering and Deadlock Analysis

Three lock types: `_spawn_lock`, `_LockedSelector._lock` (call it
`sel_lock`), and per-future `Future._rlock`.

Observed acquisition orders:

| Site | Order |
|---|---|
| `submit()` spawn tail | `_spawn_lock` → `sel_lock` (register) |
| `reclaim_resources()` | `sel_lock` (select) released **before** `Future._rlock` (done); cleanup `unregister` takes `sel_lock` again |
| Future cleanup callback (from `reclaim` **or** direct `result()`) | `Future._rlock` → `sel_lock` (unregister) |

The key invariant that makes this acyclic:

- **`sel_lock` is never held while acquiring `Future._rlock`.**
  `reclaim_resources()` does `selector.select(0)` (acquire+release
  `sel_lock`) and only *then* calls `data.done()` (acquire `Future._rlock`).
  So there is no `sel_lock → Future._rlock` edge to pair with the
  `Future._rlock → sel_lock` edge from callbacks.  No cycle between those
  two locks.
- **`sel_lock` holders never acquire `_spawn_lock`.**  `_spawn_lock →
  sel_lock` (register) has no reverse edge.  No cycle.
- **`_spawn_lock` holders never acquire `Future._rlock`.**  The
  `_when_done` wiring that touches `Future._rlock` runs *after*
  `_spawn_lock` is released.  No cycle.
- **The blocking `epoll_wait` holds no Python lock at all**, so it cannot
  participate in any cycle; and because `register()`/`unregister()` only
  need `sel_lock` (free during the wait), a parked submitter never starves
  a registering submitter — resolving the deadlock described under
  "Blocking wait."

Contested-future safety is preserved: a thread in `future.result()` holding
`Future._rlock` then taking `sel_lock` for `unregister()` always gets
`sel_lock` quickly, because every `sel_lock` holder releases it after a
bounded, non-blocking critical section (register/unregister/non-blocking
select, or the two short bookkeeping halves of a blocking select).

Two submitters parked in `epoll.poll()` on the same epoll fd concurrently
is safe (kernel-synchronized, level-triggered): both may observe a readable
`slots.waitable()`, but the lockless `FixedBytesQueue.get(timeout=0)` hands
the token to exactly one and the loser retries — the existing post-poll
race path.

## Fork Safety

- **spawn / forkserver:** the `Jobserver` is pickled; `__setstate__`
  eagerly builds a fresh wrapper and `_spawn_lock`.  No inherited lock
  state.
- **fork:** the child inherits the parent's wrapper (with a possibly-stuck
  `sel_lock`) and `_spawn_lock`.  `_lazy_selector()` detects the pid change
  on first use and rebuilds both before any acquisition.  Only the forking
  thread survives, so the unsynchronized rebuild is safe — identical in
  spirit to today's selector rebuild, now extended to cover the locks
  because they live in the rebuilt objects.  This is best-effort recovery,
  not a license to mix `os.fork()` with threads arbitrarily (the standard
  CPython caveat).

## Edge Cases

- **`submit()` racing `__exit__`:** resolved by the `_spawn_lock` +
  `selector._closed` re-check (see `__exit__`).  No worker starts after
  close; a submit that loses the race raises `RuntimeError` and restores its
  token (which `_restore_token`/`put()` tolerate against a closing queue).
- **Concurrent `reclaim_resources()` and direct `future.result()` on the
  same future:** `done()` is idempotent under `Future._rlock`; the cleanup
  `unregister()` is called at most once (callbacks pop from the heap) and
  tolerates `KeyError`.  Selector mutation from both is serialized by
  `sel_lock`.
- **Future completed by a reclaim *between* a submit's two `register()`
  calls:** the connection becomes ready, reclaim completes the future, and
  the as-yet-unattached `_restore_token`/cleanup callbacks fire immediately
  when `submit()` attaches them.  `_tracked()`'s "transiently
  half-unregistered" tolerance (`_jobserver.py:675`) already anticipates
  this shape.
- **Partial construction:** `__del__` guards with `getattr`/`hasattr`;
  `_tracked()` and `wrapper.get_map()` tolerate a missing or closed
  selector.
- **Callbacks that raise:** unchanged — `CallbackRaised` propagates;
  `sel_lock`/`_spawn_lock` are released by their `with` blocks first.

## Comparison to the Coarse-RLock Design

**Pros of the wrapper:**
- Closes the `submit() ‖ future.result()` correctness hole — the headline
  reason to prefer it.  Selector safety holds regardless of which thread or
  which lock-context drives a Future's completion.
- Higher concurrency: token-waiting and reclaiming run in parallel; only the
  millisecond `Process.start()` is serialized, versus the RLock serializing
  whole submits including their blocking `select()`.
- Simpler `__del__` (no finalizer lock dance) and no re-entrancy
  requirement (a plain `Lock` suffices; `submit → reclaim → done` re-entry
  is fine because `sel_lock` is released between `select()` and `done()`,
  and `_spawn_lock` is not held during reclaim).

**Cons of the wrapper:**
- More moving parts: two locks, a wrapper class, eager construction.
- The blocking-wait fast path depends on a private `EpollSelector`
  attribute (mitigated by feature-detect + fallback + a guard test).
- Must reason explicitly about `multiprocessing` spawn safety
  (`_spawn_lock`), which the RLock got incidentally.

## Alternatives Within This Design

### A1. Time-sliced blocking instead of releasing the lock

Keep `select()` fully under `sel_lock` but cap each blocking wait at a small
slice (e.g. `min(timeout, 0.05)`), returning empty on slice expiry and
letting `_maybe_obtain_token` loop.  Avoids the private attribute entirely
and is trivially portable.  Cost: up to one slice of added registration
latency and wakeup latency, and a low-rate wake/relock cycle.  Good enough
for a process-spawning workload (spawns are milliseconds); arguably the
better default for portability.  Recommend this as the **fallback** the
non-epoll branch uses, and consider it as the primary if the private-attr
dependence is judged unacceptable.

### A2. Build directly on `select.epoll`

Instead of wrapping `DefaultSelector` and reaching into `_epoll`/
`_fd_to_key`, have `_LockedSelector` own a `select.epoll()` and its own
`{fd: (fileobj, data)}` dict.  Then the "internals" are ours, not stdlib's,
and the lock discipline around `epoll.poll()` is unimpeachable.  Cost: we
reimplement the small amount of `register`/`unregister` bookkeeping and lose
`DefaultSelector` portability (epoll = Linux), so a separate portable path
(A1) is still needed for non-Linux.  Cleanest on Linux; most code.

### A3. Hold `sel_lock` across the whole blocking `select()`

Simplest to write, but reintroduces the registration-starvation **deadlock**
documented under "Blocking wait."  Rejected except as the degenerate
non-epoll fallback, where it is acceptable only because that path is not the
supported hot platform. (A1 is strictly safer than A3 and similarly simple;
prefer A1 for the fallback.)

## Docstring Update

Replace:

> Concurrent submit() / reclaim_resources() calls on a Jobserver are not
> thread-safe.  In contrast, returned Futures are thread-safe.

With:

> Concurrent submit() and reclaim_resources() calls on a Jobserver are
> thread-safe, as is driving a returned Future's completion
> (result()/wait()/done()) from another thread concurrently with submission.

## Tests To Add

All in `test/test_jobserver_concurrency.py`:

1. **`test_concurrent_submit_from_multiple_threads`** — N threads submit
   through a Jobserver with fewer slots; all futures complete; no assertion
   errors or selector corruption.
2. **`test_concurrent_submit_and_result`** — the gap the coarse RLock
   misses: one thread submits in a loop while another thread calls
   `result()` on returned futures (driving the cleanup `unregister()`);
   stress for many iterations; no crash, all results correct.
3. **`test_concurrent_submit_and_reclaim`** — one thread submits, another
   loops `reclaim_resources()`; no crash.
4. **`test_blocking_select_does_not_starve_register`** — reproduce the
   "Blocking wait" scenario (finite + infinite worker, two submitters) and
   assert both submitters make progress within a wall-clock bound — i.e. a
   regression test for the release-around-`epoll_wait` requirement.
5. **`test_concurrent_exit_blocks_submit`** — after `__exit__`, a concurrent
   `submit()` raises `RuntimeError` and starts no worker (assert no orphan
   via process count / `_tracked()`).
6. **`test_locked_selector_uses_epoll_fast_path`** — on Linux, assert the
   wrapper's `_epoll` is non-None and the backend is `EpollSelector`, so a
   silent fallback is caught.
7. **`test_fork_rebuilds_locks`** — submit from a child after fork; confirm
   no inherited-lock stall.

## Implementation Checklist

- [ ] Add `_LockedSelector` (register/unregister/select/get_map/close,
      `_closed`, epoll fast path + fallback)
- [ ] Add `"_spawn_lock"` to `Jobserver.__slots__`
- [ ] Add `_build_selector()`; construct wrapper + `_spawn_lock` eagerly in
      `__init__` and `__setstate__`
- [ ] Simplify `_lazy_selector()` to closed-check + fork-rebuild (rebuild
      both locks)
- [ ] Wrap `submit()`'s spawn tail in `_spawn_lock` with a `_closed`
      re-check; keep token acquisition and callback wiring outside it
- [ ] Leave `reclaim_resources()` body unchanged (now selector-synchronized)
- [ ] `__exit__`: drain, then close under `_spawn_lock`
- [ ] Simplify `__del__` (no lock); `_selector_close()` calls
      `wrapper.close()`
- [ ] `__getstate__` unchanged; `__setstate__` eager construction
- [ ] Update class docstring
- [ ] Add concurrency tests (incl. the submit‖result and
      register-starvation regressions)
- [ ] Run full test suite under both `fork` and `spawn` start methods
