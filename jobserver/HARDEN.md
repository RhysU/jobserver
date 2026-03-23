# Hardening Plan for Jobserver Internals

A specification for testing and hardening the internal components of
`jobserver/impl.py`: `MinimalQueue`, `Future`, `Wrapper` hierarchy,
`_obtain_tokens`, `_worker_entrypoint`, slot-token management, and
callback machinery.

Companion to `draft/HARDEN.md`, which covers the public
`concurrent.futures.Executor` API exclusively.  This document covers
everything beneath that layer.

---

## 1. `MinimalQueue`

`MinimalQueue` is a custom unbounded queue built on `Pipe` +
`ForkingPickler` with per-end locks.  It is the foundation for both
slot-token management and inter-process communication.

### 1.1 Basic put/get round-trip

Put a value, get it back.  Verify identity for simple types (int,
str, bytes, None) and structural equality for compound types (dict,
list, nested structures).

### 1.2 FIFO ordering

Put N values, get them back.  Assert they arrive in insertion order.

### 1.3 Multi-item `put()`

`put(a, b, c)` must be equivalent to `put(a); put(b); put(c)` in
terms of values received, but the multi-item form must deliver all
items contiguously under a single write-lock acquisition.  Verify
ordering is preserved.

### 1.4 Zero-item `put()`

`put()` with no arguments must be a no-op: no data sent, no
exception raised.

### 1.5 `get(timeout=0)` on empty queue raises `queue.Empty`

### 1.6 `get(timeout=T)` on empty queue blocks for approximately T seconds

Measure wall-clock time.  Assert elapsed >= T and elapsed < T + 1 s
(allowing for OS scheduling).

### 1.7 `get(timeout=None)` blocks indefinitely until data arrives

Spawn a thread that puts data after a delay.  Verify `get(timeout=None)`
returns the expected value.

### 1.8 `get()` raises `EOFError` when writer is closed

Close `_writer`, then call `get()`.  Must raise `EOFError`.

### 1.9 `put()` raises `BrokenPipeError` when reader is closed

Close `_reader`, then call `put(value)`.  Must raise `BrokenPipeError`.

### 1.10 Serialization happens outside the lock

Instrument `ForkingPickler.dumps` (or time a slow-to-pickle object)
and verify that `_write_lock` is not held during serialization.
Similarly, verify `_read_lock` is not held during deserialization.
This is critical for avoiding lock contention on large objects.

### 1.11 Concurrent put from multiple threads

Spawn T=8 threads each putting M=100 values.  A single consumer
thread gets all T*M values.  Verify all values are received, no
duplicates, no corruption.

### 1.12 Concurrent get from multiple threads

A single producer puts T*M values.  T=8 consumer threads each get
values.  Verify all values are consumed exactly once, no duplicates.

### 1.13 Large object round-trip

Put and get objects of increasing size: 1 KB, 1 MB, 10 MB, 100 MB.
Verify correctness and no hang.  (The `multiprocessing.Connection`
documentation mentions a possible 32 MB limit on some platforms.)

### 1.14 Unpicklable object in `put()` raises

Attempt to put a lambda or threading.Lock.  Must raise a pickling
error, not hang or corrupt the queue for subsequent operations.

### 1.15 Corrupted/truncated data in pipe

If the writer sends partial data and closes (simulated by closing
`_writer` mid-stream), the reader must raise `EOFError` or a
deserialization error, not hang.

### 1.16 `waitable()` returns a valid file descriptor

`waitable()` must return an `int` that is usable with
`multiprocessing.connection.wait()`.  Put a value, then verify
`wait([q.waitable()], timeout=1)` returns promptly.

### 1.17 Copy semantics

`copy.copy(mq)` and `copy.deepcopy(mq)` must return `mq` itself
(identity, not a new queue).  Verify with `assertIs`.

### 1.18 Start method coverage

All `MinimalQueue` tests should be parameterized over every available
start method (`fork`, `forkserver`, `spawn`) since the underlying
`Pipe` and `Lock` implementations differ.

### 1.19 GC during queue operations

Force `gc.collect()` while a put or get is in progress (via a
background thread).  Must not deadlock.  (CPython `#76757`,
`#65208`.)

---

## 2. `Wrapper` / `ResultWrapper` / `ExceptionWrapper`

### 2.1 `ResultWrapper.unwrap()` returns the wrapped value

For various types: int, str, None, list, Exception-as-value.

### 2.2 `ExceptionWrapper.unwrap()` raises the wrapped exception

Verify the raised exception is the same object (identity) as the
one passed to the constructor.

### 2.3 `ExceptionWrapper` rejects non-Exception types

`ExceptionWrapper(42)` must fail the `assert isinstance(raised,
Exception)` check.

### 2.4 `ExceptionWrapper` rejects `BaseException` subclasses

`ExceptionWrapper(KeyboardInterrupt())` must fail the assert.
This documents a design constraint: workers catching only
`Exception` means `BaseException` subclasses bypass the wrapper.

### 2.5 Round-trip through pickle

Both `ResultWrapper` and `ExceptionWrapper` must survive
`pickle.dumps` / `pickle.loads` since they are sent through pipes.

### 2.6 Result is an Exception object (not raised)

`ResultWrapper(ValueError("returned"))` must unwrap to return the
`ValueError`, not raise it.  This is the critical distinction
between the two wrapper types.

---

## 3. `Future` (Jobserver's own Future)

The Jobserver's `Future` has a different contract from
`concurrent.futures.Future`.  It is a thin wrapper around a
`Process` + `Connection` pair.

### 3.1 `done(timeout=0)` returns `False` before result is sent

Create a `Future` with a process that blocks.  Verify `done(timeout=0)`
returns `False`.

### 3.2 `done(timeout=None)` blocks until result arrives

Verify it returns `True` once the worker sends its result.

### 3.3 `done()` is idempotent

After the first `done()` returns `True`, subsequent calls return
`True` immediately.

### 3.4 `result()` returns the value after `done()`

### 3.5 `result(timeout=0)` raises `Blocked` when not ready

### 3.6 `result()` raises the wrapped exception

Submit a callable that raises.  `result()` must re-raise the
original exception.

### 3.7 `result()` raises `SubmissionDied` on worker death

Kill the worker process.  `result()` must raise `SubmissionDied`.

### 3.8 `result()` is idempotent

Both the success and exception paths must return/raise the same
value on repeated calls.

### 3.9 `done()` joins the process and closes the connection

After `done()` returns `True`, verify `_process` is `None` and
`_connection` is `None`.  This confirms OS resource cleanup.

### 3.10 `__copy__` raises `NotImplementedError`

### 3.11 `__reduce__` raises `NotImplementedError` (no pickling)

### 3.12 Future cannot be submitted as an argument (non-fork)

Under `spawn`/`forkserver` contexts, passing a Future as an
argument to `submit()` must raise `NotImplementedError` from
`__reduce__`.

---

## 4. Callback Machinery (`when_done` / `_issue_callbacks`)

### 4.1 Callbacks fire after `done()` returns `True`

Register a callback before calling `done()`.  After `done()`,
verify the callback has fired.

### 4.2 Callbacks fire in registration order

Register A, B, C.  Verify they fire in order A, B, C.

### 4.3 Callback registered after `done()` fires immediately

After `done()`, register a callback.  It must fire synchronously
inside `when_done()`.

### 4.4 Non-internal callback that raises produces `CallbackRaised`

Register a user callback that raises `ValueError`.  Calling
`done()` must raise `CallbackRaised` with `__cause__` being the
`ValueError`.

### 4.5 `CallbackRaised` surfaces at most one error per `done()` call

Register two raising callbacks.  The first `done()` raises for
callback #1.  The second `done()` raises for callback #2.  The
third `done()` returns `True` normally.

### 4.6 Internal callbacks never produce `CallbackRaised`

Internal callbacks (registered with `__internal=True`) that raise
must propagate the exception directly, not wrap it in
`CallbackRaised`.  This is important because internal callbacks
(slot restoration, sentinel cleanup) indicate bugs in the Jobserver
itself.

### 4.7 Callbacks can access `Future.result()`

Inside a callback, calling `result()` on the same future must
succeed (the future is already done).

### 4.8 Callbacks can register additional callbacks

Inside a callback, calling `when_done()` on the same future must
succeed and the new callback must fire immediately.

### 4.9 Callback receives correct arguments

`when_done(fn, *args, **kwargs)` must call `fn(*args, **kwargs)`.
Verify with a spy function.

---

## 5. Slot Token Management (`_obtain_tokens`)

### 5.1 `consume=0` returns immediately with empty list

No token is consumed.  No blocking.

### 5.2 `consume=1` with available token returns `[token]`

Pre-populate the slot queue with tokens.  Verify one is consumed.

### 5.3 `consume=1` with no token and `timeout=0` raises `Blocked`

### 5.4 `consume=1` with no token blocks until a token is restored

Spawn a thread that restores a token after a delay.  Verify
`_obtain_tokens` returns the token.

### 5.5 `consume=1` with no token and expired deadline raises `Blocked`

Set a short deadline.  Verify `Blocked` is raised after the
deadline.

### 5.6 `reclaim_tokens_fn` is called eagerly

Provide a spy `reclaim_tokens_fn`.  Verify it is called on every
iteration before attempting to acquire a token.

### 5.7 `sleep_fn` returning a duration causes sleep

Provide a `sleep_fn` that returns 0.1 twice then `None`.  Verify
at least 0.2 s elapse before the token is acquired.

### 5.8 `sleep_fn` returning negative raises `AssertionError`

### 5.9 `sleep_fn` vetoing indefinitely causes `Blocked` at deadline

Provide a `sleep_fn` that always returns 0.1.  Set a short
deadline.  Verify `Blocked` is raised.

### 5.10 `sentinels_fn` is passed to `wait()`

Provide sentinels for a process that will complete.  Verify
`_obtain_tokens` unblocks when the sentinel fires (even if no
token is immediately available).

### 5.11 `consume` values other than 0 or 1 raise `AssertionError`

Try `consume=2`, `consume=-1`.  Both must fail the assert.

### 5.12 Token values are preserved

Tokens are integers (slot IDs).  Verify that the returned list
contains the exact token value from the queue, not a substitute.

---

## 6. `_worker_entrypoint`

### 6.1 Successful result is wrapped in `ResultWrapper`

Submit `len((1,2,3))`.  Intercept the pipe and verify the sent
object is `ResultWrapper(3)`.

### 6.2 Raised exception is wrapped in `ExceptionWrapper`

Submit a callable that raises `ValueError("boom")`.  Verify the
sent object is `ExceptionWrapper(ValueError("boom"))`.

### 6.3 `env` parameter sets environment variables

Submit with `env={"FOO": "bar"}`.  Inside the worker, verify
`os.environ["FOO"] == "bar"`.

### 6.4 `env` parameter with `None` value unsets variables

Pre-set `os.environ["FOO"] = "bar"`, then submit with
`env={"FOO": None}`.  Inside the worker, verify `"FOO"` is absent
from `os.environ`.

### 6.5 `preexec_fn` is called before `fn`

Provide a `preexec_fn` that sets a global flag.  Provide `fn` that
reads the flag.  Verify `preexec_fn` ran first.

### 6.6 `env` is applied before `preexec_fn`

Submit with `env={"KEY": "from_env"}` and a `preexec_fn` that reads
`os.environ["KEY"]`.  Verify the `preexec_fn` sees `"from_env"`.

### 6.7 `BrokenPipeError` on send is silently ignored

If the receiving end of the pipe is closed before the worker sends
its result, the worker must exit cleanly (no crash, no traceback).

### 6.8 `BaseException` (e.g. `SystemExit`, `KeyboardInterrupt`) is not caught

Only `Exception` subclasses are caught and wrapped.  Verify that
`SystemExit` raised inside `fn` causes the worker process to exit
without sending a `ResultWrapper` or `ExceptionWrapper`.  The
parent side must see `SubmissionDied` via `EOFError`.

### 6.9 `send.close()` is always called

Even when `fn` raises, even when the pipe is broken, the send end
must be closed in the `finally` block.  Verify via file descriptor
counts or by observing `EOFError` on the receive end.

---

## 7. `Jobserver.submit()` Integration

### 7.1 Slot tokens are consumed on submission

Submit with `consume=1`.  Verify the slot queue has one fewer token.

### 7.2 Slot tokens are restored on `done()` via internal callback

After `done()`, verify the slot queue has the token restored.

### 7.3 `_future_sentinels` tracks in-flight futures

After submission, verify the future is in `_future_sentinels`.
After `done()`, verify it is removed.

### 7.4 Token restoration on process-start failure

Mock `Process.start()` to raise.  Verify the consumed token is
returned to the slot queue (the `except` unwind path).

### 7.5 `consume=0` does not consume a token

Submit with `consume=0`.  Verify the slot queue is unchanged.

### 7.6 `callbacks=True` triggers `reclaim_resources` during token acquisition

Submit enough work to fill slots.  Submit another with
`callbacks=True`.  The internal `reclaim_resources` call must
poll completed futures to free tokens, avoiding deadlock.

### 7.7 `callbacks=False` skips `reclaim_resources`

Submit with `callbacks=False` and all slots full.  Verify
`reclaim_resources` is not called (the `noop` path).

### 7.8 `__getstate__` / `__setstate__` omit in-flight futures

Pickle and unpickle a `Jobserver` with in-flight futures.  The
unpickled instance must have an empty `_future_sentinels` dict
but share the same slot queue.

### 7.9 `Jobserver` as a submit argument with in-flight futures

Pass the Jobserver itself as an argument to `submit()`.  The
`__getstate__` exclusion of futures must prevent the unpicklable-
Future from breaking serialization.

### 7.10 Copy semantics

`copy.copy(js)` and `copy.deepcopy(js)` must return `js` itself.

---

## 8. `reclaim_resources()`

### 8.1 Reclaims completed futures

Submit work, let it complete, call `reclaim_resources()`.  Verify
`_future_sentinels` is empty afterward.

### 8.2 Leaves incomplete futures alone

Submit work that blocks.  Call `reclaim_resources()`.  The
incomplete future must remain in `_future_sentinels`.

### 8.3 Fires callbacks on completed futures

Submit work, register a callback, call `reclaim_resources()`.  The
callback must fire.

### 8.4 Safe under concurrent modification

`reclaim_resources` copies `_future_sentinels.keys()` to avoid
`RuntimeError: dictionary changed size during iteration`.  Verify
this by calling `reclaim_resources` while another thread is
submitting work.

---

## 9. `absolute_deadline` and `resolve_context` Utilities

### 9.1 `absolute_deadline(None)` returns a large value

The result must be at least `time.monotonic() + 604800` (one week).

### 9.2 `absolute_deadline(T)` returns `monotonic() + T`

For T in {0, 0.5, 10}, verify the result is approximately
`time.monotonic() + T`.

### 9.3 `resolve_context(None)` returns the default context

### 9.4 `resolve_context("fork")` returns the fork context

### 9.5 `resolve_context(existing_context)` returns it unchanged

---

## 10. Resource Leak Detection

### 10.1 Process is joined after `done()`

After `Future.done()`, the child process must have been `join()`ed.
Verify by checking that the process is no longer alive.

### 10.2 Connection is closed after `done()`

After `Future.done()`, the pipe connection must be closed.  Verify
via `_connection is None`.

### 10.3 File descriptors returned after full lifecycle

Count open FDs before creating a `Jobserver`, submitting work,
calling `done()`, and verifying FDs return to baseline.

### 10.4 Slot tokens are conserved

Create a Jobserver with N slots.  Submit and complete M tasks.
Verify exactly N tokens remain in the slot queue.

### 10.5 Repeated submit/done cycles do not leak

Run 100 submit/done cycles.  Monitor process count, FD count,
and RSS for monotonic growth.

### 10.6 Worker death still restores tokens

Kill a worker.  After `done()` raises `SubmissionDied`, verify
the slot token was restored to the queue.

---

## 11. Multiprocessing Start Method Coverage

All tests in sections 1--10 must be parameterized over every
available start method: `fork`, `forkserver`, `spawn`.

Key differences exercised:

- `fork`: inherits parent memory, including lock state.  Can mask
  pickling bugs.
- `spawn`: requires full picklability of `fn`, `args`, `kwargs`,
  and the `Wrapper` result.  Catches serialization issues.
- `forkserver`: different process-tree topology; the forkserver
  process itself can accumulate state across requests.

Use `subTest(method=method)` or `@parameterized`.

---

## 12. Timing and Deadlock Detection

### 12.1 Global test timeout

Every test must complete within 60 s.  A test exceeding this is a
deadlock failure.

### 12.2 `_obtain_tokens` does not busy-spin

With no tokens available and a 1 s deadline, measure CPU time
(via `time.process_time()`).  CPU consumption must be << 1 s,
confirming the `wait()` call blocks efficiently.

### 12.3 `MinimalQueue.get(timeout=T)` does not busy-spin

Same approach: measure CPU time during a blocking `get()`.

### 12.4 `done(timeout=T)` does not busy-spin

A `Future` whose worker is slow must not consume CPU while
polling with `connection.poll(timeout)`.

---

## 13. Edge Cases

### 13.1 Submit with empty `args` and empty `kwargs`

Both `args=()` and `kwargs={}` must work and produce the correct
result from a zero-argument callable.

### 13.2 Submit with iterable `env` (not just dict)

The `env` parameter accepts `Iterable[Tuple[str, str]]`.  Verify
it works with a list of tuples, a generator, and an empty iterable.

### 13.3 `noop` function

`noop(*args, **kwargs)` must return `None` for any arguments.
Verify it is used as the default for `preexec_fn` and `sleep_fn`.

### 13.4 `Blocked` is raised with correct timing

Submit with `timeout=0.5` when no slots are available.  Verify
`Blocked` is raised and elapsed time is approximately 0.5 s
(within 0.5--1.5 s).

### 13.5 Nested submission respects slot limits

With N slots, recursively submit work that itself submits.
Maximum recursion depth must be N (each level consumes one slot).
Already tested, but verify with `consume=0` at leaves to confirm
zero-consumption does not count against the limit.

### 13.6 `CallbackRaised.__cause__` is set

When a user callback raises, the resulting `CallbackRaised`
exception must have a non-None `__cause__` pointing to the
original exception (PEP 3134 chaining).
