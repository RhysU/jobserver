# Plan: GNU Make `--jobserver-auth` Compatibility

## Goal

When `Jobserver(slots=None)`, detect `MAKEFLAGS` in the environment and
participate in an existing GNU Make jobserver token pool.  This enables
Python processes invoked from Make recipes to respect `-jN` parallelism
limits alongside C compilers, linkers, and other tools.

As part of this work, replace `MinimalQueue` as the slot-queue backend
with a new `FdTokenQueue` that operates on raw file descriptors and
single-byte tokens.  `MinimalQueue` remains for structured IPC
(executor request/response), but slots always flow through
`FdTokenQueue` — whether backed by a local `os.pipe()` or an inherited
GNU Make pipe/FIFO.

---

## Background: GNU Make Jobserver Protocol

- `make -jN` creates a pipe (or, since Make 4.4+, a named FIFO) and
  places N-1 single-byte tokens into it.
- Make itself holds one implicit token (the "+1"), so N total jobs can
  run concurrently.
- Child recipes discover the pipe via `MAKEFLAGS`:
  - **fd pair**: `--jobserver-auth=R,W` (anonymous pipe, inherited fds)
  - **FIFO path**: `--jobserver-auth=fifo:/path/to/fifo`
  - **Legacy**: `--jobserver-fds=R,W` (GNU Make < 4.2)
- To start a sub-job: read one byte from the pipe.
- When a sub-job finishes: write that byte back.
- Tokens are opaque bytes (any value 0x00-0xFF).  Their values must be
  preserved — write back exactly what was read.

### The implicit +1

Make considers the recipe that invoked our process as one of the N jobs.
Our process does **not** own a token for itself — it is already counted.
Every subprocess we spawn requires reading a token from the pipe.  This
maps directly to the existing `consume=1` default in `submit()`.

---

## Phase 1: `FdTokenQueue` + `Token` alias + `Jobserver.__init__` wiring

**New file**: `src/jobserver/_fdqueue.py`
**Modified files**: `_jobserver.py`, `__init__.py`

This phase is the core change.  It introduces `FdTokenQueue`, defines
`Token = int`, and replaces `MinimalQueue` as the slot-queue backend
for **all** Jobserver instances (not just MAKEFLAGS ones).

### `Token` type alias

In `_jobserver.py`, add near the top:
```python
Token = int
```
Export via `__all__` and `__init__.py`.

A single byte read from a pipe with `os.read(fd, 1)` is naturally
represented as `int` (via `b[0]`).  `range(N)` also produces `int`.
Both paths produce the same type with no conversion.

### Class: `FdTokenQueue`

```python
class FdTokenQueue:
    """A token queue backed by raw file descriptors.

    Tokens are single bytes, represented as int (0-255).
    Uses os.read/os.write for I/O — no pickle serialization.
    """

    __slots__ = (
        "_read_fd",
        "_write_fd",
        "_owned",
        "_origin",
        "_read_lock",
        "_write_lock",
    )

    def __init__(
        self,
        read_fd: int,
        write_fd: int,
        *,
        owned: bool = False,
        origin: tuple | None = None,
    ) -> None: ...

    def get(self, timeout: float | None = None) -> int: ...
    def put(self, *args: int) -> None: ...
    def waitable(self) -> int: ...
```

### Implementation notes

- **`get(timeout)`**: Use `select.select([read_fd], [], [], timeout)`
  to wait for readability, then `os.read(read_fd, 1)`.  Return
  `buf[0]` (an `int`).  Raise `queue.Empty` on timeout.  Raise
  `EOFError` if `os.read` returns empty bytes (pipe closed).
  Use deadline semantics: convert timeout to absolute deadline
  before acquiring the read lock so that lock acquisition time is
  deducted from the caller's budget.

- **`put(*args)`**: `os.write(write_fd, bytes(args))`.  Writes all
  tokens contiguously under a lock, matching `MinimalQueue.put`
  semantics.  POSIX guarantees atomicity for writes <= PIPE_BUF
  (at least 512 bytes), so this is safe.

- **`waitable()`**: Return `read_fd`.  This is already an `int`, which
  `multiprocessing.connection.wait()` accepts on Unix.

- **Locking**: `threading.Lock` (not `multiprocessing.Lock`).
  Cross-process synchronization is handled by the kernel pipe buffer —
  single-byte `read()`/`write()` are atomic on POSIX.  Per-process
  thread locks prevent two threads from interleaving their
  `select`+`read` sequences.

- **`owned` parameter**: When `True`, the queue owns the fds and will
  close them in `__del__` (or an explicit close method if needed
  later).  When `False` (default for inherited Make fds), the fds
  are not closed.  This prevents accidentally closing fds that Make
  expects to remain open for sibling processes.

- **`__repr__`**: Show read/write fd numbers and owned status.

- **`__copy__` / `__deepcopy__`**: Return `self`, matching
  `MinimalQueue` semantics.

### Opening a FIFO path

```python
@classmethod
def open_fifo(cls, path: str) -> "FdTokenQueue":
    """Open a named FIFO for both reading and writing.

    The returned queue owns both fds.
    """
```

Opening order matters for FIFOs:
1. `os.open(path, os.O_RDONLY | os.O_NONBLOCK)` — non-blocking avoids
   deadlock when no writer yet exists.
2. `os.open(path, os.O_WRONLY)` — now succeeds because a reader exists.
3. Clear `O_NONBLOCK` on the read fd via `fcntl.fcntl(fd, F_SETFL, ...)`.

This avoids the need for `O_RDWR` (which is not POSIX-guaranteed on
FIFOs, though it works on Linux).

### Pickling (`__reduce__`)

`FdTokenQueue` needs to pickle correctly because `Jobserver.__getstate__`
includes `self._slots`, and child processes receive the Jobserver via
pickle when nesting.

The `_origin` field distinguishes two strategies:

- **FIFO path** (`_origin = ("fifo", path)`): `__reduce__` stores the
  path.  The child calls `open_fifo(path)` to reconstruct.  Works
  across all start methods.

- **Pipe fds** (`_origin = None` or `("pipe",)`): `__reduce__` wraps
  each fd in `multiprocessing.reduction.DupFd`.  `DupFd` handles fd
  passing to `spawn`/`forkserver` children through the same mechanism
  that `multiprocessing.Connection` uses.  This is the documented
  public API for fd reduction in multiprocessing.

### Wiring into `Jobserver.__init__`

Replace the current `MinimalQueue`-based slot initialization:

```python
# Before:
self._slots: MinimalQueue[int] = MinimalQueue(self._context)
self._slots.put(*range(slots))

# After:
if slots is None:
    slots = sched_getaffinity0()
r, w = os.pipe()
os.write(w, bytes(range(slots)))
self._slots: FdTokenQueue = FdTokenQueue(r, w, owned=True)
```

All slot queues are now `FdTokenQueue`.  No union type.  No branch.

Update `_obtain_tokens` parameter annotation:
```python
slots: MinimalQueue[int]    # before
slots: FdTokenQueue          # after
```

And the return type / local:
```python
retval: list[int] = []      # before
retval: list[Token] = []    # after
```

### `__repr__`

Include the slot backend info:
```python
f"Jobserver({method!r}, tracked={n})"
```
No change needed — `FdTokenQueue` is now the only backend.

### No changes to `_executor.py`

`JobserverExecutor` receives a fully-constructed `Jobserver`.  It never
touches `_slots` directly.  `MinimalQueue` continues to serve the
executor's request/response channels.

---

## Phase 2: MAKEFLAGS parsing — new `_makeflags.py`

**New file**: `src/jobserver/_makeflags.py`

A pure-function module that extracts jobserver connection info from
the `MAKEFLAGS` environment variable.  No side effects, no fd
operations — just string parsing.

### Public interface

```python
class JobserverAuth(NamedTuple):
    """Parsed --jobserver-auth from MAKEFLAGS."""
    # Exactly one of these is set:
    fds: tuple[int, int] | None = None    # (read_fd, write_fd)
    fifo: str | None = None               # filesystem path

def parse_makeflags(flags: str | None = None) -> JobserverAuth | None:
    """Parse MAKEFLAGS (default: os.environ) for jobserver-auth.

    Returns None if MAKEFLAGS is absent, empty, or contains no
    jobserver-auth directive.
    """
```

`JobserverAuth` is a `NamedTuple` (not a `dataclass`) because
`__slots__` is unavailable for dataclasses on Python 3.9, and
NamedTuples are naturally immutable, lightweight, and picklable.

### Parsing rules

1. Default `flags` to `os.environ.get("MAKEFLAGS")`.
2. Split on whitespace.  Scan for tokens matching:
   - `--jobserver-auth=R,W` -> `JobserverAuth(fds=(R, W))`
   - `--jobserver-auth=fifo:PATH` -> `JobserverAuth(fifo=PATH)`
   - `--jobserver-fds=R,W` -> `JobserverAuth(fds=(R, W))` (legacy)
3. Return `None` if no match found.
4. R and W must be non-negative integers; PATH must be non-empty.
   Return `None` (not raise) on malformed values — a broken MAKEFLAGS
   should not crash a program that would otherwise fall back to CPU
   count.

### Tests (in `test_makeflags.py`)

- No MAKEFLAGS -> `None`
- Empty MAKEFLAGS -> `None`
- `--jobserver-auth=3,4` -> `JobserverAuth(fds=(3, 4))`
- `--jobserver-auth=fifo:/tmp/gmake12345` -> `JobserverAuth(fifo=...)`
- `--jobserver-fds=5,6` -> `JobserverAuth(fds=(5, 6))`
- Mixed flags: `"-j --jobserver-auth=3,4 -Otarget"` -> picks out auth
- Malformed values (negative fds, missing comma, empty path) -> `None`
- Multiple auth tokens -> first one wins (matches Make behavior)

---

## Phase 3: MAKEFLAGS integration in `Jobserver.__init__`

**Files**: `_jobserver.py`

Connect Phase 1 and Phase 2: when `slots=None`, check MAKEFLAGS
before falling back to CPU count.

### Modified `__init__` logic

```python
def __init__(
    self,
    context: Union[None, str, BaseContext] = None,
    slots: Optional[int] = None,
    *,
    env: ... = (),
    preexec_fn: ... = noop,
    sleep_fn: ... = noop,
) -> None:
    self._context = resolve_context(context)

    if slots is not None:
        # Explicit slot count — create local pipe, mint tokens
        assert isinstance(slots, int) and slots >= 1
        r, w = os.pipe()
        os.write(w, bytes(range(slots)))
        self._slots: FdTokenQueue = FdTokenQueue(r, w, owned=True)
    else:
        # Auto-detect: check MAKEFLAGS first, fall back to CPU count
        auth = parse_makeflags()
        if auth is not None:
            self._slots = _open_jobserver_auth(auth)
        else:
            slots = sched_getaffinity0()
            r, w = os.pipe()
            os.write(w, bytes(range(slots)))
            self._slots = FdTokenQueue(r, w, owned=True)

    # ... rest unchanged ...
```

### Helper: `_open_jobserver_auth`

```python
def _open_jobserver_auth(auth: JobserverAuth) -> FdTokenQueue:
    if auth.fifo is not None:
        return FdTokenQueue.open_fifo(auth.fifo)
    assert auth.fds is not None
    read_fd, write_fd = auth.fds
    # Validate fds are open before committing
    os.fstat(read_fd)   # raises OSError if invalid
    os.fstat(write_fd)  # raises OSError if invalid
    return FdTokenQueue(read_fd, write_fd, owned=False)
```

If `fstat` fails, let the `OSError` propagate — the caller gets a clear
"bad file descriptor" rather than a mysterious failure later.

---

## Phase 4: Comprehensive testing

### `test_fdqueue.py` — Unit tests for FdTokenQueue (Phase 1)

- Round-trip: `put(42)` then `get()` -> `42`, using `os.pipe()`.
- Multiple tokens: `put(1, 2, 3)` then three `get()` calls.
- Timeout: `get(timeout=0)` on empty pipe -> `queue.Empty`.
- EOF: close write end, then `get()` -> `EOFError`.
- `waitable()` returns the read fd.
- FIFO round-trip: `mkfifo`, `open_fifo`, put/get cycle.
- Owned vs unowned: verify close behavior.
- Thread safety: concurrent get/put from multiple threads.
- Pickling via `DupFd`: pickle/unpickle round-trip, verify tokens
  survive.
- FIFO pickling: pickle stores path, unpickle re-opens.

### `test_makeflags.py` — Unit tests for parsing (Phase 2)

Covered in Phase 2.  Pure string-in, NamedTuple-out.  No OS resources.

### `test_jobserver_makeflags.py` — Integration tests (Phase 3)

These test the full `Jobserver` with a Make-style token pipe.

#### Setup pattern

```python
@pytest.fixture
def make_pipe():
    """Simulate a GNU Make jobserver with N tokens."""
    r, w = os.pipe()
    os.write(w, b'\x00\x01\x02\x03')  # 4 tokens
    old = os.environ.get("MAKEFLAGS")
    os.environ["MAKEFLAGS"] = f"--jobserver-auth={r},{w}"
    yield r, w
    # Restore
    if old is None:
        os.environ.pop("MAKEFLAGS", None)
    else:
        os.environ["MAKEFLAGS"] = old
    os.close(r)
    os.close(w)
```

#### Test cases

1. **Auto-detection**: `Jobserver(slots=None)` with `MAKEFLAGS` set
   uses the Make pipe (verify via fd identity).

2. **Explicit slots ignore MAKEFLAGS**: `Jobserver(slots=2)` with
   `MAKEFLAGS` set creates a fresh local pipe (different fds).

3. **Submit and result**: Submit work through a Make-pipe-backed
   Jobserver, verify results return correctly.

4. **Token conservation**: After all futures complete, verify the pipe
   contains exactly the same number of tokens as before (read them
   all out and count).

5. **Token value preservation**: Put known byte values in the pipe.
   Submit and complete work.  Read tokens back out and verify the
   exact same byte values are returned (multiset match — order may
   differ).

6. **Slot exhaustion / blocking**: Put 1 token in the pipe.  Submit
   one job (consumes the token).  A second `submit(timeout=0)` raises
   `Blocked`.  Complete the first job.  Now submit succeeds.

7. **Concurrent submissions**: Put 4 tokens in.  Submit 4 concurrent
   jobs.  All run.  5th blocks.  As jobs complete, tokens return.

8. **FIFO path mode**: Create a temporary FIFO via `os.mkfifo()`.
   Set `MAKEFLAGS=--jobserver-auth=fifo:/tmp/xxx`.  Verify Jobserver
   opens and operates correctly.

9. **Legacy `--jobserver-fds`**: Same as #1 but with the old flag
   format.

10. **Invalid fds**: Set `MAKEFLAGS=--jobserver-auth=999,998`.
    `Jobserver(slots=None)` raises `OSError`.

11. **Malformed MAKEFLAGS**: Set to garbage.  `Jobserver(slots=None)`
    falls back to CPU count (no crash).

12. **Callbacks fire normally**: Register `when_done` callbacks on
    futures from a Make-pipe-backed Jobserver.  Verify they fire and
    tokens are restored.

13. **Nesting (fork)**: Pickle a Make-pipe-backed Jobserver (fork
    context), submit work that itself submits work via the inherited
    Jobserver.  Verify no deadlock with sufficient tokens.

14. **Nesting (spawn) + FIFO**: Verify that a FIFO-backed Jobserver
    pickles and unpickles correctly under `spawn`/`forkserver` by
    re-opening the path.

15. **Nesting (spawn) + pipe**: Verify that a pipe-backed Jobserver
    pickles correctly under `spawn`/`forkserver` via `DupFd`.

### Existing test suite

All existing tests in `test_jobserver_*.py`, `test_executor_*.py`,
`test_queue.py`, and `test_str_repr.py` must continue to pass.  These
exercise the slot queue through the public API and validate that
replacing `MinimalQueue` with `FdTokenQueue` for slots is a safe
substitution.

---

## Phase 5: Documentation and export cleanup

1. Export `Token`, `FdTokenQueue`, `JobserverAuth`, and
   `parse_makeflags` from `__init__.py` and `__all__`.

2. Update `Jobserver.__init__` docstring to document MAKEFLAGS
   auto-detection behavior.

3. Add an example (`examples/`) showing a Makefile that invokes a
   Python script using jobserver:
   ```makefile
   MAKEFLAGS += -j4
   all: ; +python3 example_make_child.py
   ```
   (The `+` prefix tells Make to pass jobserver fds to the child.)

---

## Ordering and dependencies

```
Phase 1 ──→ Phase 3 ──→ Phase 4
               ↑            ↑
Phase 2 ───────╯            │
                             │
                        Phase 5
```

- Phases 1 and 2 are independent of each other.
- Phase 3 depends on both Phase 1 and Phase 2.
- Phase 4 (testing) depends on Phase 3.
- Phase 5 can proceed in parallel with Phase 4.

---

## Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Replacing `MinimalQueue` for slots changes the critical path for all users | Existing test suite is comprehensive; `FdTokenQueue` is simpler (no pickle, no cross-process locks) and will be exercised by all existing tests |
| FIFO open-order deadlock | `O_NONBLOCK` on read end, clear after open |
| Token leak on exception paths | Existing `try/except` unwind in `submit()` already handles this — `FdTokenQueue.put` writes bytes back |
| `select.select` portability | Only needed on Unix, which is the only platform with Make jobserver support; guard with platform check |
| MAKEFLAGS detection is surprising | Only triggers when `slots=None` (the default); explicit `slots=N` always overrides; document clearly |
| Sibling process token starvation | Inherent to the protocol — not our bug to fix, but worth documenting |
| `DupFd` ties us to `multiprocessing.reduction` | This is the same mechanism `Connection` objects use; it is the documented public API for fd passing across start methods |
