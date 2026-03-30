# Plan: GNU Make `--jobserver-auth` Compatibility

## Goal

When `Jobserver(slots=None)`, detect `MAKEFLAGS` in the environment and
participate in an existing GNU Make jobserver token pool.  This enables
Python processes invoked from Make recipes to respect `-jN` parallelism
limits alongside C compilers, linkers, and other tools.

---

## Background: GNU Make Jobserver Protocol

- `make -jN` creates a pipe (or, since Make 4.4+, a named FIFO) and
  places N−1 single-byte tokens into it.
- Make itself holds one implicit token (the "+1"), so N total jobs can
  run concurrently.
- Child recipes discover the pipe via `MAKEFLAGS`:
  - **fd pair**: `--jobserver-auth=R,W` (anonymous pipe, inherited fds)
  - **FIFO path**: `--jobserver-auth=fifo:/path/to/fifo`
  - **Legacy**: `--jobserver-fds=R,W` (GNU Make < 4.2)
- To start a sub-job: read one byte from the pipe.
- When a sub-job finishes: write that byte back.
- Tokens are opaque bytes (any value 0x00–0xFF).  Their values must be
  preserved — write back exactly what was read.

### The implicit +1

Make considers the recipe that invoked our process as one of the N jobs.
Our process does **not** own a token for itself — it is already counted.
Every subprocess we spawn requires reading a token from the pipe.  This
maps directly to the existing `consume=1` default in `submit()`.

---

## Phase 1: `Token` type alias

**Files**: `_jobserver.py`, `_queue.py`, `__init__.py`, tests

Introduce `Token = int` at module level and replace all `int`-specific
annotations on token-carrying code paths.

### Steps

1. In `_jobserver.py`, add near the top:
   ```python
   Token = int
   ```
   Export it via `__all__`.

2. Update the 3 annotation sites in `_jobserver.py`:
   - `self._slots: MinimalQueue[int]` → `MinimalQueue[Token]`
   - `_obtain_tokens(... slots: MinimalQueue[int] ...)` → `MinimalQueue[Token]`
   - `retval: list[int] = []` → `list[Token] = []`

3. Update `__init__.py` to export `Token`.

4. Update test annotations in `test_queue.py` and `test_str_repr.py`:
   `MinimalQueue[int]` → `MinimalQueue[Token]` (or leave as-is since
   `Token = int` is an alias — this is cosmetic).

5. Run mypy and the full test suite.  This phase is a no-op at runtime.

### Why `Token = int` and not `bytes`

A single byte read from a pipe with `os.read(fd, 1)` is naturally
represented as `int` (via `b[0]`).  Keeping `Token = int` means
`MinimalQueue[Token]` stays valid for both the pickle-based path
(tokens are `range(N)` integers) and the fd-based path (tokens are
byte values 0–255).  No generics change shape.

---

## Phase 2: MAKEFLAGS parsing — new `_makeflags.py`

**New file**: `src/jobserver/_makeflags.py`

A pure-function module that extracts jobserver connection info from
the `MAKEFLAGS` environment variable.  No side effects, no fd
operations — just string parsing.

### Public interface

```python
@dataclass(frozen=True)
class JobserverAuth:
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

### Parsing rules

1. Default `flags` to `os.environ.get("MAKEFLAGS")`.
2. Split on whitespace.  Scan for tokens matching:
   - `--jobserver-auth=R,W` → `JobserverAuth(fds=(R, W))`
   - `--jobserver-auth=fifo:PATH` → `JobserverAuth(fifo=PATH)`
   - `--jobserver-fds=R,W` → `JobserverAuth(fds=(R, W))` (legacy)
3. Return `None` if no match found.
4. R and W must be non-negative integers; PATH must be non-empty.
   Return `None` (not raise) on malformed values — a broken MAKEFLAGS
   should not crash a program that would otherwise fall back to CPU
   count.

### Tests (in `test_makeflags.py`)

- No MAKEFLAGS → `None`
- Empty MAKEFLAGS → `None`
- `--jobserver-auth=3,4` → `JobserverAuth(fds=(3, 4))`
- `--jobserver-auth=fifo:/tmp/gmake12345` → `JobserverAuth(fifo=...)`
- `--jobserver-fds=5,6` → `JobserverAuth(fds=(5, 6))`
- Mixed flags: `"-j --jobserver-auth=3,4 -Otarget"` → picks out auth
- Malformed values (negative fds, missing comma, empty path) → `None`
- Multiple auth tokens → first one wins (matches Make behavior)

---

## Phase 3: Fd-based token queue — new `_fdqueue.py`

**New file**: `src/jobserver/_fdqueue.py`

A slot queue backed by raw OS file descriptors, speaking the GNU Make
single-byte token protocol.

### Class: `FdTokenQueue`

```python
class FdTokenQueue:
    """A token queue backed by raw file descriptors.

    Conforms to the same duck-type interface as MinimalQueue for use
    as a Jobserver slot queue: get(), put(), waitable(), close_get(),
    close_put().

    Tokens are single bytes, represented as int (0–255).
    """

    def __init__(
        self,
        read_fd: int,
        write_fd: int,
        *,
        owned: bool = False,
    ) -> None: ...

    def get(self, timeout: float | None = None) -> Token: ...
    def put(self, *args: Token) -> None: ...
    def waitable(self) -> int: ...
    def close_get(self) -> None: ...
    def close_put(self) -> None: ...
```

### Implementation notes

- **`get(timeout)`**: Use `select.select([read_fd], [], [], timeout)`
  to wait for readability, then `os.read(read_fd, 1)`.  Return
  `buf[0]` (an `int`).  Raise `queue.Empty` on timeout.  Raise
  `EOFError` if `os.read` returns empty bytes (pipe closed).
  Respect deadline semantics like `MinimalQueue.get`.

- **`put(*args)`**: `os.write(write_fd, bytes(args))`.  Writes all
  tokens contiguously under a lock, matching `MinimalQueue.put`
  semantics.

- **`waitable()`**: Return `read_fd`.  This is already an `int`, which
  `multiprocessing.connection.wait()` accepts on Unix.

- **Locking**: `threading.Lock` (not `multiprocessing.Lock`) since
  cross-process synchronization is handled by the kernel pipe buffer.
  Multiple threads in the same process still need mutual exclusion on
  the read and write sides respectively.

- **`owned` parameter**: When `True`, `close_get()`/`close_put()` call
  `os.close()` on the respective fd.  When `False` (default for
  inherited Make fds), closing is the caller's responsibility.
  This prevents accidentally closing fds that Make expects to remain
  open for sibling processes.

- **No pickle serialization**: Unlike `MinimalQueue`, data on the wire
  is raw bytes, not pickle.  This is what makes cross-language
  interop possible.

- **`__repr__`**: Show read/write fd numbers and owned status.

- **`__copy__` / `__deepcopy__`**: Return `self`, matching
  `MinimalQueue` semantics.

### Opening a FIFO path

Provide a class method or factory function:

```python
@classmethod
def open_fifo(cls, path: str) -> "FdTokenQueue":
    """Open a named FIFO for both reading and writing.

    The read end is opened O_RDONLY|O_NONBLOCK to avoid blocking
    when no writer is present, then O_NONBLOCK is cleared so that
    subsequent reads can block normally via select().
    The write end is opened O_WRONLY.
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

### Pickling (`__getstate__` / `__setstate__`)

For `fork` context: fds are inherited — pickle the fd numbers.

For `spawn`/`forkserver` contexts: raw fd numbers are meaningless in
the child.  Two options:
- **FIFO path**: Store the path; `__setstate__` re-opens via
  `open_fifo()`.
- **fd pair**: Raise `TypeError` on pickle — fd-pair mode requires
  `fork`.  Document this limitation.

Store an `_origin` field that is either `("fifo", path)` or
`("fds", read_fd, write_fd)` to distinguish at pickle time.

### Tests (in `test_fdqueue.py`)

- Round-trip: `put(42)` then `get()` → `42`, using `os.pipe()`.
- Multiple tokens: `put(1, 2, 3)` then three `get()` calls.
- Timeout: `get(timeout=0)` on empty pipe → `queue.Empty`.
- EOF: close write end, then `get()` → `EOFError`.
- `waitable()` returns the read fd.
- FIFO round-trip: `mkfifo`, `open_fifo`, put/get cycle.
- Owned vs unowned: verify `close_get`/`close_put` behavior.
- Thread safety: concurrent get/put from multiple threads.

---

## Phase 4: Wire `FdTokenQueue` into `Jobserver.__init__`

**Files**: `_jobserver.py`, `__init__.py`

### `_slots` type annotation

Change:
```python
self._slots: MinimalQueue[Token]
```
to:
```python
self._slots: MinimalQueue[Token] | FdTokenQueue
```

This union is sufficient.  A `Protocol` is premature until a third
backend exists.

Update `_obtain_tokens` parameter annotation to match.

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
        # Explicit slot count — create local queue, mint tokens
        self._slots: MinimalQueue[Token] | FdTokenQueue = MinimalQueue(self._context)
        assert isinstance(slots, int) and slots >= 1
        self._slots.put(*range(slots))
    else:
        # Auto-detect: check MAKEFLAGS first, fall back to CPU count
        auth = parse_makeflags()
        if auth is not None:
            self._slots = _open_jobserver_auth(auth)
        else:
            self._slots = MinimalQueue(self._context)
            self._slots.put(*range(sched_getaffinity0()))

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

### `__getstate__` / `__setstate__`

The existing pickle path serializes `self._slots`.  Both
`MinimalQueue` and `FdTokenQueue` must be picklable (Phase 3 covers
`FdTokenQueue`'s pickle support).  No changes needed here beyond what
Phase 3 provides.

### `__repr__`

Consider including the slot backend type:
```python
f"Jobserver({method!r}, tracked={n}, slots={type(self._slots).__name__})"
```

### No changes to `_executor.py`

`JobserverExecutor` receives a fully-constructed `Jobserver`.  It never
touches `_slots` directly.  No changes needed.

---

## Phase 5: Comprehensive testing

### `test_makeflags.py` — Unit tests for parsing (Phase 2)

Covered above.  Pure string-in, dataclass-out.  No OS resources needed.

### `test_fdqueue.py` — Unit tests for FdTokenQueue (Phase 3)

Covered above.  Uses `os.pipe()` and `tempfile`+`os.mkfifo()`.

### `test_jobserver_makeflags.py` — Integration tests

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
   creates an `FdTokenQueue`-backed instance, not a `MinimalQueue`.

2. **Explicit slots ignore MAKEFLAGS**: `Jobserver(slots=2)` with
   `MAKEFLAGS` set still creates a local `MinimalQueue`.

3. **Submit and result**: Submit work through a Make-pipe-backed
   Jobserver, verify results return correctly.

4. **Token conservation**: After all futures complete, verify
   the pipe contains exactly the same number of tokens as before
   (read them all out and count).

5. **Token value preservation**: Put known byte values in the pipe.
   Submit and complete work.  Read tokens back out and verify the
   exact same byte values are returned (order may differ due to
   concurrent acquisition, but the multiset must match).

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
    `Jobserver(slots=None)` raises `OSError` (bad file descriptor).

11. **Malformed MAKEFLAGS**: Set to garbage.  `Jobserver(slots=None)`
    falls back to CPU count (no crash).

12. **Callbacks fire normally**: Register `when_done` callbacks on
    futures from a Make-pipe-backed Jobserver.  Verify they fire and
    tokens are restored.

13. **Nesting**: Pickle a Make-pipe-backed Jobserver (fork context),
    submit work that itself submits work via the inherited Jobserver.
    Verify no deadlock with sufficient tokens.

14. **spawn context + fd pair**: Verify that pickling a fd-pair-backed
    `FdTokenQueue` for `spawn`/`forkserver` raises `TypeError` with
    a clear message.

15. **spawn context + FIFO**: Verify that a FIFO-backed Jobserver
    pickles and unpickles correctly under `spawn`/`forkserver` by
    re-opening the path.

---

## Phase 6: Documentation and export cleanup

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
Phase 1 ──→ Phase 2 ──→ Phase 4 ──→ Phase 5
               ↗                       ↑
Phase 3 ──────────────────────────────-╯
```

- Phases 1, 2, and 3 are independent of each other.
- Phase 4 depends on all three.
- Phase 5 depends on Phase 4.
- Phase 6 can proceed in parallel with Phase 5.

---

## Risks and mitigations

| Risk | Mitigation |
|------|------------|
| FIFO open-order deadlock | `O_NONBLOCK` on read end, clear after open |
| `spawn`/`forkserver` + fd pair | Fail fast with `TypeError` at pickle time |
| Token leak on exception paths | Existing `try/except` unwind in `submit()` already handles this — `FdTokenQueue.put` writes bytes back |
| `select.select` portability | Only needed on Unix, which is the only platform with Make jobserver support; guard with platform check |
| MAKEFLAGS detection is surprising | Only triggers when `slots=None` (the default); explicit `slots=N` always overrides; document clearly |
| Sibling process token starvation | Inherent to the protocol — not our bug to fix, but worth documenting |
| Byte values not preserved across pickle | `FdTokenQueue` stores tokens as `int` (0–255); pickle round-trips `int` correctly |
