# Plan: GNU Make `--jobserver-auth` Compatibility

## Goal

When `Jobserver(slots=None)`, detect `MAKEFLAGS` in the environment and
participate in an existing GNU Make jobserver token pool.  This enables
Python processes invoked from Make recipes to respect `-jN` parallelism
limits alongside C compilers, linkers, and other tools.

To support this, the slot-queue backend must operate on **inherited** raw
file descriptors and a named FIFO, in addition to the locally-minted
`os.pipe()` it already uses.  Slots always flow through a single-byte token
queue — whether backed by a local pipe owned by the process or an inherited
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
  preserved — write back exactly what was read.  The byte-token queue does
  this inherently; it never interprets a token's value.

### The implicit +1

Make considers the recipe that invoked our process as one of the N jobs.
Our process does **not** own a token for itself — it is already counted.
Every subprocess we spawn requires reading a token from the pipe.  This
maps directly to the existing `consume=1` default in `submit()`.

---

## Current slot plumbing (what we are building on)

The relevant pieces are already in place and shaped well for this work:

- **Slots live in `Resources`.**  `Jobserver` is a thin handle; the slot
  queue and selector live in `_jobserver.Resources`.  All slot wiring
  below targets `Resources.__init__`.

- **The slot backend is a byte-token queue.**  `_queue.py` provides a small
  family of pipe-backed queues:
  - `AbstractQueue` — pipe lifecycle (`waitable()`, `close_get()`,
    `close_put()`, repr, pickling) over a `multiprocessing` `Pipe`.
  - `AbstractPicklingQueue` / `SPSCQueue` / `MPMCQueue` — pickle generic
    objects across the pipe; used for the executor's request/response and
    other structured IPC.  Untouched by this work.
  - `FixedBytesQueue` — **the slot backend**.  A lockless, single-byte
    (`fixedlen=1`) token queue: `get()` is one indivisible `os.read`,
    `put()` one atomic `os.write` of `<= PIPE_BUF` bytes.  It relies on
    POSIX atomicity instead of locks, sets the reader non-blocking, and
    retries `get()` on `BlockingIOError`.  It already exposes the
    `get()/put()/waitable()/close_get()/close_put()` surface this plan
    needs.

- **Tokens are `bytes`.**  The pipeline uses single-byte `bytes` tokens
  (`_maybe_obtain_token` returns `Optional[bytes]`;
  `_restore_token(slots, token: Optional[bytes])`).  Byte-token I/O
  **preserves Make's opaque token values for free** — whatever byte is read
  is the byte written back.

`Resources.__init__` currently does, in effect:

```python
if slots is None:
    slots = sched_getaffinity0()
# ... type/range validation, including slots <= pipe_buf() ...
self._context = resolve_context(context)
self._slots = FixedBytesQueue(self._context, fixedlen=1)
self._slots.put(b"J" * slots)   # opaque local tokens; value is irrelevant
```

The slot byte values are all `b"J"` today because nothing reads them — but
the read/write path is already value-preserving, which is exactly what the
Make protocol requires.

`Resources.__getstate__` pickles `(self._context, self._slots)`, so whatever
backs `_slots` must pickle for nesting.  `Resources.lazy_selector()`
registers `self._slots.waitable()` once with a `SlotsSentinel`.
`FixedBytesQueue.waitable()` returns the reader `Connection`, which
`lazy_selector()` registers in a `DefaultSelector`.

---

## Phase 1: an inherited-fd / FIFO byte-token queue

**Modified file**: `src/jobserver/_queue.py`

We need a queue with `FixedBytesQueue` semantics that can wrap fds it does
**not** own (an inherited Make pipe) or open a named FIFO, rather than
minting a fresh `multiprocessing` `Pipe`.  Two viable shapes; pick one:

1. **A sibling class** (e.g. `FdTokenQueue`) that mirrors `FixedBytesQueue`'s
   `get()/put()` byte logic but stores raw int fds instead of
   `Connection`s.  It does **not** derive `AbstractQueue` (whose `__init__`
   creates a Pipe); it re-implements the small surface `Resources` uses.

2. **Extending `FixedBytesQueue`** with an alternate constructor that adopts
   pre-existing fds and an `owned` flag.  This keeps one class but mixes two
   fd-ownership models in `AbstractQueue`.

Either way the object must duck-type with `FixedBytesQueue` for `Resources`:
`get(timeout)`, `put(obj: bytes)`, `waitable()`, `close_get()`,
`close_put()`, and pickling.  `Resources` never type-checks `_slots`, so a
compatible interface is sufficient — no union annotation is forced.

### Interface (sibling-class shape shown)

```python
class FdTokenQueue:
    """A single-byte token queue over raw, possibly inherited, fds."""

    __slots__ = ("_read_fd", "_write_fd", "_owned", "_origin",
                 "_read_lock", "_write_lock")

    def __init__(self, read_fd: int, write_fd: int, *,
                 owned: bool = False, origin: tuple | None = None) -> None: ...

    @classmethod
    def open_fifo(cls, path: str) -> "FdTokenQueue": ...

    def get(self, timeout: float | None = None) -> bytes: ...
    def put(self, obj: bytes) -> None: ...
    def waitable(self) -> int: ...
    def close_get(self) -> None: ...
    def close_put(self) -> None: ...
```

### Implementation notes

- **Token type is `bytes`.**  `get()` returns a 1-byte `bytes`, `put()`
  accepts `bytes` (one or more whole tokens), matching `FixedBytesQueue`
  and the existing `_restore_token` / `_maybe_obtain_token` contract.  No
  `int` conversion anywhere.

- **`get(timeout)`**: deadline-first (convert timeout to an absolute
  deadline before acquiring the read lock, mirroring `FixedBytesQueue`).
  Set the read fd non-blocking and retry on `BlockingIOError` after a
  readability wait, exactly as `FixedBytesQueue.get()` does.  Raise
  `queue.Empty` on timeout, `EOFError` on a closed/drained pipe.  Wait for
  readability with `select.select([read_fd], [], [], timeout)` (or reuse
  the `poll()` discipline already used by the queue family).

- **`put(obj)`**: one atomic `os.write(write_fd, obj)` under a write lock.
  POSIX guarantees atomicity for writes `<= PIPE_BUF`.

- **`waitable()`**: return `read_fd` (an `int`).  `DefaultSelector.register`
  and `multiprocessing.connection.wait` both accept bare ints on Unix.
  Note the contrast with `FixedBytesQueue.waitable()`, which returns a
  `Connection`; `Resources` handles both because it only registers the
  return value and compares it by identity in `_maybe_obtain_token`.

- **Locking**: `threading.Lock` only.  Cross-process exclusion is the
  kernel pipe buffer's job (single-byte read/write are atomic on POSIX);
  per-process locks just keep two local threads from interleaving a
  readability-wait + read.

- **`owned` parameter**: when `True` the queue closes its fds in
  `close_get()`/`close_put()`/`__del__`; when `False` (the default for
  inherited Make fds) it never closes them, so sibling processes keep the
  pipe open.  `FixedBytesQueue` always owns its pipe — this flag is the key
  new behavior.

- **`__repr__`**: show read/write fd numbers and `owned`.

- **`__copy__` / `__deepcopy__`**: return `self`, matching the rest of the
  queue family.

### Opening a FIFO path

```python
@classmethod
def open_fifo(cls, path: str) -> "FdTokenQueue":
    """Open a named FIFO for both reading and writing; owns both fds."""
```

Open order matters for FIFOs:
1. `os.open(path, os.O_RDONLY | os.O_NONBLOCK)` — non-blocking avoids
   deadlock when no writer yet exists.
2. `os.open(path, os.O_WRONLY)` — succeeds because a reader now exists.
3. Leave the read fd non-blocking (the `get()` loop already tolerates
   `BlockingIOError`), or clear `O_NONBLOCK` via `fcntl` if a blocking
   reader is preferred — pick whichever matches `get()`'s implementation.

This avoids `O_RDWR` (not POSIX-guaranteed on FIFOs, though it works on
Linux).

### Pickling (`__reduce__` / `__getstate__`)

`Resources.__getstate__` pickles `self._slots`, so the slot queue must
survive pickling when a Jobserver nests into a child.  `FixedBytesQueue`
gets this from `AbstractQueue.__getstate__`, which ships its `Connection`s
(reduced by multiprocessing's forking machinery).  An inherited-fd queue
needs its own strategy, keyed by `_origin`:

- **FIFO** (`_origin = ("fifo", path)`): pickle the path; the child calls
  `open_fifo(path)`.  Works under every start method.

- **Pipe fds** (`_origin = None` / `("pipe",)`): wrap each fd in
  `multiprocessing.reduction.DupFd`, the same documented mechanism that
  `Connection` pickling uses to pass fds to `spawn`/`forkserver` children.

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

`JobserverAuth` is a `NamedTuple` (not a `dataclass`) because it is
naturally immutable, lightweight, picklable, and avoids the Python 3.9
dataclass-`__slots__` gap.  (The project still tests on CPython 3.9.)

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

## Phase 3: MAKEFLAGS integration in `Resources.__init__`

**Files**: `_jobserver.py`

Connect Phase 1 and Phase 2: when `slots=None`, check MAKEFLAGS
before falling back to CPU count.  This is wired in `Resources.__init__`
(the slot owner), not `Jobserver.__init__`.

### Modified `Resources.__init__` logic

```python
def __init__(self, context=None, slots: Optional[int] = None) -> None:
    if slots is not None:
        # Explicit slot count: validate, then mint a local pipe of tokens.
        if isinstance(slots, bool) or not isinstance(slots, int):
            raise TypeError(...)
        if slots < 1:
            raise ValueError(...)
        if slots > pipe_buf():            # one atomic put() caps slots
            raise ValueError(...)
        self._context = resolve_context(context)
        self._slots = FixedBytesQueue(self._context, fixedlen=1)
        self._slots.put(b"J" * slots)
    else:
        self._context = resolve_context(context)
        auth = parse_makeflags()
        if auth is not None:
            # Inherit Make's pool: no minting, so the pipe_buf() cap on
            # slots does not apply (Make may seed far more tokens).
            self._slots = _open_jobserver_auth(auth)
        else:
            slots = sched_getaffinity0()
            self._slots = FixedBytesQueue(self._context, fixedlen=1)
            self._slots.put(b"J" * slots)

    # ... selector/refcount setup unchanged ...
```

Notes:
- The current code validates `slots` and enforces `slots <= pipe_buf()`
  because every token is written in one atomic `put()`.  That cap is a
  property of *minting*, so it stays on the explicit/auto-CPU paths and is
  skipped for an inherited Make pool.
- `_slots` is annotated to accept either backend (e.g.
  `Union[FixedBytesQueue, FdTokenQueue]`, or a small typing `Protocol`
  capturing `get/put/waitable/close_*`).

### Helper: `_open_jobserver_auth`

```python
def _open_jobserver_auth(auth: JobserverAuth) -> FdTokenQueue:
    if auth.fifo is not None:
        return FdTokenQueue.open_fifo(auth.fifo)
    assert auth.fds is not None
    read_fd, write_fd = auth.fds
    os.fstat(read_fd)   # raises OSError if invalid
    os.fstat(write_fd)  # raises OSError if invalid
    return FdTokenQueue(read_fd, write_fd, owned=False)
```

If `fstat` fails, let the `OSError` propagate — the caller gets a clear
"bad file descriptor" rather than a mysterious failure later.

---

## Phase 4: Comprehensive testing

### `test_queue.py` additions — the inherited-fd queue (Phase 1)

Co-locate with the existing `FixedBytesQueue` tests:

- Round-trip: `put(b"*")` then `get()` -> `b"*"`, over an `os.pipe()`.
- Multiple tokens: `put(b"\x01\x02\x03")` then three `get()` calls.
- Timeout: `get(timeout=0)` on an empty pipe -> `queue.Empty`.
- EOF: close the write end, then `get()` -> `EOFError`.
- `waitable()` returns the read fd (an int).
- FIFO round-trip: `mkfifo`, `open_fifo`, put/get cycle.
- Owned vs unowned: verify `close_*`/`__del__` close only when `owned`.
- Thread safety: concurrent get/put from multiple threads.
- Pickling via `DupFd`: pickle/unpickle round-trip; tokens survive.
- FIFO pickling: pickle stores the path; unpickle re-opens.
- **Value preservation**: push known bytes `b"\x00\x7f\xff"`, read them
  back; exact bytes returned (the Make-token guarantee).

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
`test_queue.py`, `test_str_repr.py`, and `test_resource_warning.py` must
continue to pass.  These exercise the slot queue through the public API and
validate that adding an alternate slot backend leaves the default
`FixedBytesQueue` path unchanged.

---

## Phase 5: Documentation and export cleanup

1. Export `JobserverAuth` and `parse_makeflags` (and the inherited-fd
   queue, if intended to be public) from `_makeflags.py` / `_queue.py`.
   The top-level `__all__` currently exports only `Blocked`,
   `CallbackRaised`, `Future`, `Jobserver`, `JobserverExecutor`, and
   `LostResult`; decide deliberately whether the new symbols join it or
   stay internal.

2. Update the `Resources.__init__` and `Jobserver.__init__` docstrings to
   document MAKEFLAGS auto-detection.  Both currently say slots default to
   `len(os.sched_getaffinity(0))`; the auto path now reads "an inherited
   Make jobserver if `MAKEFLAGS` advertises one, else the usable CPU count."

3. Add an example (`examples/`) showing a Makefile that invokes a
   Python script using jobserver:
   ```makefile
   MAKEFLAGS += -j4
   all: ; +python3 example_make_child.py
   ```
   (The `+` prefix tells Make to pass jobserver fds to the child.)
   Name it to fit the existing `exNN_*.py` sequence.

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
| Adding a second slot backend changes the critical path | The default `FixedBytesQueue` path is unchanged; the inherited-fd queue duck-types the same `get/put/waitable/close_*` surface and is covered by the existing suite plus new tests |
| `waitable()` returning a raw int vs. a `Connection` | `Resources` only registers the value in a `DefaultSelector` and compares it by identity; both ints and `Connection`s are accepted by the selector and by `connection.wait` on Unix |
| FIFO open-order deadlock | `O_NONBLOCK` on the read end; the `get()` loop already tolerates `BlockingIOError` |
| Token leak on exception paths | Existing `try/except` unwind in `submit()` plus `_restore_token` writes the byte back; the byte-token queue never loses values |
| Inherited fds closed out from under sibling processes | `owned=False` for inherited Make fds; `close_*`/`__del__` are no-ops there |
| `select.select` / non-blocking portability | Only needed on Unix, the only platform with Make jobserver support; guard with a platform check |
| MAKEFLAGS detection is surprising | Only triggers when `slots=None` (the default); explicit `slots=N` always overrides; document on both `Resources` and `Jobserver` |
| `slots <= pipe_buf()` cap vs. a large Make pool | The cap is a minting constraint; it stays on the local-pipe paths and is skipped when inheriting a Make pool |
| Sibling process token starvation | Inherent to the protocol — not our bug to fix, but worth documenting |
| `DupFd` ties us to `multiprocessing.reduction` | Same mechanism `Connection` pickling already uses; it is the documented public API for fd passing across start methods |
</content>
</invoke>
