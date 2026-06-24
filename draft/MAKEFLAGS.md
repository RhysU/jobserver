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

## Phase 1: a pipe-source model for the queues

**Modified file**: `src/jobserver/_queue.py`.

How a queue obtains its pipe ends is decoupled from what the queue does
with them.  Every queue — `FixedBytesQueue`, `SPSCQueue`, `MPMCQueue` —
takes a single positional-only first argument that is *either* a
multiprocessing context (mint a fresh private pipe, as before) *or* a
**source**: a zero-argument callable returning a `(reader, writer)` pair of
`Connection`s.  Two sources cover the Make cases — `from_fds` for an
inherited fd pair, `from_fifo` for a named FIFO:

```python
EndsFactory = Callable[[], tuple[Connection, Connection]]
Source = Union[None, str, BaseContext, EndsFactory]

def from_fds(read_fd: int, write_fd: int) -> EndsFactory: ...   # inherited pipe
def from_fifo(path: str) -> EndsFactory: ...                    # named FIFO

FixedBytesQueue(source=None, /, *, fixedlen)
SPSCQueue(source=None, /)
MPMCQueue(source=None, /, *, context=None)
```

```python
FixedBytesQueue(fixedlen=1)                   # default-context minted pipe
FixedBytesQueue("spawn", fixedlen=1)          # minted with a named context
FixedBytesQueue(ctx, fixedlen=1)              # minted with a BaseContext
FixedBytesQueue(from_fds(r, w), fixedlen=1)   # inherited Make pipe
FixedBytesQueue(from_fifo(path), fixedlen=1)  # Make 4.4+ named FIFO
```

A source yields two `Connection`s and nothing else, so each queue keeps its
existing `get/put/waitable/close_*`/pickling surface and `Resources`
consumes it with no union type or `Protocol`.  `from_fds` `dup()`s the
caller's fds so the queue owns a private copy: closing it never disturbs
Make's fds or sibling processes, so no ownership flag is required.
`from_fifo` opens and owns both ends, opening the read end
`O_RDONLY | O_NONBLOCK` first to dodge the open() deadlock and then clearing
`O_NONBLOCK`; `FixedBytesQueue.__setstate__` re-applies non-blocking for its
own reader discipline.

`to_ends_factory(source)` performs the coercion — a `callable()` is the
source itself, anything else is resolved as a context and wrapped in a
minting source — and `AbstractQueue.__init__(state, /)` installs whatever
state the source yields via `__setstate__`, the canonical installer, with
each subclass composing its own state tail (a `None` locks placeholder,
`fixedlen`, or minted IPC locks).

Pickling is unaffected: a source runs only at construction, after which the
queue holds `Connection`s and pickles via `DupFd` as before, with no path
re-opening and no origin tag, so a getstate/setstate round-trip followed by
put/get works for every queue type.  `MPMCQueue` retains a `context`
keyword solely for its IPC locks, because a `SemLock` is bound to its start
method and cannot cross contexts; that context comes from `context=` or
from a context passed as the source.  `check_fixedlen` is a module-level
function.

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
- `_slots` stays typed as `FixedBytesQueue`; no union or `Protocol` is
  needed because the Make pool is just a `FixedBytesQueue` over a `from_fds`
  / `from_fifo` source.

### Helper: `_open_jobserver_auth`

```python
def _open_jobserver_auth(auth: JobserverAuth) -> FixedBytesQueue:
    if auth.fifo is not None:
        return FixedBytesQueue(from_fifo(auth.fifo), fixedlen=1)
    assert auth.fds is not None
    read_fd, write_fd = auth.fds
    os.fstat(read_fd)   # raises OSError if invalid
    os.fstat(write_fd)  # raises OSError if invalid
    return FixedBytesQueue(from_fds(read_fd, write_fd), fixedlen=1)
```

If `fstat` fails, let the `OSError` propagate — the caller gets a clear
"bad file descriptor" rather than a mysterious failure later.  (`from_fds`
dups, so Make's fds are never closed when this queue is torn down.)

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
| Adding a slot backend changes the critical path | No new backend: the Make pool is the same `FixedBytesQueue` over a `from_fds`/`from_fifo` source, so the existing suite plus new source tests cover it |
| `waitable()` shape | Unchanged: a source yields `Connection`s, so `waitable()` still returns the reader `Connection` exactly as before |
| FIFO open-order deadlock | `O_NONBLOCK` on the read end; the `get()` loop already tolerates `BlockingIOError` |
| Token leak on exception paths | Existing `try/except` unwind in `submit()` plus `_restore_token` writes the byte back; the byte-token queue never loses values |
| Inherited fds closed out from under sibling processes | `from_fds` `dup()`s the caller's fds, so the queue owns a private copy and tearing it down never touches Make's fds |
| `select.select` / non-blocking portability | Only needed on Unix, the only platform with Make jobserver support; guard with a platform check |
| MAKEFLAGS detection is surprising | Only triggers when `slots=None` (the default); explicit `slots=N` always overrides; document on both `Resources` and `Jobserver` |
| `slots <= pipe_buf()` cap vs. a large Make pool | The cap is a minting constraint; it stays on the local-pipe paths and is skipped when inheriting a Make pool |
| Sibling process token starvation | Inherent to the protocol — not our bug to fix, but worth documenting |
| `DupFd` ties us to `multiprocessing.reduction` | Same mechanism `Connection` pickling already uses; it is the documented public API for fd passing across start methods |
</content>
</invoke>
