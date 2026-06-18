# Worker-completion design constraints

Why the worker-completion path is shaped the way it is: each future owns a
pipe; completion drains that pipe when it is readable, then blocks in
`join()` until the worker has fully exited before returning its slot and
firing callbacks. The shape is forced by the *conjunction* of the
constraints below -- no single one justifies it, and the blocking `join()`
is not, on its own, the fastest option.

## Load-bearing constraints

1. **At most N OS processes for `slots == N`** (counting not-yet-reaped
   zombies). A slot token is returned only after a worker is fully reaped,
   so a replacement cannot start until its predecessor is gone. Returning
   the slot at result-receipt instead is faster but allows up to ~2N live
   processes.

2. **A `when_done` callback can immediately submit new work.** Worst case
   (`slots=1`, one future whose callback submits): a slot is free only if
   that future's worker has already exited. With (1) this forces
   completion -- slot return *and* callbacks -- to happen at/after the
   worker exits, with the slot restored before user callbacks run.

3. **Per-future pipes detect process-specific failure.** Each future owns
   its `Connection`; `recv()` raising `EOFError` is that worker's death.
   This keeps the model at one fresh process per job (a shared worker pool
   would reuse pipes and lose this) and supplies the channel that (4)
   drains.

4. **Large results must complete (issue #269).** A worker blocked
   mid-`send()` on a result larger than the pipe buffer never exits until
   the parent reads it, so completion must wake on the connection being
   readable and `recv()`, not on the exit sentinel alone.

5. **`JobserverExecutor` must work.** A worker forked from the long-lived
   dispatcher exits cleanly under a blocking `join()` but lingers
   (`is_alive()` stays true) under a non-blocking, sentinel-driven reap,
   which deadlocks the executor. This is what forces the *blocking* join
   rather than a deferred, sentinel-driven completion.

## Supporting constraints

- **No background threads** -- the exit wait cannot be offloaded to a
  reaper thread; it stays on the foreground reclaim path.
- **No `os._exit()` in workers** -- worker teardown latency cannot be
  collapsed, so the exit wait is a real cost that can only be paid.
- **No busy-spin backoff without contention** -- the obtain-token loop
  reclaims promptly via the selector, not via stall sleeps.
- **`consume=0`** is the intentional exception to (1): the implicit free
  token that lets nested fan-out exceed N rather than deadlock.

## Consequence

Constraints (1) + (2) + (4) force "drain on readable; return the slot and
fire callbacks only after the worker exits," which puts the worker's exit
latency on the slot-recycling path. (3) plus the no-pool / no-thread /
no-`os._exit()` choices mean that latency can be neither avoided nor
shrunk, only paid; (5) forces paying it via a blocking `join()`. A faster
sentinel-driven reap satisfies (1)-(4) for the bare Jobserver but
deadlocks the executor, so the blocking `join()` is justified only by the
whole set together.
