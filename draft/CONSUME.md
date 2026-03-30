# Issue #17: Recursive Submission Deadlock

## The Problem

The following program hangs:

```python
from typing import Callable
from jobserver import Blocked, Future, Jobserver


def work(jobserver: Jobserver, func: Callable[..., int], head, *rest) -> int:
    if not rest:
        return func(head)

    while True:
        try:
            future = jobserver.submit(
                work,
                args=(jobserver, func, *rest),
                timeout=0.5
            )
            return func(head) + future.result()
        except Blocked:
            pass


if __name__ == '__main__':
    jobserver = Jobserver(context="forkserver", slots=2)

    future = jobserver.submit(
        fn=work,
        args=(jobserver, len, [1], [1, 2], [1, 2, 3], [1, 2, 3, 4]),
        timeout=0.5
    )

    future.when_done(print, "Done")
    print(future.result())
    assert future.result() == 10
```

## Trace

With `slots=2`, tokens `[0, 1]` are placed in the shared `_slots` queue.

| Step | Actor  | Event                              | Queue  |
|------|--------|------------------------------------|--------|
| 1    | Parent | `submit(work, [...])` acquires token 0 | `[1]`  |
| 2    | P1     | Starts. Calls `submit(work, [...])`, acquires token 1 | `[]`   |
| 3    | P2     | Starts. Calls `submit(work, [...])`, queue empty | `[]`   |
| 4    | P2     | `_obtain_tokens` enters `wait(sentinels + (slots.waitable(),))` | `[]`   |
| 5    | P2     | Timeout fires. `Blocked` raised. `while True` retries. | `[]`   |
| 6    | P2     | Step 3-5 repeats forever.          | `[]`   |

Token 0 is held by the parent's callback on P1's Future (returned when P1
exits).  Token 1 is held by P1's callback on P2's Future (returned when P2
exits).  P2 cannot exit because it loops forever trying to submit P3.  P1
cannot exit because it blocks on P2's `future.result()`.  The parent cannot
exit because it blocks on P1's `future.result()`.

Note that P2's `wait()` call *does* include `slots.waitable()` -- the file
descriptor for the shared IPC queue.  The waiting mechanism is correct and
complete.  If any process anywhere returned a token, P2 would wake up.  The
problem is that no process *will* return a token: every token is held by a
process that is blocked waiting for its child, which needs a token to proceed.

## The Implicit Token in GNU Make

This is a well-known problem.  The GNU Make jobserver protocol -- the direct
inspiration for this library -- solves it with the concept of a **free token**
(also called the **implicit job slot**):

> Every command make starts has one implicit job slot reserved for it before
> it starts.  Any tool which wants to participate in the jobserver protocol
> should assume it can always run one job without having to contact the
> jobserver at all.
>
> -- [GNU Make Manual: POSIX Jobserver](https://www.gnu.org/software/make/manual/html_node/POSIX-Jobserver.html)

The design is:

1. Top-level `make -j N` writes **N-1** tokens into the pipe, not N.
2. The top-level make holds the Nth slot implicitly -- it never touches the
   pipe for its own first job.
3. Every recursive `$(MAKE)` invocation inherits the same implicit slot.
   It can always run one job without acquiring a token from the pipe.
4. Tokens from the pipe are only needed for *additional* concurrent jobs
   beyond the first.

This breaks the deadlock cycle.  Even when every pipe token is held by a
waiting parent, each child process can still execute one unit of work using
its implicit slot.  The recursion bottoms out, results propagate back up,
and pipe tokens are returned as parents finish.

## The Same Problem Everywhere

In the broader concurrency literature, this is known as **thread-pool-induced
deadlock** or **thread starvation deadlock**.  It occurs in any bounded worker
pool where tasks submit subtasks to the same pool and block on the results.

> The simplest reason of deadlock is depending in one task (T1) on the
> results of another task (T2).  If a thread pool is full and the second
> task (T2) gets queued, the first task (T1) might block and wait
> infinitely.
>
> -- [SEI CERT: TPS01-J](https://wiki.sei.cmu.edu/confluence/display/java/TPS01-J.+Do+not+execute+interdependent+tasks+in+a+bounded+thread+pool)

Different systems solve it differently:

| System | Mechanism |
|--------|-----------|
| GNU Make | Implicit free token per process |
| Java `ForkJoinPool` | Work-stealing: `join()` executes pending tasks from the local queue instead of blocking. Compensation threads spun up via `ManagedBlocker` when true blocking is unavoidable. |
| .NET ThreadPool | Starvation detection injects new threads when existing threads are blocked |
| Go | Goroutines are multiplexed onto OS threads by the runtime; blocking one goroutine doesn't consume a thread |
| General guidance | Don't submit interdependent tasks to the same bounded pool ([SEI CERT TPS01-J](https://wiki.sei.cmu.edu/confluence/display/java/TPS01-J.+Do+not+execute+interdependent+tasks+in+a+bounded+thread+pool)) |

The Jobserver library puts **all N** tokens into the `_slots` queue and
requires every submission to acquire one (when `consume=1`, the default).
There is no implicit slot.  This is a faithful implementation of a bounded
token pool, but it lacks the deadlock-avoidance mechanism that GNU Make
applies on top.

## Why Library-Side Fixes Are Difficult

Our conversation explored several approaches to solving this inside the
library.  Each failed for a clear reason:

**Token lending (shared flag).**  When `_obtain_tokens` fails, the child
puts "its" token back into `_slots` and sets a shared
`multiprocessing.Value` so the parent's callback skips the return.  This
works mechanically, but introduces state outside `_slots` to track token
ownership.  The `_slots` queue is the single source of truth for slot
availability in the current design; a parallel bookkeeping channel
undermines that.

**Dynamic `consume=0` fallback.**  When `_obtain_tokens` fails and
`_future_sentinels` is empty, fall back to `consume=0`.  This prevents
the deadlock but removes all backpressure: the child can submit arbitrary
amounts of work without acquiring any tokens.

**Token surrender in `Future.done()`.**  Before `connection.poll()`, put
the held token back into `_slots`.  When `timeout=None`, there is no
need to re-acquire.  The problem is that this makes timeouts indeterminate:
surrendering the token enables a chain of work whose total duration is
unknowable at the time the caller specified their timeout.

Each approach is an elaborate way to dynamically change the `consume`
parameter -- either from 1 to 0 inside the library, or by adding side-channel
state so the library can retroactively undo a `consume=1`.  The `consume`
parameter already exists as the explicit, user-facing mechanism for exactly
this purpose.

## The Fix

The caller knows the shape of their computation.  In a recursive fork-join,
interior nodes submit a child and block on `future.result()`.  They do not
perform concurrent work.  This is exactly the GNU Make "implicit slot"
pattern: the child inherits the right to run without consuming a token from
the pipe.

```python
def work(jobserver: Jobserver, func: Callable[..., int], head, *rest) -> int:
    if not rest:
        return func(head)

    while True:
        try:
            future = jobserver.submit(
                work,
                args=(jobserver, func, *rest),
                consume=0,       # I will block, not compute concurrently
                timeout=0.5,
            )
            return func(head) + future.result()
        except Blocked:
            pass
```

With `consume=0`, no token is acquired for the child.  The child runs using
the implicit slot that its parent already holds.  When the recursion bottoms
out, results propagate back, and tokens are released as ancestors exit.

This matches GNU Make's design: `consume=0` *is* the implicit free token.
The top-level submission uses `consume=1` (the default).  Recursive interior
nodes use `consume=0` because they are not adding concurrency -- they are
subdividing work that already has a slot.

## References

- [GNU Make: POSIX Jobserver](https://www.gnu.org/software/make/manual/html_node/POSIX-Jobserver.html)
- [GNU Make: Job Slots](https://www.gnu.org/software/make/manual/html_node/Job-Slots.html)
- [Paul Smith: Jobserver Implementation](https://make.mad-scientist.net/papers/jobserver-implementation/)
- [SEI CERT: TPS01-J -- Do not execute interdependent tasks in a bounded thread pool](https://wiki.sei.cmu.edu/confluence/display/java/TPS01-J.+Do+not+execute+interdependent+tasks+in+a+bounded+thread+pool)
- [Thread Pool Self-Induced Deadlocks (Nurkiewicz)](https://nurkiewicz.com/2018/09/thread-pool-self-induced-deadlocks.html)
- [Pool-Induced Deadlock (Jessitron)](https://jessitron.com/2014/01/29/fun-with-pool-induced-deadlock/)
- [How to Avoid Thread Pool Induced Deadlocks (Sznajder)](https://asznajder.github.io/thread-pool-induced-deadlocks/)
- [Michał Górny: One Jobserver to Rule Them All](https://blogs.gentoo.org/mgorny/2025/11/30/one-jobserver-to-rule-them-all/)
