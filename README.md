Jobserver
=========

A nestable Jobserver with thread-safe futures, callbacks, and complete
type-hinting

Purpose
-------

[Jobserver](src/jobserver/_jobserver.py) is similar in spirit to
`multiprocessing.Pool` or `concurrent.futures.Executor` with a few differences:

 * First, the implementation choices are based upon the [GNU Make
   Jobserver](https://www.gnu.org/software/make/manual/html_node/POSIX-Jobserver.html).
 * Second, as a result, the Jobserver is "nestable" meaning that resource
   constraints will be shared with work submitted by other work.
 * Third, no background threads are spun up to handle any backing
   queues consequently permitting the implementation to play well with
   more 3rd party libraries.
 * Fourth, Futures are eagerly scanned to quickly reclaim resources.
 * Fifth, Futures can detect when a child process died unexpectedly.
 * Sixth, the user can specify additional work acceptance criteria.
   For example, not launching work unless some amount of RAM is available.
 * Lastly, the API communicates when Exceptions occur within a callback.

In particular, `Jobserver` does not inherit from `concurrent.futures.Executor`
because that `Executor` API fundamentally requires a background thread for
asynchronously issuing `concurrent.futures.Future` callbacks.  `Jobserver`,
eschewing threads, consequently is both somehow less-than and more-than a
standard `Executor`.

In contrast, [JobserverExecutor](src/jobserver/_executor.py) combines
a `Jobserver` with a background thread to provide full
`concurrent.futures.Executor` compatibility.  `JobserverExecutor` is a drop-in
replacement for `concurrent.futures.ProcessPoolExecutor` that aims to provide
more robustness at the expense of slower process launching.

Dependencies
------------

None aside from the Python Standard Library.

Comparison with the Python Standard Library
-------------------------------------------

How `Jobserver` and `JobserverExecutor` compare to the standard library's
[`Pool`](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.Pool)
and
[`ProcessPoolExecutor`](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor):

| Feature                           | `Pool` | `ProcessPoolExecutor` | `Jobserver` | `JobserverExecutor` |
|-----------------------------------|:------:|:---------------------:|:-----------:|:-------------------:|
| Nested work shares the slot pool  |   no   |          no           |     yes     |         yes         |
| Background thread                 |  yes   |          yes          |     no      |         yes         |
| Cancel pending work               |   no   |          yes          |     no      |         yes         |
| Cancel running work               |   no   |          no           |     yes     |         no          |
| Detects individual worker death   |   no   |       partial\*       |     yes     |         yes         |
| User-defined launch criteria      |   no   |          no           |     yes     |         yes         |
| Lambdas/closures via `fork`       |   no   |          no           |     yes     |         no          |
| Lambdas/closures via `spawn`      |   no   |          no           |     no      |         no          |
| Lambdas/closures via `forkserver` |   no   |          no           |     no      |         no          |

\* `ProcessPoolExecutor` notices a worker died but cannot pin it to a single
submission, so it fails every outstanding future at once instead of just the
one whose worker died.

Examples
--------

 * [ex01_basic](examples/ex01_basic.py) - Submitting jobs and collecting results
   via shorthand, keyword args, and `submit()`.
 * [ex02_lifecycle](examples/ex02_lifecycle.py) - Polling with `done()`,
   `wait()`, and `result()`, plus `reclaim_resources()` and cleanup.
 * [ex03_nested](examples/ex03_nested.py) - Nesting submissions so child work
   shares slot constraints with its parent.
 * [ex04_cancel](examples/ex04_cancel.py) - Cancelling running work by sending
   `SIGTERM` to a worker via `Future.wait(signal=...)`.
 * [ex05_death](examples/ex05_death.py) - Detecting when a worker process is
   killed unexpectedly via `SubmissionDied`.
 * [ex06_sleep_fn](examples/ex06_sleep_fn.py) - Gating work acceptance on an
   external condition using `sleep_fn`.
 * [ex07_callbacks](examples/ex07_callbacks.py) - Registering `when_done`
   callbacks and draining errors via `CallbackRaised`.
 * [ex08_environment](examples/ex08_environment.py) - Setting and unsetting
   environment variables in child processes via `env=`.
 * [ex09_preexec_fn](examples/ex09_preexec_fn.py) - Using `preexec_fn` as
   a plain callable or context manager factory for entry/exit semantics.
 * [ex10_timeouts](examples/ex10_timeouts.py) - Using non-blocking polling,
   finite deadlines, and `Blocked` from `result()` and `submit()`.
 * [ex11_pdeathsig](examples/ex11_pdeathsig.py) - On Linux, using `preexec_fn`
   to call `prctl(PR_SET_PDEATHSIG)` so a child dies when its parent does.
 * [ex12_executor](examples/ex12_executor.py) - Using `JobserverExecutor` as
   a context manager supporting `map()` and `c.f.Future` cancellation.

Testing
-------

Tested with CPython 3.9, 3.10, 3.11, 3.12, 3.13, and 3.14 per [ci](ci) script.<br>
Implementation passes both PEP 8 (per `ruff`) and type-hinting (per `mypy`).<br>
[![Build Status](https://circleci.com/gh/RhysU/jobserver.svg?style=shield)](https://app.circleci.com/pipelines/github/RhysU/jobserver)
