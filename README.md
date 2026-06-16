Jobserver
=========

A nestable Jobserver with thread-safe futures, callbacks, and type-hinting

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
 * Fifth, Futures can detect when a result pipe closes without a result.
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

| Feature                           | `Jobserver` | `JobserverExecutor` | `ProcessPoolExecutor` | `Pool` |
|-----------------------------------|:-----------:|:-------------------:|:---------------------:|:------:|
| Nested work shares the slots      |     yes     |         yes         |          no           |   no   |
| Background thread                 |     no      |         yes         |          yes          |  yes   |
| Cancel pending work               |     no      |         yes         |          yes          |   no   |
| Cancel running work               |     yes     |         no          |          no           |   no   |
| Detects individual lost results   |     yes     |         yes         |       partial\*       |   no   |
| User-defined launch criteria      |     yes     |         yes         |          no           |   no   |
| Lambdas/closures via `fork`       |     yes     |         no          |          no           |   no   |
| Lambdas/closures via `spawn`      |     no      |         no          |          no           |   no   |
| Lambdas/closures via `forkserver` |     no      |         no          |          no           |   no   |

\* Each submission owns its own result pipe, so a pipe that closes without a
result is reported as `LostResult` against exactly that one `Future`.
`ProcessPoolExecutor`, by contrast, cannot tell which submission lost its
worker, so it fails all outstanding futures at once.

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
 * [ex05_death](examples/ex05_death.py) - Detecting a submission whose result
   pipe closes without a result (e.g. a killed worker) via `LostResult`.
 * [ex06_sleep_fn](examples/ex06_sleep_fn.py) - Gating work acceptance on an
   external condition using `replace_sleep()`.
 * [ex07_callbacks](examples/ex07_callbacks.py) - Registering `when_done`
   callbacks and draining errors via `CallbackRaised`.
 * [ex08_environment](examples/ex08_environment.py) - Setting and unsetting
   environment variables in child processes via `modify_env()`.
 * [ex09_preexec_fn](examples/ex09_preexec_fn.py) - Using `replace_preexec()`
   with a plain callable or context manager factory for entry/exit semantics.
 * [ex10_timeouts](examples/ex10_timeouts.py) - Using non-blocking polling,
   finite deadlines, and `Blocked` from `result()` and `submit()`.
 * [ex11_pdeathsig](examples/ex11_pdeathsig.py) - On Linux, using
   `replace_preexec()` to call `prctl(PR_SET_PDEATHSIG)` so a child dies when
   its parent does.
 * [ex12_executor](examples/ex12_executor.py) - Using `JobserverExecutor` as
   a context manager supporting `map()` and `c.f.Future` cancellation.

Testing
-------

Tested with CPython 3.9, 3.10, 3.11, 3.12, 3.13, and 3.14 per [ci](ci) script.<br>
Implementation passes both PEP 8 (per `ruff`) and type-hinting (per `mypy`).<br>
[![Build Status](https://circleci.com/gh/RhysU/jobserver.svg?style=shield)](https://app.circleci.com/pipelines/github/RhysU/jobserver)
