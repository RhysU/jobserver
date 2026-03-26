Jobserver
=========

A nestable Jobserver with thread-safe futures, callbacks, and complete
type-hinting

Purpose
-------

A Jobserver is similar in spirit to multiprocessing.Pool or
concurrent.futures.Executor with a few differences:

 * First, the [implementation](jobserver/_jobserver.py) choices are based upon the [GNU Make
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

The Jobserver does not inherit from concurrent.futures.Executor because that
Executor API fundamentally requires a background thread for asynchronously
issuing concurrent.futures.Future callbacks.  Jobserver, eschewing threads,
consequently is both somehow less-than and more-than a standard Executor.

In contrast, [JobserverExecutor](jobserver/_executor.py) combines a Jobserver
with a background thread to provide full concurrent.futures.Executor
compatibility.  JobserverExecutor is a drop-in replacement for
concurrent.futures.ProcessPoolExecutor that aims to provide more robustness
at the expense of slower process launching.

Examples
--------

 * [ex1_basic](examples/ex1_basic.py) — Submitting jobs and collecting results via shorthand, keyword args, and `submit()`.
 * [ex2_nested](examples/ex2_nested.py) — Nested submissions where child work shares slot constraints with its parent.
 * [ex3_death](examples/ex3_death.py) — Detecting when a worker process is killed unexpectedly via `SubmissionDied`.
 * [ex4_sleep_fn](examples/ex4_sleep_fn.py) — Gating work acceptance on an external condition using `sleep_fn`.
 * [ex5_callbacks](examples/ex5_callbacks.py) — Registering `when_done` callbacks and draining errors via `CallbackRaised`.
 * [ex6_environment](examples/ex6_environment.py) — Setting and unsetting environment variables in child processes via `env=`.
 * [ex7_timeouts](examples/ex7_timeouts.py) — Non-blocking polling, finite deadlines, and `Blocked` on `done()`, `result()`, and `submit()`.
 * [ex8_pdeathsig](examples/ex8_pdeathsig.py) — Using `preexec_fn` to call `prctl(PR_SET_PDEATHSIG)` so a child dies when its parent does.
 * [ex9_executor](examples/ex9_executor.py) — `JobserverExecutor` as a context manager supporting `map()` and `c.f.Future` cancellation.

Testing
-------

All logic tested with CPython 3.9, 3.10, 3.11, 3.12, 3.13, and 3.14.<br>
Implementation passes both PEP 8 (per flake8) and type-hinting (per mypy).<br>
[![Build Status](https://circleci.com/gh/RhysU/jobserver.svg?style=shield)](https://app.circleci.com/pipelines/github/RhysU/jobserver)
