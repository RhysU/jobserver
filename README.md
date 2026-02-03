Jobserver
=========

A nestable Jobserver with futures, callbacks, and complete type-hinting!<br>
[![Build Status](https://circleci.com/gh/RhysU/jobserver.svg?style=shield)](https://app.circleci.com/pipelines/github/RhysU/jobserver)

Purpose
-------

This Jobserver is similar in spirit to multiprocessing.Pool or
concurrent.futures.Executor with a few differences:

 * First, the [implementation](jobserver/impl.py) choices are based upon the [GNU Make
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

Examples
--------

 1. [ex1_basic.py](examples/ex1_basic.py) — Submitting jobs and collecting results via shorthand, keyword args, and `submit()`.
 2. [ex2_nested.py](examples/ex2_nested.py) — Nested submissions where child work shares slot constraints with its parent.
 3. [ex3_death.py](examples/ex3_death.py) — Detecting when a worker process is killed unexpectedly via `SubmissionDied`.
 4. [ex4_sleep_fn.py](examples/ex4_sleep_fn.py) — Gating work acceptance on an external condition using `sleep_fn`.
 5. [ex5_callbacks.py](examples/ex5_callbacks.py) — Registering `when_done` callbacks and draining errors via `CallbackRaised`.
 6. [ex6_environment.py](examples/ex6_environment.py) — Setting and unsetting environment variables in child processes via `env=`.
 7. [ex7_timeouts.py](examples/ex7_timeouts.py) — Non-blocking polling, finite deadlines, and `Blocked` on `done()`, `result()`, and `submit()`.
 8. [ex8_pdeathsig.py](examples/ex8_pdeathsig.py) — Using `preexec_fn` to call `prctl(PR_SET_PDEATHSIG)` so a child dies when its parent does.

Testing
-------

Tested with CPython 3.8, 3.9, 3.10, 3.11, 3.12, and 3.13.<br>
Implementation passes both PEP 8 (per flake8) and type-hinting (per mypy).<br>
