# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
A nestable Jobserver with futures, callbacks, and complete type-hinting

Jobserver is similar in spirit to multiprocessing.Pool or
concurrent.futures.Executor with a few differences:

 * First, the implementation choices are based upon
   https://www.gnu.org/software/make/manual/html_node/POSIX-Jobserver.html.
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

In particular, Jobserver does not inherit from concurrent.futures.Executor
because that Executor API fundamentally requires a background thread for
asynchronously issuing concurrent.futures.Future callbacks.  Jobserver,
eschewing threads, consequently is both somehow less-than and more-than a
standard Executor.

In contrast, JobserverExecutor combines a Jobserver with a background thread
to provide full concurrent.futures.Executor compatibility.  JobserverExecutor
is a drop-in replacement for concurrent.futures.ProcessPoolExecutor that aims
to provide more robustness at the expense of slower process launching.

All logic tested with CPython 3.9, 3.10, 3.11, 3.12, 3.13, and 3.14.
Implementation passes both PEP 8 (per ruff) and type-hinting (per mypy).
Refer to https://github.com/RhysU/jobserver for the upstream project.
"""

from ._executor import JobserverExecutor
from ._jobserver import (
    Blocked,
    CallbackRaised,
    Future,
    Jobserver,
    MinimalQueue,
    SubmissionDied,
)
from ._queue import absolute_deadline, relative_timeout, resolve_context

__all__ = (
    "Blocked",
    "CallbackRaised",
    "Future",
    "Jobserver",
    "JobserverExecutor",
    "MinimalQueue",
    "SubmissionDied",
    "absolute_deadline",
    "relative_timeout",
    "resolve_context",
)
