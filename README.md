Jobserver
=========

A nestable Jobserver with futures, callbacks, and complete type-hinting!<br>
Read [jobserver.py](jobserver.py) for the API and note usage examples inside
JobserverTest.

This Jobserver is similar in spirit to multiprocessing.Pool or
concurrent.futures.Executor with a few differences:

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

Implementation tested with CPython 3.6, 3.7, 3.8, 3.9, 3.10, 3.11, and 3.12.<br>
Implementation passes both PEP 8 (per flake8) and type-hinting (per mypy).<br>
[![Build Status](https://circleci.com/gh/RhysU/jobserver.svg?style=shield)](https://app.circleci.com/pipelines/github/RhysU/jobserver)<br>
