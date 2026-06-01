# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Portability across interpreters and platforms."""

import os
import pickle
import signal

# Failures raised when ForkingPickler.dumps() (hence MinimalQueue.put())
# cannot serialize a payload: pickle's own PicklingError, an AttributeError
# when the payload names a locally-defined (unpicklable-by-name) class, and a
# TypeError when an object is fundamentally unpicklable.  Both the worker's
# _worker_entrypoint and the executor's _responses_put_failed classify dump
# failures with this same tuple so their fallbacks stay aligned.
PICKLE_DUMP_ERRORS = (pickle.PicklingError, AttributeError, TypeError)


def ignore_sigpipe() -> None:
    """Ignore SIGPIPE so broken pipes raise BrokenPipeError.

    CPython does this at startup; PyPy does not. Called only in child
    workers and the dispatcher, never the user's main process; a child's
    preexec_fn may reinstall a different SIGPIPE handler if desired.
    """
    if hasattr(signal, "SIGPIPE"):
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)


def sched_getaffinity0() -> int:
    """Return the number of usable CPUs for the current process.

    Falls back to os.cpu_count() when os.sched_getaffinity is
    not available (e.g. PyPy, macOS).
    """
    # The sched_getaffinity syscall may exist on some container runtimes
    # yet raise OSError when invoked, so fall back on failure.
    if hasattr(os, "sched_getaffinity"):
        try:
            return len(os.sched_getaffinity(0))
        except OSError:
            pass
    # os.cpu_count() can return None
    return os.cpu_count() or 1
