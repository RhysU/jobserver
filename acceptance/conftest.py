"""Shared fixtures and constants for acceptance tests.

Provides the fast start method, generous timeouts, and forkserver bootstrap.
"""

import multiprocessing
import os
import sys

# Use forkserver on 3.12+ (fork deprecated when multi-threaded); else fork.
FAST_METHOD = "forkserver" if sys.version_info >= (3, 12) else "fork"

# Generous timeout to avoid flaky failures on slow CI.
TIMEOUT = 30

# All contexts to test for cross-context compatibility.
ALL_METHODS = multiprocessing.get_all_start_methods()


def bootstrap_forkserver():
    """Start the forkserver once so subsequent spawns are fast.

    Silences forkserver stderr to avoid benign traceback noise.
    """
    if FAST_METHOD != "forkserver":
        return
    devnull_fd = os.open(os.devnull, os.O_WRONLY)
    saved_fd = os.dup(2)
    os.dup2(devnull_fd, 2)
    os.close(devnull_fd)
    try:
        ctx = multiprocessing.get_context("forkserver")
        p = ctx.Process(target=_noop)
        p.start()
        p.join()
    finally:
        os.dup2(saved_fd, 2)
        os.close(saved_fd)


def _noop():
    pass
