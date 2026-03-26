"""Shared module-level helpers for acceptance tests.

All helpers must be importable and picklable (required by spawn/forkserver).
"""

import os
import signal
import sys
import time


def return_value(x):
    """Return the argument unchanged."""
    return x


def return_none():
    """Return None explicitly."""
    return None


def return_exception():
    """Return (not raise) an Exception object."""
    return ValueError("I am a returned exception, not raised")


def add(a, b):
    """Add two numbers."""
    return a + b


def add_kw(a, b, c=0):
    """Add with keyword argument."""
    return a + b + c


def length(x):
    """Return len(x), picklable alternative to len."""
    return len(x)


def identity(x):
    """Return x."""
    return x


def sleep_and_return(seconds, value):
    """Sleep then return value."""
    time.sleep(seconds)
    return value


def record_timestamps(duration):
    """Record start/end timestamps for concurrency verification."""
    start = time.monotonic()
    time.sleep(duration)
    end = time.monotonic()
    return (start, end)


def make_bytes(size):
    """Return a bytes object of the given size."""
    return b"\x42" * size


def raise_value_error(msg="boom"):
    """Raise a ValueError."""
    raise ValueError(msg)


def raise_type_error(msg="type boom"):
    """Raise a TypeError."""
    raise TypeError(msg)


def raise_runtime_error(msg="runtime boom"):
    """Raise a RuntimeError."""
    raise RuntimeError(msg)


class CustomTestError(Exception):
    """A custom exception for testing propagation."""
    pass


def raise_custom_error():
    """Raise a CustomTestError."""
    raise CustomTestError("custom boom")


def raise_system_exit():
    """Raise SystemExit (a BaseException, not Exception)."""
    raise SystemExit(1)


def raise_keyboard_interrupt():
    """Raise KeyboardInterrupt (a BaseException)."""
    raise KeyboardInterrupt()


def exit_abruptly(code=1):
    """Call os._exit() to kill the process without cleanup."""
    os._exit(code)


def self_sigkill():
    """Send SIGKILL to self."""
    os.kill(os.getpid(), signal.SIGKILL)


def self_sigterm():
    """Send SIGTERM to self."""
    os.kill(os.getpid(), signal.SIGTERM)


def self_sigsegv():
    """Send SIGSEGV to self."""
    os.kill(os.getpid(), signal.SIGSEGV)


def infinite_loop():
    """Loop forever (for timeout testing)."""
    while True:
        time.sleep(0.1)


def get_env_var(key):
    """Return an environment variable value or None."""
    return os.environ.get(key)


def get_cwd():
    """Return current working directory."""
    return os.getcwd()


def noop():
    """Do nothing."""
    return None


def preexec_chdir_tmp():
    """Change directory to /tmp (for preexec_fn testing)."""
    os.chdir("/tmp")


def preexec_raise():
    """Raise from preexec_fn."""
    raise RuntimeError("preexec failed")


def nested_submit(jobserver, fn, args=(), timeout=None):
    """Submit work from within a child process (nesting test)."""
    future = jobserver.submit(fn=fn, args=args, timeout=timeout)
    return future.result()


def nested_submit_two(jobserver, fn, args1=(), args2=(), timeout=None):
    """Submit two jobs from within a child process."""
    f1 = jobserver.submit(fn=fn, args=args1, timeout=timeout)
    f2 = jobserver.submit(fn=fn, args=args2, timeout=timeout)
    return (f1.result(), f2.result())


def nested_three_levels(jobserver):
    """Three levels of nesting: parent -> child -> grandchild."""
    return jobserver.submit(
        fn=nested_submit,
        args=(jobserver, return_value, (42,)),
        timeout=10,
    ).result()


def sleep_fn_always_veto():
    """A sleep_fn that always vetoes (returns a sleep duration)."""
    return 0.01


class SleepFnCountdown:
    """A sleep_fn that vetoes N times then accepts."""

    def __init__(self, n):
        self.remaining = n

    def __call__(self):
        if self.remaining > 0:
            self.remaining -= 1
            return 0.01
        return None


def callback_appender(results, value):
    """Append value to results list (for callback testing)."""
    results.append(value)


def callback_raiser(msg="callback boom"):
    """A callback that raises."""
    raise RuntimeError(msg)
