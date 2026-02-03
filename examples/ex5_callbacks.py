# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 5 shows callbacks, exception handling, and CallbackRaised."""
from logging import basicConfig, info, DEBUG

from jobserver import CallbackRaised, Jobserver


def main() -> None:
    jobserver = Jobserver(context="fork", slots=2)

    # Register callbacks that fire when the future completes
    results = []  # type: list
    future = jobserver.submit(fn=len, args=("hello",))
    future.when_done(callback_record, results, "first")
    future.when_done(callback_record, results, "second")
    info("Result: %s", future.result())
    info("Callbacks fired: %s", results)

    # Register a callback after the future already has a result.
    # It fires immediately.
    future.when_done(callback_record, results, "after-done")
    info("Callbacks after done: %s", results)

    # When a callback raises, CallbackRaised wraps the original exception.
    # Re-calling done() drains the remaining callbacks one by one.
    future_err = jobserver.submit(fn=len, args=("world",))
    future_err.when_done(callback_raise, ValueError, "bad")
    future_err.when_done(callback_raise, TypeError, "worse")
    future_err.when_done(callback_record, results, "survivor")

    for i in range(3):
        try:
            future_err.done()
            info("done() call %d: no more errors", i)
            break
        except CallbackRaised as e:
            info(
                "done() call %d: caught %s",
                i,
                type(e.__cause__).__name__,
            )

    # The result is still available after all callbacks drain
    info("Result after errors: %s", future_err.result())


def callback_record(results: list, tag: str) -> None:
    """Append a tag to the results list."""
    results.append(tag)


def callback_raise(klass: type, *args) -> None:
    """Raise the requested exception."""
    raise klass(*args)


if __name__ == "__main__":
    basicConfig(
        level=DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
