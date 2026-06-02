# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 6 shows callbacks, exception handling, and CallbackRaised."""

from logging import INFO, basicConfig, captureWarnings, info

from jobserver import CallbackRaised, Jobserver


def main() -> None:
    """Shows callbacks, exception handling, and CallbackRaised."""
    with Jobserver(context="spawn", slots=2) as jobserver:
        future1 = jobserver.submit(fn=len, args=("hello",))

        # Register callbacks to fire after observing the future
        # completes.  Callbacks receive exactly and only the
        # arguments given to when_done(...).
        accumulator: list = []
        future1.when_done(accumulator.append, "first")
        future1.when_done(accumulator.append, "second")
        info("Result: %s", future1.result())
        info("Callbacks fired: %s", accumulator)

        # Unlike concurrent.futures.Future.add_done_callback(...), you must
        # pass the future as an argument if wanting the callback to receive it.
        future1.when_done(type, future1)

        # Registering a callback after completion causes it to immediately fire
        future1.when_done(accumulator.append, "after-completion")
        info("Callbacks after completion: %s", accumulator)

        # when_done(...) returns a Future-specific seqno per registration.
        future2 = jobserver.submit(fn=len, args=("world",))
        assert 0 == future2.when_done(raise_exception, klass=ValueError)
        assert 1 == future2.when_done(raise_exception, klass=TypeError)
        assert 2 == future2.when_done(accumulator.append, "survivor")

        # Each wait() fires pending callbacks; raising ones surface as
        # CallbackRaised wrapping the original exception, others run silently.
        # CallbackRaised.seqno reports which registration raised.  Loop until
        # no error is raised to drain all callbacks.
        for i in range(3):
            try:
                future2.wait()
                info("wait() call %d: no more errors", i)
                break
            except CallbackRaised as e:
                info(
                    "wait() call %d: caught %s from registration seqno=%d",
                    i,
                    type(e.__cause__).__name__,
                    e.seqno,
                )

        # The result is still available after all callbacks drain
        info("Result after errors: %s", future2.result())

        # Callbacks may register additional callbacks or be provided any future
        future1.when_done(future1.when_done, tuple)
        future1.when_done(future2.when_done, list)


def raise_exception(klass: type) -> None:
    """Raise the requested exception."""
    raise klass()


if __name__ == "__main__":
    basicConfig(
        level=INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    captureWarnings(True)
    main()
