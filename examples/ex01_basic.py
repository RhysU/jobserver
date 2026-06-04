# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 1 shows submitting jobs and collecting results."""

from logging import INFO, basicConfig, captureWarnings, info

from jobserver import Jobserver


def main() -> None:
    """Shows submitting jobs and collecting results."""
    # Instance will use the default multiprocessing context for this platform
    with Jobserver(slots=2) as jobserver:
        # Calling Jobserver.submit(...) submits work and returns a Future
        future_a = jobserver.submit(fn=pow, args=(2, 10), kwargs={"mod": 1000})

        # Simpler shorthand via Jobserver.__call__(...) with positional args
        future_b = jobserver(len, (1, 2, 3))

        # Shorthand also permits kwargs or mixed args/kwargs (not shown)
        future_c = jobserver(str, object=42)

        # Results retrieved in arbitrary order
        info("str(object=42) = %s", future_c.result())
        info("len((1, 2, 3)) = %s", future_b.result())
        info("pow(2, 10, mod=1000) = %s", future_a.result())

        # Map over multiple inputs yielding results in order
        # (Argument names argses and kwargses are plurals for args and kwargs)
        lengths = list(jobserver.map(fn=len, argses=[("ab",), ("cde",)]))
        info("lengths via map: %s", lengths)

        # A worker raising an ordinary Exception has it re-raised by result().
        future_err = jobserver.submit(fn=int, args=("not a number",))
        try:
            future_err.result()
            raise RuntimeError("Expected ValueError was not raised")
        except ValueError as e:
            info("Worker raised and result() re-raised: %r", e)
            # __cause__ shows the worker's traceback for the original failure.
            info("Child traceback preserved via __cause__: %s", e.__cause__)


# The spawn and forkserver start methods re-import this module in every
# child, so the if __name__ == "__main__" guard below is required.
if __name__ == "__main__":
    basicConfig(
        level=INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    captureWarnings(True)
    main()
