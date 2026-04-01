# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Example 1 shows submitting jobs and collecting results."""

from logging import INFO, basicConfig, info

from jobserver import Jobserver


def main() -> None:
    """Shows submitting jobs and collecting results."""
    # Instance will use the default multiprocessing context for this platform
    jobserver = Jobserver(slots=2)

    # Full Jobserver.submit(...) example with args and kwargs
    # The submit(...) method has many additional options
    future_a = jobserver.submit(fn=pow, args=(2, 10), kwargs={"mod": 1000})

    # Simpler shorthand via Jobserver.__call__(...) with positional args
    future_b = jobserver(len, (1, 2, 3))

    # Simpler shorthand also permits kwargs or mixed args/kwargs (not shown)
    future_c = jobserver(str, object=42)

    # Results retrieved in arbitrary order
    info("str(object=42) = %s", future_c.result())
    info("len((1, 2, 3)) = %s", future_b.result())
    info("pow(2, 10, mod=1000) = %s", future_a.result())

    # Map over multiple inputs yielding results in order
    # (argses and kwargses are the plurals of args and kwargs)
    lengths = list(jobserver.map(fn=len, argses=[("ab",), ("cde",)]))
    info("lengths via map: %s", lengths)


if __name__ == "__main__":
    basicConfig(
        level=INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    main()
