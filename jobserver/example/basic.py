# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Demo: Basic Jobserver usage."""
from ..impl import Jobserver


def square(x: int) -> int:
    return x * x


if __name__ == "__main__":
    # Create a jobserver with 2 slots
    jobserver = Jobserver(slots=2)

    # Submit work using the __call__ shorthand
    future_a = jobserver(square, 5)
    future_b = jobserver(square, 7)

    # Submit work using the explicit submit() method
    future_c = jobserver.submit(fn=len, args=("hello",))

    # Retrieve results (blocks until complete)
    assert future_a.result() == 25
    assert future_b.result() == 49
    assert future_c.result() == 5

    # Results can be retrieved multiple times
    assert future_a.result() == 25

    print("basic: OK")
