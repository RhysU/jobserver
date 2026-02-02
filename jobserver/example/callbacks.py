# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Demo: Callbacks fire when work completes."""
from ..impl import Jobserver


def record_completion(results: list, index: int, value: int) -> None:
    results[index] = value


if __name__ == "__main__":
    jobserver = Jobserver(slots=2)
    results = [0, 0, 0]

    # Register callback before work completes
    future = jobserver(len, "hello")
    future.when_done(record_completion, results, 0, 100)
    future.when_done(record_completion, results, 1, 200)

    # Wait for completion
    assert future.result() == 5
    assert results[0] == 100
    assert results[1] == 200

    # Callbacks registered after completion fire immediately
    future.when_done(record_completion, results, 2, 300)
    assert results[2] == 300

    print("callbacks: OK")
