# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""JobserverExecutor request messages (main process -> dispatcher process).

Imperative verbs: commands sent to the dispatcher.
Using NamedTuple (dataclass slots require 3.10+).
"""

import typing
from collections.abc import Callable
from typing import Any, Optional

__all__ = ["Submit", "Cancel", "Shutdown"]


class Submit(typing.NamedTuple):
    """Submit work for execution."""

    work_id: int
    fn: Callable
    args: tuple
    kwargs: dict[str, Any]


class Cancel(typing.NamedTuple):
    """Cancel pending (not yet dispatched) work.

    When work_id is None, cancel all pending work.
    When work_id is set, cancel only that specific work item.
    """

    work_id: Optional[int] = None


class Shutdown(typing.NamedTuple):
    """Shut down the dispatcher process."""
