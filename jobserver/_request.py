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

__all__ = ["Submit", "Cancel", "Shutdown"]


class Submit(typing.NamedTuple):
    """Submit work for execution."""

    work_id: int
    fn: Callable
    args: tuple
    kwargs: dict


class Cancel(typing.NamedTuple):
    """Cancel all pending (not yet dispatched) work."""


class Shutdown(typing.NamedTuple):
    """Shut down the dispatcher process."""
