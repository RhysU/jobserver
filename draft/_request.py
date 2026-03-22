# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Request messages (main process -> dispatcher process).

Imperative verbs: commands sent to the dispatcher.
Using NamedTuple for pre-3.10 compatibility (dataclass slots require 3.10+).
"""
import typing


class Submit(typing.NamedTuple):
    work_id: int
    fn: typing.Callable
    args: tuple
    kwargs: dict


class Cancel(typing.NamedTuple):
    pass


class Shutdown(typing.NamedTuple):
    pass
