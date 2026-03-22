# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Response messages (dispatcher process -> main process).

Past participles: events reporting what happened.
Using NamedTuple for pre-3.10 compatibility (dataclass slots require 3.10+).
"""
import typing


class Started(typing.NamedTuple):
    work_id: int


class Completed(typing.NamedTuple):
    work_id: int
    value: typing.Any


class Failed(typing.NamedTuple):
    work_id: int
    exc: BaseException


class Cancelled(typing.NamedTuple):
    work_id: int


class Shutdown(typing.NamedTuple):
    pass
