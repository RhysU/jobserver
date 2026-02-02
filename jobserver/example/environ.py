# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Demo: Environment variables and preexec_fn in workers."""
import os

from ..impl import Jobserver

KEY = "JOBSERVER_DEMO_VAR"


def get_env(key: str) -> str:
    return os.environ.get(key, "MISSING")


def set_env_via_preexec() -> None:
    os.environ[KEY] = "from_preexec"


if __name__ == "__main__":
    jobserver = Jobserver(slots=1)

    # Set environment variable via env parameter
    future = jobserver.submit(fn=get_env, args=(KEY,), env={KEY: "hello"})
    assert future.result() == "hello"

    # Without env, variable is not set
    future = jobserver.submit(fn=get_env, args=(KEY,))
    assert future.result() == "MISSING"

    # Use preexec_fn to run setup code before the function
    future = jobserver.submit(
        fn=get_env, args=(KEY,), preexec_fn=set_env_via_preexec
    )
    assert future.result() == "from_preexec"

    # preexec_fn runs after env is applied (preexec_fn wins)
    future = jobserver.submit(
        fn=get_env,
        args=(KEY,),
        env={KEY: "from_env"},
        preexec_fn=set_env_via_preexec,
    )
    assert future.result() == "from_preexec"

    print("environ: OK")
